"""Fail-closed run recovery using immutable plans and backend-verified evidence."""

from dataclasses import asdict
import hashlib
import json
import os
from pathlib import Path
import socket
import uuid

from config.settings import settings
from orchestration.run_manifest import (
    checkpoint_stage,
    heartbeat,
    load_manifest,
    local_owner,
    reference_output,
    save_manifest,
    utc_now,
)
from storage.storage_manager import storage


def _digest(value):
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
    ).hexdigest()


def _backend_identity(mode):
    from storage.local_backend import LocalBackend

    if settings.runtime_mode != mode:
        raise ValueError("Manifest mode differs from configured runtime mode")
    backend = storage.backend
    if mode in {"test", "local"}:
        if not isinstance(backend, LocalBackend):
            raise ValueError("Recovery requires the actual configured local backend")
        return {
            "kind": "local",
            "lake": str(backend.base_dir.resolve()),
            "warehouse": str(Path(settings.database.sqlite_path).resolve()),
        }
    from storage.minio_backend import MinIOBackend

    if not isinstance(backend, MinIOBackend):
        raise ValueError("Recovery requires the actual configured MinIO backend")
    db = settings.database
    from database.warehouse import WarehouseManager
    warehouse=WarehouseManager(mode=mode)
    try:
        cursor=warehouse._conn.cursor()
        cursor.execute('SELECT current_schema()')
        schema=cursor.fetchone()[0]
        if not schema:raise ValueError('Recovery requires a resolved SQL schema')
    finally:warehouse.close()
    return {
        "kind": "minio-postgres",
        "endpoint": backend.endpoint,
        "bucket": backend.bucket,
        "secure": backend.secure,
        "warehouse_host": db.host,
        "warehouse_port": db.port,
        "warehouse_database": db.database,
        "warehouse_schema":schema,
    }


class AirflowMetadataAdapter:
    """Query Airflow's configured metadata DB, never an operator status file.

    Configure AIRFLOW__DATABASE__SQL_ALCHEMY_CONN for the actual scheduler DB.
    Both the DagRun and every task instance must be terminal.
    """

    def verify_stopped(self, owner):
        from operations.airflow_metadata import verify_terminal_run

        return verify_terminal_run(owner)


def _stopped(owner, airflow_adapter=None):
    if owner.get("kind") == "local":
        pids = owner.get("pids")
        if owner.get("hostname") != socket.gethostname() or not isinstance(pids, list) or not pids:
            raise ValueError("Local owner requires same-host recorded process evidence")
        for pid in pids:
            if type(pid) is not int or pid <= 0:
                raise ValueError("Invalid owner PID evidence")
            try:
                os.kill(pid, 0)
            except ProcessLookupError:
                continue
            except PermissionError as error:
                raise RuntimeError("Cannot prove owner process stopped") from error
            raise RuntimeError(f"Recorded owner PID {pid} is live; recovery refused")
        return {"kind": "local", "hostname": socket.gethostname(), "stopped_pids": pids}
    if owner.get("kind") == "airflow":
        if not owner.get("dag_id") or not owner.get("dag_run_id") or airflow_adapter is None:
            raise ValueError("Airflow owner requires explicit metadata backend adapter")
        evidence = airflow_adapter.verify_stopped(owner)
        if owner.get("pids"):
            evidence["worker_processes"] = _stopped(dict(owner, kind="local"))
        return evidence
    raise ValueError("Recorded owner evidence is required; stopped self-attestation is unsupported")


def _verify_outputs(manifest):
    if manifest.status not in {"completed", "aborted"}:
        for output in manifest.working_outputs:
            if reference_output(output["path"]) != output:
                raise ValueError("Working output differs: " + output["path"])
    completed = []
    incomplete = False
    for stage in manifest.stage_order:
        checkpoint = manifest.checkpoints.get(stage, {})
        if checkpoint.get("state") != "completed":
            incomplete = True
            continue
        if incomplete:
            raise ValueError("Completed checkpoints must form a contiguous stage prefix")
        outputs = checkpoint.get("outputs")
        if not outputs:
            raise ValueError(f"Completed stage {stage} has no verifiable output manifest")
        for output in outputs:
            if reference_output(output["path"]) != output:
                raise ValueError(f"Recorded output differs: {output['path']}")
        completed.append(stage)
    return completed


def _assert_lease(warehouse, run_id):
    cursor = warehouse._conn.cursor()
    param = "%s" if warehouse._engine_type == "postgres" else "?"
    cursor.execute("SELECT run_id FROM pipeline_run_lease WHERE resource=" + param, ("canonical",))
    row = cursor.fetchone()
    if row is None or row[0] != run_id:
        raise RuntimeError("Recovery run is not the canonical lease owner")


def _release(warehouse, run_id):
    param = "%s" if warehouse._engine_type == "postgres" else "?"
    with warehouse._conn:
        warehouse._conn.cursor().execute(
            f"DELETE FROM pipeline_run_lease WHERE resource={param} AND run_id={param}", ("canonical", run_id)
        )


def inspect_run(manifest_path):
    """Read durable state without declaring an old heartbeat safe to recover."""
    manifest = load_manifest(manifest_path)
    return {
        "manifest_path": manifest_path,
        "manifest": asdict(manifest),
        "manifest_hash": _digest(asdict(manifest)),
        "backend": _backend_identity(manifest.mode),
    }


def plan_recovery(manifest_path, output, action="resume", airflow_adapter=None):
    if action not in {"resume", "abort"}:
        raise ValueError("Recovery action must be resume or abort")
    from operations.backup import _admission

    manifest = load_manifest(manifest_path)
    identity = _backend_identity(manifest.mode)
    with _admission(manifest.mode, exclusive=True) as warehouse:
        manifest = load_manifest(manifest_path)
        _assert_lease(warehouse, manifest.run_id)
        evidence = _stopped(manifest.owner, airflow_adapter)
        completed = _verify_outputs(manifest)
        if manifest.status in {"completed", "aborted"}:
            raise ValueError("Terminal manifest cannot be recovered")
        remaining = manifest.stage_order[len(completed) :]
        if action == "resume" and not remaining:
            raise ValueError("Run has no declared resumable stages")
        plan = {
            "version": 1,
            "operation_id": str(uuid.uuid4()),
            "created_at": utc_now(),
            "manifest_path": manifest_path,
            "manifest_hash": _digest(asdict(manifest)),
            "run_id": manifest.run_id,
            "mode": manifest.mode,
            "backend": identity,
            "action": action,
            "next_stage": remaining[0] if remaining else None,
            "verified_stages": completed,
            "stopped_evidence": evidence,
        }
        plan["plan_hash"] = _digest(plan)
        from operations.backup import _json

        _json(Path(output), plan)
        return plan


def apply_recovery(plan_path, expected_hash, executor=None, airflow_adapter=None):
    """Apply one next stage with a stable operation ID, or abort under the fence.

    A resume executor returns actual output references. It must make its writes
    idempotent using operation_id; recovery never assumes an opaque write safe.
    """
    plan = json.loads(Path(plan_path).read_text())
    claimed = plan.pop("plan_hash", None)
    if claimed != expected_hash or _digest(plan) != expected_hash:
        raise ValueError("Recovery plan hash differs from approved hash")
    plan["plan_hash"] = claimed
    if _backend_identity(plan["mode"]) != plan["backend"]:
        raise ValueError("Recovery backend differs from pinned plan")
    if plan["action"] not in {"resume", "abort"}:
        raise ValueError("Invalid recovery action")
    from operations.backup import _admission, writer_admission

    with _admission(plan["mode"], exclusive=True) as warehouse:
        manifest = load_manifest(plan["manifest_path"])
        if manifest.run_id != plan["run_id"] or manifest.mode != plan["mode"]:
            raise ValueError("Recovery manifest identity changed")
        previous = manifest.recovery_history.get(plan["operation_id"])
        if previous and previous.get("plan_hash") == expected_hash and previous.get("result"):
            _verify_outputs(manifest)
            if previous["result"]["status"] in {"aborted", "completed"}:
                # A process may exit after the terminal manifest PUT but before
                # its conditional SQL DELETE. Finish that release on retry.
                _release(warehouse, manifest.run_id)
            return previous["result"]
        if previous is None and _digest(asdict(manifest)) != plan["manifest_hash"]:
            raise ValueError("Recovery manifest changed after planning")
        _assert_lease(warehouse, manifest.run_id)
        evidence = _stopped(manifest.owner, airflow_adapter)
        if manifest.owner.get("kind") == "airflow" and evidence.get("metadata_backend") != plan["stopped_evidence"].get(
            "metadata_backend"
        ):
            raise ValueError("Airflow metadata backend differs from pinned plan")
        _verify_outputs(manifest)
        if plan["action"] == "abort":
            result = {"status": "aborted", "run_id": manifest.run_id, "operation_id": plan["operation_id"]}
            manifest.status = "aborted"
            manifest.recovery_history[plan["operation_id"]] = {"plan_hash": expected_hash, "result": result}
            heartbeat(manifest)
            _release(warehouse, manifest.run_id)
            return result
        if executor is None:
            raise ValueError("Resume requires an explicit idempotent stage executor")
        # Publish live local ownership before opening the fence. A second recovery
        # now refuses this PID, and canonical writers still see the existing lease.
        manifest.status = "recovering"
        manifest.recovery_history[plan["operation_id"]] = {"plan_hash": expected_hash, "state": "running"}
        heartbeat(manifest, owner=local_owner())
    with writer_admission(manifest.mode):
        try:
            existing = manifest.checkpoints.get(plan["next_stage"], {})
            already_committed = (
                existing.get("state") == "completed" and existing.get("operation_id") == plan["operation_id"]
            )
            if not already_committed:
                outputs = executor(manifest, plan["next_stage"], plan["operation_id"])
                checkpoint_stage(manifest, plan["next_stage"], outputs=outputs, operation_id=plan["operation_id"])
            completed = _verify_outputs(manifest)
            done = len(completed) == len(manifest.stage_order)
            manifest.status = "completed" if done else "running"
            result = {
                "status": manifest.status,
                "run_id": manifest.run_id,
                "operation_id": plan["operation_id"],
                "completed_stage": plan["next_stage"],
            }
            manifest.recovery_history[plan["operation_id"]] = {"plan_hash": expected_hash, "result": result}
            heartbeat(manifest)
            if done:
                from orchestration.run_manifest import RunLease

                lease = RunLease(manifest.mode)
                try:
                    lease.release(manifest.run_id)
                finally:
                    lease.close()
            return result
        except BaseException:
            manifest.status = "failed"
            heartbeat(manifest)
            raise
