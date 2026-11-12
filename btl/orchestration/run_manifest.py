"""Durable JSON handoff between workers; no process-global execution state."""

from dataclasses import asdict, dataclass, field
import hashlib
import json
from datetime import datetime, timezone
from contextvars import ContextVar
from pathlib import PurePosixPath
import os
import socket
from storage.storage_manager import storage


@dataclass
class RunManifest:
    run_id: str
    mode: str
    source_type: str = "dataset"
    batches_by_topic: dict = field(default_factory=dict)
    required_datasets: list = field(default_factory=lambda: ["products", "reviews"])
    silver_paths: dict = field(default_factory=dict)
    dq_report_ids: list = field(default_factory=list)
    dq_metrics: list = field(default_factory=list)
    status: str = "running"
    owner: dict = field(default_factory=dict)
    heartbeat_at: str | None = None
    stage_order: list = field(
        default_factory=lambda: [
            "start",
            "collect_and_publish",
            "spark_batch",
            "data_quality",
            "gold_analytics",
            "warehouse_upsert",
            "finish",
        ]
    )
    current_stage: str | None = None
    checkpoints: dict = field(default_factory=dict)
    working_outputs: list = field(default_factory=list)
    recovery_history: dict = field(default_factory=dict)
    result: object = None


def manifest_path(run_id: str) -> str:
    return f"runs/{hashlib.sha256(run_id.encode()).hexdigest()}/manifest.json"


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def local_owner() -> dict:
    return {"kind": "local", "hostname": socket.gethostname(), "pids": [os.getpid()]}


def worker_owner() -> dict:
    """Bind managed child workers to their actual Airflow DagRun and local PID."""
    raw = os.environ.get("SENTINEL_AIRFLOW_OWNER")
    if not raw:
        return local_owner()
    owner = json.loads(raw)
    if not owner.get("dag_id") or not owner.get("dag_run_id"):
        raise ValueError("Airflow worker requires its actual DagRun identity")
    return dict(local_owner(), kind="airflow", dag_id=owner["dag_id"], dag_run_id=owner["dag_run_id"])


def heartbeat(manifest: RunManifest, owner: dict | None = None) -> str:
    if owner is not None:
        manifest.owner = owner
    manifest.heartbeat_at = utc_now()
    return save_manifest(manifest)


def _output_bytes(path: str) -> bytes:
    value = PurePosixPath(path)
    if not path or value.is_absolute() or ".." in value.parts or "\\" in path:
        raise ValueError("Unsafe output reference path")
    from storage.local_backend import LocalBackend

    backend = storage.backend
    if isinstance(backend, LocalBackend):
        root = backend.base_dir.resolve()
        target = root.joinpath(*value.parts)
        current = root
        for part in value.parts:
            current /= part
            if current.is_symlink():
                raise ValueError("Output reference contains symlink")
        if not target.resolve().is_relative_to(root):
            raise ValueError("Output reference escapes lake")
        return target.read_bytes()
    from storage.minio_backend import MinIOBackend

    if not isinstance(backend, MinIOBackend):
        raise ValueError("Unsupported output backend")
    response = backend.client.get_object(backend.bucket, path)
    try:
        return response.read()
    finally:
        response.close()
        response.release_conn()


def reference_output(path: str) -> dict:
    payload = _output_bytes(path)
    return {"path": path, "sha256": hashlib.sha256(payload).hexdigest(), "size": len(payload)}


def reference_canonical_output(path: str) -> dict:
    """Normalize producer absolute paths without allowing another lake/bucket."""
    from pathlib import Path
    from urllib.parse import urlsplit
    from storage.local_backend import LocalBackend

    if isinstance(storage.backend, LocalBackend) and Path(path).is_absolute():
        path = Path(path).relative_to(storage.backend.base_dir.absolute()).as_posix()
    elif path.startswith(("s3://", "s3a://")):
        uri = urlsplit(path)
        if uri.netloc != getattr(storage.backend, "bucket", None):
            raise ValueError("Output belongs to another bucket")
        path = uri.path.lstrip("/")
    return reference_output(path)


def refresh_working_outputs(manifest, stage):
    """Pin current canonical files while the run retains its writer lease.

    Historical completed receipts remain reusable after subsequent legitimate
    generations replace these canonical files; immutable stage evidence remains
    verified independently.
    """
    if stage not in {"spark_batch", "data_quality", "gold_analytics", "warehouse_upsert", "finish"}:
        return
    prefixes = ["silver/"]
    prefixes += ["gold/data_quality_metrics/"] if stage in {"spark_batch", "data_quality"} else ["gold/"]
    objects = {
        path
        for prefix in prefixes
        for path in storage.list_objects(prefix)
        if "/_temporary/" not in path
        and not path.rsplit("/", 1)[-1].endswith(".crc")
        and not path.rsplit("/", 1)[-1].startswith(".")
    }
    manifest.working_outputs = [reference_output(path) for path in sorted(objects)]


def checkpoint_stage(
    manifest: RunManifest,
    stage: str,
    outputs: list | None = None,
    state: str = "completed",
    operation_id: str | None = None,
) -> str:
    if state not in {"running", "completed", "failed"}:
        raise ValueError("Invalid checkpoint state")
    if stage not in manifest.stage_order:
        raise ValueError("Stage must belong to the declared stage_order")
    references = outputs or []
    for reference in references:
        if reference_output(reference["path"]) != reference:
            raise ValueError("Checkpoint output differs from reference")
    manifest.current_stage = stage
    manifest.checkpoints[stage] = {
        "state": state,
        "outputs": references,
        "operation_id": operation_id,
        "updated_at": utc_now(),
    }
    return heartbeat(manifest)


_current_manifest = ContextVar("canonical_run_manifest", default=None)


def current_manifest() -> RunManifest | None:
    """Return the CLI owner's manifest for explicit stage telemetry hooks."""
    return _current_manifest.get()


def save_manifest(manifest: RunManifest) -> str:
    payload = asdict(manifest)
    json.dumps(payload, allow_nan=False)  # Refuse DataFrames/NaN before persistence.
    path = manifest_path(manifest.run_id)
    from storage.local_backend import LocalBackend

    if isinstance(storage.backend, LocalBackend):
        import os, tempfile

        target = storage.backend._get_full_path(path)
        target.parent.mkdir(parents=True, exist_ok=True)
        fd, temp = tempfile.mkstemp(dir=target.parent, prefix=".manifest-")
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as stream:
                json.dump(payload, stream, ensure_ascii=False, allow_nan=False)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temp, target)
        finally:
            if os.path.exists(temp):
                os.unlink(temp)
    else:
        # One complete object PUT; never a partially overwritten local JSON file.
        storage.write_json(path, payload)
    return path


def load_manifest(path: str) -> RunManifest:
    if not path or not path.startswith("runs/") or ".." in path.split("/"):
        raise ValueError("A valid run manifest path is required")
    return RunManifest(**storage.read_json(path))


def completed_result(run_id: str, mode: str):
    """Return a committed CLI receipt only after verifying every stage output."""
    from operations.recovery import _backend_identity, _verify_outputs

    _backend_identity(mode)
    manifest = load_manifest(manifest_path(run_id))
    if manifest.run_id != run_id or manifest.mode != mode or manifest.status != "completed":
        raise ValueError("Run does not have a completed receipt")
    if not manifest.stage_order or len(_verify_outputs(manifest)) != len(manifest.stage_order):
        raise ValueError("Completed run requires verified stage checkpoints")
    return manifest.result


class RunLease:
    """Durable single-run ownership of classic canonical Parquet outputs.

    A crashed run retains ownership until an explicit recovery releases it.
    This favors integrity over automatically stealing an expired lease.
    """

    def __init__(self, mode):
        from database.warehouse import WarehouseManager

        self.wh = WarehouseManager(mode=mode)
        from operations.backup import writer_admission

        try:
            with writer_admission(mode):
                self.wh.execute(
                    "CREATE TABLE IF NOT EXISTS pipeline_run_lease (resource TEXT PRIMARY KEY, run_id TEXT NOT NULL)"
                )
        except Exception:
            self.wh.close()
            raise

    def acquire(self, run_id):
        with self.wh._conn:
            cur = self.wh._conn.cursor()
            param = "%s" if self.wh._engine_type == "postgres" else "?"
            cur.execute(
                f"INSERT INTO pipeline_run_lease (resource,run_id) VALUES ({param},{param}) ON CONFLICT (resource) DO NOTHING",
                ("canonical", run_id),
            )
            cur.execute("SELECT run_id FROM pipeline_run_lease WHERE resource = " + param, ("canonical",))
            if cur.fetchone()[0] != run_id:
                raise RuntimeError("Another run owns canonical outputs; recover/release its lease before retrying")

    def assert_owner(self, run_id):
        cur = self.wh._conn.cursor()
        cur.execute(
            "SELECT run_id FROM pipeline_run_lease WHERE resource="
            + ("%s" if self.wh._engine_type == "postgres" else "?"),
            ("canonical",),
        )
        row = cur.fetchone()
        if row is None or row[0] != run_id:
            raise RuntimeError("Run is not the current canonical-output owner")

    def release(self, run_id):
        with self.wh._conn:
            cur = self.wh._conn.cursor()
            param = "%s" if self.wh._engine_type == "postgres" else "?"
            cur.execute(
                f"DELETE FROM pipeline_run_lease WHERE resource={param} AND run_id={param}", ("canonical", run_id)
            )

    def close(self):
        self.wh.close()


def canonical_writer(function):
    """Own one CLI run; interrupted writes retain their durable lease for recovery.

    Nested decorated entrypoints share ownership. Opaque CLI functions gain
    heartbeat telemetry but need explicit stage hooks before they can resume.
    """
    import functools, inspect, uuid
    from config.settings import settings

    @functools.wraps(function)
    def wrapped(*args, **kwargs):
        bound = inspect.signature(function).bind_partial(*args, **kwargs)
        mode = bound.arguments.get("mode") or settings.runtime_mode
        parent = current_manifest()
        if parent is not None:
            if parent.mode != mode or bound.arguments.get("run_id") not in (None, parent.run_id):
                raise ValueError("Nested canonical writer ownership differs")
            return function(*args, **kwargs)
        settings.set_runtime_mode(mode)
        storage.set_mode(mode)
        run_id = bound.arguments.get("run_id") or str(uuid.uuid4())
        lease = RunLease(mode)
        token = None
        try:
            from operations.backup import writer_admission

            with writer_admission(mode):
                path = manifest_path(run_id)
                article_lifecycle = function.__module__ == "spark.batch.article_etl"
                if not article_lifecycle and storage.object_exists(path):
                    if load_manifest(path).status == "completed":
                        return completed_result(run_id, mode)
                    raise RuntimeError("Existing CLI run requires an explicit recovery plan")
                owner = worker_owner() if not article_lifecycle else {}
                lease.acquire(run_id)
                # Article publishing already owns an immutable SQL/lake lifecycle.
                if function.__module__ == "spark.batch.article_etl":
                    try:
                        return function(*args, **kwargs)
                    finally:
                        lease.release(run_id)
                manifest = RunManifest(
                    run_id, mode, source_type=bound.arguments.get("source_type", "dataset"), owner=owner, stage_order=[]
                )
                heartbeat(manifest)
                token = _current_manifest.set(manifest)
                try:
                    result = function(*args, **kwargs)
                except BaseException:
                    manifest.status = "failed"
                    heartbeat(manifest)
                    try:
                        from operations.status import OperationStatus

                        with OperationStatus(mode=mode) as status:
                            status.fail(run_id)
                    except Exception:
                        # Preserve the pipeline failure when SQL telemetry itself
                        # is unavailable; durable failed manifest remains evidence.
                        pass
                    raise
                manifest.status = "completed" if result is not False else "failed"
                if manifest.status == "completed":
                    json.dumps(result, allow_nan=False)
                    manifest.result = result
                heartbeat(manifest)
                if manifest.status == "completed":
                    lease.release(run_id)
                return result
        finally:
            if token is not None:
                _current_manifest.reset(token)
            lease.close()

    return wrapped
