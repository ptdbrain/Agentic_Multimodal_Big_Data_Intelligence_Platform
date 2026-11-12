"""Recovery must never mistake a quiet heartbeat for stopped writer evidence."""

import hashlib
import json
import os
from pathlib import Path
import socket
import subprocess
import sys

import pytest

pytestmark = pytest.mark.unit


def stopped_owner():
    process = subprocess.Popen([sys.executable, "-c", "pass"])
    process.wait(timeout=10)
    return {"kind": "local", "hostname": socket.gethostname(), "pids": [process.pid]}


def prepared_run():
    from orchestration.run_manifest import RunManifest, RunLease, checkpoint_stage, reference_output, save_manifest
    from storage.storage_manager import storage

    storage.write_json("bronze/recovery/input.json", {"rows": 2})
    manifest = RunManifest("recover-me", "test", owner=stopped_owner(), stage_order=["bronze", "silver"])
    checkpoint_stage(manifest, "bronze", outputs=[reference_output("bronze/recovery/input.json")])
    path = save_manifest(manifest)
    lease = RunLease("test")
    lease.acquire(manifest.run_id)
    lease.close()
    return path, manifest


def test_checkpoint_preserves_old_fields_and_records_verified_output():
    from orchestration.run_manifest import RunManifest, checkpoint_stage, load_manifest, reference_output
    from storage.storage_manager import storage

    storage.write_json("bronze/input.json", {"rows": 2})
    manifest = RunManifest("checkpoint", "test", stage_order=["bronze", "silver"])
    path = checkpoint_stage(manifest, "bronze", outputs=[reference_output("bronze/input.json")])
    loaded = load_manifest(path)
    assert loaded.checkpoints["bronze"]["state"] == "completed"
    assert (
        loaded.checkpoints["bronze"]["outputs"][0]["sha256"]
        == hashlib.sha256((storage.base_dir / "bronze/input.json").read_bytes()).hexdigest()
    )
    assert loaded.heartbeat_at
    assert loaded.required_datasets == ["products", "reviews"]


def test_plan_is_exclusive_and_pins_outputs(tmp_path):
    from operations.recovery import plan_recovery

    path, _ = prepared_run()
    target = tmp_path / "plan.json"
    plan = plan_recovery(path, target)
    assert plan["next_stage"] == "silver"
    assert plan["operation_id"]
    assert len(plan["plan_hash"]) == 64
    with pytest.raises(FileExistsError):
        plan_recovery(path, target)


def test_live_pid_cannot_be_recovered_even_with_old_heartbeat(tmp_path):
    from operations.recovery import plan_recovery
    from orchestration.run_manifest import save_manifest

    path, manifest = prepared_run()
    manifest.owner["pids"] = [os.getpid()]
    manifest.heartbeat_at = "2000-01-01T00:00:00+00:00"
    save_manifest(manifest)
    with pytest.raises(RuntimeError, match="live|running"):
        plan_recovery(path, tmp_path / "plan.json", action="abort")


def test_legacy_missing_owner_cannot_self_attest_stopped(tmp_path):
    from operations.recovery import plan_recovery
    from orchestration.run_manifest import save_manifest

    path, manifest = prepared_run()
    manifest.owner = {"writers_stopped": True}
    save_manifest(manifest)
    with pytest.raises(ValueError, match="owner|evidence"):
        plan_recovery(path, tmp_path / "plan.json")


def test_modified_output_blocks_plan(tmp_path):
    from operations.recovery import plan_recovery
    from storage.storage_manager import storage

    path, _ = prepared_run()
    storage.write_json("bronze/recovery/input.json", {"rows": 3})
    with pytest.raises(ValueError, match="output|Output"):
        plan_recovery(path, tmp_path / "plan.json")


def test_abort_releases_only_verified_owner_and_is_idempotent(tmp_path):
    from operations.recovery import plan_recovery, apply_recovery
    from orchestration.run_manifest import load_manifest, RunLease

    path, manifest = prepared_run()
    plan_path = tmp_path / "abort.json"
    plan = plan_recovery(path, plan_path, action="abort")
    result = apply_recovery(plan_path, plan["plan_hash"])
    assert result["status"] == "aborted"
    assert load_manifest(path).status == "aborted"
    assert apply_recovery(plan_path, plan["plan_hash"]) == result
    lease = RunLease("test")
    lease.acquire("new-run")
    lease.release("new-run")
    lease.close()


def test_resume_uses_stable_operation_id_and_skips_verified_stage(tmp_path):
    from operations.recovery import plan_recovery, apply_recovery
    from orchestration.run_manifest import load_manifest, reference_output
    from storage.storage_manager import storage

    path, _ = prepared_run()
    plan_path = tmp_path / "resume.json"
    plan = plan_recovery(path, plan_path)

    def executor(manifest, stage, operation_id):
        assert stage == "silver"
        assert operation_id == plan["operation_id"]
        assert manifest.checkpoints["bronze"]["state"] == "completed"
        storage.write_json("silver/recovered.json", {"operation_id": operation_id})
        return [reference_output("silver/recovered.json")]

    result = apply_recovery(plan_path, plan["plan_hash"], executor=executor)
    assert result["status"] == "completed"
    assert load_manifest(path).checkpoints["silver"]["operation_id"] == plan["operation_id"]
    assert apply_recovery(plan_path, plan["plan_hash"], executor=lambda *args: pytest.fail("replayed")) == result


def test_changed_manifest_or_plan_is_refused(tmp_path):
    from operations.recovery import plan_recovery, apply_recovery
    from orchestration.run_manifest import load_manifest, save_manifest

    path, _ = prepared_run()
    plan_path = tmp_path / "plan.json"
    plan = plan_recovery(path, plan_path, action="abort")
    manifest = load_manifest(path)
    manifest.source_type = "crawler"
    save_manifest(manifest)
    with pytest.raises(ValueError, match="manifest"):
        apply_recovery(plan_path, plan["plan_hash"])
    data = json.loads(plan_path.read_text())
    data["action"] = "resume"
    plan_path.write_text(json.dumps(data))
    with pytest.raises(ValueError, match="hash"):
        apply_recovery(plan_path, plan["plan_hash"])


def test_abort_refuses_writer_admission_even_when_owner_stopped(tmp_path):
    from operations.recovery import plan_recovery, apply_recovery
    from operations.backup import writer_admission

    path, _ = prepared_run()
    plan_path = tmp_path / "plan.json"
    plan = plan_recovery(path, plan_path, action="abort")
    with writer_admission("test"):
        with pytest.raises(RuntimeError, match="busy"):
            apply_recovery(plan_path, plan["plan_hash"])


def test_airflow_requires_actual_terminal_backend_adapter(tmp_path):
    from operations.recovery import plan_recovery
    from orchestration.run_manifest import save_manifest

    path, manifest = prepared_run()
    manifest.owner = {"kind": "airflow", "dag_id": "commerce", "dag_run_id": "daily"}
    save_manifest(manifest)
    with pytest.raises(ValueError, match="Airflow|adapter"):
        plan_recovery(path, tmp_path / "no-adapter.json")


def test_canonical_writer_failure_retains_lease_and_manifest():
    from orchestration.run_manifest import canonical_writer, RunLease, load_manifest, manifest_path

    @canonical_writer
    def failing(mode=None, run_id=None):
        raise RuntimeError("interrupted")

    with pytest.raises(RuntimeError, match="interrupted"):
        failing(mode="test", run_id="failed-cli")
    manifest = load_manifest(manifest_path("failed-cli"))
    assert manifest.status == "failed"
    assert manifest.owner["pids"] == [os.getpid()]
    lease = RunLease("test")
    lease.assert_owner("failed-cli")
    lease.release("failed-cli")
    lease.close()


def test_completed_stage_without_output_evidence_refuses_resume(tmp_path):
    from operations.recovery import plan_recovery
    from orchestration.run_manifest import save_manifest

    path, manifest = prepared_run()
    manifest.checkpoints["bronze"]["outputs"] = []
    save_manifest(manifest)
    with pytest.raises(ValueError, match="output manifest"):
        plan_recovery(path, tmp_path / "missing-output.json")


def test_interrupted_resume_reuses_operation_id_after_actual_process_exit(tmp_path):
    from config.settings import settings
    from operations.recovery import plan_recovery, apply_recovery
    from orchestration.run_manifest import reference_output
    from storage.storage_manager import storage

    path, _ = prepared_run()
    plan_path = tmp_path / "retry.json"
    plan = plan_recovery(path, plan_path)
    script = """
import os, sys
from pathlib import Path
from config.settings import settings
settings.set_runtime_mode('test')
settings.database.sqlite_path = Path(sys.argv[1])
from storage.storage_manager import storage
storage.set_mode('test', base_dir=Path(sys.argv[2]))
from operations.recovery import apply_recovery
def interrupted(manifest, stage, operation_id):
    storage.write_json('silver/retry.json', {'operation_id': operation_id})
    os._exit(23)
apply_recovery(sys.argv[3], sys.argv[4], executor=interrupted)
"""
    child = subprocess.run(
        [
            sys.executable,
            "-c",
            script,
            str(settings.database.sqlite_path),
            str(storage.base_dir),
            str(plan_path),
            plan["plan_hash"],
        ],
        capture_output=True,
        text=True,
        timeout=15,
    )
    assert child.returncode == 23, child.stderr

    def retry(manifest, stage, operation_id):
        # An idempotent sink can recognize the interrupted operation's output.
        assert storage.read_json("silver/retry.json") == {"operation_id": operation_id}
        return [reference_output("silver/retry.json")]

    assert apply_recovery(plan_path, plan["plan_hash"], executor=retry)["status"] == "completed"


def test_cli_inspect_and_abort_are_explicit_and_do_not_import_executor(tmp_path, capsys):
    from scripts.recover_pipeline import main
    from operations.recovery import plan_recovery
    from orchestration.run_manifest import load_manifest

    path, _ = prepared_run()
    assert main(["inspect", "--mode", "test", "--manifest", path]) == 0
    assert json.loads(capsys.readouterr().out)["manifest"]["run_id"] == "recover-me"
    plan_path = tmp_path / "cli-abort.json"
    plan = plan_recovery(path, plan_path, action="abort")
    assert main(["abort", "--mode", "test", "--plan", str(plan_path), "--plan-hash", plan["plan_hash"]]) == 0
    assert load_manifest(path).status == "aborted"


def test_completed_cli_retry_verifies_outputs_before_reusing_result():
    from orchestration.run_manifest import canonical_writer, checkpoint_stage, current_manifest, reference_output
    from storage.storage_manager import storage

    @canonical_writer
    def publish(mode=None, run_id=None):
        manifest = current_manifest()
        manifest.stage_order = ["publish"]
        storage.write_json("gold/receipt.json", {"rows": 2})
        checkpoint_stage(manifest, "publish", outputs=[reference_output("gold/receipt.json")])
        return {"published": 2}

    assert publish(mode="test", run_id="committed") == {"published": 2}
    assert publish(mode="test", run_id="committed") == {"published": 2}
    storage.write_json("gold/receipt.json", {"rows": 3})
    with pytest.raises(ValueError, match="output"):
        publish(mode="test", run_id="committed")


def test_opaque_completed_cli_has_no_verified_replay_receipt():
    from orchestration.run_manifest import canonical_writer, completed_result

    @canonical_writer
    def opaque(mode=None, run_id=None):
        return True

    assert opaque(mode="test", run_id="opaque") is True
    with pytest.raises(ValueError, match="checkpoint|resumable"):
        completed_result("opaque", "test")


def test_retry_after_durable_stage_checkpoint_does_not_execute_stage_twice(tmp_path):
    from config.settings import settings
    from operations.recovery import plan_recovery, apply_recovery
    from storage.storage_manager import storage

    path, _ = prepared_run()
    plan_path = tmp_path / "checkpointed-retry.json"
    plan = plan_recovery(path, plan_path)
    script = """
import os, sys
from pathlib import Path
from config.settings import settings
settings.set_runtime_mode('test')
settings.database.sqlite_path = Path(sys.argv[1])
from storage.storage_manager import storage
storage.set_mode('test', base_dir=Path(sys.argv[2]))
from operations.recovery import apply_recovery
from orchestration.run_manifest import checkpoint_stage, reference_output
def interrupted(manifest, stage, operation_id):
    storage.write_json('silver/checkpointed.json', {'operation_id': operation_id})
    checkpoint_stage(manifest, stage, outputs=[reference_output('silver/checkpointed.json')], operation_id=operation_id)
    os._exit(23)
apply_recovery(sys.argv[3], sys.argv[4], executor=interrupted)
"""
    child = subprocess.run(
        [
            sys.executable,
            "-c",
            script,
            str(settings.database.sqlite_path),
            str(storage.base_dir),
            str(plan_path),
            plan["plan_hash"],
        ],
        capture_output=True,
        text=True,
        timeout=15,
    )
    assert child.returncode == 23, child.stderr
    result = apply_recovery(
        plan_path, plan["plan_hash"], executor=lambda *args: pytest.fail("completed stage replayed")
    )
    assert result["status"] == "completed"


def test_retry_finishes_abort_lease_release_after_manifest_commit(tmp_path, monkeypatch):
    import operations.recovery as recovery
    from orchestration.run_manifest import RunLease

    path, _ = prepared_run()
    plan_path = tmp_path / "abort-retry.json"
    plan = recovery.plan_recovery(path, plan_path, action="abort")
    original = recovery._release

    def interrupted(*args):
        raise RuntimeError("lease release interrupted")

    monkeypatch.setattr(recovery, "_release", interrupted)
    with pytest.raises(RuntimeError, match="interrupted"):
        recovery.apply_recovery(plan_path, plan["plan_hash"])
    monkeypatch.setattr(recovery, "_release", original)
    assert recovery.apply_recovery(plan_path, plan["plan_hash"])["status"] == "aborted"
    lease = RunLease("test")
    lease.acquire("after-abort")
    lease.release("after-abort")
    lease.close()


def test_airflow_metadata_backend_identity_cannot_change_between_plan_and_apply(tmp_path):
    from operations.recovery import plan_recovery, apply_recovery
    from orchestration.run_manifest import save_manifest

    path, manifest = prepared_run()
    manifest.owner = {"kind": "airflow", "dag_id": "commerce", "dag_run_id": "daily"}
    save_manifest(manifest)

    class MetadataAdapter:
        # Boundary stand-in for a remote metadata connection; recovery and lease
        # persistence remain real and must reject changing this connection.
        database = "scheduler-a"

        def verify_stopped(self, owner):
            return {
                "kind": "airflow",
                "metadata_backend": {"database": self.database},
                "dag_state": "failed",
                "task_states": ["failed"],
            }

    adapter = MetadataAdapter()
    plan_path = tmp_path / "airflow.json"
    plan = plan_recovery(path, plan_path, action="abort", airflow_adapter=adapter)
    adapter.database = "scheduler-b"
    with pytest.raises(ValueError, match="Airflow.*backend|metadata.*backend"):
        apply_recovery(plan_path, plan["plan_hash"], airflow_adapter=adapter)


def test_recovery_rejects_changed_working_data_even_when_receipt_is_intact(tmp_path):
    from operations.recovery import plan_recovery
    from orchestration.run_manifest import save_manifest, reference_output
    from storage.storage_manager import storage

    path, manifest = prepared_run()
    storage.write_json("silver/working.json", {"rows": [1, 2]})
    manifest.working_outputs = [reference_output("silver/working.json")]
    save_manifest(manifest)
    storage.write_json("silver/working.json", {"rows": [99]})
    with pytest.raises(ValueError, match="Working output"):
        plan_recovery(path, tmp_path / "changed-working.json")


def test_managed_airflow_worker_records_child_pid_and_requires_it_to_stop(tmp_path, monkeypatch):
    from orchestration.run_manifest import worker_owner, save_manifest
    from operations.recovery import plan_recovery

    monkeypatch.setenv(
        "SENTINEL_AIRFLOW_OWNER", json.dumps({"dag_id": "sentinel_source_tiki", "dag_run_id": "scheduled__one"})
    )
    owner = worker_owner()
    assert owner["kind"] == "airflow" and owner["pids"] == [os.getpid()]
    path, manifest = prepared_run()
    manifest.owner = owner
    save_manifest(manifest)

    class TerminalMetadata:
        def verify_stopped(self, owner):
            return {
                "kind": "airflow",
                "metadata_backend": {"database": "scheduler"},
                "dag_state": "failed",
                "task_states": ["failed"],
            }

    with pytest.raises(RuntimeError, match="live"):
        plan_recovery(path, tmp_path / "still-live.json", airflow_adapter=TerminalMetadata())


def test_invalid_worker_owner_cannot_leave_a_lease_without_a_manifest(monkeypatch):
    from orchestration.run_manifest import canonical_writer, RunLease

    monkeypatch.setenv("SENTINEL_AIRFLOW_OWNER", "{}")

    @canonical_writer
    def publish(mode=None, run_id=None):
        return True

    with pytest.raises(ValueError, match="DagRun"):
        publish(mode="test", run_id="invalid-owner")
    lease = RunLease("test")
    try:
        lease.acquire("next-run")
        lease.release("next-run")
    finally:
        lease.close()


def test_explicit_bronze_batches_never_fall_back_to_a_different_runtime_topic(monkeypatch):
    from scripts.run_e2e_pipeline import stage_4_spark_batch_silver_dq
    monkeypatch.setattr('scripts.run_e2e_pipeline.SparkBatchETLJob',lambda:pytest.fail('Unpinned input reached the Spark engine'))
    with pytest.raises(ValueError,match='Pinned Bronze'):
        stage_4_spark_batch_silver_dq(batches_by_topic={'previous.products':['bronze/old.json']},include_prices=False)
