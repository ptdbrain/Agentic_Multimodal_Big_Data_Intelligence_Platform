import pytest

pytestmark = [pytest.mark.unit, pytest.mark.spark]


def test_completed_cli_retry_uses_verified_receipt_without_reprocessing(monkeypatch, spark_session):
    import scripts.run_e2e_pipeline as pipeline
    from orchestration.run_manifest import load_manifest, manifest_path

    calls = []
    original = pipeline.stage_1_ingest_sources

    def observe(*args, **kwargs):
        calls.append("read")
        return original(*args, **kwargs)

    monkeypatch.setattr(pipeline, "stage_1_ingest_sources", observe)
    assert pipeline.run_end_to_end(mode="test", run_id="stable-source-run")
    manifest = load_manifest(manifest_path("stable-source-run"))
    assert len(manifest.checkpoints) == 7
    assert pipeline.run_end_to_end(mode="test", run_id="stable-source-run")
    assert calls == ["read"]


def test_failed_cli_is_resumed_by_real_stage_executor_in_fresh_processes(tmp_path):
    """Local Spark/SQLite drill, never evidence of public collection or Airflow."""
    import json, os, subprocess, sys
    from config.settings import settings
    from storage.storage_manager import storage
    from orchestration.run_manifest import manifest_path, load_manifest
    from operations.recovery import plan_recovery

    env = dict(
        os.environ,
        RUNTIME_MODE="test",
        SQLITE_DB_PATH=str(settings.database.sqlite_path),
        LOCAL_DATA_DIR=str(storage.base_dir),
        SPARK_MASTER="local[2]",
        SPARK_LOCAL_IP="127.0.0.1",
    )
    script = """
from scripts import run_e2e_pipeline as pipeline
def interrupted(*args,**kwargs):raise RuntimeError('warehouse intentionally stopped')
pipeline.stage_6_warehouse_and_dashboard=interrupted
try:pipeline.run_end_to_end(mode='test',run_id='interrupted-real-stages')
except RuntimeError as error:
    if 'intentionally stopped' in str(error):raise SystemExit(23)
    raise
"""
    child = subprocess.run([sys.executable, "-c", script], env=env, capture_output=True, text=True, timeout=180)
    assert child.returncode == 23, child.stdout[-3000:] + child.stderr[-3000:]
    path = manifest_path("interrupted-real-stages")
    manifest = load_manifest(path)
    assert manifest.status == "failed" and len(manifest.checkpoints) == 5
    assert manifest.working_outputs
    for index, stage in enumerate(["warehouse_upsert", "finish"]):
        plan_path = tmp_path / f"resume-{index}.json"
        plan = plan_recovery(path, plan_path)
        assert plan["next_stage"] == stage
        resumed = subprocess.run(
            [
                sys.executable,
                "scripts/recover_pipeline.py",
                "resume",
                "--mode",
                "test",
                "--plan",
                str(plan_path),
                "--plan-hash",
                plan["plan_hash"],
            ],
            env=env,
            capture_output=True,
            text=True,
            timeout=90,
        )
        assert resumed.returncode == 0, resumed.stdout[-3000:] + resumed.stderr[-3000:]
    manifest = load_manifest(path)
    assert manifest.status == "completed" and len(manifest.checkpoints) == 7
    from orchestration.run_manifest import completed_result, RunLease

    assert completed_result("interrupted-real-stages", "test") is True
    lease = RunLease("test")
    try:
        lease.acquire("next-independent-run")
        lease.release("next-independent-run")
    finally:
        lease.close()


def test_empty_observed_review_delta_replays_context_without_inventing_new_reviews(tmp_path, spark_session):
    """Controlled HTTP fixture plus real Spark; not public-source collection evidence."""
    import json
    from tests.test_tiki_daily_capture import FakeTiki, _capture, _load, DAY, NEXT_DAY
    from orchestration.source_jobs import commerce_input
    from scripts.run_e2e_pipeline import run_end_to_end
    from storage.storage_manager import storage
    from ingestion.crawler import tiki as tiki_module
    from unittest.mock import patch

    def run(datasets, run_id):
        paths = {}
        for name, rows in datasets.items():
            path = tmp_path / f"{run_id}-{name}.json"
            path.write_text(json.dumps(rows))
            paths[name + "_path"] = str(path)
        assert run_end_to_end(mode="test", run_id=run_id, **paths)

    root = tmp_path / "capture"
    with patch.object(tiki_module.time, "sleep", lambda seconds: None):
        first = _capture(root, FakeTiki({101: "Phone 101"}), day=DAY, incremental_reviews=True)
        initial = _load(first)
    run(initial, "initial-observed-fixture")
    original_reviews = storage.read_silver_parquet("reviews")
    original_clocks = dict(zip(original_reviews["review_id"], original_reviews["review_date"].astype(str)))
    with patch.object(tiki_module.time, "sleep", lambda seconds: None):
        second = _capture(root, FakeTiki({101: "Phone 101"}), day=NEXT_DAY, incremental_reviews=True)
    delta = _load(second)
    assert delta["reviews"] == []
    prepared, replayed = commerce_input(
        {"source_group": "marketplace", "parameters": {"allow_review_context_replay": True}}, {"datasets": delta}
    )
    assert replayed == 2
    run(prepared, "empty-new-review-delta")
    reviews = storage.read_silver_parquet("reviews")
    assert len(reviews) == 2 and reviews["review_id"].is_unique
    assert dict(zip(reviews["review_id"], reviews["review_date"].astype(str))) == original_clocks
    assert all(row["acquisition_method"] == "snapshot_replay" for row in prepared["reviews"])
