import json
from datetime import date
import pytest

pytestmark = pytest.mark.unit


def config(tmp_path):
    return {
        "version": 1,
        "root": str(tmp_path / "jobs"),
        "jobs": [
            {
                "id": "tiki",
                "adapter": "tiki",
                "source_group": "marketplace",
                "namespace": "observed-commerce",
                "schedule": "0 9 * * *",
                "owner": "airflow",
                "minimum_records": 1,
            },
            {
                "id": "web",
                "adapter": "web",
                "source_group": "web_content",
                "namespace": "observed-web",
                "schedule": "0 10 * * *",
                "owner": "airflow",
                "minimum_records": 1,
            },
            {
                "id": "simulation",
                "adapter": "simulation",
                "source_group": "simulation",
                "namespace": "generated",
                "schedule": "0 11 * * *",
                "owner": "airflow",
                "minimum_records": 1,
            },
        ],
    }


def test_source_configuration_rejects_mixed_namespace_and_wrong_provenance(tmp_path):
    from orchestration.source_jobs import validate_config

    value = config(tmp_path)
    assert len(validate_config(value)["jobs"]) == 3
    value["jobs"][2]["namespace"] = "observed-commerce"
    with pytest.raises(ValueError):
        validate_config(value)
    value = config(tmp_path)
    value["jobs"][0]["source_group"] = "simulation"
    with pytest.raises(ValueError):
        validate_config(value)


def test_processing_retry_reuses_capture_and_never_recrawls(tmp_path):
    from orchestration.source_jobs import execute_job

    calls = []

    def capture(job, path):
        calls.append("capture")
        return {"status": "complete", "records": [{"id": "one"}]}

    def broken(job, records, run_id):
        calls.append("process")
        raise RuntimeError("warehouse down")

    with pytest.raises(RuntimeError):
        execute_job(config(tmp_path), "tiki", "run1", capture=capture, process=broken)
    result = execute_job(
        config(tmp_path), "tiki", "run1", capture=capture, process=lambda *args: calls.append("resume")
    )
    assert calls == ["capture", "process", "resume"]
    assert result["state"] == "complete"
    execute_job(config(tmp_path), "tiki", "run1", capture=capture, process=broken)
    assert calls == ["capture", "process", "resume"]


def test_capture_tampering_and_concurrent_scheduler_owner_are_rejected(tmp_path):
    from orchestration.source_jobs import execute_job

    value = config(tmp_path)
    with pytest.raises(ValueError):
        execute_job(value, "tiki", "r", scheduler="systemd")
    execute_job(
        value, "tiki", "r", action="capture", capture=lambda *args: {"status": "partial", "records": [{"id": "one"}]}
    )
    path = tmp_path / "jobs/tiki/r/capture.json"
    path.write_text("{}")
    with pytest.raises(ValueError, match="hash"):
        execute_job(value, "tiki", "r", process=lambda *args: None)


def test_false_pipeline_result_is_failed_and_retry_keeps_original_capture(tmp_path):
    from orchestration.source_jobs import execute_job

    value = config(tmp_path)
    calls = []

    def capture(*args):
        calls.append("capture")
        return {"status": "complete", "records": [{"id": "one"}]}

    with pytest.raises(RuntimeError, match="Processing did not complete"):
        execute_job(value, "tiki", "bad-process", capture=capture, process=lambda *args: {"completed": False})
    manifest = json.loads((tmp_path / "jobs/tiki/bad-process/manifest.json").read_text())
    assert manifest["state"] == "failed" and manifest.get("processing_state") != "complete"
    result = execute_job(value, "tiki", "bad-process", capture=capture, process=lambda *args: {"completed": True})
    assert result["state"] == "complete" and calls == ["capture"]


def test_processed_partial_capture_remains_partial(tmp_path):
    from orchestration.source_jobs import execute_job

    result = execute_job(
        config(tmp_path),
        "web",
        "partial",
        capture=lambda *args: {"status": "partial", "records": [{"id": "one"}]},
        process=lambda *args: {"completed": True},
    )
    assert result["state"] == "partial" and result["processing_state"] == "complete"


def test_target_binding_rejects_shared_physical_lake_or_warehouse(tmp_path):
    from orchestration.source_jobs import bind_target

    value = config(tmp_path)
    real = {
        "lake_endpoint": "127.0.0.1:9000",
        "bucket": "observed",
        "db_host": "localhost",
        "db_port": 5432,
        "db_name": "sentinel",
        "db_schema": "observed",
    }
    bind_target(value, value["jobs"][0], real)
    for change in ({"db_schema": "generated"}, {"bucket": "generated"}):
        with pytest.raises(ValueError, match="Physical target"):
            bind_target(value, value["jobs"][2], dict(real, **change))
    generated = dict(real, bucket="generated", db_schema="generated")
    bind_target(value, value["jobs"][2], generated)
    with pytest.raises(ValueError, match="changed"):
        bind_target(value, value["jobs"][2], dict(generated, bucket="other"))


def test_source_failure_and_partial_are_visible_in_filesystem_metrics(tmp_path):
    from orchestration.source_jobs import execute_job
    from monitoring.source_metrics import source_metrics

    value = config(tmp_path)
    untouched = source_metrics(value)
    assert 'sentinel_source_last_capture_timestamp{source_job="tiki",source="marketplace"} NaN' in untouched
    with pytest.raises(RuntimeError):
        execute_job(value, "tiki", "failed", capture=lambda *args: (_ for _ in ()).throw(RuntimeError("source down")))
    failed = source_metrics(value)
    assert 'sentinel_source_state{source_job="tiki",source="marketplace",state="failed"} 1' in failed
    execute_job(
        value,
        "web",
        "partial",
        capture=lambda *args: {"status": "partial", "records": [{"id": "a"}], "finished_at": "2026-11-12T00:00:00Z"},
        process=lambda *args: {"completed": True},
    )
    measured = source_metrics(value)
    assert 'sentinel_source_state{source_job="web",source="web_content",state="partial"} 1' in measured
    assert 'sentinel_source_input_records{source_job="web",source="web_content"} 1' in measured
    assert 'sentinel_source_last_process_timestamp{source_job="web",source="web_content"} NaN' not in measured
    assert 'sentinel_source_last_capture_timestamp{source_job="web",source="web_content"} NaN' not in measured


def test_source_task_uses_configured_processor_python_and_pins_scheduler_run(monkeypatch, tmp_path):
    from orchestration.dags.sentinel_sources import run_source_task

    calls = []
    monkeypatch.setenv("SOURCE_JOB_PYTHON", "/private/processor/bin/python")
    monkeypatch.setattr(
        "orchestration.dags.sentinel_sources.subprocess.run", lambda command, **kwargs: calls.append(command)
    )
    for action in ("capture", "process"):
        run_source_task(tmp_path / "jobs.yml", "tiki", action, run_id="scheduled__observed-clock")
    assert [command[0] for command in calls] == ["/private/processor/bin/python"] * 2
    assert all(command[command.index("--run-id") + 1] == "scheduled__observed-clock" for command in calls)


def test_source_timezone_is_explicit_and_invalid_zone_is_refused(tmp_path):
    from orchestration.source_jobs import validate_config

    value = config(tmp_path)
    assert validate_config(value)["jobs"][0]["timezone"] == "Asia/Ho_Chi_Minh"
    value["jobs"][0]["timezone"] = "Invented/Nowhere"
    with pytest.raises(ValueError, match="timezone"):
        validate_config(value)


def test_empty_review_delta_reuses_only_explicit_observed_context_with_original_clocks():
    import pandas as pd
    from storage.storage_manager import storage
    from orchestration.source_jobs import commerce_input

    original = {
        "review_id": "observed-r1",
        "product_id": "p1",
        "rating": 5,
        "review_text": "Already observed",
        "review_date": "2026-11-01T00:00:00Z",
        "collected_at": "2026-11-02T00:00:00Z",
        "source_group": "marketplace",
        "acquisition_method": "live",
        "collection_run_id": "old-observation",
    }
    storage.write_silver_parquet("reviews", pd.DataFrame([original]))
    captured = {"datasets": {"products": [{"product_id": "p1"}], "reviews": [], "prices": []}}
    job = {"source_group": "marketplace", "parameters": {"allow_review_context_replay": True}}
    datasets, replayed = commerce_input(job, captured)
    assert replayed == 1 and captured["datasets"]["reviews"] == []
    replay = datasets["reviews"][0]
    assert replay["acquisition_method"] == "snapshot_replay" and replay["original_acquisition_method"] == "live"
    for key in ("review_id", "review_date", "collected_at", "collection_run_id"):
        assert replay[key] == original[key]
    with pytest.raises(ValueError, match="Explicit review context policy"):
        commerce_input(dict(job, parameters={}), captured)
    with pytest.raises(ValueError, match="source group"):
        commerce_input(dict(job, source_group="simulation"), captured)


def test_handoff_does_not_relabel_old_collection_time_as_fresh_capture(tmp_path):
    from orchestration.source_jobs import execute_job

    result = execute_job(
        config(tmp_path),
        "web",
        "old-input",
        action="capture",
        capture=lambda *args: {"status": "complete", "records": [{"id": "one"}], "finished_at": "2026-10-01T00:00:00Z"},
    )
    assert result["captured_at"] == "2026-10-01T00:00:00Z"
    assert result["handed_off_at"] != result["captured_at"]


def test_simulation_sizes_and_anomaly_injection_are_explicit(tmp_path):
    from orchestration.source_jobs import capture_source

    job = {
        "adapter": "simulation",
        "parameters": {"products": 3, "base_reviews": 7, "inject_anomalies": False, "burst_count": 4},
    }
    plain = capture_source(job, tmp_path)
    assert len(plain["datasets"]["products"]) == 3 and len(plain["datasets"]["reviews"]) == 7
    job["parameters"]["inject_anomalies"] = True
    anomalous = capture_source(job, tmp_path)
    assert len(anomalous["datasets"]["reviews"]) == 11
    assert anomalous["generator_parameters"]["burst_count"] == 4
    job["parameters"]["products"] = 1000
    with pytest.raises(ValueError, match="catalogue"):
        capture_source(job, tmp_path)


def test_managed_tiki_reuses_the_existing_frozen_capture_root(tmp_path, monkeypatch):
    from tests.test_tiki_daily_capture import FakeTiki, _capture, DAY
    from ingestion.crawler import tiki as tiki_module
    from orchestration.source_jobs import capture_source

    monkeypatch.setattr(tiki_module.time, "sleep", lambda seconds: None)
    root = tmp_path / "old-timer-captures"
    _capture(root, FakeTiki({101: "Phone 101", 102: "Phone 102"}), day=DAY)
    changed = FakeTiki({101: "Phone 101", 102: "Phone 102", 103: "New listing"})
    monkeypatch.setattr("scripts.collect_tiki_daily.tiki_crawler_factory", lambda delay: changed.factory)
    value = capture_source(
        {
            "adapter": "tiki",
            "minimum_records": 1,
            "parameters": {
                "capture_root": str(root),
                "max_products": 3,
                "reviews_per_product": 1,
                "review_max_pages": 2,
            },
        },
        tmp_path / "new-handoff" / "run",
    )
    assert len(value["datasets"]["products"]) == 2
    assert changed.search_calls() == []


def test_commerce_consumer_uses_configured_namespace_group(monkeypatch):
    from config.settings import settings
    from kafka.consumers.raw_consumer import RawConsumer

    monkeypatch.setattr(settings.kafka, "group_id", "isolated-observed-commerce")
    assert RawConsumer().group_id == "isolated-observed-commerce"
    assert RawConsumer(group_id="explicit-replay").group_id == "explicit-replay"


@pytest.mark.parametrize("interrupted_file", ["capture-receipt.json", "capture.json"])
def test_durable_capture_receipt_recovers_handoff_crash_without_recrawling(tmp_path, monkeypatch, interrupted_file):
    from orchestration import source_jobs as jobs

    calls = []
    original = jobs.write_atomic

    def capture(*args):
        calls.append("observed-once")
        return {"status": "partial", "finished_at": "2026-11-12T12:00:00Z", "records": [{"id": "durable"}]}

    def interrupted(path, value):
        original(path, value)
        if path.name == interrupted_file:
            raise SystemExit("simulated abrupt process exit after durable rename")

    monkeypatch.setattr(jobs, "write_atomic", interrupted)
    with pytest.raises(SystemExit):
        jobs.execute_job(config(tmp_path), "web", "crashed-handoff", capture=capture, action="capture")
    monkeypatch.setattr(jobs, "write_atomic", original)
    result = jobs.execute_job(
        config(tmp_path), "web", "crashed-handoff", capture=capture, process=lambda *args: {"completed": True}
    )
    assert calls == ["observed-once"] and result["processing_state"] == "complete"
    assert result["captured_at"] == "2026-11-12T12:00:00Z" and result["state"] == "partial"


@pytest.mark.parametrize("tamper", ["identity", "contents", "published_payload"])
def test_orphan_handoff_refuses_a_changed_receipt_or_payload(tmp_path, monkeypatch, tamper):
    from orchestration import source_jobs as jobs

    original = jobs.write_atomic

    def interrupted(path, value):
        original(path, value)
        if path.name == "capture.json":
            raise SystemExit("interrupt before manifest link")

    monkeypatch.setattr(jobs, "write_atomic", interrupted)
    with pytest.raises(SystemExit):
        jobs.execute_job(
            config(tmp_path),
            "web",
            "tampered-handoff",
            action="capture",
            capture=lambda *args: {"status": "complete", "records": [{"id": "original"}]},
        )
    monkeypatch.setattr(jobs, "write_atomic", original)
    folder = tmp_path / "jobs/web/tampered-handoff"
    path = folder / "capture-receipt.json"
    receipt = json.loads(path.read_text())
    if tamper == "identity":
        receipt["run_id"] = "other-run"
    elif tamper == "contents":
        receipt["capture"]["records"][0]["id"] = "changed"
    else:
        (folder / "capture.json").write_text('{"different":"payload"}')
    path.write_text(json.dumps(receipt))
    with pytest.raises(ValueError, match="[Cc]apture.*(identity|hash)"):
        jobs.execute_job(
            config(tmp_path),
            "web",
            "tampered-handoff",
            action="process",
            capture=lambda *args: pytest.fail("must not recollect"),
        )
