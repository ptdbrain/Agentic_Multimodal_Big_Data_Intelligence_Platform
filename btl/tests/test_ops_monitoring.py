"""Operational status is durable and missing measurements stay unknown."""

import pytest

pytestmark = pytest.mark.unit


def test_status_survives_reopen_and_has_no_run_id_metric_label():
    from operations.status import OperationStatus

    status = OperationStatus(mode="test")
    status.start("unique-private-run", "commerce", "marketplace")
    status.finish("unique-private-run", 10, 8, 2, data_as_of=100)
    status.close()
    reopened = OperationStatus(mode="test")
    metrics = reopened.metrics()
    reopened.close()
    assert 'sentinel_ops_received{pipeline="commerce",source="marketplace"} 10' in metrics
    assert "unique-private-run" not in metrics
    assert 'sentinel_ops_data_as_of_timestamp{pipeline="commerce",source="marketplace"} 100' in metrics


def test_invalid_status_and_accounting_fail_without_partial_commit():
    from operations.status import OperationStatus

    with OperationStatus(mode="test") as status:
        status.start("run", "articles", "web_content")
        with pytest.raises(ValueError):
            status.finish("run", 10, 9, 3)
        with pytest.raises(ValueError):
            status.finish("missing", 1, 1, 0)
        status.fail("run")
        with pytest.raises(ValueError):
            status.finish("run", 10, 8, 2)
        assert 'state="failed"} 1' in status.metrics()


def test_unknown_gauges_are_nan_and_labels_are_bounded():
    from operations.status import OperationStatus

    with OperationStatus(mode="test") as status:
        assert (
            'sentinel_ops_backup_last_complete_timestamp{pipeline="platform",source="system"} NaN' in status.metrics()
        )
        with pytest.raises(ValueError):
            status.start("run", "http://arbitrary-url", "marketplace")
        with pytest.raises(ValueError):
            status.start("run", "commerce", "some-user@example.org")
        with pytest.raises(ValueError):
            status.set_gauge("arbitrary", 1)


def test_new_run_does_not_erase_previous_success_and_failed_count():
    from operations.status import OperationStatus

    with OperationStatus(mode="test") as status:
        status.start("first", "commerce", "marketplace", now=100)
        status.finish("first", 2, 2, 0, now=200)
        status.start("second", "commerce", "marketplace", now=300)
        status.fail("second", now=400)
        actual = status.metrics()
        assert 'sentinel_ops_last_success_timestamp{pipeline="commerce",source="marketplace"} 200' in actual
        assert 'sentinel_ops_failure_total{pipeline="commerce",source="marketplace"} 1' in actual


def test_chaos_refuses_unowned_or_non_resilience_projects():
    from scripts.verify_operations_stack import assert_isolated_container

    with pytest.raises(ValueError):
        assert_isolated_container({"Config": {"Labels": {"com.docker.compose.project": "sentinel"}}})
    assert_isolated_container(
        {"Config": {"Labels": {"com.docker.compose.project": "sentinel-resilience-drill", "sentinel.drill": "true"}}}
    )


def test_loopback_webhook_receiver_records_firing_and_resolved(tmp_path):
    import json
    import threading
    from urllib.request import Request, urlopen
    from scripts.verify_operations_stack import webhook_server

    output = tmp_path / "alerts.jsonl"
    with webhook_server(output, port=0) as server:
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            for state in ["firing", "resolved"]:
                payload = {
                    "status": state,
                    "alerts": [{"labels": {"alertname": "SentinelPipelineFailed"}, "status": state}],
                }
                request = Request(
                    f"http://127.0.0.1:{server.server_port}/alerts",
                    data=json.dumps(payload).encode(),
                    headers={"Content-Type": "application/json"},
                )
                with urlopen(request, timeout=5) as response:
                    assert response.status == 200
        finally:
            server.shutdown()
            thread.join(timeout=5)
    assert [json.loads(line)["notification"]["status"] for line in output.read_text().splitlines()] == [
        "firing",
        "resolved",
    ]


def test_compose_profiles_and_rules_have_bounded_resources_and_networks():
    from pathlib import Path
    import yaml

    root = Path(__file__).resolve().parents[1]
    resilience = yaml.safe_load((root / "docker-compose.resilience.yml").read_text())
    services = resilience["services"]
    assert len([key for key in services if key.startswith("kafka")]) == 3
    assert sum(int(value["mem_limit"][:-1]) for value in services.values()) <= 18
    for service in services.values():
        assert service["labels"]["sentinel.drill"] == "true"
    assert services["kafka1"]["environment"]["KAFKA_MIN_INSYNC_REPLICAS"] == "2"
    operations = yaml.safe_load((root / "docker-compose.operations.yml").read_text())
    assert all(service["network_mode"] == "host" for service in operations["services"].values())
    rules = yaml.safe_load((root / "monitoring/alerts.yml").read_text())
    assert {"SentinelPipelineFailed", "SentinelBackupStale", "SentinelKafkaBacklog"}.issubset(
        {rule["alert"] for rule in rules["groups"][0]["rules"]}
    )


def test_expected_pipeline_is_visible_before_first_execution(monkeypatch):
    from operations.status import OperationStatus

    monkeypatch.setenv("OPS_EXPECTED_PIPELINES", "articles:web_content")
    with OperationStatus(mode="test") as status:
        metrics = status.metrics()
        assert 'sentinel_ops_expected_since_timestamp{pipeline="articles",source="web_content"}' in metrics
        assert 'sentinel_ops_last_success_timestamp{pipeline="articles",source="web_content"} NaN' in metrics
        assert 'sentinel_ops_last_started_timestamp{pipeline="articles",source="web_content"} NaN' in metrics


def test_stream_progress_waits_for_all_sinks_and_does_not_fabricate_data_on_idle_batches():
    from operations.status import OperationStatus

    with OperationStatus(mode="test") as status:
        first = {"batchId": 0, "numInputRows": 3, "timestamp": "2026-11-12T00:00:00Z"}
        status.stream_progress("checkpoint-a", "window", first, required_sinks=["window", "quarantine"])
        assert 'sentinel_ops_stream_progress_timestamp{pipeline="streaming",source="system"}' not in status.metrics()
        status.stream_progress("checkpoint-a", "quarantine", first, required_sinks=["window", "quarantine"])
        actual = status.metrics()
        assert 'sentinel_ops_stream_progress_timestamp{pipeline="streaming",source="system"} 1794441600' in actual
        idle = {"batchId": 1, "numInputRows": 0, "timestamp": "2026-11-12T00:01:00Z"}
        for sink in ["window", "quarantine"]:
            status.stream_progress("checkpoint-a", sink, idle, required_sinks=["window", "quarantine"])
        metrics = status.metrics()
        assert 'sentinel_ops_stream_progress_timestamp{pipeline="streaming",source="system"} 1794441600' in metrics
        assert 'sentinel_ops_stream_heartbeat_timestamp{pipeline="streaming",source="system"} 1794441660' in metrics
        assert 'sentinel_ops_stream_input_rows{pipeline="streaming",source="system"} 0' in metrics


def test_stream_progress_ignores_replayed_older_batch_and_persists_across_restart():
    from operations.status import OperationStatus

    with OperationStatus(mode="test") as status:
        point = {"batchId": 5, "numInputRows": 2, "timestamp": "2026-11-12T00:05:00Z"}
        for sink in ["window", "quarantine"]:
            status.stream_progress("cp", "%s" % sink, point)
    with OperationStatus(mode="test") as status:
        before = status.metrics()
        status.stream_progress("cp", "window", dict(point, batchId=4, timestamp="2026-11-12T00:04:00Z"))
        assert status.metrics() == before
        with pytest.raises(ValueError):
            status.stream_progress("cp", "window", dict(point, numInputRows=-1))


def test_stream_job_polls_real_progress_contract_into_durable_sql():
    from spark.streaming.spark_streaming_job import SparkStreamingJob
    from operations.status import OperationStatus

    class ProgressReport:
        # Stand-in only for Spark's external query API; real SQL and job method.
        lastProgress = {"batchId": 1, "numInputRows": 7, "timestamp": "2026-11-12T00:00:00Z"}

    job = SparkStreamingJob(checkpoint_dir="telemetry-test")
    job._active_query = ProgressReport()
    job._quarantine_query = ProgressReport()
    job.observe_progress()
    job.observe_progress()
    with OperationStatus(mode="test") as status:
        metrics = status.metrics()
        assert 'sentinel_ops_stream_input_rows{pipeline="streaming",source="system"} 7' in metrics
        assert status._execute("SELECT COUNT(*) FROM operations_stream_progress").fetchone()[0] == 2


def test_stream_error_is_cleared_only_after_all_sinks_report_committed_progress():
    from operations.status import OperationStatus

    with OperationStatus(mode="test") as status:
        status.stream_error(now=1794441599)
        assert 'sentinel_ops_stream_errors{pipeline="streaming",source="system"} 1' in status.metrics()
        point = {"batchId": 0, "numInputRows": 1, "timestamp": "2026-11-12T00:00:00Z"}
        status.stream_progress("recovered", "window", point)
        assert 'sentinel_ops_stream_errors{pipeline="streaming",source="system"} 1' in status.metrics()
        status.stream_progress("recovered", "quarantine", point)
        assert 'sentinel_ops_stream_errors{pipeline="streaming",source="system"} 0' in status.metrics()


def test_old_sink_progress_cannot_clear_a_new_stream_failure():
    from operations.status import OperationStatus

    with OperationStatus(mode="test") as status:
        baseline = {"batchId": 0, "numInputRows": 1, "timestamp": "2026-11-12T00:00:00Z"}
        for sink in ("window", "quarantine"):
            status.stream_progress("cp", sink, baseline)
        status.stream_error(now=1794441660)
        recovered = dict(baseline, batchId=1, timestamp="2026-11-12T00:02:00Z")
        status.stream_progress("cp", "window", recovered)
        assert 'sentinel_ops_stream_errors{pipeline="streaming",source="system"} 1' in status.metrics()
        status.stream_progress("cp", "quarantine", recovered)
        assert 'sentinel_ops_stream_errors{pipeline="streaming",source="system"} 0' in status.metrics()


def test_docker_spark_fault_requires_tasks_on_both_owned_workers():
    from scripts.verify_operations_stack import verify_active_spark_workers

    state = {
        "workers": [
            {"id": "w1", "host": "172.20.0.2", "state": "ALIVE", "coresused": 2},
            {"id": "w2", "host": "172.20.0.3", "state": "ALIVE", "coresused": 2},
        ]
    }
    assert len(verify_active_spark_workers(state, {"172.20.0.2", "172.20.0.3"})) == 2
    state["workers"][1]["coresused"] = 0
    with pytest.raises(ValueError):
        verify_active_spark_workers(state, {"172.20.0.2", "172.20.0.3"})
    state["workers"][1].update(coresused=2, host="172.20.0.99")
    with pytest.raises(ValueError):
        verify_active_spark_workers(state, {"172.20.0.2", "172.20.0.3"})
