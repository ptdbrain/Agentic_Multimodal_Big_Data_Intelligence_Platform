"""Actual loopback webhook idempotency across receiver restart."""

import json, threading
import requests, pytest

pytestmark = pytest.mark.unit


def test_business_notification_retry_is_durable_and_changed_payload_is_refused(tmp_path):
    from scripts.verify_operations_stack import webhook_server

    path = tmp_path / "notifications.jsonl"
    payload = {"status": "firing", "alerts": [{"status": "firing", "labels": {"alertname": "Explicit fixture"}}]}
    headers = {"Idempotency-Key": "a" * 64}
    for _ in range(2):
        server = webhook_server(path, port=0)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            url = f"http://127.0.0.1:{server.server_port}/alerts"
            for i in range(2):
                assert requests.post(url, json=payload, headers=headers, timeout=5).status_code == 200
            assert (
                requests.post(url, json={**payload, "status": "resolved"}, headers=headers, timeout=5).status_code
                == 400
            )
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=5)
    records = [json.loads(line) for line in path.read_text().splitlines()]
    assert len(records) == 1


def test_outbox_worker_delivers_actual_loopback_firing_and_resolved_once(tmp_path):
    from serving.registry import DatasetRegistry
    from serving.store import connect_dataset, ServingStore
    from serving.notifications import deliver_notifications
    from scripts.verify_operations_stack import webhook_server
    from tests.test_serving_registry import registry_file

    binding = DatasetRegistry.load(registry_file(tmp_path)).datasets["tiki"]
    conn = connect_dataset(binding)
    store = ServingStore(conn, binding.id, binding.source_group)
    event = {
        "event_id": "b" * 64,
        "product_id": "p",
        "window_start": "2026-11-13T00:00:00Z",
        "window_end": "2026-11-13T00:01:00Z",
        "rule": "traffic",
        "event_type": "REVIEW_BURST",
        "state": "firing",
        "score": 5,
        "evidence": {"quality_status": "valid"},
    }
    with conn:
        store.initialize()
        store.stage_evaluations("g", [event])
        store.stage_evaluations("g", [{**event, "state": "clear"}])
    path = tmp_path / "actual-http-receipts.jsonl"
    server = webhook_server(path, port=0)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        url = f"http://127.0.0.1:{server.server_port}/alerts"
        assert deliver_notifications(binding, url=url) == 2
        assert deliver_notifications(binding, url=url) == 0
        rows = [json.loads(line) for line in path.read_text().splitlines()]
        assert [row["notification"]["status"] for row in rows] == ["firing", "resolved"]
        assert (
            conn.execute("SELECT COUNT(*) FROM alert_notifications WHERE delivered_at IS NOT NULL").fetchone()[0] == 2
        )
    finally:
        conn.close()
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)
