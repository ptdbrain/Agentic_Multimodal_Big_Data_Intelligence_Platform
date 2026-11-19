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


def test_killed_worker_after_receiver_fsync_recovers_without_duplicate_receipt(tmp_path):
    """Kill a real child after durable HTTP receipt but before response/SQL acknowledgement."""
    import subprocess, sys
    from pathlib import Path
    from serving.registry import DatasetRegistry
    from serving.store import connect_dataset, ServingStore
    from serving.notifications import deliver_notifications
    from scripts.verify_operations_stack import webhook_server
    from tests.test_serving_registry import registry_file

    config = registry_file(tmp_path)
    binding = DatasetRegistry.load(config).datasets["tiki"]
    connection = connect_dataset(binding)
    store = ServingStore(connection, binding.id, binding.source_group)
    with connection:
        store.initialize()
        store.stage_evaluations(
            "g",
            [
                {
                    "event_id": "c" * 64,
                    "product_id": "p",
                    "rule": "traffic",
                    "event_type": "REVIEW_BURST",
                    "state": "firing",
                    "score": 5,
                    "window_start": "2026-11-13T00:00:00Z",
                    "window_end": "2026-11-13T00:01:00Z",
                    "evidence": {"quality_status": "valid"},
                }
            ],
        )
    receipt = tmp_path / "crash-receipt.jsonl"
    server = webhook_server(receipt, port=0)
    accepted, release = threading.Event(), threading.Event()
    original = server.RequestHandlerClass.send_response

    def hold_first_response(self, code, message=None):
        if code == 200 and not accepted.is_set():
            accepted.set()
            release.wait(timeout=10)
        return original(self, code, message)

    server.RequestHandlerClass.send_response = hold_first_response
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    url = f"http://127.0.0.1:{server.server_port}/alerts"
    command = [
        sys.executable,
        "-c",
        "import sys;from serving.registry import DatasetRegistry;from serving.notifications import deliver_notifications;"
        'deliver_notifications(DatasetRegistry.load(sys.argv[1]).datasets["tiki"],url=sys.argv[2])',
        str(config),
        url,
    ]
    child = subprocess.Popen(
        command, cwd=Path(__file__).resolve().parents[1], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
    )
    try:
        assert accepted.wait(timeout=8), "Receiver did not reach durable write before crash"
        assert len(receipt.read_text().splitlines()) == 1
        child.kill()
        child.wait(timeout=5)
        release.set()
        assert connection.execute("SELECT delivered_at FROM alert_notifications").fetchone()[0] is None
        assert deliver_notifications(binding, url=url) == 1
        assert deliver_notifications(binding, url=url) == 0
        assert len(receipt.read_text().splitlines()) == 1
        assert connection.execute("SELECT delivered_at FROM alert_notifications").fetchone()[0] is not None
    finally:
        if child.poll() is None:
            child.kill()
            child.wait(timeout=5)
        release.set()
        connection.close()
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


@pytest.mark.parametrize('tail', [b'{"interrupted', b'{"valid_json":"missing delimiter"}', b'\xf0\x9f'])
def test_receiver_recovers_only_uncommitted_final_append(tmp_path, tail):
    from scripts.verify_operations_stack import webhook_server

    path = tmp_path/'receipts.jsonl'
    payload = {'status':'firing', 'alerts':[]}
    server = webhook_server(path, port=0)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    url = f'http://127.0.0.1:{server.server_port}/alerts'
    try:
        assert requests.post(url, json=payload, headers={'Idempotency-Key':'first'}, timeout=5).status_code == 200
        original = path.read_bytes()
        with path.open('ab') as output:
            output.write(tail)
        assert requests.post(url, json=payload, headers={'Idempotency-Key':'second'}, timeout=5).status_code == 200
        assert requests.post(url, json=payload, headers={'Idempotency-Key':'first'}, timeout=5).status_code == 200
        assert path.read_bytes().startswith(original)
        assert [json.loads(line)['idempotency_key'] for line in path.read_bytes().splitlines()] == ['first','second']
        committed = path.read_bytes()
        path.write_bytes(b'broken committed record\n'+committed)
        assert requests.post(url, json=payload, headers={'Idempotency-Key':'third'}, timeout=5).status_code == 400
        assert path.read_bytes() == b'broken committed record\n'+committed
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def test_duplicate_receipt_is_synced_before_acknowledgement(tmp_path, monkeypatch):
    import os
    from scripts.verify_operations_stack import webhook_server

    calls = []
    real_sync = os.fsync
    def sync(fd):
        calls.append(fd)
        return real_sync(fd)
    monkeypatch.setattr(os, 'fsync', sync)
    server = webhook_server(tmp_path/'receipts.jsonl', port=0)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        url = f'http://127.0.0.1:{server.server_port}/alerts'
        kwargs = dict(json={'status':'firing','alerts':[]}, headers={'Idempotency-Key':'retry'}, timeout=5)
        assert requests.post(url, **kwargs).status_code == 200
        before = len(calls)
        assert requests.post(url, **kwargs).status_code == 200
        assert len(calls) > before
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)
