"""SQL serving receipts must survive replay and refuse changed batch identities."""

import sqlite3
import pytest

pytestmark = pytest.mark.unit


def store(tmp_path):
    from serving.store import ServingStore

    connection = sqlite3.connect(tmp_path / "serving.sqlite")
    result = ServingStore(connection, "tiki", "marketplace")
    result.initialize()
    connection.commit()
    return result


def row(count=3):
    return {
        "window_start": "2026-11-13T10:00:00Z",
        "window_end": "2026-11-13T10:01:00Z",
        "product_id": "p",
        "source_group": "marketplace",
        "review_count": count,
        "avg_rating": 4.0,
        "negative_count": 1,
    }


def test_sql_window_replay_is_idempotent_and_old_batch_cannot_regress(tmp_path):
    s = store(tmp_path)
    assert s.write_windows("generation-one", 1, [row()], window_seconds=60) == 1
    assert s.write_windows("generation-one", 1, [row()], window_seconds=60) == 0
    s.write_windows("generation-one", 3, [row(5)], window_seconds=60)
    s.write_windows("generation-one", 2, [row(4)], window_seconds=60)
    assert s.conn.execute("SELECT review_count FROM stream_windows").fetchone()[0] == 5
    with pytest.raises(ValueError, match="identity|changed"):
        s.write_windows("generation-one", 1, [row(99)], window_seconds=60)
    assert s.conn.execute("SELECT review_count FROM stream_windows").fetchone()[0] == 5
    s.conn.close()


def test_partial_or_wrong_source_batch_rolls_back_rows_and_receipt(tmp_path):
    s = store(tmp_path)

    def interrupted():
        yield row()
        raise RuntimeError("sink interrupted")

    with pytest.raises(RuntimeError):
        s.write_windows("g", 0, interrupted(), window_seconds=60)
    assert s.conn.execute("SELECT COUNT(*) FROM stream_windows").fetchone()[0] == 0
    assert s.conn.execute("SELECT COUNT(*) FROM stream_batches").fetchone()[0] == 0
    with pytest.raises(ValueError, match="source"):
        s.write_windows("g", 0, [dict(row(), source_group="simulation")], window_seconds=60)
    assert s.conn.execute("SELECT COUNT(*) FROM stream_windows").fetchone()[0] == 0
    s.conn.close()


def test_window_contract_checks_counts_clocks_and_generation(tmp_path):
    s = store(tmp_path)
    for bad in [
        dict(row(), negative_count=4),
        dict(row(), review_count=-1),
        dict(row(), avg_rating=float("nan")),
        dict(row(), window_end="2026-11-13T10:05:00Z"),
    ]:
        with pytest.raises(ValueError):
            s.write_windows("g", 0, [bad], window_seconds=60)
    assert s.conn.execute("SELECT COUNT(*) FROM stream_batches").fetchone()[0] == 0
    s.conn.close()


def test_receipt_identity_is_independent_of_iterator_order(tmp_path):
    s = store(tmp_path)
    rows = [row(), dict(row(), product_id="other")]
    assert s.write_windows("g", 1, rows, window_seconds=60) == 2
    assert s.write_windows("g", 1, reversed(rows), window_seconds=60) == 0
    s.conn.close()


def test_stream_deployment_requires_explicit_generation_and_pins_configuration(tmp_path):
    s = store(tmp_path)
    s.register_stream("g", {"window_seconds": 60, "watermark_seconds": 600, "topic": "reviews"})
    s.register_stream("g", {"window_seconds": 60, "watermark_seconds": 600, "topic": "reviews"})
    with pytest.raises(ValueError, match="configuration"):
        s.register_stream("g", {"window_seconds": 300, "watermark_seconds": 600, "topic": "reviews"})
    with pytest.raises(ValueError, match="generation"):
        s.register_stream("other", {"window_seconds": 60, "watermark_seconds": 600, "topic": "reviews"})
    s.conn.close()


def test_same_dataset_cannot_have_two_live_stream_owners(tmp_path):
    from serving.registry import DatasetRegistry
    from serving.ownership import acquire_stream_owner
    from serving.store import connect_dataset
    from tests.test_serving_registry import registry_file

    binding = DatasetRegistry.load(registry_file(tmp_path)).datasets["tiki"]
    first = connect_dataset(binding)
    second = connect_dataset(binding)
    try:
        with acquire_stream_owner(first, binding):
            with pytest.raises(RuntimeError, match="owner"):
                with acquire_stream_owner(second, binding):
                    pass
        with acquire_stream_owner(second, binding):
            pass
    finally:
        first.close()
        second.close()


def test_alerts_are_published_only_after_confirmed_stream_progress(tmp_path):
    from datetime import datetime, timedelta, timezone

    s = store(tmp_path)
    s.register_stream("g", {"window_seconds": 60, "watermark_seconds": 600, "topic": "reviews"})
    start = datetime(2026, 11, 13, 0, tzinfo=timezone.utc)
    rows = []
    for index in range(13):
        rows.append(
            dict(
                row(20 if index == 12 else 3),
                window_start=(start + timedelta(minutes=index)).isoformat(),
                window_end=(start + timedelta(minutes=index + 1)).isoformat(),
            )
        )
    s.write_windows("g", 1, rows, window_seconds=60)
    assert s.conn.execute("SELECT COUNT(*) FROM serving_alerts").fetchone()[0] == 0
    progress = {"batchId": 2, "timestamp": "2026-11-13T00:14:00Z", "eventTime": {"watermark": "2026-11-13T00:13:00Z"}}
    assert not s.stream_progress("g", progress)
    assert s.conn.execute("SELECT COUNT(*) FROM alert_notifications").fetchone()[0] == 0
    progress["batchId"] = 1
    assert s.stream_progress("g", progress)
    assert s.conn.execute("SELECT COUNT(*) FROM serving_alerts WHERE state='firing'").fetchone()[0] == 1
    assert s.conn.execute("SELECT COUNT(*) FROM alert_notifications").fetchone()[0] == 1
    assert s.stream_progress("g", progress)
    assert s.conn.execute("SELECT COUNT(*) FROM alert_notifications").fetchone()[0] == 1
    s.conn.close()


def test_empty_committed_batch_finalizes_previously_provisional_alerts(tmp_path):
    from datetime import datetime, timedelta, timezone
    import json

    s = store(tmp_path)
    s.register_stream("g", {"window_seconds": 60, "watermark_seconds": 600, "topic": "reviews"})
    start = datetime(2026, 11, 13, tzinfo=timezone.utc)
    rows = [
        dict(
            row(20 if i == 12 else 3),
            window_start=(start + timedelta(minutes=i)).isoformat(),
            window_end=(start + timedelta(minutes=i + 1)).isoformat(),
        )
        for i in range(13)
    ]
    s.write_windows("g", 1, rows, window_seconds=60)
    progress = {"batchId": 1, "timestamp": "2026-11-13T00:14:00Z", "eventTime": {"watermark": "2026-11-13T00:12:00Z"}}
    s.stream_progress("g", progress)
    assert json.loads(s.conn.execute("SELECT evidence_json FROM serving_alerts WHERE state='firing'").fetchone()[0])[
        "provisional"
    ]
    s.write_windows("g", 2, [], window_seconds=60)
    s.stream_progress("g", {**progress, "batchId": 2, "eventTime": {"watermark": "2026-11-13T00:13:00Z"}})
    assert not json.loads(
        s.conn.execute("SELECT evidence_json FROM serving_alerts WHERE state='firing'").fetchone()[0]
    )["provisional"]
    assert s.conn.execute("SELECT COUNT(*) FROM alert_notifications").fetchone()[0] == 1
    s.conn.close()


def test_stream_search_and_delivery_have_independent_worker_locks(tmp_path):
    from dataclasses import replace
    from serving.registry import DatasetRegistry
    from serving.ownership import acquire_stream_owner
    from serving.store import connect_dataset
    from tests.test_serving_registry import registry_file

    binding = DatasetRegistry.load(registry_file(tmp_path)).datasets["tiki"]
    first, second = connect_dataset(binding), connect_dataset(binding)
    try:
        with acquire_stream_owner(first, binding):
            with acquire_stream_owner(second, replace(binding, id=binding.id + "-notifications")):
                pass
    finally:
        first.close()
        second.close()


def test_first_progress_may_follow_empty_batch_without_losing_prior_windows(tmp_path):
    s = store(tmp_path)
    s.register_stream("g", {"window_seconds": 60, "watermark_seconds": 600, "topic": "reviews"})
    s.write_windows("g", 0, [row()], window_seconds=60)
    s.write_windows("g", 1, [], window_seconds=60)
    assert s.stream_progress(
        "g", {"batchId": 1, "timestamp": "2026-11-13T10:02:00Z", "eventTime": {"watermark": "2026-11-13T09:50:00Z"}}
    )
    assert s.conn.execute("SELECT COUNT(*) FROM serving_alerts").fetchone()[0] == 2
    s.conn.close()


def test_alerts_exclude_sql_windows_ahead_of_all_sink_frontier(tmp_path):
    s = store(tmp_path)
    s.register_stream("g", {"window_seconds": 60, "watermark_seconds": 600, "topic": "reviews"})
    s.write_windows("g", 0, [row()], window_seconds=60)
    s.write_windows("g", 1, [], window_seconds=60)
    s.write_windows("g", 2, [dict(row(99), product_id="ahead")], window_seconds=60)
    progress = {"batchId": 1, "timestamp": "2026-11-13T10:02:00Z", "eventTime": {"watermark": "2026-11-13T09:50:00Z"}}
    assert s.stream_progress("g", progress)
    assert s.conn.execute("SELECT DISTINCT product_id FROM serving_alerts").fetchall() == [("p",)]
    assert s.stream_progress("g", {**progress, "batchId": 2})
    assert s.conn.execute("SELECT COUNT(*) FROM serving_alerts").fetchone()[0] == 4
    s.conn.close()
