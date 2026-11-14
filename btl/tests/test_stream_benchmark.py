"""Benchmark accounting uses actual observed SQL frontiers, never target throughput as a result."""

import pytest

pytestmark = pytest.mark.unit


def test_sql_visibility_samples_wait_for_their_own_product_window_and_rank():
    from scripts.benchmark_stream_serving import VisibilityTracker

    tracker = VisibilityTracker()
    key = ("p", "2026-11-14T00:00:00Z")
    tracker.register(key, 5, 10.0)
    tracker.register(key, 10, 11.0)
    tracker.observe({("other", key[1]): 100, key: 4}, 12.0)
    assert tracker.summary()["samples"] == 0
    tracker.observe({key: 5}, 13.0)
    assert tracker.summary()["samples"] == 1
    tracker.observe({key: 10}, 14.0)
    result = tracker.summary()
    assert result["pending_samples"] == 0 and result["p50_ms"] == 3000
    assert result["p95_ms"] == 3000 and result["p99_ms"] == 3000
    with pytest.raises(ValueError, match="regress"):
        tracker.observe({key: 9}, 15.0)


def test_missing_visibility_stays_unknown_and_invalid_workloads_fail_before_services():
    from scripts.benchmark_stream_serving import VisibilityTracker, validate_workload

    tracker = VisibilityTracker()
    tracker.register(("p", "w"), 1, 1.0)
    result = tracker.summary()
    assert result["samples"] == 0 and result["p95_ms"] is None and result["pending_samples"] == 1
    for rate, warm, measure, drain in [
        (0, 30, 120, 120),
        (50, -1, 120, 120),
        (50, 30, 0, 120),
        (50, 30, 120, float("nan")),
    ]:
        with pytest.raises(ValueError):
            validate_workload(rate, warm, measure, drain)


def test_drain_checks_every_configured_sink_without_requiring_optional_audit():
    from scripts.benchmark_stream_serving import sinks_reconciled

    broker = {0: 34, 1: 36}
    caught_up = {0: 34, 1: 36}
    assert sinks_reconciled({'window': caught_up, 'quarantine': caught_up}, broker)
    assert not sinks_reconciled({'window': caught_up}, broker)
    assert not sinks_reconciled({'window': caught_up, 'quarantine': {0: 33, 1: 36}}, broker)
    assert not sinks_reconciled({'window': caught_up, 'quarantine': caught_up, 'audit': {}}, broker)
    assert not sinks_reconciled({'window': {}, 'quarantine': {}}, {})
