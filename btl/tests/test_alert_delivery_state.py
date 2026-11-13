import sqlite3
import pytest

pytestmark = pytest.mark.unit


def test_changed_alert_has_history_but_only_transitions_enqueue_notifications(tmp_path):
    from serving.store import ServingStore

    c = sqlite3.connect(tmp_path / "alerts.sqlite")
    s = ServingStore(c, "fixture", "simulation")
    s.initialize()
    c.commit()
    event = {
        "event_id": "a" * 64,
        "product_id": "p",
        "window_start": "2026-11-13T00:00:00+00:00",
        "window_end": "2026-11-13T00:01:00+00:00",
        "rule": "traffic",
        "event_type": "REVIEW_BURST",
        "state": "firing",
        "score": 5,
        "evidence": {
            "baseline_windows": 12,
            "configuration_sha256": "fixture",
            "provisional": True,
            "quality_status": "valid",
        },
    }
    with c:
        s.stage_evaluations("g", [event])
    with c:
        s.stage_evaluations("g", [event])
    assert c.execute("SELECT revision FROM serving_alerts").fetchone()[0] == 1
    assert c.execute("SELECT COUNT(*) FROM alert_notifications").fetchone()[0] == 1
    changed = {**event, "score": 6}
    with c:
        s.stage_evaluations("g", [changed])
    assert c.execute("SELECT revision FROM serving_alerts").fetchone()[0] == 2
    assert c.execute("SELECT COUNT(*) FROM alert_notifications").fetchone()[0] == 1
    with c:
        s.stage_evaluations("g", [{**changed, "state": "clear"}])
    assert c.execute("SELECT COUNT(*) FROM alert_notifications").fetchone()[0] == 2
    assert c.execute("SELECT COUNT(*) FROM serving_alert_history").fetchone()[0] == 3
    c.close()
