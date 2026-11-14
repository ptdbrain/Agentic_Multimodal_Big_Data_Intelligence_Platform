"""Known active incidents survive unknown data and resolve their original classification."""

import json, sqlite3
import pytest

pytestmark = pytest.mark.unit


@pytest.fixture
def alert_store(tmp_path):
    from serving.store import ServingStore

    connection = sqlite3.connect(tmp_path / "transitions.sqlite")
    store = ServingStore(connection, "fixture", "simulation")
    store.initialize()
    connection.commit()
    event = {
        "event_id": "d" * 64,
        "product_id": "p",
        "rule": "traffic",
        "event_type": "REVIEW_BOMBING",
        "window_start": "2026-11-13T00:00:00Z",
        "window_end": "2026-11-13T00:01:00Z",
        "score": 5,
        "state": "firing",
        "evidence": {"quality_status": "valid", "source_group": "simulation"},
    }
    yield store, event
    connection.close()


def stage(store, event):
    with store.conn:
        store.stage_evaluations("g", [event])


def test_unknown_quality_does_not_forget_an_incident_that_later_clears(alert_store):
    store, event = alert_store
    stage(store, event)
    unknown = {**event, "state": "insufficient_data", "evidence": {"quality_status": "failed"}}
    stage(store, unknown)
    assert store.execute("SELECT COUNT(*) FROM alert_notifications").fetchone()[0] == 1
    stage(store, {**event, "state": "clear"})
    assert store.execute("SELECT state FROM alert_notifications ORDER BY revision").fetchall() == [
        ("firing",),
        ("resolved",),
    ]


def test_classification_change_resolves_old_type_before_firing_new_type(alert_store):
    store, event = alert_store
    stage(store, event)
    stage(store, {**event, "event_type": "REVIEW_BURST"})
    rows = store.execute(
        "SELECT state,payload_json FROM alert_notifications ORDER BY revision,CASE state WHEN 'resolved' THEN 0 ELSE 1 END"
    ).fetchall()
    assert [(state, json.loads(payload)["alerts"][0]["labels"]["alertname"]) for state, payload in rows] == [
        ("firing", "REVIEW_BOMBING"),
        ("resolved", "REVIEW_BOMBING"),
        ("firing", "REVIEW_BURST"),
    ]


@pytest.mark.parametrize(
    "field,value", [("product_id", "different"), ("rule", "different"), ("window_start", "2026-11-12T23:59:00Z")]
)
def test_stable_event_id_cannot_change_its_entity_rule_or_window(alert_store, field, value):
    store, event = alert_store
    stage(store, event)
    with pytest.raises(ValueError, match="identity"):
        stage(store, {**event, field: value})
    assert store.execute("SELECT revision FROM serving_alerts").fetchone()[0] == 1
