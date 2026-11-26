"""Price alerts honor observation time, source binding and Kafka replay boundaries."""
from datetime import datetime, timezone
import json
import sqlite3

import pytest

from analytics.causal import StatisticalConfig
from serving.price_stream import initialize_price_stream, stage_price_event
from serving.store import ServingStore

pytestmark = pytest.mark.unit
NOW = datetime(2026, 11, 26, 10, 0, tzinfo=timezone.utc)


def envelope(key, day, value=100, seller="a", currency="VND", method="generated"):
    return json.dumps(dict(event_id=key, event_type="PRICE_UPDATE", source="fixture", ingested_at=NOW.isoformat(),
                           payload=dict(price_id=key,product_id="product",price=value,seller=seller,
                                        currency=currency,timestamp=day,source_group="simulation",
                                        acquisition_method=method))).encode()


def test_price_stream_baseline_and_replay_are_bound_to_real_observation_days():
    with sqlite3.connect(":memory:") as conn:
        store = ServingStore(conn, "fixture", "simulation")
        initialize_price_stream(store)
        cfg = StatisticalConfig(price_min_points=3)
        send = lambda offset, raw: stage_price_event(store, "source", 0, offset, raw, NOW, cfg)
        for offset, day in enumerate(("2026-11-23", "2026-11-24", "2026-11-25")):
            assert send(offset, envelope(str(offset), day + "T00:00:00Z")) == "historical_observation"
        assert send(4, envelope("other-seller", "2026-11-25T00:00:00Z", 5000, seller="b")) == "historical_observation"
        assert send(5, envelope("usd", "2026-11-25T00:00:00Z", 1, currency="USD")) == "historical_observation"
        assert send(6, envelope("future", "2026-11-26T12:00:00Z", 900)) == "future_timestamp"
        assert send(7, envelope("invalid", "2026-11-26T08:55:00", 900)) == "rejected"
        current = envelope("signal", "2026-11-26T09:00:00Z", 250)
        assert send(8, current) == "evaluated"
        assert send(8, current) == "evaluated"
        assert send(9, current) == "duplicate"
        assert send(10, envelope("signal", "2026-11-26T09:00:00Z", 999)) == "identity_conflict"
        with pytest.raises(ValueError, match="coordinates"):
            send(8, b"{}")
        alerts = store.execute("SELECT state,evidence_json,revision FROM serving_alerts").fetchall()
        assert len(alerts) == 2
        for state, item, rev in alerts:
            evidence = json.loads(item)
            assert state == "firing" and rev == 1 and evidence["baseline_samples"] == 3
            assert evidence["baseline_mean"] == 100 and evidence["current_price"] == 250
            assert evidence["currency"] == "VND" and evidence["seller"] == "a"
        assert store.execute("SELECT count(*) FROM alert_notifications").fetchone() == (2,)
        assert store.execute("SELECT count(*) FROM price_stream_observations").fetchone() == (6,)
