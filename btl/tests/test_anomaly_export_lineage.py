"""Real detector→Parquet→warehouse path, including an existing legacy table."""
import pandas as pd
import pytest
from analytics.anomaly.rating_anomaly import RatingAnomalyDetector
from database.export_gold import export_gold_to_db
from database.warehouse import WarehouseManager
from dashboard.db_connector import DashboardDB
from storage.storage_manager import storage

pytestmark = pytest.mark.unit


def test_legacy_anomaly_schema_migration_and_gold_export_preserve_baseline():
    wh = WarehouseManager(mode='test')
    wh._conn.execute('''CREATE TABLE anomaly_events (
        event_id VARCHAR(100) PRIMARY KEY, entity_type TEXT NOT NULL, entity_id TEXT NOT NULL,
        anomaly_type TEXT NOT NULL, score NUMERIC NOT NULL, timestamp TIMESTAMP NOT NULL,
        description TEXT, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)''')
    wh._conn.execute("INSERT INTO anomaly_events(event_id,entity_type,entity_id,anomaly_type,score,timestamp) VALUES ('old','PRODUCT','p0','RATING_DROP',1,'2026-01-01')")
    wh._conn.commit()
    wh.initialize_schema()
    wh.initialize_schema()
    old = wh._conn.execute("SELECT run_id FROM anomaly_events WHERE event_id='old'").fetchone()
    assert old == (None,)
    wh.close()

    rows = pd.DataFrame([
        dict(review_id=f'{date}-{i}', product_id='p1', rating=rating, review_date=date)
        for date, rating, n in [('2026-10-01',5,4),('2026-10-02',5,6),('2026-10-03',1,3)]
        for i in range(n)
    ])
    events = RatingAnomalyDetector.detect_rating_drops(rows, run_id='run-real', data_quality_status='valid')
    assert len(events) == 1
    storage.write_gold_parquet('anomaly_events', pd.DataFrame(events))
    for _ in range(2):
        assert export_gold_to_db(marts=['anomaly_events'], silver_tables=[], mode='test')['anomaly_events'] == 1
    db = DashboardDB(mode='test')
    try:
        served = db.get_anomaly_events().set_index('event_id')
        assert len(served) == 2  # Replay updates the same ID; legacy history remains.
        event = served.loc[events[0]['event_id']]
        assert event['run_id'] == 'run-real'
        assert event['data_quality_status'] == 'valid'
        assert event['baseline_sample_size'] == 10
        assert event['current_sample_size'] == 3
        assert event['baseline_sample_unit'] == 'reviews'
        assert event['baseline_method'] == 'past_calendar_window'
        assert event['baseline_observed_days'] == 2
    finally:
        db.close()
