import pandas as pd
import pytest

from analytics.anomaly.rating_anomaly import RatingAnomalyDetector
from dashboard.db_connector import DashboardDB
from database.export_gold import export_gold_to_db
from scripts.run_e2e_pipeline import stage_5_spark_gold_marts
from storage.storage_manager import storage

pytestmark = pytest.mark.spark


def test_zero_current_events_replace_gold_snapshot_but_preserve_warehouse_incident_history(spark_session):
    historical_reviews = pd.DataFrame([
        {'review_id': f'{day}-{i}', 'product_id': 'p1', 'rating': rating, 'review_date': day}
        for day, rating in [('2026-10-01',5.0), ('2026-10-02',5.0), ('2026-10-03',1.0)] for i in range(3)
    ])
    old_events = RatingAnomalyDetector.detect_rating_drops(historical_reviews, run_id='old-run')
    assert len(old_events) == 1
    storage.write_gold_parquet('anomaly_events', pd.DataFrame(old_events))
    export_gold_to_db(marts=['anomaly_events'], silver_tables=[], mode='test')
    current_reviews = historical_reviews.assign(rating=5.0)
    products = pd.DataFrame([{'product_id':'p1','product_name':'Phone','brand':'A','category':'Phone','price':10.0}])
    prices = pd.DataFrame([{'product_id':'p1','price':10.0,'timestamp':'2026-10-01'}])
    for name, frame in [('products',products), ('reviews',current_reviews), ('prices',prices)]:
        storage.write_silver_parquet(name, frame)
    result = stage_5_spark_gold_marts(products, current_reviews, prices, run_id='current-run', enforce_dq_gate=False)
    assert result['anomalies_count'] == 0
    current = storage.read_gold_parquet('anomaly_events')
    assert current.empty
    assert 'event_id' in current.columns
    export_gold_to_db(marts=['anomaly_events'], silver_tables=[], mode='test')
    db = DashboardDB(mode='test')
    try:
        history = db.get_anomaly_events()
        assert len(history) == 1
        assert history.iloc[0]['run_id'] == 'old-run'
    finally:
        db.close()
