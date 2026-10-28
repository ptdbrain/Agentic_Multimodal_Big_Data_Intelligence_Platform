import pandas as pd
import pytest
from analytics.descriptive.stats import DescriptiveStats
from database.warehouse import WarehouseManager
from dashboard.db_connector import DashboardDB

pytestmark = pytest.mark.unit


def test_catalog_and_ingested_review_counts_are_distinct():
    p=pd.DataFrame([{'product_id':'p1','review_count':420}, {'product_id':'p2','review_count':100}])
    r=pd.DataFrame([{'product_id':'p1','review_id':'r1','rating':1}, {'product_id':'p1','review_id':'r2','rating':5}])
    result=DescriptiveStats.compute_product_metrics(r,p).set_index('product_id')
    assert result.loc['p1','catalog_review_count'] == 420
    assert result.loc['p1','review_count'] == 2
    assert result.loc['p2','review_count'] == 0


def test_warehouse_rating_is_weighted_by_review_count():
    wh=WarehouseManager(mode='test'); wh.initialize_schema()
    wh.upsert_dataframe('product_daily_stats',pd.DataFrame([
        {'date':'2026-10-01','product_id':'p1','review_count':1,'avg_rating':1},
        {'date':'2026-10-01','product_id':'p2','review_count':9,'avg_rating':5},
    ]),['date','product_id']); wh.close()
    db=DashboardDB(mode='test')
    try: assert db.get_kpi_summary()['avg_rating'] == 4.6
    finally: db.close()


def test_empty_warehouse_kpi_keeps_rating_unavailable():
    wh = WarehouseManager(mode='test'); wh.initialize_schema(); wh.close()
    db = DashboardDB(mode='test')
    try:
        summary = db.get_kpi_summary()
        assert summary['avg_rating'] is None
        assert summary['rating_sample_size'] == 0
        assert summary['avg_dq_score'] is None
    finally:
        db.close()


def test_rating_kpi_excludes_unknown_means_from_weighted_denominator():
    wh = WarehouseManager(mode='test'); wh.initialize_schema()
    wh.upsert_dataframe('product_daily_stats', pd.DataFrame([
        {'date': '2026-10-01', 'product_id': 'unknown', 'review_count': 100, 'avg_rating': None},
        {'date': '2026-10-01', 'product_id': 'observed', 'review_count': 5, 'rating_sample_size': 2, 'avg_rating': 4.5},
    ]), ['date', 'product_id']); wh.close()
    db = DashboardDB(mode='test')
    try:
        summary = db.get_kpi_summary()
        assert summary['total_reviews'] == 105
        assert summary['avg_rating'] == 4.5
        assert summary['rating_sample_size'] == 2
    finally:
        db.close()


def test_local_sample_fallback_is_explicitly_labeled_synthetic():
    from dashboard.utils import load_catalog_data
    frame=load_catalog_data()
    assert not frame.empty
    assert frame.attrs.get('data_source')=='Sample fixture (synthetic)'


def test_warehouse_data_is_not_labeled_as_sample():
    from dashboard.utils import load_catalog_data
    wh=WarehouseManager(mode='test');wh.initialize_schema()
    wh.upsert_dataframe('products',pd.DataFrame([{'product_id':'p1','product_name':'Observed phone',
        'brand':'Brand','category':'Phone','price':100}]),['product_id'])
    wh.close()
    frame=load_catalog_data()
    assert frame.attrs.get('data_source')=='SQLite warehouse'
    assert frame['product_id'].tolist()==['p1']


def test_dashboard_quality_summary_exposes_time_sample_and_price_provenance():
    from dashboard.utils import summarize_analytics_frame
    frame = pd.DataFrame([
        {'data_as_of': '2026-10-01', 'time_available': True,
         'price_source': 'catalog_fallback', 'rating_sample_size': 3},
        {'data_as_of': None, 'time_available': False,
         'price_source': 'observed', 'rating_sample_size': 0},
    ])
    assert summarize_analytics_frame(frame) == {
        'rows': 2, 'timed_rows': 1, 'untimed_rows': 1, 'unknown_time_rows': 0,
        'data_as_of': '2026-10-01', 'catalog_price_fallback_rows': 1,
        'rating_sample_size': 3,
    }


def test_quality_summary_does_not_invent_missing_time_coverage():
    from dashboard.utils import summarize_analytics_frame
    result = summarize_analytics_frame(pd.DataFrame([{'product_id': 'p1'}]))
    assert result['timed_rows'] is None
    assert result['untimed_rows'] is None


def test_anomaly_warehouse_schema_preserves_lineage_metadata():
    wh = WarehouseManager(mode='test')
    wh.initialize_schema()
    columns = {row[1] for row in wh._conn.execute('PRAGMA table_info(anomaly_events)').fetchall()}
    wh.close()
    assert {'run_id', 'data_quality_status', 'baseline_sample_size'}.issubset(columns)


def test_anomaly_warehouse_round_trip_preserves_lineage_metadata():
    wh = WarehouseManager(mode='test')
    wh.initialize_schema()
    wh.upsert_dataframe('anomaly_events', pd.DataFrame([{
        'event_id': 'a1', 'entity_type': 'PRODUCT', 'entity_id': 'p1',
        'anomaly_type': 'REVIEW_BURST', 'score': 2.0,
        'timestamp': '2026-10-01', 'run_id': 'run-1',
        'data_quality_status': 'valid', 'baseline_sample_size': 24,
    }]), ['event_id'])
    row = wh._conn.execute('SELECT run_id, data_quality_status, baseline_sample_size FROM anomaly_events WHERE event_id=?', ('a1',)).fetchone()
    wh.close()
    assert row == ('run-1', 'valid', 24)


def test_legacy_null_quality_metadata_is_unknown_not_failed_or_zero_sample():
    from dashboard.utils import summarize_analytics_frame
    summary = summarize_analytics_frame(pd.DataFrame([
        {'time_available':None, 'rating_sample_size':None},
    ]))
    assert summary['timed_rows'] is None
    assert summary['untimed_rows'] is None
    assert summary['unknown_time_rows'] == 1
    assert summary['rating_sample_size'] is None


def test_mixed_known_and_unknown_time_metadata_preserves_both_counts():
    from dashboard.utils import summarize_analytics_frame
    summary = summarize_analytics_frame(pd.DataFrame({'time_available':[True,False,None]}))
    assert summary['timed_rows'] == 1
    assert summary['untimed_rows'] == 1
    assert summary['unknown_time_rows'] == 1
