from pathlib import Path
import pandas as pd
import pytest
from database.warehouse import WarehouseManager

pytestmark = pytest.mark.unit


def test_dashboard_home_renders_unavailable_quality_and_percent_units():
    from streamlit.testing.v1 import AppTest
    wh = WarehouseManager(mode='test'); wh.initialize_schema(); wh.close()
    path = str(Path(__file__).resolve().parents[1]/'dashboard/app.py')
    empty = AppTest.from_file(path).run(timeout=20)
    assert not empty.exception
    assert any('Unavailable' in item.value for item in empty.markdown)
    wh = WarehouseManager(mode='test')
    wh.upsert_dataframe('data_quality_metrics', pd.DataFrame([{
        'batch_id': 'quality-1', 'dataset_name': 'reviews', 'records_received': 10, 'records_valid': 9,
        'records_invalid': 1, 'records_duplicate': 0, 'records_missing': 0, 'dq_score': 90.0,
    }]), ['batch_id']); wh.close()
    populated = AppTest.from_file(path).run(timeout=20)
    assert not populated.exception
    cards = ' '.join(item.value for item in populated.markdown)
    assert '90.0%' in cards
    assert '9000.0%' not in cards


def test_products_page_renders_full_catalog_schema_without_exception():
    from streamlit.testing.v1 import AppTest
    wh=WarehouseManager(mode='test');wh.initialize_schema()
    wh.upsert_dataframe('products',pd.DataFrame([{'product_id':'p1','product_name':'Actual phone','brand':'Apple','category':'Phone','price':100,'review_count':420}]),['product_id'])
    wh.upsert_dataframe('reviews',pd.DataFrame([{'review_id':'r1','product_id':'p1','rating':5,'review_text':'real record','review_date':'2026-10-01'}]),['review_id']);wh.close()
    app=AppTest.from_file(str(Path(__file__).resolve().parents[1]/'dashboard/pages/3_Products.py')).run(timeout=20)
    assert not app.exception
    assert len(app.dataframe)==1
    assert app.dataframe[0].value.iloc[0]['review_count']==1


def test_quality_panel_renders_missing_time_and_catalog_fallback():
    from streamlit.testing.v1 import AppTest
    app = AppTest.from_string('''
import pandas as pd
from dashboard.utils import render_analytics_quality
render_analytics_quality(pd.DataFrame([
    {'time_available': False, 'price_source': 'catalog_fallback', 'rating_sample_size': 0}
]))
''').run(timeout=20)
    assert not app.exception
    warnings = ' '.join(x.value for x in app.warning)
    assert '1 records without valid observation time' in warnings
    assert '1 catalog price fallback' in warnings
    assert 'No valid rating sample' in warnings


def test_trends_page_exposes_insufficient_calendar_coverage():
    from streamlit.testing.v1 import AppTest
    wh = WarehouseManager(mode='test')
    wh.initialize_schema()
    wh.upsert_dataframe('reviews', pd.DataFrame([
        {'review_id': 'r1', 'product_id': 'p1', 'rating': 5.0, 'review_text': 'review', 'review_date': '2026-01-01'},
        {'review_id': 'r2', 'product_id': 'p1', 'rating': 1.0, 'review_text': 'review', 'review_date': '2026-10-01'},
    ]), ['review_id'])
    wh.close()
    app = AppTest.from_file(str(Path(__file__).resolve().parents[1]/'dashboard/pages/5_Trends.py')).run(timeout=20)
    assert not app.exception
    assert any('Insufficient calendar coverage' in w.value for w in app.warning)
    assert any('missing_days' in frame.value.columns for frame in app.dataframe)


def test_anomalies_page_displays_persisted_run_and_baseline_units():
    from streamlit.testing.v1 import AppTest
    wh = WarehouseManager(mode='test')
    wh.initialize_schema()
    wh.upsert_dataframe('anomaly_events', pd.DataFrame([{
        'event_id': 'a1', 'entity_type': 'PRODUCT', 'entity_id': 'p1',
        'anomaly_type': 'RATING_DROP', 'score': 4.0, 'timestamp': '2026-10-03',
        'run_id': 'observed-run', 'data_quality_status': 'valid',
        'baseline_sample_size': 10, 'baseline_sample_unit': 'reviews',
        'baseline_method': 'past_calendar_window', 'baseline_window_days': 7,
        'baseline_start': '2026-09-26', 'baseline_end': '2026-10-03',
        'current_sample_size': 3,
    }]), ['event_id'])
    wh.close()
    app = AppTest.from_file(str(Path(__file__).resolve().parents[1]/'dashboard/pages/6_Anomalies.py')).run(timeout=20)
    assert not app.exception
    row = app.dataframe[0].value.iloc[0]
    assert row['run_id'] == 'observed-run'
    assert row['baseline_sample_unit'] == 'reviews'
    assert row['baseline_sample_size'] == 10
    assert row['current_sample_size'] == 3
