import pandas as pd
import pytest

from database.export_gold import export_gold_to_db
from database.warehouse import WarehouseManager
from storage.storage_manager import storage

pytestmark = pytest.mark.unit


def gold(name, run_id='new'):
    dimension = 'brand' if name == 'brand_daily_stats' else 'product_id'
    storage.write_gold_parquet(name, pd.DataFrame([{
        'date': '2026-10-01', dimension: 'entity', 'review_count': 3, 'avg_rating': 4.0, 'run_id': run_id,
    }]))


def seed():
    wh = WarehouseManager(mode='test'); wh.initialize_schema()
    wh.upsert_dataframe('product_daily_stats', pd.DataFrame([{
        'date': '2026-10-01', 'product_id': 'entity', 'review_count': 1, 'run_id': 'old',
    }]), ['date', 'product_id'])
    wh.close()


def test_second_table_export_failure_rolls_back_the_first_table(monkeypatch):
    seed(); gold('product_daily_stats'); gold('brand_daily_stats')
    original = WarehouseManager.upsert_dataframe
    def fail_second(self, name, *args, **kwargs):
        if name == 'brand_daily_stats':
            raise RuntimeError('injected second-table failure')
        return original(self, name, *args, **kwargs)
    monkeypatch.setattr(WarehouseManager, 'upsert_dataframe', fail_second)
    with pytest.raises(RuntimeError, match='second-table failure'):
        export_gold_to_db(marts=['product_daily_stats', 'brand_daily_stats'], silver_tables=[], mode='test')
    wh = WarehouseManager(mode='test')
    try:
        row = wh.query('SELECT review_count,run_id FROM product_daily_stats').iloc[0]
        assert row['run_id'] == 'old'
        assert row['review_count'] == 1
        assert wh.query('SELECT * FROM brand_daily_stats').empty
    finally:
        wh.close()


@pytest.mark.parametrize('other_run', ['old', None])
def test_mixed_gold_generations_are_rejected_before_serving_changes(other_run):
    seed(); gold('product_daily_stats', 'new'); gold('brand_daily_stats', other_run)
    with pytest.raises(ValueError, match='run'):
        export_gold_to_db(marts=['product_daily_stats', 'brand_daily_stats'], silver_tables=[], mode='test')
    wh = WarehouseManager(mode='test')
    try:
        assert wh.query('SELECT run_id FROM product_daily_stats')['run_id'].tolist() == ['old']
    finally:
        wh.close()


def test_successful_multi_mart_export_commits_both_tables():
    seed(); gold('product_daily_stats'); gold('brand_daily_stats')
    export_gold_to_db(marts=['product_daily_stats', 'brand_daily_stats'], silver_tables=[], mode='test')
    wh = WarehouseManager(mode='test')
    try:
        for name in ['product_daily_stats', 'brand_daily_stats']:
            assert wh.query(f'SELECT run_id FROM {name}')['run_id'].tolist() == ['new']
    finally:
        wh.close()


def test_dashboard_reader_keeps_one_generation_across_a_concurrent_export():
    from dashboard.db_connector import DashboardDB
    seed()
    wh = WarehouseManager(mode='test')
    wh.upsert_dataframe('brand_daily_stats', pd.DataFrame([{
        'date': '2026-10-01', 'brand': 'entity', 'review_count': 1, 'run_id': 'old',
    }]), ['date', 'brand'])
    wh.close()
    gold('product_daily_stats'); gold('brand_daily_stats')
    reader = DashboardDB(mode='test')
    try:
        assert reader.get_product_daily_stats()['run_id'].tolist() == ['old']
        export_gold_to_db(marts=['product_daily_stats', 'brand_daily_stats'], silver_tables=[], mode='test')
        assert reader.get_brand_daily_stats()['run_id'].tolist() == ['old']
    finally:
        reader.close()
    fresh = DashboardDB(mode='test')
    try:
        assert fresh.get_product_daily_stats()['run_id'].tolist() == ['new']
        assert fresh.get_brand_daily_stats()['run_id'].tolist() == ['new']
    finally:
        fresh.close()
