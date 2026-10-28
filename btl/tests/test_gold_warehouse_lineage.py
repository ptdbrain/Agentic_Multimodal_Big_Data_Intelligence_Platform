"""Additive serving schema must preserve existing marts and Gold lineage."""
import pandas as pd
import pytest
from database.export_gold import export_gold_to_db
from database.warehouse import WarehouseManager
from dashboard.db_connector import DashboardDB
from storage.storage_manager import storage

pytestmark = pytest.mark.unit

MARTS = [('product_daily_stats', 'product_id'), ('brand_daily_stats', 'brand'),
         ('category_daily_stats', 'category'), ('price_daily_stats', 'product_id')]


@pytest.mark.parametrize('mart,key', MARTS)
def test_mart_export_preserves_lineage_and_sample_metadata(mart, key):
    row = {'date': '2026-10-01', key: 'p1', 'run_id': 'current-run',
           'source_run_id': 'silver-run', 'data_as_of': '2026-10-01', 'time_available': True}
    if mart != 'price_daily_stats':
        row.update(review_count=10, rating_sample_size=10, avg_rating=4.6, weighted_avg_rating=4.6)
    if mart == 'product_daily_stats':
        row.update(price_source='catalog_fallback', avg_price=10.)
    storage.write_gold_parquet(mart, pd.DataFrame([row]))
    export_gold_to_db(marts=[mart], silver_tables=[], mode='test')
    db = DashboardDB(mode='test')
    try:
        served = db.query(f'SELECT * FROM {mart}').iloc[0]
        assert served['run_id'] == 'current-run'
        assert served['source_run_id'] == 'silver-run'
        assert str(served['data_as_of']) == '2026-10-01'
        assert bool(served['time_available'])
        if mart != 'price_daily_stats':
            assert served['rating_sample_size'] == 10
            assert served['weighted_avg_rating'] == 4.6
        if mart == 'product_daily_stats':
            assert served['price_source'] == 'catalog_fallback'
    finally:
        db.close()


def test_existing_gold_tables_upgrade_without_invented_lineage():
    wh = WarehouseManager(mode='test')
    wh._conn.execute('''CREATE TABLE product_daily_stats (
        date DATE NOT NULL, product_id TEXT NOT NULL, review_count INTEGER,
        avg_rating NUMERIC, PRIMARY KEY(date,product_id))''')
    wh._conn.execute("INSERT INTO product_daily_stats VALUES ('2026-01-01','old',5,4.2)")
    wh._conn.commit()
    wh.initialize_schema()
    wh.initialize_schema()
    old = wh._conn.execute("SELECT review_count,avg_rating,run_id,rating_sample_size,time_available FROM product_daily_stats").fetchone()
    wh.close()
    assert old == (5, 4.2, None, None, None)
