import json

import pandas as pd
import pytest

from dashboard.db_connector import DashboardDB
from database.export_gold import export_gold_to_db
from database.warehouse import WarehouseManager
from spark.etl.normalizer import EntityNormalizer
from storage.storage_manager import storage

pytestmark = pytest.mark.unit


@pytest.mark.parametrize('dataset', ['products', 'reviews'])
def test_silver_export_preserves_raw_normalization_and_separate_clocks(dataset):
    if dataset == 'products':
        record = EntityNormalizer.normalize_product({
            'product_id': 'p1', 'product_name': 'Phone', 'price': ' 10 VND ', 'ingested_at': '2026-10-01',
        })
        key, numeric = 'product_id', 'price'
    else:
        record = EntityNormalizer.normalize_record({
            'review_id': 'r1', 'product_id': 'p1', 'rating': ' 5 ', 'review_text': 'text', 'ingested_at': '2026-10-01',
        }, 'reviews')
        key, numeric = 'review_id', 'rating'
    storage.write_silver_parquet(dataset, pd.DataFrame([record]))
    for _ in range(2):
        export_gold_to_db(marts=[], silver_tables=[dataset], mode='test')
    db = DashboardDB(mode='test')
    try:
        rows = db.query(f'SELECT * FROM {dataset}')
        assert len(rows) == 1
        row = rows.iloc[0]
        assert row[key] == record[key]
        assert row[f'{numeric}_raw'] == record[f'{numeric}_raw']
        assert row['normalization_status'] == 'valid'
        assert json.loads(row['normalization_errors']) == []
        assert not row['time_available']
        assert row['ingestion_time_available']
        assert row['ingested_at_raw'] == '2026-10-01'
    finally:
        db.close()


def test_existing_silver_tables_upgrade_without_inventing_metadata():
    wh = WarehouseManager(mode='test')
    wh._conn.executescript('''
        CREATE TABLE products(product_id TEXT PRIMARY KEY,product_name TEXT,brand TEXT,category TEXT,price NUMERIC);
        INSERT INTO products VALUES('p1','Old phone','A','Phone',10);
        CREATE TABLE reviews(review_id TEXT PRIMARY KEY,product_id TEXT,rating NUMERIC,review_date DATE);
        INSERT INTO reviews VALUES('r1','p1',4,'2026-01-01');
    ''')
    wh.initialize_schema()
    wh.initialize_schema()
    for table, metric in [('products', 'price'), ('reviews', 'rating')]:
        row = wh.query(f'SELECT {metric},normalization_status,normalization_errors,time_available,ingestion_time_available FROM {table}').iloc[0]
        assert row[metric] == (10 if table == 'products' else 4)
        assert all(pd.isna(row[field]) for field in ['normalization_status', 'normalization_errors', 'time_available', 'ingestion_time_available'])
    wh.close()


@pytest.mark.parametrize('table,key,count', [('products','product_id','review_count'),
                                           ('reviews','review_id','helpful_count')])
def test_unknown_source_count_does_not_take_legacy_database_zero_default(table, key, count):
    wh = WarehouseManager(mode='test')
    try:
        # Reproduce an older deployed schema whose default would invent a zero.
        wh.execute(f'CREATE TABLE {table}({key} TEXT PRIMARY KEY, {count} INTEGER DEFAULT 0)')
        wh.upsert_dataframe(table, pd.DataFrame([{key:'unknown'}]), [key])
        wh.upsert_dataframe(table, pd.DataFrame([{key:'observed-zero', count:0}]), [key])
        rows = wh.query(f'SELECT * FROM {table}').set_index(key)
        assert pd.isna(rows.loc['unknown', count])
        assert rows.loc['observed-zero', count] == 0
    finally:
        wh.close()


def test_missing_currency_does_not_acquire_a_legacy_vnd_database_default():
    wh=WarehouseManager(mode='test')
    try:
        wh.execute("CREATE TABLE products(product_id TEXT PRIMARY KEY,currency TEXT DEFAULT 'VND')")
        wh.upsert_dataframe('products',pd.DataFrame([{'product_id':'unknown'}]),['product_id'])
        wh.upsert_dataframe('products',pd.DataFrame([{'product_id':'observed','currency':'VND'}]),['product_id'])
        rows=wh.query('SELECT * FROM products').set_index('product_id')
        assert pd.isna(rows.loc['unknown','currency'])
        assert rows.loc['observed','currency'] == 'VND'
    finally:
        wh.close()
