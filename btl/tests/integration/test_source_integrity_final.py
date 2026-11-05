"""Actual PostgreSQL/MinIO regressions; all inputs here are controlled fixtures."""
import hashlib
import uuid
import pandas as pd
import pytest
from tests.integration.test_processing_contracts import postgres_scope, remote_storage
from tests.test_article_pipeline import article
from database.warehouse import WarehouseManager
from database.export_gold import export_gold_to_db
from storage.storage_manager import storage
from orchestration.article_pipeline import run_article_pipeline
from spark.batch.article_etl import publish_articles

pytestmark=pytest.mark.integration


def product(identity='fixture-p',currency='VND',known=True):
    row={'product_id':identity,'product_name':'Explicit integration fixture',
         'brand':'Fixture','category':'Phone','price':100,'currency':currency}
    if known:row.update(source_group='simulation',acquisition_method='generated')
    return row


def test_actual_warehouse_legacy_class_cannot_admit_generated_catalog(remote_storage):
    storage.write_silver_parquet('products',pd.DataFrame([product(known=False)]))
    export_gold_to_db(marts=[],silver_tables=['products'],mode='integration')
    storage.write_silver_parquet('products',pd.DataFrame([product('fixture-new')]))
    with pytest.raises(ValueError,match='source'):
        export_gold_to_db(marts=[],silver_tables=['products'],mode='integration')
    wh=WarehouseManager(mode='integration')
    try:assert wh.query('SELECT product_id FROM products')['product_id'].tolist()==['fixture-p']
    finally:wh.close()


@pytest.mark.parametrize('case',['unknown_unit','gold_only'])
def test_actual_warehouse_validates_currency_of_retained_and_new_money(remote_storage,case):
    storage.write_silver_parquet('products',pd.DataFrame([product(currency=None if case=='unknown_unit' else 'USD')]))
    export_gold_to_db(marts=[],silver_tables=['products'],mode='integration')
    if case=='unknown_unit':
        storage.write_silver_parquet('products',pd.DataFrame([product('fixture-new',currency='USD')]))
        kwargs={'marts':[],'silver_tables':['products']}
    else:
        storage.write_gold_parquet('price_daily_stats',pd.DataFrame([{
            'date':'2026-11-01','product_id':'fixture-p','avg_price':1000,
            'currency':'VND','source_group':'simulation'}]))
        kwargs={'marts':['price_daily_stats'],'silver_tables':[]}
    with pytest.raises(ValueError,match='currency'):
        export_gold_to_db(mode='integration',**kwargs)
    wh=WarehouseManager(mode='integration')
    try:
        assert wh.query('SELECT product_id FROM products')['product_id'].tolist()==['fixture-p']
        assert wh.query('SELECT * FROM price_daily_stats').empty
    finally:wh.close()


def test_actual_committed_article_retry_cannot_change_minio_evidence(remote_storage):
    run_id=uuid.uuid4().hex
    first=run_article_pipeline([article()],mode='integration',run_id=run_id)
    prefix=hashlib.sha256(run_id.encode()).hexdigest()
    paths=[first['quarantine_path'],f'article_runs/{prefix}/quality.json']
    before={p:storage.read_json(p) for p in paths}
    with pytest.raises(ValueError,match='already committed'):
        publish_articles([dict(article(),content_sha256='0'*64)],mode='integration',run_id=run_id)
    assert {p:storage.read_json(p) for p in paths}==before
