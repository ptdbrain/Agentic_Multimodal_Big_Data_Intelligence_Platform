"""Processing contracts on actual PostgreSQL/MinIO; outages fail, never skip.

Each test uses a private PostgreSQL schema. Object-store tests additionally use
a new bucket; cleanup targets only those generated resources.
"""
import json
import os
import uuid

import numpy as np
import pandas as pd
import pytest

from config.settings import settings
from dashboard.db_connector import DashboardDB
from database.export_gold import export_gold_to_db
from database.warehouse import WarehouseManager
from storage.storage_manager import storage

pytestmark = pytest.mark.integration


def test_gold_acceptance_verifier_does_not_seed_unverified_sample_data(remote_storage):
    from scripts.verify_gold_postgres_dashboard import test_gold_generation_and_postgres_sync

    with pytest.raises(RuntimeError, match='Silver snapshot'):
        test_gold_generation_and_postgres_sync()
    assert not list(storage.backend.client.list_objects(remote_storage, recursive=True))


def test_pipeline_drains_partial_real_kafka_batches_before_verifying_delivery(remote_storage, monkeypatch):
    from kafka.consumers.raw_consumer import RawConsumer
    from scripts.run_e2e_pipeline import stage_2_kafka_produce_and_consume

    prefix = 'processing-' + uuid.uuid4().hex
    for name in ['products', 'reviews', 'prices', 'events']:
        monkeypatch.setattr(settings.kafka, 'topic_' + name, prefix + '-' + name)
    original = RawConsumer.consume_batch_once

    def bounded_poll(self, *args, **kwargs):
        # Exercise the real broker, offsets and Bronze writes, with a deterministic
        # partial fetch instead of depending on host speed to exhaust a deadline.
        kwargs['max_records'] = 2
        return original(self, *args, **kwargs)

    monkeypatch.setattr(RawConsumer, 'consume_batch_once', bounded_poll)
    products = [{'product_id':'p1','product_name':'Synthetic fixture','brand':'Fixture',
                 'category':'Fixture','price':100,'currency':'VND','source':'synthetic_test'}]
    reviews = [{'review_id':f'r{i}','product_id':'p1','user_id':'anon_redacted','rating':4,
                'review_title':'Synthetic fixture','review_text':'Partial poll fixture',
                'review_date':'2026-10-01T00:00:00Z'} for i in range(3)]
    result = stage_2_kafka_produce_and_consume(products, reviews, [], effective_mode='integration')
    assert result['records_consumed'] == result['records_valid'] == 4
    assert result['records_dlq'] == 0
    assert result['topic_counts'] == {settings.kafka.topic_products:1, settings.kafka.topic_reviews:3}
    assert len(result['message_latencies']) == 4
    assert len(result['validation_latencies_ms']) == len(result['latencies_ms']) <= 4
    assert result['committed_offsets']
    assert sorted(result['batches_written']) == sorted(
        path for paths in result['batches_by_topic'].values() for path in paths
    )
    stored = [row for path in result['batches_by_topic'][settings.kafka.topic_reviews]
              for row in storage.read_json(path)]
    assert sorted(row['payload']['review_id'] for row in stored) == ['r0', 'r1', 'r2']


@pytest.fixture
def postgres_scope(monkeypatch, isolated_local_runtime):
    schema = 'processing_' + uuid.uuid4().hex
    admin = WarehouseManager(mode='integration')
    try:
        admin.execute(f'CREATE SCHEMA "{schema}"')
        options = os.environ.get('PGOPTIONS', '')
        monkeypatch.setenv('PGOPTIONS', options + f' -c search_path={schema} -c lock_timeout=5000 -c statement_timeout=20000')
        yield schema
    finally:
        try:
            admin.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        finally:
            admin.close()


@pytest.fixture
def remote_storage(postgres_scope, monkeypatch):
    from minio.deleteobjects import DeleteObject
    bucket = 'processing-' + uuid.uuid4().hex
    monkeypatch.setattr(settings.storage, 'data_lake_bucket', bucket)
    storage.set_mode('integration')
    backend = storage.backend
    assert backend.health_check()
    try:
        yield bucket
    finally:
        errors = list(backend.client.remove_objects(
            bucket, (DeleteObject(item.object_name) for item in backend.client.list_objects(bucket, recursive=True)),
        ))
        assert not errors, 'Cannot clean isolated processing bucket'
        backend.client.remove_bucket(bucket)


def test_postgres_additive_migrations_preserve_legacy_rows_and_unknowns(postgres_scope):
    wh = WarehouseManager(mode='integration')
    try:
        wh.execute('''CREATE TABLE product_daily_stats(
            date DATE NOT NULL,product_id TEXT NOT NULL,review_count INTEGER,avg_rating NUMERIC,
            PRIMARY KEY(date,product_id));
            INSERT INTO product_daily_stats VALUES('2026-01-01','legacy',5,4.2);
            CREATE TABLE reviews(review_id TEXT PRIMARY KEY,product_id TEXT,rating NUMERIC,review_date DATE);
            INSERT INTO reviews VALUES('legacy','legacy',4,'2026-01-01');
            CREATE TABLE anomaly_events(event_id TEXT PRIMARY KEY,entity_type TEXT,entity_id TEXT,
                anomaly_type TEXT,score NUMERIC,timestamp TIMESTAMP);
            INSERT INTO anomaly_events VALUES('legacy','PRODUCT','legacy','RATING_DROP',1,'2026-01-01');
            CREATE TABLE data_quality_metrics(batch_id TEXT PRIMARY KEY,dataset_name TEXT,
                records_received INTEGER,records_valid INTEGER,records_invalid INTEGER,
                records_duplicate INTEGER,records_missing INTEGER,dq_score NUMERIC,
                processing_time_ms INTEGER NOT NULL,timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP);
            INSERT INTO data_quality_metrics(batch_id,dataset_name,records_received,records_valid,
                records_invalid,records_duplicate,records_missing,dq_score,processing_time_ms)
                VALUES('legacy','reviews',5,5,0,0,0,100,125);''')
        wh.initialize_schema()
        wh.initialize_schema()
        daily = wh.query("SELECT * FROM product_daily_stats WHERE product_id='legacy'").iloc[0]
        assert daily['review_count'] == 5 and daily['avg_rating'] == 4.2
        assert all(pd.isna(daily[key]) for key in ['run_id','source_run_id','rating_sample_size','time_available'])
        review = wh.query("SELECT * FROM reviews WHERE review_id='legacy'").iloc[0]
        assert review['rating'] == 4
        assert all(pd.isna(review[key]) for key in ['rating_raw','normalization_status','normalization_errors','time_available'])
        incident = wh.query("SELECT * FROM anomaly_events WHERE event_id='legacy'").iloc[0]
        assert all(pd.isna(incident[key]) for key in ['run_id','baseline_sample_size','baseline_start','data_quality_status'])
        quality = wh.query("SELECT * FROM data_quality_metrics WHERE batch_id='legacy'").iloc[0]
        assert quality['processing_time_ms'] == 125
        assert pd.isna(quality['records_quarantined']) and pd.isna(quality['run_id'])
        nullable = wh.query("SELECT is_nullable FROM information_schema.columns WHERE table_schema=current_schema() AND table_name='data_quality_metrics' AND column_name='processing_time_ms'")
        assert nullable.iloc[0]['is_nullable'] == 'YES'
    finally:
        wh.close()


def test_postgres_initialized_schema_does_not_block_an_active_read_snapshot(postgres_scope):
    _seed_daily()
    reader = DashboardDB(mode='integration')
    writer = WarehouseManager(mode='integration')
    try:
        assert reader.get_product_daily_stats()['run_id'].tolist() == ['old']
        # Export initializes its schema on every invocation. An already migrated
        # schema must not take an ALTER TABLE lock against a serving reader.
        writer.initialize_schema()
        with writer.transaction():
            writer.upsert_dataframe('product_daily_stats',_daily('product_daily_stats','new'),['date','product_id'])
            writer.upsert_dataframe('brand_daily_stats',_daily('brand_daily_stats','new'),['date','brand'])
        assert reader.get_brand_daily_stats()['run_id'].tolist() == ['old']
    finally:
        writer.close()
        reader.close()
    fresh = DashboardDB(mode='integration')
    try:
        assert fresh.get_product_daily_stats()['run_id'].tolist() == ['new']
        assert fresh.get_brand_daily_stats()['run_id'].tolist() == ['new']
    finally:
        fresh.close()


def test_minio_parquet_metadata_arrays_survive_postgres_export_replay(remote_storage):
    storage.write_silver_parquet('products', pd.DataFrame([{
        'product_id':'p1','product_name':'Observed phone','brand':'A','category':'Phone','price':10.0,
        'price_raw':' 10 VND ','normalization_status':'valid','normalization_errors':np.array([],dtype=str),
        'time_available':False,'ingestion_time_available':True,'ingested_at':'2026-10-01',
        'ingested_at_raw':'2026-10-01','created_at_raw':None,
    }]))
    storage.write_silver_parquet('reviews', pd.DataFrame([{
        'review_id':'r1','product_id':'p1','rating':5.0,'rating_raw':' 5 ','review_text':'Actual test record',
        'normalization_status':'valid','normalization_errors':np.array([],dtype=str),
        'time_available':False,'ingestion_time_available':True,'review_date_raw':None,
    }]))
    for _ in range(2):
        export_gold_to_db(marts=[],silver_tables=['products','reviews'],mode='integration')
    db = DashboardDB(mode='integration')
    try:
        product = db.query('SELECT * FROM products').iloc[0]
        assert product['price_raw'] == ' 10 VND '
        assert json.loads(product['normalization_errors']) == []
        assert not product['time_available'] and product['ingestion_time_available']
        reviews = db.query('SELECT * FROM reviews')
        assert len(reviews) == 1
        assert reviews.iloc[0]['rating_raw'] == ' 5 '
        assert json.loads(reviews.iloc[0]['normalization_errors']) == []
    finally:
        db.close()


def _daily(name, run, count=3):
    dimension = 'brand' if name == 'brand_daily_stats' else 'product_id'
    return pd.DataFrame([{'date':'2026-10-01',dimension:'entity','review_count':count,
                          'rating_sample_size':count,'avg_rating':4.0,'run_id':run}])


def _seed_daily():
    wh = WarehouseManager(mode='integration')
    try:
        wh.initialize_schema()
        wh.upsert_dataframe('product_daily_stats', _daily('product_daily_stats','old',1), ['date','product_id'])
        wh.upsert_dataframe('brand_daily_stats', _daily('brand_daily_stats','old',1), ['date','brand'])
    finally:
        wh.close()


def _write_candidate():
    for name in ['product_daily_stats','brand_daily_stats']:
        storage.write_gold_parquet(name,_daily(name,'new'))


def test_postgres_real_constraint_failure_rolls_back_all_mart_writes(remote_storage):
    _seed_daily(); _write_candidate()
    wh = WarehouseManager(mode='integration')
    try:
        wh.execute("ALTER TABLE brand_daily_stats ADD CONSTRAINT reject_candidate CHECK(review_count < 3)")
    finally:
        wh.close()
    with pytest.raises(RuntimeError,match='brand_daily_stats'):
        export_gold_to_db(marts=['product_daily_stats','brand_daily_stats'],silver_tables=[],mode='integration')
    db = DashboardDB(mode='integration')
    try:
        for name in ['product_daily_stats','brand_daily_stats']:
            row = db.query(f'SELECT run_id,review_count FROM {name}').iloc[0]
            assert row['run_id'] == 'old' and row['review_count'] == 1
    finally:
        db.close()


def test_postgres_reader_retains_one_snapshot_across_real_export_commit(remote_storage):
    _seed_daily(); _write_candidate()
    reader = DashboardDB(mode='integration')
    try:
        assert reader.get_product_daily_stats()['run_id'].tolist() == ['old']
        export_gold_to_db(marts=['product_daily_stats','brand_daily_stats'],silver_tables=[],mode='integration')
        assert reader.get_brand_daily_stats()['run_id'].tolist() == ['old']
    finally:
        reader.close()
    fresh = DashboardDB(mode='integration')
    try:
        assert fresh.get_product_daily_stats()['run_id'].tolist() == ['new']
        assert fresh.get_brand_daily_stats()['run_id'].tolist() == ['new']
    finally:
        fresh.close()


def test_postgres_empty_snapshot_clears_daily_rows_and_keeps_incident_history(remote_storage):
    _seed_daily()
    wh = WarehouseManager(mode='integration')
    try:
        wh.upsert_dataframe('anomaly_events',pd.DataFrame([{
            'event_id':'history','entity_type':'PRODUCT','entity_id':'entity','anomaly_type':'RATING_DROP',
            'score':1.0,'timestamp':'2026-10-01','run_id':'old',
        }]),['event_id'])
    finally:
        wh.close()
    storage.write_gold_parquet('product_daily_stats',_daily('product_daily_stats','new').iloc[:0],allow_empty=True)
    export_gold_to_db(marts=['product_daily_stats'],silver_tables=[],mode='integration')
    db = DashboardDB(mode='integration')
    try:
        assert db.get_product_daily_stats().empty
        assert db.get_anomaly_events()['event_id'].tolist() == ['history']
    finally:
        db.close()


def test_postgres_anomaly_baseline_and_dq_categories_round_trip(remote_storage):
    from analytics.anomaly.rating_anomaly import RatingAnomalyDetector
    from spark.quality.spark_dq import SparkDataQualityEvaluator as DQ
    reviews = pd.DataFrame([
        {'review_id':f'{day}-{i}','product_id':'p1','rating':rating,'review_date':day}
        for day,rating,n in [('2026-10-01',5,4),('2026-10-02',5,6),('2026-10-03',1,3)] for i in range(n)
    ])
    events = RatingAnomalyDetector.detect_rating_drops(reviews,run_id='run-real',data_quality_status='valid')
    assert len(events) == 1
    storage.write_gold_parquet('anomaly_events',pd.DataFrame(events))
    report = DQ.generate_report(dict(dataset_name='reviews',run_id='run-real',records_received=5,
        records_valid=2,records_invalid=3,records_duplicate=0,records_missing=0,dq_score=40,
        records_quarantined=3,records_missing_observation_time=2,untimed_silver_count=1,
        normalization_error_counts={'INVALID_RATING':2,'INVALID_REVIEW_DATE':1}))
    storage.write_gold_parquet('data_quality_metrics',pd.DataFrame([report]))
    for _ in range(2):
        export_gold_to_db(marts=['anomaly_events','data_quality_metrics'],silver_tables=[],mode='integration')
    db = DashboardDB(mode='integration')
    try:
        incidents = db.get_anomaly_events()
        assert len(incidents) == 1
        event = incidents.iloc[0]
        assert event['run_id'] == 'run-real' and event['data_quality_status'] == 'valid'
        assert event['baseline_sample_size'] == 10 and event['current_sample_size'] == 3
        assert event['baseline_sample_unit'] == 'reviews' and event['baseline_method'] == 'past_calendar_window'
        quality = db.get_data_quality_metrics().iloc[0]
        assert quality['records_quarantined'] == 3 and quality['untimed_silver_count'] == 1
        assert json.loads(quality['normalization_error_counts']) == {'INVALID_RATING':2,'INVALID_REVIEW_DATE':1}
    finally:
        db.close()
