"""Independent article route: no invented ratings or publication times."""
import pytest
from ingestion.web_content import parse_article
from spark.batch.article_etl import prepare_articles, publish_articles
from database.warehouse import WarehouseManager
from storage.storage_manager import storage

pytestmark = pytest.mark.unit


def article(url='https://tinhte.vn/thread/one/', published='2026-11-01T00:00:00Z'):
    row = parse_article('<article><h1>Actual title</h1><p>Observed body</p></article>', url, '2026-11-02T00:00:00Z')
    row['published_at'] = published
    return row


def test_article_quality_counts_and_untimed_rows():
    observed = article()
    unknown = article('https://tinhte.vn/thread/two/', None)
    invalid = article('https://tinhte.vn/thread/bad/', 'not a date')
    silver, gold, quarantine, quality = prepare_articles([observed, dict(observed), unknown, invalid], 'run1')
    assert quality == {'records_received': 4, 'records_valid': 2, 'records_duplicate': 1, 'records_superseded': 0, 'records_quarantined': 1, 'records_missing_publication_time': 1}
    assert set(silver['article_id']) == {observed['article_id'], unknown['article_id']}
    assert len(quarantine) == 1
    assert gold['article_count'].tolist() == [1]
    assert str(gold.iloc[0]['date']) == '2026-11-01'
    assert set(silver['run_id']) == {'run1'}
    assert not {'rating','product_id'}.intersection(silver.columns)


def test_article_publish_replay_replaces_serving_and_preserves_history():
    one = publish_articles([article()], mode='test', run_id='article-run-1')
    two = publish_articles([article()], mode='test', run_id='article-run-2')
    wh = WarehouseManager(mode='test')
    try:
        assert len(wh.query('SELECT * FROM articles')) == 1
        assert wh.query('SELECT run_id FROM articles').iloc[0]['run_id'] == 'article-run-2'
        assert len(wh.query('SELECT * FROM article_pipeline_runs')) == 2
    finally:
        wh.close()
    assert one['lake']['backend'] == 'LocalBackend'
    assert one['lake']['root'] == str(storage.base_dir.resolve())
    assert storage.object_exists(one['silver_path'])
    assert storage.object_exists(two['gold_path'])


def test_article_updates_are_not_reported_as_duplicate_observations():
    old = article()
    new = dict(old, collected_at='2026-11-03T00:00:00Z')
    silver, _, _, quality = prepare_articles([old, new], 'run1')
    assert len(silver) == 1
    assert quality['records_duplicate'] == 0
    assert quality['records_superseded'] == 1


def test_article_schema_is_applied_on_kafka_topic():
    from kafka.consumers.raw_consumer import RawConsumer
    row = article()
    del row['body']
    envelope = {'event_id':'e1','event_type':'NEW_ARTICLE','source':'tinhte.vn','ingested_at':'2026-11-02T00:00:00Z','payload':row}
    valid, errors, _ = RawConsumer().validate_message('raw.articles', envelope)
    assert not valid
    assert any('body' in error for error in errors)


def test_local_article_transport_reads_actual_bronze_payloads():
    from orchestration.article_pipeline import run_article_pipeline
    result = run_article_pipeline([article()], mode='test', run_id='transport-1')
    assert result['transport'] == 'offline'
    assert result['records_delivered'] == 1
    assert result['records_consumed'] == 1
    assert result['bronze_paths']
    assert result['quality']['records_valid'] == 1


def test_articles_page_reads_served_content_and_provenance():
    from pathlib import Path
    from streamlit.testing.v1 import AppTest
    publish_articles([article()], mode='test', run_id='page-1')
    page = AppTest.from_file(str(Path(__file__).resolve().parents[1] / 'dashboard/pages/8_Web_Content.py')).run(timeout=20)
    assert not page.exception
    assert any('tinhte.vn' in frame.value.to_string() for frame in page.dataframe)


def test_article_quality_failure_does_not_replace_previous_snapshot():
    good = article()
    publish_articles([good], mode='test', run_id='good-run')
    invalid = dict(good, published_at='bad time')
    with pytest.raises(ValueError, match='quality'):
        publish_articles([good] + [dict(invalid) for _ in range(10)], mode='test', run_id='bad-run')
    wh = WarehouseManager(mode='test')
    try:
        assert wh.query('SELECT run_id FROM articles')['run_id'].tolist() == ['good-run']
    finally:
        wh.close()


def test_conflicting_article_versions_at_same_time_are_quarantined():
    import hashlib
    first = article()
    other = dict(first, body='Conflicting actual text', content_sha256=hashlib.sha256(b'Conflicting actual text').hexdigest())
    silver, _, quarantine, quality = prepare_articles([first, other], 'conflict')
    assert silver.empty
    assert len(quarantine) == 2
    assert quality['records_superseded'] == 0


def test_serving_preserves_source_metadata_and_legacy_unknowns():
    import pandas as pd
    from database.export_gold import export_gold_to_db
    row = {'product_id':'p','product_name':'Observed','brand':'A','category':'Phone','price':1,
           'source_group':'marketplace','source_name':'lazada','source_record_id':'123',
           'source_url':'https://www.lazada.vn/products/pdp-i123.html','acquisition_method':'live',
           'collected_at':'2026-11-02T00:00:00Z','evidence_sha256':'abc'}
    storage.write_silver_parquet('products', pd.DataFrame([row]))
    export_gold_to_db(marts=[], silver_tables=['products'], mode='test')
    wh = WarehouseManager(mode='test')
    try:
        served = wh.query('SELECT * FROM products').iloc[0]
        for key in ['source_group','source_name','source_record_id','source_url','acquisition_method','collected_at','evidence_sha256']:
            assert served[key] == row[key]
    finally:
        wh.close()


def test_article_publication_offsets_are_normalized_without_losing_source_clock():
    row = article(published='2026-11-02T01:00:00+07:00')
    silver, gold, _, _ = prepare_articles([row], 'offset')
    assert silver.iloc[0]['published_at'] == '2026-11-01T18:00:00.000000Z'
    assert silver.iloc[0]['published_at_raw'] == '2026-11-02T01:00:00+07:00'
    assert str(gold.iloc[0]['date']) == '2026-11-01'


def test_article_date_without_observed_time_does_not_become_midnight():
    row = article(published='2026-11-02')
    silver, gold, _, quality = prepare_articles([row], 'day-only')
    assert silver.iloc[0]['published_at'] is None
    assert silver.iloc[0]['published_at_raw'] == '2026-11-02'
    assert silver.iloc[0]['publication_time_precision'] == 'date'
    assert gold.empty
    assert quality['records_missing_publication_time'] == 1


def test_article_source_evidence_survives_silver_and_sql_serving():
    row=article()
    metadata={'source_record_id':row['source_url'],'evidence_ref':'private-http-evidence',
              'parser_version':'article-fixture-v2','schema_version':'1.0','collection_run_id':'original-collection',
              'original_acquisition_method':'live'}
    row.update(metadata)
    result=publish_articles([row],mode='test',run_id='source-metadata')
    silver=storage.read_parquet(result['silver_path']).iloc[0]
    wh=WarehouseManager(mode='test')
    try:
        observed=wh.query('SELECT * FROM articles').iloc[0]
        for key,value in metadata.items():
            assert silver.get(key) == value
            assert observed.get(key) == value
    finally:
        wh.close()


def test_commerce_evidence_reference_is_not_silently_dropped_by_export():
    import pandas as pd
    from database.export_gold import export_gold_to_db
    row={'product_id':'p','product_name':'Fixture','brand':'Fixture','category':'Phone','price':1,'currency':'VND',
         'source_group':'simulation','acquisition_method':'generated','evidence_ref':'private-fixture-reference'}
    storage.write_silver_parquet('products',pd.DataFrame([row]))
    export_gold_to_db(marts=[],silver_tables=['products'],mode='test')
    wh=WarehouseManager(mode='test')
    try:
        assert wh.query('SELECT * FROM products').iloc[0].get('evidence_ref') == 'private-fixture-reference'
    finally:
        wh.close()

@pytest.mark.parametrize('invalid_retry',[False,True])
def test_rejected_committed_article_run_preserves_all_original_evidence(invalid_retry):
    import hashlib
    run_id='immutable-committed'
    manifest=publish_articles([article()],mode='test',run_id=run_id)
    prefix=hashlib.sha256(run_id.encode()).hexdigest()
    paths=[manifest['quarantine_path'],f'article_runs/{prefix}/quality.json']
    before={path:storage.read_json(path) for path in paths}
    retry=article('https://tinhte.vn/thread/different/')
    if invalid_retry:
        retry['content_sha256']='0'*64
    with pytest.raises(ValueError,match='already committed'):
        publish_articles([retry] if invalid_retry else [retry,article('https://tinhte.vn/thread/third/')],mode='test',run_id=run_id)
    assert {path:storage.read_json(path) for path in paths}==before
    wh=WarehouseManager(mode='test')
    try:
        assert wh.query('SELECT article_id FROM articles')['article_id'].tolist()==[article()['article_id']]
    finally:wh.close()

def test_failed_article_attempts_keep_separate_durable_evidence():
    row=dict(article(),content_sha256='0'*64)
    for count in [1,2]:
        with pytest.raises(ValueError,match='quality'):
            publish_articles([row]*count,mode='test',run_id='failed-run')
    paths=[p for p in storage.list_objects('article_attempts/') if p.endswith('/quality.json')]
    assert len(paths)==2
    assert {storage.read_json(path)['records_quarantined'] for path in paths}=={1,2}

def test_same_article_run_concurrent_attempts_cannot_replace_committed_evidence(monkeypatch):
    import threading
    from concurrent.futures import ThreadPoolExecutor, TimeoutError
    from spark.batch import article_etl
    entered=threading.Event()
    release=threading.Event()
    prepare=article_etl.prepare_articles
    first=article('https://tinhte.vn/thread/first-writer/')
    second=article('https://tinhte.vn/thread/second-writer/')
    def controlled(records,run_id):
        if records[0]['article_id']==first['article_id']:
            entered.set()
            assert release.wait(5), 'First writer was not released'
        return prepare(records,run_id)
    monkeypatch.setattr(article_etl,'prepare_articles',controlled)
    with ThreadPoolExecutor(max_workers=2) as pool:
        one=pool.submit(publish_articles,[first],mode='test',run_id='same-concurrent-run')
        assert entered.wait(3)
        two=pool.submit(publish_articles,[second],mode='test',run_id='same-concurrent-run')
        try:
            try:two.result(timeout=1)
            except TimeoutError:pass
        finally:release.set()
        one.result(timeout=5)
        with pytest.raises(ValueError,match='already committed'):
            two.result(timeout=5)
    wh=WarehouseManager(mode='test')
    try:assert wh.query('SELECT article_id FROM articles')['article_id'].tolist()==[first['article_id']]
    finally:wh.close()
