"""Actual Kafka/MinIO/PostgreSQL article route, labelled synthetic test input."""
import uuid
import pytest
from tests.integration.test_processing_contracts import postgres_scope, remote_storage
from tests.test_article_pipeline import article
from orchestration.article_pipeline import run_article_pipeline
from database.warehouse import WarehouseManager

pytestmark = pytest.mark.integration


def test_article_two_distinct_batches_use_actual_transport_and_replace_serving(remote_storage):
    first = run_article_pipeline([article('https://tinhte.vn/thread/fixture-one/')], mode='integration', run_id=str(uuid.uuid4()))
    second_input = article('https://tinhte.vn/thread/fixture-two/')
    second = run_article_pipeline([second_input], mode='integration', run_id=str(uuid.uuid4()))
    assert first['transport'] == second['transport'] == 'kafka'
    assert second['records_consumed'] == 1
    wh = WarehouseManager(mode='integration')
    try:
        rows = wh.query('SELECT * FROM articles')
        assert rows['article_id'].tolist() == [second_input['article_id']]
        assert len(wh.query('SELECT * FROM article_pipeline_runs')) == 2
    finally:
        wh.close()


def test_article_source_offset_survives_actual_postgres_serving(remote_storage):
    import pandas as pd
    record = article('https://tinhte.vn/thread/offset-fixture/', published='2026-11-02T01:00:00+07:00')
    result = run_article_pipeline([record], mode='integration', run_id=str(uuid.uuid4()))
    assert result['lake']['bucket'] == remote_storage
    wh = WarehouseManager(mode='integration')
    try:
        row = wh.query('SELECT * FROM articles').iloc[0]
        assert row['published_at_raw'] == '2026-11-02T01:00:00+07:00'
        assert pd.Timestamp(row['published_at']).day == 1
        assert pd.Timestamp(row['published_at']).hour == 18
        assert str(wh.query('SELECT date FROM article_daily_stats').iloc[0]['date']) == '2026-11-01'
    finally:
        wh.close()
