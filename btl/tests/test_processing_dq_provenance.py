import json

import pandas as pd
import pytest

from spark.quality.spark_dq import SparkDataQualityEvaluator


@pytest.mark.spark
def test_bronze_exclusions_and_all_normalization_errors_survive_serving(spark_session):
    from spark.batch.spark_etl_job import SparkBatchETLJob
    from spark.analytics.gold_builder import SparkGoldBuilder
    from storage.storage_manager import storage
    from database.export_gold import export_gold_to_db
    from dashboard.db_connector import DashboardDB

    raw = [
        {'review_id': 'r1', 'product_id': 'p1', 'rating': '5', 'review_text': 'ok', 'review_date': '2026-10-01'},
        {'review_id': 'r2', 'product_id': 'p1', 'rating': '4', 'review_text': 'untimed'},
        {'review_id': 'r3', 'product_id': 'p1', 'rating': '6', 'review_text': 'bad rating', 'review_date': '2026-10-01'},
        {'review_id': 'r4', 'product_id': 'p1', 'rating': '-1', 'review_text': 'two errors', 'review_date': 'invalid'},
        {'review_id': 'r5', 'product_id': 'p1', 'rating': '5', 'review_text': 'bad ingestion clock',
         'review_date': '2026-10-01', 'ingested_at': 'invalid'},
    ]
    result = SparkBatchETLJob(spark_session).process_dataset('reviews', raw)
    assert result['valid_count'] == 2
    assert result['quarantine_count'] == 3
    assert result['records_missing_observation_time'] == 2
    assert result['untimed_silver_count'] == 1
    assert result['normalization_error_counts'] == {'INVALID_RATING': 2, 'INVALID_REVIEW_DATE': 1, 'INVALID_INGESTED_AT': 1}
    assert result['quarantine_reason_counts'] == {
        'INVALID_RATING_OUT_OF_RANGE': 1, 'INVALID_TYPE_TIMESTAMP': 1, 'NORMALIZATION_ERROR': 1,
    }
    daily = SparkGoldBuilder(spark_session).build_product_daily_stats(result['silver_df'], [])
    assert daily.count() == 1
    assert daily.first()['review_count'] == 1

    report = SparkDataQualityEvaluator.generate_report(dict(result, run_id='dq-observed-run'))
    storage.write_gold_parquet('data_quality_metrics', pd.DataFrame([report]))
    export_gold_to_db(marts=['data_quality_metrics'], silver_tables=[], mode='test')
    db = DashboardDB(mode='test')
    try:
        served = db.get_data_quality_metrics().iloc[0]
        assert served['run_id'] == 'dq-observed-run'
        assert served['records_received'] == 5
        assert served['records_valid'] == 2
        assert served['records_invalid'] == 3
        assert served['records_quarantined'] == 3
        assert served['records_missing_observation_time'] == 2
        assert served['untimed_silver_count'] == 1
        assert json.loads(served['normalization_error_counts']) == {
            'INVALID_RATING': 2, 'INVALID_REVIEW_DATE': 1, 'INVALID_INGESTED_AT': 1,
        }
        assert sum(json.loads(served['quarantine_reason_counts']).values()) == 3
    finally:
        db.close()


@pytest.mark.unit
def test_legacy_quality_report_does_not_invent_exclusion_or_quarantine_counts():
    report = SparkDataQualityEvaluator.generate_report({'dataset_name': 'reviews', 'total_count': 10})
    assert report['records_quarantined'] is None
    assert report['records_missing_observation_time'] is None
    assert report['untimed_silver_count'] is None
    assert report['normalization_error_counts'] is None
    assert report['quarantine_reason_counts'] is None


@pytest.mark.unit
def test_dq_page_shows_persisted_bronze_quality_instead_of_only_rechecking_silver():
    from pathlib import Path
    from streamlit.testing.v1 import AppTest
    from database.warehouse import WarehouseManager

    wh = WarehouseManager(mode='test')
    wh.initialize_schema()
    wh.upsert_dataframe('data_quality_metrics', pd.DataFrame([{
        'batch_id': 'report-1', 'run_id': 'pipeline-1', 'dataset_name': 'reviews',
        'records_received': 5, 'records_valid': 2, 'records_invalid': 3,
        'records_duplicate': 0, 'records_missing': 0, 'dq_score': 40.0,
        'records_quarantined': 3, 'records_missing_observation_time': 2, 'untimed_silver_count': 1,
        'normalization_error_counts': '{"INVALID_RATING":2}',
    }]), ['batch_id'])
    wh.close()
    app = AppTest.from_file(str(Path(__file__).resolve().parents[1] / 'dashboard/pages/7_Data_Quality.py')).run(timeout=20)
    assert not app.exception
    reports = next(frame.value for frame in app.dataframe if 'records_quarantined' in frame.value)
    assert reports.iloc[0]['records_received'] == 5
    assert reports.iloc[0]['records_quarantined'] == 3
    assert reports.iloc[0]['run_id'] == 'pipeline-1'
