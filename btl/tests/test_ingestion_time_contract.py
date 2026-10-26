import pytest
from spark.batch.spark_etl_job import SparkBatchETLJob

pytestmark = pytest.mark.spark


@pytest.mark.parametrize('dataset', ['products', 'reviews', 'prices'])
def test_bad_ingestion_time_stays_unknown_with_raw_evidence(spark_session, dataset):
    df = spark_session.createDataFrame([{'ingested_at': 'broken-clock'}])
    row = SparkBatchETLJob(spark=spark_session).standardize_schema(df, dataset).first()
    assert row['ingested_at'] is None
    assert row['ingested_at_raw'] == 'broken-clock'
    assert row['ingestion_time_available'] is False


@pytest.mark.parametrize('dataset', ['products', 'reviews', 'prices'])
def test_missing_ingestion_time_is_explicitly_unavailable(spark_session, dataset):
    df = spark_session.createDataFrame([{'product_id': 'p1'}])
    row = SparkBatchETLJob(spark=spark_session).standardize_schema(df, dataset).first()
    assert row['ingested_at'] is None
    assert row['ingestion_time_available'] is False


@pytest.mark.parametrize(
    ('dataset', 'field', 'raw_field'),
    [('reviews', 'review_date', 'review_date_raw'),
     ('products', 'created_at', 'created_at_raw'),
     ('prices', 'timestamp', 'timestamp_raw')],
)
def test_timestamp_normalization_preserves_raw_value_and_status(spark_session, dataset, field, raw_field):
    df = spark_session.createDataFrame([{field: 'broken-time'}])
    row = SparkBatchETLJob(spark=spark_session).standardize_schema(df, dataset).first()
    assert row[raw_field] == 'broken-time'
    assert row[field] is None
    assert row['normalization_status'] == 'invalid'
