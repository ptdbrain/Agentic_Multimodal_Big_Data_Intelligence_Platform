"""Raw evidence supplied by one source must not erase another source's value."""
import pytest
from spark.batch.spark_etl_job import SparkBatchETLJob

pytestmark = [pytest.mark.unit, pytest.mark.spark]


@pytest.mark.parametrize('dataset,field', [('products','price'), ('prices','price'), ('reviews','rating')])
def test_partial_raw_metadata_does_not_null_valid_source_values(spark_session, dataset, field):
    frame = spark_session.createDataFrame([('one', '4', 4.0), ('two', None, 5.0)],
        f'product_id string, {field}_raw string, {field} double')
    normalized = SparkBatchETLJob(spark_session).standardize_schema(frame, dataset)
    rows = {row['product_id']: row for row in normalized.select('product_id', field, field+'_raw').collect()}
    assert rows['one'][field] == 4.0
    assert rows['one'][field+'_raw'] == '4'
    assert rows['two'][field] == 5.0
    assert rows['two'][field+'_raw'] == '5.0'
