import pandas as pd
import pytest

from spark.quality.spark_dq import SparkDataQualityEvaluator as DQ

RECORDS = {
    'reviews': {'review_id': 'r1', 'product_id': 'p1', 'rating': 4.0, 'review_text': 'text'},
    'products': {'product_id': 'p1', 'product_name': 'Phone', 'price': 10.0},
    'prices': {'price_id': 'pr1', 'product_id': 'p1', 'price': 10.0},
}


@pytest.fixture(params=['pandas', pytest.param('spark', marks=pytest.mark.spark)])
def evaluate(request):
    def run(record, dataset):
        data = pd.DataFrame([record])
        if request.param == 'spark':
            data = request.getfixturevalue('spark_session').createDataFrame(data)
        return DQ.evaluate(data, dataset)
    return run


@pytest.mark.parametrize('dataset,missing', [
    (dataset, missing) for dataset, record in RECORDS.items() for missing in record
])
def test_required_column_absent_from_entire_schema_cannot_be_ignored(evaluate, dataset, missing):
    record = dict(RECORDS[dataset])
    del record[missing]
    metrics = evaluate(record, dataset)
    assert metrics['total_count'] == metrics['invalid_count'] == metrics['quarantine_count'] == 1
    assert metrics['valid_count'] == 0
    assert metrics['null_row_count'] == 1
    assert metrics['field_null_counts'][missing] == 1


@pytest.mark.parametrize('dataset', ['products', 'prices'])
def test_positive_infinity_is_not_a_valid_price_for_any_quality_engine(evaluate, dataset):
    metrics = evaluate(dict(RECORDS[dataset], price=float('inf')), dataset)
    assert metrics['valid_count'] == 0
    assert metrics['range_error_count'] == metrics['quarantine_count'] == 1


def test_normalized_invalid_status_cannot_be_overridden_by_optional_timestamp(evaluate):
    metrics = evaluate(dict(RECORDS['reviews'], normalization_status='invalid',
                            normalization_errors=['INVALID_REVIEW_DATE']), 'reviews')
    assert metrics['valid_count'] == 0
    assert metrics['quarantine_count'] == 1


@pytest.mark.spark
@pytest.mark.parametrize('dataset,missing', [('reviews', 'review_text'), ('products', 'product_name'), ('prices', 'price_id')])
def test_batch_routes_entire_schema_missing_a_required_field_to_quarantine(spark_session, dataset, missing):
    from spark.batch.spark_etl_job import SparkBatchETLJob
    record = dict(RECORDS[dataset])
    del record[missing]
    result = SparkBatchETLJob(spark_session).process_dataset(dataset, [record])
    assert result['input_count'] == result['quarantine_count'] == result['null_row_count'] == 1
    assert result['silver_written_count'] == 0
    assert result['field_null_counts'][missing] == 1
    assert result['reconciled']


@pytest.mark.unit
def test_served_json_error_array_does_not_invalidate_valid_records():
    from spark.quality.metrics import DataQualityEvaluator
    from spark.quality.rules import REVIEW_QUALITY_RULES
    result = DataQualityEvaluator.evaluate_batch('reviews', [
        dict(RECORDS['reviews'], normalization_status='valid', normalization_errors='[]'),
    ], REVIEW_QUALITY_RULES)
    assert result['records_valid'] == 1
    assert result['error_breakdown'] == {}


@pytest.mark.unit
@pytest.mark.parametrize('allow_zero', [False, True])
def test_legacy_positive_number_rule_cannot_authorize_infinite_price(allow_zero):
    from spark.quality.metrics import DataQualityEvaluator
    from spark.quality.rules import PositiveNumberRule
    rule = PositiveNumberRule('price', allow_zero=allow_zero)
    result = DataQualityEvaluator.evaluate_batch('products', [
        {'product_id': 'invalid', 'price': float('inf')}, {'product_id': 'valid', 'price': 10.0},
    ], [rule], primary_key='product_id')
    assert result['records_received'] == 2
    assert result['records_valid'] == result['records_invalid'] == 1
    assert result['valid_records'][0]['product_id'] == 'valid'
