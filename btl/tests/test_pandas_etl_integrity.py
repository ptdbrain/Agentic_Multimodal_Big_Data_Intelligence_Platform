import pytest

from spark.batch.batch_etl_job import BatchETLJob
from spark.etl.cleaner import DataCleaner
from spark.etl.price_pipeline import PricePipeline

pytestmark = pytest.mark.unit


def test_pandas_etl_accounts_for_every_input_before_validation_and_never_clamps_rating():
    result = BatchETLJob.run_pipeline([
        {'product_id': 'p1', 'product_name': 'Phone', 'price': '19.990.000 VND', 'brand': 'A', 'category': 'Phone'},
        {'product_id': 'p2', 'product_name': 'Broken price', 'price': 'Infinity', 'brand': 'A', 'category': 'Phone'},
        {'product_id': 'p3', 'price': '10', 'brand': 'A', 'category': 'Phone'},
    ], [
        {'review_id': 'r1', 'product_id': 'p1', 'rating': '4', 'review_text': ' ok ', 'review_date': '2026-10-01'},
        {'review_id': 'r2', 'product_id': 'p1', 'rating': '7', 'review_text': 'rating too high'},
        {'review_id': 'r3', 'product_id': 'p1', 'rating': '5', 'review_text': None},
        {'review_id': 'r4', 'product_id': 'p1', 'rating': '5', 'review_text': 'bad date', 'review_date': 'invalid'},
    ], save_silver=False)
    for key, received, valid, invalid in [('products_dq', 3, 1, 2), ('reviews_dq', 4, 1, 3)]:
        assert result[key]['records_received'] == received
        assert result[key]['records_valid'] == valid
        assert result[key]['records_invalid'] == invalid
    assert result['products_dq']['valid_records'][0]['price'] == 19990000.0
    assert result['reviews_dq']['valid_records'][0]['rating'] == 4.0
    bad_rating = next(r for r in result['reviews_dq']['invalid_records'] if r['review_id'] == 'r2')
    assert bad_rating['rating'] is None
    assert bad_rating['rating_raw'] == '7'


@pytest.mark.parametrize('rating', [6, -1, float('inf'), float('nan')])
def test_review_cleaner_cannot_fabricate_a_bounded_rating(rating):
    assert DataCleaner.clean_reviews([{'review_id': 'r1', 'review_text': 'text', 'rating': rating}]) == []


def test_product_cleaner_does_not_fabricate_zero_price():
    row = DataCleaner.clean_products([{'product_id': 'p1', 'product_name': 'Phone'}])[0]
    assert row['price'] is None
    assert row['price_raw'] is None
    assert row['normalization_errors'] == ['MISSING_PRICE']


def test_price_pipeline_reports_bad_time_and_nonfinite_price_without_dropping_evidence():
    result = PricePipeline.run_pipeline([
        {'price_id': 'pr1', 'product_id': 'p1', 'price': '1.299,50 €', 'timestamp': '2026-10-01', 'currency': 'EUR'},
        {'price_id': 'pr2', 'product_id': 'p1', 'price': float('inf'), 'timestamp': '2026-10-01'},
        {'price_id': 'pr3', 'product_id': 'p1', 'price': '10', 'timestamp': 'invalid'},
    ], save_silver=False)
    assert result['processed'] == 3
    assert result['valid'] == 1
    assert result['invalid'] == 2
    assert result['silver_records'][0]['price'] == 1299.5
    assert result['silver_records'][0]['price_raw'] == '1.299,50 €'
    assert {r['price_id'] for r in result['invalid_records']} == {'pr2', 'pr3'}
