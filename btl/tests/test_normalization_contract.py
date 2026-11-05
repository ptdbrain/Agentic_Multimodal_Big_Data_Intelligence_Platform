import datetime

import pytest

from spark.etl.normalizer import EntityNormalizer


@pytest.fixture(params=['pandas', pytest.param('spark', marks=pytest.mark.spark)])
def normalize(request):
    def run(record, dataset):
        if request.param == 'pandas':
            if dataset == 'products':
                return EntityNormalizer.normalize_product(record)
            return EntityNormalizer.normalize_record(record, dataset)
        from spark.batch.spark_etl_job import SparkBatchETLJob
        spark = request.getfixturevalue('spark_session')
        frame = spark.createDataFrame([record])
        result = SparkBatchETLJob(spark).standardize_schema(frame, dataset)
        # Inspect SQL timestamps in the configured UTC session. collect() converts
        # TimestampType to a naive datetime in the Python host's local timezone.
        from pyspark.sql import functions as F
        result = result.select(*[
            F.date_format(F.col(name), "yyyy-MM-dd'T'HH:mm:ss'Z'").alias(name) if dtype == 'timestamp' else F.col(name)
            for name, dtype in result.dtypes
        ])
        return result.first().asDict()
    return run


@pytest.mark.parametrize(('dataset', 'field', 'raw', 'time_field'), [
    ('reviews', 'rating', ' 6 ', 'review_date'),
    ('products', 'price', ' Infinity ', 'created_at'),
    ('prices', 'price', ' -10 ', 'timestamp'),
])
def test_invalid_values_stay_null_with_all_error_codes_and_original_evidence(normalize, dataset, field, raw, time_field):
    row = normalize({field: raw, time_field: ' broken-time ', 'ingested_at': '2026-10-01'}, dataset)
    assert row[field] is None
    assert row[time_field] is None
    assert row[f'{field}_raw'] == raw
    assert row[f'{time_field}_raw'] == ' broken-time '
    assert set(row['normalization_errors']) == {f'INVALID_{field.upper()}', f'INVALID_{time_field.upper()}'}
    assert row['normalization_status'] == 'invalid'
    assert not row['time_available']
    assert row['ingestion_time_available']


@pytest.mark.parametrize(('raw', 'want'), [
    ('19.990.000 VND', 19990000.0), ('$1,299.99', 1299.99), ('1.299,50 €', 1299.5), (25.5, 25.5),
    (25000000.0, 25000000.0), ('2.5e7', 25000000.0),
    (25.555, 25.555), ('1000000.000', 1000000.0),
])
def test_valid_price_formats_have_same_value_and_utc_time(normalize, raw, want):
    row = normalize({'price': raw, 'created_at': '2026-10-02T00:30:00+07:00'}, 'products')
    assert row['price'] == want
    if isinstance(raw, str):
        assert row['price_raw'] == raw
    else:
        assert float(row['price_raw']) == raw  # Spark's numeric string representation may use exponent notation.
    assert row['normalization_errors'] == []
    assert row['normalization_status'] == 'valid'
    assert row['time_available']
    parsed = row['created_at']
    if isinstance(parsed, str):
        parsed = datetime.datetime.fromisoformat(parsed.replace('Z', '+00:00')).replace(tzinfo=None)
    assert parsed == datetime.datetime(2026, 10, 1, 17, 30)
    assert not row['ingestion_time_available']


def test_missing_business_time_is_available_to_audit_not_replaced_by_ingestion(normalize):
    row = normalize({'price': 10.0, 'ingested_at': '2026-10-01'}, 'prices')
    assert row['timestamp'] is None
    assert row['timestamp_raw'] is None
    assert not row['time_available']
    assert row['ingestion_time_available']


@pytest.mark.parametrize(('raw', 'want'), [
    (1727000000, '2024-09-22T10:13:20Z'),
    (1727000000000, '2024-09-22T10:13:20Z'),
    ('2026/09/24 08:30:00', '2026-09-24T08:30:00Z'),
    ('24/09/2026 08:30:00', '2026-09-24T08:30:00Z'),
])
def test_supported_observation_time_formats_match_across_engines(normalize, raw, want):
    row = normalize({'rating': 4.0, 'review_date': raw}, 'reviews')
    assert row['review_date'] == want
    assert str(row['review_date_raw']) == str(raw)
    assert row['normalization_status'] == 'valid'
    assert row['time_available']


@pytest.mark.parametrize(('brand', 'category', 'currency', 'want_brand', 'want_category', 'want_currency'), [
    ('mi', 'phone', 'vnđ', 'Xiaomi', 'Smartphone', 'VND'),
    ('samsung electronics', 'accessory', '$', 'Samsung', 'Accessories', 'USD'),
])
def test_product_entity_and_currency_aliases_match_across_engines(
    normalize, brand, category, currency, want_brand, want_category, want_currency,
):
    row = normalize({'price': 10.0, 'brand': brand, 'category': category, 'currency': currency}, 'products')
    assert (row['brand'], row['category'], row['currency']) == (want_brand, want_category, want_currency)


@pytest.mark.parametrize('raw', ['1e', '12-34', '1,2,3', '1e-3 USD', '++10'])
def test_malformed_price_grammar_cannot_be_repaired_into_a_different_price(normalize, raw):
    row = normalize({'price':raw}, 'products')
    assert row['price'] is None
    assert row['price_raw'] == raw
    assert 'INVALID_PRICE' in row['normalization_errors']


@pytest.mark.parametrize('dataset', ['products','prices'])
@pytest.mark.parametrize('raw', ['', 'unsupported currency'])
def test_missing_or_invalid_currency_stays_unknown_with_original_evidence(normalize, dataset, raw):
    row = normalize({'price':10.0,'currency':raw}, dataset)
    assert row['currency'] is None
    assert row['currency_raw'] == raw
