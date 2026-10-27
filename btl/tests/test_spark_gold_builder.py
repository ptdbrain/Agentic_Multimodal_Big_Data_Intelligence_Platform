import pytest
from spark.analytics.gold_builder import SparkGoldBuilder

pytestmark = pytest.mark.spark


def test_gold_builder_matches_independent_oracle(spark_session):
    s=spark_session
    products=s.createDataFrame([{'product_id':'p1','brand':'Apple','category':'Phone','price':25000000.0}])
    reviews=s.createDataFrame([{'review_id':f'r{i}','product_id':'p1','rating':float(r),'review_date':'2026-10-01T00:00:00Z'} for i,r in enumerate([5,4,1,2])])
    prices=s.createDataFrame([{'product_id':'p1','price':float(p),'timestamp':'2026-10-01T00:00:00Z'} for p in [24000000,26000000]])
    marts=SparkGoldBuilder(spark=s).build_all(reviews,products,prices,save_gold=False)
    assert set(marts)=={'product_daily_stats','brand_daily_stats','category_daily_stats','price_daily_stats'}
    row=marts['product_daily_stats'].first().asDict()
    assert row['review_count']==4
    assert float(row['avg_rating'])==3
    assert float(row['positive_ratio'])==.5
    assert float(row['negative_ratio'])==.5
    assert float(row['avg_price'])==25000000
    assert float(marts['brand_daily_stats'].first()['avg_rating'])==3
    assert marts['category_daily_stats'].first()['review_count']==4
    assert marts['price_daily_stats'].first()['data_points']==2


def test_gold_builder_drops_rows_without_observation_time(spark_session):
    s = spark_session
    reviews = s.createDataFrame([
        {'review_id': 'r1', 'product_id': 'p1', 'rating': 5.0},
    ])
    products = s.createDataFrame([
        {'product_id': 'p1', 'brand': 'Apple', 'category': 'Phone', 'price': 10.0},
    ])
    result = SparkGoldBuilder(spark=s).build_product_daily_stats(reviews, products)
    assert result.count() == 0


def test_gold_builder_marks_catalog_price_fallback(spark_session):
    s = spark_session
    products = s.createDataFrame([
        {'product_id': 'p1', 'brand': 'Apple', 'category': 'Phone', 'price': 10.0},
    ])
    reviews = s.createDataFrame([
        {'review_id': 'r1', 'product_id': 'p1', 'rating': 5.0,
         'review_date': '2026-10-01T00:00:00Z'},
    ])
    result = SparkGoldBuilder(spark=s).build_product_daily_stats(reviews, products)
    row = result.first().asDict()
    assert row['price_source'] == 'catalog_fallback'


def test_gold_builder_does_not_hide_spark_read_errors(spark_session, monkeypatch):
    builder = SparkGoldBuilder(spark=spark_session)
    monkeypatch.setenv('RUNTIME_MODE', 'integration')
    with pytest.raises(Exception):
        builder._ensure_spark_df(object(), 'reviews')


def test_price_daily_stats_does_not_turn_catalog_into_current_day(spark_session):
    s = spark_session
    products = s.createDataFrame([
        {'product_id': 'p1', 'price': 10.0},
    ])
    result = SparkGoldBuilder(spark=s).build_price_daily_stats([], products)
    assert result is None


def test_gold_date_precedence_is_per_row_and_utc(spark_session):
    s = spark_session
    frame = s.createDataFrame([
        ('r1', None, '2026-10-01T00:30:00+07:00', None),
        ('r2', 'invalid', None, '2026-10-02T12:00:00Z'),
        ('r3', '2026-10-03', '2026-10-04T00:00:00Z', None),
        ('r4', None, None, None),
    ], 'review_id string, date string, review_date string, timestamp string')
    dated = SparkGoldBuilder._with_observation_date(frame, ('review_date', 'timestamp'))
    rows = {r['review_id']: str(r['date']) if r['date'] else None for r in dated.collect()}
    assert rows == {'r1': '2026-09-30', 'r2': '2026-10-02', 'r3': '2026-10-03', 'r4': None}


def test_gold_build_all_adds_run_lineage_and_data_as_of(spark_session):
    s = spark_session
    products = s.createDataFrame([{'product_id': 'p1', 'brand': 'A', 'category': 'Phone', 'price': 10.0}])
    reviews = s.createDataFrame([{'review_id': 'r1', 'product_id': 'p1', 'rating': 5.0, 'review_date': '2026-10-01'}])
    marts = SparkGoldBuilder(spark=s).build_all(
        reviews, products, save_gold=False, run_id='run-1', source_run_id='source-1'
    )
    row = marts['product_daily_stats'].first().asDict()
    assert row['run_id'] == 'run-1'
    assert row['source_run_id'] == 'source-1'
    assert str(row['data_as_of']) == '2026-10-01'
    assert row['time_available'] is True


def test_gold_validation_rejects_duplicate_grain_and_nonfinite_metrics(spark_session):
    s = spark_session
    duplicate = s.createDataFrame([
        ('2026-10-01', 'p1', 1.0), ('2026-10-01', 'p1', 2.0)
    ], 'date string, product_id string, avg_rating double')
    with pytest.raises(ValueError, match='duplicate grain'):
        SparkGoldBuilder._validate_mart(duplicate, 'product_daily_stats')

    invalid = s.createDataFrame([('2026-10-01', 'p1', float('nan'))],
                                'date string, product_id string, avg_rating double')
    with pytest.raises(ValueError, match='non-finite'):
        SparkGoldBuilder._validate_mart(invalid, 'product_daily_stats')


def test_gold_validation_rejects_null_keys_and_invalid_counts_prices(spark_session):
    s = spark_session
    null_key = s.createDataFrame([(None, 'p1', 1)], 'date string, product_id string, review_count int')
    with pytest.raises(ValueError, match='null grain'):
        SparkGoldBuilder._validate_mart(null_key, 'product_daily_stats')
    invalid = s.createDataFrame([('2026-10-01', 'p1', 0, -1.0)],
                                'date string, product_id string, review_count int, avg_price double')
    with pytest.raises(ValueError, match='positive'):
        SparkGoldBuilder._validate_mart(invalid, 'product_daily_stats')


def test_spark_marts_expose_rating_sample_size_and_weighted_rating(spark_session):
    s = spark_session
    products = s.createDataFrame([{'product_id':'p1','brand':'A','category':'Phone','price':10.}])
    reviews = s.createDataFrame([
        {'review_id':f'r{i}','product_id':'p1','rating':float(r),'review_date':'2026-10-01'}
        for i,r in enumerate([5,5,4,2])
    ])
    marts = SparkGoldBuilder(spark=s).build_all(reviews, products, [], save_gold=False, run_id='sample-run')
    for name in ['product_daily_stats','brand_daily_stats','category_daily_stats']:
        row = marts[name].first().asDict()
        assert row['rating_sample_size'] == 4
        assert float(row['weighted_avg_rating']) == 4
        assert row['run_id'] == 'sample-run'


def test_missing_gold_grain_column_is_rejected(spark_session):
    frame = spark_session.createDataFrame([('p1',5.)], 'product_id string, avg_rating double')
    with pytest.raises(ValueError, match='missing grain'):
        SparkGoldBuilder._validate_mart(frame, 'product_daily_stats')


def test_all_marts_validate_before_the_first_publication(spark_session, monkeypatch):
    from spark.storage_safe import SafeParquetIO
    builder = SparkGoldBuilder(spark=spark_session)
    good = spark_session.createDataFrame([('2026-10-01','p1',1)], 'date string, product_id string, review_count int')
    bad = spark_session.createDataFrame([(None,'A',1)], 'date string, brand string, review_count int')
    monkeypatch.setattr(builder,'build_product_daily_stats',lambda *a:good)
    monkeypatch.setattr(builder,'build_brand_daily_stats',lambda *a:bad)
    monkeypatch.setattr(builder,'build_category_daily_stats',lambda *a:None)
    monkeypatch.setattr(builder,'build_price_daily_stats',lambda *a:None)
    def must_not_publish(*args, **kwargs):
        pytest.fail('No mart may publish before the entire group passes validation')
    monkeypatch.setattr(SafeParquetIO,'publish_parquet_atomic',must_not_publish)
    with pytest.raises(ValueError, match='null grain'):
        builder.build_all([], [], [], save_gold=True, run_id='invalid-build')


def test_product_without_any_price_is_labeled_unavailable(spark_session):
    reviews = spark_session.createDataFrame([{'review_id':'r1','product_id':'p1','rating':5.,'review_date':'2026-10-01'}])
    row = SparkGoldBuilder(spark=spark_session).build_product_daily_stats(reviews, [], []).first()
    assert row['avg_price'] is None
    assert row['price_source'] == 'unavailable'
