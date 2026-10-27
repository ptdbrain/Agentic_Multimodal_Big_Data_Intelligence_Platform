import pandas as pd
import pytest

from spark.etl.gold_aggregator import GoldAggregator


@pytest.fixture(params=['pandas', pytest.param('spark', marks=pytest.mark.spark)])
def build_marts(request):
    def build(products, reviews, prices):
        if request.param == 'pandas':
            return GoldAggregator.build_gold_marts(
                products, reviews, prices, save_to_storage=False, run_id='run-parity', source_run_id='silver-source',
            )
        from spark.analytics.gold_builder import SparkGoldBuilder
        spark = request.getfixturevalue('spark_session')
        marts = SparkGoldBuilder(spark).build_all(
            reviews if not reviews.empty else [], products if not products.empty else [],
            prices if not prices.empty else [], save_gold=False, run_id='run-parity', source_run_id='silver-source',
        )
        return {name: frame.toPandas() for name, frame in marts.items()}
    return build


def test_gold_engines_match_literal_price_and_rating_contract(build_marts):
    products = pd.DataFrame([
        {'product_id': 'p1', 'brand': 'A', 'category': 'Phone', 'price': 10.0},
        {'product_id': 'p2', 'brand': 'A', 'category': 'Phone', 'price': 100.0},
        {'product_id': 'p3', 'brand': 'A', 'category': 'Phone', 'price': float('nan')},
    ])
    reviews = pd.DataFrame([
        *[{'review_id': f'r{i}', 'product_id': 'p1', 'rating': 5.0, 'review_date': '2026-10-01'} for i in range(9)],
        {'review_id': 'r9', 'product_id': 'p2', 'rating': 1.0, 'review_date': '2026-10-02T00:30:00+07:00'},
        {'review_id': 'r10', 'product_id': 'p3', 'rating': 3.0, 'review_date': '2026-10-01'},
        {'review_id': 'bad-rating', 'product_id': 'p3', 'rating': 8.0, 'review_date': '2026-10-01'},
        {'review_id': None, 'product_id': 'p3', 'rating': 2.0, 'review_date': '2026-10-01'},
        {'review_id': 'bad-time', 'product_id': 'p3', 'rating': 2.0, 'review_date': 'invalid'},
    ])
    prices = pd.DataFrame([
        {'product_id': 'p1', 'price': p, 'timestamp': '2026-10-01'} for p in [20.0, 40.0, float('inf'), 0.0]
    ] + [{'product_id': 'p2', 'price': 200.0, 'timestamp': 'invalid'}])
    marts = build_marts(products, reviews, prices)
    product = marts['product_daily_stats'].set_index('product_id')
    assert set(product.index) == {'p1', 'p2', 'p3'}
    assert len(product) == 3
    for product_id, count, rating, source, price in [
        ('p1', 9, 5.0, 'observed', 30.0), ('p2', 1, 1.0, 'catalog_fallback', 100.0),
        ('p3', 1, 3.0, 'unavailable', None),
    ]:
        row = product.loc[product_id]
        assert row['review_count'] == row['rating_sample_size'] == count
        assert float(row['avg_rating']) == float(row['weighted_avg_rating']) == rating
        assert row['price_source'] == source
        assert pd.isna(row['avg_price']) if price is None else float(row['avg_price']) == price
        assert str(row['date']) == '2026-10-01'
    for name in ['brand_daily_stats', 'category_daily_stats']:
        assert len(marts[name]) == 1
        row = marts[name].iloc[0]
        assert row['review_count'] == row['rating_sample_size'] == 11
        assert float(row['avg_rating']) == float(row['weighted_avg_rating']) == 4.45
        assert float(row['avg_price']) == 55.0  # two finite catalog products, independent of review volume
        assert row['product_count'] == 3
    observed = marts['price_daily_stats']
    assert len(observed) == 1
    assert observed.iloc[0]['data_points'] == 2
    assert float(observed.iloc[0]['avg_price']) == 30.0
    for frame in marts.values():
        assert set(frame['run_id']) == {'run-parity'}
        assert set(frame['source_run_id']) == {'silver-source'}
        assert set(frame['time_available']) == {True}
        assert {str(value) for value in frame['data_as_of']} == {'2026-10-01'}


def test_price_mart_does_not_require_review_observations(build_marts):
    marts = build_marts(pd.DataFrame(), pd.DataFrame(), pd.DataFrame([
        {'product_id': 'p1', 'price': 10.0, 'timestamp': '2026-10-01'},
        {'product_id': 'p1', 'price': 20.0, 'timestamp': '2026-10-01'},
    ]))
    assert set(marts) == {'price_daily_stats'}
    assert marts['price_daily_stats'].iloc[0]['data_points'] == 2
    assert float(marts['price_daily_stats'].iloc[0]['avg_price']) == 15.0


@pytest.mark.parametrize('second_id', ['p1', '', None])
def test_catalog_keys_cannot_duplicate_reviews_or_choose_an_arbitrary_price(build_marts, second_id):
    products = pd.DataFrame([
        {'product_id': 'p1', 'brand': 'A', 'category': 'Phone', 'price': 10.0},
        {'product_id': second_id, 'brand': 'A', 'category': 'Phone', 'price': 100.0},
    ])
    reviews = pd.DataFrame([{'review_id': 'r1', 'product_id': 'p1', 'rating': 5.0, 'review_date': '2026-10-01'}])
    with pytest.raises(ValueError):
        build_marts(products, reviews, pd.DataFrame())


def test_gold_engines_use_the_same_half_up_rounding_for_price_and_rating(build_marts):
    products = pd.DataFrame([{'product_id':'p','brand':'A','category':'Phone','price':1.005}])
    reviews = pd.DataFrame([
        {'review_id':f'r{i}','product_id':'p','rating':rating,'review_date':'2026-10-01'}
        for i,rating in enumerate([4.,4.,4.,4.5])
    ])
    prices = pd.DataFrame([{'product_id':'p','price':1.005,'timestamp':'2026-10-01'}])
    marts = build_marts(products,reviews,prices)
    row = marts['product_daily_stats'].iloc[0]
    assert float(row['avg_price']) == 1.01
    assert float(row['avg_rating']) == 4.13
    for name in ['brand_daily_stats','category_daily_stats']:
        assert float(marts[name].iloc[0]['avg_price']) == 1.01
        assert float(marts[name].iloc[0]['avg_rating']) == 4.13
