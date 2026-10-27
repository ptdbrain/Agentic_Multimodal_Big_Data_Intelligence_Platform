import pandas as pd
import pytest

from analytics.descriptive.stats import DescriptiveStats
from analytics.trend.price_tracker import PriceTracker
from spark.etl.gold_aggregator import GoldAggregator


def observations():
    products = pd.DataFrame([
        {'product_id': pid, 'price': 10.0, 'brand': 'A', 'category': 'Phone'}
        for pid in ['single', 'constant', 'variable']
    ])
    reviews = pd.DataFrame([
        {'review_id': f'{pid}-{i}', 'product_id': pid, 'rating': rating, 'review_date': '2026-10-01'}
        for pid, ratings in [('single', [4.0]), ('constant', [3.0, 3.0]), ('variable', [1.0, 5.0])]
        for i, rating in enumerate(ratings)
    ])
    prices = pd.DataFrame([
        {'product_id': pid, 'price': price, 'timestamp': '2026-10-01'}
        for pid, values in [('single', [10.0]), ('constant', [20.0, 20.0]), ('variable', [10.0, 30.0])]
        for price in values
    ])
    return products, reviews, prices


@pytest.mark.parametrize('engine', ['pandas', pytest.param('spark', marks=pytest.mark.spark)])
def test_gold_sample_deviation_is_unknown_for_one_observation_and_zero_for_constant_sample(engine, request):
    products, reviews, prices = observations()
    if engine == 'pandas':
        marts = GoldAggregator.build_gold_marts(products, reviews, prices, save_to_storage=False)
    else:
        from spark.analytics.gold_builder import SparkGoldBuilder
        spark = request.getfixturevalue('spark_session')
        marts = {name: frame.toPandas() for name, frame in SparkGoldBuilder(spark).build_all(
            reviews, products, prices, save_gold=False,
        ).items()}
    for name, metric, variable_std in [('product_daily_stats', 'rating_std', 2.83), ('price_daily_stats', 'price_std', 14.14)]:
        rows = marts[name].set_index('product_id')
        assert pd.isna(rows.loc['single', metric])
        assert float(rows.loc['constant', metric]) == 0.0
        assert float(rows.loc['variable', metric]) == variable_std


@pytest.mark.unit
def test_product_descriptive_deviation_retains_sample_uncertainty():
    products, reviews, _ = observations()
    rows = DescriptiveStats.compute_product_metrics(reviews, products).set_index('product_id')
    assert pd.isna(rows.loc['single', 'rating_std'])
    assert rows.loc['single', 'rating_sample_size'] == 1
    assert rows.loc['constant', 'rating_std'] == 0.0
    assert rows.loc['variable', 'rating_std'] == 2.83


@pytest.mark.unit
def test_price_volatility_distinguishes_unavailable_variance_from_observed_zero():
    _, _, prices = observations()
    rows = PriceTracker.calculate_price_volatility(prices).set_index('product_id')
    assert rows.loc['single', 'data_points'] == 1
    assert pd.isna(rows.loc['single', 'std_price'])
    assert pd.isna(rows.loc['single', 'volatility_ratio'])
    assert rows.loc['single', 'volatility_status'] == 'insufficient_data'
    assert rows.loc['constant', 'std_price'] == rows.loc['constant', 'volatility_ratio'] == 0.0
    assert rows.loc['constant', 'volatility_status'] == 'available'
