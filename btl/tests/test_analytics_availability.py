import pandas as pd
import pytest
from analytics.descriptive.stats import DescriptiveStats
from analytics.descriptive.comparative import ComparativeAnalytics

pytestmark = pytest.mark.unit


def test_absent_ratings_are_unavailable_not_zero_stars():
    summary = DescriptiveStats.compute_summary(pd.DataFrame(), pd.DataFrame())
    assert summary['avg_rating'] is None
    assert summary['median_rating'] is None
    assert summary['rating_sample_size'] == 0
    assert summary['rating_availability_status'] == 'unavailable'


def test_product_metrics_report_sample_size_and_missing_ratings():
    products = pd.DataFrame([{'product_id':'p1'}, {'product_id':'p2'}])
    reviews = pd.DataFrame([{'review_id':'r1','product_id':'p1','rating':5.}])
    rows = DescriptiveStats.compute_product_metrics(reviews, products).set_index('product_id')
    assert rows.loc['p1','rating_sample_size'] == 1
    assert rows.loc['p1','weighted_avg_rating'] == 5.
    assert rows.loc['p2','rating_sample_size'] == 0
    assert pd.isna(rows.loc['p2','avg_rating'])
    assert rows.loc['p2','rating_availability_status'] == 'unavailable'


def test_catalog_price_comparison_is_not_weighted_by_review_volume():
    products = pd.DataFrame([{'product_id':'p1','brand':'A','price':10.},
                             {'product_id':'p2','brand':'A','price':100.},
                             {'product_id':'bad','brand':'A','price':float('inf')},
                             {'product_id':'negative','brand':'A','price':-1.}])
    reviews = pd.DataFrame([{'review_id':f'r{i}','product_id':'p1','rating':5.} for i in range(9)] +
                           [{'review_id':'r9','product_id':'p2','rating':1.}])
    row = ComparativeAnalytics.brand_comparison(products, reviews).iloc[0]
    assert row['product_count'] == 4
    assert row['avg_price'] == 55.
    assert row['min_price'] == 10.
    assert row['max_price'] == 100.
    assert row['price_sample_size'] == 2
    assert row['rating_sample_size'] == 10
    assert row['weighted_avg_rating'] == 4.6


def test_comparison_keeps_unreviewed_brand_without_invented_rating():
    row = ComparativeAnalytics.brand_comparison(
        pd.DataFrame([{'product_id':'p1','brand':'A','price':10.}]), pd.DataFrame()
    ).iloc[0]
    assert row['review_count'] == 0
    assert pd.isna(row['avg_rating'])
    assert row['rating_availability_status'] == 'unavailable'
    assert row['avg_price'] == 10.


def test_descriptive_price_summary_excludes_nonfinite_or_nonpositive_values():
    summary = DescriptiveStats.compute_summary(
        pd.DataFrame({'price':[10.,100.,-5.,0.,float('inf'),'bad']}), pd.DataFrame()
    )
    assert summary['price_stats'] == {'min_price':10., 'max_price':100., 'mean_price':55., 'median_price':55.}
