import pytest
import pandas as pd
from analytics.descriptive.stats import DescriptiveStats
from analytics.descriptive.comparative import ComparativeAnalytics
from analytics.trend.trends import TrendAnalyzer
from analytics.anomaly.price_anomaly import PriceAnomalyDetector
from spark.etl.gold_aggregator import GoldAggregator

pytestmark = pytest.mark.unit


@pytest.fixture
def sample_data():
    prods = pd.DataFrame([
        {"product_id": "p1", "brand": "Apple", "category": "Smartphone", "price": 20000000.0},
        {"product_id": "p2", "brand": "Samsung", "category": "Smartphone", "price": 18000000.0},
    ])
    revs = pd.DataFrame([
        {"review_id": "r1", "product_id": "p1", "rating": 5.0, "review_date": "2026-09-20T10:00:00Z"},
        {"review_id": "r2", "product_id": "p1", "rating": 5.0, "review_date": "2026-09-20T11:00:00Z"},
        {"review_id": "r3", "product_id": "p2", "rating": 2.0, "review_date": "2026-09-21T10:00:00Z"},
    ])
    return prods, revs


def test_descriptive_stats(sample_data):
    prods, revs = sample_data
    summary = DescriptiveStats.compute_summary(prods, revs)
    assert summary["total_products"] == 2
    assert summary["total_reviews"] == 3
    assert summary["avg_rating"] == 4.0


def test_comparative_analytics(sample_data):
    prods, revs = sample_data
    b_df = ComparativeAnalytics.brand_comparison(prods, revs)
    assert len(b_df) == 2
    assert "brand" in b_df.columns


def test_trends_analyzer(sample_data):
    _, revs = sample_data
    daily = TrendAnalyzer.daily_review_trends(revs)
    assert len(daily) == 2
    assert "rolling_avg_count" in daily.columns


def test_price_anomaly_detector():
    """Verifies Z-Score and IQR price anomaly detection and proves that the baseline
    strictly excludes the candidate point currently being evaluated.
    """
    prices = pd.DataFrame([
        {"product_id": "p1", "price": 20000000, "timestamp": "2026-09-01T10:00:00Z"},
        {"product_id": "p1", "price": 20100000, "timestamp": "2026-09-02T10:00:00Z"},
        {"product_id": "p1", "price": 19900000, "timestamp": "2026-09-03T10:00:00Z"},
        {"product_id": "p1", "price": 20050000, "timestamp": "2026-09-04T10:00:00Z"},
        {"product_id": "p1", "price": 20000000, "timestamp": "2026-09-05T10:00:00Z"},
        {"product_id": "p1", "price": 2000000, "timestamp": "2026-09-06T10:00:00Z"},  # extreme glitch
    ])
    anomalies = PriceAnomalyDetector.detect_zscore_anomalies(prices, threshold=2.0)
    assert len(anomalies) == 1
    anom = anomalies[0]
    assert anom["entity_id"] == "p1"
    assert anom["anomaly_type"] == "PRICE_ANOMALY"
    assert anom["observed_price"] == 2000000.0
    # Exact mean of the 5 non-anomaly points is (20M + 20.1M + 19.9M + 20.05M + 20M) / 5 = 20,010,000.0
    # (If the 2M point were included in the baseline, the mean would be 17,008,333.33)
    assert anom["baseline_mean"] == 20010000.0

    iqr_anomalies = PriceAnomalyDetector.detect_iqr_anomalies(prices, k=2.0)
    assert len(iqr_anomalies) == 1
    assert iqr_anomalies[0]["baseline_median"] == 20000000.0
    assert iqr_anomalies[0]["observed_price"] == 2000000.0


def test_gold_analytics_known_aggregation_results():
    """Verifies Gold analytics aggregations against hand-calculated expected values."""
    prods = pd.DataFrame([
        {"product_id": "p1", "brand": "Apple", "category": "Smartphone", "price": 25000000.0},
        {"product_id": "p2", "brand": "Samsung", "category": "Smartphone", "price": 15000000.0},
    ])
    revs = pd.DataFrame([
        {"review_id": "r1", "product_id": "p1", "rating": 5.0, "review_date": "2026-09-20T08:00:00Z"},
        {"review_id": "r2", "product_id": "p1", "rating": 4.0, "review_date": "2026-09-20T09:00:00Z"},
        {"review_id": "r3", "product_id": "p1", "rating": 1.0, "review_date": "2026-09-20T10:00:00Z"},
        {"review_id": "r4", "product_id": "p1", "rating": 2.0, "review_date": "2026-09-20T11:00:00Z"},
        {"review_id": "r5", "product_id": "p2", "rating": 5.0, "review_date": "2026-09-20T12:00:00Z"},
    ])
    prices = pd.DataFrame([
        {"price_id": "pr1", "product_id": "p1", "price": 24000000.0, "timestamp": "2026-09-20T08:00:00Z"},
        {"price_id": "pr2", "product_id": "p1", "price": 26000000.0, "timestamp": "2026-09-20T16:00:00Z"},
        {"price_id": "pr3", "product_id": "p2", "price": 15000000.0, "timestamp": "2026-09-20T10:00:00Z"},
    ])

    marts = GoldAggregator.build_gold_marts(
        df_products=prods,
        df_reviews=revs,
        df_prices=prices,
        positive_threshold=4.0,
        negative_threshold=2.0,
        save_to_storage=False,
    )

    # 1. Product daily stats for p1 on 2026-09-20:
    # ratings = [5, 4, 1, 2] -> count=4, avg=3.0, positive_ratio=2/4=0.5, negative_ratio=2/4=0.5
    # prices = [24M, 26M] -> min=24M, max=26M, avg=25M
    p_df = marts["product_daily_stats"]
    p1_row = p_df[p_df["product_id"] == "p1"].iloc[0]
    assert p1_row["review_count"] == 4
    assert p1_row["avg_rating"] == 3.0
    assert p1_row["positive_ratio"] == 0.5
    assert p1_row["negative_ratio"] == 0.5
    assert p1_row["min_price"] == 24000000.0
    assert p1_row["max_price"] == 26000000.0
    assert p1_row["avg_price"] == 25000000.0

    # 2. Brand daily stats: Apple has 1 product, 4 reviews, avg_rating=3.0; Samsung has 1 product, 1 review, avg_rating=5.0
    b_df = marts["brand_daily_stats"]
    apple_row = b_df[b_df["brand"] == "Apple"].iloc[0]
    assert apple_row["product_count"] == 1
    assert apple_row["review_count"] == 4
    assert apple_row["avg_rating"] == 3.0

    # 3. Category daily stats: Smartphone has 2 products, 5 reviews, avg_rating=(5+4+1+2+5)/5 = 3.4
    c_df = marts["category_daily_stats"]
    cat_row = c_df[c_df["category"] == "Smartphone"].iloc[0]
    assert cat_row["product_count"] == 2
    assert cat_row["review_count"] == 5
    assert cat_row["avg_rating"] == 3.4

    # 4. Price daily stats
    pr_df = marts["price_daily_stats"]
    pr_p1 = pr_df[pr_df["product_id"] == "p1"].iloc[0]
    assert pr_p1["min_price"] == 24000000.0
    assert pr_p1["max_price"] == 26000000.0
    assert pr_p1["avg_price"] == 25000000.0
