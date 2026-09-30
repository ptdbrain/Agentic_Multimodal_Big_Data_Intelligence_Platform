import pytest
import pandas as pd
from analytics.descriptive.stats import DescriptiveStats
from analytics.descriptive.comparative import ComparativeAnalytics
from analytics.trend.trends import TrendAnalyzer
from analytics.anomaly.price_anomaly import PriceAnomalyDetector

@pytest.fixture
def sample_data():
    prods = pd.DataFrame([
        {"product_id": "p1", "brand": "Apple", "category": "Smartphone", "price": 20000000.0},
        {"product_id": "p2", "brand": "Samsung", "category": "Smartphone", "price": 18000000.0}
    ])
    revs = pd.DataFrame([
        {"review_id": "r1", "product_id": "p1", "rating": 5.0, "review_date": "2026-09-20T10:00:00Z"},
        {"review_id": "r2", "product_id": "p1", "rating": 5.0, "review_date": "2026-09-20T11:00:00Z"},
        {"review_id": "r3", "product_id": "p2", "rating": 2.0, "review_date": "2026-09-21T10:00:00Z"}
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
    prices = pd.DataFrame([
        {"product_id": "p1", "price": 20000000, "timestamp": "2026-09-01T10:00:00Z"},
        {"product_id": "p1", "price": 20100000, "timestamp": "2026-09-02T10:00:00Z"},
        {"product_id": "p1", "price": 19900000, "timestamp": "2026-09-03T10:00:00Z"},
        {"product_id": "p1", "price": 20050000, "timestamp": "2026-09-04T10:00:00Z"},
        {"product_id": "p1", "price": 20000000, "timestamp": "2026-09-05T10:00:00Z"},
        {"product_id": "p1", "price": 2000000,  "timestamp": "2026-09-06T10:00:00Z"} # extreme glitch
    ])
    anomalies = PriceAnomalyDetector.detect_zscore_anomalies(prices, threshold=2.0)
    assert len(anomalies) >= 1
    assert anomalies[0]["entity_id"] == "p1"
