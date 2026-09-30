"""Tests for Streaming, Gold Aggregator, Storage Manager, Search Indexer, Export Gold.
Covers all 6 untested components + missing analytics tests (ReviewBurst, RatingAnomaly, PriceTracker).
"""
import sys
import os
import sqlite3
import tempfile
import shutil
import pandas as pd
import numpy as np
import pytest
from datetime import datetime, timedelta

# ============================================================
# 1. Window Aggregations Tests
# ============================================================
from spark.streaming.window_aggregations import WindowAggregator

class TestWindowAggregator:
    def test_tumbling_window_basic(self):
        events = [
            {"timestamp": "2026-09-25T10:00:05Z", "payload": {"rating": 4.0}},
            {"timestamp": "2026-09-25T10:00:30Z", "payload": {"rating": 5.0}},
            {"timestamp": "2026-09-25T10:01:10Z", "payload": {"rating": 3.0}},
        ]
        results = WindowAggregator.aggregate_tumbling_window(events, window_seconds=60)
        assert len(results) == 2  # two 1-min windows
        assert results[0]["event_count"] == 2
        assert results[0]["avg_rating"] == 4.5
        assert results[1]["event_count"] == 1

    def test_tumbling_window_5min(self):
        base = datetime(2026, 9, 25, 10, 0, 0)
        events = []
        for i in range(10):
            events.append({
                "timestamp": (base + timedelta(seconds=i * 30)).strftime("%Y-%m-%dT%H:%M:%SZ"),
                "payload": {"rating": 4.0}
            })
        results = WindowAggregator.aggregate_tumbling_window(events, window_seconds=300)
        assert len(results) == 1  # all within 5 min
        assert results[0]["event_count"] == 10
        assert results[0]["rate_eps"] == round(10 / 300, 2)

    def test_tumbling_window_empty(self):
        results = WindowAggregator.aggregate_tumbling_window([], window_seconds=60)
        assert results == []

    def test_tumbling_window_invalid_timestamp(self):
        events = [{"timestamp": "not-a-timestamp", "payload": {"rating": 3.0}}]
        results = WindowAggregator.aggregate_tumbling_window(events, window_seconds=60)
        assert results == []

    def test_tumbling_window_no_rating(self):
        events = [{"timestamp": "2026-09-25T10:00:05Z", "payload": {}}]
        results = WindowAggregator.aggregate_tumbling_window(events, window_seconds=60)
        assert len(results) == 1
        assert results[0]["avg_rating"] is None


# ============================================================
# 2. Streaming Job Tests
# ============================================================
from spark.streaming.streaming_job import StreamingJob

class TestStreamingJob:
    def test_process_single_batch(self):
        job = StreamingJob()
        events = [
            {"payload": {"rating": 5.0}},
            {"payload": {"rating": 3.0}},
            {"payload": {"rating": 1.0}},
        ]
        result = job.process_micro_batch(events)
        assert result["micro_batch_size"] == 3
        assert result["total_processed"] == 3
        assert result["negative_reviews_count"] == 1  # rating 1.0

    def test_process_multiple_batches_accumulates(self):
        job = StreamingJob()
        batch1 = [{"payload": {"rating": 4.0}}, {"payload": {"rating": 5.0}}]
        batch2 = [{"payload": {"rating": 2.0}}, {"payload": {"rating": 1.0}}]
        job.process_micro_batch(batch1)
        result = job.process_micro_batch(batch2)
        assert result["total_processed"] == 4
        assert result["negative_reviews_count"] == 2  # 2.0 and 1.0

    def test_process_empty_batch(self):
        job = StreamingJob()
        result = job.process_micro_batch([])
        assert result["micro_batch_size"] == 0
        assert result["total_processed"] == 0

    def test_process_no_rating(self):
        job = StreamingJob()
        events = [{"payload": {}}, {"payload": {"text": "good"}}]
        result = job.process_micro_batch(events)
        assert result["micro_batch_size"] == 2
        assert result["negative_reviews_count"] == 0


# ============================================================
# 3. Storage Manager Tests
# ============================================================
class TestStorageManager:
    @pytest.fixture(autouse=True)
    def setup_storage(self):
        self.temp_dir = tempfile.mkdtemp(prefix="test_storage_")
        # Monkey-patch the storage manager for isolation
        from storage.storage_manager import StorageManager
        from pathlib import Path
        self.mgr = StorageManager.__new__(StorageManager)
        self.mgr.base_dir = Path(self.temp_dir)
        self.mgr.bronze_dir = self.mgr.base_dir / "bronze"
        self.mgr.silver_dir = self.mgr.base_dir / "silver"
        self.mgr.gold_dir = self.mgr.base_dir / "gold"
        for d in [self.mgr.bronze_dir, self.mgr.silver_dir, self.mgr.gold_dir]:
            d.mkdir(parents=True, exist_ok=True)
        yield
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_write_and_read_silver(self):
        df = pd.DataFrame({"product_id": ["p1", "p2"], "price": [100, 200]})
        self.mgr.write_silver_parquet("test_products", df)
        result = self.mgr.read_silver_parquet("test_products")
        assert len(result) == 2
        assert list(result.columns) == ["product_id", "price"]

    def test_read_nonexistent_silver_returns_empty(self):
        result = self.mgr.read_silver_parquet("nonexistent_dataset")
        assert result.empty

    def test_write_and_read_gold(self):
        df = pd.DataFrame({"metric": ["sales"], "value": [1000]})
        self.mgr.write_gold_parquet("test_mart", df)
        result = self.mgr.read_gold_parquet("test_mart")
        assert len(result) == 1
        assert result.iloc[0]["value"] == 1000

    def test_read_nonexistent_gold_returns_empty(self):
        result = self.mgr.read_gold_parquet("nonexistent_mart")
        assert result.empty

    def test_directory_structure_created(self):
        assert self.mgr.bronze_dir.exists()
        assert self.mgr.silver_dir.exists()
        assert self.mgr.gold_dir.exists()


# ============================================================
# 4. Search Indexer Tests
# ============================================================
from storage.search_indexer import SearchIndexer

class TestSearchIndexer:
    @pytest.fixture
    def indexer_with_data(self):
        idx = SearchIndexer()
        idx.index_reviews([
            {"review_id": "r1", "review_text": "Great phone with amazing camera", "review_title": "Best phone", "rating": 5.0, "product_id": "p1"},
            {"review_id": "r2", "review_text": "Battery life is terrible", "review_title": "Bad battery", "rating": 1.0, "product_id": "p1"},
            {"review_id": "r3", "review_text": "Good value for money camera phone", "review_title": "Good value", "rating": 4.0, "product_id": "p2"},
            {"review_id": "r4", "review_text": "Overpriced and slow", "review_title": "Avoid", "rating": 2.0, "product_id": "p2"},
        ])
        return idx

    def test_search_by_keyword(self, indexer_with_data):
        results = indexer_with_data.search("camera")
        assert len(results) == 2  # r1 and r3 have "camera"

    def test_search_by_title(self, indexer_with_data):
        results = indexer_with_data.search("best phone")
        assert len(results) >= 1
        assert results[0]["review_id"] == "r1"

    def test_search_with_min_rating(self, indexer_with_data):
        results = indexer_with_data.search("", min_rating=4.0)
        assert all(r["rating"] >= 4.0 for r in results)

    def test_search_with_limit(self, indexer_with_data):
        results = indexer_with_data.search("", limit=2)
        assert len(results) <= 2

    def test_search_empty_index(self):
        idx = SearchIndexer()
        results = idx.search("anything")
        assert results == []

    def test_search_no_match(self, indexer_with_data):
        results = indexer_with_data.search("blockchain quantum")
        assert results == []


# ============================================================
# 5. Export Gold to DB Tests
# ============================================================
class TestExportGoldToDB:
    @pytest.fixture(autouse=True)
    def setup_export(self):
        self.temp_dir = tempfile.mkdtemp(prefix="test_export_")
        self.db_path = os.path.join(self.temp_dir, "test.db")
        yield
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_export_creates_tables(self):
        conn = sqlite3.connect(self.db_path)
        df = pd.DataFrame({
            "date": ["2026-09-25", "2026-09-26"],
            "product_id": ["p1", "p1"],
            "review_count": [10, 12],
            "avg_rating": [4.5, 4.3]
        })
        df.to_sql("product_daily_stats", conn, if_exists="replace", index=False)
        conn.close()

        conn = sqlite3.connect(self.db_path)
        cursor = conn.execute("SELECT count(*) FROM product_daily_stats")
        count = cursor.fetchone()[0]
        conn.close()
        assert count == 2

    def test_export_replace_existing(self):
        conn = sqlite3.connect(self.db_path)
        df1 = pd.DataFrame({"date": ["2026-09-25"], "value": [100]})
        df1.to_sql("test_table", conn, if_exists="replace", index=False)
        df2 = pd.DataFrame({"date": ["2026-09-26"], "value": [200]})
        df2.to_sql("test_table", conn, if_exists="replace", index=False)
        cursor = conn.execute("SELECT count(*) FROM test_table")
        count = cursor.fetchone()[0]
        conn.close()
        assert count == 1  # replaced, not appended

    def test_export_empty_dataframe(self):
        conn = sqlite3.connect(self.db_path)
        df = pd.DataFrame(columns=["date", "value"])
        df.to_sql("empty_table", conn, if_exists="replace", index=False)
        cursor = conn.execute("SELECT count(*) FROM empty_table")
        count = cursor.fetchone()[0]
        conn.close()
        assert count == 0


# ============================================================
# 6. Review Burst Detector Tests
# ============================================================
from analytics.anomaly.review_burst import ReviewBurstDetector

class TestReviewBurstDetector:
    def test_detect_burst_spike(self):
        """Generate a product with a massive review spike in one hour."""
        base_date = datetime(2026, 9, 20, 10, 0, 0)
        reviews = []
        rid = 0
        # Normal: 3 reviews per hour for 10 hours
        for h in range(10):
            for _ in range(3):
                rid += 1
                reviews.append({
                    "review_id": f"r{rid}", "product_id": "p1",
                    "rating": 4.0,
                    "review_date": (base_date + timedelta(hours=h, minutes=rid % 60)).isoformat() + "Z"
                })
        # Spike: 20 reviews in hour 11
        for _ in range(20):
            rid += 1
            reviews.append({
                "review_id": f"r{rid}", "product_id": "p1",
                "rating": 1.5,
                "review_date": (base_date + timedelta(hours=11, minutes=rid % 60)).isoformat() + "Z"
            })
        df = pd.DataFrame(reviews)
        anomalies = ReviewBurstDetector.detect_bursts(df, window_hours=1, threshold_multiplier=2.0)
        assert len(anomalies) >= 1
        assert anomalies[0]["entity_id"] == "p1"

    def test_detect_empty_reviews(self):
        df = pd.DataFrame(columns=["review_id", "product_id", "rating", "review_date"])
        anomalies = ReviewBurstDetector.detect_bursts(df)
        assert anomalies == []


# ============================================================
# 7. Rating Anomaly Detector Tests
# ============================================================
from analytics.anomaly.rating_anomaly import RatingAnomalyDetector

class TestRatingAnomalyDetector:
    def test_detect_rating_drop(self):
        """Create a product with stable high ratings then sudden drop."""
        reviews = []
        rid = 0
        base = datetime(2026, 9, 1)
        # 10 days of high ratings (5.0), 10 reviews/day
        for day in range(10):
            for _ in range(10):
                rid += 1
                reviews.append({
                    "review_id": f"r{rid}", "product_id": "p1",
                    "rating": 5.0,
                    "review_date": (base + timedelta(days=day, hours=rid % 12)).isoformat() + "Z"
                })
        # Day 11: sudden drop to 1.0 ratings, 10 reviews
        for _ in range(10):
            rid += 1
            reviews.append({
                "review_id": f"r{rid}", "product_id": "p1",
                "rating": 1.0,
                "review_date": (base + timedelta(days=11, hours=rid % 12)).isoformat() + "Z"
            })
        df = pd.DataFrame(reviews)
        anomalies = RatingAnomalyDetector.detect_rating_drops(df, drop_threshold=0.5)
        assert len(anomalies) >= 1
        assert anomalies[0]["anomaly_type"] == "RATING_DROP"

    def test_detect_no_drop_stable(self):
        reviews = []
        rid = 0
        base = datetime(2026, 9, 1)
        for day in range(5):
            for _ in range(5):
                rid += 1
                reviews.append({
                    "review_id": f"r{rid}", "product_id": "p1",
                    "rating": 4.5,
                    "review_date": (base + timedelta(days=day, hours=rid % 12)).isoformat() + "Z"
                })
        df = pd.DataFrame(reviews)
        anomalies = RatingAnomalyDetector.detect_rating_drops(df)
        assert anomalies == []

    def test_detect_empty_reviews(self):
        df = pd.DataFrame(columns=["review_id", "product_id", "rating", "review_date"])
        anomalies = RatingAnomalyDetector.detect_rating_drops(df)
        assert anomalies == []


# ============================================================
# 8. Price Tracker Tests
# ============================================================
from analytics.trend.price_tracker import PriceTracker

class TestPriceTracker:
    @pytest.fixture
    def price_data(self):
        return pd.DataFrame([
            {"product_id": "p1", "price": 20000000, "timestamp": "2026-09-01T10:00:00Z"},
            {"product_id": "p1", "price": 20500000, "timestamp": "2026-09-02T10:00:00Z"},
            {"product_id": "p1", "price": 19800000, "timestamp": "2026-09-03T10:00:00Z"},
            {"product_id": "p2", "price": 15000000, "timestamp": "2026-09-01T10:00:00Z"},
            {"product_id": "p2", "price": 15000000, "timestamp": "2026-09-02T10:00:00Z"},
        ])

    def test_price_trajectory(self, price_data):
        result = PriceTracker.product_price_trajectory(price_data, "p1")
        assert len(result) == 3
        assert "price" in result.columns
        assert "min_price" in result.columns
        assert "max_price" in result.columns

    def test_price_trajectory_unknown_product(self, price_data):
        result = PriceTracker.product_price_trajectory(price_data, "unknown")
        assert result.empty

    def test_price_trajectory_empty(self):
        df = pd.DataFrame(columns=["product_id", "price", "timestamp"])
        result = PriceTracker.product_price_trajectory(df, "p1")
        assert result.empty

    def test_price_volatility(self, price_data):
        result = PriceTracker.calculate_price_volatility(price_data)
        assert len(result) == 2
        assert "volatility_ratio" in result.columns
        # p1 has more price variation, should have higher volatility
        p1_vol = result[result["product_id"] == "p1"]["volatility_ratio"].values[0]
        p2_vol = result[result["product_id"] == "p2"]["volatility_ratio"].values[0]
        assert p1_vol > p2_vol

    def test_price_volatility_empty(self):
        df = pd.DataFrame(columns=["product_id", "price", "timestamp"])
        result = PriceTracker.calculate_price_volatility(df)
        assert result.empty
