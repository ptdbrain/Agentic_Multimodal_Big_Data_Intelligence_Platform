import pytest
import sys
import pandas as pd
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from scripts.run_e2e_pipeline import run_end_to_end
from storage.storage_manager import storage
from kafka.producers.stream_producer import StreamProducer
from kafka.consumers.raw_consumer import RawConsumer
from kafka.dlq.dead_letter import DLQHandler
from data.schemas.validator import SchemaValidator
from database.warehouse import WarehouseManager
from spark.etl.price_pipeline import PricePipeline
from dashboard.db_connector import DashboardDB

def test_full_e2e_pipeline():
    success = run_end_to_end()
    assert success is True
    
    # Verify Silver Parquet exists
    df_prods = storage.read_silver_parquet("products")
    df_revs = storage.read_silver_parquet("reviews")
    assert not df_prods.empty
    assert not df_revs.empty
    assert "product_id" in df_prods.columns
    assert "review_id" in df_revs.columns

    # Verify Gold Marts exist
    df_gold = storage.read_gold_parquet("product_daily_stats")
    assert not df_gold.empty

def test_kafka_producer_envelope_structure():
    """Verify that messages produced are wrapped in canonical envelope metadata."""
    producer = StreamProducer(enable_offline_buffer=True)
    sample_records = [
        {"review_id": "rv_test_001", "product_id": "p_01", "rating": 5.0, "review_text": "Superb"}
    ]
    sent, rate = producer.produce_batch(
        topic="raw.reviews",
        records=sample_records,
        rate=100.0,
        event_type="NEW_REVIEW",
        source="test_runner"
    )
    assert sent == 1
    assert len(producer.virtual_queue) >= 1
    
    msg = producer.virtual_queue[-1]["value"]
    assert "event_id" in msg
    assert msg["event_type"] == "NEW_REVIEW"
    assert msg["source"] == "test_runner"
    assert "ingested_at" in msg
    assert "payload" in msg
    assert msg["payload"]["review_id"] == "rv_test_001"

def test_kafka_producer_fail_loudly_when_offline():
    """Verify that when offline buffer is disabled and broker is unreachable, producer raises error."""
    # Using an invalid host should cause fail-loud behavior
    with pytest.raises(Exception):
        producer = StreamProducer(
            bootstrap_servers="127.0.0.1:59999",
            enable_offline_buffer=False
        )

def test_raw_consumer_bronze_partitioning():
    """Verify consumer creates partitioned directory structure in Bronze storage."""
    consumer = RawConsumer()
    test_batch = [
        {"product_id": "prod_x", "product_name": "Test Item", "timestamp": "2026-10-02T10:00:00Z"}
    ]
    out_file = consumer.save_raw_batch("raw.products", test_batch)
    assert out_file.exists()
    assert "year=2026" in str(out_file)
    assert "raw.products" in str(out_file)
    assert out_file.suffix == ".json"

def test_schema_validator_and_quarantine():
    """Verify SchemaValidator detects valid vs malformed records."""
    valid_record = {
        "price_id": "pr_001",
        "product_id": "prod_01",
        "price": 150000.0,
        "currency": "VND",
        "timestamp": "2026-10-02T10:00:00Z"
    }
    is_valid, errs = SchemaValidator.validate_record(valid_record, "price")
    assert is_valid is True
    assert len(errs) == 0

    invalid_record = {
        "price_id": "pr_002",
        # missing product_id
        "price": -50.0,  # invalid negative price
        "currency": "VND"
    }
    is_valid, errs = SchemaValidator.validate_record(invalid_record, "price")
    assert is_valid is False
    assert len(errs) > 0

    valid_list, invalid_list = SchemaValidator.validate_batch([valid_record, invalid_record], "price")
    assert len(valid_list) == 1
    assert len(invalid_list) == 1

def test_dead_letter_queue_handling():
    """Verify DLQ handler structures failed payloads properly."""
    dlq = DLQHandler()
    res = dlq.send_to_dlq(
        error_type="SCHEMA_VALIDATION_ERROR",
        source_topic="raw.reviews",
        partition=0,
        offset=42,
        payload={"corrupt": "data"},
        errors=["missing review_text"]
    )
    assert res is not None
    assert res["error_type"] == "SCHEMA_VALIDATION_ERROR"
    assert res["source_topic"] == "raw.reviews"
    assert "timestamp" in res

def test_warehouse_manager_upsert_idempotency():
    """Verify warehouse upsert operations are idempotent and do not duplicate primary keys."""
    wm = WarehouseManager(use_postgres=False)
    wm.initialize_schema()
    
    test_df = pd.DataFrame([
        {
            "date": "2026-10-01",
            "product_id": "prod_upsert_test",
            "review_count": 10,
            "avg_rating": 4.5,
            "min_price": 100000.0,
            "max_price": 100000.0,
            "avg_price": 100000.0,
            "rating_std": 0.2,
            "positive_ratio": 0.9,
            "negative_ratio": 0.1
        }
    ])
    
    # First insert
    affected1 = wm.upsert_dataframe("product_daily_stats", test_df, conflict_keys=["date", "product_id"])
    assert affected1 == 1
    
    # Second insert with updated avg_rating
    test_df_updated = test_df.copy()
    test_df_updated["avg_rating"] = 4.8
    affected2 = wm.upsert_dataframe("product_daily_stats", test_df_updated, conflict_keys=["date", "product_id"])
    assert affected2 == 1
    
    # Query to verify single row with updated rating
    res = wm.query("SELECT * FROM product_daily_stats WHERE product_id = 'prod_upsert_test'")
    assert len(res) == 1
    assert float(res["avg_rating"].iloc[0]) == 4.8

def test_price_pipeline_execution():
    """Verify PricePipeline cleans, validates, and standardizes price records."""
    raw_prices = [
        {"price_id": "pr_1", "product_id": "p_1", "price": "250000", "currency": "VND", "timestamp": "2026-10-02T10:00:00Z"},
        {"price_id": "pr_2", "product_id": "p_2", "price": -100, "currency": "VND", "timestamp": "2026-10-02T10:00:00Z"},
        {"price_id": "pr_3", "product_id": None, "price": 50000, "currency": "VND", "timestamp": "2026-10-02T10:00:00Z"}
    ]
    res = PricePipeline.run_pipeline(raw_prices, save_silver=False)
    assert res["prices_processed"] == 1  # Only pr_1 is valid
    assert res["prices_invalid"] == 2

def test_dashboard_db_connector():
    """Verify DashboardDB connector can query metrics."""
    db = DashboardDB()
    stats = db.get_product_daily_stats()
    assert isinstance(stats, pd.DataFrame)
    kpis = db.get_kpi_summary()
    assert isinstance(kpis, dict)
    assert "total_products" in kpis
