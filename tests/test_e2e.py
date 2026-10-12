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

import tempfile
import shutil
from storage.minio_backend import MinIOBackend
from storage.local_backend import LocalBackend
from storage.storage_manager import StorageManager
from scripts.health_check import verify_integration_health
from config.settings import settings


@pytest.fixture(autouse=True)
def reset_default_integration_mode():
    """Ensure every test starts and ends with strict integration mode as the system default."""
    settings.set_runtime_mode("integration")
    storage.set_mode("integration")
    yield
    settings.set_runtime_mode("integration")
    storage.set_mode("integration")


def test_default_mode_is_strict_integration():
    """Verify default configuration is locked to integration mode with real MinIOBackend and no local mirror."""
    assert settings.runtime_mode == "integration"
    assert settings.is_integration_mode is True
    assert storage.mode == "integration"
    assert storage.backend == MinIOBackend
    assert isinstance(storage.backend, MinIOBackend)
    assert storage.backend.bucket == settings.storage.data_lake_bucket
    assert storage.backend.enable_local_mirror is False
    assert storage.backend.fallback_local is False


def test_integration_minio_fails_loudly_without_local_fallback():
    """Acceptance test: when MinIO is stopped/unreachable, MinIOBackend raises ConnectionError and never writes/reads local files."""
    temp_dir = Path(tempfile.mkdtemp(prefix="test_minio_strict_"))
    try:
        backend = MinIOBackend(
            endpoint="127.0.0.1:59998",
            access_key="minioadmin",
            secret_key="minioadmin",
            bucket="sentinel-data",
            local_dir=temp_dir,
        )
        assert backend.enable_local_mirror is False
        assert backend.fallback_local is False

        with pytest.raises(ConnectionError):
            backend.health_check()

        with pytest.raises(ConnectionError):
            backend.write_json("bronze/raw.products/test.json", [{"id": 1}])

        with pytest.raises(ConnectionError):
            backend.read_json("bronze/raw.products/test.json")

        with pytest.raises(ConnectionError):
            backend.write_parquet("silver/products/part-0.parquet", pd.DataFrame([{"id": 1}]))

        with pytest.raises(ConnectionError):
            backend.read_parquet("silver/products/part-0.parquet")

        with pytest.raises(ConnectionError):
            backend.list_objects("bronze/")

        # Confirm zero files were written to temp_dir
        assert list(temp_dir.rglob("*")) == []
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


def test_integration_postgres_fails_loudly_without_sqlite_fallback(monkeypatch):
    """Acceptance test: when PostgreSQL is stopped/unreachable, WarehouseManager and DashboardDB raise ConnectionError and never fall back to SQLite."""
    monkeypatch.setattr(settings.database, "host", "127.0.0.1")
    monkeypatch.setattr(settings.database, "port", 59997)

    with pytest.raises(ConnectionError):
        WarehouseManager(mode="integration")

    with pytest.raises(ConnectionError):
        DashboardDB(mode="integration")


def test_integration_pipeline_fails_loudly_when_services_stopped(monkeypatch):
    """Acceptance test: running pipeline in integration mode when MinIO or PostgreSQL is stopped must fail with ConnectionError."""
    monkeypatch.setattr(settings.storage, "endpoint", "127.0.0.1:59998")
    monkeypatch.setattr(settings.database, "host", "127.0.0.1")
    monkeypatch.setattr(settings.database, "port", 59997)
    storage.set_mode("integration")

    with pytest.raises(ConnectionError):
        verify_integration_health(require_kafka=False)

    with pytest.raises(ConnectionError):
        run_end_to_end(mode="integration")


def test_full_e2e_pipeline():
    # P0-1: Prove default StorageManager.backend == MinIOBackend in integration mode
    assert storage.backend == MinIOBackend
    assert storage.backend.bucket == settings.storage.data_lake_bucket
    assert storage.backend.enable_local_mirror is False

    # Run E2E in explicitly declared local mode for unit testing
    success = run_end_to_end(mode="local")
    assert success is True
    assert storage.mode == "local"
    assert isinstance(storage.backend, LocalBackend)
    
    # P0-2: Verify Bronze raw batches exist
    bronze_topics = storage.list_bronze_topics()
    assert len(bronze_topics) > 0
    bronze_prods = storage.read_bronze_records("raw.products")
    assert len(bronze_prods) > 0

    # P0-3 & P1-1: Verify Silver Parquet generated by Spark Batch exists (products, reviews, prices)
    df_prods = storage.read_silver_parquet("products")
    df_revs = storage.read_silver_parquet("reviews")
    df_prices = storage.read_silver_parquet("prices")
    assert not df_prods.empty
    assert not df_revs.empty
    assert not df_prices.empty
    assert "product_id" in df_prods.columns
    assert "review_id" in df_revs.columns
    assert "price_id" in df_prices.columns

    # P0-4 & P1-1: Verify all 4 Gold Marts generated by SparkGoldBuilder exist
    df_p_gold = storage.read_gold_parquet("product_daily_stats")
    df_b_gold = storage.read_gold_parquet("brand_daily_stats")
    df_c_gold = storage.read_gold_parquet("category_daily_stats")
    df_pr_gold = storage.read_gold_parquet("price_daily_stats")
    assert not df_p_gold.empty
    assert not df_b_gold.empty
    assert not df_c_gold.empty
    assert not df_pr_gold.empty

    # P1-2: Verify Data Quality metrics and Anomaly events persisted
    df_dq = storage.read_gold_parquet("data_quality_metrics")
    assert not df_dq.empty
    df_anoms = storage.read_gold_parquet("anomaly_events")
    assert not df_anoms.empty

    # P0-5 & P1-4: Verify Warehouse and Dashboard connection in explicit local mode
    db = DashboardDB(mode="local")
    stats = db.get_product_daily_stats()
    assert not stats.empty
    kpis = db.get_kpi_summary()
    assert isinstance(kpis, dict)
    assert kpis["total_products"] > 0
    assert not db.get_price_daily_stats().empty
    assert not db.get_anomaly_events().empty
    assert not db.get_data_quality_metrics().empty

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
    """Verify consumer creates partitioned directory structure in Bronze storage (explicit local mode)."""
    storage.set_mode("local")
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
    storage.set_mode("local")
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
    wm = WarehouseManager(use_postgres=False, mode="local")
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
    """Verify DashboardDB connector can query metrics in explicit local mode."""
    db = DashboardDB(mode="local")
    stats = db.get_product_daily_stats()
    assert isinstance(stats, pd.DataFrame)
    kpis = db.get_kpi_summary()
    assert isinstance(kpis, dict)
    assert "total_products" in kpis

def test_spark_data_quality_evaluator():
    """P1-2: Verify SparkDataQualityEvaluator metrics calculation and quarantine routing."""
    from spark.quality.spark_dq import SparkDataQualityEvaluator
    storage.set_mode("local")
    
    test_df = pd.DataFrame([
        {"id": "1", "val": 10},
        {"id": "2", "val": None},
        {"id": "1", "val": 10}  # duplicate
    ])
    metrics = SparkDataQualityEvaluator.evaluate(test_df, "test_dataset", primary_key="id")
    assert metrics["total_count"] == 3
    assert metrics["null_count"] == 1
    assert metrics["duplicate_count"] == 1
    assert metrics["dq_score"] < 100.0

    report = SparkDataQualityEvaluator.generate_report(metrics, batch_id="batch_test_01")
    assert report["batch_id"] == "batch_test_01"
    assert report["dataset_name"] == "test_dataset"

    quarantine_path = SparkDataQualityEvaluator.save_quarantine("test_dataset", test_df.iloc[[1]])
    assert quarantine_path is not None
    assert "quarantine/test_dataset" in quarantine_path

def test_prometheus_metrics_exporter():
    """P2-2: Verify Prometheus metrics exporter produces standard exposition telemetry in explicit local mode."""
    from monitoring.metrics_exporter import SentinelMetricsExporter
    storage.set_mode("local")

    data = SentinelMetricsExporter.collect_all_metrics(mode="local")
    assert "datalake" in data
    assert "warehouse" in data
    assert "bronze_bytes" in data["datalake"]

    prom_text = SentinelMetricsExporter.generate_prometheus_metrics(mode="local")
    assert "sentinel_datalake_size_bytes" in prom_text
    assert "sentinel_data_quality_score" in prom_text
    assert "sentinel_warehouse_records" in prom_text

def test_airflow_dag_standalone_execution():
    """P2-1: Verify Airflow DAG executes sequentially in explicit local mode without throwing errors."""
    from orchestration.dags.sentinel_daily_pipeline import run_dag_standalone, dag
    assert dag.dag_id == "sentinel_daily_pipeline"
    # Execute full standalone run in explicit local mode
    run_dag_standalone(mode="local")

def test_lakehouse_table_time_travel_and_upsert():
    """P2-3: Verify Lakehouse ACID transaction logs, Time Travel, and Upsert capabilities."""
    from storage.lakehouse import LakehouseTable
    storage.set_mode("local")

    table = LakehouseTable("test_lakehouse_entity", layer="silver")
    
    # Version 0: initial write
    df_v0 = pd.DataFrame([
        {"id": "rec_1", "status": "PENDING", "price": 100.0},
        {"id": "rec_2", "status": "PENDING", "price": 200.0}
    ])
    v0 = table.write(df_v0, mode="overwrite")
    assert v0 >= 0
    read_v0 = table.read(version=v0)
    assert len(read_v0) == 2

    # Version 1: Upsert updating rec_1 and adding rec_3
    df_upsert = pd.DataFrame([
        {"id": "rec_1", "status": "COMPLETED", "price": 150.0},
        {"id": "rec_3", "status": "PENDING", "price": 300.0}
    ])
    v1 = table.upsert(df_upsert, merge_keys=["id"])
    assert v1 > v0

    # Query latest (v1)
    read_latest = table.read()
    assert len(read_latest) == 3
    rec1_row = read_latest[read_latest["id"] == "rec_1"].iloc[0]
    assert rec1_row["status"] == "COMPLETED"
    assert rec1_row["price"] == 150.0

    # Time Travel query back to v0
    time_travel_v0 = table.read(version=v0)
    assert len(time_travel_v0) == 2
    rec1_v0 = time_travel_v0[time_travel_v0["id"] == "rec_1"].iloc[0]
    assert rec1_v0["status"] == "PENDING"
    assert rec1_v0["price"] == 100.0

    # History audit check
    history = table.get_history()
    assert len(history) >= 2
    assert any(h.get("operation") == "MERGE_UPSERT" for h in history)

def test_kafka_bronze_acceptance_3_valid_1_invalid():
    """Acceptance test: send 3 valid messages and 1 invalid message through Kafka pipeline.
    Verify 3 appear in Bronze, 1 in DLQ, offsets are committed correctly, and envelopes are intact.
    """
    from scripts.verify_kafka_bronze_acceptance import run_acceptance_verification
    assert run_acceptance_verification(mode="local") is True

def test_raw_consumer_consume_batch_once_empty_poll():
    """Verify consume_batch_once handles empty polls gracefully."""
    consumer = RawConsumer()
    res = consumer.consume_batch_once(topics=["raw.reviews"], offline_records=[])
    assert res["records_consumed"] == 0
    assert res["records_valid"] == 0
    assert res["records_dlq"] == 0
    assert res["batches_written"] == []
    assert res["committed_offsets"] == {}

def test_topic_manager_provisioning():
    """Verify TopicManager lists canonical topics and handles provisioning idempotently."""
    from kafka.topics.topic_manager import TopicManager
    defs = TopicManager.get_topic_definitions()
    topic_names = [d[0] for d in defs]
    assert "dead-letter" in topic_names
    assert settings.kafka.topic_reviews in topic_names

    res = TopicManager.ensure_topics(mode="local")
    assert res["status"] in ("ok", "simulated")


def test_e2e_stage_verification_fails_fast_on_invalid_stage():
    """Acceptance test: verify that stage verification raises RuntimeError immediately when any stage check fails."""
    from scripts.run_e2e_pipeline import (
        stage_1_ingest_sources,
        stage_3_verify_bronze,
    )
    storage.set_mode("local")

    # 1. Stage 1 must fail if source file does not exist
    with pytest.raises(RuntimeError, match="Stage 1 failed"):
        stage_1_ingest_sources(products_path="non_existent_products_999.json")

    # 2. Stage 3 must fail if Bronze batch list is empty or missing from storage
    with pytest.raises(RuntimeError, match="Stage 3 verification failed"):
        stage_3_verify_bronze(
            consume_result={"batches_written": [], "batches_by_topic": {}, "topic_counts": {}, "records_valid": 0},
            effective_mode="local"
        )

    with pytest.raises(RuntimeError, match="Stage 3 verification failed"):
        stage_3_verify_bronze(
            consume_result={
                "batches_written": ["bronze/raw.products/missing_batch.json"],
                "batches_by_topic": {"raw.products": ["bronze/raw.products/missing_batch.json"]},
                "topic_counts": {"raw.products": 1},
                "records_valid": 1
            },
            effective_mode="local"
        )


def test_e2e_idempotency_no_logical_duplicates_on_rerun():
    """Acceptance test: running the full E2E pipeline twice with identical data must not duplicate logical records."""
    assert run_end_to_end(mode="local") is True

    silver_prods_1 = len(storage.read_silver_parquet("products"))
    silver_revs_1 = len(storage.read_silver_parquet("reviews"))
    silver_prices_1 = len(storage.read_silver_parquet("prices"))

    gold_p_1 = len(storage.read_gold_parquet("product_daily_stats"))
    gold_b_1 = len(storage.read_gold_parquet("brand_daily_stats"))
    gold_c_1 = len(storage.read_gold_parquet("category_daily_stats"))
    gold_pr_1 = len(storage.read_gold_parquet("price_daily_stats"))
    gold_dq_1 = len(storage.read_gold_parquet("data_quality_metrics"))

    db1 = DashboardDB(mode="local")
    kpis_1 = db1.get_kpi_summary()
    wh_p_1 = len(db1.get_product_daily_stats())
    wh_b_1 = len(db1.get_brand_daily_stats())
    wh_c_1 = len(db1.get_category_daily_stats())
    wh_pr_1 = len(db1.get_price_daily_stats())
    db1.close()

    # Second run with identical input data
    assert run_end_to_end(mode="local") is True

    assert len(storage.read_silver_parquet("products")) == silver_prods_1
    assert len(storage.read_silver_parquet("reviews")) == silver_revs_1
    assert len(storage.read_silver_parquet("prices")) == silver_prices_1

    assert len(storage.read_gold_parquet("product_daily_stats")) == gold_p_1
    assert len(storage.read_gold_parquet("brand_daily_stats")) == gold_b_1
    assert len(storage.read_gold_parquet("category_daily_stats")) == gold_c_1
    assert len(storage.read_gold_parquet("price_daily_stats")) == gold_pr_1
    assert len(storage.read_gold_parquet("data_quality_metrics")) == gold_dq_1

    db2 = DashboardDB(mode="local")
    kpis_2 = db2.get_kpi_summary()
    assert len(db2.get_product_daily_stats()) == wh_p_1
    assert len(db2.get_brand_daily_stats()) == wh_b_1
    assert len(db2.get_category_daily_stats()) == wh_c_1
    assert len(db2.get_price_daily_stats()) == wh_pr_1
    assert kpis_2["total_products"] == kpis_1["total_products"]
    assert kpis_2["total_reviews"] == kpis_1["total_reviews"]
    db2.close()


def test_unified_entrypoints_share_canonical_pipeline():
    """Verify bootstrap.py, run_batch.py, and sentinel_daily_pipeline.py delegate to scripts.run_e2e_pipeline."""
    import scripts.bootstrap as bootstrap_mod
    import scripts.run_batch as run_batch_mod
    import orchestration.dags.sentinel_daily_pipeline as dag_mod

    assert bootstrap_mod.run_end_to_end is run_end_to_end
    assert run_batch_mod.run_end_to_end is run_end_to_end
    assert hasattr(dag_mod, "stage_4_spark_batch_silver_dq")


def test_dq_field_nulls_vs_row_invalid_and_parse_cast():
    """Verify field-level null statistics are separated from row-level invalid count and parse/cast runs before validation."""
    from spark.quality.spark_dq import SparkDataQualityEvaluator

    df = pd.DataFrame([
        # Valid row with 2 null optional fields
        {"review_id": "r1", "product_id": "p1", "rating": "5.0", "review_text": "Great", "review_title": None, "helpful_count": None},
        # Single invalid row with 3 null required fields + 2 null optional fields (5 cell nulls, 1 invalid row)
        {"review_id": "r2", "product_id": None, "rating": None, "review_text": "  ", "review_title": None, "helpful_count": None},
        # Malformed rating type string (0 cell nulls before cast, 1 cast error row)
        {"review_id": "r3", "product_id": "p1", "rating": "invalid_num", "review_text": "Text", "review_title": "T", "helpful_count": 1},
        # Duplicate primary key r1
        {"review_id": "r1", "product_id": "p1", "rating": "4.0", "review_text": "Dup", "review_title": "T2", "helpful_count": 2},
    ])

    metrics = SparkDataQualityEvaluator.evaluate(df, "reviews", primary_key="review_id")
    assert metrics["total_count"] == 4
    assert metrics["field_null_counts"]["review_title"] == 2
    assert metrics["field_null_counts"]["helpful_count"] == 2
    assert metrics["field_null_counts"]["product_id"] == 1
    assert metrics["field_null_counts"]["rating"] == 1
    assert metrics["field_null_counts"]["review_text"] == 1
    assert metrics["total_field_nulls"] == 7
    assert metrics["null_row_count"] == 1
    assert metrics["cast_error_count"] == 1
    assert metrics["quarantine_count"] == 2
    assert metrics["duplicate_count"] == 1
    assert metrics["invalid_count"] == 3
    assert metrics["valid_count"] == 1
    assert metrics["valid_count"] + metrics["invalid_count"] == metrics["total_count"]


def test_dq_gate_blocks_gold_when_below_threshold():
    """Verify configurable DQ threshold gate raises DataQualityGateError and blocks Gold mart generation."""
    from spark.quality.spark_dq import SparkDataQualityEvaluator, DataQualityGateError
    from spark.analytics.gold_builder import SparkGoldBuilder

    low_dq_metrics = {
        "dataset_name": "reviews",
        "total_count": 10,
        "valid_count": 5,
        "invalid_count": 5,
        "duplicate_count": 0,
        "null_row_count": 5,
        "dq_score": 50.0,
    }

    with pytest.raises(DataQualityGateError, match="Data Quality Gate blocked Gold generation"):
        SparkDataQualityEvaluator.check_dq_gate(low_dq_metrics, min_dq_score=90.0, raise_on_failure=True)

    with pytest.raises(DataQualityGateError, match="Data Quality Gate blocked Gold generation"):
        SparkGoldBuilder().build_all(
            dq_metrics=[low_dq_metrics],
            enforce_dq_gate=True,
            min_dq_score=90.0,
            save_gold=False,
        )


def test_streaming_late_data_malformed_types_dedup_and_restart():
    """Acceptance test: verify streaming handles late data, malformed types, duplicate keys, and checkpoint restart."""
    from scripts.verify_streaming_dq_acceptance import run_streaming_dq_acceptance
    assert run_streaming_dq_acceptance(mode="local") is True


def test_pii_anonymization_and_user_id_hashing_at_ingest():
    """Verify raw user_id / customer_id and PII fields are hashed or removed immediately at ingest."""
    import json
    from ingestion.privacy import anonymize_user_id, is_anonymized_user_id, sanitize_record_pii
    from ingestion.file_loader.loader import FileLoader
    from ingestion.crawler.tiki import TikiCrawler
    from ingestion.crawler.mock import MockEcommerceCrawler
    from data.generator import generate_products, generate_reviews

    temp_dir = Path(tempfile.mkdtemp(prefix="test_pii_ingest_"))
    try:
        # 1. Deterministic SHA-256 hashing & idempotency
        hashed_1 = anonymize_user_id("raw_user_998877")
        hashed_2 = anonymize_user_id("raw_user_998877")
        assert hashed_1 == hashed_2
        assert hashed_1 != "raw_user_998877"
        assert is_anonymized_user_id(hashed_1) is True
        assert anonymize_user_id(hashed_1) == hashed_1
        assert anonymize_user_id("raw_user_998877", remove=True) == "anon_redacted"

        # 2. Record sanitization strips email/customer_name/phone and hashes user_id
        raw_rec = {
            "review_id": "r_pii_1",
            "product_id": "p1",
            "customer_id": "cust_555",
            "customer_name": "Nguyen Van A",
            "email": "nguyenvana@gmail.com",
            "phone": "0901234567",
            "rating": 5.0,
            "review_text": "Good",
        }
        clean_rec = sanitize_record_pii(raw_rec)
        assert is_anonymized_user_id(clean_rec["user_id"]) is True
        assert "customer_id" not in clean_rec
        assert "customer_name" not in clean_rec
        assert "email" not in clean_rec
        assert "phone" not in clean_rec

        # 3. FileLoader hashes user_id and strips PII immediately on load
        raw_file = temp_dir / "raw_reviews.json"
        raw_file.write_text(json.dumps([raw_rec]), encoding="utf-8")
        loaded = FileLoader.load_records(raw_file)
        assert len(loaded) == 1
        assert is_anonymized_user_id(loaded[0]["user_id"]) is True
        assert "email" not in loaded[0]
        assert "customer_name" not in loaded[0]

        # 4. Crawlers & generator emit only anonymized user_ids
        tiki_rev = TikiCrawler().standardize_review({"id": 10, "product_id": 20, "customer_id": "tiki_raw_42", "rating": 5, "title": "T", "content": "C"})
        assert is_anonymized_user_id(tiki_rev["user_id"]) is True

        mock_crawl = MockEcommerceCrawler(seed=42).crawl_category("laptop", max_products=2, reviews_per_product=3)
        assert len(mock_crawl["reviews"]) == 6
        assert all(is_anonymized_user_id(r["user_id"]) and "customer_email" not in r for r in mock_crawl["reviews"])

        gen_revs = generate_reviews(generate_products(seed=42), count=25, inject_anomalies=False, seed=42)
        assert all(is_anonymized_user_id(r["user_id"]) for r in gen_revs)
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


def test_e2e_pipeline_with_crawler_and_public_dataset():
    """Verify canonical 6-stage E2E pipeline runs end-to-end with both Crawler and Public Dataset sources."""
    from ingestion.privacy import is_anonymized_user_id

    assert run_end_to_end(
        mode="local",
        source_type="public_dataset",
    ) is True
    df_revs_pub = storage.read_silver_parquet("reviews")
    assert not df_revs_pub.empty
    assert all(is_anonymized_user_id(uid) for uid in df_revs_pub["user_id"].dropna())

    assert run_end_to_end(
        mode="local",
        source_type="crawler",
        crawler_platform="mock",
        crawler_query="iphone 15",
        crawler_max_products=3,
        crawler_reviews_per_product=5,
    ) is True
    df_revs_crawl = storage.read_silver_parquet("reviews")
    assert not df_revs_crawl.empty
    assert all(is_anonymized_user_id(uid) for uid in df_revs_crawl["user_id"].dropna())


def test_real_benchmark_execution_and_report_consistency():
    """Verify benchmark.py executes Volume, Velocity, and Variety on the pipeline and generates matching reports without fake 1M claims."""
    import json
    from scripts.benchmark import run_all_benchmarks

    res = run_all_benchmarks(
        mode="local",
        seed=42,
        volume_sizes=[50, 100],
        velocity_rates=[50],
        variety_count=40,
        dirty_ratio=0.05,
        duplicate_ratio=0.02,
    )

    assert len(res["volume"]) == 2
    for v in res["volume"]:
        assert v["seed"] > 0
        assert v["records_input"] in (50, 100)
        assert v["valid_count"] > 0
        assert v["quarantine_count"] > 0
        assert v["duration_sec"] > 0
        assert v["throughput_rps"] > 0
        assert v["bronze_bytes"] > 0
        assert v["silver_bytes"] > 0
        assert v["quarantine_bytes"] > 0

    assert len(res["velocity"]) == 1
    vel = res["velocity"][0]
    assert vel["messages_sent"] >= 100
    assert vel["messages_consumed"] == vel["messages_sent"]
    assert vel["valid_count"] + vel["quarantine_dlq_count"] == vel["messages_consumed"]
    assert vel["bronze_bytes"] > 0

    assert len(res["variety"]) == 6
    formats = {r["format"] for r in res["variety"]}
    assert {"JSON", "JSONL", "CSV", "TSV", "Parquet", "HTML_JSONLD_Crawler"} == formats
    for r in res["variety"]:
        assert r["valid_count"] > 0
        assert r["quarantine_count"] > 0
        assert r["silver_bytes"] > 0
        assert r["pii_anonymized"] is True

    # Verify generated report files match JSON output and do not advertise unrun 1,000,000-record metrics
    json_report = json.loads((REPO_ROOT / "docs" / "reports" / "benchmark_3v.json").read_text(encoding="utf-8"))
    md_report = (REPO_ROOT / "docs" / "reports" / "benchmark_report.md").read_text(encoding="utf-8")
    doc_bench = (REPO_ROOT / "docs" / "benchmark.md").read_text(encoding="utf-8")

    assert json_report["timestamp"] == res["timestamp"]
    assert res["timestamp"] in md_report
    assert res["timestamp"] in doc_bench
    assert "1,000,000" not in md_report
    assert "1,000,000" not in doc_bench


def test_github_actions_ci_workflow_valid():
    """Verify .github/workflows/ci.yml exists, is valid YAML, and defines lint, unit-tests, and integration-tests jobs."""
    import yaml

    ci_path = REPO_ROOT / ".github" / "workflows" / "ci.yml"
    assert ci_path.exists()
    ci_data = yaml.safe_load(ci_path.read_text(encoding="utf-8"))
    assert "jobs" in ci_data
    jobs = ci_data["jobs"]
    assert "lint" in jobs
    assert "unit-tests" in jobs
    assert "integration-tests" in jobs



