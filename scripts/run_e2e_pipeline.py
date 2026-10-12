"""SentinelAI Canonical End-to-End Pipeline Entrypoint.

Canonical 6-Stage Execution Path:
  Stage 1: Source / Dataset / Crawler
  Stage 2: Kafka Producer -> Kafka -> RawConsumer
  Stage 3: MinIO Bronze Verification
  Stage 4: Spark Batch -> Silver / Quarantine / DQ
  Stage 5: Spark Gold Marts & Anomaly Detection
  Stage 6: PostgreSQL Warehouse -> Dashboard Verification

Enforces strict count and content verification at every stage, failing fast if any check fails,
and guarantees idempotency (re-running with identical data produces zero duplicate logical records).
"""
import sys
import os
import hashlib
import argparse
from pathlib import Path
from typing import Optional, Dict, Any, List
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from config.settings import settings
from storage.storage_manager import storage
from storage.minio_backend import MinIOBackend
from storage.local_backend import LocalBackend
from ingestion.file_loader.loader import FileLoader
from ingestion.crawler.registry import CrawlerRegistry
from ingestion.privacy import sanitize_records_pii, is_anonymized_user_id, _RAW_PII_FIELDS
from kafka.topics.topic_manager import TopicManager
from kafka.producers.stream_producer import StreamProducer
from kafka.consumers.raw_consumer import RawConsumer
from spark.batch.spark_etl_job import SparkBatchETLJob
from spark.analytics.gold_builder import SparkGoldBuilder
from database.export_gold import export_gold_to_db
from database.warehouse import WarehouseManager
from dashboard.db_connector import DashboardDB
from analytics.descriptive.stats import DescriptiveStats
from analytics.anomaly.review_burst import ReviewBurstDetector
from analytics.anomaly.price_anomaly import PriceAnomalyDetector
from analytics.anomaly.rating_anomaly import RatingAnomalyDetector
from scripts.health_check import verify_integration_health


def configure_pipeline_mode(mode: Optional[str] = None) -> str:
    """Configures runtime mode across settings and storage, running pre-flight checks in integration mode."""
    effective_mode = (mode or os.environ.get("RUNTIME_MODE", settings.runtime_mode)).strip().lower()
    is_integration = effective_mode in ("integration", "production")

    settings.set_runtime_mode(effective_mode)
    storage.set_mode(effective_mode)

    if is_integration:
        verify_integration_health(require_kafka=False)
        if not isinstance(storage.backend, MinIOBackend):
            raise RuntimeError(f"Expected MinIOBackend in integration mode, got {type(storage.backend).__name__}")
        if storage.backend.enable_local_mirror:
            raise RuntimeError("Local mirror must be disabled in integration mode.")
    else:
        print(f"[Notice] Running in explicit '{effective_mode}' mode (LocalBackend + SQLite).")

    return effective_mode


def stage_1_ingest_sources(
    products_path: Optional[str] = None,
    reviews_path: Optional[str] = None,
    prices_path: Optional[str] = None,
    source_type: str = "dataset",
    crawler_platform: str = "tiki",
    crawler_query: str = "iphone 15",
    crawler_max_products: int = 5,
    crawler_reviews_per_product: int = 10,
    remove_user_id: bool = False,
) -> Dict[str, Any]:
    """Stage 1: Source / Dataset / Crawler ingestion with PII anonymization and content verification."""
    resolved_source_type = (source_type or "dataset").strip().lower()
    print(f"\n[Stage 1/6] Source / Dataset / Crawler Ingestion (source_type={resolved_source_type})...")

    if resolved_source_type == "crawler":
        crawler = CrawlerRegistry.get(crawler_platform, remove_user_id=remove_user_id)
        crawl_res = crawler.crawl_category(
            query=crawler_query,
            max_products=crawler_max_products,
            reviews_per_product=crawler_reviews_per_product,
        )
        products_raw = crawl_res.get("products", [])
        reviews_raw = crawl_res.get("reviews", [])
        prices_raw = crawl_res.get("prices", [])
        src_prods_ref = f"crawler://{crawler_platform}/{crawler_query}/products"
        src_revs_ref = f"crawler://{crawler_platform}/{crawler_query}/reviews"
        src_prices_ref = f"crawler://{crawler_platform}/{crawler_query}/prices"
    elif resolved_source_type == "public_dataset":
        pub_dir = REPO_ROOT / "data" / "public"
        sample_prods_path = Path(products_path or (pub_dir / "ecommerce_public_products.json"))
        sample_revs_path = Path(reviews_path or (pub_dir / "ecommerce_public_reviews.jsonl"))
        sample_prices_path = Path(prices_path or (pub_dir / "ecommerce_public_prices.csv"))
        if not sample_prods_path.exists() or not sample_revs_path.exists():
            raise RuntimeError(
                f"Stage 1 failed: Public dataset files missing ({sample_prods_path}, {sample_revs_path})."
            )
        products_raw = FileLoader.load_records(sample_prods_path, anonymize_pii=True, remove_user_id=remove_user_id)
        reviews_raw = FileLoader.load_records(sample_revs_path, anonymize_pii=True, remove_user_id=remove_user_id)
        prices_raw = (
            FileLoader.load_records(sample_prices_path, anonymize_pii=True, remove_user_id=remove_user_id)
            if sample_prices_path.exists()
            else []
        )
        src_prods_ref = str(sample_prods_path)
        src_revs_ref = str(sample_revs_path)
        src_prices_ref = str(sample_prices_path)
    else:
        sample_prods_path = Path(products_path or (REPO_ROOT / "data" / "sample" / "products.json"))
        sample_revs_path = Path(reviews_path or (REPO_ROOT / "data" / "sample" / "reviews.json"))
        sample_prices_path = Path(prices_path or (REPO_ROOT / "data" / "sample" / "prices.json"))

        if not sample_prods_path.exists() or not sample_revs_path.exists():
            raise RuntimeError(
                f"Stage 1 failed: Required source files missing ({sample_prods_path}, {sample_revs_path})."
            )

        products_raw = FileLoader.load_records(sample_prods_path, anonymize_pii=True, remove_user_id=remove_user_id)
        reviews_raw = FileLoader.load_records(sample_revs_path, anonymize_pii=True, remove_user_id=remove_user_id)
        prices_raw = (
            FileLoader.load_records(sample_prices_path, anonymize_pii=True, remove_user_id=remove_user_id)
            if sample_prices_path.exists()
            else []
        )
        src_prods_ref = str(sample_prods_path)
        src_revs_ref = str(sample_revs_path)
        src_prices_ref = str(sample_prices_path)

    # Guarantee ingest-time PII sanitization on all records
    products_raw = sanitize_records_pii(products_raw, remove_user_id=remove_user_id)
    reviews_raw = sanitize_records_pii(reviews_raw, remove_user_id=remove_user_id)

    # --- Stage 1 Verification: Count, Content & PII Sanitization ---
    if not isinstance(products_raw, list) or len(products_raw) == 0:
        raise RuntimeError("Stage 1 verification failed: products source dataset is empty.")
    if not isinstance(reviews_raw, list) or len(reviews_raw) == 0:
        raise RuntimeError("Stage 1 verification failed: reviews source dataset is empty.")

    req_prod_keys = {"product_id", "product_name", "price"}
    if not all(isinstance(r, dict) and req_prod_keys.issubset(r.keys()) for r in products_raw):
        raise RuntimeError(f"Stage 1 verification failed: products records missing required keys {req_prod_keys}.")

    req_rev_keys = {"review_id", "product_id", "rating", "review_text"}
    if not all(isinstance(r, dict) and req_rev_keys.issubset(r.keys()) for r in reviews_raw):
        raise RuntimeError(f"Stage 1 verification failed: reviews records missing required keys {req_rev_keys}.")

    # Verify PII anonymization at ingest boundary
    forbidden_pii_keys = set(_RAW_PII_FIELDS)
    for r in reviews_raw:
        if forbidden_pii_keys.intersection(r.keys()):
            raise RuntimeError(
                f"Stage 1 PII verification failed: Raw PII fields {forbidden_pii_keys.intersection(r.keys())} leaked into review record."
            )
        if not is_anonymized_user_id(r.get("user_id")):
            raise RuntimeError(
                f"Stage 1 PII verification failed: Review user_id '{r.get('user_id')}' was not hashed/anonymized at ingest."
            )

    if prices_raw:
        req_price_keys = {"price_id", "product_id", "price", "timestamp"}
        if not all(isinstance(r, dict) and req_price_keys.issubset(r.keys()) for r in prices_raw):
            raise RuntimeError(f"Stage 1 verification failed: prices records missing required keys {req_price_keys}.")

    print(
        f"[Stage 1 OK] Loaded, PII-sanitized, and verified {len(products_raw)} products, "
        f"{len(reviews_raw)} reviews, {len(prices_raw)} prices from '{resolved_source_type}'."
    )
    return {
        "source_type": resolved_source_type,
        "products_raw": products_raw,
        "reviews_raw": reviews_raw,
        "prices_raw": prices_raw,
        "products_path": src_prods_ref,
        "reviews_path": src_revs_ref,
        "prices_path": src_prices_ref,
    }


def stage_2_kafka_produce_and_consume(
    products_raw: List[Dict[str, Any]],
    reviews_raw: List[Dict[str, Any]],
    prices_raw: List[Dict[str, Any]],
    effective_mode: str = "integration"
) -> Dict[str, Any]:
    """Stage 2: Kafka Producer -> Kafka -> RawConsumer with delivery & offset verification."""
    print("\n[Stage 2/6] Kafka Producer -> Kafka -> RawConsumer...")
    is_local = effective_mode not in ("integration", "production")

    TopicManager.ensure_topics(mode=effective_mode)

    producer = StreamProducer(enable_offline_buffer=is_local)
    p_sent, _ = producer.produce_batch(
        settings.kafka.topic_products, products_raw, rate=0, event_type="NEW_PRODUCT"
    )
    r_sent, _ = producer.produce_batch(
        settings.kafka.topic_reviews, reviews_raw, rate=0, event_type="NEW_REVIEW"
    )
    pr_sent = 0
    if prices_raw:
        pr_sent, _ = producer.produce_batch(
            settings.kafka.topic_prices, prices_raw, rate=0, event_type="PRICE_UPDATE"
        )
    producer.close()

    total_sent = p_sent + r_sent + pr_sent
    expected_sent = len(products_raw) + len(reviews_raw) + len(prices_raw)
    if p_sent != len(products_raw) or r_sent != len(reviews_raw) or pr_sent != len(prices_raw):
        raise RuntimeError(
            f"Stage 2 verification failed: Producer delivery mismatch "
            f"(sent={p_sent}/{r_sent}/{pr_sent}, expected={len(products_raw)}/{len(reviews_raw)}/{len(prices_raw)})."
        )

    print(f"Published {p_sent} product events, {r_sent} review events, and {pr_sent} price events to Kafka.")

    consumer = RawConsumer()
    offline_items = producer.virtual_queue if is_local else None
    topics_to_consume = [settings.kafka.topic_products, settings.kafka.topic_reviews]
    if prices_raw:
        topics_to_consume.append(settings.kafka.topic_prices)

    try:
        consume_result = consumer.consume_batch_once(
            topics=topics_to_consume,
            max_records=max(total_sent * 10, 50000),
            timeout_ms=4000,
            offline_records=offline_items
        )
    finally:
        consumer.close()

    # --- Stage 2 Verification: Count & Content ---
    consumed = consume_result["records_consumed"]
    valid_count = consume_result["records_valid"]
    dlq_count = consume_result["records_dlq"]
    topic_counts = consume_result.get("topic_counts", {})

    if consumed < expected_sent:
        raise RuntimeError(
            f"Stage 2 verification failed: Consumed fewer records ({consumed}) than produced ({expected_sent})."
        )
    if consumed != valid_count + dlq_count:
        raise RuntimeError(
            f"Stage 2 verification failed: Consumer accounting mismatch "
            f"consumed ({consumed}) != valid ({valid_count}) + dlq ({dlq_count})."
        )
    if valid_count <= 0 or not consume_result.get("batches_written"):
        raise RuntimeError("Stage 2 verification failed: Zero valid records written to Bronze.")
    if topic_counts.get(settings.kafka.topic_products, 0) <= 0:
        raise RuntimeError("Stage 2 verification failed: No valid product records consumed to Bronze.")
    if topic_counts.get(settings.kafka.topic_reviews, 0) <= 0:
        raise RuntimeError("Stage 2 verification failed: No valid review records consumed to Bronze.")
    if prices_raw and topic_counts.get(settings.kafka.topic_prices, 0) <= 0:
        raise RuntimeError("Stage 2 verification failed: No valid price records consumed to Bronze.")
    if not is_local and not consume_result.get("committed_offsets"):
        raise RuntimeError("Stage 2 verification failed: Kafka offsets were not committed in integration mode.")

    print(
        f"[Stage 2 OK] Consumed {consumed} messages ({valid_count} valid to Bronze, {dlq_count} to DLQ). "
        f"Committed partitions: {len(consume_result.get('committed_offsets', {}))}"
    )
    return consume_result


def stage_3_verify_bronze(
    consume_result: Dict[str, Any],
    effective_mode: str = "integration"
) -> Dict[str, Any]:
    """Stage 3: MinIO Bronze storage verification (checks object existence, counts, and envelope content)."""
    print("\n[Stage 3/6] Verifying MinIO Bronze Layer...")
    is_integration = effective_mode in ("integration", "production")

    if is_integration and not isinstance(storage.backend, MinIOBackend):
        raise RuntimeError(
            f"Stage 3 verification failed: Expected MinIOBackend, got {type(storage.backend).__name__}."
        )

    batches_written: List[str] = consume_result.get("batches_written", [])
    batches_by_topic: Dict[str, List[str]] = consume_result.get("batches_by_topic", {})
    topic_counts: Dict[str, int] = consume_result.get("topic_counts", {})

    if not batches_written:
        raise RuntimeError("Stage 3 verification failed: No Bronze batch files recorded from Stage 2.")

    envelope_required_keys = {"event_id", "event_type", "source", "ingested_at", "payload"}
    total_bronze_records = 0
    verified_by_topic: Dict[str, int] = {}

    for topic, paths in batches_by_topic.items():
        topic_total = 0
        for bpath in paths:
            if not storage.backend.object_exists(bpath):
                raise RuntimeError(f"Stage 3 verification failed: Bronze object '{bpath}' does not exist in storage.")
            records = storage.backend.read_json(bpath)
            if not isinstance(records, list) or len(records) == 0:
                raise RuntimeError(f"Stage 3 verification failed: Bronze batch '{bpath}' is empty or invalid JSON.")
            for rec in records:
                if not isinstance(rec, dict) or not envelope_required_keys.issubset(rec.keys()):
                    raise RuntimeError(
                        f"Stage 3 verification failed: Record in '{bpath}' missing canonical envelope keys "
                        f"{envelope_required_keys}."
                    )
                if not isinstance(rec.get("payload"), dict) or len(rec["payload"]) == 0:
                    raise RuntimeError(
                        f"Stage 3 verification failed: Record in '{bpath}' has empty or non-dict payload."
                    )
            topic_total += len(records)
        verified_by_topic[topic] = topic_total
        total_bronze_records += topic_total

        expected_topic_count = topic_counts.get(topic, 0)
        if topic_total != expected_topic_count:
            raise RuntimeError(
                f"Stage 3 verification failed: Bronze count for topic '{topic}' ({topic_total}) "
                f"!= consumer valid count ({expected_topic_count})."
            )

    if total_bronze_records != consume_result["records_valid"]:
        raise RuntimeError(
            f"Stage 3 verification failed: Total records read from Bronze ({total_bronze_records}) "
            f"!= consumer valid records ({consume_result['records_valid']})."
        )

    backend_label = (
        f"MinIOBackend (s3a://{storage.backend.bucket})"
        if isinstance(storage.backend, MinIOBackend)
        else f"LocalBackend ({storage.base_dir})"
    )
    print(
        f"[Stage 3 OK] Verified {len(batches_written)} Bronze batch files on {backend_label} "
        f"({total_bronze_records} valid envelope records: {verified_by_topic})."
    )
    return {
        "batches_by_topic": batches_by_topic,
        "verified_by_topic": verified_by_topic,
        "total_bronze_records": total_bronze_records
    }


def stage_4_spark_batch_silver_dq(
    batches_by_topic: Optional[Dict[str, List[str]]] = None,
    include_prices: bool = True,
    enforce_dq_gate: bool = True,
    min_dq_score: Optional[float] = None,
) -> Dict[str, Any]:
    """Stage 4: Spark Batch -> Silver / Quarantine / DQ.
    Reads directly from Bronze storage paths on MinIO/Data Lake (never in-memory lists)
    and verifies mathematical reconciliation, Silver uniqueness, DQ scorecards, and DQ gate thresholds.
    """
    print("\n[Stage 4/6] Executing Spark Batch ETL (Bronze -> Silver / Quarantine / DQ)...")
    topic_prods = settings.kafka.topic_products
    topic_revs = settings.kafka.topic_reviews
    topic_prices = settings.kafka.topic_prices

    if batches_by_topic:
        prods_source = batches_by_topic.get(topic_prods) or topic_prods
        revs_source = batches_by_topic.get(topic_revs) or topic_revs
        prices_source = (batches_by_topic.get(topic_prices) or topic_prices) if include_prices else None
    else:
        prods_source = topic_prods
        revs_source = topic_revs
        prices_source = topic_prices if include_prices else None

    etl_job = SparkBatchETLJob()
    etl_res = etl_job.run_pipeline(
        products_source=prods_source,
        reviews_source=revs_source,
        prices_source=prices_source,
        enforce_dq_gate=enforce_dq_gate,
        min_dq_score=min_dq_score,
    )

    # --- Stage 4 Verification: Reconciliation, Silver Counts & Content, DQ Metrics ---
    for ds_name in ("products", "reviews", "prices"):
        rec = etl_res["reconciliation"].get(ds_name)
        if rec is None:
            continue
        if not rec.get("reconciled", False):
            raise RuntimeError(
                f"Stage 4 verification failed: Reconciliation mismatch for '{ds_name}': {rec}"
            )
        if ds_name in ("products", "reviews") and rec["valid_count"] <= 0:
            raise RuntimeError(
                f"Stage 4 verification failed: 0 valid records processed for '{ds_name}'."
            )

    df_prods = storage.read_silver_parquet("products")
    df_revs = storage.read_silver_parquet("reviews")
    df_prices = storage.read_silver_parquet("prices") if include_prices else pd.DataFrame()

    if df_prods.empty or df_revs.empty:
        raise RuntimeError("Stage 4 verification failed: Silver products or reviews Parquet is empty.")
    if include_prices and df_prices.empty:
        raise RuntimeError("Stage 4 verification failed: Silver prices Parquet is empty.")

    # Verify primary key uniqueness (zero logical duplication in Silver)
    if not df_prods["product_id"].is_unique:
        dups = int(df_prods["product_id"].duplicated().sum())
        raise RuntimeError(f"Stage 4 verification failed: Silver products has {dups} duplicate product_id rows.")
    if not df_revs["review_id"].is_unique:
        dups = int(df_revs["review_id"].duplicated().sum())
        raise RuntimeError(f"Stage 4 verification failed: Silver reviews has {dups} duplicate review_id rows.")
    if not df_prices.empty and not df_prices["price_id"].is_unique:
        dups = int(df_prices["price_id"].duplicated().sum())
        raise RuntimeError(f"Stage 4 verification failed: Silver prices has {dups} duplicate price_id rows.")

    # Verify Silver content constraints
    if (pd.to_numeric(df_prods["price"], errors="coerce") <= 0).any():
        raise RuntimeError("Stage 4 verification failed: Silver products contains non-positive prices.")
    ratings_num = pd.to_numeric(df_revs["rating"], errors="coerce")
    if ((ratings_num < 1.0) | (ratings_num > 5.0) | ratings_num.isna()).any():
        raise RuntimeError("Stage 4 verification failed: Silver reviews contains out-of-range or null ratings.")
    if not df_prices.empty and (pd.to_numeric(df_prices["price"], errors="coerce") <= 0).any():
        raise RuntimeError("Stage 4 verification failed: Silver prices contains non-positive prices.")

    # Verify DQ metrics persisted to Gold
    df_dq = storage.read_gold_parquet("data_quality_metrics")
    if df_dq.empty:
        raise RuntimeError("Stage 4 verification failed: gold/data_quality_metrics is empty.")
    if not df_dq["batch_id"].is_unique:
        raise RuntimeError("Stage 4 verification failed: gold/data_quality_metrics has duplicate batch_id rows.")

    print(
        f"[Stage 4 OK] Silver Parquet verified: {len(df_prods)} unique products, "
        f"{len(df_revs)} unique reviews, {len(df_prices)} unique prices. "
        f"DQ Scores -> Products: {etl_res['products_dq']['dq_score']}%, Reviews: {etl_res['reviews_dq']['dq_score']}%"
    )
    return {
        "etl_res": etl_res,
        "df_prods": df_prods,
        "df_revs": df_revs,
        "df_prices": df_prices,
        "df_dq": df_dq
    }


def stage_5_spark_gold_marts(
    df_prods: Optional[pd.DataFrame] = None,
    df_revs: Optional[pd.DataFrame] = None,
    df_prices: Optional[pd.DataFrame] = None,
    burst_window_hours: Optional[int] = None,
    burst_multiplier: Optional[float] = None,
    dq_metrics: Optional[Any] = None,
    enforce_dq_gate: bool = True,
    min_dq_score: Optional[float] = None,
) -> Dict[str, Any]:
    """Stage 5: Spark Gold Marts & Anomaly Detection with DQ Gate check and grain & content verification."""
    print("\n[Stage 5/6] Building Spark Gold Analytical Marts & Anomaly Events...")
    if df_prods is None:
        df_prods = storage.read_silver_parquet("products")
    if df_revs is None:
        df_revs = storage.read_silver_parquet("reviews")
    if df_prices is None:
        try:
            df_prices = storage.read_silver_parquet("prices")
        except Exception:
            df_prices = pd.DataFrame()

    gold_builder = SparkGoldBuilder()
    # Let SparkGoldBuilder read Silver Parquet directly via Spark S3A/local Parquet reader, enforcing DQ Gate first
    marts = gold_builder.build_all(
        silver_reviews=None,
        silver_products=None,
        silver_prices=None,
        save_gold=True,
        dq_metrics=dq_metrics,
        enforce_dq_gate=enforce_dq_gate,
        min_dq_score=min_dq_score,
    )

    # Compute Anomaly Events with deterministic event_id and deduplicate
    all_anomalies: List[Dict[str, Any]] = []
    if not df_revs.empty:
        bursts = ReviewBurstDetector.detect_bursts(
            df_revs,
            window_hours=burst_window_hours,
            threshold_multiplier=burst_multiplier
        )
        all_anomalies.extend(bursts)
        rating_drops = RatingAnomalyDetector.detect_rating_drops(df_revs)
        all_anomalies.extend(rating_drops)

    if not df_prices.empty:
        price_anoms = PriceAnomalyDetector.detect_zscore_anomalies(df_prices)
        all_anomalies.extend(price_anoms)

    if all_anomalies:
        for anom in all_anomalies:
            if not anom.get("event_id"):
                raw_key = f"{anom.get('entity_type')}|{anom.get('entity_id')}|{anom.get('anomaly_type')}|{anom.get('timestamp')}"
                anom["event_id"] = f"anom_{hashlib.sha256(raw_key.encode('utf-8')).hexdigest()[:20]}"
        df_anom = pd.DataFrame(all_anomalies).drop_duplicates(subset=["event_id"], keep="last")
        storage.write_gold_parquet("anomaly_events", df_anom)
    else:
        df_anom = pd.DataFrame()

    # --- Stage 5 Verification: Count, Schema, and Composite Grain Uniqueness ---
    expected_mart_grains = {
        "product_daily_stats": ["date", "product_id"],
        "brand_daily_stats": ["date", "brand"],
        "category_daily_stats": ["date", "category"],
        "price_daily_stats": ["date", "product_id"]
    }
    gold_dfs: Dict[str, pd.DataFrame] = {}

    for mart_name, grain_cols in expected_mart_grains.items():
        if mart_name == "price_daily_stats" and df_prices.empty:
            continue
        df_mart = storage.read_gold_parquet(mart_name)
        if df_mart.empty:
            raise RuntimeError(f"Stage 5 verification failed: Gold mart '{mart_name}' is empty.")
        missing_cols = set(grain_cols) - set(df_mart.columns)
        if missing_cols:
            raise RuntimeError(
                f"Stage 5 verification failed: Gold mart '{mart_name}' missing grain columns {missing_cols}."
            )
        dup_count = int(df_mart.duplicated(subset=grain_cols).sum())
        if dup_count > 0:
            raise RuntimeError(
                f"Stage 5 verification failed: Gold mart '{mart_name}' has {dup_count} duplicate rows on grain {grain_cols}."
            )
        gold_dfs[mart_name] = df_mart

    if not df_anom.empty:
        df_anom_stored = storage.read_gold_parquet("anomaly_events")
        if not df_anom_stored["event_id"].is_unique:
            raise RuntimeError("Stage 5 verification failed: gold/anomaly_events has duplicate event_id rows.")
        gold_dfs["anomaly_events"] = df_anom_stored

    print(
        f"[Stage 5 OK] Verified Gold Marts ({ {k: len(v) for k, v in gold_dfs.items()} }) "
        f"with 0 grain duplicates."
    )
    return {
        "marts": marts,
        "gold_dfs": gold_dfs,
        "anomalies_count": len(df_anom)
    }


def stage_6_warehouse_and_dashboard(
    effective_mode: str = "integration"
) -> Dict[str, Any]:
    """Stage 6: PostgreSQL Warehouse Upsert -> DashboardDB & Independent SQL Reconciliation."""
    print("\n[Stage 6/6] Syncing Gold to Warehouse & Verifying DashboardDB Reconciliation...")
    is_integration = effective_mode in ("integration", "production")

    synced_tables = export_gold_to_db(use_postgres=is_integration, mode=effective_mode)
    if not synced_tables:
        raise RuntimeError("Stage 6 verification failed: export_gold_to_db synced 0 tables.")

    # 1. Independent Warehouse SQL verification (count + uniqueness + content reconciliation)
    wh = WarehouseManager(use_postgres=is_integration, mode=effective_mode)
    try:
        table_pks = {
            "product_daily_stats": ["date", "product_id"],
            "brand_daily_stats": ["date", "brand"],
            "category_daily_stats": ["date", "category"],
            "price_daily_stats": ["date", "product_id"],
            "products": ["product_id"],
            "reviews": ["review_id"],
            "data_quality_metrics": ["batch_id"],
            "anomaly_events": ["event_id"]
        }
        wh_counts: Dict[str, int] = {}
        for tbl, pk_cols in table_pks.items():
            if tbl not in synced_tables:
                continue
            pk_expr = ", ".join(pk_cols)
            cnt_df = wh.query(
                f"SELECT COUNT(*) AS total_rows, "
                f"COUNT(DISTINCT ({pk_expr})) AS distinct_pks "
                f"FROM {tbl}"
                if len(pk_cols) == 1
                else f"SELECT COUNT(*) AS total_rows FROM ("
                     f"SELECT 1 FROM {tbl} GROUP BY {pk_expr}"
                     f") sub"
            )
            total_df = wh.query(f"SELECT COUNT(*) AS total_rows FROM {tbl}")
            total_rows = int(total_df["total_rows"].iloc[0])
            distinct_rows = int(cnt_df["total_rows"].iloc[0]) if len(pk_cols) > 1 else int(cnt_df["distinct_pks"].iloc[0])

            if total_rows <= 0:
                raise RuntimeError(f"Stage 6 verification failed: Warehouse table '{tbl}' is empty.")
            if total_rows != distinct_rows:
                raise RuntimeError(
                    f"Stage 6 verification failed: Warehouse table '{tbl}' has duplicate primary keys "
                    f"(total={total_rows}, distinct={distinct_rows})."
                )
            wh_counts[tbl] = total_rows
    finally:
        wh.close()

    # 2. DashboardDB verification
    db = DashboardDB(mode=effective_mode, use_postgres=is_integration)
    try:
        if is_integration and db.source_name != "postgres":
            raise RuntimeError(
                f"Stage 6 verification failed: DashboardDB must use 'postgres' in integration mode, got '{db.source_name}'."
            )

        stats = db.get_product_daily_stats()
        brand_stats = db.get_brand_daily_stats()
        cat_stats = db.get_category_daily_stats()
        price_stats = db.get_price_daily_stats()
        kpis = db.get_kpi_summary()
        anoms_db = db.get_anomaly_events()
        dq_db = db.get_data_quality_metrics()
    finally:
        db.close()

    if stats.empty or brand_stats.empty or cat_stats.empty:
        raise RuntimeError("Stage 6 verification failed: DashboardDB returned empty Gold mart DataFrames.")
    if kpis.get("total_products", 0) <= 0 or kpis.get("total_reviews", 0) <= 0:
        raise RuntimeError(f"Stage 6 verification failed: Invalid Dashboard KPIs: {kpis}")
    if dq_db.empty:
        raise RuntimeError("Stage 6 verification failed: DashboardDB returned empty DQ metrics.")

    # Cross-check Gold Parquet vs Warehouse counts for the 4 Gold marts
    for mart_name, dash_df in [
        ("product_daily_stats", stats),
        ("brand_daily_stats", brand_stats),
        ("category_daily_stats", cat_stats),
        ("price_daily_stats", price_stats),
    ]:
        gold_df = storage.read_gold_parquet(mart_name)
        if len(dash_df) < len(gold_df):
            raise RuntimeError(
                f"Stage 6 verification failed: DashboardDB table '{mart_name}' has fewer rows ({len(dash_df)}) "
                f"than Gold Parquet ({len(gold_df)})."
            )

    print(
        f"[Stage 6 OK] Warehouse ({db.source_name}) & DashboardDB reconciled: "
        f"{wh_counts}, KPIs={kpis}"
    )
    return {
        "synced_tables": synced_tables,
        "warehouse_counts": wh_counts,
        "kpis": kpis,
        "source_name": db.source_name
    }


def run_end_to_end(
    products_path: Optional[str] = None,
    reviews_path: Optional[str] = None,
    prices_path: Optional[str] = None,
    burst_window_hours: Optional[int] = None,
    burst_multiplier: Optional[float] = None,
    mode: Optional[str] = None,
    enforce_dq_gate: bool = True,
    min_dq_score: Optional[float] = None,
    source_type: str = "dataset",
    crawler_platform: str = "tiki",
    crawler_query: str = "iphone 15",
    crawler_max_products: int = 5,
    crawler_reviews_per_product: int = 10,
    remove_user_id: bool = False,
) -> bool:
    """Executes the canonical 6-stage SentinelAI End-to-End Pipeline with strict stage verification."""
    effective_mode = configure_pipeline_mode(mode)

    print("==========================================================")
    print(f"  SENTINELAI END-TO-END PIPELINE (MODE: {effective_mode.upper()})")
    print("==========================================================")

    # Stage 1: Source / Dataset / Crawler
    s1 = stage_1_ingest_sources(
        products_path=products_path,
        reviews_path=reviews_path,
        prices_path=prices_path,
        source_type=source_type,
        crawler_platform=crawler_platform,
        crawler_query=crawler_query,
        crawler_max_products=crawler_max_products,
        crawler_reviews_per_product=crawler_reviews_per_product,
        remove_user_id=remove_user_id,
    )

    # Stage 2: Kafka Producer -> Kafka -> RawConsumer
    s2 = stage_2_kafka_produce_and_consume(
        products_raw=s1["products_raw"],
        reviews_raw=s1["reviews_raw"],
        prices_raw=s1["prices_raw"],
        effective_mode=effective_mode
    )

    # Stage 3: MinIO Bronze Verification
    s3 = stage_3_verify_bronze(
        consume_result=s2,
        effective_mode=effective_mode
    )

    # Stage 4: Spark Batch -> Silver / Quarantine / DQ (reads directly from Bronze batches!)
    s4 = stage_4_spark_batch_silver_dq(
        batches_by_topic=s3["batches_by_topic"],
        include_prices=bool(s1["prices_raw"]),
        enforce_dq_gate=enforce_dq_gate,
        min_dq_score=min_dq_score,
    )

    # Stage 5: Spark Gold Marts & Anomaly Events (blocked if DQ Gate failed)
    s5 = stage_5_spark_gold_marts(
        df_prods=s4["df_prods"],
        df_revs=s4["df_revs"],
        df_prices=s4["df_prices"],
        burst_window_hours=burst_window_hours,
        burst_multiplier=burst_multiplier,
        dq_metrics=[s4["etl_res"]["products_dq"], s4["etl_res"]["reviews_dq"]],
        enforce_dq_gate=enforce_dq_gate,
        min_dq_score=min_dq_score,
    )

    # Stage 6: PostgreSQL Warehouse -> Dashboard Verification
    s6 = stage_6_warehouse_and_dashboard(
        effective_mode=effective_mode
    )

    summary = DescriptiveStats.compute_summary(s4["df_prods"], s4["df_revs"])
    print("\n==========================================================")
    print(f"  PIPELINE VERIFICATION COMPLETE (MODE: {effective_mode.upper()})")
    print(f"  - Source Type:        {s1['source_type']}")
    print(f"  - Storage Backend:    {type(storage.backend).__name__}")
    print(f"  - Warehouse Engine:   {s6['source_name']}")
    print(f"  - Products In Silver: {summary['total_products']}")
    print(f"  - Reviews In Silver:  {summary['total_reviews']}")
    print(f"  - Prices In Silver:   {len(s4['df_prices'])}")
    print(f"  - Products DQ Score:  {s4['etl_res']['products_dq']['dq_score']}%")
    print(f"  - Reviews DQ Score:   {s4['etl_res']['reviews_dq']['dq_score']}%")
    print("==========================================================\n")
    return True


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="SentinelAI Canonical End-to-End Pipeline Runner")
    parser.add_argument(
        "--mode",
        choices=["integration", "local", "test"],
        default=None,
        help="Execution mode: 'integration' (real Kafka/MinIO/Spark/Postgres, default) or 'local'/'test'"
    )
    parser.add_argument(
        "--source-type",
        choices=["dataset", "public_dataset", "crawler"],
        default="dataset",
        help="Ingestion source type: 'dataset' (default), 'public_dataset' (data/public/*), or 'crawler'"
    )
    parser.add_argument("--crawler-platform", default="tiki", help="Crawler platform when --source-type=crawler")
    parser.add_argument("--crawler-query", default="iphone 15", help="Search query when --source-type=crawler")
    parser.add_argument("--crawler-max-products", type=int, default=5, help="Max products to crawl")
    parser.add_argument("--crawler-reviews-per-prod", type=int, default=10, help="Reviews per product to crawl")
    parser.add_argument("--remove-user-id", action="store_true", help="Redact user_id completely instead of hashing")
    parser.add_argument("--products", default=None, help="Path to products raw file (JSON, CSV, Parquet)")
    parser.add_argument("--reviews", default=None, help="Path to reviews raw file (JSON, CSV, Parquet)")
    parser.add_argument("--prices", default=None, help="Path to prices raw file (JSON, CSV, Parquet)")
    parser.add_argument("--burst-hours", type=int, default=None, help="Burst window duration in hours")
    parser.add_argument("--burst-mult", type=float, default=None, help="Burst standard deviation multiplier")
    args = parser.parse_args()

    try:
        ok = run_end_to_end(
            products_path=args.products,
            reviews_path=args.reviews,
            prices_path=args.prices,
            burst_window_hours=args.burst_hours,
            burst_multiplier=args.burst_mult,
            mode=args.mode,
            source_type=args.source_type,
            crawler_platform=args.crawler_platform,
            crawler_query=args.crawler_query,
            crawler_max_products=args.crawler_max_products,
            crawler_reviews_per_product=args.crawler_reviews_per_prod,
            remove_user_id=args.remove_user_id,
        )
        if not ok:
            sys.exit(1)
    except Exception as exc:
        print(f"\n[PIPELINE FAILED] {exc}", file=sys.stderr)
        sys.exit(1)
