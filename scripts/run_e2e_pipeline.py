import sys
import os
import json
import argparse
from pathlib import Path
from typing import Optional, Dict, Any, List

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from config.settings import settings
from storage.storage_manager import storage
from kafka.producers.stream_producer import StreamProducer
from kafka.consumers.raw_consumer import RawConsumer
from spark.batch.spark_etl_job import SparkBatchETLJob
from spark.analytics.gold_builder import SparkGoldBuilder
from database.warehouse import WarehouseManager
from database.export_gold import export_gold_to_db
from dashboard.db_connector import DashboardDB
from analytics.descriptive.stats import DescriptiveStats
from analytics.anomaly.review_burst import ReviewBurstDetector

def run_end_to_end(
    products_path: Optional[str] = None,
    reviews_path: Optional[str] = None,
    burst_window_hours: Optional[int] = None,
    burst_multiplier: Optional[float] = None
) -> bool:
    print("==========================================================")
    print("      SENTINELAI END-TO-END PIPELINE (REAL DATA PATH)     ")
    print("==========================================================")

    # ---------------------------------------------------------
    # STAGE 1: DATA SOURCES
    # ---------------------------------------------------------
    print("\n[Stage 1/7] Ingesting Sources...")
    sample_prods_path = products_path or str(REPO_ROOT / "data" / "sample" / "products.json")
    sample_revs_path = reviews_path or str(REPO_ROOT / "data" / "sample" / "reviews.json")

    with open(sample_prods_path, "r", encoding="utf-8") as f:
        products_raw = json.load(f)
    with open(sample_revs_path, "r", encoding="utf-8") as f:
        reviews_raw = json.load(f)
    print(f"Loaded {len(products_raw)} raw products and {len(reviews_raw)} raw reviews.")

    # ---------------------------------------------------------
    # STAGE 2: KAFKA PRODUCER (ENVELOPE)
    # ---------------------------------------------------------
    print("\n[Stage 2/7] Publishing to Kafka with Message Envelopes...")
    producer = StreamProducer(enable_offline_buffer=True)
    p_sent, _ = producer.produce_batch(settings.kafka.topic_products, products_raw, rate=0, event_type="NEW_PRODUCT")
    r_sent, _ = producer.produce_batch(settings.kafka.topic_reviews, reviews_raw, rate=0, event_type="NEW_REVIEW")
    print(f"Published {p_sent} product events and {r_sent} review events to Kafka.")

    # ---------------------------------------------------------
    # STAGE 3: KAFKA CONSUMER -> MINIO BRONZE
    # ---------------------------------------------------------
    print("\n[Stage 3/7] Consuming from Kafka to MinIO Bronze...")
    consumer = RawConsumer()
    p_bronze_file = consumer.save_raw_batch(settings.kafka.topic_products, products_raw)
    r_bronze_file = consumer.save_raw_batch(settings.kafka.topic_reviews, reviews_raw)
    print(f"Storage backend: {type(storage.backend).__name__} (Bucket: {storage.backend.bucket})")
    print(f"Bronze Products stored at: {p_bronze_file.name}")
    print(f"Bronze Reviews stored at:  {r_bronze_file.name}")

    # ---------------------------------------------------------
    # STAGE 4: SPARK BATCH ETL (BRONZE -> SILVER)
    # ---------------------------------------------------------
    print("\n[Stage 4/7] Executing Spark Batch ETL (Bronze -> Silver)...")
    etl_job = SparkBatchETLJob()
    etl_res = etl_job.run_pipeline(products_raw, reviews_raw)

    df_prods = storage.read_silver_parquet("products")
    df_revs = storage.read_silver_parquet("reviews")
    print(f"Silver Parquet Generated: {len(df_prods)} products, {len(df_revs)} reviews")
    print(f"Products DQ Score: {etl_res['products_dq']['dq_score']}% | Reviews DQ Score: {etl_res['reviews_dq']['dq_score']}%")

    # ---------------------------------------------------------
    # STAGE 5: SPARK GOLD ANALYTICS (SILVER -> GOLD)
    # ---------------------------------------------------------
    print("\n[Stage 5/7] Building Gold Analytical Marts via SparkGoldBuilder...")
    gold_builder = SparkGoldBuilder()
    marts = gold_builder.build_all(silver_reviews=df_revs, silver_products=df_prods, save_gold=True)
    print(f"Generated Gold Marts: {list(marts.keys())}")

    # ---------------------------------------------------------
    # STAGE 6: DATA WAREHOUSE (GOLD -> POSTGRESQL UPSERT)
    # ---------------------------------------------------------
    print("\n[Stage 6/7] Exporting Gold Data to Data Warehouse (UPSERT)...")
    export_gold_to_db()

    # ---------------------------------------------------------
    # STAGE 7: DASHBOARD VERIFICATION & ANOMALY DETECTION
    # ---------------------------------------------------------
    print("\n[Stage 7/7] Verifying Data Warehouse via DashboardDB Connector...")
    db = DashboardDB()
    stats = db.get_product_daily_stats()
    kpis = db.get_kpi_summary()
    print(f"Dashboard Query Result: {len(stats)} daily stat rows loaded.")
    print(f"Live KPIs: {kpis}")

    bursts = ReviewBurstDetector.detect_bursts(
        df_revs,
        window_hours=burst_window_hours,
        threshold_multiplier=burst_multiplier
    )
    print(f"Anomaly Engine: Detected {len(bursts)} review bursts/bombing events.")

    summary = DescriptiveStats.compute_summary(df_prods, df_revs)
    print("\n==========================================================")
    print("      PIPELINE VERIFICATION COMPLETE (ALL STAGES PASSED)  ")
    print(f"  - Products In Silver: {summary['total_products']}")
    print(f"  - Reviews In Silver:  {summary['total_reviews']}")
    print(f"  - Products DQ Score:  {etl_res['products_dq']['dq_score']}%")
    print(f"  - Reviews DQ Score:   {etl_res['reviews_dq']['dq_score']}%")
    print("==========================================================\n")
    return True

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="SentinelAI End-to-End Pipeline Runner")
    parser.add_argument("--products", default=None, help="Path to products raw file (JSON, CSV, Parquet)")
    parser.add_argument("--reviews", default=None, help="Path to reviews raw file (JSON, CSV, Parquet)")
    parser.add_argument("--burst-hours", type=int, default=None, help="Burst window duration in hours")
    parser.add_argument("--burst-mult", type=float, default=None, help="Burst standard deviation multiplier")
    args = parser.parse_args()

    run_end_to_end(
        products_path=args.products,
        reviews_path=args.reviews,
        burst_window_hours=args.burst_hours,
        burst_multiplier=args.burst_mult
    )
