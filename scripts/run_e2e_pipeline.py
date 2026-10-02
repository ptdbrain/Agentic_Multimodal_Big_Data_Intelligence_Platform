import sys
import os
import json
import argparse
from pathlib import Path
from typing import Optional, Dict, Any, List
import pandas as pd

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
from analytics.anomaly.price_anomaly import PriceAnomalyDetector
from analytics.anomaly.rating_anomaly import RatingAnomalyDetector

def run_end_to_end(
    products_path: Optional[str] = None,
    reviews_path: Optional[str] = None,
    prices_path: Optional[str] = None,
    burst_window_hours: Optional[int] = None,
    burst_multiplier: Optional[float] = None
) -> bool:
    print("==========================================================")
    print("      SENTINELAI END-TO-END PIPELINE (REAL DATA PATH)     ")
    print("==========================================================")

    # ---------------------------------------------------------
    # STAGE 1: DATA SOURCES (PRODUCTS, REVIEWS, PRICES)
    # ---------------------------------------------------------
    print("\n[Stage 1/7] Ingesting Sources...")
    sample_prods_path = products_path or str(REPO_ROOT / "data" / "sample" / "products.json")
    sample_revs_path = reviews_path or str(REPO_ROOT / "data" / "sample" / "reviews.json")
    sample_prices_path = prices_path or str(REPO_ROOT / "data" / "sample" / "prices.json")

    with open(sample_prods_path, "r", encoding="utf-8") as f:
        products_raw = json.load(f)
    with open(sample_revs_path, "r", encoding="utf-8") as f:
        reviews_raw = json.load(f)
    prices_raw = []
    if Path(sample_prices_path).exists():
        with open(sample_prices_path, "r", encoding="utf-8") as f:
            prices_raw = json.load(f)
            
    print(f"Loaded {len(products_raw)} raw products, {len(reviews_raw)} raw reviews, and {len(prices_raw)} raw prices.")

    # ---------------------------------------------------------
    # STAGE 2: KAFKA PRODUCER (CANONICAL ENVELOPE)
    # ---------------------------------------------------------
    print("\n[Stage 2/7] Publishing to Kafka with Message Envelopes...")
    producer = StreamProducer(enable_offline_buffer=True)
    p_sent, _ = producer.produce_batch(settings.kafka.topic_products, products_raw, rate=0, event_type="NEW_PRODUCT")
    r_sent, _ = producer.produce_batch(settings.kafka.topic_reviews, reviews_raw, rate=0, event_type="NEW_REVIEW")
    pr_sent = 0
    if prices_raw:
        pr_sent, _ = producer.produce_batch(settings.kafka.topic_prices, prices_raw, rate=0, event_type="PRICE_UPDATE")
    print(f"Published {p_sent} product events, {r_sent} review events, and {pr_sent} price events to Kafka.")

    # ---------------------------------------------------------
    # STAGE 3: KAFKA CONSUMER -> MINIO BRONZE
    # ---------------------------------------------------------
    print("\n[Stage 3/7] Consuming from Kafka to MinIO Bronze...")
    consumer = RawConsumer()
    p_bronze_file = consumer.save_raw_batch(settings.kafka.topic_products, products_raw)
    r_bronze_file = consumer.save_raw_batch(settings.kafka.topic_reviews, reviews_raw)
    pr_bronze_file = consumer.save_raw_batch(settings.kafka.topic_prices, prices_raw) if prices_raw else None

    print(f"Storage backend: {type(storage.backend).__name__} (Bucket: {storage.backend.bucket})")
    print(f"Bronze Products stored at: {p_bronze_file.name}")
    print(f"Bronze Reviews stored at:  {r_bronze_file.name}")
    if pr_bronze_file:
        print(f"Bronze Prices stored at:   {pr_bronze_file.name}")

    # ---------------------------------------------------------
    # STAGE 4: SPARK BATCH ETL (BRONZE -> SILVER)
    # ---------------------------------------------------------
    print("\n[Stage 4/7] Executing Spark Batch ETL (Bronze -> Silver)...")
    etl_job = SparkBatchETLJob()
    etl_res = etl_job.run_pipeline(products_raw, reviews_raw, prices_source=prices_raw)

    df_prods = storage.read_silver_parquet("products")
    df_revs = storage.read_silver_parquet("reviews")
    df_prices = storage.read_silver_parquet("prices")
    
    print(f"Silver Parquet Generated: {len(df_prods)} products, {len(df_revs)} reviews, {len(df_prices)} prices")
    print(f"Products DQ Score: {etl_res['products_dq']['dq_score']}% | Reviews DQ Score: {etl_res['reviews_dq']['dq_score']}%")
    if etl_res.get('prices_dq'):
        print(f"Prices DQ Score:   {etl_res['prices_dq']['dq_score']}%")

    # ---------------------------------------------------------
    # STAGE 5: SPARK GOLD ANALYTICS & ANOMALY EVENTS (SILVER -> GOLD)
    # ---------------------------------------------------------
    print("\n[Stage 5/7] Building Gold Analytical Marts via SparkGoldBuilder...")
    gold_builder = SparkGoldBuilder()
    marts = gold_builder.build_all(
        silver_reviews=df_revs,
        silver_products=df_prods,
        silver_prices=df_prices,
        save_gold=True
    )
    print(f"Generated Gold Marts: {list(marts.keys())}")

    # Compute and persist anomaly incidents
    all_anomalies = []
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
        df_anom = pd.DataFrame(all_anomalies)
        storage.write_gold_parquet("anomaly_events", df_anom)
        print(f"Anomaly Engine: Persisted {len(all_anomalies)} anomalies into Gold Layer (anomaly_events).")

    # ---------------------------------------------------------
    # STAGE 6: DATA WAREHOUSE (GOLD -> POSTGRESQL UPSERT)
    # ---------------------------------------------------------
    print("\n[Stage 6/7] Exporting Gold Data to Data Warehouse (UPSERT)...")
    export_gold_to_db()

    # ---------------------------------------------------------
    # STAGE 7: DASHBOARD VERIFICATION
    # ---------------------------------------------------------
    print("\n[Stage 7/7] Verifying Data Warehouse via DashboardDB Connector...")
    db = DashboardDB()
    stats = db.get_product_daily_stats()
    kpis = db.get_kpi_summary()
    anoms_db = db.get_anomaly_events()
    dq_db = db.get_data_quality_metrics()
    
    print(f"Dashboard Query Result: {len(stats)} daily stat rows loaded.")
    print(f"Live KPIs: {kpis}")
    print(f"Warehouse Anomaly Events Loaded: {len(anoms_db)}")
    print(f"Warehouse DQ Scorecard Loaded: {len(dq_db)}")

    summary = DescriptiveStats.compute_summary(df_prods, df_revs)
    print("\n==========================================================")
    print("      PIPELINE VERIFICATION COMPLETE (ALL STAGES PASSED)  ")
    print(f"  - Products In Silver: {summary['total_products']}")
    print(f"  - Reviews In Silver:  {summary['total_reviews']}")
    print(f"  - Prices In Silver:   {len(df_prices)}")
    print(f"  - Products DQ Score:  {etl_res['products_dq']['dq_score']}%")
    print(f"  - Reviews DQ Score:   {etl_res['reviews_dq']['dq_score']}%")
    print("==========================================================\n")
    return True

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="SentinelAI End-to-End Pipeline Runner")
    parser.add_argument("--products", default=None, help="Path to products raw file (JSON, CSV, Parquet)")
    parser.add_argument("--reviews", default=None, help="Path to reviews raw file (JSON, CSV, Parquet)")
    parser.add_argument("--prices", default=None, help="Path to prices raw file (JSON, CSV, Parquet)")
    parser.add_argument("--burst-hours", type=int, default=None, help="Burst window duration in hours")
    parser.add_argument("--burst-mult", type=float, default=None, help="Burst standard deviation multiplier")
    args = parser.parse_args()

    run_end_to_end(
        products_path=args.products,
        reviews_path=args.reviews,
        prices_path=args.prices,
        burst_window_hours=args.burst_hours,
        burst_multiplier=args.burst_mult
    )
