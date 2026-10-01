import sys
import os
import json
import argparse
from pathlib import Path
from typing import Optional

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from spark.batch.batch_etl_job import BatchETLJob
from spark.etl.gold_aggregator import GoldAggregator
from database.export_gold import export_gold_to_db
from analytics.descriptive.stats import DescriptiveStats
from analytics.anomaly.review_burst import ReviewBurstDetector
from analytics.anomaly.price_anomaly import PriceAnomalyDetector
from storage.storage_manager import storage

def run_end_to_end(
    products_path: Optional[str] = None,
    reviews_path: Optional[str] = None,
    burst_window_hours: Optional[int] = None,
    burst_multiplier: Optional[float] = None
) -> bool:
    print("==========================================================")
    print("      SENTINELAI END-TO-END PIPELINE VERIFICATION         ")
    print("==========================================================")

    sample_prods = products_path or str(REPO_ROOT / "data" / "sample" / "products.json")
    sample_revs = reviews_path or str(REPO_ROOT / "data" / "sample" / "reviews.json")

    # Step 1: Batch ETL
    print("\n[Step 1] Executing Batch ETL (Bronze -> Silver)...")
    etl_res = BatchETLJob.run_pipeline(sample_prods, sample_revs)

    # Step 2: Build Gold Data Marts
    print("\n[Step 2] Aggregating Gold Analytical Marts...")
    marts = GoldAggregator.build_gold_marts()

    # Step 3: Export to Database
    print("\n[Step 3] Exporting Gold Data to Database...")
    export_gold_to_db()

    # Step 4: Run Anomaly Detection
    print("\n[Step 4] Running Anomaly Detection...")
    df_revs = storage.read_silver_parquet("reviews")
    bursts = ReviewBurstDetector.detect_bursts(
        df_revs,
        window_hours=burst_window_hours,
        threshold_multiplier=burst_multiplier
    )
    print(f"Detected {len(bursts)} review bursts/bombing events.")

    # Step 5: Summary
    df_prods = storage.read_silver_parquet("products")
    summary = DescriptiveStats.compute_summary(df_prods, df_revs)
    print("\n[Pipeline Complete] Verification Summary:")
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

