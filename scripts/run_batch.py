"""SentinelAI Run Batch CLI Script.
Executes the batch ETL and Gold analytics pipeline with Spark / fallback engine.
"""
import sys
import argparse
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from config.settings import settings

def main():
    parser = argparse.ArgumentParser(description="SentinelAI Batch Pipeline Runner")
    parser.add_argument("--products", default=None, help="Path to products raw file")
    parser.add_argument("--reviews", default=None, help="Path to reviews raw file")
    parser.add_argument("--engine", choices=["spark", "pandas", "auto"], default="auto",
                        help="Execution engine for batch processing")
    parser.add_argument("--export-db", action="store_true", default=True, help="Export Gold marts to database")
    args = parser.parse_args()

    sample_prods = args.products or str(REPO_ROOT / "data" / "sample" / "products.json")
    sample_revs = args.reviews or str(REPO_ROOT / "data" / "sample" / "reviews.json")

    print("=" * 60)
    print("      SENTINELAI BATCH ETL RUNNER")
    print(f"  Products Source: {sample_prods}")
    print(f"  Reviews Source:  {sample_revs}")
    print(f"  Engine Mode:     {args.engine}")
    print("=" * 60)

    # 1. Run ETL
    if args.engine == "spark":
        from spark.batch.spark_etl_job import SparkBatchETLJob
        job = SparkBatchETLJob()
        etl_result = job.run_pipeline(sample_prods, sample_revs)
    elif args.engine == "pandas":
        from spark.batch.batch_etl_job import BatchETLJob
        etl_result = BatchETLJob.run_pipeline(sample_prods, sample_revs)
    else:
        # Auto: try spark, fallback to pandas
        try:
            from spark.batch.spark_etl_job import SparkBatchETLJob
            job = SparkBatchETLJob()
            etl_result = job.run_pipeline(sample_prods, sample_revs)
            print("[Engine] Spark Batch ETL executed successfully.")
        except Exception as e:
            print(f"[Engine] Spark failed or unavailable ({e}). Using standard BatchETLJob...")
            from spark.batch.batch_etl_job import BatchETLJob
            etl_result = BatchETLJob.run_pipeline(sample_prods, sample_revs)

    # 2. Build Gold Marts
    print("\n>>> Aggregating Gold Analytical Marts...")
    from spark.etl.gold_aggregator import GoldAggregator
    marts = GoldAggregator.build_gold_marts()
    print(f"[Gold] Generated {len(marts)} marts: {list(marts.keys())}")

    # 3. Export to Database
    if args.export_db:
        print("\n>>> Syncing Gold Marts to Data Warehouse...")
        from database.export_gold import export_gold_to_db
        export_gold_to_db()

    print("\n[Done] Batch execution completed successfully.")

if __name__ == "__main__":
    main()
