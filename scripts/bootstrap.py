"""SentinelAI Bootstrap Script
One-command script to initialize and run the entire pipeline end-to-end.
"""
import sys
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

def main():
    print("=" * 60)
    print("  SENTINELAI PIPELINE BOOTSTRAP")
    print("=" * 60)
    
    # 1. Provision Kafka Topics
    print("\n[1/7] Provisioning Kafka topics...")
    try:
        from kafka.topics.topic_manager import provision_topics
        provision_topics()
    except Exception as e:
        print(f"Topic provisioning skipped: {e}")
    
    # 2. MinIO Buckets / Storage Directories
    print("\n[2/7] Initializing Storage / MinIO buckets...")
    from storage.storage_manager import storage
    print(f"Data Lake root / bucket ready: {storage.base_dir}")
    
    # 3. PostgreSQL / SQLite Schema
    print("\n[3/7] Initializing database schema...")
    try:
        from database.warehouse import WarehouseManager
        wm = WarehouseManager()
        wm.initialize_schema()
        print(f"Database schema initialized ({wm._engine_type}).")
    except Exception as e:
        print(f"Database initialization failed: {e}")
    time.sleep(1)
    
    # 4. Generate Sample Data
    print("\n[4/7] Generating sample data...")
    from data.generator import generate_products, generate_reviews
    import json
    
    products = generate_products()
    reviews = generate_reviews(products, count=500)
    
    sample_dir = REPO_ROOT / "data" / "sample"
    sample_dir.mkdir(parents=True, exist_ok=True)
    
    with open(sample_dir / "products.json", "w") as f:
        json.dump(products, f)
    with open(sample_dir / "reviews.json", "w") as f:
        json.dump(reviews, f)
    print("Sample data generated.")
    
    # 5. Run Spark Batch ETL
    print("\n[5/7] Running Spark Batch ETL...")
    try:
        from spark.batch.spark_etl_job import SparkBatchETLJob
        etl = SparkBatchETLJob()
        etl.run_pipeline(str(sample_dir / "products.json"), str(sample_dir / "reviews.json"))
    except Exception as e:
        print(f"Spark unavailable, falling back to Pandas ETL... {e}")
        from spark.batch.batch_etl_job import BatchETLJob
        BatchETLJob.run_pipeline(str(sample_dir / "products.json"), str(sample_dir / "reviews.json"))
    
    # 6. Build Gold Marts
    print("\n[6/7] Building Gold analytical marts...")
    from spark.etl.gold_aggregator import GoldAggregator
    GoldAggregator.build_gold_marts()
    
    # 7. Export to Warehouse
    print("\n[7/7] Exporting Gold marts to PostgreSQL...")
    from database.export_gold import export_gold_to_db
    export_gold_to_db()
    
    print("\n" + "=" * 60)
    print("  BOOTSTRAP COMPLETE! You can now start the dashboard:")
    print("  streamlit run dashboard/app.py")
    print("=" * 60)

if __name__ == "__main__":
    main()
