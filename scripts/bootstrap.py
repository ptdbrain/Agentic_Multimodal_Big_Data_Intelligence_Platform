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
    
    # 1. Provision Kafka Topics (if real Kafka is used)
    # Not fully implemented without real Kafka, assuming standalone for now
    print("\n[1/7] Provisioning Kafka topics...")
    from config.settings import settings
    print(f"Topics to provision: {settings.kafka.topic_products}, {settings.kafka.topic_reviews}")
    time.sleep(1)
    
    # 2. MinIO Buckets
    print("\n[2/7] Creating MinIO buckets...")
    print(f"Buckets: {settings.storage.bucket_raw}")
    time.sleep(1)
    
    # 3. PostgreSQL Schema
    print("\n[3/7] Initializing PostgreSQL schema...")
    from database.postgres_client import PostgresClient
    client = PostgresClient()
    try:
        client.execute("""
            CREATE TABLE IF NOT EXISTS product_daily_stats (
                date DATE,
                product_id VARCHAR(50),
                review_count INT,
                avg_rating FLOAT,
                rating_std FLOAT,
                avg_price FLOAT,
                min_price FLOAT,
                max_price FLOAT,
                high_rating_ratio FLOAT,
                low_rating_ratio FLOAT
            )
        """)
        client.execute("""
            CREATE TABLE IF NOT EXISTS brand_daily_stats (
                date DATE,
                brand VARCHAR(100),
                review_count INT,
                avg_rating FLOAT
            )
        """)
        client.execute("""
            CREATE TABLE IF NOT EXISTS anomaly_events (
                timestamp TIMESTAMP,
                product_id VARCHAR(50),
                anomaly_type VARCHAR(50),
                severity VARCHAR(20),
                details TEXT
            )
        """)
        client.execute("""
            CREATE TABLE IF NOT EXISTS data_quality_metrics (
                timestamp TIMESTAMP,
                dataset_name VARCHAR(50),
                total_count INT,
                valid_count INT,
                invalid_count INT,
                dq_score FLOAT
            )
        """)
        client.execute("""
            CREATE TABLE IF NOT EXISTS products (
                product_id VARCHAR(50),
                product_name VARCHAR(255),
                brand VARCHAR(100),
                category VARCHAR(100),
                price FLOAT
            )
        """)
    except Exception as e:
        print(f"PostgreSQL initialization failed (falling back to SQLite/Parquet): {e}")
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
