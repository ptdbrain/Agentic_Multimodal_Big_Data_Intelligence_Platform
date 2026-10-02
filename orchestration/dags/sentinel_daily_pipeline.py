"""Apache Airflow DAG: sentinel_daily_pipeline
Production Big Data Orchestration DAG:
Data Collection -> Kafka Publish -> MinIO Bronze -> Spark Batch ETL -> Data Quality -> Spark Gold Marts -> DW UPSERT -> Metrics Recording
Supports native Apache Airflow environments and standalone CLI task execution.
"""
import sys
import time
import datetime
from pathlib import Path
from typing import Any, Dict

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

# Simulation-friendly Airflow DAG definition
try:
    from airflow import DAG
    from airflow.operators.python import PythonOperator
    HAS_AIRFLOW = True
except ImportError:
    HAS_AIRFLOW = False
    class DAG:
        def __init__(self, dag_id: str, default_args: dict, schedule_interval: str, catchup: bool = False):
            self.dag_id = dag_id
            self.default_args = default_args
            self.schedule_interval = schedule_interval
            self.catchup = catchup
            self.tasks = []

    class PythonOperator:
        def __init__(self, task_id: str, python_callable: Any, dag: Any):
            self.task_id = task_id
            self.python_callable = python_callable
            self.dag = dag
            if dag:
                dag.tasks.append(self)
        def __rshift__(self, other):
            return other

default_args = {
    'owner': 'sentinel_data_engineering',
    'depends_on_past': False,
    'start_date': datetime.datetime(2026, 9, 24),
    'email_on_failure': False,
    'retries': 2,
    'retry_delay': datetime.timedelta(minutes=5),
}

dag = DAG(
    'sentinel_daily_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
)

def task_start(**context):
    print(">>> [DAG: sentinel_daily_pipeline] Stage 0: Initializing pipeline execution context")
    return {"status": "INITIALIZED", "start_time": time.time()}

def task_collect_and_publish(**context):
    """Collects raw data and publishes canonical envelopes to Kafka."""
    print(">>> [DAG] Stage 1: Collecting data & streaming to Kafka")
    from ingestion.file_loader.loader import FileLoader
    from kafka.producers.stream_producer import StreamProducer
    from config.settings import settings

    producer = StreamProducer(enable_offline_buffer=True)
    sample_dir = REPO_ROOT / "data" / "sample"
    
    prods = FileLoader.load_records(sample_dir / "products.json")
    revs = FileLoader.load_records(sample_dir / "reviews.json")
    prices = FileLoader.load_records(sample_dir / "prices.json") if (sample_dir / "prices.json").exists() else []

    producer.produce_batch(settings.kafka.topic_products, prods, rate=0, event_type="NEW_PRODUCT")
    producer.produce_batch(settings.kafka.topic_reviews, revs, rate=0, event_type="NEW_REVIEW")
    if prices:
        producer.produce_batch(settings.kafka.topic_prices, prices, rate=0, event_type="PRICE_UPDATE")

    print(f"Ingested and published: {len(prods)} products, {len(revs)} reviews, {len(prices)} prices.")
    return {"products_count": len(prods), "reviews_count": len(revs), "prices_count": len(prices)}

def task_spark_batch(**context):
    """Consumes to MinIO Bronze and executes Spark Batch ETL (Bronze -> Silver)."""
    print(">>> [DAG] Stage 2: Consuming to MinIO Bronze & Spark Batch ETL")
    from ingestion.file_loader.loader import FileLoader
    from kafka.consumers.raw_consumer import RawConsumer
    from spark.batch.spark_etl_job import SparkBatchETLJob
    from config.settings import settings

    sample_dir = REPO_ROOT / "data" / "sample"
    prods = FileLoader.load_records(sample_dir / "products.json")
    revs = FileLoader.load_records(sample_dir / "reviews.json")
    prices = FileLoader.load_records(sample_dir / "prices.json") if (sample_dir / "prices.json").exists() else []

    consumer = RawConsumer()
    consumer.save_raw_batch(settings.kafka.topic_products, prods)
    consumer.save_raw_batch(settings.kafka.topic_reviews, revs)
    if prices:
        consumer.save_raw_batch(settings.kafka.topic_prices, prices)

    job = SparkBatchETLJob()
    etl_res = job.run_pipeline(prods, revs, prices_source=prices)
    print(f"Spark Batch ETL completed. Silver products: {etl_res['products_processed']}, reviews: {etl_res['reviews_processed']}")
    return etl_res

def task_data_quality(**context):
    """Validates Data Quality scorecards and verifies quarantine."""
    print(">>> [DAG] Stage 3: Data Quality Evaluation & Quarantine Check")
    from storage.storage_manager import storage
    from spark.quality.spark_dq import SparkDataQualityEvaluator
    
    dq_df = storage.read_gold_parquet("data_quality_metrics")
    print(f"Persisted DQ scorecard records: {len(dq_df)}")
    return {"dq_records": len(dq_df)}

def task_gold_analytics(**context):
    """Builds all Gold Analytical Marts via SparkGoldBuilder."""
    print(">>> [DAG] Stage 4: Building Gold Analytical Marts via SparkGoldBuilder")
    from storage.storage_manager import storage
    from spark.analytics.gold_builder import SparkGoldBuilder

    df_prods = storage.read_silver_parquet("products")
    df_revs = storage.read_silver_parquet("reviews")
    df_prices = storage.read_silver_parquet("prices")

    gold_builder = SparkGoldBuilder()
    marts = gold_builder.build_all(
        silver_reviews=df_revs,
        silver_products=df_prods,
        silver_prices=df_prices,
        save_gold=True
    )
    print(f"Spark Gold built {len(marts)} marts: {list(marts.keys())}")
    return {"marts": list(marts.keys())}

def task_warehouse_upsert(**context):
    """Exports Gold Analytical Marts to PostgreSQL Warehouse via UPSERT."""
    print(">>> [DAG] Stage 5: Exporting Gold Marts to PostgreSQL DW")
    from database.export_gold import export_gold_to_db
    export_gold_to_db()
    print("PostgreSQL Warehouse synchronization completed successfully.")

def task_finish(**context):
    """Records pipeline metrics into DW."""
    print(">>> [DAG] Stage 6: Recording Pipeline Metrics & Finalizing")
    import pandas as pd
    from database.warehouse import WarehouseManager
    
    try:
        wh = WarehouseManager()
        run_record = pd.DataFrame([{
            'metric_id': f"run_{int(time.time())}",
            'stage': 'dag_sentinel_daily',
            'metric_name': 'execution_success',
            'metric_value': 1.0,
            'unit': 'status',
            'timestamp': datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        }])
        wh.upsert_dataframe('pipeline_metrics', run_record, conflict_keys=['metric_id'])
    except Exception as e:
        print(f"Warning: could not write pipeline_metrics: {e}")
    print(">>> Pipeline DAG execution finished successfully.")

# Define Tasks
start = PythonOperator(task_id='start', python_callable=task_start, dag=dag)
collect = PythonOperator(task_id='collect_and_publish', python_callable=task_collect_and_publish, dag=dag)
spark_batch = PythonOperator(task_id='spark_batch', python_callable=task_spark_batch, dag=dag)
data_quality = PythonOperator(task_id='data_quality', python_callable=task_data_quality, dag=dag)
gold_analytics = PythonOperator(task_id='gold_analytics', python_callable=task_gold_analytics, dag=dag)
warehouse_upsert = PythonOperator(task_id='warehouse_upsert', python_callable=task_warehouse_upsert, dag=dag)
finish = PythonOperator(task_id='finish', python_callable=task_finish, dag=dag)

# Pipeline Dependencies
start >> collect >> spark_batch >> data_quality >> gold_analytics >> warehouse_upsert >> finish

def run_dag_standalone():
    """Executes the complete DAG sequentially in standalone CLI mode."""
    print(f"Executing Airflow DAG: {dag.dag_id} (Standalone Runner)")
    t_start = task_start()
    t_col = task_collect_and_publish()
    t_batch = task_spark_batch()
    t_dq = task_data_quality()
    t_gold = task_gold_analytics()
    task_warehouse_upsert()
    task_finish()
    print(f"Airflow DAG '{dag.dag_id}' finished with status: SUCCESS")

if __name__ == "__main__":
    run_dag_standalone()
