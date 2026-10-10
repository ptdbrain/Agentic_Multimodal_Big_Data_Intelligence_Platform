"""Apache Airflow DAG: sentinel_daily_pipeline
Canonical Production Big Data Orchestration DAG synchronized with scripts/run_e2e_pipeline.py:
  Stage 1: Source / Dataset / Crawler
  Stage 2: Kafka Producer -> Kafka -> RawConsumer
  Stage 3: MinIO Bronze Verification
  Stage 4: Spark Batch -> Silver / Quarantine / DQ
  Stage 5: Spark Gold Marts & Anomaly Events
  Stage 6: PostgreSQL Warehouse -> Dashboard Verification
"""
import sys
import time
import datetime
from pathlib import Path
from typing import Any, Dict

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from config.settings import settings
from storage.storage_manager import storage
from scripts.run_e2e_pipeline import (
    configure_pipeline_mode,
    stage_1_ingest_sources,
    stage_2_kafka_produce_and_consume,
    stage_3_verify_bronze,
    stage_4_spark_batch_silver_dq,
    stage_5_spark_gold_marts,
    stage_6_warehouse_and_dashboard,
)

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

# Shared execution state across sequential tasks within a run
_DAG_RUN_STATE: Dict[str, Any] = {}


def task_start(**context):
    print(">>> [DAG: sentinel_daily_pipeline] Stage 0: Initializing pipeline execution context")
    effective_mode = configure_pipeline_mode(settings.runtime_mode)
    _DAG_RUN_STATE.clear()
    _DAG_RUN_STATE["effective_mode"] = effective_mode
    return {"status": "INITIALIZED", "mode": effective_mode, "start_time": time.time()}


def task_collect_and_publish(**context):
    """Canonical Stages 1-3: Ingest sources -> Kafka Producer -> RawConsumer -> Verify MinIO Bronze."""
    effective_mode = _DAG_RUN_STATE.get("effective_mode") or settings.runtime_mode
    s1 = stage_1_ingest_sources()
    s2 = stage_2_kafka_produce_and_consume(
        products_raw=s1["products_raw"],
        reviews_raw=s1["reviews_raw"],
        prices_raw=s1["prices_raw"],
        effective_mode=effective_mode
    )
    s3 = stage_3_verify_bronze(consume_result=s2, effective_mode=effective_mode)
    _DAG_RUN_STATE["batches_by_topic"] = s3["batches_by_topic"]
    _DAG_RUN_STATE["include_prices"] = bool(s1["prices_raw"])
    return {
        "products_count": len(s1["products_raw"]),
        "reviews_count": len(s1["reviews_raw"]),
        "prices_count": len(s1["prices_raw"]),
        "bronze_batches": s3["batches_by_topic"]
    }


def task_spark_batch(**context):
    """Canonical Stage 4: Spark Batch ETL reading directly from Bronze storage -> Silver / Quarantine / DQ."""
    batches_by_topic = _DAG_RUN_STATE.get("batches_by_topic")
    include_prices = _DAG_RUN_STATE.get("include_prices", True)
    s4 = stage_4_spark_batch_silver_dq(
        batches_by_topic=batches_by_topic,
        include_prices=include_prices
    )
    _DAG_RUN_STATE["s4"] = s4
    return s4["etl_res"]


def task_data_quality(**context):
    """Verifies Data Quality scorecards persisted in Gold layer by Stage 4."""
    print(">>> [DAG] Verifying Data Quality Scorecards in Gold Layer")
    dq_df = storage.read_gold_parquet("data_quality_metrics")
    if dq_df.empty or not dq_df["batch_id"].is_unique:
        raise RuntimeError("DAG Data Quality check failed: invalid or duplicate DQ metrics in Gold.")
    print(f"Verified DQ scorecard records: {len(dq_df)}")
    return {"dq_records": len(dq_df)}


def task_gold_analytics(**context):
    """Canonical Stage 5: Builds all Gold Analytical Marts & Anomaly Events via SparkGoldBuilder."""
    s4 = _DAG_RUN_STATE.get("s4", {})
    s5 = stage_5_spark_gold_marts(
        df_prods=s4.get("df_prods"),
        df_revs=s4.get("df_revs"),
        df_prices=s4.get("df_prices")
    )
    return {"marts": list(s5["marts"].keys()), "anomalies_count": s5["anomalies_count"]}


def task_warehouse_upsert(**context):
    """Canonical Stage 6: Exports Gold Analytical Marts to Warehouse via UPSERT and verifies DashboardDB."""
    effective_mode = _DAG_RUN_STATE.get("effective_mode") or settings.runtime_mode
    s6 = stage_6_warehouse_and_dashboard(effective_mode=effective_mode)
    return s6


def task_finish(**context):
    """Records idempotent pipeline execution telemetry into Warehouse."""
    print(">>> [DAG] Recording Pipeline Metrics & Finalizing")
    import pandas as pd
    from database.warehouse import WarehouseManager

    effective_mode = _DAG_RUN_STATE.get("effective_mode") or settings.runtime_mode
    is_integration = effective_mode in ("integration", "production")
    wh = WarehouseManager(use_postgres=is_integration, mode=effective_mode)
    try:
        run_record = pd.DataFrame([{
            'metric_id': 'dag_sentinel_daily_latest',
            'stage': 'dag_sentinel_daily',
            'metric_name': 'execution_success',
            'metric_value': 1.0,
            'unit': 'status',
            'timestamp': datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        }])
        wh.upsert_dataframe('pipeline_metrics', run_record, conflict_keys=['metric_id'])
    finally:
        wh.close()
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


def run_dag_standalone(mode: str = None):
    """Executes the complete canonical DAG sequentially in standalone CLI mode."""
    if mode is not None:
        settings.set_runtime_mode(mode)
        storage.set_mode(mode)
    print(f"Executing Airflow DAG: {dag.dag_id} (Standalone Runner, mode={settings.runtime_mode})")
    task_start()
    task_collect_and_publish()
    task_spark_batch()
    task_data_quality()
    task_gold_analytics()
    task_warehouse_upsert()
    task_finish()
    print(f"Airflow DAG '{dag.dag_id}' finished with status: SUCCESS")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="SentinelAI Standalone DAG Runner")
    parser.add_argument("--mode", choices=["integration", "local", "test"], default=None)
    args = parser.parse_args()
    try:
        run_dag_standalone(mode=args.mode)
    except Exception as exc:
        print(f"\n[DAG FAILED] {exc}", file=sys.stderr)
        sys.exit(1)
