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


def report_run_failure(context):
    from operations.status import OperationStatus
    with OperationStatus(mode=context.get('params',{}).get('mode',settings.runtime_mode)) as status:
        status.fail(str(context.get('run_id')))


default_args = {
    'owner': 'sentinel_data_engineering',
    'depends_on_past': False,
    'start_date': datetime.datetime(2026, 9, 24),
    'email_on_failure': False,
    'on_failure_callback': report_run_failure,
    'retries': 2,
    'retry_delay': datetime.timedelta(minutes=5),
}

dag = DAG(
    'sentinel_daily_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
)

from orchestration.run_manifest import RunManifest, save_manifest, load_manifest, RunLease
import uuid


def _manifest(context):
    path = context.get("manifest_path")
    ti = context.get("ti") or context.get("task_instance")
    if path is None and ti is not None:
        path = ti.xcom_pull(task_ids="start", key="return_value")["manifest_path"]
    if not path:
        raise ValueError("Task requires a run manifest")
    mode = context.get("params", {}).get("mode", settings.runtime_mode)
    settings.set_runtime_mode(mode)
    storage.set_mode(mode)
    m = load_manifest(path)
    if m.mode != mode:
        raise ValueError("Manifest mode differs from worker mode")
    lease = RunLease(m.mode)
    try:
        lease.assert_owner(m.run_id)
    finally:
        lease.close()
    return m


def _task_start(**context):
    mode = context.get("params", {}).get("mode", settings.runtime_mode)
    mode = configure_pipeline_mode(mode)
    run_id = str(context.get("run_id") or uuid.uuid4())
    lease = RunLease(mode)
    try:
        lease.acquire(run_id)
        import hashlib
        path = f"runs/{hashlib.sha256(run_id.encode()).hexdigest()}/manifest.json"
        if storage.object_exists(path):
            manifest = load_manifest(path)
            if manifest.run_id != run_id or manifest.mode != mode or manifest.status != "running":
                raise ValueError("Existing manifest cannot resume this run")
        else:
            manifest = RunManifest(run_id=run_id, mode=mode)
            path = save_manifest(manifest)
    except Exception:
        lease.release(run_id)
        raise
    finally:
        lease.close()
    from operations.status import OperationStatus
    with OperationStatus(mode=mode) as status:
        status.start(run_id,'commerce','unverified')
    return {"manifest_path": path, "run_id": run_id}


def task_start(**context):
    from operations.backup import writer_admission
    mode=context.get('params',{}).get('mode',settings.runtime_mode)
    with writer_admission(mode):
        return _task_start(**context)


def task_collect_and_publish(**context):
    m = _manifest(context)
    s1 = stage_1_ingest_sources(source_type=m.source_type)
    s2 = stage_2_kafka_produce_and_consume(
        products_raw=s1["products_raw"], reviews_raw=s1["reviews_raw"],
        prices_raw=s1["prices_raw"], effective_mode=m.mode,
    )
    s3 = stage_3_verify_bronze(consume_result=s2, effective_mode=m.mode)
    m.batches_by_topic = s3["batches_by_topic"]
    m.required_datasets = ["products", "reviews"] + (["prices"] if s1["prices_raw"] else [])
    return {"manifest_path": save_manifest(m)}


def task_spark_batch(**context):
    m = _manifest(context)
    if not m.batches_by_topic:
        raise ValueError("Run manifest contains no Bronze batches")
    s4 = stage_4_spark_batch_silver_dq(
        batches_by_topic=m.batches_by_topic, include_prices="prices" in m.required_datasets,
        run_id=m.run_id,
    )
    etl = s4["etl_res"]
    m.dq_metrics = [etl[k] for k in ("products_dq", "reviews_dq", "prices_dq") if etl.get(k)]
    m.dq_report_ids = [f"dq:{m.run_id}:{d}" for d in m.required_datasets]
    from spark.spark_session import SparkSessionFactory
    m.silver_paths = {d: SparkSessionFactory.resolve_storage_path(d, layer="silver") for d in m.required_datasets}
    return {"manifest_path": save_manifest(m), "dq_metrics": m.dq_metrics}


def task_data_quality(**context):
    m = _manifest(context)
    from spark.quality.spark_dq import SparkDataQualityEvaluator
    return SparkDataQualityEvaluator.check_dq_gate(
        m.dq_metrics, run_id=m.run_id, required_datasets=m.required_datasets,
    )


def task_gold_analytics(**context):
    import pandas as pd
    m = _manifest(context)
    s5 = stage_5_spark_gold_marts(dq_metrics=m.dq_metrics, run_id=m.run_id,
                                 required_datasets=m.required_datasets, source_run_id=m.run_id,
                                 df_prices=None if 'prices' in m.required_datasets else pd.DataFrame())
    return {"marts": list(s5["marts"]), "anomalies_count": s5["anomalies_count"]}


def task_warehouse_upsert(**context):
    m = _manifest(context)
    return stage_6_warehouse_and_dashboard(effective_mode=m.mode)


def task_finish(**context):
    m = _manifest(context)
    import pandas as pd
    from database.warehouse import WarehouseManager
    wh = WarehouseManager(mode=m.mode)
    try:
        wh.upsert_dataframe("pipeline_metrics", pd.DataFrame([{
            "metric_id": f"dag:{m.run_id}", "stage": "dag_sentinel_daily",
            "metric_name": "execution_success", "metric_value": 1.0, "unit": "status",
            "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        }]), conflict_keys=["metric_id"])
    finally:
        wh.close()
    from operations.status import OperationStatus
    with OperationStatus(mode=m.mode) as status:
        received=sum(int(row.get('records_received',0)) for row in m.dq_metrics)
        valid=sum(int(row.get('records_valid',0)) for row in m.dq_metrics)
        quarantined=sum(int(row.get('records_quarantined',0)) for row in m.dq_metrics)
        status.finish(m.run_id,received,valid,quarantined)
    m.status = "finished"
    save_manifest(m)
    lease = RunLease(m.mode)
    try:
        lease.release(m.run_id)
    finally:
        lease.close()
    return {"run_id": m.run_id, "status": "SUCCESS"}


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
    start_result = task_start()
    context = {"manifest_path": start_result["manifest_path"]}
    task_collect_and_publish(**context)
    task_spark_batch(**context)
    task_data_quality(**context)
    task_gold_analytics(**context)
    task_warehouse_upsert(**context)
    task_finish(**context)
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
