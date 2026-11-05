"""Runs the real LocalExecutor via backfill; requires a dedicated metadata PostgreSQL."""
import os
import subprocess
from pathlib import Path
import pytest

pytestmark=[pytest.mark.integration,pytest.mark.airflow]


def test_airflow_local_executor_completes_pipeline(tmp_path,record_property):
    assert os.environ.get('SENTINEL_AIRFLOW_DISPOSABLE_METADATA')=='true', 'Use a dedicated disposable metadata database and explicitly set SENTINEL_AIRFLOW_DISPOSABLE_METADATA=true'
    import airflow
    from airflow.models import DagBag
    folder=Path(__file__).resolve().parents[2]/'orchestration/dags'
    bag=DagBag(str(folder),include_examples=False)
    assert not bag.import_errors
    assert bag.get_dag('sentinel_daily_pipeline') is not None
    assert os.environ.get('AIRFLOW__CORE__EXECUTOR')=='LocalExecutor'
    from datetime import datetime, timezone
    started=datetime.now(timezone.utc)
    result=subprocess.run(['airflow','dags','backfill','sentinel_daily_pipeline',
                           '--start-date','2026-10-25','--end-date','2026-10-25','--reset-dagruns','--yes'],
                          env=dict(os.environ,RUNTIME_MODE="test",LOCAL_DATA_DIR=str(tmp_path/'executor-lake'),
                                   SQLITE_DB_PATH=str(tmp_path/'executor-warehouse.sqlite')),
                          capture_output=True,text=True,timeout=900)
    assert result.returncode==0,result.stdout[-10000:]+result.stderr[-10000:]

    from airflow.models import DagRun, TaskInstance
    from airflow.utils.session import create_session
    with create_session() as session:
        run=session.query(DagRun).filter(
            DagRun.dag_id=='sentinel_daily_pipeline',
            DagRun.run_id=='backfill__2026-10-25T00:00:00+00:00',
        ).one()
        assert run.state=='success' and run.start_date>=started, 'Backfill reused an old completed run'
        tasks=session.query(TaskInstance).filter(
            TaskInstance.dag_id==run.dag_id,TaskInstance.run_id==run.run_id).all()
        assert {task.task_id for task in tasks}=={
            'start','collect_and_publish','spark_batch','data_quality',
            'gold_analytics','warehouse_upsert','finish'}
        assert all(task.state=='success' and task.start_date>=started for task in tasks)
        record_property('executor','LocalExecutor')
        record_property('fresh_tasks_executed',len(tasks))
        record_property('dagrun_started',run.start_date.isoformat())
        record_property('dagrun_ended',run.end_date.isoformat())
