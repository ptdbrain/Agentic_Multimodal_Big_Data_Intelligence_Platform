"""Runs the real LocalExecutor via backfill; requires a dedicated metadata PostgreSQL."""
import os
import subprocess
from pathlib import Path
import pytest

pytestmark=[pytest.mark.integration,pytest.mark.airflow]


def test_airflow_local_executor_completes_pipeline():
    import airflow
    from airflow.models import DagBag
    folder=Path(__file__).resolve().parents[2]/'orchestration/dags'
    bag=DagBag(str(folder),include_examples=False)
    assert not bag.import_errors
    assert bag.get_dag('sentinel_daily_pipeline') is not None
    assert os.environ.get('AIRFLOW__CORE__EXECUTOR')=='LocalExecutor'
    result=subprocess.run(['airflow','dags','backfill','sentinel_daily_pipeline',
                           '--start-date','2026-10-25','--end-date','2026-10-25'],
                          env=dict(os.environ,RUNTIME_MODE="test"),capture_output=True,text=True,timeout=900)
    assert result.returncode==0,result.stdout[-10000:]+result.stderr[-10000:]
