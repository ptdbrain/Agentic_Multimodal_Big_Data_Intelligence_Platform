"""The built image must import project DAGs from its Airflow Python environment."""

import json
import os
import subprocess
import pytest

pytestmark = [pytest.mark.integration, pytest.mark.docker]


def test_built_airflow_environment_imports_real_project_dag():
    program = '''import json,sys,pandas,pyspark
import orchestration.dags.sentinel_daily_pipeline as pipeline
print(json.dumps({"dag":pipeline.dag.dag_id,"python":sys.executable,
                  "pandas":pandas.__version__,"pyspark":pyspark.__version__}))'''
    result = subprocess.run(
        ['docker','run','--rm','--pull','never','--entrypoint','/opt/airflow-venv/bin/python',
         '-e','AIRFLOW_HOME=/tmp/dag-import',
         '-e','AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=sqlite:////tmp/dag-import.sqlite',
         '-e','AIRFLOW__CORE__EXECUTOR=SequentialExecutor',
         os.environ.get('SENTINEL_TEST_APPLICATION_IMAGE','sentinel-application:local'),'-c',program],
        capture_output=True,text=True,timeout=90,
    )
    assert result.returncode == 0, result.stderr[-3000:]
    payload = json.loads(result.stdout.splitlines()[-1])
    assert payload['dag']=='sentinel_daily_pipeline'
    assert payload['python']=='/opt/airflow-venv/bin/python'
    assert payload['pyspark']=='3.5.0'
