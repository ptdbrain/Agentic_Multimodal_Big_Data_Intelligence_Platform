"""Managed source DAGs are enabled only by an explicit private configuration."""

from pathlib import Path
import json, os, subprocess, sys

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def run_source_task(config_path, job_id, action, **context):
    run_id = str(context.get("run_id") or context.get("dag_run").run_id)
    python = os.environ.get("SOURCE_JOB_PYTHON", sys.executable)
    environment = dict(os.environ)
    ti = context.get("ti") or context.get("task_instance")
    if ti is not None:
        environment["SENTINEL_AIRFLOW_OWNER"] = json.dumps({"dag_id": ti.dag_id, "dag_run_id": run_id})
    else:
        environment.pop("SENTINEL_AIRFLOW_OWNER", None)
    subprocess.run(
        [
            python,
            str(ROOT / "scripts/run_source_job.py"),
            action,
            "--config",
            str(config_path),
            "--job",
            job_id,
            "--run-id",
            run_id,
            "--scheduler",
            "airflow",
        ],
        cwd=ROOT,
        env=environment,
        check=True,
    )


def configured_jobs():
    import yaml
    from orchestration.source_jobs import validate_config

    path = os.environ.get("SOURCE_JOBS_CONFIG")
    if not path:
        return []
    config = validate_config(yaml.safe_load(Path(path).read_text()))
    return [(Path(path).resolve(), job) for job in config["jobs"] if job["owner"] == "airflow"]


try:
    from airflow import DAG
    from airflow.operators.python import PythonOperator
except ImportError:
    DAG = None

if DAG:
    from datetime import timedelta
    import pendulum

    for config_path, job in configured_jobs():
        dag = DAG(
            "sentinel_source_" + job["id"],
            start_date=pendulum.datetime(2026, 11, 12, tz=job["timezone"]),
            schedule_interval=job["schedule"],
            catchup=False,
            max_active_runs=1,
            default_args={
                "retries": 2,
                "retry_delay": timedelta(minutes=5),
                "execution_timeout": timedelta(minutes=45),
            },
        )
        capture = PythonOperator(
            task_id="capture",
            python_callable=run_source_task,
            op_kwargs={"config_path": str(config_path), "job_id": job["id"], "action": "capture"},
            dag=dag,
        )
        process = PythonOperator(
            task_id="process",
            python_callable=run_source_task,
            op_kwargs={"config_path": str(config_path), "job_id": job["id"], "action": "process"},
            dag=dag,
        )
        capture >> process
        globals()[dag.dag_id] = dag
