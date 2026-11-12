"""Rehearse a complete isolated recovery using actual services and LocalExecutor."""

from pathlib import Path
from contextlib import contextmanager
from datetime import datetime, timezone
import argparse, hashlib, json, os, shlex, subprocess, sys, time, uuid

root = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(root))
parser = argparse.ArgumentParser(
    description="Full isolated PostgreSQL/Airflow/MinIO/Kafka-checkpoint restore and LocalExecutor resumption rehearsal"
)
parser.add_argument("--source-state", type=Path, required=True)
parser.add_argument("--scheduler-state", type=Path, required=True)
parser.add_argument("--scheduler-environment", type=Path, required=True)
parser.add_argument("--pg-bin", type=Path, required=True)
parser.add_argument("--work-root", type=Path, required=True)
parser.add_argument("--report", type=Path, required=True)
args = parser.parse_args()
if args.report.exists() or args.work_root.exists():
    raise FileExistsError("Fresh rehearsal work/report required")
from operations.backup import (
    create_backup,
    restore_plan,
    restore_apply,
    restore_verify,
    maintenance_guard,
    kafka_drain_state,
)
from config.settings import settings
import psycopg2
from psycopg2 import sql
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic

runtime = root.parent / ".runtime"
source_state = json.loads(args.source_state.read_text())
scheduler = json.loads(args.scheduler_state.read_text())


def envfile(path):
    result = {}
    for line in Path(path).read_text().splitlines():
        if line.strip() and not line.startswith("#"):
            key, value = line.split("=", 1)
            parts = shlex.split(value)
            result[key] = parts[0] if parts else ""
    return result


import yaml

jobs = yaml.safe_load(Path(source_state["config"]).read_text())["jobs"]
tiki_job = next(job for job in jobs if job["adapter"] == "tiki")
source_environment = Path(tiki_job["environment_file"])
source_env = envfile(source_environment)
os.environ.update(source_env)
if any(envfile(job["environment_file"])["POSTGRES_DB"] != source_state["database"] for job in jobs):
    raise ValueError(
        "This rehearsal requires an explicit shared warehouse database; split configurations for separate databases"
    )
for key, attribute in [
    ("HOST", "host"),
    ("PORT", "port"),
    ("USER", "user"),
    ("PASSWORD", "password"),
    ("DB", "database"),
]:
    setattr(
        settings.database,
        attribute,
        int(source_env["POSTGRES_" + key]) if key == "PORT" else source_env["POSTGRES_" + key],
    )
settings.set_runtime_mode("integration")
nonce = uuid.uuid4().hex[:10]
work = args.work_root.resolve()
work.mkdir(parents=True, mode=0o700)
project_python = sys.executable
airflow_python = envfile(args.scheduler_environment)["SENTINEL_AIRFLOW_PYTHON"]
afenv = dict(os.environ, **envfile(args.scheduler_environment))
afenv.pop("PGOPTIONS", None)
afenv["AIRFLOW__LOGGING__LOG_FILENAME_TEMPLATE"] = (
    "dag_id={{ ti.dag_id }}/run_id={{ ti.run_id | replace(':','_') }}/task_id={{ ti.task_id }}/map={{ ti.map_index }}/attempt={{ try_number }}.log"
)
afenv["AIRFLOW__CORE__EXECUTE_TASKS_NEW_PYTHON_INTERPRETER"] = "true"
afenv["PATH"] = str(Path(airflow_python).parent) + os.pathsep + afenv["PATH"]
secret = uuid.uuid4().hex
connection_id = "de_restore_" + nonce
afenv["DE_PROBE_SECRET"] = secret
afenv["DE_PROBE_CONN_ID"] = connection_id
create_connection = "from airflow.models import Connection;from airflow.utils.session import create_session;import os\nwith create_session() as s:s.add(Connection(conn_id=os.environ['DE_PROBE_CONN_ID'],conn_type='generic',password=os.environ['DE_PROBE_SECRET']))"
with (work / "connection-create.log").open("x") as log:
    subprocess.run(
        [airflow_python, "-c", create_connection], env=afenv, stdout=log, stderr=subprocess.STDOUT, check=True
    )
expected = {}
with psycopg2.connect(
    host=source_env["POSTGRES_HOST"],
    port=source_env["POSTGRES_PORT"],
    user=source_env["POSTGRES_USER"],
    password=source_env["POSTGRES_PASSWORD"],
    dbname=source_state["database"],
) as baseline:
    for job in jobs:
        for table in ["articles"] if job["adapter"] == "web" else ["products", "reviews"]:
            schema = job["target"]["postgres_schema"]
            cursor = baseline.cursor()
            cursor.execute(sql.SQL("SELECT COUNT(*) FROM {}.{}").format(sql.Identifier(schema), sql.Identifier(table)))
            count = cursor.fetchone()[0]
            if count < 1:
                raise ValueError("An accepted source dataset is required before rehearsal")
            expected[schema + "." + table] = count
import yaml

jobs = yaml.safe_load(Path(source_state["config"]).read_text())["jobs"]
buckets = [job["target"]["bucket"] for job in jobs]
topic = "de.restore." + nonce
servers = source_env["KAFKA_BOOTSTRAP_SERVERS"]
admin = KafkaAdminClient(bootstrap_servers=servers)
admin.create_topics([NewTopic(topic, 1, 1)])
admin.close()
producer = KafkaProducer(bootstrap_servers=servers)
clock = datetime.now(timezone.utc).replace(second=10, microsecond=0).isoformat()


def publish(index):
    value = {
        "event_id": "restore-" + str(index),
        "review_id": "restore-" + str(index),
        "product_id": "restore-fixture",
        "rating": index + 1,
        "review_text": "Explicit generated recovery fixture",
        "review_date": clock,
        "source_group": "simulation",
        "acquisition_method": "generated",
        "generator_seed": "42",
        "generator_config": '{"purpose":"checkpoint_restore"}',
    }
    producer.send(topic, json.dumps(value).encode()).get(timeout=15)
    producer.flush()


for index in (0, 1):
    publish(index)
probe = {
    "environment_file": str(source_environment),
    "warehouse_database": source_state["database"],
    "expected_tables": expected,
    "buckets": buckets,
    "stream_generation": str(work / "stream"),
    "bootstrap_servers": servers,
    "stream_topic": topic,
    "expected_stream_rows": 2,
    "evidence_dir": str(work / "before"),
}
Path(probe["stream_generation"]).mkdir()
Path(probe["evidence_dir"]).mkdir()
probe_path = work / "source-probe.json"
probe_path.write_text(json.dumps(probe))
probe_path.chmod(0o600)
dag_id = "de_full_recovery_" + nonce
dag_path = work / "recovery_probe_dag.py"
dag_path.write_text(
    """from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
import hashlib,json,os,subprocess
from datetime import datetime,timezone

def execute(phase):
    if hashlib.sha256(BaseHook.get_connection(os.environ['DE_PROBE_CONN_ID']).password.encode()).hexdigest()!=os.environ['DE_PROBE_SECRET_SHA256']:raise ValueError('Restored Fernet secret differs')
    subprocess.run([os.environ['DE_PROJECT_PYTHON'],os.environ['DE_RECOVERY_SCRIPT'],'--config',os.environ['DE_PROBE_CONFIG'],'--phase',phase],check=True)
with DAG("""
    + repr(dag_id)
    + """,start_date=datetime(2026,11,1,tzinfo=timezone.utc),schedule_interval=None,catchup=False) as dag:
    services=PythonOperator(task_id='verify_services',python_callable=execute,op_args=['services'])
    stream=PythonOperator(task_id='resume_checkpoint',python_callable=execute,op_args=['stream'])
    services >> stream
"""
)
afenv.update(
    DE_PROJECT_PYTHON=project_python,
    DE_RECOVERY_SCRIPT=str(root / "scripts/recovery_probe.py"),
    DE_PROBE_CONFIG=str(probe_path),
    DE_PROBE_SECRET_SHA256=hashlib.sha256(secret.encode()).hexdigest(),
)


def run_probe(environment, name):
    with (work / (name + ".log")).open("x") as log:
        subprocess.run(
            [airflow_python, "-m", "airflow", "dags", "test", dag_id, "--subdir", str(dag_path), "--use-executor"],
            cwd=root,
            env=environment,
            stdout=log,
            stderr=subprocess.STDOUT,
            check=True,
            timeout=300,
        )


run_probe(afenv, "source-airflow-probe")
# The actual DAG and every stream worker exited before coordinated capture.
subprocess.run(["systemctl", "--user", "stop", "sentinel-source-scheduler.service"], check=True)
values = {key: source_env["POSTGRES_" + key] for key in ("HOST", "PORT", "USER", "PASSWORD", "DB")}
components = []
targets = {}
admin_values = {
    key: values[name]
    for name, key in [("HOST", "host"), ("PORT", "port"), ("USER", "user"), ("PASSWORD", "password"), ("DB", "dbname")]
}
connection = psycopg2.connect(**admin_values)
connection.autocommit = True
try:
    for name, database, role in [
        ("warehouse", source_state["database"], "warehouse"),
        ("airflow", scheduler["metadata_database"], "orchestration"),
    ]:
        destination = "de_restored_" + name + "_" + nonce
        connection.cursor().execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(destination)))
        for prefix, db in [("DR_" + name.upper() + "_SRC_", database), ("DR_" + name.upper() + "_DST_", destination)]:
            for key, value in dict(values, DB=db).items():
                os.environ[prefix + key] = value
        components.append(
            {
                "name": name,
                "role": role,
                "kind": "postgres",
                "env_prefix": "DR_" + name.upper() + "_SRC_",
                "binaries_dir": str(args.pg_bin.resolve()),
            }
        )
        targets[name] = {"env_prefix": "DR_" + name.upper() + "_DST_", "binaries_dir": str(args.pg_bin.resolve())}
    for index, bucket in enumerate(buckets):
        name = "lake" + str(index)
        components.append({"name": name, "role": "lake", "kind": "minio", "bucket": bucket})
        targets[name] = {"bucket": "de-restored-" + nonce + "-" + str(index)}
    for name, role, path in [
        ("checkpoint", "checkpoints", work / "stream"),
        ("registry", "registry", Path(yaml.safe_load(Path(source_state["config"]).read_text())["root"])),
        (
            "capture",
            "registry",
            Path(tiki_job.get("parameters", {}).get("capture_root", root / "storage/captures/tiki")),
        ),
    ]:
        components.append({"name": name, "role": role, "kind": "directory", "path": str(path)})
        targets[name] = {"path": str(work / "restored" / name)}

    @contextmanager
    def guard():
        if (
            subprocess.run(
                ["systemctl", "--user", "is-active", "--quiet", "sentinel-source-scheduler.service"]
            ).returncode
            == 0
        ):
            raise RuntimeError("Source scheduler must be stopped")
        with maintenance_guard("integration",all_schemas=True) as state:
            state["offsets"] = kafka_drain_state(
                servers,
                [source_env["KAFKA_GROUP_ID"]],
                [
                    source_env["KAFKA_TOPIC_PRODUCTS"],
                    source_env["KAFKA_TOPIC_REVIEWS"],
                    source_env["KAFKA_TOPIC_PRICES"],
                ],
            )
            yield state

    backup = create_backup(
        work / "backups",
        components,
        guard,
        {
            "airflow_enabled": True,
            "scope": "Actual source datasets, actual LocalExecutor metadata/Fernet connection, real Kafka checkpoint, source registry and captured HTTP data; local single-host drill",
        },
    )
    started = time.monotonic()
    plan = restore_plan(backup, targets)
    (work / "restore-plan.json").write_text(json.dumps(plan, indent=2))
    assert restore_apply(backup, plan, guard)["verified"]
    assert restore_verify(backup, targets)["verified"]
    recovered = dict(
        probe,
        warehouse_database=os.environ["DR_WAREHOUSE_DST_DB"],
        buckets=[targets["lake" + str(index)]["bucket"] for index in range(len(buckets))],
        stream_generation=targets["checkpoint"]["path"],
        expected_stream_rows=3,
        evidence_dir=str(work / "after"),
    )
    Path(recovered["evidence_dir"]).mkdir()
    target_probe = work / "restored-probe.json"
    target_probe.write_text(json.dumps(recovered))
    target_probe.chmod(0o600)
    restored_env = dict(afenv, DE_PROBE_CONFIG=str(target_probe), AIRFLOW_HOME=str(work / "restored-airflow-home"))
    original_url = afenv["AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"]
    restored_env["AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"] = (
        original_url.rsplit("/", 1)[0] + "/" + os.environ["DR_AIRFLOW_DST_DB"]
    )
    publish(2)
    run_probe(restored_env, "restored-airflow-probe")
    manifest = json.loads((backup / "manifest.json").read_text())
    snapshot = datetime.fromisoformat(manifest["snapshot_at"])
    result = {
        "passed": True,
        "backup_verified": True,
        "restored_fernet_secret_verified": True,
        "actual_localexecutor_probe_runs": 2,
        "checkpoint_before_rows": 2,
        "checkpoint_after_rows": 3,
        "warehouse_expected_counts": expected,
        "full_restore_and_resume_seconds": time.monotonic() - started,
        "recovery_point_age_seconds": (datetime.now(timezone.utc) - snapshot).total_seconds(),
        "rpo_target_seconds": 86400,
        "rto_target_seconds": 3600,
        "disaster_recovery_certified": False,
        "outside_host_copy": False,
        "scope": "Actual PostgreSQL warehouse+Airflow metadata, 3 MinIO buckets, real Kafka/Spark checkpoint, registry and HTTP captures; restored LocalExecutor verifies SQL/MinIO and decrypts secret before resuming checkpoint. Simulated stream fixture is isolated from observed source data.",
    }
    assert result["full_restore_and_resume_seconds"] < 3600 and result["recovery_point_age_seconds"] < 86400
    with (work / "verification.json").open("x") as out:
        json.dump(result, out, indent=2)
    with (work / "state.json").open("x") as out:
        json.dump({"work": str(work), "report": str(work / "verification.json"), "backup": str(backup)}, out, indent=2)
    with args.report.open("x") as out:
        json.dump(result, out, indent=2)
    print(json.dumps(result, indent=2))
finally:
    connection.close()
    producer.close()
    subprocess.run(["systemctl", "--user", "start", "sentinel-source-scheduler.service"], check=True)
