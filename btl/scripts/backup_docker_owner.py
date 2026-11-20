"""Back up the active Docker source scheduler, warehouse, lake, stream and delivery journal."""

import argparse
from contextlib import closing, contextmanager
import fcntl
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import subprocess
import sys
import tempfile
from urllib.parse import unquote, urlsplit

import psycopg2
import yaml

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from operations.backup import create_backup, maintenance_guard, verify_backup
from orchestration.source_jobs import validate_config, write_atomic
from scripts.run_scheduled_backup import environment, validate_scope

WRITERS = ("airflow", "streaming", "alert-worker", "search-worker", "receiver", "operations")
APP_STATE = PurePosixPath("/var/lib/sentinel")


def capture_tree(root: str) -> PurePosixPath:
    path = PurePosixPath(root)
    if not path.is_relative_to(APP_STATE) or ".." in path.parts or path.name != "captures":
        raise ValueError("Expected a capture directory under /var/lib/sentinel")
    if path.parent.parent == APP_STATE / "live":
        return path.parent
    return path


def validate_targets(baseline: dict, jobs: list[dict], environments: list[dict]) -> None:
    validate_scope({}, baseline, environments)
    for job, env in zip(jobs, environments, strict=True):
        if (env.get("DATA_LAKE_BUCKET") != job["target"]["bucket"]
                or env.get("SOURCE_JOB_NAMESPACE") != job["namespace"]
                or env.get("PGOPTIONS") != "-c search_path=" + job["target"]["postgres_schema"]):
            raise ValueError("A source environment differs from its registered backup target")


def command(*args: str, input_text: str | None = None) -> str:
    return subprocess.run(args, input=input_text, text=True, stdout=subprocess.PIPE, check=True).stdout.strip()


def container_state(container: str, field: str) -> str:
    return command("docker", "inspect", "--format", field, container)


def docker_backup(args: argparse.Namespace) -> Path | None:
    cfg = environment(args.compose_env)
    private = Path(cfg["SENTINEL_PRIVATE_DIR"])
    jobs_file = private / "source-jobs.yml"
    config = validate_config(yaml.safe_load(jobs_file.read_text()))
    journal = str(capture_tree(config["root"]))
    if any(job["owner"] != "airflow" for job in config["jobs"]):
        raise ValueError("A separate fence is required for non-Airflow sources")
    dags = ["sentinel_source_" + job["id"] for job in config["jobs"]]
    compose = ["docker", "compose", "--env-file", str(args.compose_env), "-p", args.project]
    services = {}
    for service in (*WRITERS, "postgres", "datasets", "minio"):
        ids = command(*compose, "ps", "--all", "-q", service).splitlines()
        if len(ids) != 1:
            raise ValueError(f"Expected one Docker container for {service}")
        services[service] = ids[0]

    def ip(service: str) -> str:
        networks = json.loads(container_state(services[service], "{{json .NetworkSettings.Networks}}"))
        if len(networks) != 1:
            raise ValueError("Expected the isolated application network")
        return next(iter(networks.values()))["IPAddress"]

    def execute(code: str, *values: str) -> str:
        return command("docker", "exec", "-i", services["datasets"], "/opt/project-venv/bin/python", "-", *values, input_text=code)

    def copy(source: str, target: Path) -> None:
        command("docker", "cp", services["datasets"] + ":" + source, str(target))

    baseline = json.loads(execute(
        "import json,os; print(json.dumps({k:os.environ.get(k) for k in "
        "['POSTGRES_HOST','POSTGRES_PORT','POSTGRES_DB','MINIO_ENDPOINT','KAFKA_BOOTSTRAP_SERVERS']},sort_keys=True))"
    ))
    if baseline["POSTGRES_DB"] != cfg["POSTGRES_DB"] or baseline["POSTGRES_HOST"] != "postgres" or baseline["MINIO_ENDPOINT"] != "minio:9000":
        raise ValueError("Running application uses different storage services than the Compose recovery target")
    environments = []
    for job in config["jobs"]:
        file = PurePosixPath(job["environment_file"])
        if file.parent != PurePosixPath("/private"):
            raise ValueError("Expected a job environment in the private mount")
        environments.append(environment(private / file.name))
    validate_targets(baseline, config["jobs"], environments)

    pg_host = ip("postgres")
    metadata_db = unquote(urlsplit(cfg["AIRFLOW_SQL_ALCHEMY_CONN"]).path.lstrip("/"))
    for prefix, database in [("POSTGRES_", cfg["POSTGRES_DB"]), ("BKAIRFLOW_", metadata_db)]:
        for field, value in {"HOST": pg_host, "PORT": "5432", "USER": cfg["POSTGRES_USER"],
                             "PASSWORD": cfg["POSTGRES_PASSWORD"], "DB": database}.items():
            os.environ[prefix + field] = value
    os.environ.update(RUNTIME_MODE="integration", MINIO_ENDPOINT=ip("minio") + ":9000", MINIO_SECURE="false",
                      MINIO_ACCESS_KEY=cfg["MINIO_ACCESS_KEY"], MINIO_SECRET_KEY=cfg["MINIO_SECRET_KEY"])
    os.environ.pop("PGOPTIONS", None)

    def metadata():
        return psycopg2.connect(host=pg_host, port=5432, dbname=metadata_db, user=cfg["POSTGRES_USER"],
                                password=cfg["POSTGRES_PASSWORD"])

    def require_idle(cursor) -> None:
        cursor.execute("SELECT count(*) FROM dag_run WHERE dag_id=ANY(%s) AND state IN ('queued','running')", (dags,))
        if cursor.fetchone()[0]:
            raise RuntimeError("An Airflow run is active; retry after it finishes")

    target = args.target.resolve()
    target.mkdir(parents=True, mode=0o700, exist_ok=True)
    digest = hashlib.sha256(jobs_file.read_bytes()).hexdigest()
    if command("docker", "exec", services["datasets"], "sha256sum", "/private/source-jobs.yml").split()[0] != digest:
        raise ValueError("Mounted source registry differs from host configuration")
    identity = {"project": args.project, "containers": services, "source_config_sha256": digest,
                "warehouse": cfg["POSTGRES_DB"], "metadata": metadata_db}
    pending = target / ".owner-maintenance.json"

    def resume() -> None:
        state = json.loads(pending.read_text())
        if state["identity"] != identity or set(state["paused"]) != set(dags):
            raise ValueError("Saved owner identity changed; inspect before restoring admission")
        command("docker", "start", *(services[s] for s in WRITERS))
        with closing(metadata()) as connection, connection:
            cursor = connection.cursor()
            for dag, paused in state["paused"].items():
                cursor.execute("UPDATE dag SET is_paused=%s WHERE dag_id=%s", (paused, dag))
                if cursor.rowcount != 1:
                    raise RuntimeError("Missing Airflow DAG during owner recovery")
        pending.unlink()

    with (target / ".owner-backup.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        if pending.exists():
            resume()
        if args.recover_only:
            return None
        if not all(container_state(services[s], "{{.State.Running}}") == "true" for s in WRITERS):
            raise RuntimeError("Expected all configured Docker writers to be running before scheduled backup")
        try:
            with closing(metadata()) as connection, connection:
                cursor = connection.cursor()
                cursor.execute("SELECT dag_id,is_paused FROM dag WHERE dag_id=ANY(%s) FOR UPDATE", (dags,))
                paused = dict(cursor.fetchall())
                if set(paused) != set(dags):
                    raise ValueError("The restored scheduler does not own all configured DAGs")
                require_idle(cursor)
                write_atomic(pending, {"identity": identity, "paused": paused})
                cursor.execute("UPDATE dag SET is_paused=true WHERE dag_id=ANY(%s)", (dags,))
            with closing(metadata()) as connection:
                require_idle(connection.cursor())
            command("docker", "stop", *(services[s] for s in WRITERS))
            with tempfile.TemporaryDirectory(dir=target, prefix=".source-snapshot-") as tmp:
                stage = Path(tmp)
                registry = stage / "registry"
                registry.mkdir()
                checkpoints = stage / "checkpoints"
                checkpoints.mkdir()

                @contextmanager
                def guarded():
                    if any(container_state(services[s], "{{.State.Running}}") == "true" for s in WRITERS):
                        raise RuntimeError("Docker writers are still active")
                    with closing(metadata()) as connection:
                        require_idle(connection.cursor())
                    with maintenance_guard("integration", all_schemas=True) as fence:
                        evidence = execute(
                            "import json,yaml\nfrom pathlib import Path\n"
                            "from scripts.run_scheduled_backup import environment,drain_scopes\n"
                            "from operations.backup import kafka_drain_state\n"
                            "from scripts.verify_stream_drain import verify_deployment\n"
                            "jobs=yaml.safe_load(Path('/private/source-jobs.yml').read_text())['jobs']\n"
                            "envs=[environment(job['environment_file']) for job in jobs]\noffsets={}\n"
                            "for scope in drain_scopes(jobs,envs):\n"
                            "    offsets.update(kafka_drain_state(envs[0]['KAFKA_BOOTSTRAP_SERVERS'],scope['groups'],scope['topics']))\n"
                            "stream=verify_deployment(json.loads(Path('/private/streaming.json').read_text()))\n"
                            "print(json.dumps({'stream':stream,'offsets':offsets}))"
                        )
                        fence.update(json.loads(evidence))
                        write_atomic(checkpoints / "stream.json", fence["stream"])
                        for file in ("source-jobs.yml", "serving-datasets.yml", "streaming.json"):
                            copy("/private/" + file, registry / file)
                        copy(str(APP_STATE / "notifications.jsonl"), registry / "notifications.jsonl")
                        copy(journal, stage / "current-owner")
                        yield fence

                components = [
                    {"name": name, "kind": "postgres", "role": role, "env_prefix": prefix, "binaries_dir": str(args.pg_bin)}
                    for name, role, prefix in [("warehouse", "warehouse", "POSTGRES_"), ("airflow", "orchestration", "BKAIRFLOW_")]
                ]
                components.extend({"name": "lake_" + job["id"], "kind": "minio", "role": "lake", "bucket": job["target"]["bucket"]}
                                  for job in config["jobs"])
                components.extend({"name": name, "kind": "directory", "role": role, "path": str(stage / name)}
                                  for name, role in [("registry", "registry"), ("current-owner", "registry"), ("checkpoints", "checkpoints")])
                backup = create_backup(target, components, guarded, {"airflow_enabled": True, "prior_dag_paused": paused,
                                                                     "offhost_copy": False, "project": args.project})
                manifest = verify_backup(backup)
                write_atomic(target / "last-complete.json", {"backup": str(backup), "verified": True,
                                                           "snapshot_at": manifest["snapshot_at"]})
        finally:
            if pending.exists():
                resume()
        execute("import os,sys\nfrom datetime import datetime\nfrom scripts.run_scheduled_backup import environment\n"
                "os.environ.update(environment('/private/simulation.env'))\nfrom operations.status import OperationStatus\n"
                "with OperationStatus(mode='integration') as status:\n"
                "    status.set_gauge('backup_last_complete_timestamp',datetime.fromisoformat(sys.argv[1]).timestamp())",
                manifest["snapshot_at"])
        return backup


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--compose-env", type=Path, required=True)
    parser.add_argument("--project", required=True)
    parser.add_argument("--target", type=Path, required=True)
    parser.add_argument("--pg-bin", type=Path, required=True)
    parser.add_argument("--recover-only", action="store_true", help="Restore interrupted maintenance admission and exit")
    result = docker_backup(parser.parse_args())
    if result is not None:
        print(result)
