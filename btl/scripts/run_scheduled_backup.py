"""Pause the owned scheduler, fence all source namespaces, and save a recovery bundle."""

import argparse, json, os, shlex, subprocess, sys, time
from contextlib import ExitStack, contextmanager
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


def environment(path):
    path = Path(path)
    if path.stat().st_mode & 0o077:
        raise ValueError("Private source environment required")
    result = {}
    for line in path.read_text().splitlines():
        if line.strip() and not line.startswith("#"):
            key, value = line.split("=", 1)
            parts = shlex.split(value)
            result[key] = parts[0] if parts else ""
    return result


def validate_scope(config, baseline, environments):
    if config.get("active_streaming_queries"):
        raise ValueError(
            "Daily batch bundle does not cover active streaming queries; configure their fenced checkpoint components first"
        )
    for environment in environments:
        for key in (
            "POSTGRES_HOST",
            "POSTGRES_PORT",
            "POSTGRES_DB",
            "MINIO_ENDPOINT",
            "MINIO_SECURE",
            "KAFKA_BOOTSTRAP_SERVERS",
        ):
            default = "false" if key == "MINIO_SECURE" else None
            if environment.get(key, default) != baseline.get(key, default):
                raise ValueError("Declare a separate fenced component before backing up another " + key)


def drain_scopes(jobs, environments):
    """Read dynamic article topic ownership only AFTER admission is fenced."""
    import psycopg2
    from psycopg2 import sql

    scopes = []
    for job, env in zip(jobs, environments):
        if job["adapter"] == "web":
            with psycopg2.connect(
                host=env["POSTGRES_HOST"],
                port=env["POSTGRES_PORT"],
                user=env["POSTGRES_USER"],
                password=env["POSTGRES_PASSWORD"],
                dbname=env["POSTGRES_DB"],
            ) as connection:
                cur = connection.cursor()
                cur.execute(
                    sql.SQL("SELECT payload_json FROM {}.ops_topic_registry").format(
                        sql.Identifier(job["target"]["postgres_schema"])
                    )
                )
                for row in cur.fetchall():
                    value = json.loads(row[0])
                    if value["state"] != "deleted":
                        scopes.append({"groups": [value["group"]], "topics": [value["topic"]]})
        else:
            scopes.append(
                {
                    "groups": [env["KAFKA_GROUP_ID"]],
                    "topics": [env["KAFKA_TOPIC_" + name] for name in ("PRODUCTS", "REVIEWS", "PRICES")],
                }
            )
    return scopes


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    args = parser.parse_args()
    if args.config.stat().st_mode & 0o077:
        raise ValueError("Private backup configuration required")
    config = json.loads(args.config.read_text())
    import yaml

    sources = yaml.safe_load(Path(config["source_jobs"]).read_text())
    jobs = sources["jobs"]
    tiki = next(job for job in jobs if job["adapter"] == "tiki")
    env = environment(tiki["environment_file"])
    os.environ.update(env)
    from operations.backup import create_backup, verify_backup, maintenance_guard, kafka_drain_state
    from operations.status import OperationStatus
    from orchestration.source_jobs import write_atomic
    from ingestion.daily_capture import _locked
    from storage.write_lock import write_lock

    components = [
        {
            "name": "warehouse",
            "role": "warehouse",
            "kind": "postgres",
            "env_prefix": "POSTGRES_",
            "binaries_dir": config["pg_bin"],
        },
        {
            "name": "airflow",
            "role": "orchestration",
            "kind": "postgres",
            "env_prefix": "BKAIRFLOW_",
            "binaries_dir": config["pg_bin"],
        },
    ]
    for key in ["HOST", "PORT", "USER", "PASSWORD"]:
        os.environ["BKAIRFLOW_" + key] = env["POSTGRES_" + key]
    os.environ["BKAIRFLOW_DB"] = config["airflow_metadata_database"]
    root = Path(sources["root"])
    capture_root = Path(tiki.get("parameters", {}).get("capture_root", ROOT / "storage/captures/tiki"))
    registry = Path(config["checkpoint_inventory"])
    registry.mkdir(parents=True, exist_ok=True)
    environments = [environment(job["environment_file"]) for job in jobs]
    validate_scope(config, env, environments)
    checkpoint_refs = []
    for job in jobs:
        components.append(
            {"name": "lake_" + job["id"], "role": "lake", "kind": "minio", "bucket": job["target"]["bucket"]}
        )
        checkpoint_refs.append(
            {
                "source_job": job["id"],
                "bucket": job["target"]["bucket"],
                "batch_run_manifest_prefix": "runs/",
                "article_run_prefix": "article_runs/",
            }
        )
    components.extend(
        [
            {"name": "checkpoint_inventory", "role": "checkpoints", "kind": "directory", "path": str(registry)},
            {"name": "source_registry", "role": "registry", "kind": "directory", "path": str(root)},
            {"name": "tiki_capture", "role": "registry", "kind": "directory", "path": str(capture_root)},
        ]
    )
    active = (
        subprocess.run(["systemctl", "--user", "is-active", "--quiet", "sentinel-source-scheduler.service"]).returncode
        == 0
    )
    if active:
        subprocess.run(["systemctl", "--user", "stop", "sentinel-source-scheduler.service"], check=True)
    try:

        @contextmanager
        def guard():
            if (
                subprocess.run(
                    ["systemctl", "--user", "is-active", "--quiet", "sentinel-source-scheduler.service"]
                ).returncode
                == 0
            ):
                raise RuntimeError("Scheduler still running")
            with ExitStack() as stack:
                stack.enter_context(_locked(capture_root))
                for job in jobs:
                    if job["adapter"] == "web":
                        stack.enter_context(write_lock(root / job["id"] / "capture-state"))
                state = stack.enter_context(maintenance_guard("integration", all_schemas=True))
                state["offsets"] = {}
                for scope in drain_scopes(jobs, environments):
                    state["offsets"].update(
                        kafka_drain_state(env["KAFKA_BOOTSTRAP_SERVERS"], scope["groups"], scope["topics"])
                    )
                write_atomic(
                    registry / "batch-checkpoints.json",
                    {
                        "batch_checkpoints": checkpoint_refs,
                        "active_streaming_queries": config.get("active_streaming_queries", []),
                    },
                )
                yield state

        backup = create_backup(
            config["backup_target"],
            components,
            guard,
            {
                "airflow_enabled": True,
                "scope": "Configured source warehouses, Airflow metadata, all source lake buckets, batch checkpoint references, registry and HTTP captures",
                "secret_key_reference": config["secret_key_reference"],
                "offhost_copy": False,
            },
        )
        manifest = verify_backup(backup)
        with OperationStatus(mode="integration") as status:
            status.set_gauge("backup_last_complete_timestamp", time.time())
        write_atomic(
            Path(config["backup_target"]) / "last-complete.json",
            {
                "backup": str(backup),
                "snapshot_at": manifest["snapshot_at"],
                "verified": True,
                "disaster_recovery_certified": False,
            },
        )
        print(json.dumps({"backup": str(backup), "verified": True, "snapshot_at": manifest["snapshot_at"]}))
    finally:
        if active:
            subprocess.run(["systemctl", "--user", "start", "sentinel-source-scheduler.service"], check=True)


if __name__ == "__main__":
    main()
