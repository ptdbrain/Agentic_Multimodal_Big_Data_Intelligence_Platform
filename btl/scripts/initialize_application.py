"""Initialize fresh Docker schema/buckets before app workers start; no source collection."""

import argparse
import os
from pathlib import Path
import subprocess
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--registry", type=Path, default=Path("/private/serving-datasets.yml"))
    parser.add_argument("--worker-env", type=Path)
    args = parser.parse_args()
    if os.environ.get("RUNNING_IN_DOCKER") != "true":
        parser.error("Application initialization is restricted to the isolated Docker runtime")
    if args.worker_env:
        from scripts.run_scheduled_backup import environment

        os.environ.update(environment(args.worker_env))
        from database.warehouse import WarehouseManager
        from serving.store import ServingStore

        wh = WarehouseManager(mode="integration")
        try:
            wh.initialize_schema()
            with wh.transaction():
                ServingStore(wh._conn, "bootstrap", "unverified").initialize()
        finally:
            wh.close()
        return
    from serving.registry import DatasetRegistry
    from scripts.run_scheduled_backup import environment
    import psycopg2
    from psycopg2 import sql
    from minio import Minio

    registry = DatasetRegistry.load(args.registry)
    baseline = next(iter(registry.datasets.values()))
    params = dict(baseline.connection)
    params["options"] = ""
    connection = psycopg2.connect(**params)
    try:
        with connection:
            cursor = connection.cursor()
            for schema in ["airflow"] + [binding.schema for binding in registry.datasets.values()]:
                cursor.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(schema)))
    finally:
        connection.close()
    for binding in registry.datasets.values():
        values = environment(binding.environment_file)
        subprocess.run(
            [sys.executable, str(Path(__file__).resolve()), "--worker-env", str(binding.environment_file)],
            env=dict(os.environ, **values),
            check=True,
        )
        client = Minio(
            values["MINIO_ENDPOINT"],
            access_key=values["MINIO_ACCESS_KEY"],
            secret_key=values["MINIO_SECRET_KEY"],
            secure=values.get("MINIO_SECURE") == "true",
        )
        if not client.bucket_exists(binding.bucket):
            client.make_bucket(binding.bucket)
    print("Application namespaces initialized; collectors remain paused.")


if __name__ == "__main__":
    main()
