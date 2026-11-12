"""Actual service/checkpoint probes used by an isolated full recovery rehearsal."""

import argparse, hashlib, json, os, shlex, sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


def load_environment(path):
    path = Path(path)
    if path.stat().st_mode & 0o077:
        raise ValueError("Private probe environment required")
    for line in path.read_text().splitlines():
        if not line.strip() or line.startswith("#"):
            continue
        key, value = line.split("=", 1)
        parts = shlex.split(value)
        os.environ[key] = parts[0] if parts else ""


def services(config):
    import psycopg2
    from psycopg2 import sql
    from minio import Minio

    results = {}
    with psycopg2.connect(
        host=os.environ["POSTGRES_HOST"],
        port=os.environ["POSTGRES_PORT"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        dbname=config["warehouse_database"],
    ) as connection:
        cursor = connection.cursor()
        for name, expected in config["expected_tables"].items():
            schema, table = name.split(".")
            cursor.execute(sql.SQL("SELECT COUNT(*) FROM {}.{}").format(sql.Identifier(schema), sql.Identifier(table)))
            actual = cursor.fetchone()[0]
            if actual != expected:
                raise ValueError("Restored warehouse count differs: " + name)
            results[name] = actual
    client = Minio(
        os.environ["MINIO_ENDPOINT"],
        access_key=os.environ["MINIO_ACCESS_KEY"],
        secret_key=os.environ["MINIO_SECRET_KEY"],
        secure=False,
    )
    objects = {}
    for bucket in config["buckets"]:
        rows = list(client.list_objects(bucket, recursive=True))
        if not rows:
            raise ValueError("Restored lake bucket is empty")
        response = client.get_object(bucket, rows[0].object_name)
        try:
            hashlib.sha256(response.read()).hexdigest()
        finally:
            response.close()
            response.release_conn()
        objects[bucket] = len(rows)
    return {"tables": results, "readable_lake_objects": objects}


def stream(config):
    from config.settings import settings
    from storage.storage_manager import storage

    generation = Path(config["stream_generation"])
    settings.set_runtime_mode("test")
    settings.database.sqlite_path = generation / "probe.sqlite"
    settings.storage.local_data_dir = generation / "local-lake"
    storage.set_mode("test", base_dir=settings.storage.local_data_dir)
    settings.kafka.bootstrap_servers = config["bootstrap_servers"]
    from spark.streaming.spark_streaming_job import SparkStreamingJob

    job = SparkStreamingJob(
        checkpoint_dir=str(generation / "checkpoint"),
        output_path=str(generation / "metrics"),
        quarantine_path=str(generation / "quarantine"),
    )
    try:
        job.start_review_stream(topics=config["stream_topic"], trigger_available_now=True)
        job.await_termination(120)
        if any(query is not None and (query.isActive or query.exception()) for query in job.queries):
            raise RuntimeError("Stream probe did not commit cleanly")
        rows = job.spark.read.parquet(str(generation / "metrics")).collect()
        count = sum(row.review_count for row in rows)
        if count != config["expected_stream_rows"]:
            raise ValueError(f"Checkpoint resume count {count} differs from expected {config['expected_stream_rows']}")
        return {"review_count": count, "source_group": sorted({row.source_group for row in rows})}
    finally:
        job.stop()
        job.spark.stop()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--phase", choices=["services", "stream"], required=True)
    args = parser.parse_args()
    config = json.loads(args.config.read_text())
    load_environment(config["environment_file"])
    result = services(config) if args.phase == "services" else stream(config)
    path = Path(config["evidence_dir"]) / (args.phase + ".json")
    with path.open("x") as stream_out:
        json.dump(result, stream_out, indent=2)
    print(json.dumps(result))


if __name__ == "__main__":
    main()
