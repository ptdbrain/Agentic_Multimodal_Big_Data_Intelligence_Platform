"""Create a new private Docker staging configuration; never starts collectors or overwrites files."""

import argparse
import base64
import json
import os
from pathlib import Path
import secrets
import shlex
import sys
import uuid
import yaml

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


def _write(path: Path, value: str) -> None:
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "w") as stream:
        stream.write(value)
        stream.flush()
        os.fsync(stream.fileno())


def _env(values: dict) -> str:
    return "".join(key + "=" + shlex.quote(str(value)) + "\n" for key, value in values.items())


def prepare(directory: Path) -> None:
    directory = directory.absolute()
    directory.mkdir(mode=0o700, parents=False, exist_ok=False)
    password, access, secret = secrets.token_urlsafe(36), secrets.token_hex(12), secrets.token_urlsafe(36)
    source = yaml.safe_load((ROOT / "config/source-jobs.example.yml").read_text())
    source["root"] = "/var/lib/sentinel/captures"
    datasets = []
    for job in source["jobs"]:
        key = job["id"]
        job["environment_file"] = "/private/" + key + ".env"
        job["namespace"] = "docker-" + key
        job["target"] = {"bucket": "docker-" + key, "postgres_schema": "source_" + key}
        env = {
            "RUNTIME_MODE": "integration",
            "POSTGRES_HOST": "postgres",
            "POSTGRES_PORT": 5432,
            "POSTGRES_DB": "sentinel_dw",
            "POSTGRES_USER": "sentinel",
            "POSTGRES_PASSWORD": password,
            "PGOPTIONS": "-c search_path=source_" + key,
            "MINIO_ENDPOINT": "minio:9000",
            "MINIO_ENDPOINT_DOCKER": "minio:9000",
            "MINIO_SECURE": "false",
            "MINIO_ACCESS_KEY": access,
            "MINIO_SECRET_KEY": secret,
            "DATA_LAKE_BUCKET": job["target"]["bucket"],
            "KAFKA_BOOTSTRAP_SERVERS": "kafka:9092",
            "KAFKA_GROUP_ID": "docker-" + key,
            "SPARK_MASTER": "spark://spark-master:7077",
            "SPARK_DRIVER_MEMORY": "2g",
            "SOURCE_JOB_NAMESPACE": job["namespace"],
        }
        for entity in ("products", "reviews", "prices", "events", "articles"):
            env["KAFKA_TOPIC_" + entity.upper()] = "docker-" + key + "-" + entity
        _write(directory / (key + ".env"), _env(env))
        datasets.append(
            {
                "id": key,
                "label": job.get("label", key),
                "kind": "web" if key == "web" else "commerce",
                "source_group": job["source_group"],
                "postgres_schema": job["target"]["postgres_schema"],
                "bucket": job["target"]["bucket"],
                "environment_file": job["environment_file"],
            }
        )
    _write(directory / "source-jobs.yml", yaml.safe_dump(source, sort_keys=False, allow_unicode=True))
    _write(directory / "serving-datasets.yml", yaml.safe_dump({"version": 1, "datasets": datasets}, sort_keys=False))
    generation = "docker-stream-" + uuid.uuid4().hex[:12]
    _write(
        directory / "streaming.json",
        json.dumps(
            {
                "registry": "/private/serving-datasets.yml",
                "dataset_id": "simulation",
                "generation": generation,
                "topic": "docker-simulation-reviews",
                "window": "1 minute",
                "watermark": "10 minutes",
                "checkpoint_dir": f"s3a://docker-simulation/checkpoints/{generation}",
                "output_path": f"s3a://docker-simulation/streaming/{generation}/metrics",
                "quarantine_path": f"s3a://docker-simulation/streaming/{generation}/quarantine",
            },
            indent=2,
        )
        + "\n",
    )
    _write(
        directory / "compose.env",
        _env(
            {
                "SENTINEL_PRIVATE_DIR": directory.resolve(),
                "POSTGRES_USER": "sentinel",
                "POSTGRES_DB": "sentinel_dw",
                "POSTGRES_PASSWORD": password,
                "MINIO_ACCESS_KEY": access,
                "MINIO_SECRET_KEY": secret,
                "KAFKA_CLUSTER_ID": base64.urlsafe_b64encode(secrets.token_bytes(16)).decode().rstrip("="),
                "GRAFANA_ADMIN_PASSWORD": secrets.token_urlsafe(36),
                "AIRFLOW_SQL_ALCHEMY_CONN": f"postgresql+psycopg2://sentinel:{password}@postgres:5432/sentinel_dw?options=-csearch_path%3Dairflow",
                "AIRFLOW_FERNET_KEY": base64.urlsafe_b64encode(secrets.token_bytes(32)).decode(),
            }
        ),
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--directory", type=Path, required=True)
    args = parser.parse_args()
    prepare(args.directory)
    print("Private staging configuration created. No services or collectors started.")


if __name__ == "__main__":
    main()
