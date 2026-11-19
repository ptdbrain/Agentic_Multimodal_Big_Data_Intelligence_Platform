"""Fail before starting Spark if process-level writers do not match the selected dataset."""

import re
from pathlib import Path
from urllib.parse import urlsplit

from scripts.run_scheduled_backup import environment
from serving.registry import DatasetRef


def validate_stream_binding(binding: DatasetRef, env: dict[str, str], paths: dict[str, str]) -> None:
    expected = environment(binding.environment_file)
    keys = (
        ("RUNTIME_MODE", "SQLITE_DB_PATH")
        if binding.sqlite_path
        else (
            "RUNTIME_MODE",
            "POSTGRES_HOST",
            "POSTGRES_PORT",
            "POSTGRES_DB",
            "POSTGRES_USER",
            "POSTGRES_PASSWORD",
            "PGOPTIONS",
            "DATA_LAKE_BUCKET",
            "MINIO_ENDPOINT",
            "MINIO_ACCESS_KEY",
            "MINIO_SECRET_KEY",
            "KAFKA_BOOTSTRAP_SERVERS",
        )
    )
    for key in keys:
        if not expected.get(key) or env.get(key) != expected[key]:
            raise ValueError("Stream process runtime differs from private dataset; use --deployment")
    if not binding.sqlite_path:
        match = re.fullmatch(r"-c\s*search_path=([a-z_][a-z0-9_]*)", expected.get("PGOPTIONS", ""))
        if match is None or match.group(1) != binding.schema:
            raise ValueError("Stream runtime schema differs from SQL sink schema")
    if not binding.sqlite_path and expected.get("DATA_LAKE_BUCKET") != binding.bucket:
        raise ValueError("Dataset lake bucket differs from private runtime")
    for name in ("checkpoint_dir", "output_path", "quarantine_path"):
        value = paths.get(name)
        if not value:
            raise ValueError("Explicit dataset stream paths required")
        if binding.sqlite_path:
            if not expected.get("LOCAL_DATA_DIR"):
                raise ValueError("Explicit dataset local lake required")
            root = Path(expected["LOCAL_DATA_DIR"]).resolve()
            path = Path(value)
            if not path.is_absolute() or path.is_symlink() or not path.resolve().is_relative_to(root):
                raise ValueError("Stream path escapes dataset lake")
        else:
            uri = urlsplit(value)
            if uri.scheme not in ("s3", "s3a") or uri.netloc != binding.bucket or ".." in uri.path.split("/"):
                raise ValueError("Stream path escapes dataset bucket")


def stream_configuration(
    *, window_seconds: int, watermark_seconds: int, topic: str, paths: dict[str, str],
    output_mode: str, negative_threshold: float, kafka_cluster_id: str, kafka_source: dict | None = None,
    audit_path: str | None = None,
) -> dict:
    """Pin every output and checkpoint-affecting setting before constructing Spark."""
    if not kafka_cluster_id or (kafka_source and kafka_source.get("cluster_id", kafka_cluster_id) != kafka_cluster_id):
        raise ValueError("Kafka cluster identity differs from the stream source")
    return {
        "serving_protocol": 3,
        "kafka_cluster_id": kafka_cluster_id,
        "window_seconds": window_seconds,
        "watermark_seconds": watermark_seconds,
        "topic": topic,
        **{key: paths[key] for key in ("checkpoint_dir", "output_path", "quarantine_path")},
        "output_mode": output_mode,
        "negative_threshold": negative_threshold,
        "kafka_source": kafka_source,
        "audit_path": audit_path,
    }
