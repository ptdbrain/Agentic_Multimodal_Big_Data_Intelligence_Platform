"""Read-only metrics for explicitly registered datasets, without default-warehouse substitution."""

import argparse
import hashlib
import os
import yaml
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import json

CONTENT_TYPE_LATEST = "text/plain; version=0.0.4; charset=utf-8"
from dashboard.db_connector import DashboardDB
from serving.registry import DatasetRegistry
from serving.lake import DatasetLake
from serving.store import instant, ServingStore


def collect_binding(binding) -> dict:
    result = {
        "up": 0,
        "records": {},
        "publication_present": None,
        "published_at": None,
        "search_pending": None,
        "alerts_firing": None,
        "lake_up": 0,
        "lake": {},
    }
    db = None
    try:
        if binding.sqlite_path is not None and not binding.sqlite_path.is_file():
            raise ConnectionError("Explicit dataset database is missing")
        scope = "real" if binding.source_group in {"marketplace", "web_content"} else binding.source_group
        db = DashboardDB(binding=binding, data_scope=scope)
        for table in ("articles",) if binding.kind == "web" else ("products", "reviews"):
            if not db._columns(table):
                raise ConnectionError("Dataset schema is not initialized")
            result["records"][table] = int(
                db.query("SELECT COUNT(*) AS n FROM " + db._table_source(table)).iloc[0]["n"]
            )
        if db._columns("serving_generation"):
            publication = db.read_table("serving_generation")
            result["publication_present"] = int(not publication.empty)
            if not publication.empty:
                result["published_at"] = instant(publication.iloc[0]["published_at"]).timestamp()
                if db._columns("search_jobs"):
                    result["search_pending"] = (
                        ServingStore(db._conn, binding.id, binding.source_group)
                        .execute(
                            "SELECT COUNT(*) FROM search_jobs WHERE dataset_id=? AND generation=? "
                            "AND state IN ('queued','building','failed')",
                            (binding.id, str(publication.iloc[0]["generation"])),
                        )
                        .fetchone()[0]
                    )
        if db._columns("serving_alerts"):
            frame = db.query(
                "SELECT COUNT(*) AS n FROM " + db._table_source("serving_alerts") + " AS scoped WHERE state='firing'"
            )
            result["alerts_firing"] = int(frame.iloc[0]["n"])
        result["up"] = 1
    except Exception:
        result.update(
            up=0, records={}, publication_present=None, published_at=None, search_pending=None, alerts_firing=None
        )
    finally:
        if db:
            db.close()
    try:
        result["lake"] = DatasetLake(binding).metrics()
        result["lake_up"] = 1
    except Exception:
        pass
    return result


SOURCE_FIELDS = (
    "source_state_up",
    "source_last_failure_timestamp_seconds",
    "source_last_processed_timestamp_seconds",
    "source_unresolved_failure",
    "source_capture_partial",
    "source_input_records",
)


def collect_source_state(binding, config: dict, *, max_runs: int = 1000) -> dict:
    """Read durable source journals so capture failures are visible before SQL publication."""
    result = dict.fromkeys(SOURCE_FIELDS)
    result["source_state_up"] = 0
    try:
        from orchestration.source_jobs import validate_config

        config = validate_config(config)
        job = next(item for item in config["jobs"] if item["id"] == binding.id)
        if (
            job["source_group"] != binding.source_group
            or job["target"]["bucket"] != binding.bucket
            or job["target"]["postgres_schema"] != binding.schema
        ):
            raise ValueError("Source journal configuration differs from serving dataset")
        expected = hashlib.sha256(json.dumps(job, sort_keys=True).encode()).hexdigest()
        root = Path(config["root"]).resolve()
        folder = root / binding.id
        if folder.is_symlink() or not folder.resolve().is_relative_to(root):
            raise ValueError("Source journal escapes configured root")
        paths = list(folder.glob("*/manifest.json"))
        if type(max_runs) is not int or not 1 <= max_runs <= 1000 or len(paths) > max_runs:
            raise ValueError("Source journal scan exceeds interactive bound")
        last_failure = last_processed = None
        latest_success = None
        for path in paths:
            if (
                path.is_symlink()
                or path.parent.is_symlink()
                or not path.resolve().is_relative_to(root)
                or path.stat().st_size > 2 * 1024 * 1024
            ):
                raise ValueError("Unsafe or oversized source journal")
            manifest = json.loads(path.read_text())
            if manifest.get("config_sha256") != expected:
                continue  # A prior configuration is not evidence for this deployment.
            if (
                manifest.get("job_id") != binding.id
                or manifest.get("namespace") != job["namespace"]
                or manifest.get("source_group") != binding.source_group
            ):
                raise ValueError("Source journal identity mismatch")
            physical = manifest.get("physical_target")
            if physical:
                target = physical.get("target", {})
                if (
                    physical.get("source_group") != binding.source_group
                    or target.get("bucket") != binding.bucket
                    or target.get("db_schema") != binding.schema
                ):
                    raise ValueError("Journal physical target differs from dataset")
            if manifest.get("failed_at"):
                failure = instant(manifest["failed_at"]).timestamp()
                last_failure = max(last_failure or failure, failure)
            if manifest.get("processing_state") == "complete" and manifest.get("processed_at"):
                processed = instant(manifest["processed_at"]).timestamp()
                if last_processed is None or processed > last_processed:
                    last_processed, latest_success = processed, manifest
        result.update(
            source_state_up=1,
            source_last_failure_timestamp_seconds=last_failure,
            source_last_processed_timestamp_seconds=last_processed,
        )
        if last_failure is not None or last_processed is not None:
            result["source_unresolved_failure"] = int(
                last_failure is not None and (last_processed is None or last_failure > last_processed)
            )
        if latest_success:
            capture_status = latest_success.get("capture_status")
            if capture_status in {"complete", "partial"}:
                result["source_capture_partial"] = int(capture_status == "partial")
            count = latest_success.get("input_records")
            if type(count) is int and count >= 0:
                result["source_input_records"] = count
    except Exception:
        pass  # Unknown/corrupt journals emit state_up=0, never a fabricated success.
    return result


def generate_metrics(registry: DatasetRegistry, source_config: dict | None = None) -> bytes:
    lines = []
    fields = {
        "up": "up",
        "publication_present": "publication_present",
        "publication_timestamp_seconds": "published_at",
        "search_pending": "search_pending",
        "alerts_firing": "alerts_firing",
        "lake_up": "lake_up",
    }
    for name in list(fields) + list(SOURCE_FIELDS) + ["records", "lake_bytes"]:
        lines.append("# TYPE sentinel_dataset_" + name + " gauge")

    def emit(name, value, labels):
        encoded = ",".join(key + "=" + json.dumps(str(value)) for key, value in sorted(labels.items()))
        lines.append("sentinel_dataset_" + name + "{" + encoded + "} " + str(float(value)))

    for binding in registry.datasets.values():
        result = collect_binding(binding)
        labels = {"dataset_id": binding.id, "source_group": binding.source_group}
        for name, field in fields.items():
            if result[field] is not None:
                emit(name, result[field], labels)
        if source_config is not None:
            for name, value in collect_source_state(binding, source_config).items():
                if value is not None:
                    emit(name, value, labels)
        for entity, count in result["records"].items():
            emit("records", count, {**labels, "entity": entity})
        for layer, stats in result["lake"].items():
            emit("lake_bytes", stats["size_bytes"], {**labels, "layer": layer})
    return ("\n".join(lines) + "\n").encode()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--registry", type=Path, required=True)
    parser.add_argument("--source-jobs", type=Path, default=os.environ.get("SOURCE_JOBS_CONFIG"))
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8003)
    args = parser.parse_args()

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path != "/metrics":
                self.send_error(404)
                return
            try:
                config = yaml.safe_load(Path(args.source_jobs).read_text()) if args.source_jobs else None
                body = generate_metrics(DatasetRegistry.load(args.registry), config)
            except Exception:
                self.send_error(503, "Dataset registry is unavailable")
                return
            self.send_response(200)
            self.send_header("Content-Type", CONTENT_TYPE_LATEST)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *_):
            pass

    ThreadingHTTPServer((args.host, args.port), Handler).serve_forever()


if __name__ == "__main__":
    main()
