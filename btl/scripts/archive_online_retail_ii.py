"""Publish prepared Online Retail II invoice lines through Kafka into MinIO Bronze."""

import argparse
from collections import Counter
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
import sys
import time

import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

SOURCE = "uci-online-retail-ii"
BATCH_SIZE = 10_000


def kafka_record(row):
    return {
        **row,
        "invoice_local_time": row["invoice_local_time"].isoformat(),
        "unit_price": format(row["unit_price"], "f"),
    }


def batch_fingerprints(records):
    return Counter(
        hashlib.sha256(json.dumps(row, sort_keys=True, ensure_ascii=False).encode()).hexdigest()
        for row in records
    )


def archive(directory, output, run_id):
    from kafka.admin import KafkaAdminClient, NewTopic

    from config.settings import settings
    from kafka.consumers.raw_consumer import RawConsumer
    from kafka.producers.stream_producer import StreamProducer
    from operations.backup import kafka_drain_state
    from orchestration.source_jobs import write_atomic
    from storage.minio_backend import MinIOBackend
    from storage.storage_manager import storage

    if not re.fullmatch(r"[a-z0-9][a-z0-9-]{5,32}", run_id):
        raise ValueError("Expected 6–33 lowercase letters, digits or hyphens in the run ID")
    if not settings.is_integration_mode or not isinstance(storage.backend, MinIOBackend):
        raise ValueError("Configure real Kafka, PostgreSQL, and MinIO before archiving")
    expected_bucket = f"{SOURCE}-{run_id}"
    if settings.storage.data_lake_bucket != expected_bucket:
        raise ValueError(f"Configure the isolated DATA_LAKE_BUCKET={expected_bucket}")
    if output.exists():
        raise FileExistsError(output)

    manifest = json.loads((directory / "manifest.json").read_text(encoding="utf-8"))
    path = directory / "transaction_lines.parquet"
    with path.open("rb") as stream:
        if hashlib.file_digest(stream, "sha256").hexdigest() != manifest["parquet_sha256"]:
            raise ValueError("Prepared Parquet checksum mismatch")
    parquet = pq.ParquetFile(path)
    rows = parquet.metadata.num_rows
    if (manifest["dataset"] != "Online Retail II" or manifest["license"] != "CC BY 4.0"
            or rows < 1 or rows != manifest["counts"]["distinct"]):
        raise ValueError("Prepared source manifest does not match the Parquet file")

    topic = settings.kafka.topic_events + ".retail." + run_id
    servers = settings.kafka.bootstrap_servers
    group = f"{SOURCE}-{run_id}"
    with KafkaAdminClient(bootstrap_servers=servers) as admin:
        admin.create_topics([NewTopic(topic, num_partitions=3, replication_factor=1)])

    started = time.monotonic()
    delivered = verified = 0
    objects = []
    consumer = RawConsumer(bootstrap_servers=servers, group_id=group)
    producer = StreamProducer(bootstrap_servers=servers)
    try:
        for chunk in parquet.iter_batches(batch_size=BATCH_SIZE):
            batch = [kafka_record(row) for row in chunk.to_pylist()]
            sent, _ = producer.produce_batch(
                topic, batch, rate=0, key_field="line_fingerprint",
                event_type="RETAIL_TRANSACTION_IMPORT", source=SOURCE, timeout_sec=30,
            )
            if sent != len(batch):
                raise RuntimeError("Kafka producer acknowledgement count differs from input")
            delivered += sent
            remaining = sent
            fingerprints = batch_fingerprints(batch)
            while remaining:
                result = consumer.consume_batch_once(
                    topics=[topic], max_records=remaining, timeout_ms=3000,
                )
                consumed = result["records_consumed"]
                if not consumed or result["records_dlq"] or consumed != result["records_valid"]:
                    raise RuntimeError("Kafka drain or transaction schema validation failed")
                for object_path in result["batches_written"]:
                    envelopes = storage.read_json(object_path)
                    if any(event["source"] != SOURCE or event["event_type"] != "RETAIL_TRANSACTION_IMPORT"
                           for event in envelopes):
                        raise ValueError("Unexpected source or event type in Bronze")
                    payload = [event["payload"] for event in envelopes]
                    for fingerprint, count in batch_fingerprints(payload).items():
                        if fingerprints[fingerprint] < count:
                            raise ValueError("Unexpected or repeated payload in Bronze")
                        fingerprints[fingerprint] -= count
                    verified += len(payload)
                    objects.append({"path": object_path, "rows": len(payload)})
                remaining -= consumed
            if any(fingerprints.values()):
                raise ValueError("Missing Bronze rows")
            if delivered % 100_000 == 0 or delivered == rows:
                print(f"Kafka acknowledgements {delivered}/{rows}, verified Bronze rows {verified}", flush=True)
    finally:
        producer.close()
        consumer.close()

    offsets = kafka_drain_state(servers, [group], [topic])
    if (delivered != rows or verified != rows or sum(item["end"] for item in offsets.values()) != rows
            or len(objects) != len({item["path"] for item in objects})):
        raise ValueError("Parquet, Kafka offsets and Bronze did not reconcile")
    write_atomic(output, {
        "run_id": run_id,
        "checked_at_utc": datetime.now(timezone.utc).isoformat(),
        "source": manifest,
        "kafka_acks": delivered,
        "verified_bronze_rows": verified,
        "bronze_objects": objects,
        "kafka_offsets": offsets,
        "bucket": expected_bucket,
        "elapsed_seconds": round(time.monotonic() - started, 3),
        "provenance": "Envelope event_time is the import clock. invoice_local_time retains the original clock.",
    })


if __name__ == "__main__":
    cli = argparse.ArgumentParser(description=__doc__)
    cli.add_argument("prepared_directory", type=Path)
    cli.add_argument("--run-id", required=True)
    cli.add_argument("--output", required=True, type=Path)
    args = cli.parse_args()
    archive(args.prepared_directory, args.output, args.run_id)
