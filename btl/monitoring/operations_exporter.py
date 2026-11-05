"""Read operational state from its configured SQL warehouse; no local fallback."""

import argparse
import os
import re
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from operations.status import OperationStatus
from monitoring.metrics_exporter import create_metrics_server


def kafka_probes():
    """Aggregate actual ISR and explicitly configured committed consumer offsets."""
    from kafka.admin import KafkaAdminClient
    from kafka import KafkaConsumer
    from config.settings import settings

    admin = KafkaAdminClient(
        bootstrap_servers=settings.kafka.bootstrap_servers, request_timeout_ms=10000, bootstrap_timeout_ms=10000
    )
    consumer = None
    try:
        topics = [name for name in admin.list_topics() if not name.startswith("__")]
        metadata = admin.describe_topics(topics) if topics else []
        if any(topic.get("error_code", 0) for topic in metadata):
            raise RuntimeError("Kafka metadata probe failed")
        under_replicated = sum(
            len(part.get("isr", part.get("isr_nodes", []))) < len(part.get("replicas", part.get("replica_nodes", [])))
            for topic in metadata
            for part in topic["partitions"]
        )
        gauges = {"under_replicated_partitions": under_replicated}
        groups = [group.strip() for group in os.environ.get("OPS_CONSUMER_GROUPS", "").split(",") if group.strip()]
        if groups:
            if len(groups) > 100 or not all(re.fullmatch(r"[A-Za-z0-9_.-]{1,255}", group) for group in groups):
                raise ValueError("Invalid consumer group probe configuration")
            consumer = KafkaConsumer(
                bootstrap_servers=settings.kafka.bootstrap_servers, enable_auto_commit=False, request_timeout_ms=15000
            )
            total = 0
            for group in groups:
                offsets = admin.list_group_offsets(group)[group]
                if not offsets:
                    raise RuntimeError("Configured consumer group has no committed offsets")
                ends = consumer.end_offsets(list(offsets))
                total += sum(max(0, ends[partition] - offset.offset) for partition, offset in offsets.items())
            gauges["consumer_lag"] = total
        return gauges
    finally:
        if consumer:
            consumer.close()
        admin.close()


def collect(mode=None):
    with OperationStatus(mode=mode) as status:
        if os.environ.get("OPS_KAFKA_PROBE", "true").lower() == "true":
            for name, value in kafka_probes().items():
                status.set_gauge(name, value)
        # This measures the configured mounted filesystem only, not MinIO free space.
        disk = os.environ.get("OPS_DISK_PATH")
        if disk:
            stats = os.statvfs(disk)
            status.set_gauge("disk_free_ratio", stats.f_bavail / stats.f_blocks)
        return status.metrics()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", default=8002, type=int)
    parser.add_argument("--mode", choices=["test", "local", "integration", "production"], default=None)
    parser.add_argument("--serve", action="store_true")
    args = parser.parse_args()
    if args.serve:
        with create_metrics_server(args.host, args.port, metrics_provider=lambda: collect(args.mode)) as server:
            server.serve_forever()
    else:
        print(collect(args.mode), end="")


if __name__ == "__main__":
    main()
