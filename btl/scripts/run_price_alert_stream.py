"""Read an owned dataset's Kafka price topic and publish causal alert evaluations."""

import argparse
from collections import Counter
from contextlib import closing
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from operations.migration import read_kafka_cluster_id
from scripts.run_scheduled_backup import environment
from serving.admission import writer_transaction
from serving.price_stream import initialize_price_stream, stage_price_event
from serving.registry import DatasetRegistry
from serving.store import ServingStore, connect_dataset


def open_price_source(binding):
    from kafka import KafkaConsumer

    if binding.kind != "commerce" or binding.source_group not in {"marketplace", "simulation"} or binding.sqlite_path:
        raise ValueError("Choose an isolated PostgreSQL marketplace or simulation dataset")
    env = environment(binding.environment_file)
    servers = env["KAFKA_BOOTSTRAP_SERVERS"]
    topic = env["KAFKA_TOPIC_PRICES"]
    source = hashlib.sha256(json.dumps([read_kafka_cluster_id(servers), topic]).encode()).hexdigest()
    group = "price-alerts-" + hashlib.sha256((binding.id + ":" + source).encode()).hexdigest()[:32]
    return KafkaConsumer(
        topic, bootstrap_servers=servers, group_id=group, enable_auto_commit=False,
        auto_offset_reset="earliest", max_poll_records=100,
    ), source


def consume_price_batch(store, consumer, source_id, timeout_ms):
    counts = Counter()
    messages = consumer.poll(timeout_ms=timeout_ms, max_records=100)
    if not messages:
        return counts
    checked_at = datetime.now(timezone.utc)
    with writer_transaction(store.conn):
        for partition, records in messages.items():
            for record in records:
                outcome = stage_price_event(
                    store, source_id, partition.partition, record.offset, record.value, checked_at,
                )
                counts[outcome] += 1
    consumer.commit()
    return counts


def consume_prices(binding, consumer, source_id, max_batches=None):
    counts = Counter()
    with closing(connect_dataset(binding)) as connection:
        store = ServingStore(connection, binding.id, binding.source_group)
        with writer_transaction(connection):
            initialize_price_stream(store)
        batches = 0
        while max_batches is None or batches < max_batches:
            result = consume_price_batch(store, consumer, source_id, 5000)
            if not result and max_batches is not None:
                break
            if result:
                counts.update(result)
                batches += 1
    return counts


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--registry", required=True, type=Path)
    parser.add_argument("--dataset", required=True)
    parser.add_argument("--max-batches", type=int)
    args = parser.parse_args()
    if args.max_batches is not None and args.max_batches < 1:
        parser.error("--max-batches must be positive")
    binding = DatasetRegistry.load(args.registry).datasets[args.dataset]
    consumer, source = open_price_source(binding)
    with closing(consumer):
        result = consume_prices(binding, consumer, source, args.max_batches)
        print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
