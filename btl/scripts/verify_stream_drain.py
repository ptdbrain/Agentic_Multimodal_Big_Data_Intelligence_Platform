"""Compare quiesced Kafka broker ends, committed Spark checkpoints and the PostgreSQL serving frontier."""

import argparse
import json
import sys
from pathlib import Path
from urllib.parse import urlsplit

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


def verify_deployment(deployment: dict) -> dict:
    from kafka import KafkaConsumer
    from kafka.structs import TopicPartition
    from operations.migration import read_kafka_cluster_id
    from operations.stream_checkpoint import committed_frontiers
    from serving.lake import DatasetLake
    from serving.ownership import acquire_stream_owner
    from serving.registry import DatasetRegistry
    from serving.store import ServingStore, connect_dataset

    binding = DatasetRegistry.load(deployment["registry"]).datasets[deployment["dataset_id"]]
    lake = DatasetLake(binding)
    servers = lake.env["KAFKA_BOOTSTRAP_SERVERS"]
    generation, topic = deployment["generation"], deployment["topic"]
    conn = connect_dataset(binding)
    try:
        with acquire_stream_owner(conn, binding):
            store = ServingStore(conn, binding.id, binding.source_group)
            row = store.execute(
                "SELECT configuration,last_batch FROM stream_deployments WHERE dataset_id=? AND generation=?",
                (binding.id, generation),
            ).fetchone()
            if row is None:
                raise ValueError("Missing registered stream generation")
            config, last_batch = json.loads(row[0]), row[1]
            cluster = read_kafka_cluster_id(servers)
            if config["serving_protocol"] != 3 or config["kafka_cluster_id"] != cluster or config["topic"] != topic:
                raise ValueError("Live Kafka cluster or topic differs from registered deployment")
            path = urlsplit(config["checkpoint_dir"])
            if config["checkpoint_dir"] != deployment["checkpoint_dir"] or path.scheme != "s3a" or path.netloc != binding.bucket:
                raise ValueError("Checkpoint location differs from registered dataset")
            sinks = ["reviews_window", "reviews_quarantine"]
            if config.get("audit_path"):
                sinks.append("reviews_audit")
            consumer = KafkaConsumer(bootstrap_servers=servers, enable_auto_commit=False, allow_auto_create_topics=False)
            try:
                partitions = consumer.partitions_for_topic(topic)
                if not partitions:
                    raise ValueError("Kafka topic has no visible partitions")
                ends = consumer.end_offsets([TopicPartition(topic, number) for number in partitions])
            finally:
                consumer.close()
            frontier = {topic: {str(tp.partition): end for tp, end in ends.items()}}
            sinks_at_end = committed_frontiers(lake, path.path.strip("/"), sinks, frontier, last_batch)
            return {"dataset": binding.id, "generation": generation, "cluster_id": cluster, "sinks": sinks_at_end}
    finally:
        conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--deployment", required=True, type=Path)
    args = parser.parse_args()
    print(json.dumps(verify_deployment(json.loads(args.deployment.read_text())), sort_keys=True))
