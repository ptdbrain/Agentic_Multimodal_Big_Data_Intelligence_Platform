"""SentinelAI Dead-Letter Queue (DLQ) Replay Utility.
Inspects, validates, and replays failed messages from DLQ or Quarantine storage back to main Kafka topics.
(Plancheck WS2 - Item 165)
"""
import sys
import json
import argparse
from pathlib import Path
from typing import Dict, Any, List

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from config.settings import settings
from data.schemas.validator import SchemaValidator
from kafka.producers.stream_producer import StreamProducer
from storage.storage_manager import storage

def replay_dlq_messages(
    max_records: int = 100,
    dry_run: bool = False
) -> Dict[str, Any]:
    """Replays valid records from Quarantine or DLQ back into main ingestion pipeline."""
    print("=" * 60)
    print("      SENTINELAI DEAD-LETTER QUEUE (DLQ) REPLAY ENGINE     ")
    print("=" * 60)

    # 1. Inspect Quarantine objects in storage
    quarantine_files = storage.list_objects("quarantine")
    print(f"Discovered {len(quarantine_files)} quarantine object(s) in Data Lake.")

    total_inspected = 0
    replayed = 0
    failed = 0

    producer = StreamProducer(enable_offline_buffer=True)

    for qf in quarantine_files:
        if not qf.endswith(".json"):
            continue
        try:
            records = storage.read_json(qf)
            if not isinstance(records, list):
                records = [records]
            
            for item in records:
                if total_inspected >= max_records:
                    break
                total_inspected += 1

                # Determine schema type from payload
                schema_name = "review" if "review_id" in item else ("product" if "product_id" in item and "price" in item else "price")
                is_valid, errors = SchemaValidator.validate_record(item, schema_name)

                target_topic = (
                    settings.kafka.topic_reviews if schema_name == "review" else
                    (settings.kafka.topic_products if schema_name == "product" else settings.kafka.topic_prices)
                )

                if is_valid:
                    if not dry_run:
                        producer.send_message(
                            topic=target_topic,
                            key=item.get("product_id") or item.get("review_id"),
                            value=item,
                            event_type=f"REPLAY_{schema_name.upper()}",
                            source="dlq_replay"
                        )
                    replayed += 1
                else:
                    failed += 1

        except Exception as e:
            print(f"Warning reading quarantine file {qf}: {e}")

    print("\n--- Replay Summary ---")
    print(f"Total Inspected:    {total_inspected}")
    print(f"Successfully Replayed: {replayed} (Dry Run: {dry_run})")
    print(f"Still Invalid:       {failed}")
    print("=" * 60)

    return {
        "inspected": total_inspected,
        "replayed": replayed,
        "failed": failed,
        "dry_run": dry_run
    }

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="SentinelAI DLQ Replay Engine")
    parser.add_argument("--max", type=int, default=100, help="Max records to replay")
    parser.add_argument("--dry-run", action="store_true", help="Simulate replay without publishing")
    args = parser.parse_args()

    replay_dlq_messages(max_records=args.max, dry_run=args.dry_run)
