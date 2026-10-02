import sys
import time
import json
import uuid
import hashlib
import argparse
from datetime import datetime, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(REPO_ROOT))
from config.settings import settings

from typing import List, Dict, Any, Optional, Tuple

class StreamProducer:
    """Rate-controlled Kafka message producer demonstrating velocity control.
    Supports real Kafka integration.
    """
    
    def __init__(self, bootstrap_servers: Optional[str] = None, enable_offline_buffer: bool = False):
        self.bootstrap_servers = bootstrap_servers or settings.kafka.bootstrap_servers
        self.enable_offline_buffer = enable_offline_buffer
        self.virtual_queue: List[Dict[str, Any]] = []
        self.producer = None
        self._init_producer()

    def _init_producer(self):
        if self.enable_offline_buffer:
            # Explicit test mode, don't require Kafka
            return
            
        try:
            from kafka import KafkaProducer
            if not KafkaProducer:
                raise ImportError("KafkaProducer not available")
            
            self.producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: str(k).encode('utf-8') if k else None,
                bootstrap_timeout_ms=1000,
                request_timeout_ms=1000,
                max_block_ms=1000,
                retries=1
            )
        except Exception as e:
            # Raise exception if Kafka is unavailable in production
            raise RuntimeError(f"Failed to connect to Kafka at {self.bootstrap_servers}: {e}")

    def send_message(self, topic: str, key: Optional[str], value: dict, event_type: str = "EVENT", source: str = "stream_producer"):
        event_time = (
            value.get("timestamp") or 
            value.get("review_date") or 
            value.get("created_at") or 
            datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        )
        entity_key = key or value.get("product_id") or value.get("review_id") or value.get("price_id") or value.get("event_id") or "entity"
        det_hash = hashlib.sha256(f"{source}|{entity_key}|{event_time}".encode("utf-8")).hexdigest()[:24]

        envelope = {
            "event_id": f"evt_{det_hash}",
            "event_type": event_type,
            "schema_version": "1.0",
            "source": source,
            "event_time": str(event_time),
            "ingested_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "payload": value
        }
        
        if self.enable_offline_buffer:
            self.virtual_queue.append({"topic": topic, "key": key, "value": envelope, "sent_at": time.time()})
            return

        if self.producer:
            try:
                self.producer.send(topic, key=key, value=envelope)
            except Exception as e:
                raise RuntimeError(f"Failed to send message to Kafka topic {topic}: {e}")
        else:
            raise RuntimeError("Kafka producer is not initialized and offline buffer is disabled")

    def produce_batch(
        self,
        topic: str,
        records: list,
        rate: Optional[float] = None,
        key_field: Optional[str] = None,
        event_type: str = "EVENT",
        source: str = "stream_producer"
    ) -> Tuple[int, float]:
        target_rate = rate if rate is not None else settings.ingestion.rate_limit_rps
        interval = 1.0 / target_rate if target_rate > 0 else 0
        sent = 0
        t0 = time.time()

        for rec in records:
            if key_field:
                key = rec.get(key_field)
            else:
                # Default logic: Use product_id for reviews and products, or other ids
                key = rec.get("product_id") or rec.get("review_id") or rec.get("event_id") or rec.get("id")
            
            self.send_message(topic, key=str(key) if key else None, value=rec, event_type=event_type, source=source)
            sent += 1
            if interval > 0:
                time.sleep(interval)

        if self.producer:
            self.producer.flush()

        duration = max(time.time() - t0, 0.001)
        actual_rate = sent / duration
        print(f"Produced {sent} messages to '{topic}' in {duration:.2f}s ({actual_rate:.1f} msg/s)")
        return sent, actual_rate

    def clear_virtual_queue(self):
        self.virtual_queue.clear()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--topic", default="raw.reviews")
    parser.add_argument("--rate", type=float, default=50.0)
    parser.add_argument("--count", type=int, default=100)
    args = parser.parse_args()

    sample_file = REPO_ROOT / "data" / "sample" / "reviews.json"
    if sample_file.exists():
        with open(sample_file, "r", encoding="utf-8") as f:
            records = json.load(f)[:args.count]
        
        producer = StreamProducer()
        producer.produce_batch(args.topic, records, rate=args.rate, event_type="NEW_REVIEW")
