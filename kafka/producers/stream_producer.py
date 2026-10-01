import sys
import time
import json
import argparse
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(REPO_ROOT))
from config.settings import settings

from typing import List, Dict, Any, Optional, Tuple

class StreamProducer:
    """Rate-controlled Kafka message producer demonstrating velocity control.
    Supports Kafka cluster mode and resilient in-memory virtual queue fallback.
    """
    
    def __init__(self, bootstrap_servers: Optional[str] = None, enable_offline_buffer: bool = True):
        self.bootstrap_servers = bootstrap_servers or settings.kafka.bootstrap_servers
        self.producer = None
        self.enable_offline_buffer = enable_offline_buffer
        self.virtual_queue: List[Dict[str, Any]] = []
        self._init_producer()

    def _init_producer(self):
        try:
            from kafka import KafkaProducer
            self.producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None,
                request_timeout_ms=1000
            )
        except Exception:
            self.producer = None

    def send_message(self, topic: str, key: Optional[str], value: dict):
        if self.producer:
            try:
                self.producer.send(topic, key=key, value=value)
                return
            except Exception:
                pass
        
        # Virtual Queue fallback
        if self.enable_offline_buffer:
            self.virtual_queue.append({"topic": topic, "key": key, "value": value, "sent_at": time.time()})

    def produce_batch(
        self,
        topic: str,
        records: list,
        rate: Optional[float] = None,
        key_field: Optional[str] = None
    ) -> Tuple[int, float]:
        target_rate = rate if rate is not None else settings.ingestion.rate_limit_rps
        interval = 1.0 / target_rate if target_rate > 0 else 0
        sent = 0
        t0 = time.time()

        for rec in records:
            if key_field:
                key = rec.get(key_field)
            else:
                key = rec.get("review_id") or rec.get("product_id") or rec.get("event_id") or rec.get("id")
            
            self.send_message(topic, key=str(key) if key else None, value=rec)
            sent += 1
            if interval > 0:
                time.sleep(interval)

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
    with open(sample_file, "r", encoding="utf-8") as f:
        records = json.load(f)[:args.count]

    producer = StreamProducer()
    producer.produce_batch(args.topic, records, rate=args.rate)
