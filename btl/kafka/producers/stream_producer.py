import sys
import time
import json
import uuid
import hashlib
import math
import argparse
from datetime import datetime, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(REPO_ROOT))
from config.settings import settings
from ingestion.identity import make_event_id

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
        self.current_batch_sent: int = 0
        self._writer_guard = None
        self._init_producer()
        if not self.enable_offline_buffer:
            from operations.backup import writer_admission
            guard=writer_admission(settings.runtime_mode)
            try:
                guard.__enter__()
            except Exception:
                if self.producer is not None:
                    self.producer.close()
                    self.producer=None
                raise
            self._writer_guard=guard

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
                acks="all",
                retries=5,
                retry_backoff_ms=200,
                max_in_flight_requests_per_connection=1
            )
        except Exception as e:
            # Raise exception if Kafka is unavailable in production
            raise RuntimeError(f"Failed to connect to Kafka at {self.bootstrap_servers}: {e}")

    def send_message(
        self,
        topic: str,
        key: Optional[str],
        value: dict,
        event_type: str = "EVENT",
        source: str = "stream_producer",
        wait_for_delivery: bool = True,
        timeout_sec: float = 5.0
    ) -> Tuple[Dict[str, Any], Any]:
        """Wraps payload in canonical envelope, sends to Kafka, and checks delivery result."""
        event_time = (
            value.get("timestamp") or 
            value.get("review_date") or 
            value.get("created_at") or 
            datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        )
        entity_key = key or value.get("product_id") or value.get("review_id") or value.get("price_id") or value.get("event_id") or value.get("id") or "entity"
        resolved_key = str(key) if key is not None else str(entity_key)
        event_id = make_event_id(event_type, source, value)

        # Record precise dispatch timestamps (monotonic clock for single-host profiling, epoch for distributed)
        t_sent_mono = time.perf_counter()
        t_sent_epoch_ms = time.time() * 1000.0

        envelope = {
            "event_id": event_id,
            "event_type": event_type,
            "schema_version": "1.0",
            "source": source,
            "event_time": str(event_time),
            "ingested_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "payload": value
        }
        
        if self.enable_offline_buffer:
            offset = len(self.virtual_queue)
            delivery_record = {
                "topic": topic,
                "key": resolved_key,
                "value": envelope,
                "partition": 0,
                "offset": offset,
                "sent_at": time.time(),
                "sent_epoch_ms": t_sent_epoch_ms,
                "sent_perf_counter": t_sent_mono,
                "status": "BUFFERED"
            }
            self.virtual_queue.append(delivery_record)
            return envelope, delivery_record

        if not self.producer:
            raise RuntimeError("Kafka producer is not initialized and offline buffer is disabled.")

        try:
            kafka_headers = [
                ("sent_perf_counter", str(t_sent_mono).encode("utf-8")),
                ("sent_epoch_ms", str(t_sent_epoch_ms).encode("utf-8")),
            ]
            future = self.producer.send(topic, key=resolved_key, value=envelope, headers=kafka_headers)
            if wait_for_delivery:
                metadata = future.get(timeout=timeout_sec)
                delivery_info = {
                    "status": "DELIVERED",
                    "topic": metadata.topic,
                    "partition": metadata.partition,
                    "offset": metadata.offset,
                    "event_id": envelope["event_id"],
                    "sent_perf_counter": t_sent_mono,
                    "sent_epoch_ms": t_sent_epoch_ms,
                }
                return envelope, delivery_info
            return envelope, future
        except Exception as e:
            raise RuntimeError(f"Delivery verification failed for Kafka topic '{topic}': {e}")

    def produce_batch(
        self,
        topic: str,
        records: list,
        rate: Optional[float] = None,
        key_field: Optional[str] = None,
        event_type: str = "EVENT",
        source: str = "stream_producer",
        timeout_sec: float = 15.0,
        max_duration_sec: Optional[float] = None,
    ) -> Tuple[int, float]:
        """Produces a batch of records to Kafka with delivery verification across all messages."""
        target_rate = rate if rate is not None else settings.ingestion.rate_limit_rps
        if max_duration_sec is not None and (not math.isfinite(max_duration_sec) or max_duration_sec <= 0):
            raise ValueError('Duration must be positive and finite')
        interval = 1.0 / target_rate if target_rate > 0 else 0
        sent = 0
        self.current_batch_sent = 0
        t0 = time.perf_counter()
        pending_futures = []

        for rec in records:
            message_started = time.perf_counter()
            if max_duration_sec is not None and message_started - t0 >= max_duration_sec:
                break
            if key_field:
                key = rec.get(key_field)
            else:
                key = rec.get("product_id") or rec.get("review_id") or rec.get("event_id") or rec.get("id") or rec.get("price_id")

            # When rate limit is enabled, wait per message; otherwise pipeline sends asynchronously then verify
            wait_now = (interval > 0) or self.enable_offline_buffer
            _, delivery_result = self.send_message(
                topic,
                key=str(key) if key is not None else None,
                value=rec,
                event_type=event_type,
                source=source,
                wait_for_delivery=wait_now,
                timeout_sec=timeout_sec
            )
            if not wait_now and hasattr(delivery_result, "get"):
                pending_futures.append(delivery_result)

            sent += 1
            self.current_batch_sent = sent
            if interval > 0:
                # Acknowledgement is part of the interval. Sleep only its
                # remainder; slow delivery never triggers a catch-up burst.
                remainder = interval - (time.perf_counter() - message_started)
                if max_duration_sec is not None:
                    remainder = min(remainder, max_duration_sec - (time.perf_counter() - t0))
                if remainder > 0:
                    time.sleep(remainder)

        # Flush and verify delivery of all asynchronously dispatched messages
        if self.producer and pending_futures:
            self.producer.flush()
            for fut in pending_futures:
                try:
                    fut.get(timeout=timeout_sec)
                except Exception as e:
                    raise RuntimeError(f"Batch delivery verification failed on topic '{topic}': {e}")

        duration = max(time.perf_counter() - t0, 0.001)
        actual_rate = sent / duration
        print(f"Produced {sent} messages to '{topic}' in {duration:.2f}s ({actual_rate:.1f} msg/s)")
        return sent, actual_rate

    def clear_virtual_queue(self):
        self.virtual_queue.clear()

    def close(self):
        if self.producer is not None:
            self.producer.flush()
            self.producer.close()
            self.producer=None
        guard=getattr(self,'_writer_guard',None)
        if guard:
            guard.__exit__(None,None,None)
            self._writer_guard=None


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
