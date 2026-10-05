import sys
import json
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(REPO_ROOT))

from config.settings import settings


class DeadLetterQueueHandler:
    """Durable Dead Letter Queue (DLQ) handler for schema and pipeline failures.
    Ensures malformed records are permanently recorded in Kafka 'dead-letter' topic
    or durable Bronze Data Lake storage.
    Strictly forbids swallowing errors when DLQ persistence fails.
    """

    def __init__(self, bootstrap_servers: Optional[str] = None):
        self.bootstrap_servers = bootstrap_servers or settings.kafka.bootstrap_servers
        self.topic = "dead-letter"
        self._producer = None
        self._init_attempted = False
        self.audit_records: List[Dict[str, Any]] = []
        self.simulate_failure: bool = False

    @property
    def producer(self):
        if not self._init_attempted and self._producer is None:
            self._init_attempted = True
            try:
                from kafka import KafkaProducer
                if KafkaProducer:
                    self._producer = KafkaProducer(
                        bootstrap_servers=self.bootstrap_servers,
                        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                        key_serializer=lambda k: str(k).encode("utf-8") if k else None,
                        request_timeout_ms=1000,
                        max_block_ms=1000,
                        retries=2,
                    )
            except Exception:
                self._producer = None
        return self._producer

    def send_to_dlq(
        self,
        error_type: str,
        source_topic: str,
        partition: int,
        offset: int,
        payload: Any,
        errors: List[str],
        timestamp: Optional[str] = None
    ) -> Dict[str, Any]:
        """Routes a rejected record to the durable Dead Letter Queue.
        Raises RuntimeError if DLQ persistence fails ("nếu DLQ hỏng thì không được bỏ qua lỗi").
        """
        if self.simulate_failure:
            raise RuntimeError(f"DLQ delivery failed: simulated storage failure for {source_topic}[{partition}]@{offset}")

        dlq_record = {
            "error_type": error_type,
            "source_topic": source_topic,
            "partition": int(partition),
            "offset": int(offset),
            "payload": payload,
            "errors": errors,
            "timestamp": timestamp or datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        }

        self.audit_records.append(dlq_record)
        persisted = False
        kafka_err = None

        # 1. Attempt delivery to real Kafka 'dead-letter' topic
        if self.producer:
            try:
                future = self.producer.send(self.topic, value=dlq_record)
                future.get(timeout=3.0)
                persisted = True
            except Exception as e:
                kafka_err = e
                print(f"[DLQ Warning] Kafka DLQ send failed ({e}), falling back to durable Bronze storage.")

        # 2. Durable fallback: persist to Data Lake storage (MinIO or explicit Local)
        if not persisted:
            try:
                from storage.storage_manager import storage
                storage.write_bronze_json(self.topic, [dlq_record])
                persisted = True
            except Exception as storage_err:
                # 3. Fail-loud: strictly refuse to ignore DLQ failure
                err_detail = f"Kafka error: {kafka_err}; Storage error: {storage_err}"
                raise RuntimeError(f"CRITICAL: Failed to durably store DLQ record ({err_detail}). Cannot drop error!")

        return dlq_record

    def flush(self):
        if self.producer:
            try:
                self.producer.flush()
            except Exception:
                pass

    def clear(self):
        self.audit_records.clear()


DLQHandler = DeadLetterQueueHandler
dlq_handler = DeadLetterQueueHandler()
