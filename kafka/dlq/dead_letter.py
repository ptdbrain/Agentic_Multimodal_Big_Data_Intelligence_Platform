import sys
import json
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, Any, List

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(REPO_ROOT))

from config.settings import settings

class DeadLetterQueueHandler:
    def __init__(self, bootstrap_servers=None):
        self.bootstrap_servers = bootstrap_servers or settings.kafka.bootstrap_servers
        self.topic = "dead-letter"
        self._producer = None
        self._init_attempted = False
        
    @property
    def producer(self):
        if not self._init_attempted and self._producer is None:
            self._init_attempted = True
            try:
                from kafka import KafkaProducer
                if KafkaProducer:
                    self._producer = KafkaProducer(
                        bootstrap_servers=self.bootstrap_servers,
                        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                        key_serializer=lambda k: str(k).encode('utf-8') if k else None,
                        request_timeout_ms=500,
                        max_block_ms=500,
                    )
            except Exception:
                pass
        return self._producer
            
    def send_to_dlq(self, error_type: str, source_topic: str, partition: int, offset: int, payload: Dict[str, Any], errors: List[str]):
        dlq_record = {
            "error_type": error_type,
            "source_topic": source_topic,
            "partition": partition,
            "offset": offset,
            "payload": payload,
            "errors": errors,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        
        if not self.producer:
            print(f"DLQ [{error_type}]: {errors} in {source_topic}[{partition}]@{offset}")
            return dlq_record
            
        try:
            self.producer.send(self.topic, value=dlq_record)
        except Exception as e:
            print(f"Failed to send to DLQ: {e}")
        return dlq_record
            
    def flush(self):
        if self.producer:
            self.producer.flush()

DLQHandler = DeadLetterQueueHandler
dlq_handler = DeadLetterQueueHandler()
