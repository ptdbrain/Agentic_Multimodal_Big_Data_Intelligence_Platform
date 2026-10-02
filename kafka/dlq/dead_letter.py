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
        self.producer = None
        self._init_producer()
        
    def _init_producer(self):
        try:
            import sys
            old_path = sys.path.copy()
            sys.path = [p for p in sys.path if 'big data' not in p or p.endswith('site-packages')]
            from kafka import KafkaProducer
            sys.path = old_path
            
            self.producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: str(k).encode('utf-8') if k else None,
            )
        except Exception as e:
            pass # Keep it simple, logging can be noisy
            
    def send_to_dlq(self, error_type: str, source_topic: str, partition: int, offset: int, payload: Dict[str, Any], errors: List[str]):
        if not self.producer:
            print(f"DLQ [{error_type}]: {errors} in {source_topic}[{partition}]@{offset}")
            return
            
        dlq_record = {
            "error_type": error_type,
            "source_topic": source_topic,
            "partition": partition,
            "offset": offset,
            "payload": payload,
            "errors": errors,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        
        try:
            self.producer.send(self.topic, value=dlq_record)
        except Exception as e:
            print(f"Failed to send to DLQ: {e}")
            
    def flush(self):
        if self.producer:
            self.producer.flush()

dlq_handler = DeadLetterQueueHandler()
