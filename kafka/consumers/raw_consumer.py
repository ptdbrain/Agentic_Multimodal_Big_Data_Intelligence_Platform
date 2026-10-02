import sys
import time
import json
import datetime
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(REPO_ROOT))
from config.settings import settings
from kafka.dlq.dead_letter import dlq_handler

class RawConsumer:
    """Consumes raw Kafka streams and writes unaltered raw JSON batches to Bronze layer
    partitioned by year=YYYY/month=MM/day=DD/hour=HH (event-time preferred, processing-time fallback).
    """
    
    def __init__(self, bronze_dir: Optional[Path] = None, bootstrap_servers: Optional[str] = None):
        self.bronze_dir = bronze_dir or (settings.storage.local_data_dir / "bronze")
        self.bronze_dir.mkdir(parents=True, exist_ok=True)
        self.bootstrap_servers = bootstrap_servers or settings.kafka.bootstrap_servers
        self.consumer = None

    def _init_consumer(self, topics: List[str]):
        import sys
        old_path = sys.path.copy()
        sys.path = [p for p in sys.path if 'big data' not in p or p.endswith('site-packages')]
        from kafka import KafkaConsumer
        sys.path = old_path
        
        self.consumer = KafkaConsumer(
            *topics,
            bootstrap_servers=self.bootstrap_servers,
            group_id="sentinel-bronze-writer",
            enable_auto_commit=False,
            value_deserializer=lambda x: json.loads(x.decode('utf-8')) if x else None,
            key_deserializer=lambda x: x.decode('utf-8') if x else None,
            auto_offset_reset='earliest'
        )

    def _resolve_datetime(self, record: Dict[str, Any], timestamp_field: Optional[str] = None) -> datetime.datetime:
        ts_val = None
        if timestamp_field:
            ts_val = record.get(timestamp_field)
        if not ts_val:
            # First check envelope ingested_at
            ts_val = record.get("ingested_at")
            if not ts_val and "payload" in record:
                # Then check payload fields
                payload = record["payload"]
                if isinstance(payload, dict):
                    ts_val = payload.get("timestamp") or payload.get("review_date") or payload.get("created_at")
            
            # If no envelope, try raw fields for backward compatibility with tests
            if not ts_val:
                ts_val = record.get("timestamp") or record.get("review_date") or record.get("created_at")

        if ts_val:
            try:
                if isinstance(ts_val, (int, float)):
                    sec = ts_val / 1000.0 if ts_val > 1e11 else float(ts_val)
                    return datetime.datetime.fromtimestamp(sec, tz=datetime.timezone.utc)
                if isinstance(ts_val, str):
                    return datetime.datetime.fromisoformat(ts_val.replace("Z", "+00:00"))
            except Exception:
                pass
        return datetime.datetime.now(datetime.timezone.utc)

    def validate_envelope(self, payload: Any) -> Tuple[bool, List[str]]:
        if not isinstance(payload, dict):
            return False, ["Payload is not a dictionary"]
            
        errors = []
        required_fields = ["event_id", "event_type", "source", "ingested_at", "payload"]
        for field in required_fields:
            if field not in payload:
                errors.append(f"Missing required envelope field: {field}")
                
        return len(errors) == 0, errors

    def save_raw_batch(
        self,
        topic: str,
        records: List[Dict[str, Any]],
        timestamp_field: Optional[str] = None
    ) -> Path:
        if not records:
            now = datetime.datetime.now(datetime.timezone.utc)
        else:
            now = self._resolve_datetime(records[0], timestamp_field=timestamp_field)

        part_dir = (
            self.bronze_dir
            / topic
            / f"year={now.year}"
            / f"month={now.month:02d}"
            / f"day={now.day:02d}"
            / f"hour={now.hour:02d}"
        )
        part_dir.mkdir(parents=True, exist_ok=True)
        filename = f"batch_{int(time.time() * 1000)}.json"
        out_path = part_dir / filename
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(records, f, indent=2, ensure_ascii=False)
        print(f"Bronze Ingestion: Saved {len(records)} raw records to {out_path}")
        return out_path
        
    def consume_to_bronze(self, topics: List[str], batch_size: int = 100, timeout_ms: int = 1000):
        if not self.consumer:
            self._init_consumer(topics)
            
        try:
            while True:
                messages = self.consumer.poll(timeout_ms=timeout_ms, max_records=batch_size)
                if not messages:
                    continue
                    
                for tp, records in messages.items():
                    topic = tp.topic
                    valid_records = []
                    
                    for record in records:
                        value = record.value
                        is_valid, errors = self.validate_envelope(value)
                        
                        if is_valid:
                            valid_records.append(value)
                        else:
                            dlq_handler.send_to_dlq(
                                error_type="SCHEMA_VALIDATION_ERROR",
                                source_topic=topic,
                                partition=tp.partition,
                                offset=record.offset,
                                payload=value,
                                errors=errors
                            )
                            
                    if valid_records:
                        try:
                            self.save_raw_batch(topic, valid_records)
                        except Exception as e:
                            print(f"Failed to save batch for topic {topic}: {e}")
                            continue # Don't commit if write fails
                            
                    # Commit offset only after successful write
                    self.consumer.commit()
                    dlq_handler.flush()
        except KeyboardInterrupt:
            print("Stopping consumer...")
        except Exception as e:
            print(f"Consumer error: {e}")
            raise
        finally:
            if self.consumer:
                self.consumer.close()
