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
    
    def __init__(
        self,
        bronze_dir: Optional[Path] = None,
        bootstrap_servers: Optional[str] = None,
        group_id: str = "sentinel-bronze-writer",
        auto_offset_reset: str = "earliest",
    ):
        self._custom_bronze_dir = Path(bronze_dir) if bronze_dir is not None else None
        self.bronze_dir = self._custom_bronze_dir or (settings.storage.local_data_dir / "bronze")
        if self._custom_bronze_dir is not None:
            self.bronze_dir.mkdir(parents=True, exist_ok=True)
        self.bootstrap_servers = bootstrap_servers or settings.kafka.bootstrap_servers
        self.group_id = group_id
        self.auto_offset_reset = auto_offset_reset
        self.consumer = None

    def _init_consumer(self, topics: List[str]):
        from kafka import KafkaConsumer
        if not KafkaConsumer:
            raise ImportError("KafkaConsumer not available")
        
        self.consumer = KafkaConsumer(
            *topics,
            bootstrap_servers=self.bootstrap_servers,
            group_id=self.group_id,
            enable_auto_commit=False,
            max_poll_records=5000,
            value_deserializer=lambda x: json.loads(x.decode('utf-8')) if x else None,
            key_deserializer=lambda x: x.decode('utf-8') if x else None,
            auto_offset_reset=self.auto_offset_reset,
        )
        try:
            self.consumer.topics()
        except Exception:
            pass

    def close(self):
        if self.consumer is not None:
            try:
                self.consumer.close()
            except Exception:
                pass
            self.consumer = None

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

    SUPPORTED_SCHEMA_VERSIONS = {"1.0"}

    def validate_envelope(self, payload: Any) -> Tuple[bool, List[str]]:
        if not isinstance(payload, dict):
            return False, ["Payload is not a dictionary"]
            
        errors = []
        required_fields = ["event_id", "event_type", "source", "ingested_at", "payload"]
        for field in required_fields:
            if field not in payload or payload[field] is None:
                errors.append(f"Missing required envelope field: {field}")

        for str_field in ("event_id", "event_type", "source", "ingested_at"):
            if str_field in payload and payload[str_field] is not None and not isinstance(payload[str_field], str):
                errors.append(f"Envelope field '{str_field}' must be string, got {type(payload[str_field]).__name__}")

        if "payload" in payload and payload["payload"] is not None and not isinstance(payload["payload"], dict):
            errors.append(f"Envelope 'payload' must be a dict, got {type(payload['payload']).__name__}")

        if "schema_version" in payload:
            ver = payload.get("schema_version")
            if not isinstance(ver, str) or ver not in self.SUPPORTED_SCHEMA_VERSIONS:
                errors.append(f"Unsupported envelope schema_version: '{ver}'")
                
        return len(errors) == 0, errors

    def _resolve_schema_name(self, topic: str) -> Optional[str]:
        """Maps Kafka topic to corresponding data contract JSON schema."""
        topic_lower = topic.lower()
        if topic_lower == settings.kafka.topic_products.lower() or "product" in topic_lower:
            return "product"
        if topic_lower == settings.kafka.topic_reviews.lower() or "review" in topic_lower:
            return "review"
        if topic_lower == settings.kafka.topic_prices.lower() or "price" in topic_lower:
            return "price"
        if topic_lower == settings.kafka.topic_events.lower() or "event" in topic_lower:
            return "event"
        return None

    def validate_message(self, topic: str, message: Any) -> Tuple[bool, List[str], Optional[Dict[str, Any]]]:
        """Validates envelope structure and payload contract.
        Returns (is_valid, list_of_errors, canonical_envelope).
        """
        is_env_valid, env_errors = self.validate_envelope(message)
        if not is_env_valid:
            # Backward-compatible fallback: wrap flat legacy records
            if isinstance(message, dict) and any(k in message for k in ("product_id", "review_id", "price_id")):
                envelope = {
                    "event_id": f"evt_legacy_{abs(hash(str(message)))}",
                    "event_type": "LEGACY",
                    "schema_version": "1.0",
                    "source": "legacy_stream",
                    "event_time": datetime.datetime.now(datetime.timezone.utc).isoformat().replace("+00:00", "Z"),
                    "ingested_at": datetime.datetime.now(datetime.timezone.utc).isoformat().replace("+00:00", "Z"),
                    "payload": message,
                }
            else:
                return False, env_errors, None
        else:
            envelope = message

        payload = envelope.get("payload")
        schema_name = self._resolve_schema_name(topic)
        if schema_name and isinstance(payload, dict):
            from data.schemas.validator import SchemaValidator
            try:
                is_schema_valid, schema_errors = SchemaValidator.validate_record(payload, schema_name)
                if not is_schema_valid:
                    return False, [f"Contract violation for '{schema_name}': {e}" for e in schema_errors], envelope
            except Exception as e:
                return False, [f"Schema validation error: {e}"], envelope

        return True, [], envelope

    @staticmethod
    def _extract_send_time(record_item: Any, canonical_env: Optional[Dict[str, Any]] = None) -> Tuple[Optional[float], Optional[float]]:
        """Extracts (sent_perf_counter, sent_epoch_ms) from envelope, record item, or Kafka headers."""
        sent_perf = None
        sent_epoch_ms = None

        if isinstance(canonical_env, dict):
            sent_perf = canonical_env.get("sent_perf_counter")
            sent_epoch_ms = canonical_env.get("sent_epoch_ms")

        if sent_perf is None and isinstance(record_item, dict):
            sent_perf = record_item.get("sent_perf_counter")
            sent_epoch_ms = record_item.get("sent_epoch_ms") or (record_item.get("sent_at", 0) * 1000.0 if record_item.get("sent_at") else None)
            if sent_perf is None and "value" in record_item and isinstance(record_item["value"], dict):
                sent_perf = record_item["value"].get("sent_perf_counter")
                sent_epoch_ms = record_item["value"].get("sent_epoch_ms")
            if sent_perf is None and "payload" in record_item and isinstance(record_item["payload"], dict):
                sent_perf = record_item["payload"].get("sent_perf_counter")
                sent_epoch_ms = record_item["payload"].get("sent_epoch_ms")

        if hasattr(record_item, "headers") and record_item.headers:
            for k, v in record_item.headers:
                if k == "sent_perf_counter" and sent_perf is None:
                    try:
                        sent_perf = float(v.decode("utf-8"))
                    except Exception:
                        pass
                elif k == "sent_epoch_ms" and sent_epoch_ms is None:
                    try:
                        sent_epoch_ms = float(v.decode("utf-8"))
                    except Exception:
                        pass

        if sent_epoch_ms is None and hasattr(record_item, "timestamp") and record_item.timestamp:
            sent_epoch_ms = float(record_item.timestamp)

        return sent_perf, sent_epoch_ms

    @staticmethod
    def _compute_message_latency(
        t_finish_perf: float,
        t_finish_epoch_ms: float,
        sent_perf: Optional[float],
        sent_epoch_ms: Optional[float],
    ) -> Tuple[Optional[float], bool]:
        """Calculates per-message latency in milliseconds.
        Returns:
            (latency_ms, is_measurement_error)

        Strict rule: Does NOT clamp non-positive latencies with max(0.01, ...).
        Non-positive latencies (<= 0), clock skews, or missing send timestamps
        are counted as measurement errors (is_measurement_error=True) rather
        than fabricating valid latencies.
        """
        if sent_perf is not None:
            diff = (t_finish_perf - sent_perf) * 1000.0
            if diff > 0.0:
                return round(diff, 3), False
            return None, True
        elif sent_epoch_ms is not None and sent_epoch_ms > 0:
            diff = t_finish_epoch_ms - sent_epoch_ms
            if diff > 0.0:
                return round(diff, 3), False
            return None, True
        return None, True

    @staticmethod
    def _extract_msg_id(raw_val: Any, record_item: Any = None) -> str:
        """Resolves canonical review_id, event_id, or key from message payload."""
        if isinstance(raw_val, dict):
            payload = raw_val.get("payload")
            if isinstance(payload, dict):
                if payload.get("review_id"):
                    return str(payload["review_id"])
                if payload.get("product_id"):
                    return str(payload["product_id"])
            if raw_val.get("review_id"):
                return str(raw_val["review_id"])
            if raw_val.get("product_id"):
                return str(raw_val["product_id"])
            if raw_val.get("event_id"):
                return str(raw_val["event_id"])

        if isinstance(record_item, dict):
            if record_item.get("key"):
                return str(record_item["key"])
            if "offset" in record_item:
                return f"off_{record_item['offset']}"
        elif hasattr(record_item, "key") and record_item.key:
            try:
                return record_item.key.decode("utf-8") if isinstance(record_item.key, bytes) else str(record_item.key)
            except Exception:
                pass

        return "unknown_msg"

    def save_raw_batch(
        self,
        topic: str,
        records: List[Dict[str, Any]],
        timestamp_field: Optional[str] = None
    ) -> Path:
        from storage.storage_manager import storage, StorageManager
        from storage.local_backend import LocalBackend

        if self._custom_bronze_dir is not None:
            local_mgr = StorageManager(
                backend=LocalBackend(str(self._custom_bronze_dir.parent)),
                base_dir=self._custom_bronze_dir.parent,
                mode="local"
            )
            object_path = local_mgr.write_bronze_json(topic, records, timestamp_field=timestamp_field)
            return (local_mgr.base_dir / object_path).resolve()

        # Main Path: RawConsumer -> StorageManager -> Active Backend (MinIO in integration, LocalBackend in explicit local mode)
        object_path = storage.write_bronze_json(topic, records, timestamp_field=timestamp_field)
        if isinstance(storage.backend, LocalBackend):
            return (storage.base_dir / object_path).resolve()
        return str(Path(object_path).as_posix())

    def consume_batch_once(
        self,
        topics: Optional[List[str]] = None,
        max_records: int = 100,
        timeout_ms: int = 2000,
        offline_records: Optional[List[Dict[str, Any]]] = None,
        live_stream: bool = False,
        stop_event: Optional[Any] = None,
    ) -> Dict[str, Any]:
        """Finite batch consumer polling up to max_records once.
        Validates canonical envelopes and payload schemas.
        Saves valid envelopes to MinIO Bronze.
        Routes bad records to durable DLQ (failing loud if DLQ is broken).
        Commits offsets strictly for successfully persisted partitions.
        Records individual message completion timestamp and per-message latency L_i = t_processed - t_sent.
        """
        target_topics = topics or [
            settings.kafka.topic_products,
            settings.kafka.topic_reviews,
            settings.kafka.topic_prices
        ]

        total_consumed = 0
        total_valid = 0
        total_dlq = 0
        batches_written = []
        batches_by_topic: Dict[str, List[str]] = {}
        topic_counts: Dict[str, int] = {}
        committed_offsets: Dict[str, int] = {}
        quarantined_records: List[Dict[str, Any]] = []
        validation_latencies_ms: List[float] = []
        durable_sink_latencies_ms: List[float] = []
        timing_measurement_errors: int = 0
        message_latencies: List[Dict[str, Any]] = []

        # -------------------------------------------------------------
        # 1. Real Kafka Consumer Branch
        # -------------------------------------------------------------
        if offline_records is None:
            if not self.consumer:
                self._init_consumer(target_topics)

            polled: Dict[Any, List[Any]] = {}
            fetched_total = 0
            empty_polls = 0
            deadline = time.time() + max(timeout_ms / 1000.0, 4.0)
            while fetched_total < max_records and time.time() < deadline:
                if stop_event is not None and stop_event.is_set():
                    break
                remaining = max(1, max_records - fetched_total)
                poll_timeout = min(timeout_ms, 200 if live_stream else 800)
                batch = self.consumer.poll(timeout_ms=poll_timeout, max_records=remaining)
                if batch:
                    empty_polls = 0
                    for tp, recs in batch.items():
                        polled.setdefault(tp, []).extend(recs)
                        fetched_total += len(recs)
                elif fetched_total > 0 and self.consumer.assignment() and not live_stream:
                    empty_polls += 1
                    if empty_polls >= 2:
                        break

            if not polled:
                return {
                    "records_consumed": 0,
                    "records_valid": 0,
                    "records_dlq": 0,
                    "batches_written": [],
                    "batches_by_topic": {},
                    "topic_counts": {},
                    "committed_offsets": {},
                    "dlq_records": [],
                    "latencies_ms": [],
                    "validation_latencies_ms": [],
                    "durable_sink_latencies_ms": [],
                    "timing_measurement_errors": 0,
                    "message_latencies": [],
                }

            from kafka.structs import OffsetAndMetadata

            # Group partitions by topic so each topic writes a consolidated Bronze batch
            topic_partitions: Dict[str, List[Tuple[Any, List[Any]]]] = {}
            for tp, records in polled.items():
                topic_partitions.setdefault(tp.topic, []).append((tp, records))

            for topic, tp_list in topic_partitions.items():
                valid_batch: List[Dict[str, Any]] = []
                valid_msg_entries: List[Dict[str, Any]] = []
                dlq_msg_entries: List[Dict[str, Any]] = []
                tp_offsets: Dict[Any, int] = {}
                topic_failed = False

                for tp, records in tp_list:
                    highest_offset = -1
                    for record in records:
                        total_consumed += 1
                        highest_offset = max(highest_offset, record.offset)
                        is_valid, errors, canonical_env = self.validate_message(topic, record.value)

                        # Metric 1: validation_latency_ms (send -> validation complete)
                        t_val_perf = time.perf_counter()
                        t_val_epoch_ms = time.time() * 1000.0
                        sent_perf, sent_epoch_ms = self._extract_send_time(record, canonical_env or record.value)
                        msg_id = self._extract_msg_id(canonical_env or record.value, record)

                        val_lat_ms, is_timing_err = self._compute_message_latency(
                            t_val_perf, t_val_epoch_ms, sent_perf, sent_epoch_ms
                        )
                        if is_timing_err:
                            timing_measurement_errors += 1
                        else:
                            validation_latencies_ms.append(val_lat_ms)

                        msg_entry = {
                            "id": msg_id,
                            "review_id": msg_id,
                            "topic": topic,
                            "sent_mono": sent_perf,
                            "sent_epoch_ms": sent_epoch_ms,
                            "validation_mono": t_val_perf,
                            "processed_mono": t_val_perf,
                            "validation_latency_ms": val_lat_ms,
                            "latency_ms": val_lat_ms if val_lat_ms is not None else 0.0,
                            "durable_sink_latency_ms": None,
                            "sink_mono": None,
                            "sink_includes_batch_buffering": True,
                            "is_valid": is_valid,
                        }
                        message_latencies.append(msg_entry)

                        if is_valid and canonical_env is not None:
                            valid_batch.append(canonical_env)
                            valid_msg_entries.append(msg_entry)
                            total_valid += 1
                        else:
                            # Route through durable DLQ
                            try:
                                dlq_res = dlq_handler.send_to_dlq(
                                    error_type="SCHEMA_VALIDATION_ERROR",
                                    source_topic=topic,
                                    partition=tp.partition,
                                    offset=record.offset,
                                    payload=record.value,
                                    errors=errors
                                )
                                quarantined_records.append(dlq_res)
                                dlq_msg_entries.append(msg_entry)
                                total_dlq += 1
                            except Exception as e:
                                topic_failed = True
                                print(f"[RawConsumer CRITICAL] DLQ failure on {topic}[{tp.partition}]@{record.offset}: {e}")
                                raise
                    if highest_offset >= 0:
                        tp_offsets[tp] = highest_offset

                # Metric 2: durable_sink_latency_ms (send -> Bronze batch write confirmed, includes batch buffering)
                if valid_batch and not topic_failed:
                    try:
                        written_path = self.save_raw_batch(topic, valid_batch)
                        t_sink_perf = time.perf_counter()
                        t_sink_epoch_ms = time.time() * 1000.0
                        batches_written.append(str(written_path))
                        batches_by_topic.setdefault(topic, []).append(str(written_path))
                        topic_counts[topic] = topic_counts.get(topic, 0) + len(valid_batch)

                        for m in valid_msg_entries:
                            m["sink_mono"] = t_sink_perf
                            m["durable_sink_status"] = "bronze_written"
                            s_lat, s_err = self._compute_message_latency(
                                t_sink_perf, t_sink_epoch_ms, m["sent_mono"], m["sent_epoch_ms"]
                            )
                            if s_err:
                                timing_measurement_errors += 1
                            else:
                                durable_sink_latencies_ms.append(s_lat)
                                m["durable_sink_latency_ms"] = s_lat
                    except Exception as write_err:
                        topic_failed = True
                        print(f"[RawConsumer Error] Failed to write Bronze batch for {topic}: {write_err}")
                        raise

                # Flush DLQ handler and record sink latency for quarantined records
                if dlq_msg_entries and not topic_failed:
                    try:
                        dlq_handler.flush()
                        t_dlq_perf = time.perf_counter()
                        t_dlq_epoch_ms = time.time() * 1000.0
                        for m in dlq_msg_entries:
                            m["sink_mono"] = t_dlq_perf
                            m["durable_sink_status"] = "dlq_persisted"
                            s_lat, s_err = self._compute_message_latency(
                                t_dlq_perf, t_dlq_epoch_ms, m["sent_mono"], m["sent_epoch_ms"]
                            )
                            if s_err:
                                timing_measurement_errors += 1
                            else:
                                durable_sink_latencies_ms.append(s_lat)
                                m["durable_sink_latency_ms"] = s_lat
                    except Exception as dlq_err:
                        topic_failed = True
                        print(f"[RawConsumer Error] Failed to flush DLQ for {topic}: {dlq_err}")
                        raise

                # Commit partition offsets strictly if all records in this topic succeeded
                if not topic_failed and tp_offsets:
                    for tp, high_off in tp_offsets.items():
                        commit_target = OffsetAndMetadata(high_off + 1, "", -1)
                        self.consumer.commit({tp: commit_target})
                        committed_offsets[f"{tp.topic}-{tp.partition}"] = high_off + 1

            dlq_handler.flush()

        # -------------------------------------------------------------
        # 2. Offline / Virtual Queue Branch
        # -------------------------------------------------------------
        else:
            if live_stream:
                by_topic_valid: Dict[str, List[Dict[str, Any]]] = {}
                by_topic_valid_entries: Dict[str, List[Dict[str, Any]]] = {}
                dlq_msg_entries: List[Dict[str, Any]] = []
                highest_offset_by_topic: Dict[str, int] = {}
                partition_failed_by_topic: Dict[str, bool] = {}
                consumed_idx = 0
                deadline = time.time() + max(timeout_ms / 1000.0, 4.0)

                while total_consumed < max_records and time.time() < deadline:
                    if stop_event is not None and stop_event.is_set():
                        break
                    if offline_records is not None and consumed_idx < len(offline_records):
                        item = offline_records[consumed_idx]
                        consumed_idx += 1
                        topic = item.get("topic") or target_topics[0]
                        total_consumed += 1
                        offset = item.get("offset", total_consumed - 1)
                        partition = item.get("partition", 0)
                        highest_offset_by_topic[topic] = max(highest_offset_by_topic.get(topic, -1), offset)
                        raw_val = item.get("value", item)

                        is_valid, errors, canonical_env = self.validate_message(topic, raw_val)
                        t_val_perf = time.perf_counter()
                        t_val_epoch_ms = time.time() * 1000.0
                        sent_perf, sent_epoch_ms = self._extract_send_time(item, canonical_env or raw_val)
                        msg_id = self._extract_msg_id(canonical_env or raw_val, item)

                        val_lat_ms, is_timing_err = self._compute_message_latency(
                            t_val_perf, t_val_epoch_ms, sent_perf, sent_epoch_ms
                        )
                        if is_timing_err:
                            timing_measurement_errors += 1
                        else:
                            validation_latencies_ms.append(val_lat_ms)

                        msg_entry = {
                            "id": msg_id,
                            "review_id": msg_id,
                            "topic": topic,
                            "sent_mono": sent_perf,
                            "sent_epoch_ms": sent_epoch_ms,
                            "validation_mono": t_val_perf,
                            "processed_mono": t_val_perf,
                            "validation_latency_ms": val_lat_ms,
                            "latency_ms": val_lat_ms if val_lat_ms is not None else 0.0,
                            "durable_sink_latency_ms": None,
                            "sink_mono": None,
                            "sink_includes_batch_buffering": True,
                            "is_valid": is_valid,
                        }
                        message_latencies.append(msg_entry)

                        if is_valid and canonical_env is not None:
                            by_topic_valid.setdefault(topic, []).append(canonical_env)
                            by_topic_valid_entries.setdefault(topic, []).append(msg_entry)
                            total_valid += 1
                        else:
                            try:
                                dlq_res = dlq_handler.send_to_dlq(
                                    error_type="SCHEMA_VALIDATION_ERROR",
                                    source_topic=topic,
                                    partition=partition,
                                    offset=offset,
                                    payload=raw_val,
                                    errors=errors
                                )
                                quarantined_records.append(dlq_res)
                                dlq_msg_entries.append(msg_entry)
                                total_dlq += 1
                            except Exception:
                                partition_failed_by_topic[topic] = True
                                raise
                    else:
                        time.sleep(0.0005)

                for topic, valid_batch in by_topic_valid.items():
                    if valid_batch and not partition_failed_by_topic.get(topic, False):
                        try:
                            written_path = self.save_raw_batch(topic, valid_batch)
                            t_sink_perf = time.perf_counter()
                            t_sink_epoch_ms = time.time() * 1000.0
                            batches_written.append(str(written_path))
                            batches_by_topic.setdefault(topic, []).append(str(written_path))
                            topic_counts[topic] = topic_counts.get(topic, 0) + len(valid_batch)

                            for m in by_topic_valid_entries.get(topic, []):
                                m["sink_mono"] = t_sink_perf
                                m["durable_sink_status"] = "bronze_written"
                                s_lat, s_err = self._compute_message_latency(
                                    t_sink_perf, t_sink_epoch_ms, m["sent_mono"], m["sent_epoch_ms"]
                                )
                                if s_err:
                                    timing_measurement_errors += 1
                                else:
                                    durable_sink_latencies_ms.append(s_lat)
                                    m["durable_sink_latency_ms"] = s_lat
                        except Exception:
                            partition_failed_by_topic[topic] = True
                            raise
                    high_off = highest_offset_by_topic.get(topic, -1)
                    if not partition_failed_by_topic.get(topic, False) and high_off >= 0:
                        committed_offsets[f"{topic}-0"] = high_off + 1

                if dlq_msg_entries:
                    dlq_handler.flush()
                    t_dlq_perf = time.perf_counter()
                    t_dlq_epoch_ms = time.time() * 1000.0
                    for m in dlq_msg_entries:
                        m["sink_mono"] = t_dlq_perf
                        m["durable_sink_status"] = "dlq_persisted"
                        s_lat, s_err = self._compute_message_latency(
                            t_dlq_perf, t_dlq_epoch_ms, m["sent_mono"], m["sent_epoch_ms"]
                        )
                        if s_err:
                            timing_measurement_errors += 1
                        else:
                            durable_sink_latencies_ms.append(s_lat)
                            m["durable_sink_latency_ms"] = s_lat
            else:
                by_topic: Dict[str, List[Dict[str, Any]]] = {}
                for item in (offline_records or []):
                    t = item.get("topic") or target_topics[0]
                    by_topic.setdefault(t, []).append(item)

                dlq_msg_entries_all: List[Dict[str, Any]] = []

                for topic, items in by_topic.items():
                    valid_batch = []
                    valid_msg_entries = []
                    highest_offset = -1
                    partition_failed = False

                    for item in items:
                        total_consumed += 1
                        offset = item.get("offset", total_consumed - 1)
                        partition = item.get("partition", 0)
                        highest_offset = max(highest_offset, offset)
                        raw_val = item.get("value", item)

                        is_valid, errors, canonical_env = self.validate_message(topic, raw_val)
                        t_val_perf = time.perf_counter()
                        t_val_epoch_ms = time.time() * 1000.0
                        sent_perf, sent_epoch_ms = self._extract_send_time(item, canonical_env or raw_val)
                        msg_id = self._extract_msg_id(canonical_env or raw_val, item)

                        val_lat_ms, is_timing_err = self._compute_message_latency(
                            t_val_perf, t_val_epoch_ms, sent_perf, sent_epoch_ms
                        )
                        if is_timing_err:
                            timing_measurement_errors += 1
                        else:
                            validation_latencies_ms.append(val_lat_ms)

                        msg_entry = {
                            "id": msg_id,
                            "review_id": msg_id,
                            "topic": topic,
                            "sent_mono": sent_perf,
                            "sent_epoch_ms": sent_epoch_ms,
                            "validation_mono": t_val_perf,
                            "processed_mono": t_val_perf,
                            "validation_latency_ms": val_lat_ms,
                            "latency_ms": val_lat_ms if val_lat_ms is not None else 0.0,
                            "durable_sink_latency_ms": None,
                            "sink_mono": None,
                            "sink_includes_batch_buffering": True,
                            "is_valid": is_valid,
                        }
                        message_latencies.append(msg_entry)

                        if is_valid and canonical_env is not None:
                            valid_batch.append(canonical_env)
                            valid_msg_entries.append(msg_entry)
                            total_valid += 1
                        else:
                            try:
                                dlq_res = dlq_handler.send_to_dlq(
                                    error_type="SCHEMA_VALIDATION_ERROR",
                                    source_topic=topic,
                                    partition=partition,
                                    offset=offset,
                                    payload=raw_val,
                                    errors=errors
                                )
                                quarantined_records.append(dlq_res)
                                dlq_msg_entries_all.append(msg_entry)
                                total_dlq += 1
                            except Exception:
                                partition_failed = True
                                raise

                    if valid_batch and not partition_failed:
                        try:
                            written_path = self.save_raw_batch(topic, valid_batch)
                            t_sink_perf = time.perf_counter()
                            t_sink_epoch_ms = time.time() * 1000.0
                            batches_written.append(str(written_path))
                            batches_by_topic.setdefault(topic, []).append(str(written_path))
                            topic_counts[topic] = topic_counts.get(topic, 0) + len(valid_batch)

                            for m in valid_msg_entries:
                                m["sink_mono"] = t_sink_perf
                                m["durable_sink_status"] = "bronze_written"
                                s_lat, s_err = self._compute_message_latency(
                                    t_sink_perf, t_sink_epoch_ms, m["sent_mono"], m["sent_epoch_ms"]
                                )
                                if s_err:
                                    timing_measurement_errors += 1
                                else:
                                    durable_sink_latencies_ms.append(s_lat)
                                    m["durable_sink_latency_ms"] = s_lat
                        except Exception:
                            partition_failed = True
                            raise

                    if not partition_failed and highest_offset >= 0:
                        committed_offsets[f"{topic}-0"] = highest_offset + 1

                if dlq_msg_entries_all:
                    dlq_handler.flush()
                    t_dlq_perf = time.perf_counter()
                    t_dlq_epoch_ms = time.time() * 1000.0
                    for m in dlq_msg_entries_all:
                        m["sink_mono"] = t_dlq_perf
                        m["durable_sink_status"] = "dlq_persisted"
                        s_lat, s_err = self._compute_message_latency(
                            t_dlq_perf, t_dlq_epoch_ms, m["sent_mono"], m["sent_epoch_ms"]
                        )
                        if s_err:
                            timing_measurement_errors += 1
                        else:
                            durable_sink_latencies_ms.append(s_lat)
                            m["durable_sink_latency_ms"] = s_lat

        return {
            "records_consumed": total_consumed,
            "records_valid": total_valid,
            "records_dlq": total_dlq,
            "batches_written": batches_written,
            "batches_by_topic": batches_by_topic,
            "topic_counts": topic_counts,
            "committed_offsets": committed_offsets,
            "dlq_records": quarantined_records,
            "latencies_ms": validation_latencies_ms,
            "validation_latencies_ms": validation_latencies_ms,
            "durable_sink_latencies_ms": durable_sink_latencies_ms,
            "timing_measurement_errors": timing_measurement_errors,
            "message_latencies": message_latencies,
        }

    def consume_to_bronze(
        self,
        topics: List[str],
        batch_size: int = 100,
        timeout_ms: int = 1000,
        max_iterations: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """Runs the consume loop writing valid messages to Bronze and routing errors to DLQ."""
        iterations = 0
        all_results = []
        try:
            while max_iterations is None or iterations < max_iterations:
                res = self.consume_batch_once(topics=topics, max_records=batch_size, timeout_ms=timeout_ms)
                if res["records_consumed"] > 0:
                    all_results.append(res)
                iterations += 1
                if max_iterations is None and res["records_consumed"] == 0:
                    time.sleep(0.5)
        except KeyboardInterrupt:
            print("Stopping consumer...")
        finally:
            if self.consumer:
                self.consumer.close()
        return all_results
