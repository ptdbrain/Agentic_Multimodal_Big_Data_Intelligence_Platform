import json
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple
from config.settings import settings
from storage.storage_manager import storage
from spark.spark_session import SparkSessionFactory
from spark.streaming.window_aggregations import WindowAggregator


class StreamingJob:
    """Stateful micro-batch streaming processor with event-time watermarking,
    pre-validation parse/cast, primary-key deduplication, S3A/local checkpointing,
    and idempotent restart recovery.
    """

    VALID_OUTPUT_MODES = ("update", "append", "complete")

    def __init__(
        self,
        negative_threshold: Optional[float] = None,
        checkpoint_dir: Optional[str] = None,
        watermark_seconds: int = 600,
        window_seconds: int = 60,
        output_mode: str = "update",
        strict_validation: bool = False,
    ):
        if output_mode not in self.VALID_OUTPUT_MODES:
            raise ValueError(
                f"Invalid output_mode '{output_mode}'. Expected one of {self.VALID_OUTPUT_MODES}."
            )
        self.negative_threshold = (
            negative_threshold
            if negative_threshold is not None
            else settings.analytics.sentiment_negative_threshold
        )
        self.watermark_seconds = int(watermark_seconds)
        self.window_seconds = int(window_seconds)
        self.output_mode = output_mode
        self.strict_validation = strict_validation
        self.checkpoint_dir = checkpoint_dir

        self._init_empty_state()
        if self.checkpoint_dir:
            self.load_checkpoint()

    def _init_empty_state(self) -> None:
        self.processed_batches: int = 0
        self.negative_reviews_detected: int = 0
        self.valid_events_processed: int = 0
        self.invalid_events_count: int = 0
        self.cast_error_count: int = 0
        self.duplicates_dropped: int = 0
        self.late_events_accepted: int = 0
        self.late_events_dropped: int = 0
        self.max_event_time: Optional[float] = None
        self.current_watermark: Optional[float] = None
        self.seen_keys: Dict[str, float] = {}
        self.committed_batch_ids: List[str] = []
        self.window_state: Dict[str, Dict[str, Any]] = {}
        self.quarantined_events: List[Dict[str, Any]] = []

    def _resolve_checkpoint_file(self) -> Tuple[bool, str]:
        """Returns (use_storage_backend, path_str) for reading/writing checkpoint state."""
        if not self.checkpoint_dir:
            return False, ""
        chk_str = str(self.checkpoint_dir)
        if chk_str.startswith("s3a://"):
            # Strip s3a://<bucket>/ prefix for storage.backend
            without_scheme = chk_str[len("s3a://"):]
            parts = without_scheme.split("/", 1)
            rel_path = parts[1] if len(parts) > 1 else "checkpoints/streaming_job"
            return True, f"{rel_path.rstrip('/')}/checkpoint_state.json"

        p = Path(chk_str)
        if p.is_absolute() or chk_str.startswith(".") or "\\" in chk_str or "/" in chk_str:
            # Local directory path if local or explicit temp dir
            if p.is_absolute() or Path(chk_str).parent.exists():
                return False, str(p / "checkpoint_state.json")

        resolved = SparkSessionFactory.resolve_checkpoint_path(chk_str)
        if resolved.startswith("s3a://"):
            without_scheme = resolved[len("s3a://"):]
            parts = without_scheme.split("/", 1)
            rel_path = parts[1] if len(parts) > 1 else "checkpoints/streaming_job"
            return True, f"{rel_path.rstrip('/')}/checkpoint_state.json"
        return False, str(Path(resolved) / "checkpoint_state.json")

    def save_checkpoint(self) -> Optional[str]:
        """Persists streaming state (offsets, watermark, dedup keys, window metrics) to checkpoint."""
        if not self.checkpoint_dir:
            return None

        state_payload = {
            "processed_batches": self.processed_batches,
            "negative_reviews_detected": self.negative_reviews_detected,
            "valid_events_processed": self.valid_events_processed,
            "invalid_events_count": self.invalid_events_count,
            "cast_error_count": self.cast_error_count,
            "duplicates_dropped": self.duplicates_dropped,
            "late_events_accepted": self.late_events_accepted,
            "late_events_dropped": self.late_events_dropped,
            "max_event_time": self.max_event_time,
            "current_watermark": self.current_watermark,
            "seen_keys": self.seen_keys,
            "committed_batch_ids": self.committed_batch_ids,
            "window_state": self.window_state,
            "output_mode": self.output_mode,
        }

        use_storage, target_path = self._resolve_checkpoint_file()
        if use_storage:
            storage.backend.write_json(target_path, state_payload)
            return f"s3a://{settings.storage.data_lake_bucket}/{target_path}"
        else:
            p = Path(target_path)
            p.parent.mkdir(parents=True, exist_ok=True)
            with open(p, "w", encoding="utf-8") as f:
                json.dump(state_payload, f, ensure_ascii=False, indent=2)
            return str(p)

    def load_checkpoint(self) -> bool:
        """Restores streaming state from checkpoint if present."""
        if not self.checkpoint_dir:
            return False

        use_storage, target_path = self._resolve_checkpoint_file()
        try:
            if use_storage:
                if not storage.backend.object_exists(target_path):
                    return False
                data = storage.backend.read_json(target_path)
            else:
                p = Path(target_path)
                if not p.exists():
                    return False
                with open(p, "r", encoding="utf-8") as f:
                    data = json.load(f)
        except Exception:
            return False

        if not isinstance(data, dict):
            return False

        self.processed_batches = int(data.get("processed_batches", 0))
        self.negative_reviews_detected = int(data.get("negative_reviews_detected", 0))
        self.valid_events_processed = int(data.get("valid_events_processed", 0))
        self.invalid_events_count = int(data.get("invalid_events_count", 0))
        self.cast_error_count = int(data.get("cast_error_count", 0))
        self.duplicates_dropped = int(data.get("duplicates_dropped", 0))
        self.late_events_accepted = int(data.get("late_events_accepted", 0))
        self.late_events_dropped = int(data.get("late_events_dropped", 0))
        self.max_event_time = data.get("max_event_time")
        self.current_watermark = data.get("current_watermark")
        self.seen_keys = {str(k): float(v) for k, v in (data.get("seen_keys") or {}).items()}
        self.committed_batch_ids = [str(x) for x in (data.get("committed_batch_ids") or [])]
        self.window_state = dict(data.get("window_state") or {})
        return True

    def restart(self) -> Dict[str, Any]:
        """Simulates stopping the stream processor and recovering state strictly from checkpoint."""
        self._init_empty_state()
        restored = self.load_checkpoint()
        return {
            "restored_from_checkpoint": restored,
            "total_processed": self.processed_batches,
            "negative_reviews_count": self.negative_reviews_detected,
            "committed_batches": len(self.committed_batch_ids),
            "seen_keys_count": len(self.seen_keys),
            "current_watermark": self.current_watermark,
        }

    def _parse_and_validate_event(
        self, evt: Dict[str, Any]
    ) -> Tuple[bool, Optional[str], Optional[float], Optional[float], Optional[str], Optional[str]]:
        """Performs explicit parse/cast BEFORE validation.
        Returns (is_valid, quarantine_reason, parsed_rating, parsed_epoch, event_key, product_id).
        """
        payload = evt.get("payload", evt) if isinstance(evt.get("payload"), dict) else evt

        # Extract key & product_id
        raw_key = (
            payload.get("review_id")
            or evt.get("review_id")
            or payload.get("id")
            or evt.get("event_id")
        )
        event_key = str(raw_key).strip() if raw_key is not None and str(raw_key).strip() != "" else None
        raw_prod = payload.get("product_id") or evt.get("product_id")
        product_id = str(raw_prod).strip() if raw_prod is not None and str(raw_prod).strip() != "" else "unknown"

        # 1. Parse/Cast timestamp BEFORE validation
        raw_ts = (
            payload.get("review_date")
            or payload.get("timestamp")
            or evt.get("timestamp")
            or evt.get("ingested_at")
        )
        parsed_epoch: Optional[float] = None
        if raw_ts is not None and str(raw_ts).strip() != "":
            parsed_epoch = WindowAggregator._parse_timestamp(raw_ts)
            if parsed_epoch is None:
                self.cast_error_count += 1
                return False, "INVALID_TYPE_TIMESTAMP", None, None, event_key, product_id

        # 2. Parse/Cast rating BEFORE validation
        rating_present = "rating" in payload or "rating" in evt
        raw_rating = payload.get("rating") if "rating" in payload else evt.get("rating")
        parsed_rating: Optional[float] = None

        if raw_rating is not None and (not isinstance(raw_rating, str) or raw_rating.strip() != ""):
            try:
                parsed_rating = float(raw_rating)
                if parsed_rating != parsed_rating:  # NaN
                    self.cast_error_count += 1
                    return False, "INVALID_TYPE_RATING", None, parsed_epoch, event_key, product_id
            except (ValueError, TypeError):
                self.cast_error_count += 1
                return False, "INVALID_TYPE_RATING", None, parsed_epoch, event_key, product_id
        elif rating_present and raw_rating is None:
            return False, "MISSING_RATING", None, parsed_epoch, event_key, product_id
        elif self.strict_validation:
            return False, "MISSING_RATING", None, parsed_epoch, event_key, product_id

        # 3. Validate domain/range on parsed rating
        if parsed_rating is not None:
            if parsed_rating < 1.0 or parsed_rating > 5.0:
                return False, "INVALID_RATING_OUT_OF_RANGE", parsed_rating, parsed_epoch, event_key, product_id

        if self.strict_validation and not event_key:
            return False, "MISSING_REVIEW_ID", parsed_rating, parsed_epoch, event_key, product_id

        return True, None, parsed_rating, parsed_epoch, event_key, product_id

    def process_micro_batch(
        self,
        events: List[Dict[str, Any]],
        batch_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Processes a micro-batch of streaming events with:
        - Idempotent batch_id check (prevents duplicate processing after restart)
        - Parse/cast before validation (routes malformed types & out-of-range values to quarantine)
        - Primary-key deduplication (review_id / event_id)
        - Event-time watermarking (accepts late data within watermark, drops too-late data)
        - Automatic checkpoint persistence when checkpoint_dir is configured
        """
        if batch_id is not None and str(batch_id) in self.committed_batch_ids:
            return {
                "batch_id": str(batch_id),
                "skipped_committed_batch": True,
                "micro_batch_size": 0,
                "batch_valid_count": 0,
                "batch_invalid_count": 0,
                "batch_duplicate_count": 0,
                "batch_late_accepted": 0,
                "batch_late_dropped": 0,
                "total_processed": self.processed_batches,
                "negative_reviews_count": self.negative_reviews_detected,
                "invalid_events_count": self.invalid_events_count,
                "cast_error_count": self.cast_error_count,
                "duplicates_dropped": self.duplicates_dropped,
                "late_events_accepted": self.late_events_accepted,
                "late_events_dropped": self.late_events_dropped,
                "current_watermark": self.current_watermark,
                "window_metrics": self.get_window_metrics(),
                "status": "active",
            }

        batch_valid = 0
        batch_invalid = 0
        batch_dups = 0
        batch_late_accepted = 0
        batch_late_dropped = 0
        batch_neg = 0

        for evt in events:
            is_valid, reason, rating_val, evt_epoch, evt_key, prod_id = self._parse_and_validate_event(evt)
            if not is_valid:
                batch_invalid += 1
                self.invalid_events_count += 1
                self.quarantined_events.append({
                    "event": evt,
                    "quarantine_reason": reason,
                })
                continue

            # Primary key deduplication
            if evt_key is not None:
                if evt_key in self.seen_keys:
                    batch_dups += 1
                    self.duplicates_dropped += 1
                    continue

            # Event-time watermarking check for late-arriving data
            if evt_epoch is not None:
                if self.max_event_time is not None:
                    self.current_watermark = self.max_event_time - self.watermark_seconds
                    if evt_epoch < self.current_watermark:
                        # Arrived older than watermark -> drop too-late event
                        batch_late_dropped += 1
                        self.late_events_dropped += 1
                        continue
                    elif evt_epoch < self.max_event_time:
                        # Arrived late, but within watermark -> accept and update window
                        batch_late_accepted += 1
                        self.late_events_accepted += 1

                if self.max_event_time is None or evt_epoch > self.max_event_time:
                    self.max_event_time = evt_epoch
                    self.current_watermark = self.max_event_time - self.watermark_seconds

            # Record seen key after passing validation & watermark
            if evt_key is not None:
                self.seen_keys[evt_key] = evt_epoch if evt_epoch is not None else 0.0

            batch_valid += 1
            self.processed_batches += 1
            self.valid_events_processed += 1

            is_neg = rating_val is not None and rating_val <= self.negative_threshold
            if is_neg:
                batch_neg += 1
                self.negative_reviews_detected += 1

            # Update tumbling window state if timestamp is available
            if evt_epoch is not None and self.window_seconds > 0:
                w_start = int(evt_epoch // self.window_seconds) * self.window_seconds
                w_key = f"{w_start}|{prod_id}"
                if w_key not in self.window_state:
                    self.window_state[w_key] = {
                        "window_start_epoch": w_start,
                        "window_end_epoch": w_start + self.window_seconds,
                        "product_id": prod_id,
                        "review_count": 0,
                        "rating_sum": 0.0,
                        "rating_count": 0,
                        "negative_count": 0,
                    }
                w_entry = self.window_state[w_key]
                w_entry["review_count"] += 1
                if rating_val is not None:
                    w_entry["rating_sum"] += rating_val
                    w_entry["rating_count"] += 1
                if is_neg:
                    w_entry["negative_count"] += 1

        if batch_id is not None:
            self.committed_batch_ids.append(str(batch_id))

        if self.checkpoint_dir:
            self.save_checkpoint()

        return {
            "batch_id": str(batch_id) if batch_id is not None else None,
            "skipped_committed_batch": False,
            "micro_batch_size": batch_valid,
            "batch_valid_count": batch_valid,
            "batch_invalid_count": batch_invalid,
            "batch_duplicate_count": batch_dups,
            "batch_late_accepted": batch_late_accepted,
            "batch_late_dropped": batch_late_dropped,
            "total_processed": self.processed_batches,
            "negative_reviews_count": self.negative_reviews_detected,
            "invalid_events_count": self.invalid_events_count,
            "cast_error_count": self.cast_error_count,
            "duplicates_dropped": self.duplicates_dropped,
            "late_events_accepted": self.late_events_accepted,
            "late_events_dropped": self.late_events_dropped,
            "current_watermark": self.current_watermark,
            "window_metrics": self.get_window_metrics(),
            "status": "active",
        }

    def get_window_metrics(self) -> List[Dict[str, Any]]:
        """Returns current tumbling window metrics sorted by (window_start_epoch, product_id)."""
        results = []
        for _, w in sorted(
            self.window_state.items(),
            key=lambda item: (item[1]["window_start_epoch"], item[1]["product_id"]),
        ):
            r_cnt = w["rating_count"]
            avg_r = round(w["rating_sum"] / r_cnt, 2) if r_cnt > 0 else None
            results.append({
                "window_start_epoch": w["window_start_epoch"],
                "window_end_epoch": w["window_end_epoch"],
                "product_id": w["product_id"],
                "review_count": w["review_count"],
                "avg_rating": avg_r,
                "negative_count": w["negative_count"],
            })
        return results

    def reset(self):
        """Resets internal counters and clears checkpoint state."""
        self._init_empty_state()
