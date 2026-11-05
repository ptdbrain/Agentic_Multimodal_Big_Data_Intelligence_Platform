from typing import List, Dict, Any, Optional
from collections import defaultdict
import datetime
import math

class WindowAggregator:
    """Local tumbling-window summaries; not a Kafka transport benchmark.
    Robust against diverse timestamp encodings and nested/flat event payloads.
    """

    @staticmethod
    def _parse_timestamp(ts_val: Any) -> Optional[float]:
        from spark.etl.normalizer import EntityNormalizer
        normalized = EntityNormalizer.normalize_timestamp(ts_val)
        if normalized is None:
            return None
        return datetime.datetime.fromisoformat(normalized.replace('Z','+00:00')).timestamp()

    @classmethod
    def aggregate_tumbling_window(
        cls,
        events: List[Dict[str, Any]],
        window_seconds: int = 60,
        timestamp_field: str = "timestamp",
        value_field: str = "rating"
    ) -> List[Dict[str, Any]]:
        if not events or window_seconds <= 0:
            return []

        windows = defaultdict(lambda: {"count": 0, "values": []})

        for evt in events:
            # 1. Resolve timestamp (search root or payload)
            if not isinstance(evt,dict):
                continue
            payload = evt.get("payload", evt) if isinstance(evt.get("payload"), dict) else evt
            raw_ts = evt.get(timestamp_field)
            if raw_ts is None:
                raw_ts = payload.get(timestamp_field)
            epoch = cls._parse_timestamp(raw_ts)
            if epoch is None:
                continue

            window_start = int(epoch // window_seconds) * window_seconds
            windows[window_start]["count"] += 1

            # 2. Resolve value (search payload or root)
            val = payload.get(value_field) if value_field in payload else evt.get(value_field)
            if val is not None:
                try:
                    numeric = float(val)
                    if (not isinstance(val,bool) and math.isfinite(numeric)
                            and (value_field != 'rating' or 1 <= numeric <= 5)):
                        windows[window_start]["values"].append(numeric)
                except (ValueError, TypeError, OverflowError):
                    pass

        results = []
        for w_start, data in sorted(windows.items()):
            w_time = datetime.datetime.fromtimestamp(w_start, tz=datetime.timezone.utc)
            vals = data["values"]
            avg_val = round(sum(vals) / len(vals), 2) if vals else None
            results.append({
                "window_start": w_time.isoformat(),
                "window_duration_sec": window_seconds,
                "event_count": data["count"],
                ("avg_rating" if value_field == 'rating' else "avg_value"): avg_val,
                "value_field": value_field,
                "value_sample_size": len(vals),
                "rate_eps": round(data["count"] / window_seconds, 2)
            })
        return results

