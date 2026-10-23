from typing import List, Dict, Any, Optional
from collections import defaultdict
import datetime

class WindowAggregator:
    """Tumbling and sliding window aggregations for velocity metrics.
    Robust against diverse timestamp encodings and nested/flat event payloads.
    """

    @staticmethod
    def _parse_timestamp(ts_val: Any) -> Optional[float]:
        if ts_val is None:
            return None
        if isinstance(ts_val, (int, float)):
            # If timestamp is in milliseconds, convert to seconds
            return ts_val / 1000.0 if ts_val > 1e11 else float(ts_val)
        if isinstance(ts_val, datetime.datetime):
            return ts_val.timestamp()
        if isinstance(ts_val, str):
            try:
                # Handle ISO format with Z or timezone offset
                dt = datetime.datetime.fromisoformat(ts_val.replace("Z", "+00:00"))
                return dt.timestamp()
            except Exception:
                try:
                    return float(ts_val)
                except ValueError:
                    return None
        return None

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
            payload = evt.get("payload", evt) if isinstance(evt.get("payload"), dict) else evt
            raw_ts = evt.get(timestamp_field) or payload.get(timestamp_field)
            epoch = cls._parse_timestamp(raw_ts)
            if epoch is None:
                continue

            window_start = int(epoch // window_seconds) * window_seconds
            windows[window_start]["count"] += 1

            # 2. Resolve value (search payload or root)
            val = payload.get(value_field) if value_field in payload else evt.get(value_field)
            if val is not None:
                try:
                    windows[window_start]["values"].append(float(val))
                except (ValueError, TypeError):
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
                "avg_rating": avg_val,
                "rate_eps": round(data["count"] / window_seconds, 2)
            })
        return results

