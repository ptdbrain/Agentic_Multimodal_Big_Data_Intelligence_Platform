from typing import List, Dict, Any
from collections import defaultdict
import datetime

class WindowAggregator:
    """Tumbling and sliding window aggregations for 1-minute and 5-minute velocity metrics."""

    @staticmethod
    def aggregate_tumbling_window(events: List[Dict[str, Any]], window_seconds: int = 60) -> List[Dict[str, Any]]:
        windows = defaultdict(lambda: {"count": 0, "ratings": []})

        for evt in events:
            ts_str = evt.get("timestamp")
            try:
                dt = datetime.datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
            except Exception:
                continue

            epoch = int(dt.timestamp())
            window_start = (epoch // window_seconds) * window_seconds
            
            windows[window_start]["count"] += 1
            rating = evt.get("payload", {}).get("rating")
            if rating:
                windows[window_start]["ratings"].append(float(rating))

        results = []
        for w_start, data in sorted(windows.items()):
            w_time = datetime.datetime.fromtimestamp(w_start, tz=datetime.timezone.utc)
            ratings = data["ratings"]
            avg_r = round(sum(ratings) / len(ratings), 2) if ratings else None
            results.append({
                "window_start": w_time.isoformat(),
                "window_duration_sec": window_seconds,
                "event_count": data["count"],
                "avg_rating": avg_r,
                "rate_eps": round(data["count"] / window_seconds, 2)
            })
        return results
