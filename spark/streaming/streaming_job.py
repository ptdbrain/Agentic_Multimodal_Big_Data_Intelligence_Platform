import time
from typing import Dict, Any, List, Optional
from config.settings import settings

class StreamingJob:
    """Spark Structured Streaming Processor simulation consuming live events.
    Configurable aggregation thresholds and robust event payload inspection.
    """

    def __init__(self, negative_threshold: Optional[float] = None, rating_field: str = "rating"):
        self.negative_threshold = (
            negative_threshold if negative_threshold is not None 
            else settings.analytics.sentiment_negative_threshold
        )
        self.rating_field = rating_field
        self.total_processed = 0
        self.rating_sum = 0.0
        self.negative_count = 0

    def process_micro_batch(self, events: List[Dict[str, Any]]) -> Dict[str, Any]:
        batch_size = len(events)
        self.total_processed += batch_size

        for evt in events:
            # Check nested payload or root level
            payload = evt.get("payload", evt) if isinstance(evt.get("payload"), dict) else evt
            rating = payload.get(self.rating_field)
            if rating is not None:
                try:
                    num_r = float(rating)
                    self.rating_sum += num_r
                    if num_r <= self.negative_threshold:
                        self.negative_count += 1
                except (ValueError, TypeError):
                    continue

        avg_rating = round(self.rating_sum / max(1, self.total_processed), 2)
        return {
            "micro_batch_size": batch_size,
            "total_processed": self.total_processed,
            "running_avg_rating": avg_rating,
            "negative_reviews_count": self.negative_count,
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
        }

    def reset(self):
        self.total_processed = 0
        self.rating_sum = 0.0
        self.negative_count = 0

