import time
from typing import Dict, Any, List

class StreamingJob:
    """Spark Structured Streaming Processor simulation consuming live events."""

    def __init__(self):
        self.total_processed = 0
        self.rating_sum = 0.0
        self.negative_count = 0

    def process_micro_batch(self, events: List[Dict[str, Any]]) -> Dict[str, Any]:
        batch_size = len(events)
        self.total_processed += batch_size

        for evt in events:
            payload = evt.get("payload", {})
            rating = payload.get("rating")
            if rating:
                self.rating_sum += float(rating)
                if float(rating) <= 2.0:
                    self.negative_count += 1

        avg_rating = round(self.rating_sum / max(1, self.total_processed), 2)
        return {
            "micro_batch_size": batch_size,
            "total_processed": self.total_processed,
            "running_avg_rating": avg_rating,
            "negative_reviews_count": self.negative_count,
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
        }
