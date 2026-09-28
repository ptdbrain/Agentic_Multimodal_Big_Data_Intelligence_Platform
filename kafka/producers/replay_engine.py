import sys
import time
import json
import argparse
import datetime
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(REPO_ROOT))
from kafka.producers.stream_producer import StreamProducer

class ReplayEngine:
    """Replays static historical datasets as real-time event streams with updated timestamps.
    Direct proof of Velocity for Big Data evaluations.
    """
    
    def __init__(self, producer: StreamProducer = None):
        self.producer = producer or StreamProducer()

    def replay_dataset(self, file_path: str, topic: str, rate: float = 100.0, limit: int = None):
        p = Path(file_path)
        with open(p, "r", encoding="utf-8") as f:
            data = json.load(f)

        if limit:
            data = data[:limit]

        print(f"[*] Starting Replay Engine on {len(data)} records -> topic '{topic}' @ {rate} events/sec")
        interval = 1.0 / rate if rate > 0 else 0
        t0 = time.time()
        emitted = 0

        for record in data:
            # Replay with current timestamp
            record_copy = dict(record)
            now_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()
            if "timestamp" in record_copy:
                record_copy["timestamp"] = now_iso
            if "review_date" in record_copy:
                record_copy["review_date"] = now_iso
            
            key = record_copy.get("product_id", "")
            self.producer.send_message(topic, key=key, value=record_copy)
            emitted += 1
            if interval > 0:
                time.sleep(interval)

        duration = max(time.time() - t0, 0.001)
        actual_throughput = emitted / duration
        print(f"[OK] Replay finished: {emitted} events in {duration:.2f}s (Throughput: {actual_throughput:.1f} events/sec)")
        return {
            "emitted": emitted,
            "duration_sec": duration,
            "throughput_eps": actual_throughput
        }

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset", default=str(REPO_ROOT / "data" / "sample" / "reviews.json"))
    parser.add_argument("--topic", default="raw.reviews")
    parser.add_argument("--rate", type=float, default=100.0)
    parser.add_argument("--limit", type=int, default=200)
    args = parser.parse_args()

    engine = ReplayEngine()
    engine.replay_dataset(args.dataset, args.topic, rate=args.rate, limit=args.limit)
