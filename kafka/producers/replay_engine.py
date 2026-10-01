import sys
import time
import json
import argparse
import datetime
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(REPO_ROOT))
from typing import Optional, List, Dict, Any, Union
from kafka.producers.stream_producer import StreamProducer
from ingestion.file_loader.loader import FileLoader
from config.settings import settings

class ReplayEngine:
    """Replays static historical datasets (CSV, JSON, JSONL, Parquet) as real-time event streams.
    Direct proof of Velocity for Big Data evaluations.
    """
    
    def __init__(self, producer: Optional[StreamProducer] = None):
        self.producer = producer or StreamProducer()

    def replay_dataset(
        self,
        file_path: Union[str, Path],
        topic: str,
        rate: Optional[float] = None,
        limit: Optional[int] = None,
        key_field: Optional[str] = None,
        timestamp_fields: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        p = Path(file_path)
        data = FileLoader.load_records(p)

        if limit:
            data = data[:limit]

        target_rate = rate if rate is not None else settings.ingestion.rate_limit_rps
        print(f"[*] Starting Replay Engine on {len(data)} records -> topic '{topic}' @ {target_rate} events/sec")
        interval = 1.0 / target_rate if target_rate > 0 else 0
        t0 = time.time()
        emitted = 0
        ts_fields = timestamp_fields or ["timestamp", "review_date", "created_at"]

        for record in data:
            # Replay with current timestamp
            record_copy = dict(record)
            now_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()
            for tf in ts_fields:
                if tf in record_copy:
                    record_copy[tf] = now_iso
            
            if key_field:
                key = record_copy.get(key_field)
            else:
                key = record_copy.get("product_id") or record_copy.get("review_id") or record_copy.get("id")

            self.producer.send_message(topic, key=str(key) if key else None, value=record_copy)
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
