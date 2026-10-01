import sys
import time
import json
import datetime
from pathlib import Path
from typing import List, Dict, Any, Optional

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(REPO_ROOT))
from config.settings import settings

class RawConsumer:
    """Consumes raw Kafka streams and writes unaltered raw JSON batches to Bronze layer
    partitioned by year=YYYY/month=MM/day=DD/hour=HH (event-time preferred, processing-time fallback).
    """
    
    def __init__(self, bronze_dir: Optional[Path] = None):
        self.bronze_dir = bronze_dir or (settings.storage.local_data_dir / "bronze")
        self.bronze_dir.mkdir(parents=True, exist_ok=True)

    def _resolve_datetime(self, record: Dict[str, Any], timestamp_field: Optional[str] = None) -> datetime.datetime:
        ts_val = None
        if timestamp_field:
            ts_val = record.get(timestamp_field)
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

