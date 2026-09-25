import sys
import time
import json
import datetime
from pathlib import Path
from typing import List, Dict, Any

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(REPO_ROOT))
from config.settings import settings

class RawConsumer:
    """Consumes raw Kafka streams and writes unaltered raw JSON batches to Bronze layer
    partitioned by year=YYYY/month=MM/day=DD/hour=HH.
    """
    
    def __init__(self, bronze_dir: Path = None):
        self.bronze_dir = bronze_dir or (settings.storage.local_data_dir / "bronze")
        self.bronze_dir.mkdir(parents=True, exist_ok=True)

    def save_raw_batch(self, topic: str, records: List[Dict[str, Any]]) -> Path:
        now = datetime.datetime.now(datetime.timezone.utc)
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
