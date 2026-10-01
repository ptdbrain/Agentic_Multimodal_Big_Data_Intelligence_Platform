import os
import json
import pandas as pd
from pathlib import Path
from typing import List, Dict, Any, Optional, Union
from config.settings import settings

class StorageManager:
    """Unified Data Lake Abstraction Layer managing Bronze (Raw), Silver (Cleaned),
    and Gold (Marts) storage across MinIO (S3) and Local Filesystem.
    """
    
    def __init__(self, base_dir: Optional[Union[str, Path]] = None):
        self.base_dir = Path(base_dir) if base_dir else settings.storage.local_data_dir
        self.bronze_dir = self.base_dir / "bronze"
        self.silver_dir = self.base_dir / "silver"
        self.gold_dir = self.base_dir / "gold"
        
        for d in [self.bronze_dir, self.silver_dir, self.gold_dir]:
            d.mkdir(parents=True, exist_ok=True)

    def write_silver_parquet(self, dataset_name: str, df: pd.DataFrame, partition_col: Optional[str] = None):
        if df.empty:
            print(f"Storage: Skipping empty DataFrame for Silver ({dataset_name})")
            return
        target_dir = self.silver_dir / dataset_name
        target_dir.mkdir(parents=True, exist_ok=True)
        if partition_col and partition_col in df.columns:
            df.to_parquet(target_dir, partition_cols=[partition_col], index=False)
        else:
            file_path = target_dir / f"{dataset_name}.parquet"
            df.to_parquet(file_path, index=False)
        print(f"Storage: Written {len(df)} rows to Silver Parquet ({dataset_name})")

    def read_silver_parquet(self, dataset_name: str) -> pd.DataFrame:
        target_dir = self.silver_dir / dataset_name
        file_path = target_dir / f"{dataset_name}.parquet"
        if file_path.exists():
            return pd.read_parquet(file_path)
        if target_dir.exists() and any(target_dir.glob("*.parquet")):
            return pd.read_parquet(target_dir)
        return pd.DataFrame()

    def write_gold_parquet(self, mart_name: str, df: pd.DataFrame):
        if df.empty:
            print(f"Storage: Skipping empty DataFrame for Gold ({mart_name})")
            return
        target_dir = self.gold_dir / mart_name
        target_dir.mkdir(parents=True, exist_ok=True)
        file_path = target_dir / f"{mart_name}.parquet"
        df.to_parquet(file_path, index=False)
        print(f"Storage: Written {len(df)} rows to Gold Mart ({mart_name})")

    def read_gold_parquet(self, mart_name: str) -> pd.DataFrame:
        file_path = self.gold_dir / mart_name / f"{mart_name}.parquet"
        if not file_path.exists():
            return pd.DataFrame()
        return pd.read_parquet(file_path)

    def list_silver_datasets(self) -> List[str]:
        """Dynamically lists all Silver dataset names currently stored."""
        if not self.silver_dir.exists():
            return []
        return [d.name for d in self.silver_dir.iterdir() if d.is_dir() and not d.name.startswith(".")]

    def list_gold_marts(self) -> List[str]:
        """Dynamically lists all Gold data marts currently stored."""
        if not self.gold_dir.exists():
            return []
        return [d.name for d in self.gold_dir.iterdir() if d.is_dir() and not d.name.startswith(".")]

    def list_bronze_topics(self) -> List[str]:
        """Dynamically lists all Bronze topics with ingested data."""
        if not self.bronze_dir.exists():
            return []
        return [d.name for d in self.bronze_dir.iterdir() if d.is_dir() and not d.name.startswith(".")]

    def read_bronze_records(self, topic: str, limit: int = 100) -> List[Dict[str, Any]]:
        """Reads recent raw records from bronze storage for a given topic."""
        topic_dir = self.bronze_dir / topic
        if not topic_dir.exists():
            return []
        json_files = sorted(topic_dir.glob("**/*.json"), key=os.path.getmtime, reverse=True)
        records = []
        for jf in json_files:
            try:
                with open(jf, "r", encoding="utf-8") as f:
                    batch = json.load(f)
                    if isinstance(batch, list):
                        records.extend(batch)
                    elif isinstance(batch, dict):
                        records.append(batch)
                    if len(records) >= limit:
                        return records[:limit]
            except Exception:
                continue
        return records[:limit]

    def get_storage_stats(self) -> Dict[str, Any]:
        """Calculates storage footprint across Bronze, Silver, and Gold layers."""
        stats = {"bronze": {"files": 0, "size_bytes": 0},
                 "silver": {"files": 0, "size_bytes": 0},
                 "gold": {"files": 0, "size_bytes": 0}}
        for layer, ldir in [("bronze", self.bronze_dir), ("silver", self.silver_dir), ("gold", self.gold_dir)]:
            if ldir.exists():
                files = [f for f in ldir.glob("**/*") if f.is_file() and not f.name.startswith(".")]
                stats[layer]["files"] = len(files)
                stats[layer]["size_bytes"] = sum(f.stat().st_size for f in files)
        return stats

storage = StorageManager()

