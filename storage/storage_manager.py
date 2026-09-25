import os
import json
import pandas as pd
from pathlib import Path
from typing import List, Dict, Any, Optional
from config.settings import settings

class StorageManager:
    """Unified Data Lake Abstraction Layer managing Bronze (Raw), Silver (Cleaned),
    and Gold (Marts) storage across MinIO (S3) and Local Filesystem.
    """
    
    def __init__(self):
        self.base_dir = settings.storage.local_data_dir
        self.bronze_dir = self.base_dir / "bronze"
        self.silver_dir = self.base_dir / "silver"
        self.gold_dir = self.base_dir / "gold"
        
        for d in [self.bronze_dir, self.silver_dir, self.gold_dir]:
            d.mkdir(parents=True, exist_ok=True)

    def write_silver_parquet(self, dataset_name: str, df: pd.DataFrame, partition_col: Optional[str] = None):
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
        if not target_dir.exists():
            return pd.DataFrame()
        return pd.read_parquet(target_dir)

    def write_gold_parquet(self, mart_name: str, df: pd.DataFrame):
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

storage = StorageManager()
