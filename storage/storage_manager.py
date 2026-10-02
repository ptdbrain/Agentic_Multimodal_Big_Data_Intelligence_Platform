import os
import json
import pandas as pd
from pathlib import Path
from typing import List, Dict, Any, Optional, Union
from config.settings import settings

from storage.minio_backend import MinIOBackend
from storage.local_backend import LocalBackend

class StorageManager:
    """Unified Data Lake Abstraction Layer managing Bronze (Raw), Silver (Cleaned),
    and Gold (Marts) storage across MinIO (S3) and Local Filesystem.
    """
    
    def __init__(self, backend=None, base_dir: Optional[Union[str, Path]] = None):
        self.base_dir = Path(base_dir) if base_dir else settings.storage.local_data_dir
        self.bronze_dir = self.base_dir / "bronze"
        self.silver_dir = self.base_dir / "silver"
        self.gold_dir = self.base_dir / "gold"
        
        for d in [self.bronze_dir, self.silver_dir, self.gold_dir]:
            d.mkdir(parents=True, exist_ok=True)
            
        if backend:
            self._backend_instance = backend
        else:
            try:
                if settings.env == "test":
                    raise Exception("Test environment, fallback to LocalBackend")
                
                self._backend_instance = MinIOBackend(
                    endpoint=settings.storage.endpoint,
                    access_key=settings.storage.access_key,
                    secret_key=settings.storage.secret_key,
                    secure=settings.storage.secure,
                    bucket=settings.storage.bucket_raw
                )
            except Exception as e:
                self._backend_instance = LocalBackend(str(self.base_dir))

    @property
    def backend(self):
        # Lazy initialization fallback for tests that monkey-patch __new__
        if not hasattr(self, '_backend_instance'):
            if not hasattr(self, 'base_dir'):
                self.base_dir = settings.storage.local_data_dir
                self.bronze_dir = self.base_dir / "bronze"
                self.silver_dir = self.base_dir / "silver"
                self.gold_dir = self.base_dir / "gold"
                for d in [self.bronze_dir, self.silver_dir, self.gold_dir]:
                    d.mkdir(parents=True, exist_ok=True)
            self._backend_instance = LocalBackend(str(self.base_dir))
        return self._backend_instance

    def write_silver_parquet(self, dataset_name: str, df: pd.DataFrame, partition_col: Optional[str] = None):
        if df.empty:
            print(f"Storage: Skipping empty DataFrame for Silver ({dataset_name})")
            return
            
        if partition_col and partition_col in df.columns:
            if isinstance(self.backend, LocalBackend):
                target_dir = self.backend._get_full_path(f"silver/{dataset_name}")
                target_dir.mkdir(parents=True, exist_ok=True)
                df.to_parquet(target_dir, partition_cols=[partition_col], index=False)
            else:
                path = f"silver/{dataset_name}/{dataset_name}.parquet"
                self.backend.write_parquet(path, df)
        else:
            path = f"silver/{dataset_name}/{dataset_name}.parquet"
            self.backend.write_parquet(path, df)
            
        print(f"Storage: Written {len(df)} rows to Silver Parquet ({dataset_name})")

    def read_silver_parquet(self, dataset_name: str) -> pd.DataFrame:
        path = f"silver/{dataset_name}/{dataset_name}.parquet"
        if self.backend.object_exists(path):
            return self.backend.read_parquet(path)
            
        prefix = f"silver/{dataset_name}"
        objects = self.backend.list_objects(prefix)
        parquet_files = [obj for obj in objects if obj.endswith('.parquet')]
        
        if parquet_files:
            if isinstance(self.backend, LocalBackend):
                target_dir = self.backend._get_full_path(f"silver/{dataset_name}")
                if target_dir.exists():
                    return pd.read_parquet(target_dir)
            else:
                dfs = []
                for p in parquet_files:
                    dfs.append(self.backend.read_parquet(p))
                if dfs:
                    return pd.concat(dfs, ignore_index=True)
                    
        return pd.DataFrame()

    def write_gold_parquet(self, mart_name: str, df: pd.DataFrame):
        if df.empty:
            print(f"Storage: Skipping empty DataFrame for Gold ({mart_name})")
            return
        path = f"gold/{mart_name}/{mart_name}.parquet"
        self.backend.write_parquet(path, df)
        print(f"Storage: Written {len(df)} rows to Gold Mart ({mart_name})")

    def read_gold_parquet(self, mart_name: str) -> pd.DataFrame:
        path = f"gold/{mart_name}/{mart_name}.parquet"
        if not self.backend.object_exists(path):
            return pd.DataFrame()
        return self.backend.read_parquet(path)

    def list_silver_datasets(self) -> List[str]:
        prefix = "silver/"
        objects = self.backend.list_objects(prefix)
        datasets = set()
        for obj in objects:
            parts = obj.split('/')
            if len(parts) > 1 and parts[0] == 'silver':
                datasets.add(parts[1])
        return sorted(list(datasets))

    def list_gold_marts(self) -> List[str]:
        prefix = "gold/"
        objects = self.backend.list_objects(prefix)
        marts = set()
        for obj in objects:
            parts = obj.split('/')
            if len(parts) > 1 and parts[0] == 'gold':
                marts.add(parts[1])
        return sorted(list(marts))

    def list_bronze_topics(self) -> List[str]:
        prefix = "bronze/"
        objects = self.backend.list_objects(prefix)
        topics = set()
        for obj in objects:
            parts = obj.split('/')
            if len(parts) > 1 and parts[0] == 'bronze':
                topics.add(parts[1])
        return sorted(list(topics))

    def read_bronze_records(self, topic: str, limit: int = 100) -> List[Dict[str, Any]]:
        prefix = f"bronze/{topic}"
        objects = self.backend.list_objects(prefix)
        json_files = [obj for obj in objects if obj.endswith('.json')]
        
        if isinstance(self.backend, LocalBackend):
            try:
                full_paths = [self.backend._get_full_path(f) for f in json_files]
                full_paths.sort(key=os.path.getmtime, reverse=True)
                json_files = [p.relative_to(self.backend.base_dir).as_posix() for p in full_paths]
            except Exception:
                json_files.sort(reverse=True)
        else:
            json_files.sort(reverse=True)
            
        records = []
        for jf in json_files:
            try:
                batch = self.backend.read_json(jf)
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
        stats = {"bronze": {"files": 0, "size_bytes": 0},
                 "silver": {"files": 0, "size_bytes": 0},
                 "gold": {"files": 0, "size_bytes": 0}}
                 
        for layer in ["bronze", "silver", "gold"]:
            prefix = f"{layer}/"
            objects = self.backend.list_objects(prefix)
            stats[layer]["files"] = len(objects)
            stats[layer]["size_bytes"] = self.backend.get_size(prefix)
            
        return stats

storage = StorageManager()
