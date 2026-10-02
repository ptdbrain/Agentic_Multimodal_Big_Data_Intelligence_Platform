import os
import json
import pandas as pd
from pathlib import Path
from typing import List, Dict, Any, Optional, Union
from config.settings import settings

from storage.minio_backend import MinIOBackend
from storage.local_backend import LocalBackend

class BackendDescriptor:
    def __get__(self, obj, objtype=None):
        if obj is None:
            # Accessed on class: StorageManager.backend == MinIOBackend
            return MinIOBackend
        # Accessed on instance: storage.backend
        if not hasattr(obj, '_backend_instance') or obj._backend_instance is None:
            obj._backend_instance = MinIOBackend(bucket=settings.storage.data_lake_bucket)
        return obj._backend_instance

    def __set__(self, obj, value):
        obj._backend_instance = value

class StorageManager:
    """Unified Data Lake Abstraction Layer managing Bronze (Raw), Silver (Cleaned),
    and Gold (Marts) storage across MinIO (S3) and Local Filesystem.
    """
    backend = BackendDescriptor()
    
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
            self._backend_instance = MinIOBackend(
                endpoint=settings.storage.endpoint,
                access_key=settings.storage.access_key,
                secret_key=settings.storage.secret_key,
                secure=settings.storage.secure,
                bucket=settings.storage.data_lake_bucket
            )

    def write_json(self, path: str, data: Any) -> str:
        """Writes JSON to Data Lake via current backend."""
        return self.backend.write_json(path, data)

    def read_json(self, path: str) -> Any:
        """Reads JSON from Data Lake via current backend."""
        return self.backend.read_json(path)

    def write_parquet(self, path: str, df: pd.DataFrame) -> str:
        """Writes Parquet to Data Lake via current backend."""
        return self.backend.write_parquet(path, df)

    def read_parquet(self, path: str) -> pd.DataFrame:
        """Reads Parquet from Data Lake via current backend."""
        return self.backend.read_parquet(path)

    def list_objects(self, prefix: str) -> List[str]:
        """Lists objects under prefix via current backend."""
        return self.backend.list_objects(prefix)

    def object_exists(self, path: str) -> bool:
        """Checks if object exists via current backend."""
        return self.backend.object_exists(path)

    def write_bronze_json(
        self,
        topic: str,
        records: List[Dict[str, Any]],
        timestamp_field: Optional[str] = None
    ) -> str:
        """Writes raw batch to Bronze layer via backend (MinIO or Local)."""
        import time, datetime
        if not records:
            now = datetime.datetime.now(datetime.timezone.utc)
        else:
            rec = records[0]
            ts_val = None
            if timestamp_field:
                ts_val = rec.get(timestamp_field)
            if not ts_val:
                ts_val = rec.get("ingested_at") or rec.get("timestamp") or rec.get("created_at") or rec.get("review_date")
                if not ts_val and isinstance(rec.get("payload"), dict):
                    ts_val = rec["payload"].get("timestamp") or rec["payload"].get("review_date") or rec["payload"].get("created_at")
            if ts_val:
                try:
                    if isinstance(ts_val, (int, float)):
                        sec = ts_val / 1000.0 if ts_val > 1e11 else float(ts_val)
                        now = datetime.datetime.fromtimestamp(sec, tz=datetime.timezone.utc)
                    elif isinstance(ts_val, str):
                        now = datetime.datetime.fromisoformat(ts_val.replace("Z", "+00:00"))
                    else:
                        now = datetime.datetime.now(datetime.timezone.utc)
                except Exception:
                    now = datetime.datetime.now(datetime.timezone.utc)
            else:
                now = datetime.datetime.now(datetime.timezone.utc)

        filename = f"batch_{int(time.time() * 1000)}.json"
        object_path = f"bronze/{topic}/year={now.year}/month={now.month:02d}/day={now.day:02d}/hour={now.hour:02d}/{filename}"
        
        self.backend.write_json(object_path, records)
        print(f"Bronze Ingestion: Saved {len(records)} raw records to {object_path} ({type(self.backend).__name__})")
        return object_path

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
