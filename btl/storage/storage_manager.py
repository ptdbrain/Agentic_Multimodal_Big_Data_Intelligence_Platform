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
        if getattr(obj, "_explicit_backend", False) and getattr(obj, "_backend_instance", None) is not None:
            return obj._backend_instance

        # Support unit tests that construct StorageManager via __new__ with custom base_dir
        if not hasattr(obj, "_mode"):
            custom_base = getattr(obj, "base_dir", None)
            if custom_base is not None:
                obj._backend_instance = LocalBackend(str(custom_base))
                obj._explicit_backend = True
                return obj._backend_instance

        resolved_mode = settings.storage.resolve_backend_mode(getattr(obj, "_mode", None))
        current = getattr(obj, "_backend_instance", None)

        if resolved_mode == "local":
            if not isinstance(current, LocalBackend):
                base = getattr(obj, "base_dir", settings.storage.local_data_dir)
                for d in [Path(base) / "bronze", Path(base) / "silver", Path(base) / "gold"]:
                    d.mkdir(parents=True, exist_ok=True)
                obj._backend_instance = LocalBackend(str(base))
        else:
            if not isinstance(current, MinIOBackend):
                obj._backend_instance = MinIOBackend(
                    endpoint=settings.storage.endpoint,
                    access_key=settings.storage.access_key,
                    secret_key=settings.storage.secret_key,
                    secure=settings.storage.secure,
                    bucket=settings.storage.data_lake_bucket,
                    enable_local_mirror=settings.storage.enable_local_mirror
                )
        return obj._backend_instance

    def __set__(self, obj, value):
        obj._backend_instance = value
        obj._explicit_backend = True

class StorageManager:
    """Unified Data Lake Abstraction Layer managing Bronze (Raw), Silver (Cleaned),
    and Gold (Marts) storage with strict separation between:
      - Integration/Production mode: Real MinIO S3 (no local mirror or silent fallback)
      - Local/Test mode: LocalBackend (only when explicitly declared)
    """
    backend = BackendDescriptor()
    
    def __init__(
        self,
        backend=None,
        base_dir: Optional[Union[str, Path]] = None,
        mode: Optional[str] = None
    ):
        self.base_dir = Path(base_dir) if base_dir else settings.storage.local_data_dir
        self.bronze_dir = self.base_dir / "bronze"
        self.silver_dir = self.base_dir / "silver"
        self.gold_dir = self.base_dir / "gold"
        self._mode: Optional[str] = mode.strip().lower() if mode else (
            "local" if base_dir is not None and backend is None else None
        )
        self._explicit_backend = False
        self._backend_instance = None

        if backend is not None:
            self._backend_instance = backend
            self._explicit_backend = True
            if isinstance(backend, LocalBackend):
                self._ensure_local_dirs()
        elif self._mode in ("local", "test") or settings.storage.resolve_backend_mode(self._mode) == "local":
            self._ensure_local_dirs()
            self._backend_instance = LocalBackend(str(self.base_dir))
            if self._mode is not None:
                self._explicit_backend = True
        else:
            self._backend_instance = MinIOBackend(
                endpoint=settings.storage.endpoint,
                access_key=settings.storage.access_key,
                secret_key=settings.storage.secret_key,
                secure=settings.storage.secure,
                bucket=settings.storage.data_lake_bucket,
                enable_local_mirror=settings.storage.enable_local_mirror
            )
            if self._mode is not None:
                self._explicit_backend = True

    def _ensure_local_dirs(self) -> None:
        for d in [self.bronze_dir, self.silver_dir, self.gold_dir]:
            d.mkdir(parents=True, exist_ok=True)

    @property
    def mode(self) -> str:
        """Returns the current resolved mode ('integration' or 'local')."""
        resolved = settings.storage.resolve_backend_mode(getattr(self, "_mode", None))
        return "local" if resolved == "local" else "integration"

    def set_mode(self, mode: str, base_dir: Optional[Union[str, Path]] = None) -> None:
        """Explicitly configures StorageManager mode ('integration' vs 'local'/'test')."""
        resolved = settings.storage.resolve_backend_mode(mode)
        self._mode = mode.strip().lower()
        if base_dir is not None:
            self.base_dir = Path(base_dir)
            self.bronze_dir = self.base_dir / "bronze"
            self.silver_dir = self.base_dir / "silver"
            self.gold_dir = self.base_dir / "gold"

        if resolved == "local":
            self._ensure_local_dirs()
            self._backend_instance = LocalBackend(str(self.base_dir))
            self._explicit_backend = True
        else:
            self._backend_instance = MinIOBackend(
                endpoint=settings.storage.endpoint,
                access_key=settings.storage.access_key,
                secret_key=settings.storage.secret_key,
                secure=settings.storage.secure,
                bucket=settings.storage.data_lake_bucket,
                enable_local_mirror=settings.storage.enable_local_mirror
            )
            self._explicit_backend = True

    def reset_to_env_mode(self) -> None:
        """Resets backend selection to follow environment / settings.runtime_mode dynamically."""
        self._mode = None
        self._explicit_backend = False
        self._backend_instance = None

    def health_check(self) -> bool:
        """Verifies active storage backend connectivity. Raises ConnectionError if unreachable."""
        b = self.backend
        if isinstance(b, MinIOBackend):
            return b.health_check()
        if isinstance(b, LocalBackend):
            self._ensure_local_dirs()
            return self.base_dir.exists()
        return True

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
        """Writes raw batch to Bronze layer via configured backend (MinIO or explicit Local)."""
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

        import uuid
        filename = f"batch_{int(time.time() * 1000)}_{uuid.uuid4().hex[:6]}.json"
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
        prefix = f"silver/{dataset_name}"
        objects = self.backend.list_objects(prefix)
        parquet_files = [
            obj for obj in objects
            if obj.endswith('.parquet')
            and '/_temporary/' not in obj.replace('\\', '/')
            and not Path(obj).name.startswith(('_', '.'))
        ]
        spark_parts = [obj for obj in parquet_files if Path(obj).name.startswith('part-')]
        target_files = spark_parts if spark_parts else parquet_files

        if target_files:
            if isinstance(self.backend, LocalBackend) and not spark_parts:
                target_dir = self.backend._get_full_path(f"silver/{dataset_name}")
                if target_dir.exists():
                    return pd.read_parquet(target_dir)
            dfs = [self.backend.read_parquet(p) for p in target_files]
            if dfs:
                return pd.concat(dfs, ignore_index=True)

        path = f"silver/{dataset_name}/{dataset_name}.parquet"
        if self.backend.object_exists(path):
            return self.backend.read_parquet(path)

        return pd.DataFrame()

    def write_gold_parquet(self, mart_name: str, df: Any):
        if df is None:
            return
        if hasattr(df, "empty") and df.empty:
            print(f"Storage: Skipping empty DataFrame for Gold ({mart_name})")
            return
        # If Spark DataFrame, convert to pandas if writing through standard backend
        if hasattr(df, "toPandas") and not isinstance(df, pd.DataFrame):
            df_to_write = df.toPandas()
        else:
            df_to_write = df
            
        if hasattr(df_to_write, "empty") and df_to_write.empty:
            print(f"Storage: Skipping empty DataFrame for Gold ({mart_name})")
            return

        path = f"gold/{mart_name}/{mart_name}.parquet"
        self.backend.write_parquet(path, df_to_write)
        print(f"Storage: Written {len(df_to_write)} rows to Gold Mart ({mart_name})")

    def read_gold_parquet(self, mart_name: str) -> pd.DataFrame:
        prefix = f"gold/{mart_name}"
        objects = self.backend.list_objects(prefix)
        parquet_files = [
            obj for obj in objects
            if obj.endswith('.parquet')
            and '/_temporary/' not in obj.replace('\\', '/')
            and not Path(obj).name.startswith(('_', '.'))
        ]
        spark_parts = [obj for obj in parquet_files if Path(obj).name.startswith('part-')]
        target_files = spark_parts if spark_parts else parquet_files

        if target_files:
            if isinstance(self.backend, LocalBackend) and not spark_parts:
                target_dir = self.backend._get_full_path(f"gold/{mart_name}")
                if target_dir.exists():
                    return pd.read_parquet(target_dir)
            dfs = [self.backend.read_parquet(p) for p in target_files]
            if dfs:
                return pd.concat(dfs, ignore_index=True)

        path = f"gold/{mart_name}/{mart_name}.parquet"
        if self.backend.object_exists(path):
            return self.backend.read_parquet(path)

        return pd.DataFrame()

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
            except (ConnectionError, RuntimeError):
                raise
            except FileNotFoundError:
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
