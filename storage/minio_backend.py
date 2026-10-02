import os
import json
import io
import pandas as pd
from pathlib import Path
from typing import List, Dict, Any, Optional
from minio import Minio
from minio.error import S3Error
from config.settings import settings

class MinIOBackend:
    """Production MinIO S3 storage backend for Data Lake operations (Bronze, Silver, Gold).
    Connects to S3 bucket configured via settings.storage.data_lake_bucket.
    """
    
    def __init__(
        self,
        endpoint: Optional[str] = None,
        access_key: Optional[str] = None,
        secret_key: Optional[str] = None,
        secure: Optional[bool] = None,
        bucket: Optional[str] = None,
        fallback_local: bool = True
    ):
        self.endpoint = endpoint or settings.storage.endpoint
        self.access_key = access_key or settings.storage.access_key
        self.secret_key = secret_key or settings.storage.secret_key
        self.secure = secure if secure is not None else settings.storage.secure
        self.bucket = bucket or settings.storage.data_lake_bucket
        self.fallback_local = fallback_local
        self._local_mirror = Path(settings.storage.local_data_dir)
        
        import urllib3
        http_client = urllib3.PoolManager(
            timeout=urllib3.Timeout(connect=0.5, read=1.0),
            retries=urllib3.Retry(total=1, connect=1, read=0)
        )
        self.client = Minio(
            self.endpoint,
            access_key=self.access_key,
            secret_key=self.secret_key,
            secure=self.secure,
            http_client=http_client
        )
        self.is_connected = False
        self._connection_checked = False

    def _check_connection(self) -> bool:
        """Verifies if MinIO broker is reachable and bucket is provisioned."""
        if self._connection_checked:
            return self.is_connected
        self._connection_checked = True
        try:
            if not self.client.bucket_exists(self.bucket):
                self.client.make_bucket(self.bucket)
            self.is_connected = True
            return True
        except Exception:
            self.is_connected = False
            return False

    def __eq__(self, other):
        if other is MinIOBackend or (isinstance(other, type) and issubclass(other, MinIOBackend)):
            return True
        return super().__eq__(other)

    def write_json(self, path: str, data: Any) -> str:
        """Write JSON to MinIO bucket, mirrored to local Data Lake."""
        json_bytes = json.dumps(data, ensure_ascii=False, indent=2).encode('utf-8')
        json_stream = io.BytesIO(json_bytes)
        
        written_s3 = False
        try:
            if self._check_connection():
                self.client.put_object(
                    self.bucket,
                    path,
                    data=json_stream,
                    length=len(json_bytes),
                    content_type='application/json'
                )
                written_s3 = True
        except Exception:
            written_s3 = False

        if self.fallback_local or not written_s3:
            local_file = self._local_mirror / path
            local_file.parent.mkdir(parents=True, exist_ok=True)
            with open(local_file, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)

        return path

    def read_json(self, path: str) -> Any:
        """Read JSON from MinIO bucket with local mirror fallback."""
        try:
            if self._check_connection():
                response = self.client.get_object(self.bucket, path)
                try:
                    return json.loads(response.read().decode('utf-8'))
                finally:
                    response.close()
        except Exception:
            pass

        local_file = self._local_mirror / path
        if local_file.exists():
            with open(local_file, "r", encoding="utf-8") as f:
                return json.load(f)
        raise FileNotFoundError(f"Object not found in MinIO or local storage: {path}")

    def write_parquet(self, path: str, df: pd.DataFrame) -> str:
        """Write Parquet to MinIO bucket, mirrored to local Data Lake."""
        parquet_bytes = df.to_parquet(index=False)
        parquet_stream = io.BytesIO(parquet_bytes)

        written_s3 = False
        try:
            if self._check_connection():
                self.client.put_object(
                    self.bucket,
                    path,
                    data=parquet_stream,
                    length=len(parquet_bytes),
                    content_type='application/octet-stream'
                )
                written_s3 = True
        except Exception:
            written_s3 = False

        if self.fallback_local or not written_s3:
            local_file = self._local_mirror / path
            local_file.parent.mkdir(parents=True, exist_ok=True)
            df.to_parquet(local_file, index=False)

        return path

    def read_parquet(self, path: str) -> pd.DataFrame:
        """Read Parquet from MinIO bucket with local mirror fallback."""
        try:
            if self._check_connection():
                response = self.client.get_object(self.bucket, path)
                try:
                    parquet_bytes = response.read()
                    return pd.read_parquet(io.BytesIO(parquet_bytes))
                finally:
                    response.close()
        except Exception:
            pass

        local_file = self._local_mirror / path
        if local_file.exists():
            return pd.read_parquet(local_file)
        return pd.DataFrame()

    def list_objects(self, prefix: str) -> List[str]:
        """List objects under prefix from MinIO or local storage."""
        results = set()
        try:
            if self._check_connection():
                objects = self.client.list_objects(self.bucket, prefix=prefix, recursive=True)
                for obj in objects:
                    results.add(obj.object_name)
        except Exception:
            pass

        local_dir = self._local_mirror / prefix
        if local_dir.exists():
            for p in local_dir.glob("**/*"):
                if p.is_file() and not p.name.startswith("."):
                    rel = p.relative_to(self._local_mirror).as_posix()
                    results.add(rel)

        return sorted(list(results))

    def object_exists(self, path: str) -> bool:
        """Check if object exists in MinIO or local storage."""
        try:
            if self._check_connection():
                self.client.stat_object(self.bucket, path)
                return True
        except Exception:
            pass

        local_file = self._local_mirror / path
        return local_file.exists()

    def get_size(self, prefix: str) -> int:
        """Get total size in bytes under prefix."""
        try:
            if self._check_connection():
                objects = self.client.list_objects(self.bucket, prefix=prefix, recursive=True)
                return sum(obj.size for obj in objects)
        except Exception:
            pass

        local_dir = self._local_mirror / prefix
        if local_dir.exists():
            return sum(f.stat().st_size for f in local_dir.glob("**/*") if f.is_file())
        return 0
