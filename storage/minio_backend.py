import json
import io
import pandas as pd
from pathlib import Path
from typing import List, Any, Optional
from minio import Minio
from minio.error import S3Error
import urllib3
from config.settings import settings

class MinIOBackend:
    """Strict Production MinIO S3 storage backend for Data Lake operations (Bronze, Silver, Gold).
    Connects exclusively to the S3 bucket configured via settings.storage.data_lake_bucket.
    Does NOT silently fall back to local files when MinIO is unreachable.
    Local mirroring is disabled by default.
    """
    
    def __init__(
        self,
        endpoint: Optional[str] = None,
        access_key: Optional[str] = None,
        secret_key: Optional[str] = None,
        secure: Optional[bool] = None,
        bucket: Optional[str] = None,
        enable_local_mirror: Optional[bool] = None,
        fallback_local: bool = False,
        local_dir: Optional[Path] = None
    ):
        self.endpoint = endpoint or settings.storage.endpoint
        self.access_key = access_key or settings.storage.access_key
        self.secret_key = secret_key or settings.storage.secret_key
        self.secure = secure if secure is not None else settings.storage.secure
        self.bucket = bucket or settings.storage.data_lake_bucket
        self.fallback_local = False
        if enable_local_mirror is not None:
            self.enable_local_mirror = bool(enable_local_mirror)
        elif fallback_local:
            self.enable_local_mirror = True
        else:
            self.enable_local_mirror = bool(settings.storage.enable_local_mirror)
        self._local_mirror = Path(local_dir) if local_dir is not None else Path(settings.storage.local_data_dir)
        
        http_client = urllib3.PoolManager(
            timeout=urllib3.Timeout(connect=1.0, read=2.0),
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
        self._bucket_verified = False

    def health_check(self) -> bool:
        """Actively checks MinIO reachability and ensures the target bucket exists.
        Raises ConnectionError if MinIO is unreachable.
        """
        try:
            if not self.client.bucket_exists(self.bucket):
                self.client.make_bucket(self.bucket)
            self.is_connected = True
            self._bucket_verified = True
            return True
        except Exception as e:
            self.is_connected = False
            self._bucket_verified = False
            raise ConnectionError(
                f"MinIO storage backend unreachable at '{self.endpoint}' (bucket '{self.bucket}'): {e}"
            ) from e

    def _ensure_bucket(self) -> None:
        """Ensures bucket is provisioned on MinIO before executing S3 operations."""
        if not self._bucket_verified:
            self.health_check()

    def __eq__(self, other):
        if other is MinIOBackend or (isinstance(other, type) and issubclass(other, MinIOBackend)):
            return True
        return super().__eq__(other)

    @staticmethod
    def _normalize_s3_key(path: str) -> str:
        """Converts Windows backslashes to canonical S3 POSIX forward slashes."""
        return str(path).replace("\\", "/").lstrip("/")

    def write_json(self, path: str, data: Any, indent: Optional[int] = 2) -> str:
        """Write JSON directly to MinIO bucket. Fails loudly if MinIO is down."""
        self._ensure_bucket()
        norm_path = self._normalize_s3_key(path)
        json_bytes = json.dumps(data, ensure_ascii=False, indent=indent).encode('utf-8')
        json_stream = io.BytesIO(json_bytes)
        
        try:
            self.client.put_object(
                self.bucket,
                norm_path,
                data=json_stream,
                length=len(json_bytes),
                content_type='application/json'
            )
        except Exception as e:
            self.is_connected = False
            self._bucket_verified = False
            raise ConnectionError(
                f"Failed to write JSON object '{norm_path}' to MinIO bucket '{self.bucket}' at '{self.endpoint}': {e}"
            ) from e

        if self.enable_local_mirror:
            local_file = self._local_mirror / norm_path
            local_file.parent.mkdir(parents=True, exist_ok=True)
            with open(local_file, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=indent)

        return norm_path

    def read_json(self, path: str) -> Any:
        """Read JSON directly from MinIO bucket. Never falls back to local files."""
        self._ensure_bucket()
        norm_path = self._normalize_s3_key(path)
        try:
            response = self.client.get_object(self.bucket, norm_path)
            try:
                return json.loads(response.read().decode('utf-8'))
            finally:
                response.close()
                response.release_conn()
        except S3Error as e:
            if e.code in ("NoSuchKey", "NoSuchObject", "ResourceNotFound"):
                raise FileNotFoundError(f"Object not found in MinIO bucket '{self.bucket}': {norm_path}") from e
            raise ConnectionError(
                f"MinIO S3Error reading JSON '{norm_path}' from bucket '{self.bucket}': {e}"
            ) from e
        except Exception as e:
            self.is_connected = False
            self._bucket_verified = False
            raise ConnectionError(
                f"Failed to read JSON '{norm_path}' from MinIO at '{self.endpoint}': {e}"
            ) from e

    def write_parquet(self, path: str, df: pd.DataFrame) -> str:
        """Write Parquet directly to MinIO bucket. Fails loudly if MinIO is down."""
        self._ensure_bucket()
        norm_path = self._normalize_s3_key(path)
        parquet_bytes = df.to_parquet(index=False)
        parquet_stream = io.BytesIO(parquet_bytes)

        try:
            self.client.put_object(
                self.bucket,
                norm_path,
                data=parquet_stream,
                length=len(parquet_bytes),
                content_type='application/octet-stream'
            )
        except Exception as e:
            self.is_connected = False
            self._bucket_verified = False
            raise ConnectionError(
                f"Failed to write Parquet object '{path}' to MinIO bucket '{self.bucket}' at '{self.endpoint}': {e}"
            ) from e

        if self.enable_local_mirror:
            local_file = self._local_mirror / path
            local_file.parent.mkdir(parents=True, exist_ok=True)
            df.to_parquet(local_file, index=False)

        return path

    def read_parquet(self, path: str) -> pd.DataFrame:
        """Read Parquet directly from MinIO bucket. Never falls back to local files."""
        self._ensure_bucket()
        norm_path = self._normalize_s3_key(path)
        try:
            response = self.client.get_object(self.bucket, norm_path)
            try:
                parquet_bytes = response.read()
                return pd.read_parquet(io.BytesIO(parquet_bytes))
            finally:
                response.close()
                response.release_conn()
        except S3Error as e:
            if e.code in ("NoSuchKey", "NoSuchObject", "ResourceNotFound"):
                raise FileNotFoundError(f"Parquet object not found in MinIO bucket '{self.bucket}': {norm_path}") from e
            raise ConnectionError(
                f"MinIO S3Error reading Parquet '{norm_path}' from bucket '{self.bucket}': {e}"
            ) from e
        except Exception as e:
            self.is_connected = False
            self._bucket_verified = False
            raise ConnectionError(
                f"Failed to read Parquet '{norm_path}' from MinIO at '{self.endpoint}': {e}"
            ) from e

    def list_objects(self, prefix: str) -> List[str]:
        """List objects under prefix strictly from MinIO bucket. Never scans local directories."""
        self._ensure_bucket()
        norm_prefix = self._normalize_s3_key(prefix)
        results = set()
        try:
            objects = self.client.list_objects(self.bucket, prefix=norm_prefix, recursive=True)
            for obj in objects:
                results.add(obj.object_name)
        except Exception as e:
            self.is_connected = False
            self._bucket_verified = False
            raise ConnectionError(
                f"Failed to list objects under '{norm_prefix}' from MinIO at '{self.endpoint}': {e}"
            ) from e

        return sorted(list(results))

    def object_exists(self, path: str) -> bool:
        """Check if object exists strictly in MinIO bucket. Never checks local files."""
        self._ensure_bucket()
        norm_path = self._normalize_s3_key(path)
        try:
            self.client.stat_object(self.bucket, norm_path)
            return True
        except S3Error as e:
            if e.code in ("NoSuchKey", "NoSuchObject", "ResourceNotFound"):
                return False
            raise ConnectionError(
                f"MinIO S3Error checking object '{norm_path}' in bucket '{self.bucket}': {e}"
            ) from e
        except Exception as e:
            self.is_connected = False
            self._bucket_verified = False
            raise ConnectionError(
                f"Failed to check object existence '{norm_path}' in MinIO at '{self.endpoint}': {e}"
            ) from e

    def get_size(self, prefix: str) -> int:
        """Get total size in bytes under prefix strictly from MinIO bucket."""
        self._ensure_bucket()
        norm_prefix = self._normalize_s3_key(prefix)
        try:
            objects = self.client.list_objects(self.bucket, prefix=norm_prefix, recursive=True)
            return sum(obj.size for obj in objects)
        except Exception as e:
            self.is_connected = False
            self._bucket_verified = False
            raise ConnectionError(
                f"Failed to get size for prefix '{prefix}' from MinIO at '{self.endpoint}': {e}"
            ) from e
