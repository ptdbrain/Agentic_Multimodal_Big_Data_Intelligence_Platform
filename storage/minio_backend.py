import json
import io
import pandas as pd
from typing import List, Dict, Any, Optional
from minio import Minio
from minio.error import S3Error
from config.settings import settings

class MinIOBackend:
    """Real MinIO S3 storage backend for Data Lake operations."""
    
    def __init__(self, endpoint: str, access_key: str, secret_key: str, secure: bool = False, bucket: str = 'sentinel-data'):
        self.client = Minio(
            endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=secure
        )
        self.bucket = bucket
        
        # Auto-create bucket if not exists
        try:
            if not self.client.bucket_exists(self.bucket):
                self.client.make_bucket(self.bucket)
        except S3Error as e:
            print(f"MinIO initialization error: {e}")
            raise
    
    def write_json(self, path: str, data: Any) -> str:
        """Write JSON to MinIO."""
        json_bytes = json.dumps(data, ensure_ascii=False).encode('utf-8')
        json_stream = io.BytesIO(json_bytes)
        
        self.client.put_object(
            self.bucket,
            path,
            data=json_stream,
            length=len(json_bytes),
            content_type='application/json'
        )
        return path
    
    def read_json(self, path: str) -> Any:
        """Read JSON from MinIO."""
        try:
            response = self.client.get_object(self.bucket, path)
            return json.loads(response.read().decode('utf-8'))
        except S3Error as e:
            print(f"Error reading JSON from MinIO ({path}): {e}")
            raise
        finally:
            if 'response' in locals() and hasattr(response, 'close'):
                response.close()
                
    def write_parquet(self, path: str, df: pd.DataFrame) -> str:
        """Write Parquet to MinIO."""
        parquet_bytes = df.to_parquet(index=False)
        parquet_stream = io.BytesIO(parquet_bytes)
        
        self.client.put_object(
            self.bucket,
            path,
            data=parquet_stream,
            length=len(parquet_bytes),
            content_type='application/octet-stream'
        )
        return path
    
    def read_parquet(self, path: str) -> pd.DataFrame:
        """Read Parquet from MinIO."""
        try:
            response = self.client.get_object(self.bucket, path)
            parquet_bytes = response.read()
            return pd.read_parquet(io.BytesIO(parquet_bytes))
        except S3Error as e:
            print(f"Error reading Parquet from MinIO ({path}): {e}")
            raise
        finally:
            if 'response' in locals() and hasattr(response, 'close'):
                response.close()
                
    def list_objects(self, prefix: str) -> List[str]:
        """List objects under prefix."""
        try:
            objects = self.client.list_objects(self.bucket, prefix=prefix, recursive=True)
            return [obj.object_name for obj in objects]
        except S3Error as e:
            print(f"Error listing objects in MinIO ({prefix}): {e}")
            return []
            
    def object_exists(self, path: str) -> bool:
        """Check if object exists."""
        try:
            self.client.stat_object(self.bucket, path)
            return True
        except S3Error:
            return False
            
    def get_size(self, prefix: str) -> int:
        """Get total size under prefix."""
        try:
            objects = self.client.list_objects(self.bucket, prefix=prefix, recursive=True)
            return sum(obj.size for obj in objects)
        except S3Error:
            return 0
