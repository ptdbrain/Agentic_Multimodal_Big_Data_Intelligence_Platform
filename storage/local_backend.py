import os
import json
import pandas as pd
from pathlib import Path
from typing import List, Any

class LocalBackend:
    """Local filesystem storage backend for tests and offline mode."""
    
    def __init__(self, base_dir: str):
        self.base_dir = Path(base_dir)
        self.base_dir.mkdir(parents=True, exist_ok=True)
    
    def _get_full_path(self, path: str) -> Path:
        return self.base_dir / path
        
    def write_json(self, path: str, data: Any) -> str:
        """Write JSON to local filesystem."""
        full_path = self._get_full_path(path)
        full_path.parent.mkdir(parents=True, exist_ok=True)
        with open(full_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False)
        return path
    
    def read_json(self, path: str) -> Any:
        """Read JSON from local filesystem."""
        full_path = self._get_full_path(path)
        if not full_path.exists():
            raise FileNotFoundError(f"File not found: {full_path}")
        with open(full_path, "r", encoding="utf-8") as f:
            return json.load(f)
            
    def write_parquet(self, path: str, df: pd.DataFrame) -> str:
        """Write Parquet to local filesystem."""
        full_path = self._get_full_path(path)
        full_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(full_path, index=False)
        return path
        
    def read_parquet(self, path: str) -> pd.DataFrame:
        """Read Parquet from local filesystem."""
        full_path = self._get_full_path(path)
        if not full_path.exists():
            raise FileNotFoundError(f"File not found: {full_path}")
        return pd.read_parquet(full_path)
        
    def list_objects(self, prefix: str) -> List[str]:
        """List objects under prefix."""
        full_prefix = self._get_full_path(prefix)
        if not full_prefix.exists():
            return []
            
        objects = []
        if full_prefix.is_file():
            # In case prefix points to a file directly, although unlikely based on usage pattern
            try:
                rel_path = full_prefix.relative_to(self.base_dir).as_posix()
                return [rel_path]
            except ValueError:
                return []

        # It's a directory (or we assume we list contents of dir matching prefix)
        # Actually, in S3, prefix can be part of file name, but local it usually maps to dir.
        # S3 lists all files whose key starts with prefix. For local we can list all files 
        # inside the directory that corresponds to prefix if it is a directory.
        # A safer approach for local backend acting like S3:
        search_dir = full_prefix if full_prefix.is_dir() else full_prefix.parent
        if not search_dir.exists():
            return []
            
        for root, dirs, files in os.walk(search_dir):
            for file in files:
                file_path = Path(root) / file
                try:
                    rel_path = file_path.relative_to(self.base_dir).as_posix()
                    if rel_path.startswith(prefix.replace('\\', '/')):
                        objects.append(rel_path)
                except ValueError:
                    continue
        return objects
        
    def object_exists(self, path: str) -> bool:
        """Check if object exists."""
        full_path = self._get_full_path(path)
        return full_path.exists()
        
    def get_size(self, prefix: str) -> int:
        """Get total size under prefix."""
        objects = self.list_objects(prefix)
        total_size = 0
        for obj in objects:
            full_path = self._get_full_path(obj)
            if full_path.exists():
                total_size += full_path.stat().st_size
        return total_size
