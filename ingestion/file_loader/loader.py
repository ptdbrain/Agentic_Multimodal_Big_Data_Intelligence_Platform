import json
from pathlib import Path
from typing import List, Dict, Any, Union
import pandas as pd

class FileLoader:
    """Universal Multi-Format File Loader supporting CSV, JSON, and Parquet.
    Demonstrates Big Data Variety handling.
    """
    
    @staticmethod
    def load_file(file_path: Union[str, Path]) -> pd.DataFrame:
        p = Path(file_path)
        if not p.exists():
            raise FileNotFoundError(f"File not found: {p}")
            
        ext = p.suffix.lower()
        if ext == ".csv":
            return pd.read_csv(p)
        elif ext == ".json":
            with open(p, "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, list):
                return pd.DataFrame(data)
            elif isinstance(data, dict):
                return pd.DataFrame([data])
            else:
                raise ValueError("Unsupported JSON structure")
        elif ext in [".parquet", ".pq"]:
            return pd.read_parquet(p)
        else:
            raise ValueError(f"Unsupported file format: {ext}")

    @staticmethod
    def load_records(file_path: Union[str, Path]) -> List[Dict[str, Any]]:
        df = FileLoader.load_file(file_path)
        return df.to_dict(orient="records")
