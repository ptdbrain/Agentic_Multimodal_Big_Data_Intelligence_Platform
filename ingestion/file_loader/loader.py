import json
from pathlib import Path
from typing import List, Dict, Any, Union, Optional
import pandas as pd

class FileLoader:
    """Universal Multi-Format File Loader supporting CSV, TSV, JSON, JSONL, and Parquet.
    Demonstrates Big Data Variety handling with encoding fallbacks and robust schema parsing.
    """
    
    @staticmethod
    def load_file(
        file_path: Union[str, Path],
        encoding: Optional[str] = None,
        delimiter: Optional[str] = None
    ) -> pd.DataFrame:
        p = Path(file_path)
        if not p.exists():
            raise FileNotFoundError(f"File not found: {p}")
            
        ext = p.suffix.lower()

        # 1. Delimited text files (CSV, TSV, TXT)
        if ext in [".csv", ".tsv", ".txt"]:
            encodings = [encoding] if encoding else ["utf-8", "utf-8-sig", "latin1", "cp1252"]
            sep = delimiter if delimiter else ("\t" if ext == ".tsv" else None)
            for enc in encodings:
                try:
                    return pd.read_csv(p, encoding=enc, sep=sep, engine="python" if sep is None else "c")
                except (UnicodeDecodeError, Exception):
                    continue
            return pd.read_csv(p, errors="replace")

        # 2. JSON & JSONL
        elif ext in [".json", ".jsonl"]:
            with open(p, "r", encoding=encoding or "utf-8") as f:
                if ext == ".jsonl":
                    lines = [json.loads(line) for line in f if line.strip()]
                    return pd.DataFrame(lines)
                try:
                    data = json.load(f)
                except json.JSONDecodeError:
                    # Attempt line-by-line fallback
                    f.seek(0)
                    lines = [json.loads(line) for line in f if line.strip()]
                    return pd.DataFrame(lines)

            if isinstance(data, list):
                return pd.DataFrame(data)
            elif isinstance(data, dict):
                return pd.DataFrame([data])
            else:
                raise ValueError("Unsupported JSON structure")

        # 3. Parquet
        elif ext in [".parquet", ".pq"]:
            return pd.read_parquet(p)
        else:
            raise ValueError(f"Unsupported file format: {ext}")

    @classmethod
    def load_records(
        cls,
        file_path: Union[str, Path],
        encoding: Optional[str] = None,
        delimiter: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        df = cls.load_file(file_path, encoding=encoding, delimiter=delimiter)
        return df.to_dict(orient="records")

