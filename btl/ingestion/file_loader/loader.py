import json
from pathlib import Path
from typing import List, Dict, Any, Union, Optional
import pandas as pd
from ingestion.privacy import sanitize_records_pii

class FileLoader:
    """Universal Multi-Format File Loader supporting CSV, TSV, JSON, JSONL, and Parquet.
    Demonstrates Big Data Variety handling with encoding fallbacks, robust schema parsing,
    and ingest-time PII anonymization (`user_id` hashing & raw PII removal).
    """
    
    @staticmethod
    def load_file(
        file_path: Union[str, Path],
        encoding: Optional[str] = None,
        delimiter: Optional[str] = None,
        anonymize_pii: bool = True,
        remove_user_id: bool = False,
    ) -> pd.DataFrame:
        p = Path(file_path)
        if not p.exists():
            raise FileNotFoundError(f"File not found: {p}")
            
        ext = p.suffix.lower()
        df: pd.DataFrame

        # 1. Delimited text files (CSV, TSV, TXT)
        if ext in [".csv", ".tsv", ".txt"]:
            encodings = [encoding] if encoding else ["utf-8", "utf-8-sig", "latin1", "cp1252"]
            sep = delimiter if delimiter else ("\t" if ext == ".tsv" else None)
            loaded_df = None
            for enc in encodings:
                try:
                    loaded_df = pd.read_csv(p, encoding=enc, sep=sep, engine="python" if sep is None else "c")
                    break
                except (UnicodeDecodeError, Exception):
                    continue
            df = loaded_df if loaded_df is not None else pd.read_csv(p, errors="replace")

        # 2. JSON & JSONL
        elif ext in [".json", ".jsonl"]:
            with open(p, "r", encoding=encoding or "utf-8") as f:
                if ext == ".jsonl":
                    lines = [json.loads(line) for line in f if line.strip()]
                    df = pd.DataFrame(lines)
                else:
                    try:
                        data = json.load(f)
                        if isinstance(data, list):
                            df = pd.DataFrame(data)
                        elif isinstance(data, dict):
                            df = pd.DataFrame([data])
                        else:
                            raise ValueError("Unsupported JSON structure")
                    except json.JSONDecodeError:
                        # Attempt line-by-line fallback
                        f.seek(0)
                        lines = [json.loads(line) for line in f if line.strip()]
                        df = pd.DataFrame(lines)

        # 3. Parquet
        elif ext in [".parquet", ".pq"]:
            df = pd.read_parquet(p)
        else:
            raise ValueError(f"Unsupported file format: {ext}")

        if anonymize_pii and not df.empty:
            records = sanitize_records_pii(
                df.to_dict(orient="records"),
                remove_user_id=remove_user_id,
            )
            return pd.DataFrame(records)
        return df

    @classmethod
    def load_records(
        cls,
        file_path: Union[str, Path],
        encoding: Optional[str] = None,
        delimiter: Optional[str] = None,
        anonymize_pii: bool = True,
        remove_user_id: bool = False,
    ) -> List[Dict[str, Any]]:
        df = cls.load_file(
            file_path,
            encoding=encoding,
            delimiter=delimiter,
            anonymize_pii=False,
        )
        records = df.to_dict(orient="records")
        if anonymize_pii:
            return sanitize_records_pii(records, remove_user_id=remove_user_id)
        return records

