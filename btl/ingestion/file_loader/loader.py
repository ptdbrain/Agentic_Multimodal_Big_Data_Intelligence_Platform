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
            encodings = [encoding] if encoding else ["utf-8-sig", "cp1252", "latin1"]
            sep = delimiter if delimiter else ("\t" if ext == ".tsv" else None)
            identity_types = {name: 'string' for name in
                              ('product_id', 'review_id', 'price_id', 'user_id', 'article_id', 'source_record_id')}
            for index, enc in enumerate(encodings):
                try:
                    df = pd.read_csv(p, encoding=enc, sep=sep, dtype=identity_types,
                                     engine="python" if sep is None else "c")
                    break
                except UnicodeDecodeError:
                    if index == len(encodings) - 1:
                        raise

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
        path = Path(file_path)
        if path.suffix.lower() in ('.json', '.jsonl'):
            # JSON records are heterogeneous objects, not rows in a rectangular
            # table. A DataFrame invents NaN keys and can round large integers.
            with path.open(encoding=encoding or 'utf-8-sig') as stream:
                if path.suffix.lower() == '.jsonl':
                    records = [json.loads(line) for line in stream if line.strip()]
                else:
                    try:
                        value = json.load(stream)
                        records = value if isinstance(value, list) else [value]
                    except json.JSONDecodeError:
                        stream.seek(0)
                        records = [json.loads(line) for line in stream if line.strip()]
            if not all(isinstance(record, dict) for record in records):
                raise ValueError('JSON input must contain objects')
            return sanitize_records_pii(records, remove_user_id=remove_user_id) if anonymize_pii else records
        df = cls.load_file(
            file_path,
            encoding=encoding,
            delimiter=delimiter,
            anonymize_pii=False,
        )
        # Delimited/columnar sources have rectangular missing cells. Do not send
        # pandas NaN as an observed optional value (or non-standard JSON token).
        records = [{key: value for key, value in row.items()
                    if not (pd.api.types.is_scalar(value) and pd.isna(value))}
                   for row in df.to_dict(orient="records")]
        if anonymize_pii:
            return sanitize_records_pii(records, remove_user_id=remove_user_id)
        return records

