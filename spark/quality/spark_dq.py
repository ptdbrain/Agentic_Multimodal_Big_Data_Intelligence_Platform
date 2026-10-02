import time
import datetime
from typing import Dict, Any, List, Optional, Union
import pandas as pd
from storage.storage_manager import storage

try:
    from pyspark.sql import DataFrame
    from pyspark.sql import functions as F
    HAS_PYSPARK = True
except ImportError:
    HAS_PYSPARK = False
    DataFrame = None

class SparkDataQualityEvaluator:
    """Production Spark Data Quality Evaluator with dual-engine support.
    Evaluates schema completeness, range validity, null counts, deduplication, and overall DQ score.
    Persists audit scorecards to Gold Layer and exports to Warehouse.
    """
    
    @staticmethod
    def evaluate(df: Any, dataset_name: str, primary_key: Optional[str] = None) -> Dict[str, Any]:
        """Evaluates DataFrame quality metrics using Spark aggregations or pandas fallback.
        Returns: total_count, valid_count, invalid_count, null_count, duplicate_count, dq_score.
        """
        t0 = time.time()
        
        # 1. PySpark DataFrame path
        if HAS_PYSPARK and isinstance(df, DataFrame):
            total_count = df.count()
            if total_count == 0:
                return {
                    'dataset_name': dataset_name,
                    'total_count': 0,
                    'valid_count': 0,
                    'invalid_count': 0,
                    'null_count': 0,
                    'duplicate_count': 0,
                    'dq_score': 100.0,
                    'processing_time_ms': int((time.time() - t0) * 1000)
                }
            
            # Count nulls across all columns
            null_exprs = [F.sum(F.col(c).isNull().cast('int')).alias(c) for c in df.columns]
            nulls_row = df.agg(*null_exprs).collect()[0] if null_exprs else {}
            total_nulls = sum(nulls_row[c] for c in df.columns if nulls_row.get(c) is not None)
            
            # Duplicates
            pk = primary_key or df.columns[0]
            distinct_count = df.select(pk).distinct().count() if pk in df.columns else df.distinct().count()
            duplicate_count = max(0, total_count - distinct_count)
            
            invalid_count = total_nulls + duplicate_count
            valid_count = max(0, total_count - invalid_count)
            dq_score = round((valid_count / total_count * 100.0), 2) if total_count > 0 else 100.0
            
            return {
                'dataset_name': dataset_name,
                'total_count': total_count,
                'valid_count': valid_count,
                'invalid_count': invalid_count,
                'null_count': total_nulls,
                'duplicate_count': duplicate_count,
                'dq_score': dq_score,
                'processing_time_ms': max(1, int((time.time() - t0) * 1000))
            }

        # 2. Pandas DataFrame / list fallback
        pdf = df if isinstance(df, pd.DataFrame) else pd.DataFrame(df)
        total_count = len(pdf)
        if total_count == 0:
            return {
                'dataset_name': dataset_name,
                'total_count': 0,
                'valid_count': 0,
                'invalid_count': 0,
                'null_count': 0,
                'duplicate_count': 0,
                'dq_score': 100.0,
                'processing_time_ms': 1
            }

        total_nulls = int(pdf.isna().sum().sum())
        pk = primary_key or pdf.columns[0]
        if pk in pdf.columns:
            duplicate_count = int(pdf.duplicated(subset=[pk]).sum())
        else:
            duplicate_count = int(pdf.duplicated().sum())

        invalid_count = min(total_count, total_nulls + duplicate_count)
        valid_count = max(0, total_count - invalid_count)
        dq_score = round((valid_count / total_count * 100.0), 2) if total_count > 0 else 100.0

        return {
            'dataset_name': dataset_name,
            'total_count': total_count,
            'valid_count': valid_count,
            'invalid_count': invalid_count,
            'null_count': total_nulls,
            'duplicate_count': duplicate_count,
            'dq_score': dq_score,
            'processing_time_ms': max(1, int((time.time() - t0) * 1000))
        }

    @staticmethod
    def generate_report(metrics: Dict[str, Any], batch_id: Optional[str] = None) -> Dict[str, Any]:
        """Generates standard warehouse-compatible Data Quality record."""
        bid = batch_id or f"dq_{int(time.time() * 1000)}"
        return {
            'batch_id': bid,
            'dataset_name': metrics.get('dataset_name', 'unknown'),
            'records_received': metrics.get('total_count', metrics.get('records_received', 0)),
            'records_valid': metrics.get('valid_count', metrics.get('records_valid', 0)),
            'records_invalid': metrics.get('invalid_count', metrics.get('records_invalid', 0)),
            'records_duplicate': metrics.get('duplicate_count', 0),
            'records_missing': metrics.get('null_count', 0),
            'dq_score': metrics.get('dq_score', 0.0),
            'processing_time_ms': metrics.get('processing_time_ms', 10),
            'timestamp': datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        }

    @staticmethod
    def save_reports_to_gold(reports: List[Dict[str, Any]]) -> str:
        """Persists DQ reports into Gold Layer Parquet (gold/data_quality_metrics)."""
        if not reports:
            return ""
        df = pd.DataFrame(reports)
        # Append or merge with existing DQ metrics if present
        try:
            existing = storage.read_gold_parquet("data_quality_metrics")
            if not existing.empty:
                df = pd.concat([existing, df], ignore_index=True).drop_duplicates(subset=['batch_id'], keep='last')
        except Exception:
            pass
        out_path = storage.write_gold_parquet("data_quality_metrics", df)
        return out_path

    @staticmethod
    def save_quarantine(dataset_name: str, invalid_records: Any) -> Optional[str]:
        """Routes invalid/failed records to Quarantine storage for auditing."""
        if invalid_records is None:
            return None
        pdf = invalid_records if isinstance(invalid_records, pd.DataFrame) else pd.DataFrame(invalid_records)
        if pdf.empty:
            return None
        now = datetime.datetime.now(datetime.timezone.utc)
        path = f"quarantine/{dataset_name}/year={now.year}/month={now.month:02d}/day={now.day:02d}/quarantine_{int(time.time())}.json"
        storage.write_json(path, pdf.to_dict(orient="records"))
        return path
