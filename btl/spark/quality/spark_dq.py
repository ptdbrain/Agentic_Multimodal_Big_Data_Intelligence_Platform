import time
import datetime
from typing import Dict, Any, List, Optional, Union
import pandas as pd
from config.settings import settings
from storage.storage_manager import storage

try:
    from pyspark.sql import DataFrame
    from pyspark.sql import functions as F
    from pyspark.sql.types import DoubleType, BooleanType, StringType, TimestampType
    from pyspark.sql.window import Window
    HAS_PYSPARK = True
except ImportError:
    HAS_PYSPARK = False
    DataFrame = None


class DataQualityGateError(RuntimeError):
    """Raised when a dataset fails Data Quality threshold gates, blocking Gold mart construction."""
    pass


class SparkDataQualityEvaluator:
    """Production Spark Data Quality Evaluator with dual-engine support.
    - Separates field-level null statistics (field_null_counts, total_field_nulls) from
      row-level invalid record counts (invalid_count, quarantine_count, null_row_count).
    - Performs explicit type parsing/casting BEFORE validation so malformed type strings
      are caught as cast errors and row-level invalid records.
    - Provides configurable DQ Gate (check_dq_gate) that blocks Gold layer generation
      when DQ score, duplicate ratio, or missing ratio violates configured thresholds.
    """

    REQUIRED_FIELDS: Dict[str, List[str]] = {
        "reviews": ["review_id", "product_id", "rating", "review_text"],
        "products": ["product_id", "product_name", "price"],
        "prices": ["price_id", "product_id", "price"],
    }

    PRIMARY_KEYS: Dict[str, str] = {
        "reviews": "review_id",
        "products": "product_id",
        "prices": "price_id",
    }

    TYPE_CAST_SPECS: Dict[str, Dict[str, str]] = {
        "reviews": {
            "rating": "double",
            "verified_purchase": "boolean",
            "review_date": "timestamp",
        },
        "products": {
            "price": "double",
            "created_at": "timestamp",
        },
        "prices": {
            "price": "double",
            "timestamp": "timestamp",
        },
    }

    @classmethod
    def get_required_fields(
        cls,
        dataset_name: str,
        columns: List[str],
        required_fields: Optional[List[str]] = None
    ) -> List[str]:
        if required_fields is not None:
            return [c for c in required_fields if c in columns]
        if dataset_name in cls.REQUIRED_FIELDS:
            return [c for c in cls.REQUIRED_FIELDS[dataset_name] if c in columns]
        # Generic dataset fallback: all columns except internal metadata columns
        return [c for c in columns if not c.startswith("__")]

    @classmethod
    def evaluate(
        cls,
        df: Any,
        dataset_name: str,
        primary_key: Optional[str] = None,
        required_fields: Optional[List[str]] = None,
        type_casts: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        """Evaluates DataFrame quality metrics using Spark aggregations or pandas fallback.
        1. Computes field-level null statistics across all columns (field_null_counts).
        2. Performs type parsing/casting BEFORE validation (tracking cast_error_count).
        3. Evaluates row-level validity (missing required fields, cast errors, range violations,
           and primary-key duplicates) so valid_count + invalid_count == total_count.
        """
        t0 = time.time()

        # 1. PySpark DataFrame path
        if HAS_PYSPARK and isinstance(df, DataFrame):
            return cls._evaluate_spark(
                df=df,
                dataset_name=dataset_name,
                primary_key=primary_key,
                required_fields=required_fields,
                type_casts=type_casts,
                t0=t0,
            )

        # 2. Pandas DataFrame / list path
        pdf = df.copy() if isinstance(df, pd.DataFrame) else pd.DataFrame(df)
        return cls._evaluate_pandas(
            pdf=pdf,
            dataset_name=dataset_name,
            primary_key=primary_key,
            required_fields=required_fields,
            type_casts=type_casts,
            t0=t0,
        )

    @classmethod
    def _evaluate_spark(
        cls,
        df: "DataFrame",
        dataset_name: str,
        primary_key: Optional[str],
        required_fields: Optional[List[str]],
        type_casts: Optional[Dict[str, str]],
        t0: float,
    ) -> Dict[str, Any]:
        user_cols = [c for c in df.columns if not c.startswith("__")]
        total_count = df.count()
        if total_count == 0 or not user_cols:
            return {
                "dataset_name": dataset_name,
                "total_count": 0,
                "valid_count": 0,
                "invalid_count": 0,
                "quarantine_count": 0,
                "null_count": 0,
                "null_row_count": 0,
                "total_field_nulls": 0,
                "field_null_counts": {c: 0 for c in user_cols},
                "cast_error_count": 0,
                "range_error_count": 0,
                "duplicate_count": 0,
                "duplicate_ratio": 0.0,
                "missing_ratio": 0.0,
                "dq_score": 100.0,
                "processing_time_ms": max(1, int((time.time() - t0) * 1000)),
            }

        dtypes_map = dict(df.dtypes)

        def _is_missing_expr(col_name: str):
            c_expr = F.col(col_name)
            if dtypes_map.get(col_name) == "string":
                return c_expr.isNull() | (F.length(F.trim(c_expr)) == 0)
            if dtypes_map.get(col_name) in ("double", "float"):
                return c_expr.isNull() | F.isnan(c_expr)
            return c_expr.isNull()

        # Step 1: Field-level null statistics BEFORE type casting
        null_exprs = [
            F.sum(F.when(_is_missing_expr(c), 1).otherwise(0)).alias(c)
            for c in user_cols
        ]
        nulls_row = df.agg(*null_exprs).collect()[0]
        field_null_counts: Dict[str, int] = {
            c: int(nulls_row[c] or 0) for c in user_cols
        }
        total_field_nulls = int(sum(field_null_counts.values()))

        # Step 2: Missing required fields at row level (BEFORE type casting)
        req_cols = cls.get_required_fields(dataset_name, user_cols, required_fields)
        if req_cols:
            row_missing_cond = F.lit(False)
            for rc in req_cols:
                row_missing_cond = row_missing_cond | _is_missing_expr(rc)
        else:
            row_missing_cond = F.lit(False)

        # Step 3: Parse / Cast BEFORE validation
        cast_spec = dict(cls.TYPE_CAST_SPECS.get(dataset_name, {}))
        if type_casts:
            cast_spec.update(type_casts)

        work_df = df.withColumn("__row_missing_req", row_missing_cond)
        row_cast_err_cond = F.lit(False)

        for col_name, target_type in cast_spec.items():
            if col_name not in user_cols:
                continue
            pre_err_col = f"__cast_err_{col_name}"
            if pre_err_col in df.columns:
                row_cast_err_cond = row_cast_err_cond | F.coalesce(F.col(pre_err_col), F.lit(False))
            else:
                raw_present = ~_is_missing_expr(col_name)
                if target_type in ("double", "float"):
                    casted = F.col(col_name).cast(DoubleType())
                    err_expr = raw_present & (casted.isNull() | F.isnan(casted))
                    work_df = work_df.withColumn(pre_err_col, err_expr).withColumn(col_name, casted)
                    row_cast_err_cond = row_cast_err_cond | F.col(pre_err_col)
                elif target_type == "timestamp":
                    casted = F.to_timestamp(F.col(col_name).cast(StringType()))
                    err_expr = raw_present & casted.isNull()
                    work_df = work_df.withColumn(pre_err_col, err_expr).withColumn(col_name, casted)
                    row_cast_err_cond = row_cast_err_cond | F.col(pre_err_col)
                elif target_type in ("boolean", "bool"):
                    casted = F.col(col_name).cast(BooleanType())
                    err_expr = raw_present & casted.isNull()
                    work_df = work_df.withColumn(pre_err_col, err_expr).withColumn(col_name, casted)
                    row_cast_err_cond = row_cast_err_cond | F.col(pre_err_col)

        work_df = work_df.withColumn("__row_cast_err", row_cast_err_cond)

        # Step 4: Domain / Range validation on parsed/casted columns
        row_range_err_cond = F.lit(False)
        if dataset_name == "reviews" and "rating" in user_cols:
            row_range_err_cond = (
                F.col("rating").isNotNull()
                & (~F.isnan(F.col("rating")))
                & ((F.col("rating") < 1.0) | (F.col("rating") > 5.0))
            )
        elif dataset_name in ("products", "prices") and "price" in user_cols:
            row_range_err_cond = (
                F.col("price").isNotNull()
                & (~F.isnan(F.col("price")))
                & (F.col("price") <= 0.0)
            )

        work_df = work_df.withColumn("__row_range_err", row_range_err_cond)

        # Step 5: Primary key duplicate detection at row level
        pk = primary_key or cls.PRIMARY_KEYS.get(dataset_name) or user_cols[0]
        if pk in user_cols:
            w_pk = Window.partitionBy(pk).orderBy(F.monotonically_increasing_id())
            work_df = work_df.withColumn(
                "__row_is_dup",
                F.col(pk).isNotNull() & (F.row_number().over(w_pk) > 1)
            )
        else:
            w_all = Window.partitionBy(*user_cols).orderBy(F.monotonically_increasing_id())
            work_df = work_df.withColumn("__row_is_dup", F.row_number().over(w_all) > 1)

        work_df = work_df.withColumn(
            "__row_quarantined",
            F.col("__row_missing_req") | F.col("__row_cast_err") | F.col("__row_range_err")
        ).withColumn(
            "__row_invalid",
            F.col("__row_quarantined") | F.col("__row_is_dup")
        )

        summary = work_df.agg(
            F.sum(F.when(F.col("__row_missing_req"), 1).otherwise(0)).alias("null_row_count"),
            F.sum(F.when(F.col("__row_cast_err"), 1).otherwise(0)).alias("cast_error_count"),
            F.sum(F.when(F.col("__row_range_err"), 1).otherwise(0)).alias("range_error_count"),
            F.sum(F.when(F.col("__row_is_dup"), 1).otherwise(0)).alias("duplicate_count"),
            F.sum(F.when(F.col("__row_quarantined"), 1).otherwise(0)).alias("quarantine_count"),
            F.sum(F.when(F.col("__row_invalid"), 1).otherwise(0)).alias("invalid_count"),
        ).collect()[0]

        null_row_count = int(summary["null_row_count"] or 0)
        cast_error_count = int(summary["cast_error_count"] or 0)
        range_error_count = int(summary["range_error_count"] or 0)
        duplicate_count = int(summary["duplicate_count"] or 0)
        quarantine_count = int(summary["quarantine_count"] or 0)
        invalid_count = int(summary["invalid_count"] or 0)
        valid_count = max(0, total_count - invalid_count)
        dq_score = round((valid_count / total_count * 100.0), 2) if total_count > 0 else 100.0

        return {
            "dataset_name": dataset_name,
            "total_count": total_count,
            "valid_count": valid_count,
            "invalid_count": invalid_count,
            "quarantine_count": quarantine_count,
            "null_count": null_row_count,
            "null_row_count": null_row_count,
            "total_field_nulls": total_field_nulls,
            "field_null_counts": field_null_counts,
            "cast_error_count": cast_error_count,
            "range_error_count": range_error_count,
            "duplicate_count": duplicate_count,
            "duplicate_ratio": round(duplicate_count / total_count, 4),
            "missing_ratio": round(null_row_count / total_count, 4),
            "dq_score": dq_score,
            "processing_time_ms": max(1, int((time.time() - t0) * 1000)),
        }

    @classmethod
    def _evaluate_pandas(
        cls,
        pdf: pd.DataFrame,
        dataset_name: str,
        primary_key: Optional[str],
        required_fields: Optional[List[str]],
        type_casts: Optional[Dict[str, str]],
        t0: float,
    ) -> Dict[str, Any]:
        user_cols = [c for c in pdf.columns if not str(c).startswith("__")]
        total_count = len(pdf)
        if total_count == 0 or not user_cols:
            return {
                "dataset_name": dataset_name,
                "total_count": 0,
                "valid_count": 0,
                "invalid_count": 0,
                "quarantine_count": 0,
                "null_count": 0,
                "null_row_count": 0,
                "total_field_nulls": 0,
                "field_null_counts": {c: 0 for c in user_cols},
                "cast_error_count": 0,
                "range_error_count": 0,
                "duplicate_count": 0,
                "duplicate_ratio": 0.0,
                "missing_ratio": 0.0,
                "dq_score": 100.0,
                "processing_time_ms": 1,
            }

        def _series_missing_mask(s: pd.Series) -> pd.Series:
            mask = s.isna()
            if s.dtype == object:
                str_empty = s.dropna().astype(str).str.strip() == ""
                mask = mask | str_empty.reindex(s.index, fill_value=False)
            return mask

        # Step 1: Field-level null statistics across all columns BEFORE type casting
        missing_masks: Dict[str, pd.Series] = {
            c: _series_missing_mask(pdf[c]) for c in user_cols
        }
        field_null_counts: Dict[str, int] = {
            c: int(mask.sum()) for c, mask in missing_masks.items()
        }
        total_field_nulls = int(sum(field_null_counts.values()))

        # Step 2: Row-level missing required fields
        req_cols = cls.get_required_fields(dataset_name, user_cols, required_fields)
        if req_cols:
            row_missing_mask = pd.Series(False, index=pdf.index)
            for rc in req_cols:
                row_missing_mask = row_missing_mask | missing_masks[rc]
        else:
            row_missing_mask = pd.Series(False, index=pdf.index)

        # Step 3: Parse / Cast BEFORE validation
        cast_spec = dict(cls.TYPE_CAST_SPECS.get(dataset_name, {}))
        if type_casts:
            cast_spec.update(type_casts)

        casted_cols: Dict[str, pd.Series] = {}
        row_cast_err_mask = pd.Series(False, index=pdf.index)

        for col_name, target_type in cast_spec.items():
            if col_name not in user_cols:
                continue
            raw_present = ~missing_masks[col_name]
            if target_type in ("double", "float"):
                casted = pd.to_numeric(pdf[col_name], errors="coerce")
                cast_err = raw_present & casted.isna()
                casted_cols[col_name] = casted
                row_cast_err_mask = row_cast_err_mask | cast_err
            elif target_type == "timestamp":
                casted = pd.to_datetime(pdf[col_name], errors="coerce", utc=True)
                cast_err = raw_present & casted.isna()
                casted_cols[col_name] = casted
                row_cast_err_mask = row_cast_err_mask | cast_err
            elif target_type in ("boolean", "bool"):
                valid_bool_strs = {"true", "false", "1", "0", "yes", "no"}
                is_bool_val = pdf[col_name].apply(
                    lambda v: isinstance(v, bool) or (isinstance(v, str) and v.strip().lower() in valid_bool_strs)
                )
                cast_err = raw_present & (~is_bool_val)
                row_cast_err_mask = row_cast_err_mask | cast_err

        # Step 4: Domain / Range validation on parsed/casted values
        row_range_err_mask = pd.Series(False, index=pdf.index)
        if dataset_name == "reviews" and "rating" in user_cols:
            r_num = casted_cols.get("rating", pd.to_numeric(pdf["rating"], errors="coerce"))
            row_range_err_mask = r_num.notna() & ((r_num < 1.0) | (r_num > 5.0))
        elif dataset_name in ("products", "prices") and "price" in user_cols:
            p_num = casted_cols.get("price", pd.to_numeric(pdf["price"], errors="coerce"))
            row_range_err_mask = p_num.notna() & (p_num <= 0.0)

        # Step 5: Primary key duplicate detection at row level
        pk = primary_key or cls.PRIMARY_KEYS.get(dataset_name) or user_cols[0]
        if pk in user_cols:
            row_dup_mask = (~missing_masks[pk]) & pdf.duplicated(subset=[pk], keep="first")
        else:
            row_dup_mask = pdf.duplicated(subset=user_cols, keep="first")

        row_quarantined_mask = row_missing_mask | row_cast_err_mask | row_range_err_mask
        row_invalid_mask = row_quarantined_mask | row_dup_mask

        null_row_count = int(row_missing_mask.sum())
        cast_error_count = int(row_cast_err_mask.sum())
        range_error_count = int(row_range_err_mask.sum())
        duplicate_count = int(row_dup_mask.sum())
        quarantine_count = int(row_quarantined_mask.sum())
        invalid_count = int(row_invalid_mask.sum())
        valid_count = max(0, total_count - invalid_count)
        dq_score = round((valid_count / total_count * 100.0), 2) if total_count > 0 else 100.0

        return {
            "dataset_name": dataset_name,
            "total_count": total_count,
            "valid_count": valid_count,
            "invalid_count": invalid_count,
            "quarantine_count": quarantine_count,
            "null_count": null_row_count,
            "null_row_count": null_row_count,
            "total_field_nulls": total_field_nulls,
            "field_null_counts": field_null_counts,
            "cast_error_count": cast_error_count,
            "range_error_count": range_error_count,
            "duplicate_count": duplicate_count,
            "duplicate_ratio": round(duplicate_count / total_count, 4),
            "missing_ratio": round(null_row_count / total_count, 4),
            "dq_score": dq_score,
            "processing_time_ms": max(1, int((time.time() - t0) * 1000)),
        }

    @classmethod
    def check_dq_gate(
        cls,
        dq_metrics: Union[Dict[str, Any], List[Dict[str, Any]]],
        min_dq_score: Optional[float] = None,
        max_duplicate_ratio: Optional[float] = None,
        max_missing_ratio: Optional[float] = None,
        raise_on_failure: bool = True,
    ) -> Dict[str, Any]:
        """Evaluates DQ metrics against configured thresholds and optionally raises DataQualityGateError
        to block downstream Gold mart generation when data quality is insufficient.
        """
        eff_min_score = (
            float(min_dq_score)
            if min_dq_score is not None
            else float(settings.quality.min_dq_score)
        )
        eff_max_dup = (
            float(max_duplicate_ratio)
            if max_duplicate_ratio is not None
            else float(settings.quality.max_duplicate_ratio)
        )
        eff_max_missing = (
            float(max_missing_ratio)
            if max_missing_ratio is not None
            else float(settings.quality.max_missing_ratio)
        )

        items = [dq_metrics] if isinstance(dq_metrics, dict) else list(dq_metrics or [])
        violations: List[str] = []
        failed_datasets: List[str] = []

        for m in items:
            if not m:
                continue
            ds_name = str(m.get("dataset_name", "unknown"))
            total = int(m.get("total_count", m.get("records_received", 0)) or 0)
            if total == 0:
                continue

            score = float(m.get("dq_score", 0.0))
            dups = int(m.get("duplicate_count", m.get("records_duplicate", 0)) or 0)
            missing_rows = int(
                m.get("null_row_count", m.get("records_missing", m.get("null_count", 0))) or 0
            )
            dup_ratio = float(m.get("duplicate_ratio", dups / total if total > 0 else 0.0))
            miss_ratio = float(m.get("missing_ratio", missing_rows / total if total > 0 else 0.0))

            ds_failed = False
            if score < eff_min_score:
                violations.append(
                    f"[{ds_name}] dq_score={score:.2f}% < min_dq_score={eff_min_score:.2f}%"
                )
                ds_failed = True
            if dup_ratio > eff_max_dup:
                violations.append(
                    f"[{ds_name}] duplicate_ratio={dup_ratio:.4f} > max_duplicate_ratio={eff_max_dup:.4f}"
                )
                ds_failed = True
            if miss_ratio > eff_max_missing:
                violations.append(
                    f"[{ds_name}] missing_ratio={miss_ratio:.4f} > max_missing_ratio={eff_max_missing:.4f}"
                )
                ds_failed = True

            if ds_failed and ds_name not in failed_datasets:
                failed_datasets.append(ds_name)

        passed = len(violations) == 0
        result = {
            "passed": passed,
            "min_dq_score_threshold": eff_min_score,
            "max_duplicate_ratio_threshold": eff_max_dup,
            "max_missing_ratio_threshold": eff_max_missing,
            "failed_datasets": failed_datasets,
            "violations": violations,
        }

        if not passed and raise_on_failure:
            raise DataQualityGateError(
                f"Data Quality Gate blocked Gold generation! Violations: {'; '.join(violations)}"
            )

        return result

    @staticmethod
    def generate_report(metrics: Dict[str, Any], batch_id: Optional[str] = None) -> Dict[str, Any]:
        """Generates standard warehouse-compatible Data Quality record."""
        ds_name = metrics.get("dataset_name", "unknown")
        bid = batch_id or f"dq_{ds_name}"
        return {
            "batch_id": bid,
            "dataset_name": ds_name,
            "records_received": int(metrics.get("total_count", metrics.get("records_received", 0))),
            "records_valid": int(metrics.get("valid_count", metrics.get("records_valid", 0))),
            "records_invalid": int(metrics.get("invalid_count", metrics.get("records_invalid", 0))),
            "records_duplicate": int(metrics.get("duplicate_count", metrics.get("records_duplicate", 0))),
            "records_missing": int(
                metrics.get("null_row_count", metrics.get("records_missing", metrics.get("null_count", 0)))
            ),
            "dq_score": float(metrics.get("dq_score", 0.0)),
            "processing_time_ms": int(metrics.get("processing_time_ms", 10)),
            "timestamp": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
        }

    @staticmethod
    def save_reports_to_gold(reports: List[Dict[str, Any]]) -> str:
        """Persists DQ reports into Gold Layer Parquet (gold/data_quality_metrics)."""
        if not reports:
            return ""
        df = pd.DataFrame(reports)
        try:
            existing = storage.read_gold_parquet("data_quality_metrics")
            if not existing.empty:
                df = pd.concat([existing, df], ignore_index=True).drop_duplicates(
                    subset=["batch_id"], keep="last"
                )
        except FileNotFoundError:
            pass
        out_path = storage.write_gold_parquet("data_quality_metrics", df)
        return out_path

    @staticmethod
    def save_quarantine(dataset_name: str, invalid_records: Any) -> Optional[str]:
        """Routes invalid/failed records to Quarantine storage for auditing."""
        if invalid_records is None:
            return None
        pdf = (
            invalid_records
            if isinstance(invalid_records, pd.DataFrame)
            else pd.DataFrame(invalid_records)
        )
        if pdf.empty:
            return None
        now = datetime.datetime.now(datetime.timezone.utc)
        path = (
            f"quarantine/{dataset_name}/year={now.year}/month={now.month:02d}/"
            f"day={now.day:02d}/quarantine_{int(time.time())}.json"
        )
        storage.write_json(path, pdf.to_dict(orient="records"))
        return path
