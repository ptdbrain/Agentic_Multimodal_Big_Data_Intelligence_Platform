"""Spark Batch ETL Bronze -> Silver Engine.
Reads Bronze raw JSON via object storage (S3A), unpacks message envelopes,
standardizes data types, validates against quality rules, routes bad records to quarantine,
and applies cross-batch deduplication before persisting to Silver Parquet.

Zero toPandas() in dataset processing; strict Spark DataFrame execution with mathematical reconciliation.
"""
import os
import time
import uuid
import json
import tempfile
from pathlib import Path
from typing import List, Dict, Any, Tuple, Optional, Union
from config.settings import settings
from spark.spark_session import SparkSessionFactory
from spark.storage_safe import SafeParquetIO
from storage.write_lock import synchronized_path

try:
    from pyspark.sql import SparkSession, DataFrame
    from pyspark.sql import functions as F
    from pyspark.sql.types import (
        StructType, StructField, StringType, DoubleType, LongType,
        BooleanType, TimestampType, IntegerType
    )
    from pyspark.sql.window import Window
    HAS_PYSPARK = True
except ImportError:
    HAS_PYSPARK = False
    SparkSession = None
    DataFrame = None


class SparkBatchETLJob:
    """Production Spark DataFrame-based Bronze -> Silver Batch ETL Pipeline.
    Strictly performs all filtering, validation, deduplication, and persistence
    using native Spark DataFrame operations without converting to Pandas.
    """

    PRIMARY_KEYS = {
        "reviews": "review_id",
        "products": "product_id",
        "prices": "price_id"
    }

    ORDER_COLUMNS = {
        "reviews": "ingested_at",
        "products": "ingested_at",
        "prices": "timestamp"
    }

    def __init__(self, spark: Optional[SparkSession] = None):
        if not HAS_PYSPARK:
            raise RuntimeError("PySpark is not installed or available in this environment.")
        self._spark = spark or SparkSessionFactory.get_spark_session()
        if self._spark is None:
            raise RuntimeError("Failed to obtain SparkSession from SparkSessionFactory.")

    @property
    def spark(self) -> SparkSession:
        return self._spark

    def read_bronze_json(self, source: Union[str, Path, List[Union[str, Path, Dict[str, Any]]]]) -> DataFrame:
        """Reads raw JSON records from Bronze layer (MinIO S3A / file path / path list / record list)
        into a Spark DataFrame and unpacks canonical message envelope.
        """
        empty_schema = StructType([
            StructField("event_id", StringType(), True),
            StructField("event_type", StringType(), True),
            StructField("source", StringType(), True),
            StructField("ingested_at", StringType(), True),
            StructField("payload", StringType(), True)
        ])

        if isinstance(source, list) and not source:
            return self.spark.createDataFrame([], empty_schema)
        if isinstance(source, list) and not all(isinstance(item, (str, Path)) for item in source):
            temp_dir = tempfile.mkdtemp(prefix="spark_bronze_tmp_")
            temp_file = Path(temp_dir) / "batch.json"
            with temp_file.open('w', encoding='utf-8') as stream:
                json.dump(source, stream, ensure_ascii=False)
            sources = [str(temp_file)]
        else:
            sources = [str(item) for item in source] if isinstance(source, list) else [str(source)]

        # Discover files through Hadoop for both local and remote storage. A single
        # multiline=True read silently consumes only the first object of JSONL.
        groups = {True: [], False: []}
        conf = self.spark.sparkContext._jsc.hadoopConfiguration()
        PathClass = self.spark.sparkContext._gateway.jvm.org.apache.hadoop.fs.Path
        for source_path in sources:
            if not (source_path.startswith(('s3a://', 'file://', 'hdfs://')) or Path(source_path).exists()):
                source_path = SparkSessionFactory.resolve_storage_path(source_path, layer="bronze")
            path = PathClass(source_path)
            fs = path.getFileSystem(conf)
            if not fs.exists(path):
                raise FileNotFoundError(f'Bronze source not found: {source_path}')
            files = []
            if fs.isDirectory(path):
                iterator = fs.listFiles(path, True)
                while iterator.hasNext():
                    files.append(iterator.next().getPath())
            else:
                files.append(path)
            for file_path in files:
                name = file_path.getName()
                if name.startswith(('.', '_')):
                    continue
                multiline = not name.lower().endswith(('.jsonl', '.ndjson', '.jsonl.gz', '.ndjson.gz'))
                groups[multiline].append(file_path.toString())
        frames = []
        for multiline, paths in groups.items():
            if paths:
                frames.append(self.spark.read.option('multiline', str(multiline).lower())
                              .option('mode', 'FAILFAST').json(sorted(paths)))
        if not frames:
            return self.spark.createDataFrame([], empty_schema)
        df = frames[0]
        for other in frames[1:]:
            df = df.unionByName(other, allowMissingColumns=True)
        return self.unpack_envelope(df)

    def unpack_envelope(self, df: DataFrame) -> DataFrame:
        """Extracts fields from the message envelope (event_id, event_type, source, ingested_at)
        and flattens the nested 'payload' struct into top-level columns without duplicate column ambiguity.
        """
        schema_fields = {field.name: field.dataType for field in df.schema.fields}

        # If payload is a struct, extract payload.* and coalesce any overlapping envelope columns
        if "payload" in schema_fields and isinstance(schema_fields["payload"], StructType):
            payload_fields = [f.name for f in schema_fields["payload"].fields]
            payload_field_set = set(payload_fields)
            envelope_keys = ["event_id", "event_type", "source", "ingested_at"]

            select_exprs = []
            for c in envelope_keys:
                if c in df.columns and c not in payload_field_set:
                    select_exprs.append(F.col(c))

            for pf in payload_fields:
                if pf in df.columns and pf in envelope_keys:
                    select_exprs.append(F.coalesce(F.col(f"payload.{pf}"), F.col(pf)).alias(pf))
                else:
                    select_exprs.append(F.col(f"payload.{pf}").alias(pf))

            df = df.select(*select_exprs)
        elif "payload" in schema_fields and isinstance(schema_fields["payload"], StringType):
            # Payload was stringified JSON, leave as is
            pass

        return df

    REQUIRED_COLUMNS = {
        "reviews": ["review_id", "product_id", "rating", "review_text"],
        "products": ["product_id", "product_name", "price"],
        "prices": ["price_id", "product_id", "price"]
    }

    @classmethod
    def _with_required_columns(cls, df: DataFrame, dataset_type: str) -> DataFrame:
        for name in cls.REQUIRED_COLUMNS.get(dataset_type, []):
            if name not in df.columns:
                df = df.withColumn(name, F.lit(None).cast(StringType()))
        return df

    def compute_field_null_stats(self, df: DataFrame, dataset_type: str) -> Dict[str, Any]:
        """Computes field-level null/empty statistics across all columns (field_null_counts)
        separately from row-level missing required field count (null_row_count).
        Must be called BEFORE type casting so type cast failures are not conflated with nulls.
        """
        df = self._with_required_columns(df, dataset_type)
        user_cols = [c for c in df.columns if not c.startswith("__")]
        if not user_cols:
            return {"field_null_counts": {}, "total_field_nulls": 0, "null_row_count": 0}

        dtypes_map = dict(df.dtypes)

        def _is_null_or_empty(c_name: str):
            col_expr = F.col(c_name)
            if dtypes_map.get(c_name) == "string":
                return col_expr.isNull() | (F.length(F.trim(col_expr)) == 0)
            if dtypes_map.get(c_name) in ("double", "float"):
                return col_expr.isNull() | F.isnan(col_expr)
            return col_expr.isNull()

        req_cols = [c for c in self.REQUIRED_COLUMNS.get(dataset_type, user_cols) if c in user_cols]
        row_missing_cond = F.lit(False)
        for rc in req_cols:
            row_missing_cond = row_missing_cond | _is_null_or_empty(rc)

        agg_exprs = [
            F.sum(F.when(_is_null_or_empty(c), 1).otherwise(0)).alias(c)
            for c in user_cols
        ]
        agg_exprs.append(F.sum(F.when(row_missing_cond, 1).otherwise(0)).alias("__null_row_count"))

        row = df.agg(*agg_exprs).collect()[0]
        field_null_counts = {c: int(row[c] or 0) for c in user_cols}
        return {
            "field_null_counts": field_null_counts,
            "total_field_nulls": int(sum(field_null_counts.values())),
            "null_row_count": int(row["__null_row_count"] or 0),
        }

    def parse_and_cast(self, df: DataFrame, dataset_type: str) -> DataFrame:
        """Explicitly parses and casts raw columns to target data types BEFORE validation,
        recording any non-null raw values that fail type conversion in __cast_err_* columns.
        """
        return self.standardize_schema(df, dataset_type)

    @staticmethod
    def _parse_price(value: Any) -> Any:
        """Parse the supported US/Vietnamese/EU formats using distributed SQL expressions."""
        from spark.etl.normalizer import PRICE_TEXT_PATTERN
        raw = F.trim(value.cast(StringType()))
        clean = F.regexp_replace(raw, r"[^0-9.,]", "")
        last_separator = F.regexp_extract(clean, r"([.,])[^.,]*$", 1)
        commas, dots = F.split(clean, ","), F.split(clean, r"\.")
        parsed_text = (F.when(clean.contains(",") & clean.contains("."),
                              F.when(last_separator == ".", F.regexp_replace(clean, ",", ""))
                               .otherwise(F.regexp_replace(F.regexp_replace(clean, r"\.", ""), ",", ".")))
                       .when(clean.contains(","),
                             F.when((F.size(commas) == 2) & F.length(commas[1]).isin(1, 2),
                                    F.regexp_replace(clean, ",", "."))
                              .otherwise(F.regexp_replace(clean, ",", "")))
                       .when(clean.contains("."),
                             F.when((F.size(dots) > 2) | ((F.size(dots) == 2) & (F.length(dots[1]) == 3) & (F.length(dots[0]) <= 3)),
                                    F.regexp_replace(clean, r"\.", "")).otherwise(clean))
                       .otherwise(clean))
        number = F.when(raw.rlike(r"^[+-]?\d+(?:\.\d+)?[eE][+-]?\d+$"), raw.cast(DoubleType())).otherwise(
            parsed_text.cast(DoubleType()) * F.when(raw.contains("-"), -1).otherwise(1))
        return F.when(raw.rlike(PRICE_TEXT_PATTERN), number)

    @staticmethod
    def _parse_timestamp(value: Any) -> Any:
        raw = F.trim(value.cast(StringType()))
        iso = F.regexp_replace(raw, r"^(\d{4})/(\d{2})/(\d{2})", "$1-$2-$3")
        numeric = raw.cast(DoubleType())
        seconds = F.when(F.abs(numeric) > 1e11, numeric / 1000).otherwise(numeric)
        # Match Python datetime's representable UTC years, 1 through 9999.
        epoch = F.when(seconds.between(-62135596800, 253402300799), F.timestamp_seconds(seconds))
        return F.when(raw.rlike(r"^[+-]?\d+(?:\.\d+)?$"), epoch).otherwise(F.coalesce(
            F.try_to_timestamp(iso),
            F.try_to_timestamp(raw, F.lit("dd/MM/yyyy HH:mm:ss")),
            F.try_to_timestamp(raw, F.lit("dd/MM/yyyy")),
        ))

    def standardize_schema(self, df: DataFrame, dataset_type: str) -> DataFrame:
        """Normalize without repairing invalid values or replacing unavailable clocks."""
        df = self._with_required_columns(df, dataset_type)
        business_field = {"reviews": "review_date", "products": "created_at", "prices": "timestamp"}[dataset_type]
        numeric_field = "rating" if dataset_type == "reviews" else "price"
        time_fields = {business_field, "ingested_at"} | (set(df.columns) & {"created_at", "updated_at", "timestamp", "review_date"})
        raw_fields = time_fields | {numeric_field}
        # Capture before trimming; retain earlier raw evidence during replay.
        for field in raw_fields:
            if f"{field}_raw" not in df.columns:
                value = F.col(field).cast(StringType()) if field in df.columns else F.lit(None).cast(StringType())
                df = df.withColumn(f"{field}_raw", value)
        for field, dtype in df.dtypes:
            if dtype == "string" and not field.startswith("__") and not field.endswith("_raw"):
                df = df.withColumn(field, F.trim(F.col(field)))

        errors = []
        numeric_raw = F.col(f"{numeric_field}_raw")
        present = numeric_raw.isNotNull() & (F.length(F.trim(numeric_raw)) > 0)
        numeric_type = dict(df.dtypes)[numeric_field]
        if numeric_field == 'price' and (numeric_type in {'double','float','bigint','int','smallint','tinyint'} or numeric_type.startswith('decimal')):
            # Numeric JSON/Parquet values already have a decimal interpretation.
            # Locale separator rules apply to text, never to a typed 25.555 float.
            parsed = F.col(numeric_field).cast(DoubleType())
        else:
            parsed = self._parse_price(numeric_raw) if numeric_field == "price" else numeric_raw.cast(DoubleType())
        cast_failed = present & (parsed.isNull() | F.isnan(parsed))
        finite = parsed.isNotNull() & ~F.isnan(parsed) & (F.abs(parsed) < float("inf"))
        in_domain = parsed.between(1, 5) if numeric_field == "rating" else parsed > 0
        numeric_valid = finite & in_domain
        df = df.withColumn(f"__cast_err_{numeric_field}", cast_failed)
        df = df.withColumn(numeric_field, F.when(numeric_valid, parsed).otherwise(F.lit(None).cast(DoubleType())))
        errors.append(F.when(~present, F.lit(f"MISSING_{numeric_field.upper()}"))
                       .when(~F.coalesce(numeric_valid, F.lit(False)), F.lit(f"INVALID_{numeric_field.upper()}")))
        for field in sorted(time_fields):
            raw = F.col(f"{field}_raw")
            parsed_time = self._parse_timestamp(raw)
            failed = raw.isNotNull() & (F.length(F.trim(raw)) > 0) & parsed_time.isNull()
            flag = "__cast_err_timestamp" if field == business_field else f"__cast_err_{field}"
            df = df.withColumn(flag, failed).withColumn(field, parsed_time)
            errors.append(F.when(failed, F.lit(f"INVALID_{field.upper()}")))
        df = df.withColumn("time_available", F.col(business_field).isNotNull())
        df = df.withColumn("ingestion_time_available", F.col("ingested_at").isNotNull())
        if dataset_type == "reviews":
            if "verified_purchase" in df.columns:
                df = df.withColumn("verified_purchase", F.col("verified_purchase").cast(BooleanType()))
            if "user_id" in df.columns:
                user = F.trim(F.col("user_id").cast(StringType()))
                df = df.withColumn("user_id", F.when(user.isNull() | user.isin("", "None", "null", "nan"), "anon_redacted")
                                   .when(user.rlike(r"^anon_[0-9a-f]{16}$|^anon_redacted$"), user)
                                   .otherwise(F.concat(F.lit("anon_"), F.substring(F.sha2(F.concat(F.lit("sentinel-pii-v1:"), user), 256), 1, 16))))
        if dataset_type == "products":
            from spark.etl.normalizer import DEFAULT_BRAND_MAP, DEFAULT_CATEGORY_MAP
            for field, aliases, fallback in [("brand", DEFAULT_BRAND_MAP, "Unknown"),
                                              ("category", DEFAULT_CATEGORY_MAP, "Other")]:
                value = F.trim(F.col(field)) if field in df.columns else F.lit(None).cast(StringType())
                mapping = F.create_map(*[F.lit(item) for pair in aliases.items() for item in pair])
                df = df.withColumn(field, F.when(value.isNull() | (value == ""), fallback)
                                   .otherwise(F.coalesce(F.element_at(mapping, F.lower(value)), F.initcap(value))))
        if dataset_type in {"products", "prices"}:
            from spark.etl.normalizer import EntityNormalizer
            value = F.trim(F.col("currency")) if "currency" in df.columns else F.lit(None).cast(StringType())
            mapping = F.create_map(*[F.lit(item) for pair in EntityNormalizer.CURRENCY_ALIASES.items() for item in pair])
            df = df.withColumn("currency", F.coalesce(
                F.element_at(mapping, F.lower(value)),
                F.when(F.upper(value).rlike(r"^[A-Z]{3}$"), F.upper(value)),
                F.lit(settings.analytics.default_currency),
            ))
        new_errors = F.filter(F.array(*errors), lambda code: code.isNotNull())
        if "normalization_errors" in df.columns:
            previous = F.col("normalization_errors")
            if dict(df.dtypes)["normalization_errors"] == "string":
                previous = F.from_json(previous, "array<string>")
            new_errors = F.array_union(F.coalesce(previous, F.array().cast("array<string>")), new_errors)
        df = df.withColumn("normalization_errors", F.sort_array(new_errors))
        df = df.withColumn("normalization_status", F.when(F.size("normalization_errors") > 0, "invalid").otherwise("valid"))
        return df.withColumn("__parsed_and_casted", F.lit(True))

    def validate(self, df: DataFrame, dataset_type: str) -> Tuple[DataFrame, DataFrame]:
        """Validates records strictly against schema contracts AFTER parsing and casting types.
        DOES NOT clamp bad ratings or prices; routes invalid records to quarantine with reasons.
        """
        if "__parsed_and_casted" not in df.columns:
            df = self.standardize_schema(df, dataset_type)

        cols = set(df.columns)
        cast_err_rating = F.col("__cast_err_rating") if "__cast_err_rating" in cols else F.lit(False)
        cast_err_price = F.col("__cast_err_price") if "__cast_err_price" in cols else F.lit(False)
        cast_err_ts = F.col("__cast_err_timestamp") if "__cast_err_timestamp" in cols else F.lit(False)

        if dataset_type == "reviews":
            cond_invalid_rating = F.col("rating").isNull() | F.isnan(F.col("rating")) | (F.col("rating") < 1.0) | (F.col("rating") > 5.0)
            cond_missing_text = F.col("review_text").isNull() | (F.length(F.trim(F.col("review_text"))) == 0)
            cond_missing_product = F.col("product_id").isNull() | (F.length(F.trim(F.col("product_id"))) == 0)
            cond_missing_review = F.col("review_id").isNull() | (F.length(F.trim(F.col("review_id"))) == 0)

            reason_col = (
                F.when(cast_err_rating, F.lit("INVALID_TYPE_RATING"))
                .when(cast_err_ts, F.lit("INVALID_TYPE_TIMESTAMP"))
                .when(cond_invalid_rating, F.lit("INVALID_RATING_OUT_OF_RANGE"))
                .when(cond_missing_text, F.lit("MISSING_REVIEW_TEXT"))
                .when(cond_missing_product, F.lit("MISSING_PRODUCT_ID"))
                .when(cond_missing_review, F.lit("MISSING_REVIEW_ID"))
                .otherwise(F.lit("UNKNOWN_VALIDATION_ERROR"))
            )
            is_valid = (
                (~cast_err_rating)
                & (~cast_err_ts)
                & (~cond_invalid_rating)
                & (~cond_missing_text)
                & (~cond_missing_product)
                & (~cond_missing_review)
            )

        elif dataset_type == "products":
            cond_missing_id = F.col("product_id").isNull() | (F.length(F.trim(F.col("product_id"))) == 0)
            cond_missing_name = F.col("product_name").isNull() | (F.length(F.trim(F.col("product_name"))) == 0)
            cond_invalid_price = F.col("price").isNull() | F.isnan(F.col("price")) | (F.col("price") <= 0)

            reason_col = (
                F.when(cast_err_price, F.lit("INVALID_TYPE_PRICE"))
                .when(cast_err_ts, F.lit("INVALID_TYPE_TIMESTAMP"))
                .when(cond_missing_id, F.lit("MISSING_PRODUCT_ID"))
                .when(cond_missing_name, F.lit("MISSING_PRODUCT_NAME"))
                .when(cond_invalid_price, F.lit("INVALID_PRICE_NON_POSITIVE"))
                .otherwise(F.lit("UNKNOWN_VALIDATION_ERROR"))
            )
            is_valid = (~cast_err_price) & (~cast_err_ts) & (~cond_missing_id) & (~cond_missing_name) & (~cond_invalid_price)

        elif dataset_type == "prices":
            cond_missing_price_id = F.col("price_id").isNull() | (F.length(F.trim(F.col("price_id"))) == 0)
            cond_missing_prod_id = F.col("product_id").isNull() | (F.length(F.trim(F.col("product_id"))) == 0)
            cond_invalid_price = F.col("price").isNull() | F.isnan(F.col("price")) | (F.col("price") <= 0)

            reason_col = (
                F.when(cast_err_price, F.lit("INVALID_TYPE_PRICE"))
                .when(cast_err_ts, F.lit("INVALID_TYPE_TIMESTAMP"))
                .when(cond_missing_price_id, F.lit("MISSING_PRICE_ID"))
                .when(cond_missing_prod_id, F.lit("MISSING_PRODUCT_ID"))
                .when(cond_invalid_price, F.lit("INVALID_PRICE_NON_POSITIVE"))
                .otherwise(F.lit("UNKNOWN_VALIDATION_ERROR"))
            )
            is_valid = (~cast_err_price) & (~cast_err_ts) & (~cond_missing_price_id) & (~cond_missing_prod_id) & (~cond_invalid_price)

        else:
            reason_col = F.lit("UNRECOGNIZED_DATASET_TYPE")
            is_valid = F.lit(True)

        normalization_invalid = F.col("normalization_status") == "invalid"
        is_valid = is_valid & ~normalization_invalid
        reason_col = F.when(normalization_invalid & (reason_col == "UNKNOWN_VALIDATION_ERROR"),
                            F.lit("NORMALIZATION_ERROR")).otherwise(reason_col)
        helper_cols = [c for c in df.columns if c.startswith("__")]
        valid_df = df.filter(is_valid).drop(*helper_cols)
        invalid_df = (
            df.filter(~is_valid)
            .withColumn("quarantine_reason", reason_col)
            .withColumn("quarantined_at", F.current_timestamp())
            .drop(*helper_cols)
        )

        return valid_df, invalid_df

    def deduplicate(
        self,
        df: DataFrame,
        key_cols: List[str],
        order_col: str = "ingested_at"
    ) -> DataFrame:
        """Deduplicates a Spark DataFrame using Window partitionBy(keys).orderBy(desc(order_col)).
        Keeps the latest row (row_number() == 1).
        """
        cols = set(df.columns)
        if order_col not in cols:
            df = df.withColumn(order_col, F.lit(None).cast(TimestampType()))

        window_spec = Window.partitionBy(*key_cols).orderBy(F.col(order_col).desc_nulls_last())
        return (
            df.withColumn("__row_num", F.row_number().over(window_spec))
            .filter(F.col("__row_num") == 1)
            .drop("__row_num")
        )

    def process_dataset(
        self,
        dataset_name: str,
        bronze_source: Union[str, Path, List[Dict[str, Any]]],
        partition_cols: Optional[List[str]] = None,
        silver_subpath: Optional[str] = None,
        quarantine_subpath: Optional[str] = None,
        replace_existing_silver: bool = False,
    ) -> Dict[str, Any]:
        """Runs the complete Bronze -> Silver ETL pipeline for a single dataset.
        Includes S3A read, envelope extraction, field-level null statistics,
        parse/cast BEFORE validation, intra-batch & cross-batch deduplication,
        quarantine routing, and Silver write.
        Returns audited mathematical reconciliation metrics.
        """
        processing_started = time.perf_counter()
        pk = self.PRIMARY_KEYS.get(dataset_name, f"{dataset_name.rstrip('s')}_id")
        order_col = self.ORDER_COLUMNS.get(dataset_name, "ingested_at")

        # 1. Read Bronze JSON
        raw_df = self.read_bronze_json(bronze_source)
        input_count = raw_df.count()

        if input_count == 0:
            return {
                "processing_time_ms": int((time.perf_counter() - processing_started) * 1000),
            "dataset_name": dataset_name,
                "input_count": 0,
                "valid_count": 0,
                "valid_input_count": 0,
                "unique_valid_count": 0,
                "quarantine_count": 0,
                "intra_dedup_dropped": 0,
                "intra_batch_duplicate_count": 0,
                "cross_batch_duplicate_count": 0,
                "silver_prev_count": 0,
                "new_inserts_count": 0,
                "row_invalid_count": 0,
                "silver_written_count": 0,
                "field_null_counts": {},
                "total_field_nulls": 0,
                "null_row_count": 0,
                "cast_error_count": 0,
                "records_missing_observation_time": 0,
                "untimed_silver_count": None,
                "normalization_error_counts": {},
                "quarantine_reason_counts": {},
                "reconciled": True,
                "reconciled_batch": True,
                "reconciled_silver": True,
                "dq_score": 100.0,
                "silver_df": raw_df
            }

        # 2. Compute Field-level Null Statistics BEFORE type casting
        null_stats = self.compute_field_null_stats(raw_df, dataset_name)

        # 3. Parse & Cast Data Types BEFORE Validation
        typed_df = self.standardize_schema(raw_df, dataset_name)
        missing_observation_time = typed_df.filter(~F.col("time_available")).count()
        normalization_error_counts = {
            row['code']: row['count'] for row in typed_df.select(
                F.explode('normalization_errors').alias('code')
            ).groupBy('code').count().collect()
        }
        cast_err_cols = [c for c in typed_df.columns if c.startswith("__cast_err_")]
        if cast_err_cols:
            cast_err_cond = F.lit(False)
            for cec in cast_err_cols:
                cast_err_cond = cast_err_cond | F.coalesce(F.col(cec), F.lit(False))
            cast_error_count = int(
                typed_df.agg(F.sum(F.when(cast_err_cond, 1).otherwise(0)).alias("cnt")).collect()[0]["cnt"] or 0
            )
        else:
            cast_error_count = 0

        # 4. Validate & Quarantine
        valid_df, invalid_df = self.validate(typed_df, dataset_name)
        valid_count = valid_df.count()
        valid_input_count = valid_count
        quarantine_count = invalid_df.count()
        quarantine_reason_counts = {
            row['quarantine_reason']: row['count']
            for row in invalid_df.groupBy('quarantine_reason').count().collect()
        }

        # Strict Mathematical Check: input == valid + quarantine
        assert input_count == valid_input_count + quarantine_count, (
            f"Accounting mismatch: input ({input_count}) != valid ({valid_input_count}) + quarantine ({quarantine_count})"
        )

        # 5. Save Quarantine if any invalid records exist
        q_rel_path = quarantine_subpath or f"quarantine/{dataset_name}"
        quarantine_path = SparkSessionFactory.resolve_storage_path(q_rel_path, layer="silver")
        if quarantine_count > 0:
            q_mode = "overwrite" if replace_existing_silver else "append"
            invalid_df.write.mode(q_mode).parquet(quarantine_path)
            print(f"[Spark ETL] Routed {quarantine_count} bad records to quarantine: {quarantine_path}")

        # 6. Intra-batch Deduplication
        intra_dedup_df = self.deduplicate(valid_df, [pk], order_col=order_col)
        intra_valid_count = intra_dedup_df.count()
        intra_batch_duplicate_count = valid_input_count - intra_valid_count
        intra_dedup_dropped = intra_batch_duplicate_count

        # 7. Cross-batch Deduplication against existing Silver Parquet
        s_rel_path = silver_subpath or dataset_name
        silver_path = SparkSessionFactory.resolve_storage_path(s_rel_path, layer="silver")
        final_silver_df, cross_metrics = self._cross_batch_dedup_and_write(
            intra_dedup_df,
            dataset_name,
            silver_path,
            pk,
            order_col,
            partition_cols,
            replace_existing=replace_existing_silver,
            return_metrics=True,
        )
        silver_prev_count = cross_metrics["silver_prev_count"]
        cross_batch_duplicate_count = cross_metrics["cross_batch_duplicate_count"]
        new_inserts_count = cross_metrics["new_inserts_count"]
        silver_written_count = cross_metrics["silver_written_count"]
        untimed_silver_count = final_silver_df.filter(F.col("time_available") == F.lit(False)).count()

        # Dual Reconciliation Invariants:
        # Invariant A (Batch Invariant): Every input record is definitively accounted for
        reconciled_batch = (
            input_count == quarantine_count + intra_batch_duplicate_count + cross_batch_duplicate_count + new_inserts_count
        )
        assert reconciled_batch, (
            f"Batch accounting invariant failed: input ({input_count}) != "
            f"quarantine ({quarantine_count}) + intra_dup ({intra_batch_duplicate_count}) + "
            f"cross_dup ({cross_batch_duplicate_count}) + new_inserts ({new_inserts_count})"
        )

        # Invariant B (Silver State Transition Invariant): Net change in Silver matches genuinely new inserts
        reconciled_silver = (silver_written_count == silver_prev_count + new_inserts_count)
        assert reconciled_silver, (
            f"Silver state accounting invariant failed: silver_written ({silver_written_count}) != "
            f"silver_prev ({silver_prev_count}) + new_inserts ({new_inserts_count})"
        )

        # Row-level valid count after deduplication vs row-level invalid count
        unique_valid_count = intra_valid_count
        row_invalid_count = quarantine_count + intra_batch_duplicate_count
        dq_score = round((unique_valid_count / input_count * 100.0), 2) if input_count > 0 else 100.0

        return {
            "processing_time_ms": int((time.perf_counter() - processing_started) * 1000),
            "dataset_name": dataset_name,
            "input_count": input_count,
            "valid_count": valid_count,
            "valid_input_count": valid_input_count,
            "unique_valid_count": unique_valid_count,
            "quarantine_count": quarantine_count,
            "intra_dedup_dropped": intra_dedup_dropped,
            "intra_batch_duplicate_count": intra_batch_duplicate_count,
            "cross_batch_duplicate_count": cross_batch_duplicate_count,
            "silver_prev_count": silver_prev_count,
            "new_inserts_count": new_inserts_count,
            "row_invalid_count": row_invalid_count,
            "silver_written_count": silver_written_count,
            "field_null_counts": null_stats["field_null_counts"],
            "total_field_nulls": null_stats["total_field_nulls"],
            "null_row_count": null_stats["null_row_count"],
            "cast_error_count": cast_error_count,
            "records_missing_observation_time": missing_observation_time,
            "untimed_silver_count": untimed_silver_count,
            "normalization_error_counts": normalization_error_counts,
            "quarantine_reason_counts": quarantine_reason_counts,
            "reconciled": (reconciled_batch and reconciled_silver),
            "reconciled_batch": reconciled_batch,
            "reconciled_silver": reconciled_silver,
            "dq_score": dq_score,
            "silver_path": silver_path,
            "quarantine_path": quarantine_path,
            "silver_df": final_silver_df,
            "quarantine_df": invalid_df,
        }

    @synchronized_path("silver_path")
    def _cross_batch_dedup_and_write(
        self,
        new_batch_df: DataFrame,
        dataset_name: str,
        silver_path: str,
        pk: str,
        order_col: str,
        partition_cols: Optional[List[str]] = None,
        replace_existing: bool = False,
        return_metrics: bool = False,
    ) -> Union[DataFrame, Tuple[DataFrame, Dict[str, Any]]]:
        """Consolidates new batch with existing Silver Parquet via Spark union and window deduplication,
        then writes the reconciled dataset to Silver Parquet using staged atomic publish.
        """
        intra_valid_count = new_batch_df.count()
        silver_prev_count = 0
        cross_batch_duplicate_count = 0
        combined_df = new_batch_df

        # Check if existing Silver Parquet exists (skip when replace_existing=True)
        if not replace_existing:
            existing_df = SafeParquetIO.read_parquet_safely(self.spark, silver_path, check_exists_first=True)
            if existing_df is not None and not existing_df.rdd.isEmpty():
                if dataset_name == "reviews" and "user_id" in existing_df.columns:
                    uid_trimmed = F.trim(F.col("user_id").cast(StringType()))
                    existing_df = existing_df.withColumn(
                        "user_id",
                        F.when(
                            uid_trimmed.isNull() | (uid_trimmed == "") | uid_trimmed.isin("None", "null", "nan"),
                            F.lit("anon_redacted"),
                        )
                        .when(
                            uid_trimmed.rlike(r"^anon_[0-9a-f]{16}$|^anon_redacted$"),
                            uid_trimmed,
                        )
                        .otherwise(
                            F.concat(
                                F.lit("anon_"),
                                F.substring(F.sha2(F.concat(F.lit("sentinel-pii-v1:"), uid_trimmed), 256), 1, 16),
                            )
                        ),
                    )
                # Deduplicate existing historical records by PK to ensure base is strictly unique entities
                existing_materialized = self.deduplicate(existing_df, [pk], order_col=order_col).localCheckpoint(eager=True)
                silver_prev_count = existing_materialized.count()

                # Identify records in new_batch_df whose primary key already exists in existing Silver
                existing_keys = existing_materialized.select(pk).distinct()
                cross_batch_dups_df = new_batch_df.join(existing_keys, on=pk, how="inner")
                cross_batch_duplicate_count = cross_batch_dups_df.count()

                combined_df = existing_materialized.unionByName(new_batch_df, allowMissingColumns=True)
                combined_df = self.deduplicate(combined_df, [pk], order_col=order_col)
            elif existing_df is None:
                print(f"[Spark ETL] Silver path does not exist yet ({silver_path}); initializing new Silver table.")

        silver_written_count = combined_df.count()
        new_inserts_count = intra_valid_count - cross_batch_duplicate_count

        # Atomic publish via staging to prevent data loss or partial writes
        SafeParquetIO.publish_parquet_atomic(
            spark=self.spark,
            df=combined_df,
            target_path=silver_path,
            partition_cols=partition_cols,
            expected_count=silver_written_count,
        )

        print(f"[Spark ETL] Written {silver_written_count} records to Silver Parquet: {silver_path}")

        metrics = {
            "silver_prev_count": silver_prev_count,
            "cross_batch_duplicate_count": cross_batch_duplicate_count,
            "new_inserts_count": new_inserts_count,
            "silver_written_count": silver_written_count,
        }

        if return_metrics:
            return combined_df, metrics
        return combined_df

    def run_pipeline(
        self,
        products_source: Optional[Union[str, Path, List[Union[str, Path, Dict[str, Any]]]]] = None,
        reviews_source: Optional[Union[str, Path, List[Union[str, Path, Dict[str, Any]]]]] = None,
        prices_source: Optional[Union[str, Path, List[Union[str, Path, Dict[str, Any]]]]] = None,
        enforce_dq_gate: bool = False,
        min_dq_score: Optional[float] = None,
        run_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Executes full Bronze -> Silver Batch ETL across all entity types, persists DQ reports,
        and optionally enforces the DQ threshold gate.
        """
        run_id = run_id or str(uuid.uuid4())
        print(">>> Starting SentinelAI Real Spark Batch ETL (Bronze -> Silver)")

        effective_products_source = products_source if products_source is not None else settings.kafka.topic_products
        effective_reviews_source = reviews_source if reviews_source is not None else settings.kafka.topic_reviews

        # 1. Products
        prod_res = self.process_dataset("products", effective_products_source)

        # 2. Reviews
        rev_res = self.process_dataset("reviews", effective_reviews_source)

        # 3. Prices (if provided)
        price_res = None
        if prices_source is not None:
            price_res = self.process_dataset("prices", prices_source)

        for name, result in [('products',prod_res),('reviews',rev_res),('prices',price_res)]:
            if result is not None and 'silver_path' in result:
                from storage.storage_manager import storage
                storage.write_json(f'silver/_snapshot_{name}.json', {
                    'run_id':run_id,'rows':result['silver_written_count'],'path':result['silver_path'],
                })

        def _build_dq_dict(res: Dict[str, Any]) -> Dict[str, Any]:
            return {
                "run_id": run_id,
                "processing_time_ms": res["processing_time_ms"],
                "dataset_name": res["dataset_name"],
                "records_received": res["input_count"],
                "records_valid": res["unique_valid_count"],
                "records_invalid": res["row_invalid_count"],
                "records_quarantined": res["quarantine_count"],
                "records_missing_observation_time": res["records_missing_observation_time"],
                "untimed_silver_count": res["untimed_silver_count"],
                "normalization_error_counts": res["normalization_error_counts"],
                "quarantine_reason_counts": res["quarantine_reason_counts"],
                "records_duplicate": res["intra_batch_duplicate_count"],
                "intra_batch_duplicate_count": res["intra_batch_duplicate_count"],
                "cross_batch_duplicate_count": res["cross_batch_duplicate_count"],
                "valid_input_count": res["valid_input_count"],
                "silver_prev_count": res["silver_prev_count"],
                "silver_written_count": res["silver_written_count"],
                "new_inserts_count": res["new_inserts_count"],
                "records_missing": res["null_row_count"],
                "field_null_counts": res["field_null_counts"],
                "total_field_nulls": res["total_field_nulls"],
                "cast_error_count": res["cast_error_count"],
                "dq_score": res["dq_score"],
            }

        products_dq = _build_dq_dict(prod_res)
        reviews_dq = _build_dq_dict(rev_res)
        prices_dq = _build_dq_dict(price_res) if price_res else None

        # 4. Persist Data Quality Scorecards to Gold Layer (idempotent batch_id per dataset)
        from spark.quality.spark_dq import SparkDataQualityEvaluator
        dq_reports = [
            SparkDataQualityEvaluator.generate_report(products_dq, batch_id=f"dq:{run_id}:products"),
            SparkDataQualityEvaluator.generate_report(reviews_dq, batch_id=f"dq:{run_id}:reviews"),
        ]
        if prices_dq:
            dq_reports.append(SparkDataQualityEvaluator.generate_report(prices_dq, batch_id=f"dq:{run_id}:prices"))
        SparkDataQualityEvaluator.save_reports_to_gold(dq_reports)

        dq_list = [products_dq, reviews_dq] + ([prices_dq] if prices_dq else [])
        dq_gate = SparkDataQualityEvaluator.check_dq_gate(
            dq_list,
            min_dq_score=min_dq_score,
            raise_on_failure=enforce_dq_gate,
            required_datasets=["products", "reviews"] + (["prices"] if prices_source is not None else []),
            run_id=run_id,
        )

        # Construct backward-compatible metrics summary
        return {
            "run_id": run_id,
            "products_processed": prod_res["valid_count"],
            "reviews_processed": rev_res["valid_count"],
            "prices_processed": price_res["valid_count"] if price_res else 0,
            "products_dq": products_dq,
            "reviews_dq": reviews_dq,
            "prices_dq": prices_dq,
            "dq_gate": dq_gate,
            "reconciliation": {
                "products": prod_res,
                "reviews": rev_res,
                "prices": price_res
            },
            "products_df": prod_res["silver_df"],
            "reviews_df": rev_res["silver_df"],
            "prices_df": price_res["silver_df"] if price_res else None
        }


