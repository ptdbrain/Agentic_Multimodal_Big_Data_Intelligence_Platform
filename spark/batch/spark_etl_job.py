"""Spark Batch ETL Bronze -> Silver Engine.
Reads Bronze raw JSON via object storage (S3A), unpacks message envelopes,
standardizes data types, validates against quality rules, routes bad records to quarantine,
and applies cross-batch deduplication before persisting to Silver Parquet.

Zero toPandas() in dataset processing; strict Spark DataFrame execution with mathematical reconciliation.
"""
import os
import json
import tempfile
from pathlib import Path
from typing import List, Dict, Any, Tuple, Optional, Union
from config.settings import settings
from spark.spark_session import SparkSessionFactory

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

        # 1. Handle list input: either list of file/S3A paths or in-memory list of record dicts
        if isinstance(source, list):
            if not source:
                return self.spark.createDataFrame([], empty_schema)

            if all(isinstance(x, (str, Path)) for x in source):
                resolved_paths = []
                for p in source:
                    ps = str(p)
                    if not (ps.startswith("s3a://") or ps.startswith("file://") or Path(ps).exists()):
                        ps = SparkSessionFactory.resolve_storage_path(ps, layer="bronze")
                    resolved_paths.append(ps)
                df = (
                    self.spark.read
                    .option("recursiveFileLookup", "true")
                    .option("multiline", "true")
                    .json(resolved_paths)
                )
                return self.unpack_envelope(df)

            # Dump in-memory dicts to temporary JSON file for consistent Spark multiline JSON reading
            temp_dir = tempfile.mkdtemp(prefix="spark_bronze_tmp_")
            temp_file = Path(temp_dir) / "batch.json"
            with open(temp_file, "w", encoding="utf-8") as f:
                json.dump(source, f, ensure_ascii=False)
            source_path = str(temp_file)
        else:
            source_path = str(source)

        # 2. Resolve path to S3A URL or local path
        if not (source_path.startswith("s3a://") or source_path.startswith("file://") or Path(source_path).exists()):
            # Assume it's a Bronze topic name or relative Bronze path (e.g. 'raw.reviews')
            source_path = SparkSessionFactory.resolve_storage_path(source_path, layer="bronze")

        # 3. Read multiline JSON with Spark (supports both single file and partitioned directory trees)
        df = (
            self.spark.read
            .option("recursiveFileLookup", "true")
            .option("multiline", "true")
            .json(source_path)
        )

        # 4. Parse Envelope: if 'payload' is a struct, flatten payload.* while keeping envelope metadata
        df = self.unpack_envelope(df)
        return df

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

    def compute_field_null_stats(self, df: DataFrame, dataset_type: str) -> Dict[str, Any]:
        """Computes field-level null/empty statistics across all columns (field_null_counts)
        separately from row-level missing required field count (null_row_count).
        Must be called BEFORE type casting so type cast failures are not conflated with nulls.
        """
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

    def standardize_schema(self, df: DataFrame, dataset_type: str) -> DataFrame:
        """Standardizes data types, trims strings, and parses timestamps via PySpark SQL.
        Tracks parse/cast failures on non-null inputs before validation runs.
        """
        cols = set(df.columns)

        # Common string trim
        for col_name, dtype in df.dtypes:
            if dtype == "string" and not col_name.startswith("__"):
                df = df.withColumn(col_name, F.trim(F.col(col_name)))

        def _raw_present(col_name: str):
            return F.col(col_name).isNotNull() & (F.length(F.trim(F.col(col_name).cast(StringType()))) > 0)

        if dataset_type == "reviews":
            if "rating" in cols:
                raw_ok = _raw_present("rating")
                casted = F.col("rating").cast(DoubleType())
                df = (
                    df.withColumn("__cast_err_rating", raw_ok & (casted.isNull() | F.isnan(casted)))
                    .withColumn("rating", casted)
                )
            if "verified_purchase" in cols:
                df = df.withColumn("verified_purchase", F.col("verified_purchase").cast(BooleanType()))
            if "review_date" in cols:
                raw_ok = _raw_present("review_date")
                casted_ts = F.to_timestamp(F.col("review_date").cast(StringType()))
                df = (
                    df.withColumn("__cast_err_timestamp", raw_ok & casted_ts.isNull())
                    .withColumn("review_date", casted_ts)
                )
            if "ingested_at" in cols:
                df = df.withColumn(
                    "ingested_at",
                    F.coalesce(F.to_timestamp(F.col("ingested_at").cast(StringType())), F.current_timestamp())
                )

        elif dataset_type == "products":
            if "price" in cols:
                raw_ok = _raw_present("price")
                casted = F.col("price").cast(DoubleType())
                df = (
                    df.withColumn("__cast_err_price", raw_ok & (casted.isNull() | F.isnan(casted)))
                    .withColumn("price", casted)
                )
            if "brand" in cols:
                df = df.withColumn("brand", F.initcap(F.trim(F.col("brand"))))
            if "category" in cols:
                df = df.withColumn("category", F.initcap(F.trim(F.col("category"))))
            if "created_at" in cols:
                raw_ok = _raw_present("created_at")
                casted_ts = F.to_timestamp(F.col("created_at").cast(StringType()))
                df = (
                    df.withColumn("__cast_err_timestamp", raw_ok & casted_ts.isNull())
                    .withColumn("created_at", casted_ts)
                )
            if "ingested_at" in cols:
                df = df.withColumn(
                    "ingested_at",
                    F.coalesce(F.to_timestamp(F.col("ingested_at").cast(StringType())), F.current_timestamp())
                )

        elif dataset_type == "prices":
            if "price" in cols:
                raw_ok = _raw_present("price")
                casted = F.col("price").cast(DoubleType())
                df = (
                    df.withColumn("__cast_err_price", raw_ok & (casted.isNull() | F.isnan(casted)))
                    .withColumn("price", casted)
                )
            if "currency" in cols:
                df = df.withColumn("currency", F.coalesce(F.upper(F.trim(F.col("currency"))), F.lit("VND")))
            if "timestamp" in cols:
                raw_ok = _raw_present("timestamp")
                casted_ts = F.to_timestamp(F.col("timestamp").cast(StringType()))
                df = (
                    df.withColumn("__cast_err_timestamp", raw_ok & casted_ts.isNull())
                    .withColumn("timestamp", casted_ts)
                )
            if "ingested_at" in cols:
                df = df.withColumn(
                    "ingested_at",
                    F.coalesce(F.to_timestamp(F.col("ingested_at").cast(StringType())), F.current_timestamp())
                )

        df = df.withColumn("__parsed_and_casted", F.lit(True))
        return df

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
        partition_cols: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """Runs the complete Bronze -> Silver ETL pipeline for a single dataset.
        Includes S3A read, envelope extraction, field-level null statistics,
        parse/cast BEFORE validation, intra-batch & cross-batch deduplication,
        quarantine routing, and Silver write.
        Returns audited mathematical reconciliation metrics.
        """
        pk = self.PRIMARY_KEYS.get(dataset_name, f"{dataset_name.rstrip('s')}_id")
        order_col = self.ORDER_COLUMNS.get(dataset_name, "ingested_at")

        # 1. Read Bronze JSON
        raw_df = self.read_bronze_json(bronze_source)
        input_count = raw_df.count()

        if input_count == 0:
            return {
                "dataset_name": dataset_name,
                "input_count": 0,
                "valid_count": 0,
                "unique_valid_count": 0,
                "quarantine_count": 0,
                "intra_dedup_dropped": 0,
                "row_invalid_count": 0,
                "silver_written_count": 0,
                "field_null_counts": {},
                "total_field_nulls": 0,
                "null_row_count": 0,
                "cast_error_count": 0,
                "reconciled": True,
                "dq_score": 100.0,
                "silver_df": raw_df
            }

        # 2. Compute Field-level Null Statistics BEFORE type casting
        null_stats = self.compute_field_null_stats(raw_df, dataset_name)

        # 3. Parse & Cast Data Types BEFORE Validation
        typed_df = self.standardize_schema(raw_df, dataset_name)
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
        quarantine_count = invalid_df.count()

        # Strict Mathematical Check: input == valid + quarantine
        assert input_count == valid_count + quarantine_count, (
            f"Accounting mismatch: input ({input_count}) != valid ({valid_count}) + quarantine ({quarantine_count})"
        )

        # 5. Save Quarantine if any invalid records exist
        quarantine_path = SparkSessionFactory.resolve_storage_path(f"quarantine/{dataset_name}", layer="silver")
        if quarantine_count > 0:
            invalid_df.write.mode("append").parquet(quarantine_path)
            print(f"[Spark ETL] Routed {quarantine_count} bad records to quarantine: {quarantine_path}")

        # 6. Intra-batch Deduplication
        intra_dedup_df = self.deduplicate(valid_df, [pk], order_col=order_col)
        intra_valid_count = intra_dedup_df.count()
        intra_dedup_dropped = valid_count - intra_valid_count

        # 7. Cross-batch Deduplication against existing Silver Parquet
        silver_path = SparkSessionFactory.resolve_storage_path(dataset_name, layer="silver")
        final_silver_df = self._cross_batch_dedup_and_write(
            intra_dedup_df, dataset_name, silver_path, pk, order_col, partition_cols
        )
        silver_written_count = final_silver_df.count()

        # Row-level valid count after deduplication vs row-level invalid count
        unique_valid_count = intra_valid_count
        row_invalid_count = quarantine_count + intra_dedup_dropped
        dq_score = round((unique_valid_count / input_count * 100.0), 2) if input_count > 0 else 100.0

        return {
            "dataset_name": dataset_name,
            "input_count": input_count,
            "valid_count": valid_count,
            "unique_valid_count": unique_valid_count,
            "quarantine_count": quarantine_count,
            "intra_dedup_dropped": intra_dedup_dropped,
            "row_invalid_count": row_invalid_count,
            "silver_written_count": silver_written_count,
            "field_null_counts": null_stats["field_null_counts"],
            "total_field_nulls": null_stats["total_field_nulls"],
            "null_row_count": null_stats["null_row_count"],
            "cast_error_count": cast_error_count,
            "reconciled": (input_count == valid_count + quarantine_count),
            "dq_score": dq_score,
            "silver_df": final_silver_df,
            "quarantine_df": invalid_df
        }

    def _cross_batch_dedup_and_write(
        self,
        new_batch_df: DataFrame,
        dataset_name: str,
        silver_path: str,
        pk: str,
        order_col: str,
        partition_cols: Optional[List[str]] = None
    ) -> DataFrame:
        """Consolidates new batch with existing Silver Parquet via Spark union and window deduplication,
        then writes the reconciled dataset to Silver Parquet.
        """
        combined_df = new_batch_df

        # Check if existing Silver Parquet exists
        try:
            existing_df = self.spark.read.parquet(silver_path)
            if existing_df.count() > 0:
                # Eagerly checkpoint existing_df locally to sever the read dependency before overwriting silver_path
                existing_materialized = existing_df.localCheckpoint(eager=True)
                combined_df = existing_materialized.unionByName(new_batch_df, allowMissingColumns=True)
                # Deduplicate across old + new batches
                combined_df = self.deduplicate(combined_df, [pk], order_col=order_col)
        except Exception:
            # First batch or path does not exist yet
            pass

        # Write Parquet directly via Spark DataFrame
        writer = combined_df.write.mode("overwrite")
        if partition_cols:
            writer = writer.partitionBy(*partition_cols)
        writer.parquet(silver_path)

        print(f"[Spark ETL] Written {combined_df.count()} records to Silver Parquet: {silver_path}")
        return combined_df

    def run_pipeline(
        self,
        products_source: Optional[Union[str, Path, List[Union[str, Path, Dict[str, Any]]]]] = None,
        reviews_source: Optional[Union[str, Path, List[Union[str, Path, Dict[str, Any]]]]] = None,
        prices_source: Optional[Union[str, Path, List[Union[str, Path, Dict[str, Any]]]]] = None,
        enforce_dq_gate: bool = False,
        min_dq_score: Optional[float] = None,
    ) -> Dict[str, Any]:
        """Executes full Bronze -> Silver Batch ETL across all entity types, persists DQ reports,
        and optionally enforces the DQ threshold gate.
        """
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

        def _build_dq_dict(res: Dict[str, Any]) -> Dict[str, Any]:
            return {
                "dataset_name": res["dataset_name"],
                "records_received": res["input_count"],
                "records_valid": res["unique_valid_count"],
                "records_invalid": res["row_invalid_count"],
                "records_quarantined": res["quarantine_count"],
                "records_duplicate": res["intra_dedup_dropped"],
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
            SparkDataQualityEvaluator.generate_report(products_dq, batch_id="dq_products"),
            SparkDataQualityEvaluator.generate_report(reviews_dq, batch_id="dq_reviews"),
        ]
        if prices_dq:
            dq_reports.append(SparkDataQualityEvaluator.generate_report(prices_dq, batch_id="dq_prices"))
        SparkDataQualityEvaluator.save_reports_to_gold(dq_reports)

        dq_list = [products_dq, reviews_dq] + ([prices_dq] if prices_dq else [])
        dq_gate = SparkDataQualityEvaluator.check_dq_gate(
            dq_list,
            min_dq_score=min_dq_score,
            raise_on_failure=enforce_dq_gate,
        )

        # Construct backward-compatible metrics summary
        return {
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


