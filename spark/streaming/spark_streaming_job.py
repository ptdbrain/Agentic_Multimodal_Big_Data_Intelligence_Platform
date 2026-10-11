"""Real Spark Structured Streaming job consuming from Kafka or Bronze S3A/File streams.
Features:
- Shared SparkSession & S3A configuration via SparkSessionFactory
- Parse & cast BEFORE validation (malformed types flagged and routed to Quarantine)
- Event-time watermarking (withWatermark) for late-arriving data
- Stateful primary-key deduplication (dropDuplicates(["review_id"])) within watermark
- Valid output modes:
  * "update" (default): uses foreachBatch idempotent upsert to Parquet on S3A/local
  * "append": uses native Parquet stream writer or foreachBatch append for finalized windows
- Persistent checkpointLocation and clean stop/restart recovery without data loss or duplicate metrics
"""
from pathlib import Path
from typing import Optional, Tuple, Dict, Any, List
from config.settings import settings
from spark.spark_session import SparkSessionFactory

try:
    from pyspark.sql import SparkSession, DataFrame
    from pyspark.sql import functions as F
    from pyspark.sql.types import (
        StructType, StructField, StringType, DoubleType, TimestampType, IntegerType
    )
    from pyspark.sql.window import Window
    HAS_PYSPARK = True
except ImportError:
    HAS_PYSPARK = False
    SparkSession = None
    DataFrame = None


class SparkStreamingJob:
    """Production Spark Structured Streaming engine with unified S3A, watermarking,
    pre-validation parse/cast, key deduplication, checkpointing, and restart recovery.
    """

    VALID_OUTPUT_MODES = ("update", "append", "complete")

    def __init__(
        self,
        spark_session: Optional[Any] = None,
        checkpoint_dir: Optional[str] = None,
        output_path: Optional[str] = None,
        quarantine_path: Optional[str] = None,
        output_mode: str = "update",
    ):
        if output_mode not in self.VALID_OUTPUT_MODES:
            raise ValueError(
                f"Invalid streaming output_mode '{output_mode}'. Must be one of {self.VALID_OUTPUT_MODES}."
            )
        self.output_mode = output_mode
        self._spark = spark_session
        self.checkpoint_dir = (
            SparkSessionFactory.resolve_checkpoint_path(checkpoint_dir)
            if checkpoint_dir
            else SparkSessionFactory.resolve_checkpoint_path("streaming")
        ).rstrip("/")
        self.output_path = (
            SparkSessionFactory.resolve_storage_path(output_path, layer="gold")
            if output_path
            else SparkSessionFactory.resolve_storage_path("streaming_metrics/reviews", layer="gold")
        )
        self.quarantine_path = (
            SparkSessionFactory.resolve_storage_path(quarantine_path, layer="silver")
            if quarantine_path
            else SparkSessionFactory.resolve_storage_path("quarantine/streaming_reviews", layer="silver")
        )
        self._active_query = None
        self._quarantine_query = None
        self._last_stream_args: Dict[str, Any] = {}

    @property
    def spark(self) -> "SparkSession":
        if self._spark is None:
            self._spark = SparkSessionFactory.get_spark_session(
                app_name=f"{settings.spark.app_name}-Streaming",
                extra_configs={
                    "spark.sql.streaming.checkpointLocation": self.checkpoint_dir,
                    "spark.sql.streaming.forceDeleteTempCheckpointLocation": "true",
                },
            )
        else:
            SparkSessionFactory.configure_s3a(
                self._spark,
                extra_configs={"spark.sql.streaming.checkpointLocation": self.checkpoint_dir},
            )
        if self._spark is None:
            raise RuntimeError("Failed to initialize SparkSession via SparkSessionFactory.")
        return self._spark

    @staticmethod
    def get_envelope_raw_schema() -> "StructType":
        """Returns schema with raw StringType payload fields so parse/cast runs explicitly before validation."""
        return StructType([
            StructField("event_id", StringType(), True),
            StructField("event_type", StringType(), True),
            StructField("source", StringType(), True),
            StructField("ingested_at", StringType(), True),
            StructField("payload", StructType([
                StructField("review_id", StringType(), True),
                StructField("product_id", StringType(), True),
                StructField("user_id", StringType(), True),
                StructField("rating", StringType(), True),
                StructField("review_title", StringType(), True),
                StructField("review_text", StringType(), True),
                StructField("review_date", StringType(), True),
                StructField("verified_purchase", StringType(), True),
            ]), True),
        ])

    def create_kafka_stream(self, topics: Optional[str] = None, starting_offsets: str = "earliest") -> "DataFrame":
        """Create a readStream from Kafka topics."""
        topic_list = topics or ",".join([
            settings.kafka.topic_reviews,
            settings.kafka.topic_products,
            settings.kafka.topic_prices,
            settings.kafka.topic_events,
        ])

        kafka_df = (
            self.spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", settings.kafka.get_bootstrap_servers())
            .option("subscribe", topic_list)
            .option("startingOffsets", starting_offsets)
            .option("failOnDataLoss", "false")
            .load()
        )

        return kafka_df.selectExpr(
            "CAST(key AS STRING) as key",
            "CAST(value AS STRING) as value",
            "topic",
            "partition",
            "offset",
            "timestamp"
        )

    def create_file_stream(
        self,
        input_path: str,
        max_files_per_trigger: Optional[int] = None,
    ) -> "DataFrame":
        """Create a readStream from a JSON directory (S3A or local filesystem)."""
        resolved_input = SparkSessionFactory.resolve_storage_path(input_path, layer="bronze")
        reader = (
            self.spark.readStream
            .schema(self.get_envelope_raw_schema())
            .option("multiline", "true")
        )
        if max_files_per_trigger is not None:
            reader = reader.option("maxFilesPerTrigger", int(max_files_per_trigger))
        return reader.json(resolved_input)

    def parse_review_stream(self, raw_stream: "DataFrame") -> "DataFrame":
        """Unpacks review envelope from Kafka or JSON file stream and performs explicit
        parse/cast BEFORE validation, flagging any type conversion errors.
        """
        raw_cols = set(raw_stream.columns)
        envelope_schema = self.get_envelope_raw_schema()

        if "value" in raw_cols:
            filtered = (
                raw_stream.filter(F.col("topic") == settings.kafka.topic_reviews)
                if "topic" in raw_cols
                else raw_stream
            )
            extracted = (
                filtered
                .select(
                    F.from_json(F.col("value"), envelope_schema).alias("data"),
                    (F.col("timestamp") if "timestamp" in raw_cols else F.current_timestamp()).alias("kafka_timestamp"),
                )
                .select(
                    F.col("data.event_id").alias("event_id"),
                    F.col("data.event_type").alias("event_type"),
                    F.col("data.source").alias("source"),
                    F.col("data.ingested_at").alias("ingested_at"),
                    F.col("data.payload.review_id").alias("review_id"),
                    F.col("data.payload.product_id").alias("product_id"),
                    F.col("data.payload.rating").alias("rating_raw"),
                    F.col("data.payload.review_text").alias("review_text"),
                    F.col("data.payload.review_date").alias("review_date_raw"),
                    F.col("kafka_timestamp"),
                )
            )
        elif "payload" in raw_cols:
            extracted = raw_stream.select(
                F.col("event_id"),
                F.col("event_type"),
                F.col("source"),
                F.col("ingested_at"),
                F.col("payload.review_id").alias("review_id"),
                F.col("payload.product_id").alias("product_id"),
                F.col("payload.rating").cast(StringType()).alias("rating_raw"),
                F.col("payload.review_text").alias("review_text"),
                F.col("payload.review_date").cast(StringType()).alias("review_date_raw"),
                F.to_timestamp(F.col("ingested_at")).alias("kafka_timestamp"),
            )
        else:
            extracted = raw_stream.select(
                (F.col("event_id") if "event_id" in raw_cols else F.lit(None).cast(StringType())).alias("event_id"),
                (F.col("event_type") if "event_type" in raw_cols else F.lit("NEW_REVIEW")).alias("event_type"),
                (F.col("source") if "source" in raw_cols else F.lit("stream")).alias("source"),
                (F.col("ingested_at") if "ingested_at" in raw_cols else F.lit(None).cast(StringType())).alias("ingested_at"),
                F.col("review_id").cast(StringType()).alias("review_id"),
                F.col("product_id").cast(StringType()).alias("product_id"),
                F.col("rating").cast(StringType()).alias("rating_raw"),
                (F.col("review_text") if "review_text" in raw_cols else F.lit("stream_review")).alias("review_text"),
                (F.col("review_date") if "review_date" in raw_cols else F.col("timestamp")).cast(StringType()).alias("review_date_raw"),
                F.current_timestamp().alias("kafka_timestamp"),
            )

        # Explicit Parse / Cast BEFORE Validation
        raw_rating_trimmed = F.trim(F.col("rating_raw"))
        raw_rating_present = F.col("rating_raw").isNotNull() & (F.length(raw_rating_trimmed) > 0)
        casted_rating = raw_rating_trimmed.cast(DoubleType())

        raw_date_trimmed = F.trim(F.col("review_date_raw"))
        raw_date_present = F.col("review_date_raw").isNotNull() & (F.length(raw_date_trimmed) > 0)
        casted_review_date = F.to_timestamp(raw_date_trimmed)

        parsed = (
            extracted
            .withColumn("review_id", F.trim(F.col("review_id")))
            .withColumn("product_id", F.trim(F.col("product_id")))
            .withColumn("review_text", F.trim(F.col("review_text")))
            .withColumn(
                "__cast_err_rating",
                raw_rating_present & (casted_rating.isNull() | F.isnan(casted_rating))
            )
            .withColumn("rating", casted_rating)
            .withColumn(
                "__cast_err_timestamp",
                raw_date_present & casted_review_date.isNull()
            )
            .withColumn("review_date", raw_date_trimmed)
            .withColumn(
                "event_time",
                F.when(
                    raw_date_present,
                    casted_review_date
                ).otherwise(
                    F.coalesce(F.to_timestamp(F.col("ingested_at")), F.col("kafka_timestamp"))
                )
            )
            .withColumn("__parsed_and_casted", F.lit(True))
        )
        return parsed

    def validate_review_stream(self, parsed_stream: "DataFrame") -> Tuple["DataFrame", "DataFrame"]:
        """Validates parsed/casted review stream and splits into (valid_stream, invalid_stream).
        Ensures parse/cast has run before validation.
        """
        if "__parsed_and_casted" not in parsed_stream.columns:
            parsed_stream = self.parse_review_stream(parsed_stream)

        cast_err_rating = F.coalesce(F.col("__cast_err_rating"), F.lit(False))
        cast_err_ts = F.coalesce(F.col("__cast_err_timestamp"), F.lit(False))
        cond_missing_review = F.col("review_id").isNull() | (F.length(F.col("review_id")) == 0)
        cond_missing_product = F.col("product_id").isNull() | (F.length(F.col("product_id")) == 0)
        cond_missing_text = F.col("review_text").isNull() | (F.length(F.col("review_text")) == 0)
        cond_invalid_rating = (
            F.col("rating").isNull()
            | F.isnan(F.col("rating"))
            | (F.col("rating") < 1.0)
            | (F.col("rating") > 5.0)
        )
        cond_missing_time = F.col("event_time").isNull()

        reason_col = (
            F.when(cast_err_rating, F.lit("INVALID_TYPE_RATING"))
            .when(cast_err_ts, F.lit("INVALID_TYPE_TIMESTAMP"))
            .when(cond_missing_review, F.lit("MISSING_REVIEW_ID"))
            .when(cond_missing_product, F.lit("MISSING_PRODUCT_ID"))
            .when(cond_missing_text, F.lit("MISSING_REVIEW_TEXT"))
            .when(cond_invalid_rating, F.lit("INVALID_RATING_OUT_OF_RANGE"))
            .when(cond_missing_time, F.lit("MISSING_EVENT_TIME"))
            .otherwise(F.lit("UNKNOWN_VALIDATION_ERROR"))
        )

        is_valid = (
            (~cast_err_rating)
            & (~cast_err_ts)
            & (~cond_missing_review)
            & (~cond_missing_product)
            & (~cond_missing_text)
            & (~cond_invalid_rating)
            & (~cond_missing_time)
        )

        helper_cols = [c for c in parsed_stream.columns if c.startswith("__")] + ["rating_raw", "review_date_raw"]
        existing_helpers = [c for c in helper_cols if c in parsed_stream.columns]

        valid_stream = parsed_stream.filter(is_valid).drop(*existing_helpers)
        invalid_stream = (
            parsed_stream.filter(~is_valid)
            .withColumn("quarantine_reason", reason_col)
            .withColumn("quarantined_at", F.current_timestamp())
            .drop(*[c for c in parsed_stream.columns if c.startswith("__")])
        )
        return valid_stream, invalid_stream

    def windowed_review_aggregation(
        self,
        review_stream: "DataFrame",
        window_duration: str = "1 minute",
        watermark_delay: str = "10 minutes",
    ) -> "DataFrame":
        """Apply event-time watermark, stateful primary-key deduplication on review_id,
        and tumbling window aggregation.

        Output schema per window:
        - window_start, window_end
        - product_id
        - review_count
        - avg_rating
        - negative_count (rating <= threshold)
        """
        neg_thresh = settings.analytics.sentiment_negative_threshold

        # Ensure only valid parsed records enter watermarked state store
        if "__parsed_and_casted" in review_stream.columns or "rating_raw" in review_stream.columns:
            valid_stream, _ = self.validate_review_stream(review_stream)
        else:
            valid_stream = review_stream.filter(
                F.col("review_id").isNotNull()
                & (F.length(F.trim(F.col("review_id"))) > 0)
                & F.col("product_id").isNotNull()
                & (F.length(F.trim(F.col("product_id"))) > 0)
                & F.col("rating").isNotNull()
                & (~F.isnan(F.col("rating")))
                & (F.col("rating") >= 1.0)
                & (F.col("rating") <= 5.0)
                & F.col("event_time").isNotNull()
            )

        watermarked = valid_stream.withWatermark("event_time", watermark_delay)
        if hasattr(watermarked, "dropDuplicatesWithinWatermark"):
            deduped = watermarked.dropDuplicatesWithinWatermark(["review_id"])
        else:
            deduped = watermarked.dropDuplicates(["review_id"])

        return (
            deduped
            .groupBy(
                F.window("event_time", window_duration),
                "product_id",
            )
            .agg(
                F.count("review_id").cast(IntegerType()).alias("review_count"),
                F.avg("rating").alias("avg_rating"),
                F.sum(
                    F.when(F.col("rating") <= neg_thresh, 1).otherwise(0)
                ).cast(IntegerType()).alias("negative_count"),
            )
            .select(
                F.col("window.start").alias("window_start"),
                F.col("window.end").alias("window_end"),
                "product_id",
                "review_count",
                F.round("avg_rating", 2).alias("avg_rating"),
                "negative_count",
            )
        )

    def _upsert_window_metrics_batch(self, batch_df: "DataFrame", batch_id: int) -> None:
        """Idempotent foreachBatch sink for 'update'/'complete' output modes.
        Merges updated (window_start, window_end, product_id) rows into target Parquet storage
        so late data within watermark updates window metrics in-place and restarts never duplicate rows.
        """
        if batch_df.rdd.isEmpty():
            return

        incoming = batch_df.withColumn("__batch_priority", F.lit(1)).localCheckpoint(eager=True)
        combined = incoming

        try:
            existing = self.spark.read.parquet(self.output_path)
            if not existing.rdd.isEmpty():
                existing_mat = existing.withColumn("__batch_priority", F.lit(0)).localCheckpoint(eager=True)
                unioned = existing_mat.unionByName(incoming, allowMissingColumns=True)
                w_spec = Window.partitionBy("window_start", "window_end", "product_id").orderBy(
                    F.col("__batch_priority").desc()
                )
                combined = (
                    unioned
                    .withColumn("__rn", F.row_number().over(w_spec))
                    .filter(F.col("__rn") == 1)
                    .drop("__rn")
                )
        except Exception:
            pass

        final_df = (
            combined
            .drop("__batch_priority")
            .orderBy("window_start", "product_id")
            .localCheckpoint(eager=True)
        )
        final_df.write.mode("overwrite").parquet(self.output_path)

    def _write_quarantine_batch(self, batch_df: "DataFrame", batch_id: int) -> None:
        """Idempotent foreachBatch sink for streaming quarantine records."""
        if batch_df.rdd.isEmpty():
            return
        incoming = batch_df.localCheckpoint(eager=True)
        combined = incoming
        try:
            existing = self.spark.read.parquet(self.quarantine_path)
            if not existing.rdd.isEmpty():
                existing_mat = existing.localCheckpoint(eager=True)
                combined = existing_mat.unionByName(incoming, allowMissingColumns=True)
                dedup_cols = [c for c in ("event_id", "review_id", "quarantine_reason") if c in combined.columns]
                if dedup_cols:
                    combined = combined.dropDuplicates(dedup_cols)
        except Exception:
            pass
        combined.localCheckpoint(eager=True).write.mode("overwrite").parquet(self.quarantine_path)

    def start_review_stream(
        self,
        output_path: Optional[str] = None,
        window_duration: str = "1 minute",
        watermark_delay: str = "10 minutes",
        output_mode: Optional[str] = None,
        source_path: Optional[str] = None,
        trigger_available_now: bool = False,
        processing_time: str = "30 seconds",
    ):
        """Start the full streaming pipeline: Source (Kafka or S3A/File) -> Parse/Cast -> Validate -> Dedup & Window -> Sink.
        Supports clean stop and restart from persistent checkpointLocation.
        """
        if output_path:
            self.output_path = SparkSessionFactory.resolve_storage_path(output_path, layer="gold")
        eff_mode = output_mode or self.output_mode
        if eff_mode not in self.VALID_OUTPUT_MODES:
            raise ValueError(f"Unsupported output_mode '{eff_mode}'.")

        self._last_stream_args = {
            "output_path": self.output_path,
            "window_duration": window_duration,
            "watermark_delay": watermark_delay,
            "output_mode": eff_mode,
            "source_path": source_path,
            "trigger_available_now": trigger_available_now,
            "processing_time": processing_time,
        }

        raw = (
            self.create_file_stream(source_path)
            if source_path
            else self.create_kafka_stream(topics=settings.kafka.topic_reviews)
        )
        parsed = self.parse_review_stream(raw)
        valid_stream, invalid_stream = self.validate_review_stream(parsed)
        windowed = self.windowed_review_aggregation(
            valid_stream, window_duration=window_duration, watermark_delay=watermark_delay
        )

        window_chk = f"{self.checkpoint_dir}/reviews_window"
        quarantine_chk = f"{self.checkpoint_dir}/reviews_quarantine"

        # 1. Start quarantine stream writer
        q_writer = (
            invalid_stream.writeStream
            .outputMode("append")
            .option("checkpointLocation", quarantine_chk)
            .foreachBatch(self._write_quarantine_batch)
        )
        if trigger_available_now:
            q_writer = q_writer.trigger(availableNow=True)
        else:
            q_writer = q_writer.trigger(processingTime=processing_time)
        self._quarantine_query = q_writer.start()

        # 2. Start windowed aggregation stream writer with fixed output mode compatibility
        w_writer = windowed.writeStream.outputMode(eff_mode).option("checkpointLocation", window_chk)
        if eff_mode == "append" and not trigger_available_now:
            w_writer = w_writer.format("parquet").option("path", self.output_path)
        else:
            # Parquet native sink does not support 'update'/'complete'; use idempotent foreachBatch upsert
            w_writer = w_writer.foreachBatch(self._upsert_window_metrics_batch)

        if trigger_available_now:
            w_writer = w_writer.trigger(availableNow=True)
        else:
            w_writer = w_writer.trigger(processingTime=processing_time)

        self._active_query = w_writer.start()
        return self._active_query

    def await_termination(self, timeout: Optional[float] = None) -> None:
        if self._quarantine_query is not None and self._quarantine_query.isActive:
            self._quarantine_query.awaitTermination(timeout)
        if self._active_query is not None and self._active_query.isActive:
            self._active_query.awaitTermination(timeout)

    def stop(self) -> None:
        """Cleanly stops active streaming queries while preserving checkpoint state for restart."""
        if self._active_query is not None:
            try:
                if self._active_query.isActive:
                    self._active_query.stop()
            except Exception:
                pass
        if self._quarantine_query is not None:
            try:
                if self._quarantine_query.isActive:
                    self._quarantine_query.stop()
            except Exception:
                pass

    def restart(self, **override_kwargs):
        """Stops any active query and restarts the streaming job from the same checkpointLocation."""
        self.stop()
        args = dict(self._last_stream_args)
        args.update(override_kwargs)
        return self.start_review_stream(**args)
