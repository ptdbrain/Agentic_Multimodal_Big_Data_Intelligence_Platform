"""Real Spark Structured Streaming job consuming from Kafka or Bronze S3A/File streams.
Features:
- Shared SparkSession & S3A configuration via SparkSessionFactory
- Parse & cast BEFORE validation (malformed types flagged and routed to Quarantine)
- Event-time watermarking (withWatermark) for late-arriving data
- Stateful primary-key deduplication (dropDuplicates(["source_group", "review_id"])) within watermark
- Valid output modes:
  * "update" (default): uses foreachBatch idempotent upsert to Parquet on S3A/local
  * "append": uses the same source-checked foreachBatch sink for finalized windows
- Persistent checkpointLocation and clean stop/restart recovery without data loss or duplicate metrics
"""
from pathlib import Path
from typing import Optional, Tuple, Dict, Any, List
from config.settings import settings
from spark.spark_session import SparkSessionFactory
from spark.storage_safe import SafeParquetIO
from storage.write_lock import synchronized_path

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


def _privacy_safe_quarantine_payload(encoded):
    """Keep repairable source fields, applying the ingest identifier policy."""
    import json
    from ingestion.privacy import sanitize_record_pii
    def clean(value):
        if isinstance(value,dict):
            return sanitize_record_pii({key:clean(item) for key,item in value.items()})
        if isinstance(value,list):
            return [clean(item) for item in value]
        return value
    try:
        value = json.loads(encoded)
    except (TypeError,ValueError):
        return None
    return json.dumps(clean(value),ensure_ascii=False,sort_keys=True)


class SparkStreamingJob:
    """Production Spark Structured Streaming engine with unified S3A, watermarking,
    pre-validation parse/cast, key deduplication, checkpointing, and restart recovery.
    """

    VALID_OUTPUT_MODES = ("update", "append", "complete")
    PROVENANCE_FIELDS = (
        'source_group','source_name','source_record_id','source_url','acquisition_method',
        'collected_at','evidence_sha256','evidence_ref','parser_version','schema_version',
        'collection_run_id','generator_seed','generator_config',
    )


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
        payload_fields = [
            StructField(name,StringType(),True) for name in (
                'review_id','product_id','user_id','rating','rating_raw','review_title',
                'review_text','review_date','review_date_raw','timestamp','verified_purchase','source',
            ) + SparkStreamingJob.PROVENANCE_FIELDS
        ]
        envelope_fields = [
            StructField(name,StringType(),True) for name in
            ('event_id','event_type','source','ingested_at','timestamp') + SparkStreamingJob.PROVENANCE_FIELDS
        ]
        # The actual JSON decoder supports both flat and envelope records.
        names = {field.name for field in envelope_fields}
        envelope_fields.extend(field for field in payload_fields if field.name not in names)
        return StructType(envelope_fields + [StructField('payload',StructType(payload_fields),True)])

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
            .option("kafka.bootstrap.servers", settings.kafka.bootstrap_servers)
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
        return reader.json(resolved_input).withColumn('source_file',F.input_file_name())

    def parse_review_stream(self, raw_stream: "DataFrame") -> "DataFrame":
        """Retain source evidence and parse only observed business clocks."""
        from spark.batch.spark_etl_job import SparkBatchETLJob
        raw_cols = set(raw_stream.columns)
        from_kafka = 'value' in raw_cols
        if from_kafka:
            # Subscription defines the caller's review topics, including --topic.
            filtered = raw_stream
            broker_time = F.col('timestamp') if 'timestamp' in raw_cols else F.lit(None).cast(TimestampType())
            frame = filtered.select(
                F.from_json(F.col('value'),self.get_envelope_raw_schema()).alias('data'),
                broker_time.alias('__broker_timestamp'),
                F.sha2(F.col('value'),256).alias('raw_payload_sha256'),
                F.col('value').alias('__raw_payload_json'),
                *[(F.col(name) if name in raw_cols else F.lit(None)).alias('source_' + name)
                  for name in ('topic','partition','offset')],
            ).select('data.*','__broker_timestamp','raw_payload_sha256','__raw_payload_json',
                     'source_topic','source_partition','source_offset')
        else:
            identity = F.to_json(F.struct(*[F.col(name) for name in sorted(raw_cols - {'source_file'})]))
            frame = raw_stream.withColumn('raw_payload_sha256',F.sha2(identity,256)).withColumn('__raw_payload_json',identity)
        columns = set(frame.columns)
        payload_type = frame.schema['payload'].dataType if 'payload' in columns else None
        payload_fields = set(payload_type.fieldNames()) if isinstance(payload_type,StructType) else set()

        def outer(name):
            return F.col(name).cast(StringType()) if name in columns else F.lit(None).cast(StringType())

        def business(name):
            if payload_fields:
                inner = F.col('payload.' + name).cast(StringType()) if name in payload_fields else F.lit(None).cast(StringType())
                # A present nested payload is authoritative, including NULLs.
                return F.when(F.col('payload').isNotNull(),inner).otherwise(outer(name))
            return outer(name)

        def provenance(name):
            # Envelope schema_version describes transport; do not infer a
            # missing payload schema version from it.
            if payload_fields and name != 'schema_version':
                return F.coalesce(business(name),outer(name))
            return business(name)

        conflict = F.lit(False)
        if payload_fields:
            for name in ('source_group','acquisition_method'):
                inner, root = F.trim(business(name)), F.trim(outer(name))
                conflict = conflict | (inner.isNotNull() & root.isNotNull() & (inner != root))
        business_date = F.coalesce(business('review_date_raw'),business('review_date'),business('timestamp'))
        if payload_fields:
            business_date = F.coalesce(business_date,outer('timestamp'))
        elif 'event_time' in columns:
            business_date = F.coalesce(business_date,outer('event_time'))
        kafka_time = (F.col('__broker_timestamp') if from_kafka else
                      SparkBatchETLJob._parse_timestamp(outer('ingested_at')))
        extracted = frame.select(
            outer('event_id').alias('event_id'),
            outer('raw_payload_sha256').alias('raw_payload_sha256'),
            outer('__raw_payload_json').alias('__raw_payload_json'),
            *[outer(name).alias(name) for name in ('source_topic','source_partition','source_offset','source_file')],
            F.coalesce(outer('event_type'),F.lit('NEW_REVIEW')).alias('event_type'),
            F.coalesce(business('source'),outer('source')).alias('source'),
            outer('source').alias('envelope_source'),
            outer('ingested_at').alias('ingested_at'),
            business('review_id').alias('review_id'),
            business('product_id').alias('product_id'),
            F.coalesce(business('rating_raw'),business('rating')).alias('rating_raw'),
            business('review_text').alias('review_text'),
            business_date.alias('review_date_raw'),
            kafka_time.alias('kafka_timestamp'),
            conflict.alias('__source_conflict'),
            *[provenance(name).alias(name) for name in self.PROVENANCE_FIELDS],
        )
        group = F.trim(F.col('source_group'))
        extracted = extracted.withColumn('source_group',
            F.when(group.isNull() | (group == ''),'unverified').otherwise(group)
        ).withColumn('acquisition_method',F.trim(F.col('acquisition_method')))
        rating_raw, date_raw = F.trim(F.col('rating_raw')), F.trim(F.col('review_date_raw'))
        rating = rating_raw.cast(DoubleType())
        event_time = SparkBatchETLJob._parse_timestamp(date_raw)
        return (extracted
            .withColumn('review_id',F.trim(F.col('review_id')))
            .withColumn('product_id',F.trim(F.col('product_id')))
            .withColumn('review_text',F.trim(F.col('review_text')))
            .withColumn('__cast_err_rating',F.col('rating_raw').isNotNull() & (F.length(rating_raw)>0)
                        & (rating.isNull() | F.isnan(rating)))
            .withColumn('rating',rating)
            .withColumn('__cast_err_timestamp',F.col('review_date_raw').isNotNull() & (F.length(date_raw)>0)
                        & event_time.isNull())
            .withColumn('review_date',date_raw)
            .withColumn('event_time',event_time)
            .withColumn('__parsed_and_casted',F.lit(True)))

    @staticmethod
    def _valid_source_metadata():
        group, method = F.col('source_group'), F.col('acquisition_method')
        return F.coalesce(
            (group == 'unverified')
            | ((group == 'marketplace') & method.isin('live','snapshot_replay'))
            | ((group == 'simulation') & (method == 'generated')),
            F.lit(False),
        )

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
        conflict = F.coalesce(F.col('__source_conflict'),F.lit(False)) if '__source_conflict' in parsed_stream.columns else F.lit(False)
        cond_invalid_source = conflict | ~self._valid_source_metadata()

        reason_col = (
            F.when(cast_err_rating, F.lit("INVALID_TYPE_RATING"))
            .when(cast_err_ts, F.lit("INVALID_TYPE_TIMESTAMP"))
            .when(cond_missing_review, F.lit("MISSING_REVIEW_ID"))
            .when(cond_missing_product, F.lit("MISSING_PRODUCT_ID"))
            .when(cond_missing_text, F.lit("MISSING_REVIEW_TEXT"))
            .when(cond_invalid_rating, F.lit("INVALID_RATING_OUT_OF_RANGE"))
            .when(cond_missing_time, F.lit("MISSING_EVENT_TIME"))
            .when(cond_invalid_source, F.lit("INVALID_SOURCE_METADATA"))
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
            & (~cond_invalid_source)
        )

        helper_cols = [c for c in parsed_stream.columns if c.startswith("__")] + ["rating_raw", "review_date_raw"]
        existing_helpers = [c for c in helper_cols if c in parsed_stream.columns]

        valid_stream = parsed_stream.filter(is_valid).drop(*existing_helpers)
        rejected = parsed_stream.filter(~is_valid)
        if '__raw_payload_json' in parsed_stream.columns:
            rejected = rejected.withColumn('quarantine_payload_json',
                F.udf(_privacy_safe_quarantine_payload,StringType())(F.col('__raw_payload_json')))
        invalid_stream = (
            rejected
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

        if 'source_group' not in valid_stream.columns:
            valid_stream = valid_stream.withColumn('source_group',F.lit('unverified'))
        if 'acquisition_method' not in valid_stream.columns:
            valid_stream = valid_stream.withColumn('acquisition_method',F.lit(None).cast(StringType()))
        valid_stream = valid_stream.filter(self._valid_source_metadata())
        watermarked = valid_stream.withWatermark("event_time", watermark_delay)
        if hasattr(watermarked, "dropDuplicatesWithinWatermark"):
            deduped = watermarked.dropDuplicatesWithinWatermark(["source_group","review_id"])
        else:
            deduped = watermarked.dropDuplicates(["source_group","review_id"])

        return (
            deduped
            .groupBy(
                F.window("event_time", window_duration),
                "product_id",
                "source_group",
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
                "source_group",
                "review_count",
                F.round("avg_rating", 2).alias("avg_rating"),
                "negative_count",
            )
        )

    @synchronized_path("output_path")
    def _upsert_window_metrics_batch(self, batch_df: "DataFrame", batch_id: int) -> None:
        """Idempotent foreachBatch sink for 'update'/'complete' output modes.
        Merges updated (window_start, window_end, product_id) rows into target Parquet storage
        so late data within watermark updates window metrics in-place and restarts never duplicate rows.
        """
        if batch_df.rdd.isEmpty():
            return

        if 'source_group' not in batch_df.columns:
            batch_df = batch_df.withColumn('source_group',F.lit('unverified'))
        groups = {row['source_group'] for row in batch_df.select('source_group').distinct().collect()}
        if len(groups) != 1 or not groups.issubset({'unverified','marketplace','simulation'}):
            raise ValueError('Mixed or invalid source groups require separate streaming outputs')
        incoming = batch_df.withColumn("__batch_priority", F.lit(1)).localCheckpoint(eager=True)
        combined = incoming

        existing = SafeParquetIO.read_parquet_safely(self.spark, self.output_path, check_exists_first=True)
        if existing is not None and not existing.rdd.isEmpty():
            if 'source_group' not in existing.columns:
                existing = existing.withColumn('source_group',F.lit('unverified'))
            existing_groups = {row['source_group'] for row in existing.select('source_group').distinct().collect()}
            if existing_groups != groups:
                raise ValueError('Different source group requires an isolated streaming output')
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

        final_df = (
            combined
            .drop("__batch_priority")
            .orderBy("window_start", "product_id")
            .localCheckpoint(eager=True)
        )
        final_count = final_df.count()
        SafeParquetIO.publish_parquet_atomic(
            spark=self.spark,
            df=final_df,
            target_path=self.output_path,
            expected_count=final_count,
        )

    @synchronized_path("quarantine_path")
    def _write_quarantine_batch(self, batch_df: "DataFrame", batch_id: int) -> None:
        """Idempotent foreachBatch sink for streaming quarantine records."""
        if batch_df.rdd.isEmpty():
            return
        incoming = batch_df.localCheckpoint(eager=True)
        combined = incoming

        existing = SafeParquetIO.read_parquet_safely(self.spark, self.quarantine_path, check_exists_first=True)
        if existing is not None and not existing.rdd.isEmpty():
            existing_mat = existing.localCheckpoint(eager=True)
            combined = existing_mat.unionByName(incoming, allowMissingColumns=True)
        # A stable event ID can have several failed payload versions. Preserve
        # those versions; only repeated processing of the same evidence is a
        # duplicate. Processing time must not create a new failed observation.
        identity_columns = sorted(name for name in combined.columns
                                  if name != 'quarantined_at' and not name.startswith('__'))
        projected_identity = F.sha2(
            F.to_json(F.struct(*[F.col(name) for name in identity_columns])),256)
        identity = (F.coalesce(F.col('raw_payload_sha256'),projected_identity)
                    if 'raw_payload_sha256' in combined.columns else projected_identity)
        combined = combined.withColumn('__quarantine_key',identity)
        if 'quarantined_at' in combined.columns:
            window = Window.partitionBy('__quarantine_key').orderBy(F.col('quarantined_at').desc_nulls_last())
            combined = combined.withColumn('__latest',F.row_number().over(window)).filter(F.col('__latest') == 1).drop('__latest')
        else:
            combined = combined.dropDuplicates(['__quarantine_key'])
        combined = combined.drop('__quarantine_key')

        final_df = combined.localCheckpoint(eager=True)
        final_count = final_df.count()
        SafeParquetIO.publish_parquet_atomic(
            spark=self.spark,
            df=final_df,
            target_path=self.quarantine_path,
            expected_count=final_count,
        )

    def start_review_stream(
        self,
        output_path: Optional[str] = None,
        window_duration: str = "1 minute",
        watermark_delay: str = "10 minutes",
        output_mode: Optional[str] = None,
        source_path: Optional[str] = None,
        trigger_available_now: bool = False,
        processing_time: str = "30 seconds",
        topics: Optional[str] = None,
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
            "topics": topics,
        }

        raw = (
            self.create_file_stream(source_path)
            if source_path
            else self.create_kafka_stream(topics=topics or settings.kafka.topic_reviews)
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
        # The common sink enforces source isolation for every output mode.
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
