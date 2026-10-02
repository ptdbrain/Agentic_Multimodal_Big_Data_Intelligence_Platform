"""Real Spark Structured Streaming job consuming from Kafka topics.
Features:
- readStream from Kafka
- Parse JSON with schema enforcement
- Event-time based windowed aggregation
- Watermarking for late data handling
- Checkpointing for fault tolerance
"""
from typing import Optional
from config.settings import settings

class SparkStreamingJob:
    """Spark Structured Streaming consuming from Kafka."""
    
    def __init__(self, spark_session=None, checkpoint_dir: Optional[str] = None):
        self.checkpoint_dir = checkpoint_dir or "s3a://sentinel-data/checkpoints/streaming/"
        self._spark = spark_session
    
    @property
    def spark(self):
        if self._spark is None:
            from pyspark.sql import SparkSession
            self._spark = (
                SparkSession.builder
                .master(settings.spark.master)
                .appName(f"{settings.spark.app_name}-Streaming")
                .config("spark.jars.packages",
                        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
                        "org.apache.hadoop:hadoop-aws:3.3.4")
                .config("spark.sql.streaming.checkpointLocation", self.checkpoint_dir)
                .getOrCreate()
            )
        return self._spark
    
    def create_kafka_stream(self, topics: Optional[str] = None):
        """Create a readStream from Kafka topics.
        
        Returns a streaming DataFrame with columns: key, value, topic, partition, offset, timestamp
        """
        from pyspark.sql import functions as F
        from pyspark.sql.types import StringType
        
        topic_list = topics or ",".join([
            settings.kafka.topic_reviews,
            settings.kafka.topic_products,
            settings.kafka.topic_prices,
            settings.kafka.topic_events
        ])
        
        kafka_df = (
            self.spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", settings.kafka.bootstrap_servers)
            .option("subscribe", topic_list)
            .option("startingOffsets", "earliest")
            .option("failOnDataLoss", "false")
            .load()
        )
        
        # Cast key and value from bytes to string
        return kafka_df.selectExpr(
            "CAST(key AS STRING) as key",
            "CAST(value AS STRING) as value",
            "topic",
            "partition",
            "offset",
            "timestamp"
        )
    
    def parse_review_stream(self, raw_stream):
        """Parse review JSON from Kafka stream and apply schema."""
        from pyspark.sql import functions as F
        from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType
        
        # Define review schema for the envelope payload
        review_schema = StructType([
            StructField("event_id", StringType()),
            StructField("event_type", StringType()),
            StructField("source", StringType()),
            StructField("ingested_at", StringType()),
            StructField("payload", StructType([
                StructField("review_id", StringType()),
                StructField("product_id", StringType()),
                StructField("rating", FloatType()),
                StructField("review_text", StringType()),
                StructField("review_date", StringType()),
            ]))
        ])
        
        parsed = (
            raw_stream
            .filter(F.col("topic") == settings.kafka.topic_reviews)
            .select(
                F.from_json(F.col("value"), review_schema).alias("data"),
                F.col("timestamp").alias("kafka_timestamp")
            )
            .select(
                "data.payload.review_id",
                "data.payload.product_id", 
                "data.payload.rating",
                "data.payload.review_date",
                "data.event_type",
                "data.source",
                "kafka_timestamp"
            )
        )
        
        # Use event time (review_date) or fall back to kafka_timestamp
        return parsed.withColumn(
            "event_time",
            F.coalesce(
                F.to_timestamp("review_date"),
                F.col("kafka_timestamp")
            )
        )
    
    def windowed_review_aggregation(self, review_stream, 
                                      window_duration: str = "1 minute",
                                      watermark_delay: str = "10 minutes"):
        """Apply tumbling window aggregation with watermark (plan XXXVI-XXXIX).
        
        Output schema per window:
        - window_start, window_end
        - product_id
        - review_count
        - avg_rating
        - negative_count (rating <= threshold)
        """
        from pyspark.sql import functions as F
        
        neg_thresh = settings.analytics.sentiment_negative_threshold
        
        return (
            review_stream
            .withWatermark("event_time", watermark_delay)
            .groupBy(
                F.window("event_time", window_duration),
                "product_id"
            )
            .agg(
                F.count("review_id").alias("review_count"),
                F.avg("rating").alias("avg_rating"),
                F.sum(
                    F.when(F.col("rating") <= neg_thresh, 1).otherwise(0)
                ).alias("negative_count")
            )
            .select(
                F.col("window.start").alias("window_start"),
                F.col("window.end").alias("window_end"),
                "product_id",
                "review_count",
                F.round("avg_rating", 2).alias("avg_rating"),
                "negative_count"
            )
        )
    
    def start_review_stream(self, output_path: Optional[str] = None,
                            window_duration: str = "1 minute",
                            watermark_delay: str = "10 minutes"):
        """Start the full streaming pipeline: Kafka -> Parse -> Window -> Sink.
        
        Writes to MinIO Gold streaming metrics as Parquet.
        """
        raw = self.create_kafka_stream(topics=settings.kafka.topic_reviews)
        parsed = self.parse_review_stream(raw)
        windowed = self.windowed_review_aggregation(parsed, window_duration, watermark_delay)
        
        sink_path = output_path or "s3a://sentinel-data/gold/streaming_metrics/reviews/"
        
        query = (
            windowed.writeStream
            .outputMode("update")
            .format("parquet")
            .option("path", sink_path)
            .option("checkpointLocation", f"{self.checkpoint_dir}/reviews_window/")
            .trigger(processingTime="30 seconds")
            .start()
        )
        
        return query
