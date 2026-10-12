# SentinelAI — Empirical 3V Big Data Evidence Report

- **Generated At**: `2026-10-12 05:01:02 UTC`
- **Runtime Mode**: `integration` (`MinIOBackend`)
- **Deterministic Seed**: `42`

## 1. Volume
- **Executed Scale**: Empirically benchmarked from **100** to **10,000** records per batch (`sizes=[100, 1000, 5000, 10000]`).
- **Peak Measured Spark Batch Throughput**: **476.1 records/sec** (including Bronze S3A read, envelope unpacking, schema cast, validation, quarantine routing, window deduplication, and Silver Parquet write).
- **Quarantine & Deduplication Accounting**: Every run enforces `records_input == valid_before_dedup + quarantine_count` and tracks exact byte footprint across Bronze JSON, Silver Parquet, and Quarantine Parquet.

## 2. Velocity
- **Real Kafka Streaming Ingestion**: Measured across target rates from **50** to **1,000 msg/s** via `real_kafka`.
- **Peak Producer Rate**: **349.6 msg/s**; **Peak End-to-End (Produce -> Kafka -> RawConsumer -> Schema Validate -> Bronze Write) Rate**: **171.4 msg/s**.
- **Contract Enforcement on Stream**: Malformed messages are routed to Dead-Letter Queue (`dead-letter`) before Bronze commit.

## 3. Variety
- **End-to-End Multi-Format Pipeline**: Benchmarked across **6** formats/sources (`JSON, JSONL, CSV, TSV, Parquet, HTML_JSONLD_Crawler`).
- **Full Parse/Validate/Transform Path**: Each format is loaded, stripped of raw PII (`user_id` hashed to `anon_<sha256[:16]>`), persisted to Bronze, and processed by `SparkBatchETLJob` into Silver and Quarantine Parquet.
