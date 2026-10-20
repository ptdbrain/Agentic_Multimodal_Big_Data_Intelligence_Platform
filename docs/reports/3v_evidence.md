# SentinelAI — Empirical 3V Big Data Evidence Report

- **Generated At**: `2026-10-20 07:11:43 UTC`
- **Runtime Mode**: `integration` (`MinIOBackend`)
- **Deterministic Seed**: `42`
- **Volume Iterations**: `1` (reporting median)

## 1. Volume
- **Executed Scale**: Empirically benchmarked from **100** to **10,000** records per batch (`sizes=[100, 1000, 10000]`).
- **Peak Measured Spark Batch Throughput**: **2,677.0 records/sec** (Stage 2: Spark S3A read, envelope unpacking, schema cast, validation, quarantine routing, window deduplication, and Silver Parquet write).
- **Peak Total End-to-End Throughput**: **2,527.3 records/sec** (Stage 1 Bronze Write + Stage 2 Spark Batch ETL).
- **Measurement Boundary**: Explicitly limited to Bronze write + Spark ETL. Kafka is not involved in this stage.
- **Quarantine & Deduplication Accounting**: Every run enforces `records_input == valid_unique + duplicate_count + quarantine_count` and tracks exact byte footprint across Bronze JSON, Silver Parquet, and Quarantine Parquet.

## 2. Velocity
- **Real Kafka Streaming Ingestion**: Measured across target rates from **50** to **200 msg/s** via `real_kafka`.
- **Peak Producer Rate**: **131.6 msg/s**; **Peak Consumer Throughput**: **1,119.2 msg/s**; **Peak E2E Rate**: **117.8 msg/s**.
- **Latency Profiling**: Empirically measures p50, p95, and p99 delivery-to-commit latencies per rate.
- **Contract Enforcement on Stream**: Malformed messages are routed to Dead-Letter Queue (`dead-letter`) before Bronze commit with zero silent drops.

## 3. Variety
- **End-to-End Multi-Format Pipeline**: Benchmarked across **7** formats/sources (`JSON, JSONL, CSV, TSV, Parquet, HTML_Synthetic_JSONLD, HTML_Tiki_Crawler`).
- **Authentic Tiki Crawler Distinction**: Strictly distinguishes between synthetic Schema.org JSON-LD HTML fixtures and authentic Tiki crawler datasets.
- **Full Parse/Validate/Transform Path**: Each format is loaded, stripped of raw PII (`user_id` hashed to `anon_<sha256[:16]>`), persisted to Bronze, and processed by `SparkBatchETLJob` into Silver and Quarantine Parquet.
