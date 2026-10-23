# SentinelAI — Empirical 3V Big Data Evidence Report

- **Generated At**: `2026-10-21 05:19:04 UTC`
- **Runtime Mode**: `local` (`LocalBackend`)
- **Deterministic Seed**: `42`
- **Volume Iterations**: `3` (reporting median with min-max stability range)

## 1. Volume
- **Executed Scale**: Empirically benchmarked from **10,000** to **500,000** records per batch (`sizes=[10000, 100000, 500000]`).
- **Peak Measured Spark Batch Throughput**: **7,801.3 records/sec** (Stage 2: Spark S3A read, envelope unpacking, schema cast, validation, quarantine routing, window deduplication, and Silver Parquet write).
- **Peak Total End-to-End Throughput**: **6,297.1 records/sec** (Stage 1 Bronze Write + Stage 2 Spark Batch ETL).
- **Measurement Boundary**: Explicitly limited to Bronze write + Spark ETL. Kafka is not involved in this stage.
- **Resource Tracking (CPU / RAM)**: Continuously sampled at 50ms across the full process tree (Python harness + Java JVM / Py4J children). Reflects local client/driver benchmark process tree, not distributed cluster total.
- **Quarantine & Deduplication Accounting**: Every run enforces `records_input == valid_unique + duplicate_count + quarantine_count` and tracks exact byte footprint across Bronze JSON, Silver Parquet, and Quarantine Parquet.

## 2. Velocity
- **Real Kafka Streaming Ingestion**: Measured across target rates from **50** to **1,000 msg/s** via `offline_virtual_queue`.
- **Peak Producer Rate**: **475.4 msg/s**; **Peak Consumer Throughput**: **461.7 msg/s**; **Peak E2E Rate**: **461.7 msg/s**.
- **Latency Profiling**: Empirically measures p50, p95, and p99 delivery-to-commit latencies per rate.
- **Contract Enforcement on Stream**: Malformed messages are routed to Dead-Letter Queue (`dead-letter`) before Bronze commit with zero silent drops.

## 3. Variety
- **End-to-End Multi-Format Pipeline**: Benchmarked across **7** formats/sources (`JSON, JSONL, CSV, TSV, Parquet, HTML_Synthetic_JSONLD, JSONL_Tiki_Crawler`).
- **Authentic Tiki Crawler Distinction**: Strictly distinguishes between synthetic Schema.org JSON-LD HTML fixtures and authentic Tiki crawler datasets.
- **Full Parse/Validate/Transform Path**: Each format is loaded, stripped of raw PII (`user_id` hashed to `anon_<sha256[:16]>`), persisted to Bronze, and processed by `SparkBatchETLJob` into Silver and Quarantine Parquet.
