# SentinelAI 3V Empirical Benchmark Report

**Generated At:** `2026-10-20 07:11:43 UTC`  
**Runtime Mode:** `integration`  
**Storage Backend:** `MinIOBackend`  
**Base Deterministic Seed:** `42`  
**PII Anonymization at Ingest:** `True`  
**Volume Iterations (Median):** `1`

## 1. Volume Benchmark (Bronze Storage Write + Spark Batch ETL -> Silver & Quarantine Parquet)

> [!NOTE]
> **Measurement Boundary:** Stage 1 Bronze Storage Write + Stage 2 Spark Batch ETL (Read Bronze S3A/JSON -> Schema Cast -> Validation & Quarantine -> Window Deduplication -> Silver Parquet Write). Excludes Kafka streaming.

| Seed | Input Records | Valid (Silver) | Quarantined | Deduplicated | DQ Score | Bronze Write (s) | Spark ETL (s) | Total Time (s) | Spark Rate (rec/s) | Total Rate (rec/s) | CPU % | RAM (MB) | Bronze (B) | Silver (B) | Quarantine (B) |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| 142 | 100 | 92 | 5 | 3 | 92.0% | 0.0127s | 2.8036s | 2.8162s | 35.7 | 35.5 | 2.2% | 148.3 | 64,262 | 11,424 | 5,708 |
| 1042 | 1,000 | 920 | 50 | 30 | 92.0% | 0.0387s | 2.8737s | 2.9124s | 348.0 | 343.4 | 5.4% | 149.2 | 642,052 | 52,327 | 7,801 |
| 10042 | 10,000 | 9,200 | 500 | 300 | 92.0% | 0.2214s | 3.7355s | 3.9568s | 2,677.0 | 2,527.3 | 4.7% | 158.4 | 6,433,642 | 412,904 | 26,277 |

## 2. Velocity Benchmark (Kafka Producer -> KRaft Broker -> RawConsumer -> Bronze / DLQ)

> [!NOTE]
> **Transport Mode:** `real_kafka` enforces live Kafka KRaft broker connection; offline buffer is disallowed in integration mode. Latencies p50/p95/p99 measure true message dispatch to consumer commit.

| Seed | Transport | Target (msg/s) | Sent | Consumed | Valid (Bronze) | DLQ | Lag | Produce Rate (msg/s) | Consume Rate (msg/s) | Latency p50 (ms) | Latency p95 (ms) | Latency p99 (ms) | E2E Time (s) | E2E Rate (msg/s) | Bronze (B) |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| 92 | real_kafka | 50 | 100 | 100 | 96 | 4 | 0 | 42.4 | 399.8 | 2611.14ms | 2611.15ms | 2611.15ms | 2.6112s | 38.3 | 75,407 |
| 242 | real_kafka | 200 | 400 | 400 | 384 | 16 | 0 | 131.6 | 1,119.2 | 3396.96ms | 3397.01ms | 3397.01ms | 3.3970s | 117.8 | 302,720 |

## 3. Variety Benchmark (Multi-Format Ingest -> PII Hash -> Bronze -> Spark Parse/Validate/Transform -> Silver)

> [!NOTE]
> **Format Categories:** Distinctly separates synthetic file fixtures, semi-structured Schema.org JSON-LD HTML, and authentic Tiki crawler datasets. All formats undergo strict PII anonymization and canonical schema validation.

| Seed | Format / Source | Category | Source (B) | Input Records | Valid (Silver) | Quarantined | Deduplicated | DQ Score | Duration (s) | Throughput (rec/s) | Silver (B) | Quarantine (B) |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| 42 | JSON | synthetic_file | 43,124 | 100 | 93 | 5 | 2 | 93.0% | 2.2851s | 43.8 | 11,478 | 5,699 |
| 42 | JSONL | synthetic_file | 43,124 | 100 | 93 | 5 | 2 | 93.0% | 2.3577s | 42.4 | 11,478 | 5,699 |
| 42 | CSV | synthetic_file | 21,418 | 100 | 94 | 4 | 2 | 94.0% | 2.8175s | 35.5 | 11,441 | 5,819 |
| 42 | TSV | synthetic_file | 21,272 | 100 | 94 | 4 | 2 | 94.0% | 2.4892s | 40.2 | 11,441 | 5,819 |
| 42 | Parquet | synthetic_file | 14,359 | 100 | 93 | 5 | 2 | 93.0% | 2.5648s | 39.0 | 11,478 | 5,699 |
| 42 | HTML_Synthetic_JSONLD | synthetic_html | 33,220 | 100 | 93 | 5 | 2 | 93.0% | 2.3616s | 42.3 | 10,994 | 5,719 |
| 42 | HTML_Tiki_Crawler | real_crawler_html | 3,042 | 6 | 6 | 0 | 0 | 100.0% | 1.7480s | 3.4 | 6,419 | 0 |

## 4. Accounting Reconciliation & Verification

- **Volume Accounting Invariant:** `input_count == valid_count + duplicate_count + quarantine_count` verified on 100% of runs.
- **Velocity Accounting Invariant:** `messages_consumed == valid_count + quarantine_dlq_count` verified on 100% of runs.
- **Variety Accounting Invariant:** `input_count == valid_count + duplicate_count + quarantine_count` verified across all 7 formats.
- **PII Sanitization Invariant:** Zero unanonymized customer PII stored in Silver Parquet across all formats.
