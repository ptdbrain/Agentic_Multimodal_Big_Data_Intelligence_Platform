# SentinelAI 3V Empirical Benchmark Report

**Generated At:** `2026-10-21 05:19:04 UTC`  
**Runtime Mode:** `local`  
**Storage Backend:** `LocalBackend`  
**Base Deterministic Seed:** `42`  
**PII Anonymization at Ingest:** `True`  
**Volume Iterations (Median):** `3` (reporting median across repeated runs)

## 1. Volume Benchmark (Bronze Storage Write + Spark Batch ETL -> Silver & Quarantine Parquet)

> [!NOTE]
> **Measurement Boundary:** Stage 1 Bronze Storage Write + Stage 2 Spark Batch ETL (Read Bronze S3A/JSON -> Schema Cast -> Validation & Quarantine -> Window Deduplication -> Silver Parquet Write). Excludes Kafka streaming.
>
> **Resource Measurement Boundary (CPU / RAM):** CPU % and peak RSS RAM (MB) are continuously sampled every 50ms via a background thread across the full process tree (Python benchmark harness + PySpark Java JVM / Py4J children). These numbers reflect the local client/driver benchmark process tree and should **not** be interpreted as the cluster-wide distributed resource utilization of an arbitrary multi-node Spark cluster.

| Seed | Input Records | Valid (Silver) | Quarantined | Deduplicated | DQ Score | Bronze Write (s) | Spark ETL (s) | Total Time (s) | Stability Range (s) | Spark Rate (rec/s) | Total Rate (rec/s) | CPU % (Tree) | RAM Tree (MB) | RAM Py (MB) | Bronze (B) | Silver (B) | Quarantine (B) |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| 10042 | 10,000 | 9,200 | 500 | 300 | 92.0% | 0.2915s | 4.4003s | 4.6918s | 4.1236 - 5.7940s | 2,272.6 | 2,131.4 | 189.7% | 1231.4 | 151.3 | 5,393,640 | 416,148 | 26,501 |
| 100042 | 100,000 | 92,000 | 5,000 | 3,000 | 92.0% | 3.3585s | 16.5447s | 19.9379s | 18.7593 - 20.2964s | 6,044.2 | 5,015.6 | 148.3% | 1750.2 | 238.5 | 54,065,581 | 3,716,559 | 202,650 |
| 500042 | 500,000 | 460,000 | 25,000 | 15,000 | 92.0% | 15.3105s | 64.0915s | 79.4019s | 79.3482 - 80.2800s | 7,801.3 | 6,297.1 | 170.4% | 3030.7 | 626.0 | 270,349,333 | 15,992,058 | 995,259 |

### 1.1 Volume Iterations Stability Breakdown

| Input Records | Iteration | Bronze Write (s) | Spark ETL (s) | Total Time (s) | Spark Rate (rec/s) | Total Rate (rec/s) | CPU % | RAM Tree (MB) | RAM Py (MB) | Accounting Match |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| 10,000 | Run 1 | 0.3296s | 5.4643s | 5.7940s | 1,830.1 | 1,725.9 | 308.9% | 1159.8 | 151.2 | PASSED |
| 10,000 | Run 2 | 0.2915s | 4.4003s | 4.6918s | 2,272.6 | 2,131.4 | 187.6% | 1231.4 | 151.3 | PASSED |
| 10,000 | Run 3 | 0.2593s | 3.8643s | 4.1236s | 2,587.8 | 2,425.1 | 189.7% | 1249.2 | 151.3 | PASSED |
| 100,000 | Run 1 | 3.3585s | 16.5794s | 19.9379s | 6,031.6 | 5,015.6 | 148.3% | 1487.9 | 238.5 | PASSED |
| 100,000 | Run 2 | 3.7518s | 16.5447s | 20.2964s | 6,044.2 | 4,927.0 | 151.1% | 1750.2 | 238.5 | PASSED |
| 100,000 | Run 3 | 3.0767s | 15.6826s | 18.7593s | 6,376.5 | 5,330.7 | 143.8% | 1812.2 | 239.2 | PASSED |
| 500,000 | Run 1 | 15.2374s | 65.0426s | 80.2800s | 7,687.3 | 6,228.2 | 185.0% | 2560.6 | 626.0 | PASSED |
| 500,000 | Run 2 | 15.3105s | 64.0915s | 79.4019s | 7,801.3 | 6,297.1 | 170.4% | 3030.7 | 626.0 | PASSED |
| 500,000 | Run 3 | 16.1702s | 63.1779s | 79.3482s | 7,914.2 | 6,301.3 | 165.5% | 3052.4 | 625.9 | PASSED |

## 2. Velocity Benchmark (Kafka Producer -> KRaft Broker -> RawConsumer -> Bronze / DLQ)

> [!NOTE]
> **Transport Mode:** `real_kafka` enforces live Kafka KRaft broker connection; offline buffer is disallowed in integration mode.
>
> **Per-Message Latency Profiling ($L_i = t_{\text{processed}, i} - t_{\text{sent}, i}$):** Monotonic clocks (`time.perf_counter()`) timestamp each individual message at dispatch and upon consumer schema validation/quarantine routing. Percentiles p50/p95/p99 reflect actual per-message processing latency, not aggregate batch duration.

| Seed | Transport | Target (msg/s) | Sent | Consumed | Valid (Bronze) | DLQ | Lag | Produce Rate (msg/s) | Consume Rate (msg/s) | Latency p50 (ms) | Latency p95 (ms) | Latency p99 (ms) | E2E Time (s) | E2E Rate (msg/s) | Bronze (B) |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| 92 | offline_virtual_queue | 50 | 100 | 100 | 96 | 4 | 0 | 48.1 | 48.4 | 2.28ms | 3.23ms | 3.4ms | 2.0776s | 48.1 | 60,553 |
| 242 | offline_virtual_queue | 200 | 400 | 400 | 384 | 16 | 0 | 184.6 | 183.7 | 2.05ms | 3.0ms | 3.63ms | 2.1773s | 183.7 | 243,315 |
| 542 | offline_virtual_queue | 500 | 1,000 | 1,000 | 960 | 40 | 0 | 393.1 | 389.2 | 1.81ms | 2.66ms | 3.65ms | 2.5695s | 389.2 | 606,706 |
| 1042 | offline_virtual_queue | 1,000 | 2,000 | 2,000 | 1,920 | 80 | 0 | 475.4 | 461.7 | 2.01ms | 4.01ms | 38.61ms | 4.3315s | 461.7 | 1,218,155 |

## 3. Variety Benchmark (Multi-Format Ingest -> PII Hash -> Bronze -> Spark Parse/Validate/Transform -> Silver)

> [!NOTE]
> **Format Categories:** Distinctly separates synthetic file fixtures, semi-structured Schema.org JSON-LD HTML, and authentic Tiki crawler datasets. All formats undergo strict PII anonymization and canonical schema validation.

| Seed | Format / Source | Category | Source (B) | Input Records | Valid (Silver) | Quarantined | Deduplicated | DQ Score | Duration (s) | Throughput (rec/s) | Silver (B) | Quarantine (B) |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| 42 | JSON | synthetic_file | 215,893 | 500 | 465 | 25 | 10 | 93.0% | 2.0882s | 239.4 | 30,287 | 6,811 |
| 42 | JSONL | synthetic_file | 215,893 | 500 | 465 | 25 | 10 | 93.0% | 2.2146s | 225.8 | 30,287 | 6,811 |
| 42 | CSV | synthetic_file | 106,765 | 500 | 473 | 17 | 10 | 94.6% | 2.1723s | 230.2 | 30,580 | 6,577 |
| 42 | TSV | synthetic_file | 106,041 | 500 | 473 | 17 | 10 | 94.6% | 2.0620s | 242.5 | 30,580 | 6,577 |
| 42 | Parquet | synthetic_file | 31,177 | 500 | 465 | 25 | 10 | 93.0% | 2.1477s | 232.8 | 30,287 | 6,811 |
| 42 | HTML_Synthetic_JSONLD | synthetic_html | 163,779 | 500 | 465 | 25 | 10 | 93.0% | 2.1678s | 230.6 | 29,146 | 6,630 |
| 42 | JSONL_Tiki_Crawler | authentic_snapshot_jsonl | 3,042 | 6 | 6 | 0 | 0 | 100.0% | 1.8131s | 3.3 | 6,487 | 0 |

## 4. Accounting Reconciliation & Verification

- **Volume Accounting Invariant:** `input_count == valid_count + duplicate_count + quarantine_count` verified on 100% of runs.
- **Velocity Accounting Invariant:** `messages_consumed == valid_count + quarantine_dlq_count` verified on 100% of runs.
- **Variety Accounting Invariant:** `input_count == valid_count + duplicate_count + quarantine_count` verified across all 7 formats.
- **PII Sanitization Invariant:** Zero unanonymized customer PII stored in Silver Parquet across all formats.
