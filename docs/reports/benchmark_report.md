# SentinelAI 3V Empirical Benchmark Report

**Generated At:** `2026-10-12 05:01:02 UTC`  
**Runtime Mode:** `integration`  
**Storage Backend:** `MinIOBackend`  
**Base Deterministic Seed:** `42`  
**PII Anonymization at Ingest:** `True`

## 1. Volume Benchmark (Bronze Storage -> Spark Batch ETL -> Silver & Quarantine Parquet)

| Seed | Input Records | Valid (Silver) | Quarantined | Deduplicated | DQ Score | Duration (s) | Throughput (rec/s) | Bronze (B) | Silver (B) | Quarantine (B) |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| 142 | 100 | 92 | 5 | 3 | 92.0% | 17.9701s | 5.6 | 64,262 | 11,424 | 5,708 |
| 1042 | 1,000 | 920 | 50 | 30 | 92.0% | 18.0859s | 55.3 | 642,052 | 52,327 | 7,801 |
| 5042 | 5,000 | 4,600 | 250 | 150 | 92.0% | 19.1923s | 260.5 | 3,213,561 | 222,331 | 16,057 |
| 10042 | 10,000 | 9,200 | 500 | 300 | 92.0% | 21.0059s | 476.1 | 6,433,642 | 412,904 | 26,277 |

## 2. Velocity Benchmark (Kafka Producer -> Broker -> RawConsumer -> Bronze / DLQ)

| Seed | Transport | Target Rate (msg/s) | Produce Rate (msg/s) | E2E Consume Rate (msg/s) | Sent | Consumed | Valid (Bronze) | DLQ | E2E Duration (s) | Bronze (B) |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| 92 | real_kafka | 50 | 44.2 | 22.2 | 100 | 100 | 96 | 4 | 4.5136s | 71,211 |
| 242 | real_kafka | 200 | 148.1 | 78.4 | 200 | 200 | 192 | 8 | 2.5525s | 142,986 |
| 542 | real_kafka | 500 | 277.2 | 153.0 | 500 | 500 | 480 | 20 | 3.2681s | 356,559 |
| 1042 | real_kafka | 1,000 | 349.6 | 171.4 | 500 | 500 | 480 | 20 | 2.9168s | 357,692 |

## 3. Variety Benchmark (Multi-Format Ingest -> PII Hash -> Bronze -> Spark Parse/Validate/Transform -> Silver)

| Seed | Format / Source | Source (B) | Input Records | Valid (Silver) | Quarantined | Deduplicated | DQ Score | Duration (s) | Throughput (rec/s) | Silver (B) | Quarantine (B) |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| 42 | JSON | 215,893 | 500 | 465 | 25 | 10 | 93.0% | 18.1009s | 27.6 | 30,035 | 6,739 |
| 42 | JSONL | 215,893 | 500 | 465 | 25 | 10 | 93.0% | 17.5559s | 28.5 | 30,035 | 6,739 |
| 42 | CSV | 106,765 | 500 | 473 | 17 | 10 | 94.6% | 17.6744s | 28.3 | 30,324 | 6,509 |
| 42 | TSV | 106,041 | 500 | 473 | 17 | 10 | 94.6% | 17.7482s | 28.2 | 30,324 | 6,509 |
| 42 | Parquet | 31,177 | 500 | 465 | 25 | 10 | 93.0% | 17.9053s | 27.9 | 30,035 | 6,739 |
| 42 | HTML_JSONLD_Crawler | 163,779 | 500 | 465 | 25 | 10 | 93.0% | 17.8881s | 28.0 | 28,902 | 6,562 |
