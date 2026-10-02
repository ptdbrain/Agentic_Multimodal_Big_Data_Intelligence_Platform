# SentinelAI 3V Benchmark Report

**Generated At:** 2026-10-02 12:19:03
**Engine:** Apache Spark Batch & Kafka Ingestion

## 1. Volume Benchmark (Spark Batch ETL)
| Record Count | Processing Time (s) | Throughput (rec/s) | DQ Score |
| :--- | :--- | :--- | :--- |
| 100 | 2.223s | 45.0 | 100.0% |
| 1,000 | 0.044s | 22,601.1 | 100.0% |
| 5,000 | 0.045s | 111,645.7 | 100.0% |
| 10,000 | 0.061s | 163,792.0 | 100.0% |

## 2. Velocity Benchmark (Kafka Stream Producer)
| Target Rate (msg/s) | Actual Rate (msg/s) | Messages Sent | Duration (s) |
| :--- | :--- | :--- | :--- |
| 10 | 10.0 | 100 | 10.038s |
| 50 | 48.8 | 100 | 2.048s |
| 100 | 95.6 | 100 | 1.047s |
| 500 | 410.6 | 100 | 0.244s |

## 3. Variety Benchmark (Multi-Format Ingestion)
| Format | Files Processed | Records Loaded | Load Time (s) | Throughput (rec/s) |
| :--- | :--- | :--- | :--- | :--- |
| JSON | 3 | 1,800 | 0.029s | 62,181.9 |
| CSV | 3 | 1,800 | 0.056s | 32,056.7 |
| Parquet | 3 | 1,800 | 0.044s | 40,704.7 |
