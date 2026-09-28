# SentinelAI — Data Lineage & Traceability

Every analytical metric presented on the dashboard can be traced back to its origin:

```
[Dashboard KPI / Chart]
         │
         ▼
[Gold Marts: PostgreSQL / Parquet] (e.g. product_daily_stats, brand_daily_stats)
         │
         ▼
[Spark Aggregation & Anomaly Models]
         │
         ▼
[Silver Layer: Parquet] (Deduplicated, Cleaned, Normalized, Quality-Enforced)
         │
         ▼
[Spark Batch Cleaner & DQ Evaluator]
         │
         ▼
[Bronze Layer: MinIO S3 Raw JSON] (Partitioned: year/month/day/hour)
         │
         ▼
[Kafka Topics] (raw.products, raw.reviews, raw.prices, raw.events)
         │
         ▼
[Ingestion Sources] (Web Crawler, REST API, Multi-format Files, Replay Engine)
```
