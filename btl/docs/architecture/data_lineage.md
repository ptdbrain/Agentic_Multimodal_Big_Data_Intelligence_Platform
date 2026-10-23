# SentinelAI — Data Lineage & Traceability

Every analytical metric presented on the dashboard can be traced back to its origin:

```
[Ingestion Sources] (Web Crawler, REST API, Multi-format Files, Replay Engine)
         │
         ▼
[Kafka Topics] (raw.products, raw.reviews, raw.prices, raw.events)
         │
         ▼
[Bronze Layer: MinIO S3 Raw JSON] (Partitioned: year/month/day/hour)
         │
         ▼
[Spark Batch ETL & DQ Evaluator]
         │
         ▼
[Silver Layer: MinIO Parquet] (Deduplicated, Cleaned, Normalized, Quality-Enforced)
         │
         ▼
[Spark Analytics] (Gold Builder, Aggregation & Anomaly Models)
         │
         ▼
[Gold Marts: MinIO Parquet] (e.g. product_daily_stats, brand_daily_stats)
         │
         ▼
[PostgreSQL Data Warehouse]
         │
         ▼
[Dashboard KPI / Chart]
```
