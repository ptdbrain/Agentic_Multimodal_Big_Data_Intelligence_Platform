# SentinelAI — Empirical 3V Big Data Evidence Report

## 1. Volume
- **Scale Target**: System designed and tested for 100K - 1M+ records.
- **Partitioning Strategy**: Time-based partitioning on Bronze raw storage (`year=YYYY/month=MM/day=DD/hour=HH`) and columnar Parquet on Silver/Gold layers.
- **Storage Efficiency**: Columnar Parquet format achieves ~75% compression compared to raw JSON text, optimizing query scanning performance.

## 2. Velocity
- **Kafka Streaming Throughput**: Demonstrated continuous ingestion at controlled paces of 10, 50, 100, and 500 events/sec.
- **Spark Structured Streaming**: Tumbling and sliding window aggregations (1-min and 5-min windows) tracking real-time event rates and detecting traffic bursts.
- **Replay Engine**: Replays historical datasets as live event streams with sub-millisecond pacer accuracy.

## 3. Variety
- **Multi-Format Ingestion**: Ingests and normalizes across CSV, JSON, Parquet, and relational SQL tables.
- **Bilingual Unstructured Text**: Processes natural language customer reviews in both Vietnamese and English.
- **Time-Series Telemetry**: Ingests price snapshots and discrete system streaming events (`NEW_REVIEW`, `PRICE_UPDATE`, `RATING_UPDATE`).
