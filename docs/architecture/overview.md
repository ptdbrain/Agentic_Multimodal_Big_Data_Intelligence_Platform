# SentinelAI — Data Foundation & Analytics (Phase 1)

## 1. System Vision & Architecture
SentinelAI is a high-throughput, enterprise-scale Big Data intelligence platform designed to ingest, process, validate, analyze, and monitor multimodal e-commerce data. Phase 1 focuses exclusively on the foundational data engineering and analytics pipeline, establishing an elastic infrastructure for downstream AI/ML extensions.

```mermaid
flowchart TD
    subgraph Data_Sources["1. Data Sources"]
        DS1["E-commerce Web (Crawler)"]
        DS2["REST API Feeds"]
        DS3["Multi-format Files (CSV/JSON/Parquet)"]
        DS4["Historical Replay Engine"]
    end

    subgraph Ingestion_Layer["2. Data Ingestion & Streaming"]
        KP["Kafka Producers (Rate Controlled)"]
        KT["Kafka Topics (raw.products, raw.reviews, raw.prices, raw.events)"]
        RC["Raw Consumer (Partitioned Ingestion)"]
    end

    subgraph Data_Lake["3. Data Lake (MinIO / S3)"]
        BZ["Bronze Layer (Raw JSON/CSV)"]
        SL["Silver Layer (Cleaned & Normalized Parquet)"]
        GL["Gold Layer (Analytics Marts & Aggregates)"]
    end

    subgraph Processing_Layer["4. Big Data Processing (Spark)"]
        SB["Spark Batch ETL (Clean, Deduplicate, Validate)"]
        SS["Spark Structured Streaming (Tumbling/Sliding Windows)"]
        DQ["Data Quality Framework (Scorecards & Rule Engine)"]
    end

    subgraph Storage_Layer["5. Analytics Data Serving"]
        PG["PostgreSQL (Data Warehouse & Aggregated Marts)"]
        ES["Elasticsearch (Full-Text Search Index)"]
    end

    subgraph Serving_Layer["6. Analytics & Presentation"]
        AE["Analytics Engine (Descriptive, Trends, Anomalies)"]
        ST["Streamlit Interactive Dashboard"]
        MG["Prometheus & Grafana Monitoring"]
        AF["Apache Airflow (Pipeline Orchestration)"]
    end

    DS1 & DS2 & DS3 & DS4 --> KP
    KP --> KT
    KT --> RC --> BZ
    KT --> SS
    BZ --> SB --> SL
    SL --> DQ --> GL
    GL --> PG
    GL --> ES
    PG --> AE --> ST
    SS --> AE
    MG -.-> KT & SB & PG
    AF -.-> SB & DQ & GL
```

## 2. The 3V Big Data Principles
- **Volume**: Capable of processing 100K to 1M+ records through distributed partitioning across Bronze, Silver, and Gold Parquet layers.
- **Velocity**: Streaming ingestion with rates ranging from 10 to 500+ messages/sec via Kafka and Spark Structured Streaming with sliding/tumbling windows.
- **Variety**: Ingestion of multi-structured datasets across CSV, JSON, Parquet, relational schemas, and time-series event streams.

## 3. Medallion Data Lake Strategy
- **Bronze (Raw)**: Unaltered historical records stored with partition keys `year=YYYY/month=MM/day=DD/hour=HH`.
- **Silver (Cleaned & Validated)**: Cleaned, schema-validated, normalized, and deduplicated records in columnar Parquet format.
- **Gold (Aggregated Marts)**: Dimensional models (`product_daily_stats`, `brand_daily_stats`, `anomaly_events`) optimized for low-latency BI queries.
