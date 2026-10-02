# SentinelAI Architecture Documentation

This document describes the **Current Architecture (Phase 1 Complete)** and the **Target Architecture (Phase 2+ Future Roadmap)** for SentinelAI.

---

## 1. Current Architecture (Phase 1 Production Data Foundation)

```mermaid
flowchart TD
    subgraph S1["1. Data Sources"]
        SRC1["E-Commerce Crawlers (Tiki, Universal Schema.org)"]
        SRC2["Historical Public Datasets"]
        SRC3["Replay & Simulation Engine"]
    end

    subgraph S2["2. Ingestion & Messaging (Kafka KRaft)"]
        PROD["StreamProducer (Deterministic SHA256 Envelopes)"]
        TOPICS["Kafka Topics: raw.products, raw.reviews, raw.prices"]
        DLQ["Dead-Letter Queue (dead-letter) & DLQ Replay Utility"]
        CONS["RawConsumer (sentinel-bronze-writer, commit after write)"]
    end

    subgraph S3["3. MinIO Object Storage & Data Lake"]
        SM["StorageManager Abstraction Facade"]
        BRONZE["Bronze Layer (sentinel-data/bronze/...) Append-Only"]
        QUAR["Quarantine Area (sentinel-data/quarantine/...) Auditing"]
    end

    subgraph S4["4. Distributed Compute & Analytics (Apache Spark)"]
        SPARK_ETL["SparkBatchETLJob (Clean -> Normalize -> Validate -> Cross-Batch Dedup)"]
        SILVER["Silver Layer Parquet (products, reviews, prices)"]
        DQ["SparkDataQualityEvaluator (Completeness, Validity, Uniqueness)"]
        GOLD_BUILDER["SparkGoldBuilder (4 Analytical Marts)"]
        GOLD["Gold Layer Parquet (product/brand/category/price stats, anomalies, DQ)"]
    end

    subgraph S5["5. Table Format & Serving"]
        LAKEHOUSE["LakehouseTable (ACID _delta_log, Time Travel, Upsert)"]
        POSTGRES["PostgreSQL Data Warehouse (Idempotent UPSERT on conflict)"]
        DASHBOARD["Streamlit Analytics Dashboard (Direct DW Queries)"]
        PROM["Prometheus Exporter (Telemetry & Health Observability)"]
        AIRFLOW["Airflow DAG: sentinel_daily_pipeline"]
    end

    SRC1 & SRC2 & SRC3 --> PROD
    PROD --> TOPICS
    TOPICS -- "Malformed" --> DLQ
    TOPICS --> CONS --> SM --> BRONZE
    BRONZE --> SPARK_ETL
    SPARK_ETL -- "Valid" --> SILVER
    SPARK_ETL -- "Corrupted" --> QUAR
    SPARK_ETL --> DQ --> GOLD
    SILVER --> GOLD_BUILDER --> GOLD
    SILVER <--> LAKEHOUSE
    GOLD --> POSTGRES
    POSTGRES --> DASHBOARD
    POSTGRES & SM --> PROM
    AIRFLOW -. "Orchestrates" .-> SPARK_ETL
    AIRFLOW -. "Orchestrates" .-> GOLD_BUILDER
    AIRFLOW -. "Orchestrates" .-> POSTGRES
```

### Architectural Highlights of Phase 1:
1. **Deterministic Message Envelopes:** Every event contains an immutable `event_id` computed via `sha256(source|entity_id|event_time)`.
2. **At-Least-Once Kafka Delivery:** `RawConsumer` only commits Kafka offsets after atomic write confirmation to MinIO.
3. **Cross-Batch Deduplication:** `SparkBatchETLJob` merges incoming records with the existing Silver layer via primary keys, guaranteeing idempotency.
4. **Data Quarantine:** Defective records are routed to `quarantine/` with error metadata, preventing silent drops and pipeline poisoning.
5. **Lakehouse Table Format:** ACID commit metadata logged in `_delta_log/*.json`, enabling time-travel audits and atomic merge upserts.
6. **No-Hardcoding Serving:** Streamlit and Prometheus query the Data Warehouse dynamically.

---

## 2. Target Architecture (Phase 2+ AI & Multimodal Roadmap)

```mermaid
flowchart TD
    subgraph P1["Phase 1 Foundation"]
        SILVER_P1["Silver Cleaned Text & Catalog"]
        BRONZE_IMG["Bronze Product/Review Images"]
    end

    subgraph P2["Phase 2: Vietnamese NLP & Sentiment"]
        VN_NORM["Vietnamese Text Normalizer (Unicode NFC, Teencode handling)"]
        TOKENIZER["Word Segmentation (VnCoreNLP / underthesea)"]
        SENTIMENT["PhoBERT / ViSoBERT Aspect-Based Sentiment Analysis"]
        NLP_GOLD["Gold Sentiment Marts (positive/negative ratios, aspect sentiments)"]
    end

    subgraph P3["Phase 3: Multimodal Intelligence"]
        IMG_EMBED["Image Embeddings (CLIP / DINOv2)"]
        TEXT_EMBED["Text Embeddings (BGE-M3 / OpenAI)"]
        VECTOR_DB["Qdrant Vector Database"]
        SEARCH["Multimodal Semantic Search & Duplicate Image Detection"]
    end

    subgraph P4["Phase 4: Agentic Intelligence & RAG"]
        RAG_AGENT["Read-Only RAG Analytics Agent"]
        LLM["Large Language Model with Whitelisted SQL Execution"]
        SAFETY["Guardrails & SQL Whitelist Filter"]
    end

    SILVER_P1 --> VN_NORM --> TOKENIZER --> SENTIMENT --> NLP_GOLD
    BRONZE_IMG --> IMG_EMBED --> VECTOR_DB
    NLP_GOLD --> TEXT_EMBED --> VECTOR_DB
    VECTOR_DB --> SEARCH
    NLP_GOLD & VECTOR_DB --> SAFETY --> RAG_AGENT --> LLM
```
