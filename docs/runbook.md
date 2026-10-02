# SentinelAI Production Runbook & Troubleshooting Guide

This guide describes operational maintenance procedures, diagnostic checklists, and incident response playbooks for the SentinelAI platform.

---

## 1. Quick Diagnostic Checklist

| Check | Command | Expected Output |
| :--- | :--- | :--- |
| **All Services Health** | `make health` (or `python scripts/health_check.py`) | All components `HEALTHY` |
| **Prometheus Telemetry** | `make metrics` (or `python monitoring/metrics_exporter.py`) | Valid Prometheus gauge text |
| **End-to-End Pipeline** | `make demo` (or `python scripts/run_e2e_pipeline.py`) | 7/7 stages passed (exit code 0) |
| **Test Suite Health** | `make test` (or `pytest -v`) | 100% tests passing |

---

## 2. Common Incident Response Playbooks

### Playbook 1: Kafka Consumer Lag Spike / Consumer Stalled
- **Symptoms:** High message buildup on `raw.reviews` or `raw.prices`; messages not arriving in MinIO Bronze.
- **Root Cause:**
  - Storage connectivity degradation or network partition between consumer and MinIO.
  - Consumer threw unhandled deserialization error.
- **Resolution:**
  1. Check Kafka consumer group status:
     ```bash
     docker exec -it sentinel-kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group sentinel-bronze-writer --describe
     ```
  2. Inspect consumer logs for offset commit failures. Note that `RawConsumer` enforces `commit after write`; if MinIO is unreachable, offsets remain uncommitted to ensure zero data loss.
  3. Verify MinIO health via `curl -f http://localhost:9000/minio/health/live`.

---

### Playbook 2: Dead-Letter Queue (DLQ) Buildup
- **Symptoms:** Messages accumulating in `dead-letter` topic or `quarantine/` storage tier.
- **Root Cause:** Upstream crawler parsed malformed HTML or supplier changed schema attributes (e.g. negative prices or empty review text).
- **Resolution:**
  1. Inspect quarantined objects using the DLQ Replay dry-run:
     ```bash
     python scripts/replay_dlq.py --dry-run
     ```
  2. Review specific schema validation errors reported in the replay summary.
  3. Update schema or fix parsing rules in `ingestion/crawler/`.
  4. Execute DLQ replay to re-publish valid messages:
     ```bash
     python scripts/replay_dlq.py --max 500
     ```

---

### Playbook 3: Apache Spark Batch Job Memory Issues (OOM)
- **Symptoms:** `java.lang.OutOfMemoryError: Java heap space` or worker process killed by Docker OOM killer.
- **Root Cause:** Large unpartitioned shuffle on high-volume review batches or excessive `collect()` calls.
- **Resolution:**
  1. Ensure broadcast joins are only used for dimension tables (e.g. `products`), not fact tables.
  2. Increase Spark worker memory limit in `docker-compose.yml`:
     ```yaml
     SPARK_WORKER_MEMORY: 4G
     ```
  3. Ensure `spark.sql.shuffle.partitions` is set proportionally to executor cores (e.g. 10–20 in local mode).

---

### Playbook 4: PostgreSQL Upsert Locks or Deadlocks
- **Symptoms:** `WarehouseManager.upsert_dataframe()` hanging during pipeline execution.
- **Root Cause:** Concurrent upsert transactions modifying identical primary key rows simultaneously without indexed conflict keys.
- **Resolution:**
  1. Verify conflict target indexes exist:
     ```sql
     CREATE INDEX IF NOT EXISTS idx_prod_daily_date ON product_daily_stats(date, product_id);
     ```
  2. Ensure only a single write pipeline instance runs at any given time (`max_active_runs=1` in Airflow DAG).
