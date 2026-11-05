# SentinelAI Production Runbook & Troubleshooting Guide

This guide describes operational maintenance procedures, diagnostic checklists, and incident response playbooks for the SentinelAI platform.

---

## 1. Quick Diagnostic Checklist

| Check | Command | Expected Output |
| :--- | :--- | :--- |
| **All Services Health** | `make health` (or `python scripts/health_check.py`) | All components `HEALTHY` |
| **Prometheus Telemetry** | `make metrics` (or `python monitoring/metrics_exporter.py`) | Valid Prometheus gauge text |
| **End-to-End Pipeline** | `make demo` (or `python scripts/run_e2e_pipeline.py`) | 6/6 stages passed (exit code 0) |
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

## Measured metrics endpoint

Run `python monitoring/metrics_exporter.py --serve --host 127.0.0.1 --port 8000` from `btl/`. Verify with `curl -f http://127.0.0.1:8000/metrics`. The checked-in Prometheus configuration expects the same host network; a container must use an explicitly reachable exporter address. Broker9092 and PostgreSQL5432 are protocol ports, not Prometheus endpoints. Run the dependency exporter below for protocol health and measured service gauges.

Unknown DQ is `NaN`, not a perfect or zero score. `sentinel_recent_anomalies` counts the most recent warehouse rows (at most100), including historical incidents; it is not an open-alert count. Scrape collection failures return503. The dashboard Bronze feed is a bounded sample, not a live subscription.

## Source integrity acceptance and isolated load measurements

Use Python 3.11 and Java 17. Configure the real host Kafka/MinIO/PostgreSQL endpoints from `.env.example`; install `requirements.txt`, start infrastructure, then run `make health`. Host Spark requires its Kafka/S3A connector packages; keep `PYSPARK_PYTHON` and `PYSPARK_DRIVER_PYTHON` on the same Python interpreter. The academic acceptance uses `SPARK_MASTER=local[2]`; this is not a multi-host scaling test.

From `btl/`, run each matrix sequentially in a **new** output directory:

```bash
python scripts/benchmark_fullpath.py --sizes 10000 100000 1000000 --iterations 3 --output-dir docs/reports/new-volume-matrix
python scripts/benchmark_fullpath.py --velocity-rates 50 200 500 --duration 120 --iterations 3 --output-dir docs/reports/new-velocity-matrix
```

A single smoke run uses `--reviews 100 --output /path/to/new-smoke.json`. Volume includes actual Kafka/MinIO/Spark/Gold/PostgreSQL and scoped DashboardDB queries. The deterministic workload has exactly 92% valid reviews, 3% duplicate inputs and 5% malformed observation clocks, plus 100 products and one price observation per ten review inputs. All rows are explicitly simulation/generated. Never count these as real marketplace observations.

Velocity measures actual Kafka/MinIO transport and durable validation routing, excluding Spark/Gold/warehouse. It limits dispatch to the requested monotonic time window and reports observed rates/counts, including shortfalls when ACKs are slow. Validation and sink latencies include consumer/batch buffering. Consumer drain can extend beyond the dispatch window.

Each child process creates a unique bucket/topic set; Volume also creates a dedicated PostgreSQL database. Existing source serving data is protected. Requires permission to create databases. Reports reserve a new file before service writes, retain failed runs, identify namespaces and measure only the Python/local child process tree. Service daemon resources and browser rendering are excluded. Review/report these limitations with results; the commands alone do not prove a completed benchmark. Keep the isolated namespaces until evidence review, then remove only namespaces explicitly listed in those reports.

Commerce marts currently require one observed currency per serving namespace. Unknown currency remains unknown; multiple currencies require isolated runs until an explicit FX policy exists. No implicit VND conversion is performed. The dashboard defaults to verified real data; choose Simulation or Unverified/All explicitly for generated workloads or legacy diagnostics.


## Actual dependency exporter and Grafana configuration

Run `python monitoring/services_exporter.py --serve --port 8001` with real endpoint credentials configured. The endpoint performs read-only Kafka metadata discovery, MinIO bucket lookup, PostgreSQL database-size query and Elasticsearch cluster-health request. It uses no offline backend. Probe failure yields `sentinel_dependency_up=0` and unknown measurements remain `NaN`; an absent configured bucket is separately reported as 0 and does not imply MinIO is down. HTTP scrape success does not itself mean every dependency is healthy.

The Prometheus host configuration scrapes platform metrics on 8000 and dependency metrics on 8001. Exporter address, ports and `PROMETHEUS_URL` must be reachable from the actual Prometheus/Grafana process or container. Set `PROMETHEUS_URL` when using the optional Grafana provisioning files. The dashboard has real PromQL selectors for exported metrics; missing scrapes remain missing data. Dependency health is not pipeline freshness, Kafka throughput or an open-alert count. Per-run lag/latency evidence remains in the benchmark reports. These configuration files do not claim that Prometheus/Grafana are deployed.

## Reproduce real-source and simulation routes

Use separate PostgreSQL databases, DATA_LAKE_BUCKET values and
KAFKA_TOPIC_*/KAFKA_GROUP_ID values for real and generated commerce.
The SQL/Silver guards reject mixing classes in one commerce generation;
changing the dashboard filter does not change the storage boundary.

    # Genuine HTTP collection; challenged sources fail, without generated fallback.
    python scripts/run_e2e_pipeline.py --mode integration --source-type crawler --crawler-platform tiki --crawler-query iphone --crawler-max-products 20 --crawler-reviews-per-prod 10
    python scripts/run_e2e_pipeline.py --mode integration --source-type crawler --crawler-platform lazada --crawler-query smartphone --crawler-max-products 20 --crawler-reviews-per-prod 0
    # Independent articles have separate schema, pandas ETL and serving tables.
    python scripts/run_article_pipeline.py --mode integration --max-articles 50 --max-pages 150 --output /path/to/new-article-report.json
    # Explicit replay of an existing article snapshot.
    python scripts/run_article_pipeline.py --mode integration --input /private/articles.json --max-articles 50 --output /path/to/new-replay-report.json
    # Local generated micro-batches; this does not contact Kafka.
    python scripts/run_streaming.py --mode simulator --seed 42 --window '1 minute' --watermark '10 minutes'

A successful request alone does not meet collection quotas. Check actual
products/prices/reviews, schema/DQ and source evidence. Lazada returned a
challenge on 03/11/2026; preserved 02/11 captures can be replayed with
acquisition_method=snapshot_replay, never relabelled as new live collection.
Preserve original raw HTTP hashes, clocks and parser versions when available.
A canonical capture hash proves the saved capture, not an unavailable original
HTTP response. Keep source bodies/credentials in private evidence storage.

Run streamlit run dashboard/app.py; it defaults to **real** data. Generated
and unverified records require their explicit sidebar scopes. Article records
are separate from reviews. Displaying a saved snapshot does not prove fresh crawling.

## Streaming checkpoint migration

The local micro-batch helper now writes validated **schema_version 3**
checkpoints with its source class and aggregation configuration. Corrupt,
incompatible or wrong-bucket snapshots fail rather than silently resetting.
Preserve the old file and rebuild into a **new** checkpoint directory using
the original input; do not edit counters or overwrite the old evidence.

Native Spark streaming now includes source_group in deduplication/window
state. Start this version with a new checkpoint namespace and isolated output;
replay from the required offsets, reconcile counts, then select the new output.
Do not disable Spark state schema validation or delete old checkpoints to
force compatibility. Both append and update modes use the source-checked
idempotent sink. The local simulator is not proof of Kafka streaming.

## Verification boundaries

    pytest -m 'not integration and not live and not airflow' -q
    pytest -m 'integration and not live and not airflow' -q
    pytest -m live -q

Run these sequentially with Python3.11/Java17 and actual services for integration.
Airflow additionally requires requirements-airflow.txt, a dedicated metadata
PostgreSQL database, AIRFLOW__CORE__EXECUTOR=LocalExecutor and migrated metadata;
then run pytest -m airflow -q. Do not count excluded gates as passes.
Streamlit AppTest checks page execution and data contracts; it does not certify
browser layout. Prometheus/Grafana configuration validation does not certify
a deployed monitoring server.

## Final integrity changes

Tiki reviews paginate at the observed API bound of20 rows. A request for more
returns multiple real pages, truncated to the requested total; a shortage stays
a shortage. Product currency is read from explicit fields/tokens. When the API
omits it and polite HTML is enabled, the crawler can use a complete observed HTML
offer with its own HTTP evidence. Missing/blocked HTML never becomes assumed VND.
Breadcrumb category links may supply an observed category; search keywords do not.

Committed article run IDs are immutable. Retrying one fails before evidence
writes; failed quality attempts have distinct article_attempts namespaces.
Native streaming quarantine keeps a source payload fingerprint, repairable JSON
with structured identifiers sanitized, and Kafka/file references. Repeated
delivery coordinates do not create a new payload version.

The local streaming simulator checkpoint preserves counters/windows/dedup/config,
not its in-memory rejected-event list. Export that list before exit if needed.
Use the native Kafka/Spark quarantine for durable operational replay.

Airflow execution tests reset only their named backfill range in **disposable**
metadata. Explicitly set SENTINEL_AIRFLOW_DISPOSABLE_METADATA=true only for that
test database. The test creates fresh local data paths and verifies a new DAG
run and7 newly executed tasks; an already completed backfill is not a pass.
