# Operational monitoring and resilience drills

## Start monitoring

The standalone `docker-compose.operations.yml` uses Linux host networking so native exporters stay on loopback; Prometheus, Alertmanager and Grafana also bind loopback. Supply a private `GRAFANA_ADMIN_PASSWORD`, then run:

```bash
docker compose -f docker-compose.operations.yml up -d
python monitoring/metrics_exporter.py --serve --port 8000
python monitoring/services_exporter.py --serve --port 8001
python monitoring/operations_exporter.py --serve --port 8002
python scripts/verify_operations_stack.py --action receiver --output /private/path/notifications.jsonl
python scripts/verify_operations_stack.py --action verify --output /private/path/stack-verification.json
```

Configure real endpoints and runtime mode before starting exporters. OperationStatus uses the configured SQL warehouse and never falls back from PostgreSQL. The operations exporter probes real Kafka replica state by default. Set `OPS_CONSUMER_GROUPS` to an explicit comma-separated list to aggregate committed consumer lag; an absent group has no measurement, not zero lag. `OPS_KAFKA_PROBE=false` disables Kafka probes explicitly. `OPS_DISK_PATH` selects the mounted filesystem measured for free capacity; this is not a MinIO free-space estimate.

`OperationStatus.start(run_id,pipeline,source,expected_min=1)` registers a run; `finish(run_id,received,valid,quarantined,data_as_of=None)` records completion; `fail(run_id)` records failure. Last success survives later failures. Pipeline labels are `commerce/articles/streaming/backup/migration/platform`; source labels are `marketplace/web_content/simulation/unverified/system`. No URL, user ID or run ID becomes a Prometheus label. Missing gauges stay NaN. Input accounting permits duplicates/superseded inputs and forbids outputs exceeding input.

Grafana provisions its datasource at `http://127.0.0.1:9090` in the same host network. Alerts group by alert name/pipeline/source, wait10s, regroup5m, repeat4h and send resolved notifications. The default receiver logs bounded-size webhook payloads durably at loopback18093. Replace its URL with a configured external webhook only when that destination is authorized. No secret values belong in reports.

## Runbooks

### Exporter

An unavailable scrape returns503. Check the SQL/Kafka endpoints and process logs; do not replace missing measurements with zeros. A probe gauge older than120s for2min generates a stale-measurement warning. Pipelines must be registered before freshness can be monitored; an exporter cannot infer an unconfigured source schedule.

### Pipeline

Inspect the run manifest, source acquisition label and latest durable status. Failure fires immediately. Daily commerce/article runs become stale after26h; a completed run below expected input fires shortage. Correct the underlying source/DQ issue before retry; preserve failed evidence.

### Kafka

Lag over1000 for10min and under-replication for2min alert. Inspect consumer offsets and ISR before restarting anything. Do not delete a run topic to hide lag. Set explicit consumer groups for probes.

### Backup

A complete backup older than26h alerts; a backup never observed warns after26h. Verify manifests and restore evidence before changing last-success metrics. Same-disk copies do not prove whole-host disaster recovery.

### Storage

Below15% free for5min alerts. Use the configured target mount, inspect retention plans, and preserve the last valid backup and unreplayed Bronze.

### Checkpoint

A positive checkpoint-error gauge alerts. Preserve the incompatible checkpoint; use the migration/replay workflow. Clear the gauge only after successful verified recovery.

## Isolated resilience profile

```bash
docker compose -f docker-compose.resilience.yml up -d
python scripts/verify_operations_stack.py --action chaos --service kafka1 --rows 100 --output /private/path/kafka-drill.json
python scripts/verify_operations_stack.py --action chaos --service spark-worker1 --rows 100000 --output /private/path/spark-drill.json
```

The profile has3 KRaft broker/controllers, RF3/minISR2 and2 Spark workers; hard service memory limits total12GiB (including the short-lived shared-volume initialization service) (plus Docker overhead). Host Kafka ports39094–39096 and Spark37077/38080 are loopback-only. Volumes are project-owned and distinct from serving data. The project must start with `sentinel-resilience-` and containers must carry `sentinel.drill=true`; the harness refuses unowned targets. No Kafka endpoint override is accepted by the drill.

Kafka drill sends simulation-labelled payloads with acks=all before/during one broker outage, restarts the broker in a finally block, and reconciles acknowledged payloads against actual consumed bytes. Spark drill runs a real shuffled workload and checks every ordered output ID against an independent deterministic checksum while a worker is stopped; it fails if the workload ended before injection. The workload/consumer failures are not converted into passes. Failed Kafka reconciliation retains the temporary topic for investigation.

These drills do not certify loss of a host, two failed brokers, driver recovery, SQL/UI end-to-end latency, or distributed production scaling. Use the full-path benchmark for separately measured serving latency. Native endpoint health and a parsed Compose file do not prove the stack was deployed.

## Validation boundary

Unit tests cover durable reopen, state/accounting validation, bounded labels, unknown metrics, isolation refusal, real loopback webhook firing/resolved receipt, and configuration/resource invariants. `docker compose ... config --quiet` validates both manifests without daemon access. Deployment, PromQL evaluation, Grafana rendering and outage drills require executable services and must have separate runtime evidence.

## Native verification when Docker is unavailable

`verify_native_resilience.py` starts only its own fresh three-broker KRaft
namespace from an explicitly supplied vendor Kafka bin directory. It checks
ports, waits for RF3/ISR3, stops the topic leader, continues acknowledged writes,
restarts that broker and verifies all payload IDs. It terminates only processes
it created; logs/data remain for diagnosis. Use a fresh work directory each run.

```bash
python scripts/verify_native_resilience.py --kafka-bin /vendor/kafka/bin --work-dir /private/new-drill
python scripts/benchmark_operations.py --iterations 3 --reviews 1000 --output-dir /private/new-latency
```

The latency command runs the existing full-path measured pipeline in independent
subprocesses. Its P50/P95/P99 summarize **batch cohorts** from Kafka dispatch
through Spark/SQL/DashboardDB visibility, excluding input generation and browser
rendering. It refuses incomplete reports and mixed code fingerprints. A small
sample is not a throughput/latency SLA. Configure real services and the Spark
Kafka/S3A connectors before executing it. No new latency result is claimed until
its measurements are run.

Runtime evidence on05/11 includes actual native Prometheus2.54.1 and
Alertmanager0.27.0 delivering a SQL-backed injected failure and recovery to the
loopback webhook. The checked-in twelve alert rules pass promtool. A separate
native three-broker test reconciled200 acknowledged fixture messages across one
broker/controller outage. See `docs/reports/operations_*_2026-11-05.json`.
Grafana rendering and the two-worker container drill remain unverified while
Docker socket access is unavailable.

Expected daily jobs are configured with `OPS_EXPECTED_PIPELINES` (default
`commerce:unverified,articles:web_content`). Their registration time is durable;
a job that never starts emits unknown success/start times and can alert after26h.
Set an explicit comma-separated list for enabled jobs; an empty value disables
expectations. This does not pretend a job ran and does not create an Airflow DAG.

### Sources

Set `OPS_SOURCE_JOBS_CONFIG` to the same managed source configuration used by the
scheduler. The exporter reads durable handoff manifests across their separate
warehouses, without changing its own database connection. `source_job` is a
bounded configured identifier; run IDs never become metric labels. Actual
capture and processing timestamps are distinct. A successful process does not
change a partial collection into complete coverage. Failed/partial jobs, input
shortages, captures older than 26 hours, and configured sources that have never
captured all have dedicated rules. Unknown timestamps remain NaN. The config
file's actual modification time anchors never-started monitoring.

### Streaming

The Spark CLI polls `lastProgress` for window, quarantine and optional audit
queries. SQL deduplicates repeated/older batch reports per checkpoint and sink.
`stream_heartbeat_timestamp` advances only when every required sink has reported
committed progress. `stream_progress_timestamp` additionally requires actual
input at every sink; idle batches do not refresh data freshness.
`stream_input_rows` is the window query's reported input scans, not a sum across
sinks or a unique-event counter. Actual CLI query failures increment outstanding
`stream_errors`; old progress cannot clear a newer failure. All required sinks
must report newer committed progress before it clears.

Add `streaming:system` to `OPS_EXPECTED_PIPELINES` only for an enabled continuous
stream. Its heartbeat alert uses two minutes of delay after a 120-second gap;
its no-data alert uses five minutes after 15 minutes without input. These are
configured operational thresholds, not a measured latency SLA. A daily batch
schedule does not imply a continuously fed stream.

Run the PromQL rule matrix with the same Prometheus version as deployment:

```bash
promtool check rules monitoring/alerts.yml
promtool test rules monitoring/alerts.test.yml
```

The matrix evaluates firing, selected resolved states, disabled streaming, and
unknown measurements with controlled metric samples. It does not prove webhook
routing or actual infrastructure outages. Alertmanager silences remain the
maintenance mechanism; retain the silence ID, exact matchers, operator reason
and bounded expiry when rehearsing maintenance.


## Current repeatable operational gates

Use `scripts/verify_alert_delivery.py --tools /vendor/tools --work /private/new-alert-drill` for actual native Prometheus/Alertmanager firing+resolved delivery for five immediate rules and actual maintenance silence. Fault measurements are explicitly injected into an isolated SQL test namespace. Long pending timers, freshness and unknown/disabled cases use `promtool test rules monitoring/alerts.test.yml`; shortening those production timers is not part of this drill.

`scripts/verify_pipeline_faults.py --help` describes the isolated Docker MinIO outage/consumer restart and killed Spark-driver checkpoint drill. It requires the compiled MinIO binary, cached Spark image, isolated RF3 Kafka and Java/connectors. It stops only the resources it creates. Consumer offsets must not advance while durable object writes fail. Driver recovery must produce exact counts without duplicates.

`scripts/benchmark_worker_scaling.py --spark-home /vendor/spark --environment /private/native.env --work /private/new-campaign` freezes source files and runs twelve independent10k/100k review batches with one/two actual standalone workers. Reports include worker allocation evidence. Its batch latency spans Kafka dispatch through SQL/DashboardDB visibility; it is not browser-render time or per-event streaming latency. Each p95/p99 estimate has only three samples and must not become an SLA. See [current evidence](reports/de_completion_2026-11-12.md).

For persistent native process ownership and boot behavior, use [native operations](native-operations.md). `OPS_BACKUP_STATUS_FILE` points at the managed backup's atomically written verified snapshot pointer; it reports the real recovery-point timestamp across separate dashboard/source schemas, never file modification time.
