# Data engineering operations implementation — 05/11/2026

## Implemented

1. SQL topic/run ownership registry; canonical topic protection; audited cleanup plan/apply with expiry, hash/liveness/offset rechecks. Complete run retention48h; failed runs require reviewed Bronze/quality/quarantine archive and7days. Producer all-replica acknowledgements and configurable RF/minISR/retention.
2. Durable SQL operational run/gauge state; bounded metric labels and unknown values; article and Airflow status hooks. Prometheus/Grafana/Alertmanager profiles, twelve rules, loopback webhook receiver and guarded isolated chaos runner.
3. Coordinated writer admission/maintenance fence across producer, consumer, canonical run, article, streaming and operational metadata writers. Immutable checksummed recovery bundles with directory/PostgreSQL/MinIO adapters; guarded plan/apply/verify, retention and daily user-timer templates.
4. Versioned checkpoint inspection, pinned input/baseline replay, actual local simulator/native Spark file replay, exact reconciliation and guarded generation pointer promotion/rollback. Opaque checkpoints are never edited. Legacy states without interpretable baseline fail closed. Consumers must explicitly adopt the pointer; Kafka checkpoint resumption from a file-source replay is not certified.
5. Isolated three-broker/two-worker Compose profile, guarded fault-injection tools, native three-broker fallback and repeated full-path batch latency measurement command.

## Runtime evidence and limits

- Actual Kafka/PostgreSQL/MinIO fixture delivery and topic/group deletion: `operations_kafka_native_2026-11-05.json`.
- Actual PostgreSQL custom dump/restore and MinIO SHA256 roundtrip: `operations_backup_native_2026-11-05.json`. Two SQL rows and one object; checkpoint/orchestration/registry roles were local fixtures. Not an Airflow restore or Spark restart drill.
- Actual Prometheus/Alertmanager → webhook firing and resolved notifications from SQL state: `operations_alerts_native_2026-11-05.json`.
- Three native Kafka brokers, RF3/minISR2, one broker/controller outage and200 acknowledged messages reconciled: `operations_kafka_failover_2026-11-05.json`. Single-host process failure only.
- Migration tests use the actual JVM/Structured Streaming engine for isolated old/new file replays, plus local fenced promote/rollback tests.
- Docker socket denied. Compose parsing is verified with a validation-only password; Grafana rendering, container deployment and two-worker outage remain pending.
- Outside-host backup location/credentials were not supplied; daily scheduling is provided but not enabled. The RPO24h/RTO60min objective is not certified by the tiny fixture. No whole-host disaster recovery or production SLA is claimed.
- Full-path batch percentile measurement tool is implemented; no new percentile or distributed speedup result is invented.

These are implementation and scoped test results. Remaining service/hardware acceptance is explicit and must be completed in the target environment.

## Verification

Broad local gate:673 passed,62 integration/airflow/live deselected,0 failures/errors/skips (`operations_local_2026-11-05.xml`). The final combined operational and regression tests after expected-job registration and shared maintenance admission fixes additionally passed a70-case focused gate, including native Spark migration (overlaps the broad suite; do not sum). Ruff and authored whitespace checks pass. Both Compose files parse with a validation-only Grafana password; promtool validates all12 rules. Historical native reports retain their actual fixture and execution boundaries.

Final safety regression also fences registry/lease schema initialization; maintenance cannot race first-use DDL. The final70-case gate includes actual Spark replay, admission, backup, Kafka lifecycle, monitoring, DLQ, Airflow manifest and article/producer regressions.
