# Backup and restore operations

`python scripts/backup_ops.py backup create --config /private/backup.json --output /private/result.json`

Use `backup list`, `backup verify --backup PATH`, `backup retention-plan`, and
`backup retention-apply --plan PLAN.json` with the same config. Outputs are JSON;
`--output` uses exclusive creation. Retention keeps the latest backup per day for
seven days represented in inventory and the latest per ISO week for four weeks,
plus the latest complete backup. A retention plan includes manifest hashes and
is rejected if inventory changes. Incomplete/corrupt backups are never considered
complete recovery points or automatically deleted.

## Configuration

Keep config outside Git. Passwords/access keys come from environment references:

```json
{
  "mode": "integration",
  "backup_target": "/mnt/separate-backup-volume/sentinel",
  "inventory": {"airflow_enabled": true, "code_revision": "DEPLOYED_COMMIT"},
  "kafka": {
    "bootstrap_servers": "127.0.0.1:9094",
    "groups": ["sentinel-bronze-writer"],
    "topics": ["raw.products", "raw.reviews", "raw.prices"]
  },
  "components": [
    {"name":"warehouse","role":"warehouse","kind":"postgres","env_prefix":"POSTGRES_"},
    {"name":"airflow","role":"orchestration","kind":"postgres","env_prefix":"AIRFLOW_DB_"},
    {"name":"lake","role":"lake","kind":"minio","bucket":"sentinel-data","env_prefix":"MINIO_"},
    {"name":"checkpoints","role":"checkpoints","kind":"directory","path":"/srv/sentinel/checkpoints"},
    {"name":"registry","role":"registry","kind":"directory","path":"/srv/sentinel/operational-metadata"}
  ],
  "restore_targets": {
    "warehouse":{"env_prefix":"RESTORE_PG_"},
    "airflow":{"env_prefix":"RESTORE_AIRFLOW_"},
    "lake":{"bucket":"sentinel-restore-drill","env_prefix":"RESTORE_MINIO_"},
    "checkpoints":{"path":"/srv/restore/checkpoints"},
    "registry":{"path":"/srv/restore/metadata"}
  }
}
```

PostgreSQL prefixes resolve `HOST`, `PORT`, `USER`, `PASSWORD`, `DB`; optionally
set `binaries_dir` on the component/target to select matching `pg_dump/pg_restore`.
MinIO prefixes resolve `ENDPOINT`, `ACCESS_KEY`, `SECRET_KEY`, `SECURE`.
Inventory must describe deployed software/schema versions, external secret key
references, and the actual checkpoint directories. If operational registry and
Lakehouse catalog are PostgreSQL tables, they are already in its full database
dump; the registry directory can hold exported offsets/run configuration.
Do not copy a live PostgreSQL or SQLite data directory as ordinary files.

## Coordinated capture

Pause scheduling/new external producers, stop external writers and drain received
messages durably to Bronze. The CLI takes a PostgreSQL advisory maintenance lock
(or local `flock`), rejecting participating writers, canonical run leases and
running topic-registry entries. Never remove an unresolved lease to force backup.
Integration create also checks explicit consumer-group committed offsets against
Kafka end offsets. Declare only the subscriptions each configured group consumes;
use separate coordinated invocations if groups consume different topics.

Stop Spark queries cleanly before capture. SQL and MinIO locks do not stop clients
that bypass application admission, so external-writer shutdown is mandatory.
The bundle includes snapshot/complete timestamps, immutable component inventory,
SHA256 and file sizes, canonical PostgreSQL table-row fingerprints, offsets and
quiescence evidence. A `.incomplete-*` directory remains if capture fails; only a
fully verified bundle is renamed `backup-*`. All data is private (backup root
must be access controlled); store encryption/Fernet keys separately and verify
that retained keys decrypt the restored Airflow metadata before resuming Airflow.

## Restore drill

```bash
python scripts/backup_ops.py restore plan --config /private/backup.json --backup /backups/backup-ID --output /private/restore-plan.json
python scripts/backup_ops.py restore apply --config /private/backup.json --backup /backups/backup-ID --plan /private/restore-plan.json --output /private/restore-result.json
python scripts/backup_ops.py restore verify --config /private/backup.json --backup /backups/backup-ID
```

All targets are explicit and must be isolated: directories absent, databases with
no application tables, MinIO buckets absent/empty. The source namespace cannot be
a target. Checksums, target identities and emptiness are checked again under the
maintenance fence before first write. Multi-component restore is not an atomic
transaction across services: after a failure keep writers stopped, discard the
isolated partial targets and retry with new empty targets. The tools never expose
restored data as serving or resume writers automatically.

Verification compares complete file/object inventory and hashes plus all database
table canonical row fingerprints. Then verify Fernet/application configuration,
resolve restored stale leases against confirmed stopped writers, reconstruct
Kafka from configuration and replay archived Bronze with reconciliation. Start
only the restored checkpoint/output pair; retain the previous pair for rollback.

Schedule daily execution through the deployment scheduler after the writer pause/
drain step; failed quiescence leaves no complete backup. Target RPO is 24 hours
from `snapshot_at`, and RTO is 60 minutes on the declared rehearsal dataset.
Measure restore, verification, replay and resumption together for actual RTO.
A local same-host destination is a rehearsal artifact. Off-host replicated backup
and a whole-host-loss exercise are required before claiming disaster recovery.
The manifest deliberately sets `disaster_recovery_certified=false`.

## Verification evidence

Unit tests cover roundtrip, interrupted capture, tampering, path traversal,
symlinks, existing/overlapping restore targets, maintenance exclusion, stale leases,
running topic records and retention safety. Actual PostgreSQL16/MinIO drill in
`.runtime/backup-native-drill-report.json` used two rows (including large BIGINT,
decimal, null and UTF-8) and one object. Local checkpoint/orchestration/registry
fixtures were copied; this does not certify Airflow restore, Spark restart,
off-host durability, 24-hour scheduling or production-scale RTO.

## Daily scheduling and success metrics

A user systemd service/timer is provided under `orchestration/systemd/`. Create
`~/.config/sentinel/operations.env` (mode0600) with absolute paths for
`SENTINEL_PROJECT_DIR` (the btl directory), `SENTINEL_PYTHON` and
`SENTINEL_BACKUP_CONFIG`, plus required endpoint/credential environment values.
Install the two unit files into the user's systemd directory, reload and enable
`sentinel-backup.timer` only after a manual fenced create/verify succeeds.
It runs daily at02:00 local time with Persistent=true; this is a target schedule,
not proof of a24-hour RPO. Stop scheduled input and streaming first; if admission
or offsets are busy the job fails and never pretends a valid snapshot exists.

The CLI updates `backup_last_complete_timestamp` only after successful create;
restore success updates `restore_last_success_timestamp` and measured duration.
Operational metrics and topic lifecycle writers honor the same maintenance lock.
External direct SQL/MinIO clients and Airflow metadata writers must be stopped
before capture; the application fence cannot control arbitrary external writers.
