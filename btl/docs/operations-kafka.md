# Kafka lifecycle and ownership

Run from `btl/`. The article pipeline now persists its topic/run/source/group and terminal state in the configured warehouse. Real producers require all-replica acknowledgements, bounded retries and stable event identities. Configure `KAFKA_REPLICATION_FACTOR`, `KAFKA_MIN_INSYNC_REPLICAS`, and `KAFKA_RETENTION_MS` before provisioning a new cluster (defaults 1, 1 and 7 days). Existing topics are never silently reassigned or reconfigured; inspect/migrate them explicitly. Use RF3/minISR2 only on the isolated three-broker profile.

```bash
python scripts/kafka_maintenance.py inventory --output inventory.json
python scripts/kafka_maintenance.py plan --output cleanup.json
python scripts/kafka_maintenance.py apply --plan cleanup.json --output applied.json
```

Plans expire in one hour. Apply rechecks ownership fingerprint, consumer inactivity, committed offsets versus log ends, 48-hour terminal age, and hashes of durable Bronze JSON contents. Empty partitions need no committed offset. Canonical topics and unknown topics are never cleanup candidates. Broker failures stop cleanup; retry the plan while valid. If topic deletion succeeded but group deletion failed, registry remains terminal and a retry can finish group cleanup.

Failed runs remain indefinitely unless an operator reviews a durable archive containing Bronze, quality and quarantine evidence:

```bash
python scripts/kafka_maintenance.py archive-failure --topic raw.articles.RUN --proofs archive.json --review incident-123
```

`archive.json` is a list with `kind` (`bronze`, `quality`, `quarantine`), storage `path`, and SHA256 of canonical JSON (sorted keys, compact separators). Cleanup still requires seven days after review, idle consumers and verified archive. Age alone never proves a failed payload was saved. Cleanup does not delete Bronze or quarantine objects.

The current admission fence coordinates participating producers, Bronze consumers, canonical pipelines and streaming writers with backup/migration. Direct external database/object-store clients must be stopped separately. A dead run's durable lease is never stolen automatically.
