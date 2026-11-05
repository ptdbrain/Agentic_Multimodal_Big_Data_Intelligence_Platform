# Streaming schema and checkpoint migration

`python scripts/migrate_stream_state.py --help` provides inspect, plan, replay,
verify, promote and rollback. Plans and reports are version 1 JSON. Plan files
are created exclusively and pin SHA-256 inventories of the old checkpoint,
Bronze input and native Spark baseline outputs. Old checkpoints are never edited.

The envelope/business readers currently support 1.0; simulator resume supports
checkpoint 3. Adding nullable fields needs compatibility tests; changing keys,
types, units or Spark state requires a new namespace and replay. Native Spark
state is opaque: this tool does not rewrite or disable engine state validation.

Stop the old writer first. A plan requires its PID and will recheck liveness
under the shared SQL maintenance fence at promotion. Use only local writer PIDs;
remote hosts require an external stopped-writer control and are not certified.
Pin complete Bronze JSON/JSONL input covering the old run. Kafka offsets with
expired retention are not silently replaced by `latest`; export and verify the
necessary Bronze first. Kafka coordinates and timestamps in the exported events
remain part of their pinned content.

```bash
python scripts/migrate_stream_state.py inspect --checkpoint /isolated/old/checkpoint
python scripts/migrate_stream_state.py plan --old-checkpoint /isolated/old/checkpoint \
  --destination /isolated/new --input /isolated/bronze.jsonl --writer-pid 12345 \
  --engine spark --mode integration --old-output /isolated/old/metrics \
  --old-quarantine /isolated/old/quarantine --output /isolated/plan.json
python scripts/migrate_stream_state.py replay --plan /isolated/plan.json
python scripts/migrate_stream_state.py verify --plan /isolated/plan.json
python scripts/migrate_stream_state.py promote --plan /isolated/plan.json --pointer /isolated/serving.json
python scripts/migrate_stream_state.py rollback --plan /isolated/plan.json --pointer /isolated/serving.json
```

Native replay invokes the real Spark Structured Streaming file source with
availableNow and isolated output/checkpoints. A missing JVM or failed query fails
the command. Verification compares full sorted output rows and quarantine
payloads (excluding nondeterministic quarantine processing time and replay file coordinates) with pinned old
Parquet artifacts. Input SHA-256 additionally binds IDs, source, raw clocks and
currency. A missing baseline refuses promotion. Exact old input identity is also required via the stopped old run's pinned `replay.json` (`--old-input-manifest` for an explicit location); a replay report in the parent of the old checkpoint is detected automatically. A legacy run lacking this evidence cannot be certified merely because aggregate counts match. Missing quarantine directories
must be represented by a preserved empty directory from the stopped old run.

Simulator replay invokes the actual `StreamingJob`, writes checkpoint 3 and
persists quarantine separately. Verification checks the old checkpoint's real
counters, keys, source and complete window state. Legacy simulator checkpoint
versions may be inspected/replayed but cannot be certified/promoted automatically:
the tool refuses to infer old state. This is separate from native Spark state
migration, which uses the old published Parquet baseline regardless of opaque
checkpoint encoding.

Serving changes use an atomic JSON generation pointer with previous generation
and plan fingerprint. Consumers must read this pointer to opt into migrated
outputs; existing dashboard/warehouse paths are not switched implicitly. Keep
old writers stopped while verifying/promoting. Rollback requires the pointer
still refer to this migration, so it cannot overwrite a later promotion.
