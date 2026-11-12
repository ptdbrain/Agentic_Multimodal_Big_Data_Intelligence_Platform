# Durable commerce run recovery

Canonical commerce runs retain their SQL lease when a worker fails. A heartbeat
is diagnostic telemetry: its age never authorizes another writer. Old manifests
without recorded ownership cannot be released using a `writers_stopped` flag.

Run the commands from `btl/` with the same runtime mode, lake, warehouse, and (for
Airflow ownership) scheduler metadata connection as the failed worker. Plans pin
those backend identities. Do not point production recovery at a local mirror.

```bash
python scripts/recover_pipeline.py inspect --mode local --manifest runs/HASH/manifest.json
python scripts/recover_pipeline.py plan-recovery --mode local --manifest runs/HASH/manifest.json --output /tmp/run-plan.json --recovery-action resume
python scripts/recover_pipeline.py resume --mode local --plan /tmp/run-plan.json --plan-hash REVIEWED_SHA256
```

Review the new plan's run ID, backend identity, manifest hash, stopped-process
evidence, verified stages, next stage, and operation ID before applying. Plan
creation uses exclusive file creation and fsync; an existing plan is never
replaced. Apply requires its SHA-256 hash and refuses edited plans, changed
manifests, different backends, changed outputs, or a different lease owner.

For a local run, every recorded PID must be absent on the recorded local host.
A live or reused PID, another host, a permission error, or missing PID evidence
refuses recovery. Stop and reap the worker and any recorded child processes
before planning; recovery does not kill processes. PID records must be collected
by the worker, never invented to bypass checks.

For an Airflow run, add `--airflow-metadata` to planning and apply commands and
configure Airflow's real scheduler metadata DB. The adapter queries the matching
DagRun and task instances. The DagRun must be `success` or `failed`, and all task
instances must be terminal. Queued, scheduled, deferred, and running work refuses
recovery. A copied status JSON file is not evidence. Backend connection failures
propagate; no local or test fallback is used.

Resume verifies every completed stage's referenced output objects by SHA-256 and
size. Completed checkpoints must form a contiguous prefix of the declared stage
order. A completed stage without an output manifest is not a safe checkpoint.
The explicit commerce executor resumes one next stage; make another new plan for
the following stage. An already applied plan returns its recorded result without
replaying a completed operation. After an interruption, the same plan preserves
the operation ID so an idempotent sink can recognize its previous write.

Recovery first takes the same exclusive admission fence used by maintenance,
checks stopped evidence, outputs, and lease ownership, and records live recovery
ownership. It then resumes under shared writer admission while retaining the
canonical lease. This prevents competing canonical writes and maintenance while
allowing the executor's existing participating writer calls. Sinks must use the
operation ID or an equivalent durable run/stage key to make retries idempotent.
The final verified stage releases the lease; intermediate stages retain it.

To abandon a run, create and review a separate abort plan:

```bash
python scripts/recover_pipeline.py plan-recovery --mode local --manifest runs/HASH/manifest.json --output /tmp/abort-plan.json --recovery-action abort
python scripts/recover_pipeline.py abort --mode local --plan /tmp/abort-plan.json --plan-hash REVIEWED_SHA256
```

Abort validates stopped evidence and output references under the exclusive
maintenance fence, marks the manifest aborted, and releases only that run's
lease. It does not delete Bronze, Silver, Gold, checkpoints, or recovery history.
A live participating writer causes the fence to refuse the operation.

`RunManifest` retains its original constructor fields and adds ownership,
heartbeat, ordered stage checkpoints, output references, and recovery history.
`heartbeat`, `reference_output`, and `checkpoint_stage` persist worker telemetry.
The canonical CLI decorator preserves its existing invocation signature and
nested calls share one lease. It records start/failure/completion telemetry;
opaque CLI runs have no resumable stage order until explicit durable stage hooks
are installed. Article publishing retains its independent SQL/lake lifecycle.
