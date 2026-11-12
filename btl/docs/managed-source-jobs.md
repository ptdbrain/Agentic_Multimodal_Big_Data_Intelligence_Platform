# Managed source collection

Run commands from `btl/`. Copy `config/source-jobs.example.yml` to a private deployment location; use an absolute `root`. The configuration declares three independent jobs: observed Tiki commerce, observed web content, and labelled simulation. Each job declares its scheduler owner, cron schedule, timezone, minimum input count, environment file and physical target. Lazada is not substituted with simulated or replayed data.

## Deployment and ownership

Provision a dedicated PostgreSQL schema and MinIO bucket for each namespace. Environment files must be mode `0600`, contain `KEY=VALUE` assignments (quote values containing spaces), and include:

- `SOURCE_JOB_NAMESPACE`, matching the configured namespace;
- `RUNTIME_MODE=integration`, PostgreSQL and MinIO credentials/endpoints, `DATA_LAKE_BUCKET` and `PGOPTIONS='-c search_path=your_schema'`;
- distinct Kafka topic/group names for each commerce namespace;
- Java 17 and the project's pinned Spark connectors when required.

The CLI resolves `current_schema()` against PostgreSQL and pins nonsecret physical identities in `root/targets.json`. Reusing a bucket or SQL schema across configured namespaces is refused. Use canonical endpoint names consistently; this registry is not a distributed service-discovery system.

Set `SOURCE_JOBS_CONFIG=/private/source-jobs.yml` on Airflow. `sentinel_sources.py` creates one DAG per Airflow-owned job with separate `capture` and `process` tasks. `SOURCE_JOB_PYTHON` can select the project's Python environment independently of the scheduler environment. Cron schedules use `Asia/Ho_Chi_Minh` unless explicitly changed. Catchup is disabled, active DAG runs are limited to one, and processing retries reuse the scheduler run ID. The legacy canonical DAG is manual.

Disable the old `sentinel-tiki-capture.timer` before enabling the managed Tiki DAG. Configuration validates the declared owner; it cannot detect an unrelated cron job or scheduler running with another configuration. Scheduler activation remains an operator deployment step, not evidence that scheduled cycles occurred.

```bash
python scripts/run_source_job.py capture --config /private/source-jobs.yml --job tiki --run-id manual-inspection
python scripts/run_source_job.py process --config /private/source-jobs.yml --job tiki --run-id manual-inspection
python scripts/run_source_job.py status --config /private/source-jobs.yml --job tiki --run-id manual-inspection
```

## Durability and retries

Each run has a locked directory, immutable-by-contract captured input hash, source/config identity and actual capture/process timestamps. Failed processing keeps captured input; a changed hash is rejected. A partial capture remains `partial` even after successful processing. The same completed receipt is reused without recollection. A durable capture receipt atomically stores input, identity and checksum before publishing capture.json. Interrupted handoffs resume from that verified receipt without recollection, preserving the original observation clock. Identity/hash mismatches are refused. Legacy orphan files without a matching receipt still require inspection of the provider’s immutable capture.

Tiki freezes a product watchlist. Managed same-day partial retries observe only missing products, retaining earlier attempts. Reviews use newest-first 20-row pages, a committed identity frontier and a pending anchored backlog. Only durably captured rows may advance state. A missing continuation anchor restarts discovery without advancing the committed frontier. Use a page budget of at least two for continuing a large backlog; one request can be consumed rechecking its anchor. New head arrivals during backlog completion are discovered on the following incremental pass. HTTP failures cannot count as an empty successful page; 403/429 stop collection.

Web jobs preserve a URL queue, per-page HTTP evidence and checksums, changed-content fingerprints, and at most three consecutive failed attempts per URL. Exhausted URLs remain visible for operator review. Bounds and pending work can make a run partial even when its usable articles were published.

## Current acceptance limits

The example does not activate a scheduler. Capture/process retry correctness is separate from business-stage recovery: an interrupted commerce run retains its canonical lease and needs the reviewed recovery workflow. An article run already registered under the same ID also requires explicit lifecycle recovery. Commerce requires review context for rating analytics. For an empty new-review delta, the explicit `allow_review_context_replay` policy can replay previously accepted Silver reviews of the same source group, labelled `snapshot_replay` with their original clocks. Processing reports context reviews separately from newly observed reviews. An empty first capture without accepted context remains pending. Do not substitute sample reviews to make a scheduled run pass.

Set `OPS_SOURCE_JOBS_CONFIG` on the operations exporter to read managed manifests across isolated namespaces. Source metrics report actual capture/process timestamps, primary input counts, partial/failed states and pending runs; they do not infer a DQ score or fabricate missing observations. See [operations monitoring](operations-monitoring.md).

## Persistent deployment

The native deployment uses `orchestration/systemd/sentinel-native@.service` for PostgreSQL, MinIO and Kafka, `sentinel-source-scheduler.service` for Airflow, and `sentinel-monitoring@.service` for seven monitoring processes. Copy unit templates to `~/.config/systemd/user/`. Private environment files select absolute project/Python/config paths; vendor binaries and data directories remain outside Git. `scripts/run_native_service.py` requires persistent data paths and loopback listeners. `scripts/run_monitoring_service.py` preserves existing Grafana credentials, database and provisioning. See [deployment examples](native-operations.md).

Use a dedicated Airflow environment (`requirements-airflow.txt`) with LocalExecutor and PostgreSQL metadata. Set `AIRFLOW_METADATA_PYTHON` for recovery checks from the project environment. Set `AIRFLOW__CORE__EXECUTE_TASKS_NEW_PYTHON_INTERPRETER=true` and put the Airflow environment's `bin` directory first on `PATH`; task subprocesses must use that interpreter. On filesystems rejecting colons in filenames, configure:

```text
AIRFLOW__LOGGING__LOG_FILENAME_TEMPLATE="dag_id={{ ti.dag_id }}/run_id={{ ti.run_id | replace(':','_') }}/task_id={{ ti.task_id }}/map={{ ti.map_index }}/attempt={{ try_number }}.log"
```

Before changing a previously hardcoded commerce consumer group, pause its producers/consumer. Verify terminal receipts and archived Bronze hashes, old committed offsets equal broker ends, and both groups are inactive. Copy those exact committed offsets into the configured group only when its existing offsets are absent or identical. Retain the old group. Do not initialize a new group at `latest` or silently replay old messages into an incremental capture.

## Scheduled acceptance

`verify_source_acceptance.py` runs in the Airflow environment and reads its real metadata backend. Supply `--config`, `--window` (JSON with a timezone-aware `stability_window_started_at`) and `--output`. It requires 72 elapsed hours, at least three successful **scheduled** cycles per source, successful capture/process task states, matching immutable capture hashes and minimum inputs. Manual runs never count. Prometheus supplies sampled availability; missing targets/time samples reduce coverage. The configured threshold is 99%, not a claim of uninterrupted or multi-host availability. Partial collection blocks full acceptance even when processing succeeds.

`sentinel-source-acceptance.timer` refreshes this report hourly. Never reset a stability window automatically to hide incidents. Retain failed runs and partial coverage for review.
