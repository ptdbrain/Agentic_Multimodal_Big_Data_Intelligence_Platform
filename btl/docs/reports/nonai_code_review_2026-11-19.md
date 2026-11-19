# Whole-branch review: 193067a..5d665b6

Single fresh review, begun 2026-11-14 and resumed/completed 2026-11-19. HEAD rechecked: 5d665b6. Read AGENTS.md, the approved plan/spec, supplied diff and progress-ledger rulings. No descendant reviewers, production-service calls, tracked-file edits, Git mutations or fixes. Focused reproductions used temporary SQLite/files and an ephemeral loopback receiver only.

**Assessment: changes requested.** No Critical finding established. Five Important correctness issues and one Minor contract gap follow. The architecture has substantive source isolation, transactional outbox and hash-verification protections, but the defects below prevent a clean code-correctness signoff. This is not deployment or performance acceptance.

## Important F1 — Pending SQL windows overwrite the committed serving snapshot

**Location:** `serving/store.py:398-403`, with `serving/store.py:329-345` and `dashboard/pages/10_Streaming.py:16-30`.

`write_windows` replaces the only stored version of a window as soon as the SQL sink completes, before quarantine/audit reaches that batch. Filtering `batch_id <= last_batch` later cannot recover the previous committed version. The streaming page does not even apply that filter: it displays the newest SQL values alongside the older confirmed frontier/watermark.

**Trigger/effect:** batch 1 contains an open burst window. While the observer is confirming batch 1, the independent Spark window sink writes batch 2 for the same still-open window. `_stage_stream_alerts` can no longer see batch 1's candidate; its `changed` query and evaluation input exclude the now-batch-2 row. A valid committed alert is omitted, and the UI displays unconfirmed batch 2 data. If another sink stalls or fails, that incorrect state persists. Simply adding the frontier filter to the UI would instead make the previously visible window disappear.

**Executed reproduction:** temporary SQLite, 12 closed historical minute windows (3 reviews each), watermark 00:12, candidate open window 00:22–00:23 with 20 reviews in batch 1. Control `stream_progress(batch=1)` emitted one firing notification. In the second case, write batch 2 with 21 reviews for that same open window before confirming batch 1: last_batch became 1, notification count was 0, there was no candidate alert, and the sole SQL candidate was `(review_count=21,batch_id=2)`. The candidate remains newer than the watermark, so this does not rely on updating a finalized Spark window.

Preserve pending versions until their all-sink frontier is confirmed, then atomically promote the appropriate latest-per-window versions with progress and alert evaluation. Bounded pending storage is sufficient; full permanent per-batch history is not required. Add same-window pending-update coverage, including failed ancillary sink and UI reads; the existing regression uses a different product for its ahead-of-frontier row.

## Important F2 — Same-generation restart can silently move the quarantine sink

**Location:** `scripts/run_streaming.py:185-193` (also `serving/runtime.py:39-52`).

The persisted deployment configuration includes checkpoint/output paths but omits `quarantine_path`. Validation only establishes that the new path stays inside the selected lake/bucket. Thus changing `quarantine_path` in the private deployment JSON while retaining generation and checkpoint passes `register_stream`'s equality check.

**Trigger/effect:** restart an existing generation with a different in-bucket quarantine destination. Spark resumes `reviews_quarantine` from the existing checkpoint, so already-consumed invalid records are not replayed into the new destination. Future quarantine output is now split across locations while the same generation can again advertise all sinks confirmed. This contradicts the claimed immutable generation configuration and makes a normal operator configuration edit lose the complete quarantine view.

**Proof:** static end-to-end configuration flow: deployment JSON sets `STREAM_QUARANTINE_PATH` (lines 81–88), runtime validates containment, but only window/watermark/topic/checkpoint/output enter the persisted dictionary. The checkpoint remains `<checkpoint_dir>/reviews_quarantine` in `spark_streaming_job.py`. Pin the quarantine destination and other checkpoint-affecting settings, and reject drift before Spark starts. A focused restart/config-drift regression is missing.

## Important F3 — Lineage can certify an incompatible Bronze record as the Silver winner

**Location:** `serving/lineage.py:278-298` and `serving/lineage.py:323-326`.

The matcher compares numeric/business fields only if both inputs contain them, and never checks currency. Matching event/logical IDs therefore suffice for a Bronze candidate that lacks the required value or has a different original currency. `trace_record` promotes this to `verified_winner` / `selected_silver_record`, not partial evidence.

**Trigger/effect:** two raw observations reuse an event identity, or a malformed candidate has that identity. A Silver product with price 10 VND can be attributed to a raw product with price 10 USD, or with no price at all. The byte hashes are valid for the wrong evidence, so they do not prevent false ancestry certification.

**Executed reproduction:** `_winner_matches({'event_id':'shared','product_id':'p','price':10,'currency':'USD'}, {}, {'event_id':'shared','product_id':'p','price':10,'currency':'VND'}, 'products')` returned `True`; removing `price` from the raw candidate also returned `True`. This is the same helper used by `trace_record` to assert the selected Silver relationship. Normalize and compare the relevant source values (including original currency and required fields); absent corroborating evidence should remain partial, not verified. Add reused-ID/missing-value/wrong-currency cases.

## Important F4 — A torn receiver journal tail permanently blocks new alert deliveries

**Location:** `scripts/verify_operations_stack.py:62-77`.

Every keyed delivery reparses every journal line. A crash or interrupted append before a complete newline-terminated JSON record is durable can leave a partial final line. `json.loads(line)` then fails for all subsequent keys; the handler responds 400 without recovering the tail. Restart does not help, and `deliver_notifications` stops on this first failed delivery, blocking later outbox entries too.

**Executed reproduction:** create a temporary receiver journal containing `{"received_at":"interrupted`, start the actual receiver on an ephemeral loopback port, and send two valid notifications with distinct valid idempotency keys. Both returned HTTP 400 and the journal remained unchanged. No existing services were contacted.

Use a recoverable durable receipt store, or a locked journal recovery protocol that distinguishes incomplete final append from corruption of committed records. Test receiver interruption before fsync/complete append in addition to the current worker-kill-after-receiver-fsync test. The latter does not exercise this failure boundary.

## Important F5 — Docker's dependency exporter always probes Elasticsearch in its own container

**Location:** `docker-compose.yml:477-488` (environment at line 482), with `monitoring/services_exporter.py:57-62` and `config/settings.py:191`.

The dependency exporter inherits the common environment, which supplies neither `ES_URL` nor `ES_HOST`. Only `search-worker` receives `ES_URL=http://elasticsearch:9200`. The probe consequently uses the default `http://localhost:9200` inside the separate dependencies container.

**Trigger/effect:** the supplied fresh Compose deployment is healthy, including Elasticsearch, but its dependency metrics still publish `sentinel_dependency_up{service="elasticsearch"} 0` and unknown ES health. Grafana/alerts consume a false outage, undermining source-aware operational acceptance.

**Proof:** static Compose/environment-to-probe wiring; no Docker daemon needed to establish the wrong endpoint. Set the Compose service endpoint for this exporter and cover the effective configuration. Actual Docker startup remains an independent pending gate.

## Minor F6 — New public interfaces do not satisfy the explicit typing requirement

**Location:** representative examples `serving/registry.py:164-179`, `serving/search.py:23-35`, `serving/notifications.py:13`, `serving/lake.py:15`.

The plan/review focus requires all public interfaces to be typed, but multiple new public methods and binding parameters have no annotations (including `DashboardSources.read_table`, `publication_metadata`, Elasticsearch request/serialization helpers and `deliver_notifications(binding,...)`). This is not a demonstrated runtime failure; it is a concrete unmet API contract that makes the new binding/snapshot boundaries harder to check. Add concrete DatasetRef and return/parameter contracts, rather than relying on broad Any everywhere.

## Declined to judge

- Actual Docker build/start, image dependency compatibility, memory behavior, Spark executor networking and Grafana rendering: unavailable runtime acceptance, not inferred successes or defects merely because untested. F5 is a deterministic source configuration error, not a substitute for that acceptance.
- Canonical batch/stream performance campaigns, restore/cutover/rollback, 72 hours, three genuine scheduled cycles, off-host backup and physical multihost HA: explicitly pending by approved rulings. The short native benchmark is smoke only.
- Latest broad/native suite results were supplied by the coordinator, not rerun or independently certified by this review. In particular, the 937-pass/8-failure full run has known launcher/fixture/environment questions and must not be called green. I did not classify its failures as code defects without investigation.
- Authentication, AI and assignment workflows: explicitly out of scope. No FX conversion requirement introduced.
- Preexisting dirty Grafana JSONs/root user files and unrelated pre-base defects: outside this range; untouched.
- Exhaustive numerical parity/scaling of every lineage recipe, adversarial external writers bypassing the application's fences, permanent search-index retention, and large-journal throughput: not established by this bounded code review. Known retention/capacity acceptance remains documented. The positive false-winner case in F3 was directly reproduced.
- Potential broader legacy exporter default-warehouse behavior and the Airflow legacy-DAG import environment were inspected but not promoted to findings without a sufficiently specific reproduced runtime result.

No fixes, merge, push or production mutations performed. The review artifact is the only intentional workspace write, in the ignored plan workspace.

## Implementation response — 2026-11-19

F1–F5 were reproduced or confirmed against effective configuration and fixed in one pass, with RED→GREEN regressions. Pending SQL versions are staged separately and atomically promoted with frontier/alerts; quarantine and semantic configuration are pinned before Spark; winner matching requires source values and currency corroboration; receiver recovery repairs only an uncommitted tail and fsyncs duplicates; Compose uses the Elasticsearch service endpoint. Native follow-up gates also exposed and fixed cached batch numbers after restart and valid timezone-bearing Airflow run identifiers. F6 remains a documented Minor API-typing gap. Deployment/performance/restore/stability exclusions above remain pending, with no inferred certification. Fresh final verification results are recorded in the completion report.
