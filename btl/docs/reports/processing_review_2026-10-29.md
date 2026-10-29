# Independent processing/analytics review — 2026-10-29

A fresh reviewer inspected the complete processing/analytics implementation and used small real SQLite/pandas reproductions. It did not modify source or run a second Spark/full suite. At review time393 local tests passed. No Critical findings; eight Important findings.

| Finding | Required correction | Current verification |
| --- | --- | --- |
| Obsolete warehouse keys survive daily export | Replace full daily snapshots, including explicit empty input, within the existing transaction; keep incidents/DQ as history | Shrinking/empty SQLite regressions RED→GREEN |
| Empty/missing Gold output leaves old files | Publish all four typed outputs; use a serialized in-progress/complete generation manifest; block incomplete export | Both engines plus real Spark→pandas replacement regressions RED→GREEN |
| Malformed price text becomes invented positive values | Validate supported signs/currency/number/grouping grammar before parse | Literal malformed-price tests in both engines; rejected forms include1e,12-34,1,2,3,1e-3USD,++10 |
| Incomplete/contradictory DQ reports authorize Gold | Require measured fields, reconcile counts/scores and ratios; report unique valid rows | Public gate/report regressions RED→GREEN |
| Actual price input lacks current matching DQ | Extend gate requirements to every consumed dataset, respect excluded prices, bind canonical inputs to producer snapshot evidence | Consumed-price and stale canonical snapshot regressions; actual pipeline replay |
| Pages mix separate read snapshots | Products and Data Quality read their related tables/reports through a shared connection | Actual SQLite update between page loaders remains invisible within the old snapshot |
| Unknown input provenance invented from analysis run ID | Preserve nullable source ID; derive only from authoritative source manifests or explicit caller label | Unlabelled pandas/Spark regressions; pipeline preserves actual producer run |
| Review means/ratios/baseline samples use different eligible rows | Apply consistent nonmissing-ID/rating eligibility before aggregation | Descriptive, summary, trend and rating baseline regressions RED→GREEN |

The reviewed fix pass passed428 local tests with38 cases outside the local marker selection and zero failures/errors/skips. Further boundary regressions distinguish missing snapshots from explicit empty ones, preserve numeric decimal types, validate tied rounding and propagate source read errors. The latest complete gate and source fingerprint are recorded in `remediation_validation.json` and `processing_plan_remaining.md`; older intermediate counts are historical evidence, not a substitute for that gate.

The actual rebuilt before/after/new-replay comparison is `processing_backfill_comparison_final.json`; new replay hashes match and fixture inputs are unchanged. The repeatable100k CLI load on the reviewed implementation is `processing_fullpath_100k_final.json`:100000 review inputs =92000Silver+5000quarantine+3000duplicates, plus100products/10000prices;71.1536s and2375.02MiB peak process-tree RAM through Spark ETL/Gold/anomalies→SQLite→DashboardDB. Scope exclusions remain explicit. The report retains the exact measured source version; the final empty-required-input/error-propagation guards are separately correctness-tested.

## Decisions and acceptance limits

- At review time, PostgreSQL/MinIO/Kafka/Airflow and remote-worker behavior were unverified. These functional gates now have actual native and Docker evidence: see `processing_docker_acceptance.json`, including separate Docker client IPs for remote PostgreSQL ownership. Physical multi-host load/failover remains outside the measured scope. SQLite was not substituted for real-service evidence.
- Canonical synthetic backfill is limited to the stated boundary. Production-specific history reconciliation/backfill/rollback remains unapplied. Historical date-only→UTC event identities require review before history merge.
- The local100k run does not establish a production SLA, distributed scaling or browser load behavior.
- Reviewer Spark conclusions were source-based; the implementer ran actual Spark regressions and replay for the corresponding fixes. No additional review round substitutes for these tests.
- Approved thresholds and retrospective leave-one-out price methods stand, with method/sample metadata. They are not represented as causal alerts or statistical significance guarantees.
- A four-file atomic transaction is not claimed. Per-path recovery, a common publication/read lock, complete-generation metadata and warehouse transaction gating are the contract. Interrupted publication can temporarily block export until recovery/rerun.

The review's Minor stale-audit-count finding had already been addressed by the concurrent documentation update. Dependency deprecation warnings remain deferred; they are not hidden or converted into application success claims.
