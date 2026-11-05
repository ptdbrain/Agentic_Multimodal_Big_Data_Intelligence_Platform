# Source integrity, audit and 3V implementation plan

Approved specification: the detailed plan approved in the conversation on 2026-11-02.
Goal: repair confirmed existing defects, integrate two live marketplaces, simulation and independent web articles, and measure 3V honestly.
Architecture: retain Kafka/MinIO/Spark/PostgreSQL/Streamlit; independent web content schema and source provenance; no synthetic replacement for live sources.

## Ordered delivery gates
- [x] 1. Baseline source/runtime, functional inventory, lint/unit/Spark/integration evidence.
- [x] 2. Audit all existing functions and tests; record reproducible findings and severity.
- [x] 3. Fix every confirmed defect with RED/GREEN regression and broad verification.
- [x] 4. Probe Tiki/Lazada/Shopee/Amazon and Tinhte/Voz; select two accessible marketplaces and independent articles; blocked sources remain unaccepted.
- [x] 5. Source contracts: source_group/name/record_id/url, acquisition_method, collected_at, business time, run_id, schema/parser version, evidence reference; synthetic seed/generator.
- [x] 6. End-to-end three source groups. Minimum per marketplace:20 products/20 real price observations; >=50 real reviews across marketplaces; >=50 independent articles; two collection runs. Keep articles separate from reviews. Missing fields remain unknown/quarantined.
- [x] 7. Profile before optimizing; batch/partition/query/connection fixes must preserve business hashes.
- [x] 8. Measured dashboard status, honest source labels, metrics endpoint/exporters, clean-environment demo documentation.
- [x] 9. Volume10k/100k/1M x3; real Kafka50/200/500msg/s120s x3; report actual rates, counts, lag/latency, time/resources and failures. Distinguish source, structure and format variety.
- [x] 10. Final whole-change review, regressions, browser checks, source evidence and Vietnamese3V report. No production or model-training scope.

## Acceptance rules
No fabricated rating, review count, timestamps or verified-purchase status. No old snapshot presented as live. Namespace identities. Protect PII/tokens in evidence. Preserve historical reports and AGENTS.md. No source accepted without live evidence. Synthetic volume is never counted as distinct real observations. No production SLA or distributed scaling claim from local tests.

## Verification checkpoint, 03/11/2026

Gates1–9 have implementation and measured evidence; gate3 remains subject to the
final independent review/fix pass. Source quotas use preserved actual collections;
Lazada is currently challenged, so the final transport gate is explicitly replay.
Old Lazada raw HTTP/parser metadata was never preserved: canonical capture evidence
and unknown fields are reported honestly. Current Tiki v4 and article HTML evidence
is hash-verified.

Current local gate603passed, live Tiki1passed; native-wide integration rerun pending
after correcting a legacy test that merged different source classes. Volume and
uniform-prefix Velocity9/9 each passed; see their03/11 summaries. All9Streamlit
pages pass actual-PG AppTest in real/simulation scopes and HTTP health.

Gate10 still needs the one fresh whole-change reviewer/fix pass. Browser rendering
cannot be certified: the CUA browser host is unavailable. AppTest is not a browser.
No AGENTS.md modification, commits or push. Main ledger remains
../.runtime/source-integrity-progress.md (relative to the btl directory).

## 04/11 final gate
One fresh review completed;7Important fixed with RED→GREEN and692whole-scope tests pass.
Actual v5 Tiki collections20/20/65 twice, final real routes/article/UI/metrics pass.
User manually confirmed browser counts/defaultreal. FinalVolume9 rerun is active
because the SQL guard changed;9Velocity runs remain valid for their unchanged
transport path. Gates9/10 await final matrix and the requirement completion audit.

## Completion audit —04/11/2026
All10gates verified and complete. Authoritative index:
docs/reports/source_integrity_completion_status_2026-11-04.json.
692disjoint tests passed0failures0skips; Airflow7fresh tasks.
FinalVolume9/9 on one source fingerprint/current code; uniformVelocity9/9 with
actual rates disclosed. Two actual Tiki20/20/65 captures completed, two preserved
genuine Lazada20/20 captures explicitly replayed, two50article routes verified.
Final real source routes,9page AppTest/manual browser confirmation and persistent
actual metrics endpoints passed. One fresh review and one verified fix pass done;
all7Important plus observed paging/no-op-test defects fixed, no deferredMinor.
Historical contradictory checkpoint notes above are retained chronologically and
superseded by this audit. No AGENTS.md modification, commit, push or integration.
