# Source integrity and 3V audit — in progress

This is a new audit, not a replacement for the completed processing acceptance of29October. No final whole-system correctness claim is made.

## Baseline
Base commit1b5289128c0a30cca61cd35b6e76cf1f0662b9d4. Separate branchfix/source-integrity-3v. Python3.11.17 restored from original archived environment. Unit baseline299passed/183deselected/218dependency warnings. Full local Spark gate in progress. Docker socket currently returns permission denied; no integration passes claimed for this audit.

## Confirmed findings
| ID | Severity | Behavior / impact | Evidence / status |
|---|---|---|---|
| SRC01 | Important | Universal HTML fallback invents rating4.5/review count10; JSON-LD invents5.0 | Regression RED, fixed; missing values remain absent |
| SRC02 | Important | Missing review dates replaced with collection clock; missing purchase verification becomes true | Regression RED, fixed; observed and collection time separated |
| SRC03 | Important | Review IDs depend on page order; changes can overwrite different observations | Regression RED, fixed using content fingerprint when no upstream ID |
| SRC04 | Important | Zero rating becomes5; string false becomes true; JSON-LD graphs ignored | Regression RED, fixed |
| SRC05 | Important | A single malformed review discards later reviews; invalid raw evidence lost | Regression RED, fixed; retain rating_raw and later rows |
| SRC06 | Important | Same SKU on independent domains shares product identity | Regression RED, fixed for universal parser; historical migration still needs assessment |
| SRC07 | Important | Arbitrary HTML title interpreted as product, including access-denied pages | Regression RED, fallback now requires metadata; completeness still enforced by schema |
| OBS01 | Important | Prometheus configured against broker/database protocol ports rather than exporters; exporter only prints | Confirmed static; pending repair |
| OBS02 | Important | Dashboard Active/Sync/Serving inferred from file existence; architecture status hardcoded | Confirmed static; pending repair |
| DOC01 | Important | Historical3V report labels offline_virtual_queue as real Kafka | Confirmed static; historical report preserved; new correction/report pending |

New regression file:test_web_source_integrity.py. Seven tests failed against original code;71selected tests passed after repair,5deselected. This covers parser and related existing tests, not integration.

## Source survey
See source_reachability_2026-11-02.json and source_capability_probe_2026-11-02.json. HTTP200is not acceptance. TikiAPI returned JSON; Lazada search returned HTML; Shopee search403; Amazon search503. No source yet accepted against20products/20prices,50reviews,50articles and two-run requirements.

## Remaining delivery gates
Complete audit of remaining functions/tests; full local and service gates; live-source provenance/adapters; article schema/topic/Silver/Gold/serving; simulation labels; monitoring/UI repair; profiling;3Vbenchmarks and final review. No source/benchmark acceptance may be inferred from the older reports.

## Implementation progress (not final acceptance)

- Universal parser7RED/GREEN regressions repaired, including absent facts, order-independent generated review IDs, JSON-LD graph and cross-domain product identity.
- Base crawler additionally repaired: absent price no longer becomes0/VND/Official Store; observed listing timestamp uses collection time instead of catalog update. TwoRED/GREEN regressions.
- Tiki legacy synthetic fallback removed entirely. Explicit allow_public_fallback=True now raises and directs users to MockEcommerceCrawler. Its former test demanded fake output; changed to assert rejection, with a separate RED/GREEN regression. Existing defaultFalse behavior retained.
- Public Lazada adapter implemented and registered. Listing-only; reviews explicitly unavailable. Two real live runs each20products/20prices,0reviews. See lazada_live_capture_2026-11-02.json. No downstream acceptance claimed.
- Tiki two real runs each20products/20prices but only1review.50-review requirement remains unmet. See tiki_live_capture_2026-11-02.json.
- Independent article parser implemented: own article identity/body/published_at/collected_at, no rating/product association. Three new tests passed. First Tinhte collection41articles from54URLs; failures retained. Related-link collection in progress. Kafka/Silver/Gold/serving article path remains unimplemented.
- Metrics now has a real/metrics endpoint (404unknown paths,503collection failure), closes DB connections and renders unknown DQ asNaN. Historical last100anomaly rows relabelled recent rather than active. Prometheus targets the actual host exporter, not database/broker protocol ports. Dedicated service exporters remain pending.
- Dashboard hardcoded Active text removed; file-presence and Bronze-sample labels describe actual behavior.

## Verification boundaries

Initial local gate441passed/48deselected; subsequent gate447passed/48deselected,0failures/errors/skips,526dependency warnings,167.45s. Those gates preceded the final Tiki fallback removal and independent article parser. A current-source broad gate is running; do not treat447as final-source certification. Latest focused regressions35passed. Unit gate312passed preceded the final4added cases. Ruff,compileall andgit diff--check pass. Live Tiki capability test1passed; this small check does not establish required review volume.

## Remaining confirmed findings and delivery work

DOC01still requires a new3Vcorrection/report (historical evidence preserved). OBS01partly repaired; service exporters and operational deployment still pending. Overall audit of other modules/tests is incomplete. Provenance propagation through all engines/warehouse is incomplete. Simulation isolation, article transport/processing/serving, actual load profiling,9volume runs/9Kafka-rate runs, browser verification and final whole-change review remain outstanding. Docker socket still permission denied; user said they will restore it. No whole-plan completion or production readiness claim is made.

A separate current3Vassessment is published as three_v_assessment_2026-11-02.md. It corrects interpretation of the historical offline queue claim without rewriting old evidence. Tinhte related-link follow-up reached45articles;50-article requirement remains open. New-contract gate17passed; no integration/source acceptance implied.

## Latest current-source gate

Current implementation451passed/48deselected,0failures/errors/skips,526dependency warnings,164.87s. Evidence:source_integrity_local_2026-11-02.xml. Includes finalTiki fallback removal and article parser. Source-new-contract gate17passed(overlaps451; do not sum). LiveTiki capability1passed separately. Ruff/compileall pass. Full status/source fingerprint:source_integrity_status_2026-11-02.json. All earlier running-gate notes above are chronological history, superseded by this result. Final delivery remains incomplete.
