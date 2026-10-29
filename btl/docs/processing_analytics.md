# Processing and analytics contracts

## Normalization and observation time

Spark ETL and the local pandas normalizer preserve raw numeric and timestamp
fields before trimming. Invalid rating/price/time values become null and emit
`normalization_errors` plus `normalization_status=invalid`. A record can carry
multiple codes. String raw values retain their original text; numeric values
retain their value, with Spark sometimes using exponent notation when stored as
text. Supported price formats include US, Vietnamese and EU separators and
scientific notation. Supported timestamps include ISO offsets, epoch seconds or
milliseconds, and the slash date formats covered by the normalization tests.

`time_available` describes the business observation: `review_date`, `created_at`
or price `timestamp`. `ingestion_time_available` describes the separate ingestion
clock. Missing optional observation time stays null and can remain in Silver;
malformed supplied values are quarantined. Quarantine processing time is an audit
clock, not an observation.

`STRICT_ANALYTICS_TIME=true` is the default. Daily Gold excludes reviews that
have no observation date. Setting it to `false` explicitly enables the historical
ingestion-date fallback; such groups retain `time_available=false`, and their
fallback dates do not establish `data_as_of`. `ALLOW_LOCAL_ANALYTICS_FALLBACK=false`
is the default; enabling it is permitted only in local/test mode and only for
typed missing-path failures. Permission, schema and connectivity errors propagate.

## Aggregation and publication

Product daily prices prefer observed prices on that date, then a finite positive
catalog price. `price_source` is `observed`, `catalog_fallback` or `unavailable`.
Unavailable metrics remain null. Catalog values never create price observations. Numeric source values retain their
decimal value; locale separator interpretation applies to text. Supported text
grammar is validated before conversion; malformed signs, exponents and grouping
are rejected instead of stripped into a different price. Local Gold uses the
same half-up rounding as Spark SQL.
Price daily counts record actual observations and can be built without reviews.

Rating means use individual eligible reviews, with `rating_sample_size` and the
equivalent `weighted_avg_rating`. Sample standard deviation stays null for one
observation; zero requires a multi-observation constant sample. Price volatility
also exposes an insufficient-data status instead of reporting an unknown value as
zero. Brand/category daily catalog price averages
give each reviewed product one price contribution per date. Comparative catalog
analysis includes all catalog products, independently of review volume.

Unknown input lineage stays null; an analysis run ID does not establish a source
run. Canonical Silver producer manifests bind run/count evidence to current DQ;
every consumed nonempty dataset extends the gate requirements, including prices.
An explicitly excluded price input never reloads an older canonical price path.

All four Gold marts carry `run_id`, `source_run_id`, `data_as_of` and
`time_available`. Validation checks every mart before the first write, including
required/unique grain, null keys, finite metrics and domain constraints. Storage
failure recovery remains per path; this is not a transaction spanning four files.
Publishing serializes the whole mart group and records an in-progress/complete
generation manifest with row counts. Both engines publish typed empty daily
outputs, so an empty new result cannot leave old files current. Export reads the
group under the same lock and refuses incomplete, count-mismatched, mixed or
partly unknown mart generations. A missing/invalid-schema snapshot is an error;
an explicit schema-bearing empty snapshot is a valid zero result. All warehouse writes share one transaction, so a late failure rolls back
the group. Daily mart tables are full snapshots: keys absent from the candidate
are removed, and explicit empty snapshots clear their tables. DQ and anomaly
incident tables preserve history. Dashboard connections hold a read snapshot across queries (PostgreSQL
read-only repeatable read; SQLite transaction with WAL). Close the connection
after the read group to release that snapshot. Products and Data Quality share
one connection for their related table/report loads; opening a new connection
for each loader would break this guarantee.
Warehouse migrations add nullable fields and preserve unknown legacy metadata,
including raw/status/errors and the two clocks in served products/reviews.
An integration/production `build_all` call always requires a current run ID and
passing DQ reports; `enforce_dq_gate=False` cannot disable that requirement.

## Trends and anomalies

Trends and Overview use calendar windows. Missing days are reported, not converted
to zero observations. Rolling ratings are weighted by review samples. Coverage,
window bounds and sufficient/insufficient status are visible; insufficient
smoothed points are hidden.

Anomalies carry the pipeline run and DQ status. Failed/stale/invalid quality
suppresses events; standalone analysis is labelled `unverified`. Rating baselines
count historical reviews, burst baselines count time buckets, and price baselines
count price observations. Bounds, method and current sample are persisted. Price
detectors remain retrospective leave-one-out analysis and may use observations
after a candidate; they are not causal alerts.

## Quality evidence and remaining verification

The DQ gate requires measured received/valid/duplicate/missing/score fields and
checks count reconciliation, score consistency and supplied ratios. Unknown
measurements cannot authorize Gold as if they were zero. Every review aggregate
uses the same eligible observations for means, counts, ratios and baselines.

Persisted pipeline reports include quarantine counts, input records without
observation time, untimed Silver rows, all normalization codes and first quarantine
reasons. Normalization counts can overlap; first-reason counts partition quarantine.
Dashboard serving-data revalidation is labelled separately from these Bronze reports.
Legacy unmeasured values remain null. KPI rating weights only available rating
means by their rating samples, falling back to legacy review counts when sample
metadata is absent. No rating or DQ observations means `Unavailable`, not zero.
An evaluated empty anomaly set replaces the current Gold snapshot; warehouse
incident history retains original run IDs and remains labelled as history.

See [the completion audit](reports/processing_plan_remaining.md) for remaining
rollout, concurrency, warehouse and service verification. Local Spark/SQLite tests
do not establish PostgreSQL/MinIO/Kafka integration or a production load SLA.

## Isolated rollout comparison

Run comparisons against frozen copies of the old and candidate Gold directories:

```bash
python scripts/compare_processing_snapshots.py \
  --before /isolated/old/gold --after /isolated/new/gold \
  --output /isolated/comparison.json
```

The command reads the four daily marts and anomaly events, validates their grains,
then reports counts, schema changes, common-column hashes, changed metrics and
added/removed event IDs. It refuses missing snapshots and existing report outputs.
Run/source IDs are excluded from business hashes; schema additions are explicit.
A changed report requires review, not automatic acceptance. Retain the old copy
and input hashes until rollout is accepted; rollback uses that frozen copy and
the previous configuration. Do not copy a candidate into production implicitly.

The executed canonical-fixture comparison is recorded in
`reports/processing_backfill_comparison_final.json`. New replay hashes match. Historical
price averages and single-sample deviations change by design. Anomaly ID changes
also include old date-only versus explicit UTC timestamp formatting; inspect the
semantic event comparison before merging incident history. This evidence covers
pandas Gold/anomalies on the same validated Silver inputs, not a production
historical backfill or PostgreSQL migration.

A reproducible local processing load run is available separately:

```bash
python scripts/benchmark_processing.py --output /isolated/new-load-report.json
```

It generates labelled synthetic inputs in a new temporary directory, runs 100000
review inputs plus product/price data through Spark ETL/Gold, anomaly evaluation,
SQLite export and DashboardDB queries, and measures time/process-tree memory.
It excludes Kafka, MinIO, PostgreSQL, UI rendering and JVM startup. One measured
run is acceptance evidence for this boundary, not a distributed production SLA.


## Actual service acceptance

The latest local and native-service gates, full Kafka/MinIO/Spark/PostgreSQL100k run, two-run replay, seven-page real-warehouse AppTest and actual Airflow LocalExecutor evidence are indexed in [processing_service_acceptance.json](reports/processing_service_acceptance.json). That report records final and measured source fingerprints separately. Native vendor-service execution does not establish Docker Compose deployment, multi-host saturation or a production SLA.


Final Docker acceptance is complete: [current-source Docker report](reports/processing_docker_acceptance.json) links the46-case suite,two-run Silver/Gold content replay,seven real-warehouse AppTest pages and separate-network-worker ownership proof. The native100k artifact remains a separate measurement. Production history merge/rollout and physical multi-host/browser-load guarantees remain outside this functional acceptance.
