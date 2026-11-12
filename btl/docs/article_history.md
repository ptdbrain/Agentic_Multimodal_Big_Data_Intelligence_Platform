# Article serving and version history

The article pipeline serves a cumulative `articles` table keyed by `article_id`.
A successful run inserts new identities and refreshes identities present in that
run. An article absent from a later batch remains served. Collection shortages
and bounded crawls therefore do not delete previously observed articles.

`article_versions` records the first accepted snapshot and each subsequent
change to the content fingerprint. Its key is `(article_id, version_number)`;
the publisher only inserts versions and never updates or deletes them. The
fingerprint covers title, body, normalized publication time, raw publication
value, and publication precision. A title or publication correction is a
version, even when the body's `content_sha256` is unchanged. New collection
times, evidence references, and run identifiers alone do not create versions.
A return to an earlier fingerprint after an intervening change creates another
observed version.

Each version retains its original title, body, publication and collection
clocks (raw and normalized), evidence hashes/references, acquisition metadata,
parser/schema versions, collection run, and publishing run. The current article
row carries the latest accepted observation and its `current_version`,
`content_fingerprint`, `first_seen_run_id`, and `last_seen_run_id`. Reobserving
unchanged content refreshes this current observation without modifying the
version's original collection or evidence. These run fields identify actual
pipeline observations; they are not inferred source publication clocks.

Daily SQL statistics count the cumulative current articles, rather than the
number of observations or versions. Within the publication transaction, the
publisher rebuilds dates touched by both the previous and incoming publication
times. Publication-date corrections remove obsolete contributions and add the
new ones. Dates made empty are removed. Articles with unknown, date-only, or
otherwise non-instant publication values remain served but contribute to no
daily statistic. SQL articles, versions, affected daily statistics, and the run
manifest commit or roll back together.

The Silver and Gold lake objects remain immutable **per-run snapshots** at the
existing run-specific paths. Their Gold counts describe that run's accepted
articles; cumulative serving totals are in SQL. A failed SQL transaction may
leave unreferenced lake objects, as before; only `article_pipeline_runs` commits
the lake-to-SQL handoff. The canonical lease and per-run lock still govern
publication. Retrying an already committed article `run_id` raises the existing
`ValueError`; reobserving content requires a new run ID.

Schema initialization applies additive, repeatable migrations. Existing article
rows retain their stored values; new history and lineage columns remain null.
It neither invents old raw timestamps nor fabricates versions from existing
rows. A legacy identity's next accepted observation starts recorded version
history at 1. Its unknown `first_seen_run_id` stays unknown. History before this
feature cannot be reconstructed from a current serving row. Within a batch,
the existing ETL policy still accepts only the latest unambiguous observation
per identity; superseded candidates are not promoted to committed versions.

Local verification uses isolated SQLite and local lake storage:

```bash
python -m pytest tests/test_article_history.py tests/test_article_pipeline.py -q
```

The same parameterized SQL supports PostgreSQL, but these unit tests do not
prove live PostgreSQL/MinIO operation, off-host backup restoration, or disaster
recovery. Those require their separate infrastructure and operational gates.

A delayed older observation may add a previously unseen historical version but
cannot replace a newer current observation. Replaying the same historical
observation does not append another version. Conflicting fingerprints at the
same article/collection clock are refused across runs, with the SQL transaction
rolled back. Version numbers represent acceptance order; use the retained
collection clocks to order observations by when they occurred.
