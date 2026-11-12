# Data engineering completion plan

Approved conversation plan 2026-11-12. Source acceptance: Tiki mandatory, independent real web, labelled simulation; Lazada optional/blocked honestly. Single-host implementation; alerts webhook; RPO24h/RTO60min target, local recovery only until offhost target supplied. Never fake live data or backfill observation clocks.

1. Source registry/config and Airflow-managed separate source jobs. Reuse25c490b watchlist capture. Persist source/mode/namespace/config in run manifest; no implicit dataset default for live jobs. Separate capture+pending ingestion so failures resume collected input. Legacy canonical DAG diagnostic manual only when managed schedules enabled; prevent timer/Airflow duplicate ownership.
2. Incremental review paging per product; cursor updated only after durable capture; explicit bounded shortage. Retrypartial missing products using newimmutableattempts/clocks. Persistent web URL/fingerprint/retry state. Explicit complete/partial/failed/blocked/skipped and correct labelledsimulation.
3. Cumulative articles/current upsert, immutablecontentversions, recomputeaffecteddaystats; additive migration. Retain all raw/evidence/no manufactured historicalversions. Snapshot vsincremental semantics explicit.
4. Runheartbeat/stage/output references; inspect/plan-recovery/resume/abort, verifieddeadowner notageonly, Airflowstatuscheck, lastverifiedcheckpoint execution, stableoperationIDs, no manualleaseSQL.
5. Kafka failOnDataLoss true, pinned assign/start/end replay manifest, retention pre/post, actual KafkaSource freshcheckpoints withrestart; exacteventaudit+window/quarantineverification; servingpointer wired. File replay notclaimedKafkaresumption.
6. Close monitoring actualprogress/errors/freshness/inputcounts, Kafka lifecycle repeatedruns, alerts firing+resolved+maintenance, readableGrafana and ownedservicecommands. No inventedgauges.
7. Full local PG/Airflow/MinIO/checkpoint/registry restore +resume/replay; coordinatedcapture and dailytimer only configuredfenced environment. Measure fullRPO/RTO; localnotwholehostDR.
8. Isolated3Kafka2Sparkworker chaos inclconsumer/driver/storageoutages; compare1/2workers10k100k3trials, batch vsstreamlatencyseparate, no inventedSLA. CI/docs/evidence. 72h at least3realscheduledcycles, noforcedremoteevents/reviewquota. ExternalDockerpermission/offhostdestination cannotbe markedpassed withoutaccess.

Preserve existing user files/slides and two uncommittedGrafanareadabilitychanges. No merge/push withoutrequest. Commits perverifiedsubsystem; no fabricated72h evidence.
