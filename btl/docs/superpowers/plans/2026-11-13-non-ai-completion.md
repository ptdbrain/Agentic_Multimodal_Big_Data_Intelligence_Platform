# Non-AI Completion Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans inline; one final whole-branch review.

**Goal:** Complete the approved non-AI system integration, measured performance and Docker operational acceptance.
**Architecture:** Existing pipeline with isolated datasets, explicit servingbindings, SQL transactionalpublication/outbox, SparkstatefulParquet+SQL sinks, durablelineagereceipts, ESgenerationaliases and Docker-owned runtime.
**Tech Stack:** Python3.11, Java17, Spark3.5, PostgreSQL16, Kafka3.7, MinIO, Elasticsearch8.13, Airflow2.10.5, Streamlit, Prometheus/Grafana.
**Spec:** docs/superpowers/specs/2026-11-13-non-ai-completion-design.md (approved conversational plan and user choices).

## Constraints and review focus

Preserve native data, existingtwoGrafanaJSON/userrootfiles; currentfeaturecheckout remains reachable by runningservices. No merge/push, no generatedobserveddata or fakeacceptance. Originalcurrencies; realvsimulation separation. Failures/unknowns visible. Snapshotidentity/crossdatasetmixing, concurrentgenerations, partialESbulk, multisinkcommit/replay, late/futureevents and traversal/hashverification all need regressions. All publicinterfaces typed; no hiddenenvironment mutation acrosssessions.

## Ordered deliverables

- [ ] 1 Docker foundation: RED configuration/resource/runtimecontract tests; appimage dualvenvs + compatibleSparkworkers + completeCompose + private bootstrapconfig. Actual isolated startup/health/restart gate whenDocker accessallows.
- [ ] 2 Serving registry: RED sessionisolation/source/routing tests; immutable DatasetRef/ServingContext, explicit DashboardDB connection, sidebarselectors+web/home routing, publishedgenerationmetadata. RealPG andAppTest sourcechecks.
- [ ] 3 Streaming serving: RED exact-once SQLbatch/replay/generation tests; additive schema, durable sink receipts andCLI flags; Parquet+SQL recovery; dashboard10s views. ActualKafka/Spark/PG proof.
- [ ] 4 Statistical alerts: RED causalbaseline/units/minsamples/future/lateupdate tests; versioned evidence+stableIDs+revisions; causalprices; dashboardandloopback delivery/recovery. Real eventpath proof.
- [ ] 5 ES publishing: RED transactionaloutbox/pagedsnapshot/bulk/alias/supersession tests; wirecommerce+articlepublication, actualgeneration worker anddataset searchUI. RealES outage/retry/alias gate.
- [ ] 6 Lineage: RED objecthash/recordidentity/aggregation/path/legacyunknown tests; persistentreceipts andsafequeryservice; dashboarddrilldowns andCSV; trace actualpublishedrowtoactualBronze.
- [ ] 7 Wholepath gates/docs: currentbroadlocal+realintegration+AppTest+finalreview; batch/streamcampaigns, resourceaccounting, accuratebenchmark/docs; scopedcommits.
- [ ] 8 Cutover/recovery/stability: drainednativefencedsnapshot->newDockernamespace, actualrestore+resume andoneowner; publicationverification androllbackretention; new72h/3scheduledcycles measuredwindow, no prematurelycompleteclaim.

Each task follows RED->GREEN, records exact verification and current state in its ignored workspace ledger. Independent work may continue while external Docker permission is pending; dependent runtime certification stays pending.
