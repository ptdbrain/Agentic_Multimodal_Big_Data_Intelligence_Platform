# Basic Analytics Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Make basic descriptive analysis available as a coherent, read-only dashboard workflow without AI.
**Architecture:** Pure pandas report builder, scoped warehouse snapshot, Streamlit page using existing UI helpers and TrendAnalyzer.
**Tech Stack:** Existing pandas, Plotly, Streamlit, pytest/AppTest; no new dependencies.
**Spec:** docs/superpowers/specs/2026-11-13-basic-analytics-design.md

## Global Constraints

- Preserve user dirty Grafana JSON and root user files. Keep current checkout because services use absolute paths.
- Never seed live data, modify warehouse records, infer sales, mix source classes/currencies or fill unavailable metrics with zero.
- Calendar range inclusive UTC; catalog is current context, reviews/prices are filtered observations.

## Review Focus

- Sparse/empty tables and nullable metadata must render without fake samples.
- Source/group identity collisions must not multiply or misattribute reviews/prices.
- Mixed currencies and invalid prices must not distort averages or changes.
- Reversed dates, missing timestamps and one-day history need explicit handling.
- Filtering must change KPI/charts/tables/exports consistently; ranking thresholds retain sample sizes.

## Tasks

- [x] 1. Write failing arithmetic/provenance/date/currency tests; implement analytics/basic.py reporting and summaries. Run focused tests RED→GREEN.
- [x] 2. Write failing AppTest navigation/filter/empty/price-history checks; add dashboard/pages/9_Basic_Analytics.py and navigation entry. Read products/reviews/price_daily_stats from one connection without fallback. Run interaction and existing dashboard tests.
- [x] 3. Update dashboard usage/methodology docs; run local correctness/lint and actual read-only PostgreSQL AppTest if available. Perform one final independent review, fix important findings, and record evidence. Commit focused verified changes, no merge/push.
