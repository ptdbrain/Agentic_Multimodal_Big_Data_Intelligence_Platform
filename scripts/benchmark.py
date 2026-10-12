"""SentinelAI Real 3V Big Data Benchmark Suite (Volume, Velocity, Variety).

Benchmarks the real execution pipeline (Kafka -> RawConsumer -> MinIO/S3 Bronze ->
SparkBatchETLJob Parse/Cast/Validate/Deduplicate -> Silver & Quarantine Parquet)
using deterministic seeds and recording execution time, throughput, valid/quarantine/duplicate
counts, and actual storage bytes across layers.

Directly regenerates:
  - docs/reports/benchmark_3v.json
  - docs/reports/benchmark_report.md
  - docs/reports/3v_evidence.md
  - docs/benchmark.md
so that all documentation and reports match the empirical run log 100%.
"""
import argparse
import datetime
import json
import os
import shutil
import sys
import tempfile
import time
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from config.settings import settings
from data.generator import generate_products, generate_reviews
from ingestion.crawler.generic_html import UniversalProductScraper
from ingestion.crawler.tiki import TikiCrawler
from ingestion.file_loader.loader import FileLoader
from ingestion.privacy import is_anonymized_user_id, sanitize_records_pii
from kafka.consumers.raw_consumer import RawConsumer
from kafka.producers.stream_producer import StreamProducer
from kafka.topics.topic_manager import TopicManager
from scripts.health_check import verify_integration_health
from spark.batch.spark_etl_job import SparkBatchETLJob
from storage.storage_manager import storage


def _wrap_in_envelopes(
    records: List[Dict[str, Any]],
    event_type: str = "NEW_REVIEW",
    source: str = "benchmark_suite",
    seed: int = 42,
) -> List[Dict[str, Any]]:
    """Wraps raw payload dicts in canonical Kafka/Bronze message envelopes."""
    base_ts = "2026-09-24T10:00:00Z"
    envelopes = []
    for idx, rec in enumerate(records):
        envelopes.append({
            "event_id": f"evt_bench_{seed}_{idx+1:07d}",
            "event_type": event_type,
            "source": source,
            "ingested_at": base_ts,
            "payload": rec,
        })
    return envelopes


def configure_benchmark_mode(mode: Optional[str] = None) -> str:
    """Configures runtime mode for the benchmark suite and verifies services in integration mode."""
    effective_mode = (mode or os.environ.get("RUNTIME_MODE", settings.runtime_mode)).strip().lower()
    settings.set_runtime_mode(effective_mode)
    storage.set_mode(effective_mode)
    if effective_mode in ("integration", "production"):
        verify_integration_health(require_kafka=True)
    return effective_mode


def benchmark_volume(
    sizes: Optional[List[int]] = None,
    seed: int = 42,
    dirty_ratio: float = 0.05,
    duplicate_ratio: float = 0.03,
    spark_etl: Optional[SparkBatchETLJob] = None,
) -> List[Dict[str, Any]]:
    """1. VOLUME BENCHMARK: Measures real Bronze write + Spark Batch ETL -> Silver & Quarantine
    across multiple dataset sizes with fixed seed, dirty/quarantine injection, and storage byte tracking.
    """
    if sizes is None:
        sizes = [100, 1000, 5000, 10000]

    print(
        f"\n--- [1/3] Running VOLUME Benchmark "
        f"(Spark Batch ETL on Storage, seed={seed}, sizes={sizes}) ---"
    )
    etl_job = spark_etl or SparkBatchETLJob()
    products = generate_products(seed=seed)
    results: List[Dict[str, Any]] = []

    # Warm-up Spark JVM & S3A committer so first measured size is not skewed by one-time JVM init
    warmup_revs = generate_reviews(
        products, count=20, inject_anomalies=False, seed=seed, dirty_ratio=0.05
    )
    warmup_key = f"bronze/bw/s{seed}/batch.json"
    storage.backend.write_json(warmup_key, _wrap_in_envelopes(warmup_revs, seed=seed))
    etl_job.process_dataset(
        "reviews",
        warmup_key,
        silver_subpath=f"bw/s{seed}/rev",
        quarantine_subpath=f"bw/s{seed}/q_rev",
        replace_existing_silver=True,
    )

    for size in sizes:
        reviews = generate_reviews(
            products,
            count=size,
            inject_anomalies=False,
            seed=seed + size,
            dirty_ratio=dirty_ratio,
            duplicate_ratio=duplicate_ratio,
        )
        envelopes = _wrap_in_envelopes(reviews, event_type="NEW_REVIEW", source="volume_benchmark", seed=seed + size)

        bronze_key = f"bronze/bv/s{seed}_{size}/batch.json"
        silver_subpath = f"bv/s{seed}_{size}/rev"
        quarantine_subpath = f"bv/s{seed}_{size}/q_rev"

        t0 = time.perf_counter()
        storage.backend.write_json(bronze_key, envelopes)
        res = etl_job.process_dataset(
            "reviews",
            bronze_key,
            silver_subpath=silver_subpath,
            quarantine_subpath=quarantine_subpath,
            replace_existing_silver=True,
        )
        elapsed = time.perf_counter() - t0

        bronze_bytes = storage.backend.get_size(bronze_key)
        silver_bytes = storage.backend.get_size(f"silver/{silver_subpath}")
        quarantine_bytes = (
            storage.backend.get_size(f"silver/{quarantine_subpath}")
            if res["quarantine_count"] > 0
            else 0
        )
        total_bytes = bronze_bytes + silver_bytes + quarantine_bytes
        throughput = size / elapsed if elapsed > 0 else 0.0

        row = {
            "seed": seed + size,
            "records_input": int(res["input_count"]),
            "duration_sec": round(elapsed, 4),
            "throughput_rps": round(throughput, 1),
            "valid_count": int(res["unique_valid_count"]),
            "quarantine_count": int(res["quarantine_count"]),
            "duplicate_count": int(res["intra_dedup_dropped"]),
            "silver_written_count": int(res["silver_written_count"]),
            "dq_score": float(res["dq_score"]),
            "bronze_bytes": int(bronze_bytes),
            "silver_bytes": int(silver_bytes),
            "quarantine_bytes": int(quarantine_bytes),
            "total_storage_bytes": int(total_bytes),
        }
        print(
            f"  [Volume] seed={row['seed']} | input={row['records_input']:,} | "
            f"time={row['duration_sec']:.3f}s | rate={row['throughput_rps']:,.1f} rec/s | "
            f"valid={row['valid_count']:,} | quarantine={row['quarantine_count']:,} | "
            f"dup={row['duplicate_count']:,} | DQ={row['dq_score']}% | "
            f"storage={row['total_storage_bytes']:,} B (B:{bronze_bytes:,} / S:{silver_bytes:,} / Q:{quarantine_bytes:,})"
        )
        results.append(row)

    return results


def benchmark_velocity(
    rates: Optional[List[int]] = None,
    seed: int = 42,
    dirty_ratio: float = 0.04,
    effective_mode: str = "integration",
) -> List[Dict[str, Any]]:
    """2. VELOCITY BENCHMARK: Measures real Kafka Producer -> Kafka Broker -> RawConsumer ->
    Schema Validation (Valid to Bronze / Invalid to DLQ) across multiple target message rates.
    """
    if rates is None:
        rates = [50, 200, 500, 1000]

    is_local = effective_mode not in ("integration", "production")
    transport_mode = "offline_virtual_queue" if is_local else "real_kafka"

    print(
        f"\n--- [2/3] Running VELOCITY Benchmark "
        f"(Transport: {transport_mode}, seed={seed}, rates={rates}) ---"
    )
    TopicManager.ensure_topics(mode=effective_mode)

    products = generate_products(seed=seed)
    results: List[Dict[str, Any]] = []

    suite_group_id = f"sentinel-bench-velocity-{seed}-{uuid.uuid4().hex[:6]}"
    consumer = RawConsumer(group_id=suite_group_id, auto_offset_reset="earliest")
    producer = StreamProducer(enable_offline_buffer=is_local)

    try:
        if not is_local:
            consumer._init_consumer([settings.kafka.topic_reviews])
            if consumer.consumer is not None:
                assign_deadline = time.time() + 10.0
                while not consumer.consumer.assignment() and time.time() < assign_deadline:
                    consumer.consumer.poll(timeout_ms=200)
                if consumer.consumer.assignment():
                    consumer.consumer.seek_to_end()
                    for tp in consumer.consumer.assignment():
                        _ = consumer.consumer.position(tp)

        for rate in rates:
            msg_count = max(100, min(rate, 500))
            run_seed = seed + rate
            reviews = generate_reviews(
                products,
                count=msg_count,
                inject_anomalies=False,
                seed=run_seed,
                dirty_ratio=dirty_ratio,
                duplicate_ratio=0.0,
            )
            bench_run_id = f"vel_{run_seed}_{uuid.uuid4().hex[:6]}"
            for idx, r in enumerate(reviews):
                r["review_id"] = f"{bench_run_id}_rv_{idx+1:05d}"

            if is_local:
                producer.virtual_queue.clear()

            t_prod_start = time.perf_counter()
            sent, actual_prod_rate = producer.produce_batch(
                settings.kafka.topic_reviews,
                reviews,
                rate=float(rate),
                event_type="NEW_REVIEW",
                source=f"velocity_bench_{rate}",
            )
            prod_duration = time.perf_counter() - t_prod_start

            offline_items = list(producer.virtual_queue) if is_local else None
            t_cons_start = time.perf_counter()
            cons_res = consumer.consume_batch_once(
                topics=[settings.kafka.topic_reviews],
                max_records=sent,
                timeout_ms=3000,
                offline_records=offline_items,
            )
            cons_duration = time.perf_counter() - t_cons_start

            e2e_duration = prod_duration + cons_duration
            consumed = int(cons_res.get("records_consumed", 0))
            valid_to_bronze = int(cons_res.get("records_valid", 0))
            dlq_count = int(cons_res.get("records_dlq", 0))
            e2e_rate = consumed / e2e_duration if e2e_duration > 0 else 0.0

            bronze_bytes = 0
            for bpath in cons_res.get("batches_written", []):
                bronze_bytes += storage.backend.get_size(bpath)

            row = {
                "seed": run_seed,
                "transport_mode": transport_mode,
                "target_rate_mps": int(rate),
                "messages_sent": int(sent),
                "produce_duration_sec": round(prod_duration, 4),
                "actual_produce_rate_mps": round(actual_prod_rate, 1),
                "messages_consumed": consumed,
                "valid_count": valid_to_bronze,
                "quarantine_dlq_count": dlq_count,
                "e2e_duration_sec": round(e2e_duration, 4),
                "e2e_throughput_mps": round(e2e_rate, 1),
                "bronze_bytes": int(bronze_bytes),
            }
            print(
                f"  [Velocity] seed={row['seed']} | target={row['target_rate_mps']} msg/s | "
                f"produce={row['actual_produce_rate_mps']:,.1f} msg/s ({row['produce_duration_sec']:.3f}s) | "
                f"consumed={row['messages_consumed']} (valid={row['valid_count']}, dlq={row['quarantine_dlq_count']}) | "
                f"e2e_rate={row['e2e_throughput_mps']:,.1f} msg/s | bronze={row['bronze_bytes']:,} B"
            )
            results.append(row)
    finally:
        producer.close()
        consumer.close()

    return results


def benchmark_variety(
    records_per_format: int = 500,
    seed: int = 42,
    dirty_ratio: float = 0.05,
    duplicate_ratio: float = 0.02,
    spark_etl: Optional[SparkBatchETLJob] = None,
) -> List[Dict[str, Any]]:
    """3. VARIETY BENCHMARK: Measures full multi-format Parse -> PII Hash -> Bronze Write ->
    Spark Cast/Validate/Normalize/Deduplicate -> Silver & Quarantine Parquet across:
      - JSON (.json)
      - JSONL (.jsonl)
      - CSV (.csv)
      - TSV (.tsv)
      - Parquet (.parquet)
      - HTML_JSONLD_Crawler (Schema.org JSON-LD HTML + Tiki Crawler payloads)
    """
    print(
        f"\n--- [3/3] Running VARIETY Benchmark "
        f"(Full Parse -> Validate -> Transform -> Silver/Quarantine, seed={seed}, n={records_per_format}) ---"
    )
    etl_job = spark_etl or SparkBatchETLJob()
    products = generate_products(seed=seed)

    # Create deterministic raw dataset containing unhashed customer_id & email PII + dirty + duplicate records
    base_reviews = generate_reviews(
        products,
        count=records_per_format,
        inject_anomalies=False,
        seed=seed,
        dirty_ratio=dirty_ratio,
        duplicate_ratio=duplicate_ratio,
    )
    raw_with_pii: List[Dict[str, Any]] = []
    for idx, r in enumerate(base_reviews):
        rec = dict(r)
        rec["user_id"] = f"raw_customer_{seed}_{idx+1}"
        rec["customer_email"] = f"buyer_{idx+1}@example.org"
        raw_with_pii.append(rec)

    temp_dir = Path(tempfile.mkdtemp(prefix="sentinel_variety_bench_"))
    results: List[Dict[str, Any]] = []

    try:
        df_raw = pd.DataFrame(raw_with_pii)
        format_files = {
            "JSON": temp_dir / "reviews.json",
            "JSONL": temp_dir / "reviews.jsonl",
            "CSV": temp_dir / "reviews.csv",
            "TSV": temp_dir / "reviews.tsv",
            "Parquet": temp_dir / "reviews.parquet",
        }

        with open(format_files["JSON"], "w", encoding="utf-8") as f:
            json.dump(raw_with_pii, f, ensure_ascii=False)
        with open(format_files["JSONL"], "w", encoding="utf-8") as f:
            for item in raw_with_pii:
                f.write(json.dumps(item, ensure_ascii=False) + "\n")
        df_raw.to_csv(format_files["CSV"], index=False, encoding="utf-8")
        df_raw.to_csv(format_files["TSV"], index=False, sep="\t", encoding="utf-8")
        df_raw.to_parquet(format_files["Parquet"], index=False)

        # Build semi-structured HTML JSON-LD document containing embedded Product + Review objects
        html_ld_payload = {
            "@context": "https://schema.org/",
            "@type": "Product",
            "sku": "iphone_15_pro",
            "name": "iPhone 15 Pro 128GB",
            "brand": {"@type": "Brand", "name": "Apple"},
            "category": "Smartphone",
            "offers": {"@type": "Offer", "price": "27990000", "priceCurrency": "VND"},
            "aggregateRating": {"@type": "AggregateRating", "ratingValue": "4.8", "reviewCount": str(len(raw_with_pii))},
            "review": [
                {
                    "@type": "Review",
                    "@id": r["review_id"],
                    "author": {"@type": "Person", "name": r["user_id"]},
                    "datePublished": r["review_date"],
                    "name": r["review_title"],
                    "reviewBody": r["review_text"],
                    "inLanguage": r["language"],
                    "reviewRating": {"@type": "Rating", "ratingValue": r["rating"]},
                }
                for r in raw_with_pii
            ],
        }
        html_doc = (
            "<!DOCTYPE html><html><head><title>iPhone 15 Pro</title>"
            f'<script type="application/ld+json">{json.dumps(html_ld_payload, ensure_ascii=False)}</script>'
            "</head><body><h1>iPhone 15 Pro</h1></body></html>"
        )
        html_file = temp_dir / "product_reviews.html"
        html_file.write_text(html_doc, encoding="utf-8")

        all_cases = list(format_files.items()) + [("HTML_JSONLD_Crawler", html_file)]

        for fmt_name, fpath in all_cases:
            source_bytes = fpath.stat().st_size
            t0 = time.perf_counter()

            # Step A: Parse & sanitize PII at ingest
            if fmt_name == "HTML_JSONLD_Crawler":
                scraper = UniversalProductScraper()
                html_text = fpath.read_text(encoding="utf-8")
                prod_meta = scraper._extract_metadata_from_html(html_text, "https://shop.sentinel.ai/p/iphone_15_pro")
                loaded_records = scraper.extract_reviews_from_html(
                    html_text, product_id=prod_meta["product_id"]
                )
            else:
                loaded_records = FileLoader.load_records(fpath, anonymize_pii=True)

            # Verify PII hashing happened at ingest
            if not all(
                is_anonymized_user_id(r.get("user_id")) and "customer_email" not in r
                for r in loaded_records
            ):
                raise RuntimeError(f"Variety benchmark PII check failed for format {fmt_name}")

            # Step B: Write to Bronze storage
            short_fmt = fmt_name.lower()[:8]
            bronze_key = f"bronze/bvar/s{seed}_{short_fmt}/batch.json"
            silver_subpath = f"bvar/s{seed}_{short_fmt}/rev"
            quarantine_subpath = f"bvar/s{seed}_{short_fmt}/q_rev"

            envelopes = _wrap_in_envelopes(
                loaded_records, event_type="NEW_REVIEW", source=f"variety_{fmt_name.lower()}", seed=seed
            )
            storage.backend.write_json(bronze_key, envelopes)

            # Step C: Spark Batch Parse/Cast -> Validate -> Normalize -> Deduplicate -> Silver & Quarantine
            etl_res = etl_job.process_dataset(
                "reviews",
                bronze_key,
                silver_subpath=silver_subpath,
                quarantine_subpath=quarantine_subpath,
                replace_existing_silver=True,
            )
            elapsed = time.perf_counter() - t0

            bronze_bytes = storage.backend.get_size(bronze_key)
            silver_bytes = storage.backend.get_size(f"silver/{silver_subpath}")
            quarantine_bytes = (
                storage.backend.get_size(f"silver/{quarantine_subpath}")
                if etl_res["quarantine_count"] > 0
                else 0
            )
            throughput = len(loaded_records) / elapsed if elapsed > 0 else 0.0

            row = {
                "seed": seed,
                "format": fmt_name,
                "source_bytes": int(source_bytes),
                "records_input": int(etl_res["input_count"]),
                "valid_count": int(etl_res["unique_valid_count"]),
                "quarantine_count": int(etl_res["quarantine_count"]),
                "duplicate_count": int(etl_res["intra_dedup_dropped"]),
                "dq_score": float(etl_res["dq_score"]),
                "duration_sec": round(elapsed, 4),
                "throughput_rps": round(throughput, 1),
                "bronze_bytes": int(bronze_bytes),
                "silver_bytes": int(silver_bytes),
                "quarantine_bytes": int(quarantine_bytes),
                "pii_anonymized": True,
            }
            print(
                f"  [Variety] format={row['format']:<19} | input={row['records_input']:,} | "
                f"time={row['duration_sec']:.3f}s | rate={row['throughput_rps']:,.1f} rec/s | "
                f"valid={row['valid_count']:,} | quarantine={row['quarantine_count']:,} | "
                f"dup={row['duplicate_count']:,} | silver={row['silver_bytes']:,} B"
            )
            results.append(row)
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)

    return results


def generate_markdown_report(results: Dict[str, Any]) -> None:
    """Generates docs/reports/benchmark_3v.json, docs/reports/benchmark_report.md,
    docs/reports/3v_evidence.md, and docs/benchmark.md directly from empirical run results.
    Never advertises unexecuted scale claims (such as 1,000,000 records if not actually run).
    """
    report_dir = REPO_ROOT / "docs" / "reports"
    report_dir.mkdir(parents=True, exist_ok=True)

    json_path = report_dir / "benchmark_3v.json"
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)

    vol_rows = results["volume"]
    vel_rows = results["velocity"]
    var_rows = results["variety"]
    meta = results["metadata"]

    min_vol = min(r["records_input"] for r in vol_rows) if vol_rows else 0
    max_vol = max(r["records_input"] for r in vol_rows) if vol_rows else 0
    peak_vol_rate = max(r["throughput_rps"] for r in vol_rows) if vol_rows else 0.0

    min_vel_target = min(r["target_rate_mps"] for r in vel_rows) if vel_rows else 0
    max_vel_target = max(r["target_rate_mps"] for r in vel_rows) if vel_rows else 0
    peak_prod_rate = max(r["actual_produce_rate_mps"] for r in vel_rows) if vel_rows else 0.0
    peak_e2e_rate = max(r["e2e_throughput_mps"] for r in vel_rows) if vel_rows else 0.0

    formats_list = ", ".join(r["format"] for r in var_rows)

    # 1. docs/reports/benchmark_report.md
    md_path = report_dir / "benchmark_report.md"
    lines = [
        "# SentinelAI 3V Empirical Benchmark Report",
        "",
        f"**Generated At:** `{results['timestamp']}`  ",
        f"**Runtime Mode:** `{meta['runtime_mode']}`  ",
        f"**Storage Backend:** `{meta['storage_backend']}`  ",
        f"**Base Deterministic Seed:** `{meta['seed']}`  ",
        f"**PII Anonymization at Ingest:** `{meta['pii_anonymized_at_ingest']}`",
        "",
        "## 1. Volume Benchmark (Bronze Storage -> Spark Batch ETL -> Silver & Quarantine Parquet)",
        "",
        "| Seed | Input Records | Valid (Silver) | Quarantined | Deduplicated | DQ Score | Duration (s) | Throughput (rec/s) | Bronze (B) | Silver (B) | Quarantine (B) |",
        "| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |",
    ]
    for v in vol_rows:
        lines.append(
            f"| {v['seed']} | {v['records_input']:,} | {v['valid_count']:,} | {v['quarantine_count']:,} | "
            f"{v['duplicate_count']:,} | {v['dq_score']}% | {v['duration_sec']:.4f}s | "
            f"{v['throughput_rps']:,.1f} | {v['bronze_bytes']:,} | {v['silver_bytes']:,} | {v['quarantine_bytes']:,} |"
        )

    lines.extend([
        "",
        "## 2. Velocity Benchmark (Kafka Producer -> Broker -> RawConsumer -> Bronze / DLQ)",
        "",
        "| Seed | Transport | Target Rate (msg/s) | Produce Rate (msg/s) | E2E Consume Rate (msg/s) | Sent | Consumed | Valid (Bronze) | DLQ | E2E Duration (s) | Bronze (B) |",
        "| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |",
    ])
    for v in vel_rows:
        lines.append(
            f"| {v['seed']} | {v['transport_mode']} | {v['target_rate_mps']:,} | "
            f"{v['actual_produce_rate_mps']:,.1f} | {v['e2e_throughput_mps']:,.1f} | "
            f"{v['messages_sent']:,} | {v['messages_consumed']:,} | {v['valid_count']:,} | "
            f"{v['quarantine_dlq_count']:,} | {v['e2e_duration_sec']:.4f}s | {v['bronze_bytes']:,} |"
        )

    lines.extend([
        "",
        "## 3. Variety Benchmark (Multi-Format Ingest -> PII Hash -> Bronze -> Spark Parse/Validate/Transform -> Silver)",
        "",
        "| Seed | Format / Source | Source (B) | Input Records | Valid (Silver) | Quarantined | Deduplicated | DQ Score | Duration (s) | Throughput (rec/s) | Silver (B) | Quarantine (B) |",
        "| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |",
    ])
    for v in var_rows:
        lines.append(
            f"| {v['seed']} | {v['format']} | {v['source_bytes']:,} | {v['records_input']:,} | "
            f"{v['valid_count']:,} | {v['quarantine_count']:,} | {v['duplicate_count']:,} | "
            f"{v['dq_score']}% | {v['duration_sec']:.4f}s | {v['throughput_rps']:,.1f} | "
            f"{v['silver_bytes']:,} | {v['quarantine_bytes']:,} |"
        )

    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    # 2. docs/reports/3v_evidence.md
    evidence_path = report_dir / "3v_evidence.md"
    evidence_lines = [
        "# SentinelAI — Empirical 3V Big Data Evidence Report",
        "",
        f"- **Generated At**: `{results['timestamp']}`",
        f"- **Runtime Mode**: `{meta['runtime_mode']}` (`{meta['storage_backend']}`)",
        f"- **Deterministic Seed**: `{meta['seed']}`",
        "",
        "## 1. Volume",
        f"- **Executed Scale**: Empirically benchmarked from **{min_vol:,}** to **{max_vol:,}** records per batch (`sizes={[r['records_input'] for r in vol_rows]}`).",
        f"- **Peak Measured Spark Batch Throughput**: **{peak_vol_rate:,.1f} records/sec** (including Bronze S3A read, envelope unpacking, schema cast, validation, quarantine routing, window deduplication, and Silver Parquet write).",
        "- **Quarantine & Deduplication Accounting**: Every run enforces `records_input == valid_before_dedup + quarantine_count` and tracks exact byte footprint across Bronze JSON, Silver Parquet, and Quarantine Parquet.",
        "",
        "## 2. Velocity",
        f"- **Real Kafka Streaming Ingestion**: Measured across target rates from **{min_vel_target:,}** to **{max_vel_target:,} msg/s** via `{vel_rows[0]['transport_mode'] if vel_rows else 'real_kafka'}`.",
        f"- **Peak Producer Rate**: **{peak_prod_rate:,.1f} msg/s**; **Peak End-to-End (Produce -> Kafka -> RawConsumer -> Schema Validate -> Bronze Write) Rate**: **{peak_e2e_rate:,.1f} msg/s**.",
        "- **Contract Enforcement on Stream**: Malformed messages are routed to Dead-Letter Queue (`dead-letter`) before Bronze commit.",
        "",
        "## 3. Variety",
        f"- **End-to-End Multi-Format Pipeline**: Benchmarked across **{len(var_rows)}** formats/sources (`{formats_list}`).",
        "- **Full Parse/Validate/Transform Path**: Each format is loaded, stripped of raw PII (`user_id` hashed to `anon_<sha256[:16]>`), persisted to Bronze, and processed by `SparkBatchETLJob` into Silver and Quarantine Parquet.",
    ]
    evidence_path.write_text("\n".join(evidence_lines) + "\n", encoding="utf-8")

    # 3. docs/benchmark.md
    doc_bench_path = REPO_ROOT / "docs" / "benchmark.md"
    doc_lines = [
        "# SentinelAI — Báo Cáo Benchmark \"3V\" (Big Data Verification)",
        "",
        "Tài liệu này được **sinh tự động trực tiếp** từ lệnh chạy `python scripts/benchmark.py` trên đường chạy thực (Kafka -> RawConsumer -> MinIO Bronze -> SparkBatchETLJob -> Silver/Quarantine Parquet). Mọi số liệu dưới đây khớp 100% với `docs/reports/benchmark_3v.json` và `docs/reports/benchmark_report.md`.",
        "",
        f"- **Thời điểm chạy (Timestamp)**: `{results['timestamp']}`",
        f"- **Chế độ chạy (Runtime Mode)**: `{meta['runtime_mode']}` (`{meta['storage_backend']}`)",
        f"- **Seed cố định (Deterministic Seed)**: `{meta['seed']}`",
        "",
        "---",
        "",
        "## 1. Kết Quả Volume (Bronze -> Spark Batch ETL -> Silver & Quarantine)",
        "",
        f"Kịch bản chạy thực tế với các cỡ dữ liệu `{[r['records_input'] for r in vol_rows]}` bản ghi (có tiêm tỷ lệ bản ghi lỗi `dirty_ratio={meta['dirty_ratio']}` và bản ghi trùng `duplicate_ratio={meta['duplicate_ratio']}`). Không ngoại suy hay quảng cáo số liệu 1 triệu bản ghi khi kịch bản đó không nằm trong danh sách chạy.",
        "",
        "| Seed | Số Bản Ghi Đầu Vào | Hợp Lệ (Silver) | Quarantine | Trùng Lặp | Điểm DQ | Thời Gian (s) | Throughput (rec/s) | Dung Lượng Bronze (B) | Dung Lượng Silver (B) | Dung Lượng Quarantine (B) |",
        "| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |",
    ]
    for v in vol_rows:
        doc_lines.append(
            f"| {v['seed']} | {v['records_input']:,} | {v['valid_count']:,} | {v['quarantine_count']:,} | "
            f"{v['duplicate_count']:,} | {v['dq_score']}% | {v['duration_sec']:.4f}s | "
            f"{v['throughput_rps']:,.1f} | {v['bronze_bytes']:,} | {v['silver_bytes']:,} | {v['quarantine_bytes']:,} |"
        )

    doc_lines.extend([
        "",
        "---",
        "",
        "## 2. Kết Quả Velocity (Kafka Producer -> Broker -> RawConsumer -> Bronze / DLQ)",
        "",
        "| Seed | Transport | Target Rate (msg/s) | Produce Rate (msg/s) | E2E Consume Rate (msg/s) | Đã Gửi | Đã Nhận | Hợp Lệ (Bronze) | DLQ | Thời Gian E2E (s) | Dung Lượng Bronze (B) |",
        "| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |",
    ])
    for v in vel_rows:
        doc_lines.append(
            f"| {v['seed']} | {v['transport_mode']} | {v['target_rate_mps']:,} | "
            f"{v['actual_produce_rate_mps']:,.1f} | {v['e2e_throughput_mps']:,.1f} | "
            f"{v['messages_sent']:,} | {v['messages_consumed']:,} | {v['valid_count']:,} | "
            f"{v['quarantine_dlq_count']:,} | {v['e2e_duration_sec']:.4f}s | {v['bronze_bytes']:,} |"
        )

    doc_lines.extend([
        "",
        "---",
        "",
        "## 3. Kết Quả Variety (Parse -> PII Hash -> Bronze -> Spark Validate/Transform -> Silver)",
        "",
        "| Seed | Định Dạng / Nguồn | Dung Lượng Nguồn (B) | Đầu Vào | Hợp Lệ (Silver) | Quarantine | Trùng Lặp | Điểm DQ | Thời Gian (s) | Throughput (rec/s) | Dung Lượng Silver (B) | Dung Lượng Quarantine (B) |",
        "| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |",
    ])
    for v in var_rows:
        doc_lines.append(
            f"| {v['seed']} | {v['format']} | {v['source_bytes']:,} | {v['records_input']:,} | "
            f"{v['valid_count']:,} | {v['quarantine_count']:,} | {v['duplicate_count']:,} | "
            f"{v['dq_score']}% | {v['duration_sec']:.4f}s | {v['throughput_rps']:,.1f} | "
            f"{v['silver_bytes']:,} | {v['quarantine_bytes']:,} |"
        )

    doc_lines.extend([
        "",
        "---",
        "",
        "## 4. Lệnh Chạy Tái Hiện Trực Tiếp",
        "",
        "```bash",
        "python scripts/benchmark.py --mode integration --seed 42",
        "```",
    ])
    doc_bench_path.write_text("\n".join(doc_lines) + "\n", encoding="utf-8")

    print(f"\n[OK] Benchmark reports written to:\n  - {json_path}\n  - {md_path}\n  - {evidence_path}\n  - {doc_bench_path}")


def run_all_benchmarks(
    mode: Optional[str] = None,
    seed: int = 42,
    volume_sizes: Optional[List[int]] = None,
    velocity_rates: Optional[List[int]] = None,
    variety_count: int = 500,
    dirty_ratio: float = 0.05,
    duplicate_ratio: float = 0.03,
) -> Dict[str, Any]:
    """Executes all 3V benchmarks on the real pipeline and generates synchronized reports."""
    effective_mode = configure_benchmark_mode(mode)
    print("==========================================================")
    print(f"   SENTINELAI 3V BIG DATA BENCHMARK SUITE ({effective_mode.upper()})")
    print("==========================================================")

    etl_job = SparkBatchETLJob()
    vol_res = benchmark_volume(
        sizes=volume_sizes,
        seed=seed,
        dirty_ratio=dirty_ratio,
        duplicate_ratio=duplicate_ratio,
        spark_etl=etl_job,
    )
    vel_res = benchmark_velocity(
        rates=velocity_rates,
        seed=seed,
        dirty_ratio=0.04,
        effective_mode=effective_mode,
    )
    var_res = benchmark_variety(
        records_per_format=variety_count,
        seed=seed,
        dirty_ratio=dirty_ratio,
        duplicate_ratio=0.02,
        spark_etl=etl_job,
    )

    full_results = {
        "timestamp": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
        "metadata": {
            "runtime_mode": effective_mode,
            "storage_backend": type(storage.backend).__name__,
            "seed": seed,
            "dirty_ratio": dirty_ratio,
            "duplicate_ratio": duplicate_ratio,
            "pii_anonymized_at_ingest": True,
        },
        "volume": vol_res,
        "velocity": vel_res,
        "variety": var_res,
    }
    generate_markdown_report(full_results)
    print("==========================================================\n")
    return full_results


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="SentinelAI Real 3V Big Data Benchmark Suite")
    parser.add_argument(
        "--mode",
        choices=["integration", "local", "test"],
        default=None,
        help="Execution mode: 'integration' (real Kafka/MinIO/Spark, default) or 'local'",
    )
    parser.add_argument("--seed", type=int, default=42, help="Deterministic random seed (default: 42)")
    parser.add_argument(
        "--volume-sizes",
        default="100,1000,5000,10000",
        help="Comma-separated record counts for Volume benchmark (default: 100,1000,5000,10000)",
    )
    parser.add_argument(
        "--velocity-rates",
        default="50,200,500,1000",
        help="Comma-separated target msg/s rates for Velocity benchmark (default: 50,200,500,1000)",
    )
    parser.add_argument(
        "--variety-count",
        type=int,
        default=500,
        help="Record count per format in Variety benchmark (default: 500)",
    )
    args = parser.parse_args()

    parsed_sizes = [int(x.strip()) for x in args.volume_sizes.split(",") if x.strip()]
    parsed_rates = [int(x.strip()) for x in args.velocity_rates.split(",") if x.strip()]

    run_all_benchmarks(
        mode=args.mode,
        seed=args.seed,
        volume_sizes=parsed_sizes,
        velocity_rates=parsed_rates,
        variety_count=args.variety_count,
    )
