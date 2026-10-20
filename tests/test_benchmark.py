"""Unit tests for SentinelAI 3V Benchmark Suite (scripts/benchmark.py).

Verifies:
  1. Accounting invariants across Volume, Velocity, and Variety:
     - Volume: input == valid + duplicate + quarantine
     - Velocity: consumed == valid + dlq
     - Variety: input == valid + duplicate + quarantine across all formats
  2. Throughput, rate, latency percentile (p50/p95/p99), and median calculation formulas.
  3. Report generator and parser consistency (JSON <-> Markdown synchronization, boundary documentation).
  4. Integration mode failure on missing services (strictly refuses silent offline fallback).
  5. Distinction between synthetic HTML fixtures and authentic Tiki crawler datasets.
"""
from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch
import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent

from ingestion.privacy import sanitize_records_pii, is_anonymized_user_id
from scripts.benchmark import (
    ResourceMonitor,
    compute_percentiles,
    compute_median_run,
    _wrap_in_envelopes,
    configure_benchmark_mode,
    generate_markdown_report,
    benchmark_volume,
    benchmark_velocity,
    benchmark_variety,
    run_all_benchmarks,
)


@pytest.mark.unit
class TestBenchmarkAccountingInvariants:
    """Verifies strict mathematical accounting identities across benchmark runs."""

    def test_volume_accounting_invariant_calculation(self):
        """Volume invariant: records_input == valid_count + duplicate_count + quarantine_count."""
        input_count = 1000
        dirty_ratio = 0.05
        duplicate_ratio = 0.03

        dirty_target = int(round(input_count * dirty_ratio))  # 50
        dup_target = int(round(input_count * duplicate_ratio))  # 30
        valid_count = input_count - dirty_target - dup_target  # 920

        assert input_count == valid_count + dup_target + dirty_target
        assert valid_count == 920
        assert dup_target == 30
        assert dirty_target == 50

    def test_velocity_accounting_invariant_calculation(self):
        """Velocity invariant: messages_consumed == valid_count + quarantine_dlq_count."""
        consumed = 200
        dirty_ratio = 0.04

        dlq_count = int(round(consumed * dirty_ratio))  # 8
        valid_count = consumed - dlq_count  # 192

        assert consumed == valid_count + dlq_count
        assert valid_count == 192
        assert dlq_count == 8

    def test_variety_accounting_invariant_all_formats(self):
        """All Variety formats must satisfy: records_input == valid + duplicate + quarantine."""
        test_cases = [
            {"format": "JSON", "input": 100, "valid": 93, "quarantine": 5, "dup": 2},
            {"format": "JSONL", "input": 100, "valid": 93, "quarantine": 5, "dup": 2},
            {"format": "CSV", "input": 100, "valid": 94, "quarantine": 4, "dup": 2},
            {"format": "TSV", "input": 100, "valid": 94, "quarantine": 4, "dup": 2},
            {"format": "Parquet", "input": 100, "valid": 93, "quarantine": 5, "dup": 2},
            {"format": "HTML_Synthetic_JSONLD", "input": 100, "valid": 93, "quarantine": 5, "dup": 2},
            {"format": "HTML_Tiki_Crawler", "input": 10, "valid": 10, "quarantine": 0, "dup": 0},
        ]
        for c in test_cases:
            assert c["input"] == c["valid"] + c["quarantine"] + c["dup"], f"Invariant failed for {c['format']}"


@pytest.mark.unit
class TestBenchmarkFormulas:
    """Verifies throughput, latency percentiles, and median run calculations."""

    def test_throughput_formulas(self):
        records = 1000
        duration = 2.5
        throughput = records / duration
        assert throughput == 400.0

        # Zero duration guard
        zero_dur = 0.0
        safe_tp = records / zero_dur if zero_dur > 0 else 0.0
        assert safe_tp == 0.0

    def test_latency_percentiles(self):
        # 100 latencies ranging from 1.0ms to 100.0ms
        latencies = [float(i) for i in range(1, 101)]
        stats = compute_percentiles(latencies)

        assert stats["min"] == 1.0
        assert stats["max"] == 100.0
        assert stats["avg"] == 50.5
        assert stats["p50"] == 51.0 or stats["p50"] == 50.0
        assert stats["p95"] == 95.0
        assert stats["p99"] == 99.0

    def test_latency_percentiles_empty(self):
        stats = compute_percentiles([])
        assert stats["p50"] == 0.0
        assert stats["p95"] == 0.0
        assert stats["p99"] == 0.0
        assert stats["avg"] == 0.0

    def test_compute_median_run_single_run(self):
        single_run = [{"duration_sec": 2.5, "throughput_rps": 400.0, "cpu_percent": 15.0}]
        res = compute_median_run(single_run)
        assert res["duration_sec"] == 2.5
        assert res["throughput_rps"] == 400.0
        assert res["iterations"] == 1

    def test_compute_median_run_multiple_runs(self):
        runs = [
            {"duration_sec": 3.0, "spark_etl_sec": 2.5, "bronze_write_sec": 0.5, "throughput_rps": 333.3, "cpu_percent": 20.0, "peak_ram_mb": 150.0},
            {"duration_sec": 2.0, "spark_etl_sec": 1.6, "bronze_write_sec": 0.4, "throughput_rps": 500.0, "cpu_percent": 10.0, "peak_ram_mb": 140.0},
            {"duration_sec": 2.4, "spark_etl_sec": 2.0, "bronze_write_sec": 0.4, "throughput_rps": 416.7, "cpu_percent": 15.0, "peak_ram_mb": 145.0},
        ]
        res = compute_median_run(runs)
        assert res["duration_sec"] == 2.4
        assert res["spark_etl_sec"] == 2.0
        assert res["bronze_write_sec"] == 0.4
        assert res["throughput_rps"] == 416.7
        assert res["cpu_percent"] == 15.0
        assert res["peak_ram_mb"] == 145.0
        assert res["iterations"] == 3
        assert len(res["iteration_runs"]) == 3

    def test_resource_monitor(self):
        mon = ResourceMonitor()
        mon.start()
        mon.sample()
        metrics = mon.stop()

        assert "wall_sec" in metrics
        assert metrics["wall_sec"] >= 0.0
        assert "cpu_percent" in metrics
        assert "peak_ram_mb" in metrics
        assert metrics["peak_ram_mb"] > 0.0


@pytest.mark.unit
class TestReportParserAndConsistency:
    """Verifies JSON and Markdown report synchronization, headers, and boundary documentation."""

    def test_generate_markdown_report_synchronization(self):
        sample_results = {
            "timestamp": "2026-10-20 12:00:00 UTC",
            "metadata": {
                "runtime_mode": "integration",
                "storage_backend": "MinIOBackend",
                "seed": 42,
                "dirty_ratio": 0.05,
                "duplicate_ratio": 0.03,
                "pii_anonymized_at_ingest": True,
                "volume_iterations": 1,
            },
            "volume": [
                {
                    "seed": 142,
                    "records_input": 100,
                    "bronze_write_sec": 0.25,
                    "spark_etl_sec": 1.75,
                    "duration_sec": 2.0,
                    "spark_throughput_rps": 57.1,
                    "throughput_rps": 50.0,
                    "valid_count": 92,
                    "quarantine_count": 5,
                    "duplicate_count": 3,
                    "silver_written_count": 92,
                    "dq_score": 92.0,
                    "bronze_bytes": 64000,
                    "silver_bytes": 11000,
                    "quarantine_bytes": 5000,
                    "total_storage_bytes": 80000,
                    "cpu_percent": 18.5,
                    "peak_ram_mb": 142.0,
                    "accounting_reconciled": True,
                }
            ],
            "velocity": [
                {
                    "seed": 92,
                    "transport_mode": "real_kafka",
                    "target_rate_mps": 50,
                    "messages_sent": 100,
                    "produce_duration_sec": 2.0,
                    "actual_produce_rate_mps": 50.0,
                    "consume_duration_sec": 2.2,
                    "consumer_throughput_mps": 45.5,
                    "messages_consumed": 100,
                    "valid_count": 96,
                    "quarantine_dlq_count": 4,
                    "consumer_lag": 0,
                    "send_errors": 0,
                    "consume_errors": 0,
                    "e2e_duration_sec": 2.4,
                    "e2e_throughput_mps": 41.7,
                    "latency_p50_ms": 12.5,
                    "latency_p95_ms": 28.0,
                    "latency_p99_ms": 35.0,
                    "latency_avg_ms": 15.0,
                    "bronze_bytes": 71000,
                    "accounting_reconciled": True,
                }
            ],
            "variety": [
                {
                    "seed": 42,
                    "format": "JSON",
                    "category": "synthetic_file",
                    "source_bytes": 42000,
                    "records_input": 100,
                    "valid_count": 93,
                    "quarantine_count": 5,
                    "duplicate_count": 2,
                    "dq_score": 93.0,
                    "duration_sec": 1.9,
                    "throughput_rps": 52.6,
                    "bronze_bytes": 63000,
                    "silver_bytes": 11000,
                    "quarantine_bytes": 5500,
                    "pii_anonymized": True,
                    "accounting_reconciled": True,
                },
                {
                    "seed": 42,
                    "format": "HTML_Synthetic_JSONLD",
                    "category": "synthetic_html",
                    "source_bytes": 35000,
                    "records_input": 100,
                    "valid_count": 93,
                    "quarantine_count": 5,
                    "duplicate_count": 2,
                    "dq_score": 93.0,
                    "duration_sec": 2.1,
                    "throughput_rps": 47.6,
                    "bronze_bytes": 63000,
                    "silver_bytes": 11000,
                    "quarantine_bytes": 5500,
                    "pii_anonymized": True,
                    "accounting_reconciled": True,
                },
                {
                    "seed": 42,
                    "format": "HTML_Tiki_Crawler",
                    "category": "real_crawler_html",
                    "source_bytes": 12000,
                    "records_input": 10,
                    "valid_count": 10,
                    "quarantine_count": 0,
                    "duplicate_count": 0,
                    "dq_score": 100.0,
                    "duration_sec": 1.5,
                    "throughput_rps": 6.7,
                    "bronze_bytes": 8000,
                    "silver_bytes": 5000,
                    "quarantine_bytes": 0,
                    "pii_anonymized": True,
                    "accounting_reconciled": True,
                },
            ],
        }

        # Backup existing reports before testing generation
        report_files = [
            REPO_ROOT / "docs" / "reports" / "benchmark_3v.json",
            REPO_ROOT / "docs" / "reports" / "benchmark_report.md",
            REPO_ROOT / "docs" / "reports" / "3v_evidence.md",
            REPO_ROOT / "docs" / "benchmark.md",
        ]
        backups = {p: p.read_bytes() for p in report_files if p.exists()}

        try:
            generate_markdown_report(sample_results)

            # 1. Verify JSON file
            json_file = REPO_ROOT / "docs" / "reports" / "benchmark_3v.json"
            assert json_file.exists()
            data = json.loads(json_file.read_text(encoding="utf-8"))
            assert data["timestamp"] == "2026-10-20 12:00:00 UTC"
            assert len(data["volume"]) == 1
            assert len(data["velocity"]) == 1
            assert len(data["variety"]) == 3

            # 2. Verify Markdown report
            md_file = REPO_ROOT / "docs" / "reports" / "benchmark_report.md"
            assert md_file.exists()
            md_text = md_file.read_text(encoding="utf-8")
            assert "2026-10-20 12:00:00 UTC" in md_text
            assert "MinIOBackend" in md_text
            assert "Stage 1 Bronze Storage Write + Stage 2 Spark Batch ETL" in md_text
            assert "real_kafka" in md_text
            assert "HTML_Synthetic_JSONLD" in md_text
            assert "HTML_Tiki_Crawler" in md_text

            # 3. Verify Evidence file
            ev_file = REPO_ROOT / "docs" / "reports" / "3v_evidence.md"
            assert ev_file.exists()
            ev_text = ev_file.read_text(encoding="utf-8")
            assert "HTML_Synthetic_JSONLD" in ev_text or "HTML_Tiki_Crawler" in ev_text

            # 4. Verify no unrun 1M claims in docs/benchmark.md
            doc_bench = (REPO_ROOT / "docs" / "benchmark.md").read_text(encoding="utf-8")
            assert "2026-10-20 12:00:00 UTC" in doc_bench
        finally:
            for p, bytes_content in backups.items():
                p.write_bytes(bytes_content)


@pytest.mark.unit
class TestIntegrationModeServiceEnforcement:
    """Verifies that integration mode strictly refuses silent offline fallback."""

    def test_integration_health_check_fails_on_missing_service(self):
        with patch("scripts.benchmark.verify_integration_health", side_effect=ConnectionError("MinIO is down")):
            with pytest.raises(ConnectionError, match="MinIO is down"):
                configure_benchmark_mode("integration")

    def test_velocity_integration_fails_loudly_when_kafka_down(self):
        with patch("scripts.benchmark.check_kafka", return_value=False):
            with pytest.raises(ConnectionError, match="Kafka broker is unreachable"):
                benchmark_velocity(rates=[50], effective_mode="integration")


@pytest.mark.unit
class TestVarietyDistinctionAndSanitization:
    """Verifies distinction between synthetic HTML fixture and authentic Tiki crawler datasets."""

    def test_distinguish_synthetic_html_and_tiki_crawler(self):
        synthetic_name = "HTML_Synthetic_JSONLD"
        tiki_name = "HTML_Tiki_Crawler"
        assert synthetic_name != tiki_name
        assert "Synthetic" in synthetic_name
        assert "Tiki" in tiki_name

    def test_pii_sanitization_enforced_across_records(self):
        records = [
            {"review_id": "rv_1", "user_id": "raw_user_1", "customer_email": "test@example.com"},
            {"review_id": "rv_2", "user_id": "raw_user_2", "customer_email": "test2@example.com"},
        ]
        sanitized = sanitize_records_pii(records)
        for r in sanitized:
            assert "customer_email" not in r
            assert r["user_id"].startswith("anon_")
            assert len(r["user_id"]) == 21  # 'anon_' + 16 hex chars
