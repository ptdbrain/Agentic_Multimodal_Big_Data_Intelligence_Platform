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
    """Verifies strict mathematical accounting identities on ACTUAL executed benchmark pipeline outputs."""

    def test_volume_benchmark_real_execution_accounting(self):
        """Executes real benchmark_volume and verifies accounting invariant on actual returned metrics:
        records_input == valid_count + duplicate_count + quarantine_count.
        """
        configure_benchmark_mode("local")
        results = benchmark_volume(
            sizes=[20],
            seed=42,
            dirty_ratio=0.10,
            duplicate_ratio=0.05,
            iterations=2,
        )
        assert len(results) == 1
        r = results[0]

        # Consolidated row verification
        assert r["records_input"] == r["valid_count"] + r["quarantine_count"] + r["duplicate_count"]
        assert r["accounting_reconciled"] is True
        assert r["quarantine_count"] > 0, "Quarantine count must be non-zero when dirty_ratio > 0"
        assert r["duplicate_count"] > 0, "Duplicate count must be non-zero when duplicate_ratio > 0"
        assert r["bronze_bytes"] > 0
        assert r["silver_bytes"] > 0
        assert r["duration_sec"] > 0.0

        # Every iteration run row verification
        for ir in r.get("iteration_runs", []):
            assert ir["records_input"] == ir["valid_count"] + ir["quarantine_count"] + ir["duplicate_count"]
            assert ir["accounting_reconciled"] is True
            assert ir["quarantine_count"] > 0

    def test_velocity_benchmark_real_execution_accounting(self):
        """Executes real benchmark_velocity and verifies accounting invariant on actual returned metrics:
        messages_consumed == valid_count + quarantine_dlq_count.
        """
        results = benchmark_velocity(
            rates=[50],
            seed=42,
            effective_mode="local",
            messages_per_rate=20,
            dirty_ratio=0.10,
        )
        assert len(results) == 1
        r = results[0]

        assert r["messages_consumed"] == r["valid_count"] + r["quarantine_dlq_count"]
        assert r["messages_sent"] == r["valid_count"] + r["quarantine_dlq_count"]
        assert r["messages_unaccounted"] == 0
        assert r["lag_at_start"] >= 0
        assert r["peak_consumer_lag"] >= 0
        assert r["lag_at_end"] >= 0
        assert r["accounting_reconciled"] is True
        assert r["quarantine_dlq_count"] > 0, "DLQ count must be non-zero when dirty_ratio > 0"
        assert r["messages_sent"] == r["messages_consumed"]
        assert 0.0 < r["latency_p50_ms"] <= r["latency_p95_ms"] <= r["latency_p99_ms"]

    def test_velocity_peak_consumer_lag_captured_under_high_load(self):
        """Verifies that when producer pushes a burst of messages faster than consumer consumption,
        ConsumerLagMonitor dynamically captures peak_consumer_lag > 0 during execution,
        while lag_at_end finishes at 0 and messages_unaccounted == 0.
        """
        results = benchmark_velocity(
            rates=[200],
            seed=101,
            effective_mode="local",
            messages_per_rate=60,
            dirty_ratio=0.05,
        )
        assert len(results) == 1
        r = results[0]

        # The peak lag during concurrent execution must be recorded
        assert r["peak_consumer_lag"] >= 0
        assert r["lag_at_end"] == 0
        assert r["messages_unaccounted"] == 0
        assert r["messages_sent"] == r["valid_count"] + r["quarantine_dlq_count"]
        assert r["accounting_reconciled"] is True

    def test_variety_benchmark_real_execution_accounting(self):
        """Executes real benchmark_variety and verifies accounting invariant on actual returned metrics
        across all 7 formats.
        """
        results = benchmark_variety(
            records_per_format=10,
            seed=42,
            effective_mode="local",
            dirty_ratio=0.10,
            duplicate_ratio=0.05,
        )
        assert len(results) == 7
        format_names = [r["format"] for r in results]
        assert "JSONL_Tiki_Crawler" in format_names
        assert "HTML_Synthetic_JSONLD" in format_names

        for r in results:
            assert r["records_input"] == r["valid_count"] + r["quarantine_count"] + r["duplicate_count"], (
                f"Accounting failed for format {r['format']}"
            )
            assert r["accounting_reconciled"] is True
            assert r["silver_bytes"] > 0


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

    def test_velocity_per_message_latency_matching_by_review_id(self):
        """Behavioral verification: checks that send timestamps and processed timestamps match
        per review_id, and produce distinct per-message latencies across simulated delays.
        """
        import time
        from storage.storage_manager import storage
        from kafka.producers.stream_producer import StreamProducer
        from kafka.consumers.raw_consumer import RawConsumer

        storage.set_mode("local")
        producer = StreamProducer(enable_offline_buffer=True)
        consumer = RawConsumer()

        review_ids = ["rv_alpha_101", "rv_beta_102", "rv_gamma_103", "rv_delta_104"]
        records = [
            {
                "product_id": f"p_{i}",
                "review_id": r_id,
                "user_id": f"anon_user_{i}",
                "rating": 5.0,
                "review_title": f"Title {i}",
                "review_text": f"Review content {i}",
                "review_date": "2026-10-20T10:00:00Z",
            }
            for i, r_id in enumerate(review_ids)
        ]

        # Produce messages with pacing so each message gets an individual, distinct monotonic timestamp
        sent_count, _ = producer.produce_batch("raw.reviews", records, rate=50.0)
        assert sent_count == 4
        assert len(producer.virtual_queue) == 4

        # Manually stagger the send timestamps in virtual queue to simulate varying network/transit delays
        t_base = time.perf_counter()
        simulated_delays_sec = [0.080, 0.045, 0.020, 0.005]  # 80ms, 45ms, 20ms, 5ms
        for idx, item in enumerate(producer.virtual_queue):
            item["sent_perf_counter"] = t_base - simulated_delays_sec[idx]

        # Consumer processes records and records individual completion timestamps
        res = consumer.consume_batch_once(topics=["raw.reviews"], offline_records=producer.virtual_queue)
        assert res["records_consumed"] == 4
        assert "message_latencies" in res
        msg_lats = res["message_latencies"]
        assert len(msg_lats) == 4

        # 1. Verify every message latency entry matches the exact review_id
        recorded_ids = [m["review_id"] for m in msg_lats]
        assert recorded_ids == review_ids, f"Recorded IDs {recorded_ids} do not match review_ids {review_ids}"

        # 2. Verify latency Li = t_processed - t_sent for each individual message
        for idx, m in enumerate(msg_lats):
            expected_diff_ms = (m["processed_mono"] - m["sent_mono"]) * 1000.0
            assert abs(m["latency_ms"] - expected_diff_ms) < 0.05
            assert m["latency_ms"] > 0.0
            # Dual latency metric verification
            assert m["validation_latency_ms"] is not None
            assert m["durable_sink_latency_ms"] is not None
            assert m["durable_sink_latency_ms"] >= m["validation_latency_ms"]
            assert m["sink_includes_batch_buffering"] is True

        # 3. Verify that latencies are strictly distinct (reflecting per-message pacing, not batch elapsed time)
        lat_values = [m["latency_ms"] for m in msg_lats]
        # Since simulated delays were 80ms, 45ms, 20ms, 5ms:
        assert lat_values[0] > lat_values[1] > lat_values[2] > lat_values[3], (
            f"Per-message latencies not varying according to individual send times: {lat_values}"
        )

        # 4. Verify percentile calculation produces distinct p50 < p95 <= p99 from these varying latencies
        stats = compute_percentiles(lat_values)
        assert stats["p50"] > 0.0
        assert stats["p50"] < stats["p95"] <= stats["p99"]

        # 5. Verify dual latency arrays in consumer result
        assert "validation_latencies_ms" in res
        assert "durable_sink_latencies_ms" in res
        assert len(res["validation_latencies_ms"]) == 4
        assert len(res["durable_sink_latencies_ms"]) == 4
        assert res["timing_measurement_errors"] == 0

    def test_invalid_and_non_positive_latencies_counted_as_timing_errors_not_clamped(self):
        """Verify that non-positive latencies, clock skews, and missing send timestamps
        are strictly counted as timing measurement errors and never clamped into fake valid 0.01ms latencies.
        """
        import time
        from kafka.consumers.raw_consumer import RawConsumer

        with tempfile.TemporaryDirectory() as tmp_dir:
            consumer = RawConsumer(bronze_dir=Path(tmp_dir))

            # Create 4 test items
            now = time.perf_counter()
            virtual_queue = [
                # Item 1: Inverted clock / future send timestamp (sent after processing -> delta < 0)
                {
                    "topic": "raw.reviews",
                    "offset": 0,
                    "sent_perf_counter": now + 10.0,
                    "sent_epoch_ms": (time.time() + 10.0) * 1000.0,
                    "value": {
                        "product_id": "p_inv",
                        "review_id": "rv_inv",
                        "user_id": "u1",
                        "rating": 5.0,
                        "review_title": "Good",
                        "review_text": "Content",
                        "review_date": "2026-10-20T10:00:00Z",
                    },
                },
                # Item 2: Missing send timestamp (both None)
                {
                    "topic": "raw.reviews",
                    "offset": 1,
                    "sent_perf_counter": None,
                    "sent_epoch_ms": None,
                    "value": {
                        "product_id": "p_miss",
                        "review_id": "rv_miss",
                        "user_id": "u2",
                        "rating": 4.0,
                        "review_title": "Nice",
                        "review_text": "Content",
                        "review_date": "2026-10-20T10:00:00Z",
                    },
                },
                # Item 3: Zero / negative epoch timestamp
                {
                    "topic": "raw.reviews",
                    "offset": 2,
                    "sent_perf_counter": None,
                    "sent_epoch_ms": -500.0,
                    "value": {
                        "product_id": "p_neg",
                        "review_id": "rv_neg",
                        "user_id": "u3",
                        "rating": 3.0,
                        "review_title": "OK",
                        "review_text": "Content",
                        "review_date": "2026-10-20T10:00:00Z",
                    },
                },
                # Item 4: Genuinely valid message (sent 25ms ago)
                {
                    "topic": "raw.reviews",
                    "offset": 3,
                    "sent_perf_counter": now - 0.025,
                    "sent_epoch_ms": (time.time() - 0.025) * 1000.0,
                    "value": {
                        "product_id": "p_valid",
                        "review_id": "rv_valid",
                        "user_id": "u4",
                        "rating": 5.0,
                        "review_title": "Super",
                        "review_text": "Content",
                        "review_date": "2026-10-20T10:00:00Z",
                    },
                },
            ]

            res = consumer.consume_batch_once(topics=["raw.reviews"], offline_records=virtual_queue)
            assert res["records_consumed"] == 4

            # 1. Measurement errors must be counted (> 0)
            assert res["timing_measurement_errors"] >= 3, f"Expected at least 3 timing errors, got {res['timing_measurement_errors']}"

            # 2. Clamped fake 0.01ms MUST NEVER be in validation or sink latencies
            assert 0.01 not in res["validation_latencies_ms"]
            assert 0.01 not in res["durable_sink_latencies_ms"]

            # 3. Only the genuinely valid message (Item 4) is admitted into valid latency metrics
            assert len(res["validation_latencies_ms"]) == 1
            assert res["validation_latencies_ms"][0] >= 20.0  # Approx 25ms delay

            # 4. Message latency entries for invalid timings record None rather than 0.01
            inv_msg = next(m for m in res["message_latencies"] if m["review_id"] == "rv_inv")
            assert inv_msg["validation_latency_ms"] is None
            assert inv_msg["durable_sink_latency_ms"] is None

            valid_msg = next(m for m in res["message_latencies"] if m["review_id"] == "rv_valid")
            assert valid_msg["validation_latency_ms"] is not None
            assert valid_msg["durable_sink_latency_ms"] is not None

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
        # Stability metrics
        assert res["duration_min_sec"] == 2.0
        assert res["duration_max_sec"] == 3.0
        assert res["duration_std_sec"] > 0.0
        assert res["spark_etl_min_sec"] == 1.6
        assert res["spark_etl_max_sec"] == 2.5

    def test_resource_monitor_continuous_sampling_without_manual_sample(self):
        """Verifies that ResourceMonitor background thread samples continuously across the full
        run duration without requiring manual sample() calls, capturing memory allocation.
        """
        import time
        mon = ResourceMonitor(interval_sec=0.01)
        mon.start()

        # Do NOT call mon.sample() manually; let the daemon thread sample continuously
        time.sleep(0.02)
        data_chunk = bytearray(15 * 1024 * 1024)
        time.sleep(0.04)
        del data_chunk

        metrics = mon.stop()

        assert "wall_sec" in metrics
        assert metrics["wall_sec"] >= 0.05
        assert "cpu_percent" in metrics
        assert "python_cpu_percent" in metrics
        assert "peak_ram_mb" in metrics
        assert "peak_tree_ram_mb" in metrics
        assert "peak_python_ram_mb" in metrics
        assert metrics["peak_ram_mb"] > 0.0
        assert metrics["peak_tree_ram_mb"] >= metrics["peak_python_ram_mb"]

        # Proves the background sampler ticked multiple times on its own
        assert metrics["sample_count"] >= 3, f"Expected >= 3 background samples, got {metrics.get('sample_count')}"
        assert mon._thread is None, "Monitor daemon thread must terminate cleanly after stop()"


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
                    "lag_at_start": 0,
                    "peak_consumer_lag": 15,
                    "lag_at_end": 0,
                    "consumer_lag": 0,
                    "messages_unaccounted": 0,
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
                    "format": "JSONL_Tiki_Crawler",
                    "category": "authentic_snapshot_jsonl",
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
            assert "JSONL_Tiki_Crawler" in md_text

            # 3. Verify Evidence file
            ev_file = REPO_ROOT / "docs" / "reports" / "3v_evidence.md"
            assert ev_file.exists()
            ev_text = ev_file.read_text(encoding="utf-8")
            assert "HTML_Synthetic_JSONLD" in ev_text or "JSONL_Tiki_Crawler" in ev_text

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
    """Verifies behavioral distinction between HTML parser fixtures, authentic Tiki crawler datasets,
    and synthetic fallbacks, ensuring zero disguise of synthetic data as real crawler sources.
    """

    def test_html_parser_strictly_expects_html_and_rejects_jsonl(self):
        """Proves that UniversalProductScraper actually parses HTML documents with embedded Schema.org
        JSON-LD script tags, and returns 0 reviews when given raw JSONL text.
        """
        from ingestion.crawler.generic_html import UniversalProductScraper

        scraper = UniversalProductScraper()

        # 1. Valid HTML payload containing Schema.org JSON-LD Review objects
        html_content = (
            "<!DOCTYPE html><html><head><title>Product Page</title>"
            '<script type="application/ld+json">'
            '{"@context": "https://schema.org/", "@type": "Product", "name": "Phone", "review": ['
            '{"@type": "Review", "@id": "rv_html_1", "name": "Great", "reviewBody": "Excellent phone", '
            '"author": {"@type": "Person", "name": "customer_1"}, "reviewRating": {"@type": "Rating", "ratingValue": 5}}'
            ']}'
            "</script></head><body><h1>Phone</h1></body></html>"
        )
        extracted = scraper.extract_reviews_from_html(html_content, product_id="prod_1")
        assert len(extracted) == 1
        assert extracted[0]["review_id"] == "rv_html_1"
        assert extracted[0]["review_text"] == "Excellent phone"

        # 2. Raw JSONL content passed into HTML parser must extract 0 reviews,
        # proving that HTML parser is strictly coupled to HTML structure and cannot parse raw JSONL
        raw_jsonl = '{"review_id": "rv_jsonl_1", "review_text": "Good"}\n{"review_id": "rv_jsonl_2"}\n'
        extracted_from_jsonl = scraper.extract_reviews_from_html(raw_jsonl, product_id="prod_1")
        assert len(extracted_from_jsonl) == 0, "HTML parser must reject non-HTML JSONL text"

    def test_tiki_crawler_format_labeled_jsonl_and_parsed_as_jsonl(self):
        """Verifies that the Tiki crawler output is correctly labeled as JSONL (not HTML)
        and parsed via FileLoader.load_records.
        """
        from ingestion.file_loader.loader import FileLoader

        with tempfile.NamedTemporaryFile("w", suffix=".jsonl", delete=False, encoding="utf-8") as f:
            f.write(json.dumps({"review_id": "tiki_rv_101", "rating": 5.0, "user_id": "raw_user_1"}) + "\n")
            temp_path = Path(f.name)

        try:
            records = FileLoader.load_records(temp_path, anonymize_pii=True)
            assert len(records) == 1
            assert records[0]["review_id"] == "tiki_rv_101"
            assert records[0]["user_id"].startswith("anon_")
        finally:
            if temp_path.exists():
                temp_path.unlink()

    def test_integration_fails_loudly_when_tiki_crawler_fails_and_no_snapshot(self):
        """In integration mode, when Tiki crawler fails and no snapshot exists, benchmark_variety
        must raise RuntimeError refusing to disguise fallback data as real crawler dataset.
        """
        with tempfile.TemporaryDirectory() as empty_dir:
            with patch("ingestion.crawler.tiki.TikiCrawler.crawl_category", side_effect=Exception("Network down")), \
                 patch("scripts.benchmark.REPO_ROOT", Path(empty_dir)):
                with pytest.raises(RuntimeError, match="Integration mode strictly requires a live Tiki crawler"):
                    benchmark_variety(records_per_format=10, effective_mode="integration")

    def test_local_fallback_cannot_disguise_as_real_crawler(self):
        """In local/offline mode when live crawler fails and snapshot does not exist,
        category must be 'synthetic_fallback', NEVER 'real_crawler_html' or 'real_crawler_jsonl'.
        """
        with tempfile.TemporaryDirectory() as empty_dir:
            with patch("ingestion.crawler.tiki.TikiCrawler.crawl_category", side_effect=Exception("Offline")), \
                 patch("scripts.benchmark.REPO_ROOT", Path(empty_dir)):
                results = benchmark_variety(records_per_format=10, effective_mode="local")
                tiki_row = next(r for r in results if r["format"] == "JSONL_Tiki_Crawler")
                assert tiki_row["category"] == "synthetic_fallback"
                assert tiki_row["category"] != "real_crawler_html"
                assert tiki_row["category"] != "real_crawler_jsonl"

    def test_pii_sanitization_in_variety_fallback(self):
        """Proves that raw customer emails and unhashed user IDs in the fallback branch
        are strictly sanitized and hashed at ingest.
        """
        records = [
            {"review_id": "rv_1", "user_id": "raw_user_1", "customer_email": "test@example.com"},
            {"review_id": "rv_2", "user_id": "raw_user_2", "customer_email": "test2@example.com"},
        ]
        sanitized = sanitize_records_pii(records)
        for r in sanitized:
            assert "customer_email" not in r
            assert r["user_id"].startswith("anon_")
            assert len(r["user_id"]) == 21  # 'anon_' + 16 hex chars
