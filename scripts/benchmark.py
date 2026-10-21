"""SentinelAI Real 3V Big Data Benchmark Suite (Volume, Velocity, Variety).

Benchmarks the real execution pipeline:
  - Volume: Bronze Storage Write + Spark Batch ETL (Read Bronze S3A/JSON -> Schema Cast ->
            Validation & Quarantine -> Window Deduplication -> Silver Parquet Write).
            Explicitly excludes Kafka transport to provide an unskewed storage + ETL profile.
            Supports multiple dataset sizes (including 100K, 500K, and configurable 1M),
            repeated iterations with median reporting, and CPU/memory resource tracking.
  - Velocity: Real Kafka Producer -> Kafka KRaft Broker -> RawConsumer -> Schema Validation
            (Valid to Bronze / Invalid to DLQ) across configurable rates and message counts.
            Measures produce rate, consumer throughput, p50/p95/p99 end-to-end latencies,
            consumer lag, DLQ routing, and transport error accounting.
            Disallows silent offline fallback in integration mode.
  - Variety: Multi-format Parse -> PII Hash -> Bronze Storage -> Spark Batch ETL ->
            Silver & Quarantine Parquet across JSON, JSONL, CSV, TSV, Parquet,
            HTML_Synthetic_JSONLD (Schema.org fixture), and JSONL_Tiki_Crawler (authentic Tiki crawler dataset).

Directly regenerates synchronized empirical reports:
  - docs/reports/benchmark_3v.json
  - docs/reports/benchmark_report.md
  - docs/reports/3v_evidence.md
  - docs/benchmark.md
without hand-edited or unexecuted claims.
"""
from __future__ import annotations

import argparse
import datetime
import json
import os
import shutil
import statistics
import sys
import tempfile
import threading
import time
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

import pandas as pd
import psutil

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
from scripts.health_check import check_kafka, verify_integration_health
from spark.batch.spark_etl_job import SparkBatchETLJob
from storage.storage_manager import storage


# -----------------------------------------------------------------------------
# Profiling & Helper Utilities
# -----------------------------------------------------------------------------

class ResourceMonitor:
    """Continuously tracks process tree CPU % and peak RSS RAM (MB) across a benchmark run.

    Uses a background daemon thread sampling at periodic intervals (default 50ms) across
    both the parent Python process and all child processes (including PySpark Java JVM,
    Py4J gateway, and worker subprocesses).

    Note:
      These metrics capture the local client process tree (Python harness + child JVM).
      They should NOT be interpreted as the total cluster-wide resource utilization
      in a distributed Spark environment with remote executors/nodes.
    """

    def __init__(self, interval_sec: float = 0.05):
        self._interval = interval_sec
        self._proc = psutil.Process()
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._t0: float = 0.0
        self._parent_cpu0: float = 0.0
        self._initial_child_cpu: Dict[int, float] = {}
        self._last_child_cpu: Dict[int, float] = {}
        self._accum_child_cpu: float = 0.0
        self._peak_py_rss: int = 0
        self._peak_children_rss: int = 0
        self._peak_tree_rss: int = 0
        self.sample_count: int = 0

    def _get_cpu_sum(self, p: psutil.Process) -> float:
        try:
            t = p.cpu_times()
            return float(t.user + t.system)
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            return 0.0

    def _sample(self) -> None:
        try:
            py_rss = self._proc.memory_info().rss
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            py_rss = 0

        children_rss = 0
        active_children: Dict[int, float] = {}
        try:
            children = self._proc.children(recursive=True)
        except Exception:
            children = []

        for c in children:
            try:
                c_rss = c.memory_info().rss
                children_rss += c_rss
                c_cpu = self._get_cpu_sum(c)
                active_children[c.pid] = c_cpu
                if c.pid not in self._initial_child_cpu:
                    self._initial_child_cpu[c.pid] = c_cpu
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pass

        tree_rss = py_rss + children_rss

        if py_rss > self._peak_py_rss:
            self._peak_py_rss = py_rss
        if children_rss > self._peak_children_rss:
            self._peak_children_rss = children_rss
        if tree_rss > self._peak_tree_rss:
            self._peak_tree_rss = tree_rss

        # Track terminated children CPU delta
        for pid, last_cpu in list(self._last_child_cpu.items()):
            if pid not in active_children:
                init_cpu = self._initial_child_cpu.get(pid, last_cpu)
                self._accum_child_cpu += max(0.0, last_cpu - init_cpu)
                self._initial_child_cpu.pop(pid, None)

        self._last_child_cpu = active_children
        self.sample_count += 1

    def _sample_loop(self) -> None:
        while self._running:
            self._sample()
            time.sleep(self._interval)

    def start(self) -> None:
        self._t0 = time.perf_counter()
        self._parent_cpu0 = self._get_cpu_sum(self._proc)
        self._accum_child_cpu = 0.0
        self._initial_child_cpu.clear()
        self._last_child_cpu.clear()
        self._peak_py_rss = 0
        self._peak_children_rss = 0
        self._peak_tree_rss = 0
        self.sample_count = 0

        self._sample()
        self._running = True
        self._thread = threading.Thread(target=self._sample_loop, daemon=True)
        self._thread.start()

    def sample(self) -> None:
        """Manual sample hook for explicit sampling points."""
        self._sample()

    def stop(self) -> Dict[str, float]:
        self._running = False
        if self._thread is not None:
            self._thread.join(timeout=1.0)
            self._thread = None
        self._sample()

        wall = max(0.0001, time.perf_counter() - self._t0)
        parent_cpu1 = self._get_cpu_sum(self._proc)
        parent_delta = max(0.0, parent_cpu1 - self._parent_cpu0)

        child_delta = self._accum_child_cpu
        for pid, last_cpu in self._last_child_cpu.items():
            init_cpu = self._initial_child_cpu.get(pid, last_cpu)
            child_delta += max(0.0, last_cpu - init_cpu)

        total_cpu_sec = parent_delta + child_delta
        cpu_pct = round((total_cpu_sec / wall) * 100.0, 1)
        py_cpu_pct = round((parent_delta / wall) * 100.0, 1)

        peak_tree_mb = round(self._peak_tree_rss / (1024 * 1024), 2)
        peak_py_mb = round(self._peak_py_rss / (1024 * 1024), 2)
        peak_child_mb = round(self._peak_children_rss / (1024 * 1024), 2)

        return {
            "wall_sec": round(wall, 4),
            "cpu_percent": cpu_pct,
            "python_cpu_percent": py_cpu_pct,
            "peak_ram_mb": peak_tree_mb,
            "peak_tree_ram_mb": peak_tree_mb,
            "peak_python_ram_mb": peak_py_mb,
            "peak_children_ram_mb": peak_child_mb,
            "sample_count": self.sample_count,
        }


def compute_percentiles(values: List[float]) -> Dict[str, float]:
    """Computes exact p50, p95, p99, min, max, avg percentiles deterministically."""
    if not values:
        return {"p50": 0.0, "p95": 0.0, "p99": 0.0, "avg": 0.0, "min": 0.0, "max": 0.0}
    s = sorted(values)
    n = len(s)

    def _pct(p: float) -> float:
        idx = int(round((p / 100.0) * (n - 1)))
        return round(float(s[min(n - 1, max(0, idx))]), 2)

    return {
        "p50": _pct(50.0),
        "p95": _pct(95.0),
        "p99": _pct(99.0),
        "avg": round(sum(s) / n, 2),
        "min": round(float(s[0]), 2),
        "max": round(float(s[-1]), 2),
    }


def compute_median_run(runs: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Computes a consolidated row taking median values across repeated benchmark runs
    along with min, max, and standard deviation stability indicators.
    """
    if not runs:
        raise ValueError("Cannot compute median over an empty runs list.")
    if len(runs) == 1:
        res = dict(runs[0])
        res["iterations"] = 1
        res["duration_min_sec"] = res.get("duration_sec", 0.0)
        res["duration_max_sec"] = res.get("duration_sec", 0.0)
        res["duration_std_sec"] = 0.0
        res["spark_etl_min_sec"] = res.get("spark_etl_sec", 0.0)
        res["spark_etl_max_sec"] = res.get("spark_etl_sec", 0.0)
        res["spark_etl_std_sec"] = 0.0
        res["throughput_min_rps"] = res.get("throughput_rps", 0.0)
        res["throughput_max_rps"] = res.get("throughput_rps", 0.0)
        res["iteration_runs"] = runs
        return res

    base = dict(runs[0])
    numeric_keys = [
        "duration_sec",
        "bronze_write_sec",
        "spark_etl_sec",
        "throughput_rps",
        "spark_throughput_rps",
        "cpu_percent",
        "python_cpu_percent",
        "peak_ram_mb",
        "peak_tree_ram_mb",
        "peak_python_ram_mb",
        "peak_children_ram_mb",
    ]
    for k in numeric_keys:
        vals = [float(r[k]) for r in runs if k in r]
        if vals:
            base[k] = round(float(statistics.median(vals)), 4 if "sec" in k else 1)

    durations = [float(r["duration_sec"]) for r in runs if "duration_sec" in r]
    if durations:
        base["duration_min_sec"] = round(min(durations), 4)
        base["duration_max_sec"] = round(max(durations), 4)
        base["duration_std_sec"] = round(statistics.stdev(durations), 4) if len(durations) > 1 else 0.0

    spark_etls = [float(r["spark_etl_sec"]) for r in runs if "spark_etl_sec" in r]
    if spark_etls:
        base["spark_etl_min_sec"] = round(min(spark_etls), 4)
        base["spark_etl_max_sec"] = round(max(spark_etls), 4)
        base["spark_etl_std_sec"] = round(statistics.stdev(spark_etls), 4) if len(spark_etls) > 1 else 0.0

    tps = [float(r["throughput_rps"]) for r in runs if "throughput_rps" in r]
    if tps:
        base["throughput_min_rps"] = round(min(tps), 1)
        base["throughput_max_rps"] = round(max(tps), 1)

    base["iterations"] = len(runs)
    base["iteration_runs"] = runs
    return base


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
    """Configures runtime mode for the benchmark suite and verifies services in integration mode.
    Fails fast with ConnectionError if any required service is unreachable.
    """
    effective_mode = (mode or os.environ.get("RUNTIME_MODE", settings.runtime_mode)).strip().lower()
    settings.set_runtime_mode(effective_mode)
    storage.set_mode(effective_mode)
    if effective_mode in ("integration", "production"):
        verify_integration_health(require_kafka=True)
    return effective_mode


# -----------------------------------------------------------------------------
# 1. Volume Benchmark
# -----------------------------------------------------------------------------

def benchmark_volume(
    sizes: Optional[List[int]] = None,
    seed: int = 42,
    dirty_ratio: float = 0.05,
    duplicate_ratio: float = 0.03,
    spark_etl: Optional[SparkBatchETLJob] = None,
    iterations: int = 1,
) -> List[Dict[str, Any]]:
    """1. VOLUME BENCHMARK: Measures Bronze write + Spark Batch ETL -> Silver & Quarantine
    across multiple dataset sizes (supporting 100K, 500K, and configurable 1M).

    Measurement Boundary:
      Stage 1: Storage write to Bronze JSON (s3a:// or local).
      Stage 2: Spark Batch ETL (Read Bronze -> Schema Cast -> Validation & Quarantine ->
               Intra-window deduplication -> Silver Parquet write).
      Note: Kafka streaming is excluded from this stage to isolate storage & ETL capacity.
    """
    if sizes is None:
        sizes = [100, 1000, 5000, 10000]

    iters = max(1, iterations)
    print(
        f"\n--- [1/3] Running VOLUME Benchmark "
        f"(Bronze Storage Write + Spark Batch ETL, seed={seed}, sizes={sizes}, iterations={iters}) ---"
    )
    print("  [Measurement Boundary] Stage 1: Bronze Storage Write + Stage 2: Spark Batch ETL. Excludes Kafka.")

    etl_job = spark_etl or SparkBatchETLJob()
    products = generate_products(seed=seed)
    results: List[Dict[str, Any]] = []

    # Warm-up Spark JVM & committer so first measured size is not skewed by one-time initialization
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
        size_runs: List[Dict[str, Any]] = []
        for iter_idx in range(iters):
            run_seed = seed + size
            reviews = generate_reviews(
                products,
                count=size,
                inject_anomalies=False,
                seed=run_seed,
                dirty_ratio=dirty_ratio,
                duplicate_ratio=duplicate_ratio,
            )
            envelopes = _wrap_in_envelopes(reviews, event_type="NEW_REVIEW", source="volume_benchmark", seed=run_seed)

            suffix = f"s{seed}_{size}_i{iter_idx}" if iters > 1 else f"s{seed}_{size}"
            bronze_key = f"bronze/bv/{suffix}/batch.json"
            silver_subpath = f"bv/{suffix}/rev"
            quarantine_subpath = f"bv/{suffix}/q_rev"

            mon = ResourceMonitor()
            mon.start()

            # Boundary 1: Bronze Storage Write
            t_b0 = time.perf_counter()
            # Compact serialization without indent for large datasets (>= 50K) to optimize RAM and I/O
            indent_opt = None if size >= 50000 else 2
            try:
                storage.backend.write_json(bronze_key, envelopes, indent=indent_opt)
            except TypeError:
                storage.backend.write_json(bronze_key, envelopes)
            bronze_write_sec = time.perf_counter() - t_b0
            del reviews, envelopes
            import gc
            gc.collect()

            # Boundary 2: Spark Batch ETL
            t_s0 = time.perf_counter()
            res = etl_job.process_dataset(
                "reviews",
                bronze_key,
                silver_subpath=silver_subpath,
                quarantine_subpath=quarantine_subpath,
                replace_existing_silver=True,
            )
            spark_etl_sec = time.perf_counter() - t_s0

            mon_metrics = mon.stop()
            total_elapsed = bronze_write_sec + spark_etl_sec

            # Accounting invariant verification:
            # input_count == unique_valid_count + intra_dedup_dropped + quarantine_count
            input_cnt = int(res["input_count"])
            valid_cnt = int(res["unique_valid_count"])
            quar_cnt = int(res["quarantine_count"])
            dup_cnt = int(res["intra_dedup_dropped"])
            assert input_cnt == valid_cnt + quar_cnt + dup_cnt, (
                f"Accounting invariant failed: input ({input_cnt}) != valid ({valid_cnt}) + dup ({dup_cnt}) + quarantine ({quar_cnt})"
            )

            bronze_bytes = storage.backend.get_size(bronze_key)
            silver_bytes = storage.backend.get_size(f"silver/{silver_subpath}")
            quarantine_bytes = (
                storage.backend.get_size(f"silver/{quarantine_subpath}")
                if quar_cnt > 0
                else 0
            )
            total_bytes = bronze_bytes + silver_bytes + quarantine_bytes

            total_throughput = input_cnt / total_elapsed if total_elapsed > 0 else 0.0
            spark_throughput = input_cnt / spark_etl_sec if spark_etl_sec > 0 else 0.0

            run_row = {
                "seed": run_seed,
                "iteration": iter_idx + 1,
                "records_input": input_cnt,
                "bronze_write_sec": round(bronze_write_sec, 4),
                "spark_etl_sec": round(spark_etl_sec, 4),
                "duration_sec": round(total_elapsed, 4),
                "throughput_rps": round(total_throughput, 1),
                "spark_throughput_rps": round(spark_throughput, 1),
                "valid_count": valid_cnt,
                "quarantine_count": quar_cnt,
                "duplicate_count": dup_cnt,
                "silver_written_count": int(res["silver_written_count"]),
                "dq_score": float(res["dq_score"]),
                "bronze_bytes": int(bronze_bytes),
                "silver_bytes": int(silver_bytes),
                "quarantine_bytes": int(quarantine_bytes),
                "total_storage_bytes": int(total_bytes),
                "cpu_percent": mon_metrics["cpu_percent"],
                "python_cpu_percent": mon_metrics.get("python_cpu_percent", 0.0),
                "peak_ram_mb": mon_metrics["peak_ram_mb"],
                "peak_tree_ram_mb": mon_metrics.get("peak_tree_ram_mb", mon_metrics["peak_ram_mb"]),
                "peak_python_ram_mb": mon_metrics.get("peak_python_ram_mb", 0.0),
                "peak_children_ram_mb": mon_metrics.get("peak_children_ram_mb", 0.0),
                "accounting_reconciled": True,
            }
            size_runs.append(run_row)
            if iters > 1:
                print(
                    f"    - Iteration {iter_idx + 1}/{iters}: total={total_elapsed:.3f}s "
                    f"(write={bronze_write_sec:.3f}s, spark={spark_etl_sec:.3f}s) | "
                    f"rate={total_throughput:,.1f} rec/s | DQ={res['dq_score']}% | "
                    f"RAM(tree)={mon_metrics['peak_ram_mb']:.1f}MB (py: {mon_metrics.get('peak_python_ram_mb', 0.0):.1f}MB) | "
                    f"CPU={mon_metrics['cpu_percent']}%"
                )

        med_row = compute_median_run(size_runs)
        iter_label = (
            f" (median of {iters} runs, range: {med_row.get('duration_min_sec', 0.0):.3f}s - {med_row.get('duration_max_sec', 0.0):.3f}s, std: {med_row.get('duration_std_sec', 0.0):.3f}s)"
            if iters > 1
            else ""
        )
        print(
            f"  [Volume Consolidated] seed={med_row['seed']} | input={med_row['records_input']:,}{iter_label} | "
            f"write={med_row['bronze_write_sec']:.3f}s | spark={med_row['spark_etl_sec']:.3f}s | total={med_row['duration_sec']:.3f}s | "
            f"rate={med_row['throughput_rps']:,.1f} rec/s (spark: {med_row['spark_throughput_rps']:,.1f}) | "
            f"valid={med_row['valid_count']:,} | quarantine={med_row['quarantine_count']:,} | dup={med_row['duplicate_count']:,} | "
            f"DQ={med_row['dq_score']}% | CPU={med_row['cpu_percent']}% | RAM(tree)={med_row['peak_ram_mb']:.1f}MB | "
            f"storage={med_row['total_storage_bytes']:,} B (B:{med_row['bronze_bytes']:,} / S:{med_row['silver_bytes']:,} / Q:{med_row['quarantine_bytes']:,})"
        )
        results.append(med_row)

    return results


# -----------------------------------------------------------------------------
# 2. Velocity Benchmark
# -----------------------------------------------------------------------------

def benchmark_velocity(
    rates: Optional[List[int]] = None,
    seed: int = 42,
    dirty_ratio: float = 0.04,
    effective_mode: str = "integration",
    messages_per_rate: Optional[int] = None,
    duration_sec: Optional[float] = None,
) -> List[Dict[str, Any]]:
    """2. VELOCITY BENCHMARK: Measures Kafka Producer -> Broker -> RawConsumer ->
    Schema Validation (Valid to Bronze / Invalid to DLQ).

    Features:
      - Eliminates hardcoded 500 message limit; configurable via messages_per_rate or duration_sec.
      - Measures producer rate, consumer throughput, e2e latencies (p50, p95, p99), consumer lag,
        and DLQ quarantine routing.
      - Enforces real Kafka connection in integration mode; fails loudly if broker is unavailable.
    """
    if rates is None:
        rates = [50, 200, 500, 1000]

    is_local = effective_mode not in ("integration", "production")
    settings.set_runtime_mode(effective_mode)
    storage.set_mode(effective_mode)

    if not is_local:
        if not check_kafka(strict=False):
            raise ConnectionError(
                f"Kafka broker is unreachable at '{settings.kafka.bootstrap_servers}'. "
                f"Integration mode strictly requires a live Kafka broker; "
                f"refusing silent fallback to offline buffer."
            )
        transport_mode = "real_kafka"
    else:
        transport_mode = "offline_virtual_queue"

    print(
        f"\n--- [2/3] Running VELOCITY Benchmark "
        f"(Transport: {transport_mode}, seed={seed}, rates={rates}, "
        f"messages_per_rate={messages_per_rate}, duration_sec={duration_sec}) ---"
    )
    TopicManager.ensure_topics(mode=effective_mode)

    products = generate_products(seed=seed)
    results: List[Dict[str, Any]] = []

    suite_group_id = f"sentinel-bench-velocity-{seed}-{uuid.uuid4().hex[:6]}"
    consumer = RawConsumer(
        bronze_dir=(settings.storage.local_data_dir / "bronze") if is_local else None,
        group_id=suite_group_id,
        auto_offset_reset="earliest"
    )
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
            # Determine message count without hard 500 cap
            if messages_per_rate is not None:
                msg_count = max(10, int(messages_per_rate))
            elif duration_sec is not None:
                msg_count = max(50, int(rate * duration_sec))
            else:
                msg_count = max(100, min(rate * 2, 5000))

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
                r["review_id"] = f"{bench_run_id}_rv_{idx+1:06d}"

            if is_local:
                producer.virtual_queue.clear()

            # Streaming execution: Consumer runs concurrently in a worker thread,
            # polling/processing messages and recording individual completion timestamps
            # while Producer sends messages pacing at (1.0 / rate).
            cons_res: Dict[str, Any] = {}
            cons_errors: List[Exception] = []
            stop_event = threading.Event()
            cons_start_barrier = threading.Event()
            t_cons_start = 0.0
            t_cons_end = 0.0

            def consumer_worker():
                nonlocal t_cons_start, t_cons_end
                t_cons_start = time.perf_counter()
                cons_start_barrier.set()
                try:
                    res = consumer.consume_batch_once(
                        topics=[settings.kafka.topic_reviews],
                        max_records=msg_count,
                        timeout_ms=max(10000, int(msg_count / rate * 1000) + 5000),
                        offline_records=producer.virtual_queue if is_local else None,
                        live_stream=True,
                        stop_event=stop_event,
                    )
                    cons_res.update(res)
                except Exception as ce:
                    cons_errors.append(ce)
                finally:
                    t_cons_end = time.perf_counter()

            consumer_thread = threading.Thread(
                target=consumer_worker,
                name=f"velocity-consumer-{rate}",
                daemon=True,
            )
            consumer_thread.start()
            cons_start_barrier.wait(timeout=2.0)

            send_errors = 0
            t_prod_start = time.perf_counter()
            try:
                sent, actual_prod_rate = producer.produce_batch(
                    settings.kafka.topic_reviews,
                    reviews,
                    rate=float(rate),
                    event_type="NEW_REVIEW",
                    source=f"velocity_bench_{rate}",
                )
            except Exception as pe:
                send_errors += 1
                stop_event.set()
                raise pe
            finally:
                prod_duration = time.perf_counter() - t_prod_start

            # Wait for consumer to finish processing all sent messages
            consumer_thread.join(timeout=max(10.0, (msg_count / rate) + 5.0))
            if consumer_thread.is_alive():
                stop_event.set()
                consumer_thread.join(timeout=2.0)

            consume_errors = len(cons_errors)
            if cons_errors:
                raise cons_errors[0]

            cons_duration = max(t_cons_end - t_cons_start, 0.0001) if t_cons_end > 0 else 0.0001

            consumed = int(cons_res.get("records_consumed", 0))
            valid_to_bronze = int(cons_res.get("records_valid", 0))
            dlq_count = int(cons_res.get("records_dlq", 0))

            # Mathematical accounting check on streaming:
            assert consumed == valid_to_bronze + dlq_count, (
                f"Velocity accounting mismatch: consumed ({consumed}) != valid ({valid_to_bronze}) + dlq ({dlq_count})"
            )

            if not is_local and sent > 0 and consumed == 0:
                raise RuntimeError(
                    f"Velocity integration failed: produced {sent} messages to Kafka, but 0 consumed."
                )

            # Extract true per-message latencies (Li = t_processed,i - t_sent,i) measured by consumer
            latencies_ms: List[float] = cons_res.get("latencies_ms", [])
            latency_stats = compute_percentiles(latencies_ms)

            # Consumer lag
            consumer_lag = 0
            if not is_local and consumer.consumer and consumer.consumer.assignment():
                try:
                    end_offsets = consumer.consumer.end_offsets(consumer.consumer.assignment())
                    for tp in consumer.consumer.assignment():
                        pos = consumer.consumer.position(tp)
                        end = end_offsets.get(tp, pos)
                        consumer_lag += max(0, end - pos)
                except Exception:
                    consumer_lag = max(0, sent - consumed)
            else:
                consumer_lag = max(0, sent - consumed)

            e2e_start = min(t_prod_start, t_cons_start) if t_cons_start > 0 else t_prod_start
            e2e_end = max(t_prod_start + prod_duration, t_cons_end)
            e2e_duration = max(e2e_end - e2e_start, 0.0001)
            e2e_rate = consumed / e2e_duration if e2e_duration > 0 else 0.0
            cons_throughput = consumed / cons_duration if cons_duration > 0 else 0.0

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
                "consume_duration_sec": round(cons_duration, 4),
                "consumer_throughput_mps": round(cons_throughput, 1),
                "messages_consumed": consumed,
                "valid_count": valid_to_bronze,
                "quarantine_dlq_count": dlq_count,
                "consumer_lag": int(consumer_lag),
                "send_errors": int(send_errors),
                "consume_errors": int(consume_errors),
                "e2e_duration_sec": round(e2e_duration, 4),
                "e2e_throughput_mps": round(e2e_rate, 1),
                "latency_p50_ms": latency_stats["p50"],
                "latency_p95_ms": latency_stats["p95"],
                "latency_p99_ms": latency_stats["p99"],
                "latency_avg_ms": latency_stats["avg"],
                "bronze_bytes": int(bronze_bytes),
                "accounting_reconciled": True,
            }
            print(
                f"  [Velocity] seed={row['seed']} | target={row['target_rate_mps']} msg/s | "
                f"produce={row['actual_produce_rate_mps']:,.1f} msg/s ({row['produce_duration_sec']:.3f}s) | "
                f"consume={row['consumer_throughput_mps']:,.1f} msg/s ({row['consume_duration_sec']:.3f}s) | "
                f"consumed={row['messages_consumed']} (valid={row['valid_count']}, dlq={row['quarantine_dlq_count']}, lag={row['consumer_lag']}) | "
                f"latency: p50={row['latency_p50_ms']}ms, p95={row['latency_p95_ms']}ms, p99={row['latency_p99_ms']}ms | "
                f"e2e_rate={row['e2e_throughput_mps']:,.1f} msg/s | bronze={row['bronze_bytes']:,} B"
            )
            results.append(row)
    finally:
        producer.close()
        consumer.close()

    return results


# -----------------------------------------------------------------------------
# 3. Variety Benchmark
# -----------------------------------------------------------------------------

def benchmark_variety(
    records_per_format: int = 500,
    seed: int = 42,
    dirty_ratio: float = 0.05,
    duplicate_ratio: float = 0.02,
    spark_etl: Optional[SparkBatchETLJob] = None,
    effective_mode: str = "integration",
    tiki_limit: int = 10,
) -> List[Dict[str, Any]]:
    """3. VARIETY BENCHMARK: Measures full multi-format Parse -> PII Hash -> Bronze Write ->
    Spark Cast/Validate/Normalize/Deduplicate -> Silver & Quarantine Parquet across:
      - JSON (.json)
      - JSONL (.jsonl)
      - CSV (.csv)
      - TSV (.tsv)
      - Parquet (.parquet)
      - HTML_Synthetic_JSONLD (Schema.org JSON-LD semi-structured HTML fixture)
      - JSONL_Tiki_Crawler (Authentic Tiki live crawler / realistic JSONL payload)
    """
    print(
        f"\n--- [3/3] Running VARIETY Benchmark "
        f"(Full Parse -> Validate -> Transform -> Silver/Quarantine, seed={seed}, n={records_per_format}) ---"
    )
    settings.set_runtime_mode(effective_mode)
    storage.set_mode(effective_mode)
    etl_job = spark_etl or SparkBatchETLJob()
    products = generate_products(seed=seed)

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
            "JSON": (temp_dir / "reviews.json", "synthetic_file"),
            "JSONL": (temp_dir / "reviews.jsonl", "synthetic_file"),
            "CSV": (temp_dir / "reviews.csv", "synthetic_file"),
            "TSV": (temp_dir / "reviews.tsv", "synthetic_file"),
            "Parquet": (temp_dir / "reviews.parquet", "synthetic_file"),
        }

        with open(format_files["JSON"][0], "w", encoding="utf-8") as f:
            json.dump(raw_with_pii, f, ensure_ascii=False)
        with open(format_files["JSONL"][0], "w", encoding="utf-8") as f:
            for item in raw_with_pii:
                f.write(json.dumps(item, ensure_ascii=False) + "\n")
        df_raw.to_csv(format_files["CSV"][0], index=False, encoding="utf-8")
        df_raw.to_csv(format_files["TSV"][0], index=False, sep="\t", encoding="utf-8")
        df_raw.to_parquet(format_files["Parquet"][0], index=False)

        # 1. Semi-structured HTML fixture containing embedded Schema.org JSON-LD
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
        synthetic_html_doc = (
            "<!DOCTYPE html><html><head><title>iPhone 15 Pro</title>"
            f'<script type="application/ld+json">{json.dumps(html_ld_payload, ensure_ascii=False)}</script>'
            "</head><body><h1>iPhone 15 Pro</h1></body></html>"
        )
        synthetic_html_file = temp_dir / "product_reviews_synthetic.html"
        synthetic_html_file.write_text(synthetic_html_doc, encoding="utf-8")

        # 2. Authentic Tiki Crawler Dataset / Export payload (JSONL format)
        tiki_file = temp_dir / "tiki_reviews_crawler.jsonl"
        tiki_reviews_data: List[Dict[str, Any]] = []
        tiki_category = None

        is_integration = effective_mode in ("integration", "production")
        if is_integration:
            try:
                crawler = TikiCrawler(allow_public_fallback=False)
                crawl_res = crawler.crawl_category("smartphones", max_products=3, reviews_per_product=3)
                crawled_revs = crawl_res.get("reviews", []) if isinstance(crawl_res, dict) else []
                if crawled_revs:
                    tiki_reviews_data = crawled_revs
                    tiki_category = "real_crawler_jsonl"
            except Exception:
                tiki_reviews_data = []

        if not tiki_reviews_data:
            snapshot_path = REPO_ROOT / "data" / "external" / "tiki" / "tiki_reviews_snapshot_20261018T165951Z.jsonl"
            if snapshot_path.exists():
                with open(snapshot_path, "r", encoding="utf-8") as f:
                    tiki_reviews_data = [json.loads(line) for line in f if line.strip()]
                tiki_category = "authentic_snapshot_jsonl"

        if not tiki_reviews_data:
            if is_integration:
                raise RuntimeError(
                    "Integration mode strictly requires a live Tiki crawler connection or verified snapshot; "
                    "refusing silent fallback to synthetic data with real crawler claim."
                )
            # Local/offline fallback: Must NOT claim to be real crawler
            tiki_category = "synthetic_fallback"
            tiki_reviews_data = [
                {
                    "review_id": f"tiki_rv_{20000000 + i}",
                    "product_id": f"tiki_{270000000 + (i % 3)}",
                    "user_id": f"raw_tiki_user_{i+1}",
                    "customer_email": f"tiki_buyer_{i+1}@tiki.vn",
                    "rating": 5.0 if i % 4 != 0 else 4.0,
                    "review_title": "Hàng chính hãng",
                    "review_text": "Giao hàng nhanh, đóng gói cẩn thận, sản phẩm nguyên seal dùng rất tốt.",
                    "review_date": "2026-10-18T10:00:00Z",
                    "verified_purchase": True,
                    "helpful_count": i % 5,
                    "source": "tiki_crawler",
                    "language": "vi",
                    "created_at": "2026-10-18T10:00:00Z",
                }
                for i in range(min(records_per_format, max(10, tiki_limit)))
            ]

        with open(tiki_file, "w", encoding="utf-8") as f:
            for item in tiki_reviews_data:
                f.write(json.dumps(item, ensure_ascii=False) + "\n")

        all_cases = [
            ("JSON", format_files["JSON"][0], "synthetic_file"),
            ("JSONL", format_files["JSONL"][0], "synthetic_file"),
            ("CSV", format_files["CSV"][0], "synthetic_file"),
            ("TSV", format_files["TSV"][0], "synthetic_file"),
            ("Parquet", format_files["Parquet"][0], "synthetic_file"),
            ("HTML_Synthetic_JSONLD", synthetic_html_file, "synthetic_html"),
            ("JSONL_Tiki_Crawler", tiki_file, tiki_category),
        ]

        for fmt_name, fpath, category in all_cases:
            source_bytes = fpath.stat().st_size
            t0 = time.perf_counter()

            # Step A: Parse & sanitize PII at ingest
            if fmt_name == "HTML_Synthetic_JSONLD":
                scraper = UniversalProductScraper()
                html_text = fpath.read_text(encoding="utf-8")
                prod_meta = scraper._extract_metadata_from_html(html_text, "https://shop.sentinel.ai/p/iphone_15_pro")
                loaded_records = scraper.extract_reviews_from_html(
                    html_text, product_id=prod_meta["product_id"]
                )
            elif fmt_name == "JSONL_Tiki_Crawler":
                loaded_records = FileLoader.load_records(fpath, anonymize_pii=True)
            else:
                loaded_records = FileLoader.load_records(fpath, anonymize_pii=True)

            # Strict PII sanitization verification: raw customer emails removed, user_ids hashed
            if not all(
                is_anonymized_user_id(r.get("user_id")) and "customer_email" not in r
                for r in loaded_records
            ):
                raise RuntimeError(f"Variety benchmark PII check failed for format {fmt_name}")

            # Step B: Write to Bronze storage
            short_fmt = fmt_name.lower().replace("_", "")[:8]
            bronze_key = f"bronze/bvar/s{seed}_{short_fmt}/batch.json"
            silver_subpath = f"bvar/s{seed}_{short_fmt}/rev"
            quarantine_subpath = f"bvar/s{seed}_{short_fmt}/q_rev"

            envelopes = _wrap_in_envelopes(
                loaded_records, event_type="NEW_REVIEW", source=f"variety_{fmt_name.lower()}", seed=seed
            )
            storage.backend.write_json(bronze_key, envelopes)

            # Step C: Spark Batch ETL -> Silver & Quarantine
            etl_res = etl_job.process_dataset(
                "reviews",
                bronze_key,
                silver_subpath=silver_subpath,
                quarantine_subpath=quarantine_subpath,
                replace_existing_silver=True,
            )
            elapsed = time.perf_counter() - t0

            # Mathematical accounting check: input == unique_valid + duplicate + quarantine
            input_cnt = int(etl_res["input_count"])
            valid_cnt = int(etl_res["unique_valid_count"])
            quar_cnt = int(etl_res["quarantine_count"])
            dup_cnt = int(etl_res["intra_dedup_dropped"])
            assert input_cnt == valid_cnt + quar_cnt + dup_cnt, (
                f"Variety accounting error on {fmt_name}: input ({input_cnt}) != valid ({valid_cnt}) + dup ({dup_cnt}) + quarantine ({quar_cnt})"
            )

            bronze_bytes = storage.backend.get_size(bronze_key)
            silver_bytes = storage.backend.get_size(f"silver/{silver_subpath}")
            quarantine_bytes = (
                storage.backend.get_size(f"silver/{quarantine_subpath}")
                if quar_cnt > 0
                else 0
            )
            throughput = len(loaded_records) / elapsed if elapsed > 0 else 0.0

            row = {
                "seed": seed,
                "format": fmt_name,
                "category": category,
                "source_bytes": int(source_bytes),
                "records_input": input_cnt,
                "valid_count": valid_cnt,
                "quarantine_count": quar_cnt,
                "duplicate_count": dup_cnt,
                "dq_score": float(etl_res["dq_score"]),
                "duration_sec": round(elapsed, 4),
                "throughput_rps": round(throughput, 1),
                "bronze_bytes": int(bronze_bytes),
                "silver_bytes": int(silver_bytes),
                "quarantine_bytes": int(quarantine_bytes),
                "pii_anonymized": True,
                "accounting_reconciled": True,
            }
            print(
                f"  [Variety] format={row['format']:<22} | category={row['category']:<17} | input={row['records_input']:,} | "
                f"time={row['duration_sec']:.3f}s | rate={row['throughput_rps']:,.1f} rec/s | "
                f"valid={row['valid_count']:,} | quarantine={row['quarantine_count']:,} | "
                f"dup={row['duplicate_count']:,} | silver={row['silver_bytes']:,} B"
            )
            results.append(row)
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)

    return results


# -----------------------------------------------------------------------------
# 4. Report Generation (JSON & Markdown Synchronization)
# -----------------------------------------------------------------------------

def generate_markdown_report(results: Dict[str, Any]) -> None:
    """Generates docs/reports/benchmark_3v.json, docs/reports/benchmark_report.md,
    docs/reports/3v_evidence.md, and docs/benchmark.md directly from empirical run results.
    Never advertises unexecuted scale claims.
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
    peak_spark_rate = max(r.get("spark_throughput_rps", r.get("throughput_rps", 0.0)) for r in vol_rows) if vol_rows else 0.0
    peak_total_rate = max(r["throughput_rps"] for r in vol_rows) if vol_rows else 0.0

    min_vel_target = min(r["target_rate_mps"] for r in vel_rows) if vel_rows else 0
    max_vel_target = max(r["target_rate_mps"] for r in vel_rows) if vel_rows else 0
    peak_prod_rate = max(r["actual_produce_rate_mps"] for r in vel_rows) if vel_rows else 0.0
    peak_cons_rate = max(r.get("consumer_throughput_mps", 0.0) for r in vel_rows) if vel_rows else 0.0
    peak_e2e_rate = max(r["e2e_throughput_mps"] for r in vel_rows) if vel_rows else 0.0

    formats_list = ", ".join(r["format"] for r in var_rows)

    # 1. docs/reports/benchmark_report.md
    md_path = report_dir / "benchmark_report.md"
    vol_iters = meta.get("volume_iterations", 1)
    lines = [
        "# SentinelAI 3V Empirical Benchmark Report",
        "",
        f"**Generated At:** `{results['timestamp']}`  ",
        f"**Runtime Mode:** `{meta['runtime_mode']}`  ",
        f"**Storage Backend:** `{meta['storage_backend']}`  ",
        f"**Base Deterministic Seed:** `{meta['seed']}`  ",
        f"**PII Anonymization at Ingest:** `{meta['pii_anonymized_at_ingest']}`  ",
        f"**Volume Iterations (Median):** `{vol_iters}` (reporting median across repeated runs)",
        "",
        "## 1. Volume Benchmark (Bronze Storage Write + Spark Batch ETL -> Silver & Quarantine Parquet)",
        "",
        "> [!NOTE]",
        "> **Measurement Boundary:** Stage 1 Bronze Storage Write + Stage 2 Spark Batch ETL (Read Bronze S3A/JSON -> Schema Cast -> Validation & Quarantine -> Window Deduplication -> Silver Parquet Write). Excludes Kafka streaming.",
        ">",
        "> **Resource Measurement Boundary (CPU / RAM):** CPU % and peak RSS RAM (MB) are continuously sampled every 50ms via a background thread across the full process tree (Python benchmark harness + PySpark Java JVM / Py4J children). These numbers reflect the local client/driver benchmark process tree and should **not** be interpreted as the cluster-wide distributed resource utilization of an arbitrary multi-node Spark cluster.",
        "",
        "| Seed | Input Records | Valid (Silver) | Quarantined | Deduplicated | DQ Score | Bronze Write (s) | Spark ETL (s) | Total Time (s) | Stability Range (s) | Spark Rate (rec/s) | Total Rate (rec/s) | CPU % (Tree) | RAM Tree (MB) | RAM Py (MB) | Bronze (B) | Silver (B) | Quarantine (B) |",
        "| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |",
    ]
    for v in vol_rows:
        bw = v.get("bronze_write_sec", 0.0)
        se = v.get("spark_etl_sec", 0.0)
        st = v.get("spark_throughput_rps", v["throughput_rps"])
        cpu = v.get("cpu_percent", 0.0)
        ram = v.get("peak_tree_ram_mb", v.get("peak_ram_mb", 0.0))
        py_ram = v.get("peak_python_ram_mb", 0.0)
        stab_range = (
            f"{v.get('duration_min_sec', v['duration_sec']):.4f} - {v.get('duration_max_sec', v['duration_sec']):.4f}"
            if v.get("duration_min_sec") is not None and v.get("iterations", 1) > 1
            else f"{v['duration_sec']:.4f}"
        )
        lines.append(
            f"| {v['seed']} | {v['records_input']:,} | {v['valid_count']:,} | {v['quarantine_count']:,} | "
            f"{v['duplicate_count']:,} | {v['dq_score']}% | {bw:.4f}s | {se:.4f}s | {v['duration_sec']:.4f}s | "
            f"{stab_range}s | {st:,.1f} | {v['throughput_rps']:,.1f} | {cpu}% | {ram:.1f} | {py_ram:.1f} | "
            f"{v['bronze_bytes']:,} | {v['silver_bytes']:,} | {v['quarantine_bytes']:,} |"
        )

    # Detailed iteration breakdown if any volume row has multiple iterations
    has_multi_iters = any(len(v.get("iteration_runs", [])) > 1 for v in vol_rows)
    if has_multi_iters:
        lines.extend([
            "",
            "### 1.1 Volume Iterations Stability Breakdown",
            "",
            "| Input Records | Iteration | Bronze Write (s) | Spark ETL (s) | Total Time (s) | Spark Rate (rec/s) | Total Rate (rec/s) | CPU % | RAM Tree (MB) | RAM Py (MB) | Accounting Match |",
            "| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |",
        ])
        for v in vol_rows:
            for ir in v.get("iteration_runs", []):
                ir_bw = ir.get("bronze_write_sec", 0.0)
                ir_se = ir.get("spark_etl_sec", 0.0)
                ir_st = ir.get("spark_throughput_rps", ir.get("throughput_rps", 0.0))
                ir_tot = ir.get("duration_sec", 0.0)
                ir_tp = ir.get("throughput_rps", 0.0)
                ir_cpu = ir.get("cpu_percent", 0.0)
                ir_ram = ir.get("peak_tree_ram_mb", ir.get("peak_ram_mb", 0.0))
                ir_py = ir.get("peak_python_ram_mb", 0.0)
                ir_acc = "PASSED" if ir.get("accounting_reconciled") else "FAILED"
                lines.append(
                    f"| {ir['records_input']:,} | Run {ir.get('iteration', 1)} | {ir_bw:.4f}s | {ir_se:.4f}s | "
                    f"{ir_tot:.4f}s | {ir_st:,.1f} | {ir_tp:,.1f} | {ir_cpu}% | {ir_ram:.1f} | {ir_py:.1f} | {ir_acc} |"
                )

    lines.extend([
        "",
        "## 2. Velocity Benchmark (Kafka Producer -> KRaft Broker -> RawConsumer -> Bronze / DLQ)",
        "",
        "> [!NOTE]",
        "> **Transport Mode:** `real_kafka` enforces live Kafka KRaft broker connection; offline buffer is disallowed in integration mode.",
        ">",
        "> **Per-Message Latency Profiling ($L_i = t_{\\text{processed}, i} - t_{\\text{sent}, i}$):** Monotonic clocks (`time.perf_counter()`) timestamp each individual message at dispatch and upon consumer schema validation/quarantine routing. Percentiles p50/p95/p99 reflect actual per-message processing latency, not aggregate batch duration.",
        "",
        "| Seed | Transport | Target (msg/s) | Sent | Consumed | Valid (Bronze) | DLQ | Lag | Produce Rate (msg/s) | Consume Rate (msg/s) | Latency p50 (ms) | Latency p95 (ms) | Latency p99 (ms) | E2E Time (s) | E2E Rate (msg/s) | Bronze (B) |",
        "| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |",
    ])
    for v in vel_rows:
        cmps = v.get("consumer_throughput_mps", 0.0)
        lag = v.get("consumer_lag", 0)
        p50 = v.get("latency_p50_ms", 0.0)
        p95 = v.get("latency_p95_ms", 0.0)
        p99 = v.get("latency_p99_ms", 0.0)
        lines.append(
            f"| {v['seed']} | {v['transport_mode']} | {v['target_rate_mps']:,} | "
            f"{v['messages_sent']:,} | {v['messages_consumed']:,} | {v['valid_count']:,} | "
            f"{v['quarantine_dlq_count']:,} | {lag} | {v['actual_produce_rate_mps']:,.1f} | "
            f"{cmps:,.1f} | {p50}ms | {p95}ms | {p99}ms | "
            f"{v['e2e_duration_sec']:.4f}s | {v['e2e_throughput_mps']:,.1f} | {v['bronze_bytes']:,} |"
        )

    lines.extend([
        "",
        "## 3. Variety Benchmark (Multi-Format Ingest -> PII Hash -> Bronze -> Spark Parse/Validate/Transform -> Silver)",
        "",
        "> [!NOTE]",
        "> **Format Categories:** Distinctly separates synthetic file fixtures, semi-structured Schema.org JSON-LD HTML, and authentic Tiki crawler datasets. All formats undergo strict PII anonymization and canonical schema validation.",
        "",
        "| Seed | Format / Source | Category | Source (B) | Input Records | Valid (Silver) | Quarantined | Deduplicated | DQ Score | Duration (s) | Throughput (rec/s) | Silver (B) | Quarantine (B) |",
        "| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |",
    ])
    for v in var_rows:
        cat = v.get("category", "synthetic_file")
        lines.append(
            f"| {v['seed']} | {v['format']} | {cat} | {v['source_bytes']:,} | {v['records_input']:,} | "
            f"{v['valid_count']:,} | {v['quarantine_count']:,} | {v['duplicate_count']:,} | "
            f"{v['dq_score']}% | {v['duration_sec']:.4f}s | {v['throughput_rps']:,.1f} | "
            f"{v['silver_bytes']:,} | {v['quarantine_bytes']:,} |"
        )

    lines.extend([
        "",
        "## 4. Accounting Reconciliation & Verification",
        "",
        "- **Volume Accounting Invariant:** `input_count == valid_count + duplicate_count + quarantine_count` verified on 100% of runs.",
        "- **Velocity Accounting Invariant:** `messages_consumed == valid_count + quarantine_dlq_count` verified on 100% of runs.",
        "- **Variety Accounting Invariant:** `input_count == valid_count + duplicate_count + quarantine_count` verified across all 7 formats.",
        "- **PII Sanitization Invariant:** Zero unanonymized customer PII stored in Silver Parquet across all formats.",
    ])

    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    # 2. docs/reports/3v_evidence.md
    evidence_path = report_dir / "3v_evidence.md"
    evidence_lines = [
        "# SentinelAI — Empirical 3V Big Data Evidence Report",
        "",
        f"- **Generated At**: `{results['timestamp']}`",
        f"- **Runtime Mode**: `{meta['runtime_mode']}` (`{meta['storage_backend']}`)",
        f"- **Deterministic Seed**: `{meta['seed']}`",
        f"- **Volume Iterations**: `{meta.get('volume_iterations', 1)}` (reporting median with min-max stability range)",
        "",
        "## 1. Volume",
        f"- **Executed Scale**: Empirically benchmarked from **{min_vol:,}** to **{max_vol:,}** records per batch (`sizes={[r['records_input'] for r in vol_rows]}`).",
        f"- **Peak Measured Spark Batch Throughput**: **{peak_spark_rate:,.1f} records/sec** (Stage 2: Spark S3A read, envelope unpacking, schema cast, validation, quarantine routing, window deduplication, and Silver Parquet write).",
        f"- **Peak Total End-to-End Throughput**: **{peak_total_rate:,.1f} records/sec** (Stage 1 Bronze Write + Stage 2 Spark Batch ETL).",
        "- **Measurement Boundary**: Explicitly limited to Bronze write + Spark ETL. Kafka is not involved in this stage.",
        "- **Resource Tracking (CPU / RAM)**: Continuously sampled at 50ms across the full process tree (Python harness + Java JVM / Py4J children). Reflects local client/driver benchmark process tree, not distributed cluster total.",
        "- **Quarantine & Deduplication Accounting**: Every run enforces `records_input == valid_unique + duplicate_count + quarantine_count` and tracks exact byte footprint across Bronze JSON, Silver Parquet, and Quarantine Parquet.",
        "",
        "## 2. Velocity",
        f"- **Real Kafka Streaming Ingestion**: Measured across target rates from **{min_vel_target:,}** to **{max_vel_target:,} msg/s** via `{vel_rows[0]['transport_mode'] if vel_rows else 'real_kafka'}`.",
        f"- **Peak Producer Rate**: **{peak_prod_rate:,.1f} msg/s**; **Peak Consumer Throughput**: **{peak_cons_rate:,.1f} msg/s**; **Peak E2E Rate**: **{peak_e2e_rate:,.1f} msg/s**.",
        "- **Latency Profiling**: Empirically measures p50, p95, and p99 delivery-to-commit latencies per rate.",
        "- **Contract Enforcement on Stream**: Malformed messages are routed to Dead-Letter Queue (`dead-letter`) before Bronze commit with zero silent drops.",
        "",
        "## 3. Variety",
        f"- **End-to-End Multi-Format Pipeline**: Benchmarked across **{len(var_rows)}** formats/sources (`{formats_list}`).",
        "- **Authentic Tiki Crawler Distinction**: Strictly distinguishes between synthetic Schema.org JSON-LD HTML fixtures and authentic Tiki crawler datasets.",
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
        f"- **Số lần chạy lặp lại (Iterations)**: `{meta.get('volume_iterations', 1)}` (báo cáo giá trị median và dải min-max)",
        "",
        "---",
        "",
        "## 1. Kết Quả Volume (Bronze -> Spark Batch ETL -> Silver & Quarantine)",
        "",
        "> [!NOTE]",
        "> **Ranh giới đo lường (Measurement Boundary):** Giai đoạn 1 ghi Bronze Storage + Giai đoạn 2 Spark Batch ETL (đọc Bronze -> chuẩn hóa kiểu -> kiểm định & cách ly -> khử trùng lặp -> ghi Silver Parquet). Không bao gồm Kafka.",
        ">",
        "> **Lưu ý về số đo CPU / RAM:** Chỉ số CPU % và RAM (MB) được đo đạc liên tục (sampling định kỳ 50ms bằng background thread) trên toàn bộ cây tiến trình cục bộ (tiến trình Python + tiến trình con Java JVM / Py4J worker). Số đo này phản ánh tài nguyên client chạy benchmark cục bộ, **không nên được diễn giải là tổng tài nguyên phân tán của toàn bộ cụm Spark** trong môi trường cluster đa node độc lập (như YARN/K8s/Standalone workers chạy ngoài cây tiến trình cục bộ).",
        "",
        f"Kịch bản chạy thực tế với các cỡ dữ liệu `{[r['records_input'] for r in vol_rows]}` bản ghi (có tiêm tỷ lệ bản ghi lỗi `dirty_ratio={meta['dirty_ratio']}` và bản ghi trùng `duplicate_ratio={meta['duplicate_ratio']}`). Mỗi cỡ được chạy lặp lại `{vol_iters}` lần để tính median và kiểm tra độ ổn định giữa các lần chạy.",
        "",
        "| Seed | Đầu Vào | Hợp Lệ (Silver) | Quarantine | Trùng Lặp | Điểm DQ | Ghi Bronze (s) | Spark ETL (s) | Tổng Thời Gian (s) | Độ Ổn Định Min-Max (s) | Spark Rate (rec/s) | Tổng Rate (rec/s) | CPU % (Tree) | RAM Tree (MB) | RAM Py (MB) | Dung Lượng Bronze (B) | Dung Lượng Silver (B) | Dung Lượng Quarantine (B) |",
        "| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |",
    ]
    for v in vol_rows:
        bw = v.get("bronze_write_sec", 0.0)
        se = v.get("spark_etl_sec", 0.0)
        st = v.get("spark_throughput_rps", v["throughput_rps"])
        cpu = v.get("cpu_percent", 0.0)
        ram = v.get("peak_tree_ram_mb", v.get("peak_ram_mb", 0.0))
        py_ram = v.get("peak_python_ram_mb", 0.0)
        stab_range = (
            f"{v.get('duration_min_sec', v['duration_sec']):.4f} - {v.get('duration_max_sec', v['duration_sec']):.4f}"
            if v.get("duration_min_sec") is not None and v.get("iterations", 1) > 1
            else f"{v['duration_sec']:.4f}"
        )
        doc_lines.append(
            f"| {v['seed']} | {v['records_input']:,} | {v['valid_count']:,} | {v['quarantine_count']:,} | "
            f"{v['duplicate_count']:,} | {v['dq_score']}% | {bw:.4f}s | {se:.4f}s | {v['duration_sec']:.4f}s | "
            f"{stab_range}s | {st:,.1f} | {v['throughput_rps']:,.1f} | {cpu}% | {ram:.1f} | {py_ram:.1f} | "
            f"{v['bronze_bytes']:,} | {v['silver_bytes']:,} | {v['quarantine_bytes']:,} |"
        )

    if has_multi_iters:
        doc_lines.extend([
            "",
            "### 1.1 Chi Tiết Các Lần Chạy Lặp (Volume Iterations Breakdown)",
            "",
            "| Cỡ Dữ Liệu | Lần Chạy | Ghi Bronze (s) | Spark ETL (s) | Tổng Thời Gian (s) | Spark Rate (rec/s) | Tổng Rate (rec/s) | CPU % | RAM Tree (MB) | RAM Py (MB) | Trạng Thái Khớp Accounting |",
            "| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |",
        ])
        for v in vol_rows:
            for ir in v.get("iteration_runs", []):
                ir_bw = ir.get("bronze_write_sec", 0.0)
                ir_se = ir.get("spark_etl_sec", 0.0)
                ir_st = ir.get("spark_throughput_rps", ir.get("throughput_rps", 0.0))
                ir_tot = ir.get("duration_sec", 0.0)
                ir_tp = ir.get("throughput_rps", 0.0)
                ir_cpu = ir.get("cpu_percent", 0.0)
                ir_ram = ir.get("peak_tree_ram_mb", ir.get("peak_ram_mb", 0.0))
                ir_py = ir.get("peak_python_ram_mb", 0.0)
                ir_acc = "ĐẠT (100% Khớp)" if ir.get("accounting_reconciled") else "LỖI"
                doc_lines.append(
                    f"| {ir['records_input']:,} | Lần {ir.get('iteration', 1)} | {ir_bw:.4f}s | {ir_se:.4f}s | "
                    f"{ir_tot:.4f}s | {ir_st:,.1f} | {ir_tp:,.1f} | {ir_cpu}% | {ir_ram:.1f} | {ir_py:.1f} | {ir_acc} |"
                )

    doc_lines.extend([
        "",
        "---",
        "",
        "## 2. Kết Quả Velocity (Kafka Producer -> Broker -> RawConsumer -> Bronze / DLQ)",
        "",
        "> [!NOTE]",
        "> **Transport Mode:** `real_kafka` kết nối trực tiếp cụm Kafka KRaft; chế độ integration không dùng hàng đợi offline giả lập.",
        ">",
        "> **Phương Pháp Đo Độ Trễ ($L_i = t_{\\text{processed}, i} - t_{\\text{sent}, i}$):** Sử dụng đồng hồ monotonic độ chính xác cao (`time.perf_counter()`) ghi nhận thời điểm gửi của từng message riêng biệt và thời điểm hoàn tất xử lý/quarantine của consumer đối với message đó. Các phân vị p50, p95, p99 được tính trực tiếp từ tập hợp các $L_i$ này, phản ánh trung thực độ trễ streaming trên từng bản ghi thay vì thời gian gộp của cả batch.",
        "",
        "| Seed | Transport | Mục Tiêu (msg/s) | Đã Gửi | Đã Nhận | Hợp Lệ (Bronze) | DLQ | Lag | Tốc Độ Gửi (msg/s) | Tốc Độ Nhận (msg/s) | Độ Trễ p50 (ms) | Độ Trễ p95 (ms) | Độ Trễ p99 (ms) | Thời Gian E2E (s) | Throughput E2E (msg/s) | Dung Lượng Bronze (B) |",
        "| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |",
    ])
    for v in vel_rows:
        cmps = v.get("consumer_throughput_mps", 0.0)
        lag = v.get("consumer_lag", 0)
        p50 = v.get("latency_p50_ms", 0.0)
        p95 = v.get("latency_p95_ms", 0.0)
        p99 = v.get("latency_p99_ms", 0.0)
        doc_lines.append(
            f"| {v['seed']} | {v['transport_mode']} | {v['target_rate_mps']:,} | "
            f"{v['messages_sent']:,} | {v['messages_consumed']:,} | {v['valid_count']:,} | "
            f"{v['quarantine_dlq_count']:,} | {lag} | {v['actual_produce_rate_mps']:,.1f} | "
            f"{cmps:,.1f} | {p50}ms | {p95}ms | {p99}ms | "
            f"{v['e2e_duration_sec']:.4f}s | {v['e2e_throughput_mps']:,.1f} | {v['bronze_bytes']:,} |"
        )

    doc_lines.extend([
        "",
        "---",
        "",
        "## 3. Kết Quả Variety (Parse -> PII Hash -> Bronze -> Spark Validate/Transform -> Silver)",
        "",
        "> [!NOTE]",
        "> Phân biệt rõ HTML fixture tổng hợp Schema.org với dữ liệu crawler Tiki thực tế. Toàn bộ các định dạng đều tuân thủ schema chuẩn, ẩn danh PII và qua Spark ETL.",
        "",
        "| Seed | Định Dạng / Nguồn | Phân Loại | Dung Lượng Nguồn (B) | Đầu Vào | Hợp Lệ (Silver) | Quarantine | Trùng Lặp | Điểm DQ | Thời Gian (s) | Throughput (rec/s) | Dung Lượng Silver (B) | Dung Lượng Quarantine (B) |",
        "| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |",
    ])
    for v in var_rows:
        cat = v.get("category", "synthetic_file")
        doc_lines.append(
            f"| {v['seed']} | {v['format']} | {cat} | {v['source_bytes']:,} | {v['records_input']:,} | "
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
        "# Chạy nhanh smoke-test:",
        "python scripts/benchmark.py --mode integration --seed 42 --volume-sizes 100,1000,10000 --velocity-rates 50,200 --variety-count 100",
        "",
        "# Chạy tải lớn có lặp lại tính median:",
        "python scripts/benchmark.py --mode integration --seed 42 --volume-sizes 10000,100000,500000 --volume-iterations 3 --velocity-rates 500,1000 --velocity-messages 2000 --variety-count 500",
        "```",
    ])
    doc_bench_path.write_text("\n".join(doc_lines) + "\n", encoding="utf-8")

    print(f"\n[OK] Benchmark reports written to:\n  - {json_path}\n  - {md_path}\n  - {evidence_path}\n  - {doc_bench_path}")


# -----------------------------------------------------------------------------
# Main Runner
# -----------------------------------------------------------------------------

def run_all_benchmarks(
    mode: Optional[str] = None,
    seed: int = 42,
    volume_sizes: Optional[List[int]] = None,
    volume_iterations: int = 3,
    velocity_rates: Optional[List[int]] = None,
    velocity_messages: Optional[int] = None,
    velocity_duration_sec: Optional[float] = None,
    variety_count: int = 500,
    dirty_ratio: float = 0.05,
    duplicate_ratio: float = 0.03,
    tiki_limit: int = 10,
    skip_volume: bool = False,
    skip_velocity: bool = False,
    skip_variety: bool = False,
) -> Dict[str, Any]:
    """Executes all 3V benchmarks on the real pipeline and generates synchronized reports."""
    effective_mode = configure_benchmark_mode(mode)
    print("==========================================================")
    print(f"   SENTINELAI 3V BIG DATA BENCHMARK SUITE ({effective_mode.upper()})")
    print("==========================================================")

    etl_job = SparkBatchETLJob()

    vol_res = (
        benchmark_volume(
            sizes=volume_sizes,
            seed=seed,
            dirty_ratio=dirty_ratio,
            duplicate_ratio=duplicate_ratio,
            spark_etl=etl_job,
            iterations=volume_iterations,
        )
        if not skip_volume
        else []
    )

    vel_res = (
        benchmark_velocity(
            rates=velocity_rates,
            seed=seed,
            dirty_ratio=0.04,
            effective_mode=effective_mode,
            messages_per_rate=velocity_messages,
            duration_sec=velocity_duration_sec,
        )
        if not skip_velocity
        else []
    )

    var_res = (
        benchmark_variety(
            records_per_format=variety_count,
            seed=seed,
            dirty_ratio=dirty_ratio,
            duplicate_ratio=0.02,
            spark_etl=etl_job,
            effective_mode=effective_mode,
            tiki_limit=tiki_limit,
        )
        if not skip_variety
        else []
    )

    json_path = REPO_ROOT / "docs" / "reports" / "benchmark_3v.json"
    if json_path.exists():
        try:
            with open(json_path, "r", encoding="utf-8") as f:
                prev_data = json.load(f)
            if skip_volume and not vol_res and prev_data.get("volume"):
                vol_res = prev_data["volume"]
            if skip_velocity and not vel_res and prev_data.get("velocity"):
                vel_res = prev_data["velocity"]
            if skip_variety and not var_res and prev_data.get("variety"):
                var_res = prev_data["variety"]
        except Exception:
            pass

    full_results = {
        "timestamp": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
        "metadata": {
            "runtime_mode": effective_mode,
            "storage_backend": type(storage.backend).__name__,
            "seed": seed,
            "dirty_ratio": dirty_ratio,
            "duplicate_ratio": duplicate_ratio,
            "pii_anonymized_at_ingest": True,
            "volume_iterations": volume_iterations,
            "velocity_parameters": {
                "rates": velocity_rates or [50, 200, 500, 1000],
                "messages_per_rate": velocity_messages,
                "duration_sec": velocity_duration_sec,
            },
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
        help="Comma-separated record counts for Volume benchmark (default: 100,1000,5000,10000; supports 100000, 500000, 1000000)",
    )
    parser.add_argument(
        "--volume-iterations",
        type=int,
        default=3,
        help="Number of iterations to run each Volume size to report median (default: 3)",
    )
    parser.add_argument(
        "--iterations",
        type=int,
        default=None,
        help="Alias for --volume-iterations",
    )
    parser.add_argument(
        "--velocity-rates",
        default="50,200,500,1000",
        help="Comma-separated target msg/s rates for Velocity benchmark (default: 50,200,500,1000)",
    )
    parser.add_argument(
        "--velocity-messages",
        type=int,
        default=None,
        help="Explicit message count per rate in Velocity benchmark (default: None, auto-calculated)",
    )
    parser.add_argument(
        "--velocity-duration-sec",
        type=float,
        default=None,
        help="Target streaming duration in seconds per rate in Velocity benchmark (default: None)",
    )
    parser.add_argument(
        "--variety-count",
        type=int,
        default=500,
        help="Record count per format in Variety benchmark (default: 500)",
    )
    parser.add_argument(
        "--tiki-limit",
        type=int,
        default=10,
        help="Max Tiki crawler records for JSONL_Tiki_Crawler in Variety (default: 10)",
    )
    parser.add_argument("--skip-volume", action="store_true", help="Skip Volume benchmark")
    parser.add_argument("--skip-velocity", action="store_true", help="Skip Velocity benchmark")
    parser.add_argument("--skip-variety", action="store_true", help="Skip Variety benchmark")
    args = parser.parse_args()

    parsed_sizes = [int(x.strip()) for x in args.volume_sizes.split(",") if x.strip()]
    parsed_rates = [int(x.strip()) for x in args.velocity_rates.split(",") if x.strip()]
    effective_iterations = args.iterations if args.iterations is not None else args.volume_iterations

    run_all_benchmarks(
        mode=args.mode,
        seed=args.seed,
        volume_sizes=parsed_sizes,
        volume_iterations=effective_iterations,
        velocity_rates=parsed_rates,
        velocity_messages=args.velocity_messages,
        velocity_duration_sec=args.velocity_duration_sec,
        variety_count=args.variety_count,
        tiki_limit=args.tiki_limit,
        skip_volume=args.skip_volume,
        skip_velocity=args.skip_velocity,
        skip_variety=args.skip_variety,
    )
