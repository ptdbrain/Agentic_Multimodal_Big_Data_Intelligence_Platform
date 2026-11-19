"""Measure labelled Kafka/Spark/MinIO/PostgreSQL stream visibility in isolated namespaces.

Default campaign: 50/200/500 reviews/s, three trials, 30s warmup, 120s measurement.
SQL latency is a polled upper bound at the all-sink-confirmed frontier, not browser latency.
"""

from __future__ import annotations

import argparse
from collections import defaultdict, deque
from datetime import datetime, timedelta, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import random
import subprocess
import sys
import tempfile
import threading
import time
import uuid

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


def validate_workload(rate: int, warmup: float, measurement: float, drain: float) -> None:
    if type(rate) is not int or rate <= 0:
        raise ValueError("Positive integer target rate required")
    if (
        any(not math.isfinite(value) for value in (warmup, measurement, drain))
        or warmup < 0
        or measurement <= 0
        or drain <= 0
    ):
        raise ValueError("Finite warmup>=0, measurement>0 and drain>0 required")


class VisibilityTracker:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.pending = defaultdict(deque)
        self.last_rank = {}
        self.counts = {}
        self.latencies = []
        self.registered = 0

    def register(self, key: tuple[str, str], rank: int, sent: float) -> None:
        with self.lock:
            if type(rank) is not int or rank <= self.last_rank.get(key, 0) or not math.isfinite(sent):
                raise ValueError("Strictly increasing sample rank and finite monotonic clock required")
            if self.registered >= 100000:
                raise ValueError("Visibility sample budget exceeded")
            self.last_rank[key] = rank
            self.pending[key].append((rank, sent))
            self.registered += 1

    def observe(self, counts: dict[tuple[str, str], int], observed: float) -> None:
        with self.lock:
            if not math.isfinite(observed):
                raise ValueError("Finite observation clock required")
            for key, count in counts.items():
                if type(count) is not int or count < self.counts.get(key, 0):
                    raise ValueError("SQL serving count must not regress")
                if any(sent > observed for rank, sent in self.pending[key] if rank <= count):
                    raise ValueError("SQL visibility cannot precede producer enqueue")
            for key, count in counts.items():
                self.counts[key] = count
                while self.pending[key] and self.pending[key][0][0] <= count:
                    _, sent = self.pending[key].popleft()
                    self.latencies.append(observed - sent)

    def summary(self) -> dict:
        with self.lock:
            values = sorted(self.latencies)

            def percentile(fraction):
                if not values:
                    return None
                index = (len(values) - 1) * fraction
                low, high = math.floor(index), math.ceil(index)
                return 1000 * (values[low] + (values[high] - values[low]) * (index - low))

            return {
                "samples": len(values),
                "pending_samples": self.registered - len(values),
                "p50_ms": percentile(0.5),
                "p95_ms": percentile(0.95),
                "p99_ms": percentile(0.99),
                "max_ms": 1000 * values[-1] if values else None,
            }


def _persist(path: Path, report: dict) -> None:
    from orchestration.source_jobs import write_atomic

    write_atomic(path, report)


def _frontier(progress: dict, topic: str) -> dict[int, int]:
    result = {}
    for source in progress.get("sources", []):
        value = source.get("endOffset")
        if isinstance(value, str):
            value = json.loads(value)
        if isinstance(value, dict):
            for partition, offset in value.get(topic, {}).items():
                result[int(partition)] = int(offset)
    return result


def sinks_reconciled(frontiers: dict[str, dict[int, int]], broker: dict[int, int]) -> bool:
    """Require window/quarantine plus any configured audit to reach every broker partition."""
    return bool(broker) and {"window", "quarantine"}.issubset(frontiers) and all(
        all(frontier.get(partition, -1) >= value for partition, value in broker.items())
        for frontier in frontiers.values()
    )


def run_case(
    rate: int,
    output: Path,
    *,
    warmup: float = 30,
    measurement: float = 120,
    drain_timeout: float = 120,
    trigger_seconds: int = 30,
) -> dict:
    validate_workload(rate, warmup, measurement, drain_timeout)
    if type(trigger_seconds) is not int or trigger_seconds <= 0:
        raise ValueError("Positive integer trigger interval required")
    output = output.absolute()
    with output.open("x") as stream:
        json.dump({"completed": False, "status": "preparing", "target_rate_mps": rate}, stream)
    report = {
        "completed": False,
        "status": "preparing",
        "checked_at": datetime.now(timezone.utc).isoformat(),
        "target_rate_mps": rate,
        "warmup_sec": warmup,
        "measurement_sec": measurement,
        "drain_timeout_sec": drain_timeout,
        "trigger_seconds": trigger_seconds,
        "source": "Explicit synthetic benchmark observations, seed42; not marketplace data",
    }
    _persist(output, report)
    try:
        required = (
            "POSTGRES_HOST",
            "POSTGRES_PORT",
            "POSTGRES_DB",
            "POSTGRES_USER",
            "POSTGRES_PASSWORD",
            "MINIO_ENDPOINT",
            "MINIO_ACCESS_KEY",
            "MINIO_SECRET_KEY",
            "KAFKA_BOOTSTRAP_SERVERS",
            "SPARK_MASTER",
        )
        if any(not os.environ.get(key) for key in required) or os.environ.get("RUNTIME_MODE") not in (
            "integration",
            "production",
        ):
            raise ValueError("Explicit real-service runtime environment required")
        import psycopg2
        from psycopg2 import sql
        from minio import Minio
        from kafka.admin import KafkaAdminClient, NewTopic
        from scripts.prepare_application import _env, _write
        import yaml

        identity = "stream-bench-" + uuid.uuid4().hex[:12]
        schema, bucket, topic = identity.replace("-", "_"), identity, identity + "-reviews"
        report["namespace"] = {"dataset_id": identity, "schema": schema, "bucket": bucket, "topic": topic}
        report["retention"] = "Owned checkpoint/SQL/topic evidence retained for replay; no observed namespaces modified"
        _persist(output, report)
        params = dict(
            host=os.environ["POSTGRES_HOST"],
            port=int(os.environ["POSTGRES_PORT"]),
            dbname=os.environ["POSTGRES_DB"],
            user=os.environ["POSTGRES_USER"],
            password=os.environ["POSTGRES_PASSWORD"],
        )
        connection = psycopg2.connect(**params)
        try:
            connection.autocommit = True
            connection.cursor().execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
        finally:
            connection.close()
        client = Minio(
            os.environ["MINIO_ENDPOINT"],
            access_key=os.environ["MINIO_ACCESS_KEY"],
            secret_key=os.environ["MINIO_SECRET_KEY"],
            secure=os.environ.get("MINIO_SECURE", "false") == "true",
        )
        client.make_bucket(bucket)
        admin = KafkaAdminClient(bootstrap_servers=os.environ["KAFKA_BOOTSTRAP_SERVERS"])
        try:
            admin.create_topics(
                [
                    NewTopic(
                        topic,
                        num_partitions=2,
                        replication_factor=int(os.environ.get("KAFKA_REPLICATION_FACTOR", "1")),
                        topic_configs={"retention.ms": "86400000"},
                    )
                ]
            )
        finally:
            admin.close()
        work = Path(tempfile.mkdtemp(prefix="stream-benchmark-"))
        env = dict(
            os.environ,
            PGOPTIONS="-c search_path=" + schema,
            DATA_LAKE_BUCKET=bucket,
            KAFKA_TOPIC_REVIEWS=topic,
            SERVING_DATASET_ID=identity,
            SOURCE_JOB_NAMESPACE=identity,
        )
        _write(
            work / "runtime.env",
            _env(
                {
                    key: value
                    for key, value in env.items()
                    if key in required
                    or key.startswith(("MINIO_", "POSTGRES_", "KAFKA_", "SPARK_", "STREAM_", "PYSPARK_"))
                    or key
                    in ("PGOPTIONS", "RUNTIME_MODE", "DATA_LAKE_BUCKET", "SERVING_DATASET_ID", "SOURCE_JOB_NAMESPACE")
                }
            ),
        )
        _write(
            work / "registry.yml",
            yaml.safe_dump(
                {
                    "version": 1,
                    "datasets": [
                        {
                            "id": identity,
                            "label": "Generated stream benchmark",
                            "kind": "commerce",
                            "source_group": "simulation",
                            "postgres_schema": schema,
                            "bucket": bucket,
                            "environment_file": str(work / "runtime.env"),
                        }
                    ],
                }
            ),
        )
        report.update(status="starting", private_configuration=str(work), master=os.environ["SPARK_MASTER"])
        _persist(output, report)
        with os.fdopen(os.open(output.with_suffix(".log"), os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), "w") as log:
            result = subprocess.run(
                [sys.executable, str(Path(__file__).resolve()), "--worker", str(output)],
                env=env,
                stdout=log,
                stderr=subprocess.STDOUT,
                cwd=ROOT,
            )
        final = json.loads(output.read_text())
        if result.returncode or not final.get("completed"):
            final.update(completed=False, status="failed", worker_exit_code=result.returncode)
            _persist(output, final)
            raise RuntimeError("Streaming workload failed; see retained report and private log")
        return final
    except BaseException as error:
        current = json.loads(output.read_text())
        current.update(completed=False, status="failed", parent_error_type=type(error).__name__)
        current.setdefault("error_type", type(error).__name__)
        _persist(output, current)
        raise


def worker(output: Path) -> None:
    report = json.loads(output.read_text())
    from serving.registry import DatasetRegistry
    from serving.store import ServingStore, connect_dataset, instant
    from serving.admission import writer_transaction
    from serving.ownership import acquire_stream_owner
    from operations.migration import read_kafka_cluster_id
    from config.settings import settings
    from serving.runtime import stream_configuration, validate_stream_binding
    from spark.spark_session import SparkSessionFactory
    from spark.streaming.spark_streaming_job import SparkStreamingJob
    from kafka.producers.stream_producer import StreamProducer
    from kafka import KafkaConsumer
    from kafka.structs import TopicPartition
    from scripts.benchmark import ResourceMonitor
    from scripts.benchmark_fullpath import source_fingerprint

    binding = next(iter(DatasetRegistry.load(Path(report["private_configuration"]) / "registry.yml").datasets.values()))
    generation = "benchmark-" + uuid.uuid4().hex[:12]
    paths = {
        "checkpoint_dir": f"s3a://{binding.bucket}/checkpoints/{generation}",
        "output_path": f"s3a://{binding.bucket}/streaming/{generation}/metrics",
        "quarantine_path": f"s3a://{binding.bucket}/streaming/{generation}/quarantine",
    }
    validate_stream_binding(binding, dict(os.environ), paths)
    report.update(generation=generation, paths=paths, source_fingerprint=source_fingerprint())
    tracker, stop, state_lock = VisibilityTracker(), threading.Event(), threading.Lock()
    state = {"counts": {}, "error": None}
    expected, futures = defaultdict(int), deque()
    owner = connect_dataset(binding)
    store = ServingStore(owner, binding.id, binding.source_group)
    job = spark = producer = monitor = thread = None
    owner_guard = acquire_stream_owner(owner, binding)
    owned = False
    try:
        with writer_transaction(owner):
            store.initialize()
        owner_guard.__enter__()
        owned = True
        store.register_stream(
            generation,
            stream_configuration(
                window_seconds=60, watermark_seconds=600, paths=paths, topic=report["namespace"]["topic"],
                output_mode="update", negative_threshold=settings.analytics.sentiment_negative_threshold,
                kafka_cluster_id=read_kafka_cluster_id(settings.kafka.bootstrap_servers),
            ),
        )
        spark = SparkSessionFactory.get_spark_session(master=report["master"])
        spark.range(1).count()
        job = SparkStreamingJob(spark_session=spark, serving_binding=binding, serving_generation=generation, **paths)
        job.start_review_stream(
            topics=report["namespace"]["topic"], processing_time=f"{report['trigger_seconds']} seconds"
        )

        def observe():
            connection = None
            try:
                connection = connect_dataset(binding)
                connection.autocommit = True
                data = ServingStore(connection, binding.id, binding.source_group)
                while not stop.is_set():
                    job.observe_progress()
                    for query in job.queries:
                        if query is not None and query.exception():
                            raise RuntimeError("A Spark sink terminated with an error")
                    rows = data.execute(
                        "SELECT w.product_id,w.window_start,w.review_count FROM stream_windows w "
                        "JOIN stream_deployments d ON w.dataset_id=d.dataset_id AND w.generation=d.generation "
                        "WHERE w.dataset_id=? AND w.generation=? AND d.last_batch IS NOT NULL AND w.batch_id<=d.last_batch",
                        (binding.id, generation),
                    ).fetchall()
                    counts = {(product, instant(clock).isoformat()): int(count) for product, clock, count in rows}
                    tracker.observe(counts, time.monotonic())
                    with state_lock:
                        state["counts"] = counts
                    stop.wait(0.2)
            except BaseException as error:
                with state_lock:
                    state["error"] = type(error).__name__
            finally:
                if connection:
                    connection.close()

        thread = threading.Thread(target=observe, daemon=True)
        thread.start()
        producer = StreamProducer(enable_offline_buffer=False)
        monitor = ResourceMonitor()
        monitor.start()
        report.update(
            status="warming_up",
            latency_boundary="Producer enqueue attempt to first polled SQL cumulative count at all-sink-confirmed frontier",
            latency_correlation="One ordered producer; stable per-product Kafka key; unique review IDs and cumulative (product,UTC-minute) ranks",
            resource_boundary="Client Python and child JVM during warmup/measure/drain; remote executors and service daemons excluded",
            browser_latency_status="not_measured",
            sql_poll_interval_sec=0.2,
        )
        _persist(output, report)
        anchor, base_clock, sequence = datetime.now(timezone.utc), time.monotonic(), 0
        rng = random.Random(42)
        config = json.dumps(
            {"seed": 42, "products": 100, "target_rate": report["target_rate_mps"], "generation": generation},
            sort_keys=True,
        )

        def phase(duration, measured):
            nonlocal sequence
            begin, count = time.monotonic(), 0
            deadline = begin
            while time.monotonic() - begin < duration:
                with state_lock:
                    if state["error"]:
                        raise RuntimeError("SQL observer failed: " + state["error"])
                now = time.monotonic()
                if deadline > now:
                    time.sleep(min(deadline - now, max(0, duration - (now - begin))))
                if time.monotonic() - begin >= duration:
                    break
                clock = anchor + timedelta(seconds=time.monotonic() - base_clock)
                product = f"sim-p{sequence % 100:03}"
                key = (product, clock.replace(second=0, microsecond=0).isoformat())
                payload = {
                    "review_id": generation + "-" + str(sequence),
                    "product_id": product,
                    "rating": 1 + sequence % 5,
                    "review_date": clock.isoformat(),
                    "review_text": "Explicit synthetic streaming benchmark observation",
                    "source_group": "simulation",
                    "source_name": "streaming_benchmark",
                    "acquisition_method": "generated",
                    "generator_seed": "42",
                    "generator_config": config,
                    "user_id": "anon_redacted",
                }
                expected[key] += 1
                sent = time.monotonic()
                if measured and rng.random() < min(1, 10 / report["target_rate_mps"]):
                    tracker.register(key, expected[key], sent)
                _, future = producer.send_message(
                    report["namespace"]["topic"], product, payload, event_type="NEW_REVIEW", wait_for_delivery=False
                )
                futures.append(future)
                if len(futures) > 1000:
                    futures.popleft().get(timeout=30)
                sequence += 1
                count += 1
                deadline = max(deadline + 1 / report["target_rate_mps"], time.monotonic())
            return count, time.monotonic() - begin

        warm_count, warm_seconds = phase(report["warmup_sec"], False)
        report.update(
            status="measuring",
            warmup_dispatched=warm_count,
            warmup_actual_sec=warm_seconds,
            measured_started_at=datetime.now(timezone.utc).isoformat(),
        )
        _persist(output, report)
        measured_count, measured_seconds = phase(report["measurement_sec"], True)
        drain_started = time.monotonic()
        while futures:
            futures.popleft().get(timeout=30)
        producer.close()
        producer = None
        consumer = KafkaConsumer(
            bootstrap_servers=os.environ["KAFKA_BOOTSTRAP_SERVERS"], enable_auto_commit=False, group_id=None
        )
        try:
            partitions = consumer.partitions_for_topic(report["namespace"]["topic"])
            if not partitions:
                raise RuntimeError("Kafka topic has no readable partitions")
            bounds = consumer.end_offsets([TopicPartition(report["namespace"]["topic"], p) for p in partitions])
            broker = {partition.partition: int(value) for partition, value in bounds.items()}
        finally:
            consumer.close()
        while time.monotonic() - drain_started < report["drain_timeout_sec"]:
            with state_lock:
                counts, error = dict(state["counts"]), state["error"]
            if error:
                raise RuntimeError("SQL observer failed: " + error)
            frontiers = {
                name: _frontier(query.lastProgress or {}, report["namespace"]["topic"])
                for name, query in zip(("window", "quarantine", "audit"), job.queries)
                if query is not None
            }
            all_sinks = sinks_reconciled(frontiers, broker)
            if counts == dict(expected) and all_sinks and tracker.summary()["pending_samples"] == 0:
                break
            time.sleep(0.2)
        else:
            report["drain_diagnostics"] = {
                "expected_total": sum(expected.values()),
                "confirmed_sql_total": sum(counts.values()),
                "expected_windows": len(expected),
                "confirmed_sql_windows": len(counts),
                "mismatched_windows": sum(counts.get(key) != value for key, value in expected.items()),
                "unexpected_windows": len(set(counts) - set(expected)),
                "kafka_end_offsets": broker,
                "sink_frontiers": frontiers,
                "all_sinks_confirmed": all_sinks,
            }
            raise TimeoutError("Drain did not reconcile confirmed SQL with every acknowledged input and sink offset")
        samples = tracker.summary()
        if not measured_count or not samples["samples"]:
            raise RuntimeError("No actual measured SQL visibility samples")
        report.update(
            completed=True,
            status="measured",
            measured_dispatched=measured_count,
            measured_actual_sec=measured_seconds,
            actual_dispatch_rate_mps=measured_count / measured_seconds,
            target_rate_ratio=(measured_count / measured_seconds) / report["target_rate_mps"],
            drain_sec=time.monotonic() - drain_started,
            total_acknowledged=sequence,
            total_confirmed_sql=sum(counts.values()),
            kafka_end_offsets=broker,
            all_sink_offsets_reconciled=True,
            sql_visibility_upper_bound_ms=samples,
            resources=monitor.stop(),
            performance_sla="Measured results only; target input rate is not a passed throughput guarantee",
            finished_at=datetime.now(timezone.utc).isoformat(),
        )
        _persist(output, report)
    except BaseException as error:
        report.update(
            completed=False,
            status="failed",
            error_type=type(error).__name__,
            sql_visibility_upper_bound_ms=tracker.summary(),
        )
        if monitor:
            report["resources_at_failure"] = monitor.stop()
        _persist(output, report)
        raise
    finally:
        stop.set()
        if thread:
            thread.join(timeout=15)
        try:
            if producer:
                producer.close()
            if job:
                job.stop()
            if owned:
                with writer_transaction(owner):
                    store.execute(
                        "UPDATE stream_deployments SET state=? WHERE dataset_id=? AND generation=?",
                        ("stopped" if report.get("completed") else "failed", binding.id, generation),
                    )
        finally:
            try:
                if owned:
                    owner_guard.__exit__(None, None, None)
            finally:
                owner.close()
                if monitor:
                    monitor.stop()
                if spark:
                    spark.stop()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rate", type=int)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--rates", nargs="+", type=int, default=[50, 200, 500])
    parser.add_argument("--iterations", type=int, default=3)
    parser.add_argument("--warmup", type=float, default=30)
    parser.add_argument("--measurement", type=float, default=120)
    parser.add_argument("--drain-timeout", type=float, default=120)
    parser.add_argument("--trigger-seconds", type=int, default=30)
    parser.add_argument("--worker", type=Path, help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.worker:
        worker(args.worker)
        return
    options = dict(
        warmup=args.warmup,
        measurement=args.measurement,
        drain_timeout=args.drain_timeout,
        trigger_seconds=args.trigger_seconds,
    )
    if args.rate is not None:
        if args.output is None or args.output_dir is not None:
            parser.error("Single case requires --output only")
        run_case(args.rate, args.output, **options)
        return
    if args.output_dir is None or args.output is not None or args.iterations < 1:
        parser.error("Matrix requires new --output-dir and positive iterations")
    for rate in args.rates:
        validate_workload(rate, args.warmup, args.measurement, args.drain_timeout)
    args.output_dir.mkdir(parents=True, exist_ok=False)
    matrix = {
        "completed": False,
        "rates": args.rates,
        "iterations": args.iterations,
        "configuration": options,
        "results": [],
    }
    manifest = args.output_dir / "matrix.json"
    _persist(manifest, matrix)
    for rate in args.rates:
        for iteration in range(1, args.iterations + 1):
            output = args.output_dir / f"stream-{rate}-{iteration}.json"
            try:
                result = run_case(rate, output, **options)
                matrix["results"].append({"file": output.name, "completed": result["completed"]})
            except Exception as error:
                matrix["results"].append({"file": output.name, "completed": False, "error_type": type(error).__name__})
            _persist(manifest, matrix)
    matrix["completed"] = all(item["completed"] for item in matrix["results"])
    _persist(manifest, matrix)
    if not matrix["completed"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
