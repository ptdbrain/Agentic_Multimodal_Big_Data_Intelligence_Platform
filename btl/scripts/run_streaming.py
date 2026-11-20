"""SentinelAI Run Streaming CLI Script.
Launches Spark Structured Streaming from Kafka or a labelled synthetic simulator.
"""

import sys
import argparse
import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from config.settings import settings


def simulator_duration(value, allow_zero=False):
    match = re.fullmatch(r"(\d+)\s*(seconds?|minutes?|hours?|days?)", value.strip().lower())
    if match is None:
        raise ValueError("Simulator duration requires whole seconds/minutes/hours/days")
    multiplier = {"second": 1, "minute": 60, "hour": 3600, "day": 86400}[match.group(2).rstrip("s")]
    seconds = int(match.group(1)) * multiplier
    if seconds == 0 and not allow_zero:
        raise ValueError("Simulator window must be positive")
    return seconds


def main():
    parser = argparse.ArgumentParser(description="SentinelAI Streaming Processor")
    parser.add_argument(
        "--mode", choices=["spark", "simulator"], default="simulator", help="Streaming execution engine"
    )
    parser.add_argument("--window", default=None, help="Tumbling window duration")
    parser.add_argument("--watermark", default=None, help="Event-time watermark delay")
    parser.add_argument("--topic", default=None, help="Kafka input topic")
    parser.add_argument("--seed", type=int, default=None, help="Simulator generator seed (default 42)")
    parser.add_argument(
        "--serving-pointer", help="Opt in to a verified Kafka migration generation and resume its source/checkpoint"
    )
    parser.add_argument("--deployment", type=Path, help="Private dataset-bound stream deployment JSON")
    parser.add_argument("--registry", type=Path)
    parser.add_argument("--dataset")
    parser.add_argument("--generation")
    args = parser.parse_args()
    if args.deployment:
        import json, subprocess, os
        from serving.registry import DatasetRegistry
        from scripts.run_scheduled_backup import environment

        if args.mode != "spark":
            parser.error("--deployment requires --mode spark")
        if args.deployment.stat().st_mode & 0o077:
            parser.error("Private stream deployment required")
        deployment = json.loads(args.deployment.read_text())
        binding = DatasetRegistry.load(deployment["registry"]).datasets[deployment["dataset_id"]]
        private_env = environment(binding.environment_file)
        env = dict(os.environ, **private_env)
        if binding.sqlite_path is None:
            env["MINIO_ENDPOINT_DOCKER"] = private_env.get("MINIO_ENDPOINT_DOCKER", private_env["MINIO_ENDPOINT"])
            env["MINIO_SECURE"] = private_env.get("MINIO_SECURE", "false")
        env["SERVING_DATASET_ID"] = binding.id
        command = [
            sys.executable,
            str(Path(__file__).resolve()),
            "--mode",
            "spark",
            "--registry",
            deployment["registry"],
            "--dataset",
            binding.id,
            "--generation",
            deployment["generation"],
        ]
        for name in ("window", "watermark", "topic"):
            if name in deployment:
                command.extend(["--" + name, str(deployment[name])])
        for key, envkey in [
            ("checkpoint_dir", "STREAM_CHECKPOINT_DIR"),
            ("output_path", "STREAM_OUTPUT_PATH"),
            ("quarantine_path", "STREAM_QUARANTINE_PATH"),
        ]:
            if not deployment.get(key):
                parser.error("Explicit deployment path required: " + key)
            env[envkey] = deployment[key]
        return subprocess.run(command, env=env, check=True).returncode

    resolved = None
    if args.serving_pointer:
        if args.mode != "spark":
            parser.error("--serving-pointer requires --mode spark")
        try:
            from operations.migration import resolve_stream_pointer

            resolved = resolve_stream_pointer(args.serving_pointer, expected_topics=args.topic)
            configuration = resolved["configuration"]
            for flag, supplied, key in [
                ("--window", args.window, "window_seconds"),
                ("--watermark", args.watermark, "watermark_seconds"),
            ]:
                if supplied is not None and simulator_duration(supplied, allow_zero=True) != configuration[key]:
                    raise ValueError(f"{flag} differs from promoted checkpoint configuration")
            args.window = f"{configuration['window_seconds']} seconds"
            args.watermark = f"{configuration['watermark_seconds']} seconds"
        except (ValueError, KeyError, OSError) as error:
            parser.error(str(error))
    args.window = args.window or "1 minute"
    args.watermark = args.watermark or "10 minutes"
    if args.mode == "simulator":
        try:
            window_seconds = simulator_duration(args.window)
            watermark_seconds = simulator_duration(args.watermark, allow_zero=True)
        except ValueError as error:
            parser.error(str(error))
        if args.topic:
            parser.error("--topic requires --mode spark; simulator uses generated input")
        args.seed = args.seed if args.seed is not None else 42
    elif args.seed is not None:
        parser.error("--seed applies only to --mode simulator")

    print("=" * 60)
    print("      SENTINELAI STREAMING PROCESSOR")
    print(f"  Mode:      {args.mode}")
    print(f"  Window:    {args.window}")
    print(f"  Watermark: {args.watermark}")
    if args.mode == "spark":
        print(f"  Broker:    {settings.kafka.bootstrap_servers}")
    else:
        print(f"  Source:    synthetic generator, seed={args.seed}")
    print("=" * 60)

    if args.mode == "spark":
        owner = None
        owner_owned = False
        stream_registered = False
        owner_guard = None
        job = None
        failed = False
        try:
            from spark.streaming.spark_streaming_job import SparkStreamingJob
            import os

            runtime_args = {
                name: os.environ[envkey]
                for name, envkey in [
                    ("checkpoint_dir", "STREAM_CHECKPOINT_DIR"),
                    ("output_path", "STREAM_OUTPUT_PATH"),
                    ("quarantine_path", "STREAM_QUARANTINE_PATH"),
                ]
                if envkey in os.environ
            }
            binding = None
            if any([args.registry, args.dataset, args.generation]):
                if not all([args.registry, args.dataset, args.generation]):
                    parser.error("--registry, --dataset and --generation are required together")
                from serving.registry import DatasetRegistry
                from serving.store import ServingStore, connect_dataset

                binding = DatasetRegistry.load(args.registry).datasets[args.dataset]
                from serving.runtime import validate_stream_binding

                if binding.kind != "commerce":
                    parser.error("Streaming requires commerce dataset")
                runtime_args.update(
                    serving_binding=binding,
                    serving_generation=args.generation,
                    serving_window_seconds=simulator_duration(args.window),
                )
                owner = connect_dataset(binding)
                store = ServingStore(owner, binding.id, binding.source_group)
                from serving.admission import writer_transaction

                with writer_transaction(owner):
                    store.initialize()
                from serving.ownership import acquire_stream_owner

                owner_guard = acquire_stream_owner(owner, binding)
                owner_guard.__enter__()
                owner_owned = True
            source_args = {}
            if resolved:
                runtime_args.update(
                    {name: resolved[name] for name in ("checkpoint_dir", "output_path", "quarantine_path")}
                )
                runtime_args["output_mode"] = resolved["configuration"]["output_mode"]
                source_args = {"kafka_source": resolved["kafka_source"], "audit_path": resolved["audit_path"]}
                runtime_args["negative_threshold"] = resolved["configuration"]["negative_threshold"]
            if owner is not None:
                validate_stream_binding(binding, dict(os.environ), runtime_args)
                from operations.migration import read_kafka_cluster_id
                from serving.runtime import stream_configuration

                store.register_stream(
                    args.generation,
                    stream_configuration(
                        window_seconds=simulator_duration(args.window),
                        watermark_seconds=simulator_duration(args.watermark, allow_zero=True),
                        topic=args.topic or settings.kafka.topic_reviews,
                        paths=runtime_args,
                        kafka_cluster_id=read_kafka_cluster_id(
                            resolved["kafka_source"]["bootstrap_servers"] if resolved else settings.kafka.bootstrap_servers
                        ),
                        output_mode=runtime_args.get("output_mode", "update"),
                        negative_threshold=runtime_args.get(
                            "negative_threshold", settings.analytics.sentiment_negative_threshold),
                        **source_args,
                    ),
                )
                stream_registered = True
            job = SparkStreamingJob(**runtime_args)
            print("[Spark Streaming] Initializing streaming query from Kafka...")
            query = job.start_review_stream(
                window_duration=args.window,
                watermark_delay=args.watermark,
                topics=args.topic,
                **source_args,
            )
            from operations.status import OperationStatus
            from spark.streaming.liveness import QueryHeartbeats

            listener = QueryHeartbeats(q.runId for q in job.queries if q is not None)
            job.spark.streams.addListener(listener)
            last_heartbeat = None
            print("[Spark Streaming] Query active. Awaiting termination...")
            try:
                # Monitor every sink; a failed quarantine/audit must stop serving.
                import time

                while any(q is not None and q.isActive for q in job.queries):
                    job.observe_progress()
                    heartbeat = listener.frontier()
                    if heartbeat is not None and heartbeat != last_heartbeat:
                        with OperationStatus(mode=settings.runtime_mode) as status:
                            status.set_gauge("stream_heartbeat_timestamp", heartbeat, "streaming", "system")
                        last_heartbeat = heartbeat
                    for active in job.queries:
                        if active is not None and active.exception():
                            raise RuntimeError(str(active.exception()))
                    time.sleep(1)
                job.observe_progress()
                for active in job.queries:
                    if active is not None and active.exception():
                        raise RuntimeError(str(active.exception()))
            finally:
                try:
                    job.stop()
                finally:
                    job.spark.streams.removeListener(listener)
        except Exception as e:
            failed = True
            try:
                from operations.status import OperationStatus

                with OperationStatus(mode=settings.runtime_mode) as status:
                    status.stream_error()
            except Exception:
                pass  # Preserve the original query failure if telemetry SQL is down.
            print(f"[Spark Streaming Error] Could not start streaming query: {e}")
            sys.exit(1)
        finally:
            if owner is not None:
                try:
                    owner.rollback()
                    if stream_registered:
                        with writer_transaction(owner):
                            store.execute(
                                "UPDATE stream_deployments SET state=? WHERE dataset_id=? AND generation=?",
                                ("failed" if failed else "stopped", binding.id, args.generation),
                            )
                finally:
                    try:
                        if owner_owned:
                            owner_guard.__exit__(None, None, None)
                    finally:
                        owner.close()
    else:
        print("[Simulator] Running StreamingJob micro-batch simulator...")
        from spark.streaming.streaming_job import StreamingJob
        from data.generator import generate_products, generate_streaming_events

        products = generate_products(seed=args.seed)
        events = generate_streaming_events(products, count=50, seed=args.seed)

        job = StreamingJob(window_seconds=window_seconds, watermark_seconds=watermark_seconds)
        result = job.process_micro_batch(events)
        print(f"[Simulator Result] Micro-batch processed: {result}")


if __name__ == "__main__":
    main()
