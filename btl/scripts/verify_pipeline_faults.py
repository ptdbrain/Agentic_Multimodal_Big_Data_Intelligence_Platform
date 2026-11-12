"""Actual owned Docker storage/consumer outage and killed Spark driver recovery."""

import argparse, hashlib, json, os, signal, socket, subprocess, sys, time, uuid
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


def payload(index, clock):
    return {
        "review_id": "fault-" + str(index),
        "product_id": "fault-fixture",
        "user_id": "anon_redacted",
        "rating": 1 + index % 5,
        "review_text": "Explicit generated outage fixture",
        "review_title": "Simulation",
        "review_date": clock,
        "source_group": "simulation",
        "source_name": "outage_fixture",
        "acquisition_method": "generated",
        "generator_seed": "42",
        "generator_config": '{"purpose":"fault_recovery"}',
    }


def driver(config_path, expected):
    from config.settings import settings
    from storage.storage_manager import storage
    from spark.streaming.spark_streaming_job import SparkStreamingJob

    config = json.loads(Path(config_path).read_text())
    generation = Path(config["generation"])
    settings.set_runtime_mode("test")
    settings.database.sqlite_path = generation / "status.sqlite"
    settings.storage.local_data_dir = generation / "local"
    storage.set_mode("test", base_dir=settings.storage.local_data_dir)
    settings.kafka.bootstrap_servers = ",".join(config["servers"])
    job = SparkStreamingJob(
        checkpoint_dir=str(generation / "checkpoint"),
        output_path=str(generation / "metrics"),
        quarantine_path=str(generation / "quarantine"),
    )
    try:
        job.start_review_stream(
            topics=config["topic"], processing_time="1 second", trigger_available_now=expected is not None
        )
        if expected is not None:
            job.await_termination(120)
            if any(q is not None and (q.isActive or q.exception()) for q in job.queries):
                raise RuntimeError("Restarted query did not commit")
            rows = job.spark.read.parquet(str(generation / "metrics")).collect()
            count = sum(row.review_count for row in rows)
            if count != expected:
                raise ValueError("Restarted driver count differs")
            (generation / "resumed.json").write_text(
                json.dumps({"review_count": count, "source_group": sorted({row.source_group for row in rows})})
            )
        else:
            deadline = time.monotonic() + 120
            while time.monotonic() < deadline:
                job.observe_progress()
                for query in job.queries:
                    if query is not None and query.exception():
                        raise RuntimeError(str(query.exception()))
                points = [query.lastProgress for query in job.queries if query is not None]
                if points and all(
                    point and any(source.get("endOffset") for source in point.get("sources", [])) for point in points
                ):
                    count = sum(
                        row.review_count for row in job.spark.read.parquet(str(generation / "metrics")).collect()
                    )
                    if count == 2:
                        (generation / "ready.json").write_text(json.dumps({"review_count": 2, "pid": os.getpid()}))
                        while True:
                            time.sleep(1)
                time.sleep(0.5)
            raise TimeoutError("Initial driver did not reach durable frontier")
    finally:
        job.stop()
        job.spark.stop()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project", default="sentinel-resilience-de-20261112")
    parser.add_argument("--minio-binary", type=Path)
    parser.add_argument("--work", type=Path)
    parser.add_argument("--driver-config", type=Path)
    parser.add_argument("--expected", type=int)
    args = parser.parse_args()
    if args.driver_config:
        driver(args.driver_config, args.expected)
        return
    if not args.work or not args.minio_binary:
        parser.error("Work directory and vendor object-store binary required")
    from scripts.verify_operations_stack import _container

    for service in ("kafka1", "kafka2", "kafka3"):
        _container(args.project, service)
    if args.work.exists():
        raise FileExistsError("Fresh fault drill namespace required")
    work = args.work.resolve()
    work.mkdir(parents=True, mode=0o700)
    with socket.socket() as probe:
        probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        probe.bind(("127.0.0.1", 39000))
    from kafka import KafkaProducer
    from kafka.admin import KafkaAdminClient, NewTopic
    from kafka.structs import TopicPartition
    from minio import Minio
    from config.settings import settings
    from storage.storage_manager import storage
    from storage.minio_backend import MinIOBackend
    from kafka.consumers.raw_consumer import RawConsumer

    nonce = uuid.uuid4().hex[:10]
    servers = ["127.0.0.1:39094", "127.0.0.1:39095", "127.0.0.1:39096"]
    consumer_topic = "de.fault.consumer." + nonce
    stream_topic = "de.fault.stream." + nonce
    admin = KafkaAdminClient(bootstrap_servers=servers)
    admin.create_topics(
        [NewTopic(name, 1, 3, topic_configs={"min.insync.replicas": "2"}) for name in (consumer_topic, stream_topic)]
    )
    producer = KafkaProducer(bootstrap_servers=servers, acks="all")
    clock = datetime.now(timezone.utc).replace(second=10, microsecond=0).isoformat()

    def send(topic, index):
        value = {
            "event_id": "fault-event-" + str(index),
            "event_type": "NEW_REVIEW",
            "source": "simulation",
            "ingested_at": datetime.now(timezone.utc).isoformat(),
            "payload": payload(index, clock),
        }
        producer.send(topic, json.dumps(value).encode()).get(timeout=20)
        producer.flush()

    container = "sentinel-fault-minio-" + nonce
    volume = "sentinel-fault-minio-" + nonce
    subprocess.run(
        ["docker", "volume", "create", "--label", "sentinel.drill=true", volume], check=True, capture_output=True
    )
    key = "drill-" + uuid.uuid4().hex
    subprocess.run(
        [
            "docker",
            "run",
            "-d",
            "--name",
            container,
            "--label",
            "com.docker.compose.project=" + args.project,
            "--label",
            "sentinel.drill=true",
            "--user",
            "0:0",
            "--entrypoint",
            "/vendor/minio",
            "-p",
            "127.0.0.1:39000:9000",
            "-v",
            str(args.minio_binary.resolve()) + ":/vendor/minio:ro",
            "-v",
            volume + ":/data",
            "-e",
            "MINIO_ROOT_USER=sentinel-drill",
            "-e",
            "MINIO_ROOT_PASSWORD=" + key,
            "apache/spark:3.5.0",
            "server",
            "/data",
            "--address",
            ":9000",
        ],
        check=True,
        capture_output=True,
    )
    process = None
    try:
        client = Minio("127.0.0.1:39000", access_key="sentinel-drill", secret_key=key, secure=False)
        deadline = time.monotonic() + 60
        while True:
            try:
                client.make_bucket("fault-drill")
                break
            except Exception:
                if time.monotonic() > deadline:
                    raise
                time.sleep(0.5)
        settings.set_runtime_mode("test")
        settings.database.sqlite_path = work / "fence.sqlite"
        settings.storage.local_data_dir = work / "local"
        storage.set_mode("test", base_dir=settings.storage.local_data_dir)
        settings.kafka.bootstrap_servers = servers
        settings.kafka.topic_reviews = consumer_topic
        backend = MinIOBackend(
            endpoint="127.0.0.1:39000",
            access_key="sentinel-drill",
            secret_key=key,
            bucket="fault-drill",
            secure=False,
            enable_local_mirror=False,
        )
        backend.health_check()
        storage.backend = backend
        # Bind the explicit backend through StorageManager's public property.
        if storage.backend is not backend:
            raise RuntimeError("Fault backend binding differs")
        for index in range(4):
            send(consumer_topic, index)
        group = "de-fault-" + nonce
        subprocess.run(["docker", "stop", "--time", "10", container], check=True, capture_output=True)
        consumer = RawConsumer(bootstrap_servers=servers, group_id=group)
        failure = None
        try:
            consumer.consume_batch_once([consumer_topic], max_records=4, timeout_ms=5000)
        except Exception as error:
            failure = type(error).__name__
        finally:
            consumer.close()
        if failure is None:
            raise RuntimeError("Storage outage unexpectedly accepted")
        committed = admin.list_group_offsets(group)[group]
        if any(point.offset > 0 for point in committed.values()):
            raise RuntimeError("Consumer committed before durable storage")
        subprocess.run(["docker", "start", container], check=True, capture_output=True)
        deadline = time.monotonic() + 60
        while True:
            try:
                client.bucket_exists("fault-drill")
                break
            except Exception:
                if time.monotonic() > deadline:
                    raise
                time.sleep(0.5)
        consumer = RawConsumer(bootstrap_servers=servers, group_id=group)
        try:
            result = consumer.consume_batch_once([consumer_topic], max_records=4, timeout_ms=10000)
        finally:
            consumer.close()
        rows = [row for path in result["batches_written"] for row in storage.read_json(path)]
        if {row["payload"]["review_id"] for row in rows} != {"fault-" + str(index) for index in range(4)}:
            raise RuntimeError("Consumer restart lost payloads")
        position = TopicPartition(consumer_topic, 0)
        if admin.list_group_offsets(group)[group][position].offset != 4:
            raise RuntimeError("Recovered consumer frontier differs")
        for index in (0, 1):
            send(stream_topic, index)
        generation = work / "stream"
        generation.mkdir()
        config = {"generation": str(generation), "servers": servers, "topic": stream_topic}
        path = work / "driver.json"
        path.write_text(json.dumps(config))
        with (work / "driver-before.log").open("x") as log:
            process = subprocess.Popen(
                [sys.executable, str(Path(__file__).resolve()), "--driver-config", str(path)],
                cwd=ROOT,
                env=dict(os.environ, RUNTIME_MODE="test"),
                stdout=log,
                stderr=subprocess.STDOUT,
                start_new_session=True,
            )
        deadline = time.monotonic() + 150
        while not (generation / "ready.json").exists():
            if process.poll() is not None:
                raise RuntimeError("Driver exited before committed baseline")
            if time.monotonic() > deadline:
                raise TimeoutError("Driver readiness not observed")
            time.sleep(0.5)
        os.killpg(process.pid, signal.SIGKILL)
        process.wait(timeout=10)
        process = None
        for index in (2, 3):
            send(stream_topic, index)
        with (work / "driver-after.log").open("x") as log:
            subprocess.run(
                [sys.executable, str(Path(__file__).resolve()), "--driver-config", str(path), "--expected", "4"],
                cwd=ROOT,
                env=dict(os.environ, RUNTIME_MODE="test"),
                stdout=log,
                stderr=subprocess.STDOUT,
                check=True,
                timeout=150,
            )
        report = {
            "passed": True,
            "storage_outage_observed": failure,
            "consumer_rows_recovered": len(rows),
            "consumer_committed_offset": 4,
            "driver_killed_after_committed_rows": 2,
            "driver_resumed_rows": 4,
            "source_group": "simulation",
            "acquisition_method": "generated",
            "boundary": "Real isolated Docker MinIO stop/start, real Kafka consumer commit refusal and restart, actual Spark driver process-group SIGKILL and checkpoint resumption; no production service killed or observed-source records generated",
        }
        with (work / "verification.json").open("x") as out:
            json.dump(report, out, indent=2)
        print(json.dumps(report, indent=2))
    finally:
        if process is not None and process.poll() is None:
            os.killpg(process.pid, signal.SIGKILL)
            process.wait(timeout=10)
        producer.close()
        admin.close()
        subprocess.run(["docker", "stop", "--time", "10", container], capture_output=True)


if __name__ == "__main__":
    main()
