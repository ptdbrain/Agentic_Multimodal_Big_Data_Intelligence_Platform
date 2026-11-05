"""Isolated native three-broker fault drill; no existing data/process is touched."""

import argparse
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import time
import uuid

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


def free_ports(ports):
    handles = []
    try:
        for port in ports:
            handle = socket.socket()
            handles.append(handle)
            handle.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            handle.bind(("127.0.0.1", port))
    finally:
        for handle in handles:
            handle.close()


def replicated(metadata):
    partitions = metadata.get("partitions", [])
    return (
        not metadata.get("error_code", 0)
        and len(partitions) == 3
        and all(len(p.get("isr", p.get("isr_nodes", []))) == 3 for p in partitions)
    )


def kafka_drill(binary, work, rows=200, base_port=39094):
    from kafka.admin import KafkaAdminClient, NewTopic
    from kafka import KafkaProducer, KafkaConsumer

    binary = Path(binary).resolve()
    work = Path(work).resolve()
    if rows < 2 or rows > 10000:
        raise ValueError("Rows must be between2 and10000")
    if work.exists():
        raise ValueError("Work directory must be new")
    if not 1024 <= base_port <= 65000:
        raise ValueError("Invalid port base")
    ports = list(range(base_port, base_port + 3))
    controllers = list(range(base_port + 100, base_port + 103))
    free_ports(ports + controllers)
    if not (binary / "kafka-server-start.sh").is_file():
        raise ValueError("Official Kafka bin directory required")
    work.mkdir(parents=True, mode=0o700)
    environment = dict(os.environ, KAFKA_HEAP_OPTS="-Xms256m -Xmx512m")
    processes = []
    logs = []
    admin = producer = consumer = None

    def start(index):
        log = (work / f"broker{index + 1}.log").open("a")
        logs.append(log)
        process = subprocess.Popen(
            [str(binary / "kafka-server-start.sh"), str(work / f"broker{index + 1}.properties")],
            env=environment,
            stdout=log,
            stderr=subprocess.STDOUT,
            stdin=subprocess.DEVNULL,
            start_new_session=True,
        )
        return process

    try:
        cluster = subprocess.check_output(
            [str(binary / "kafka-storage.sh"), "random-uuid"], env=environment, text=True
        ).strip()
        voters = ",".join(f"{i + 1}@127.0.0.1:{port}" for i, port in enumerate(controllers))
        for i, port in enumerate(ports):
            config = work / f"broker{i + 1}.properties"
            config.write_text(f"""process.roles=broker,controller
node.id={i + 1}
controller.quorum.voters={voters}
listeners=PLAINTEXT://127.0.0.1:{port},CONTROLLER://127.0.0.1:{controllers[i]}
advertised.listeners=PLAINTEXT://127.0.0.1:{port}
controller.listener.names=CONTROLLER
listener.security.protocol.map=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT
inter.broker.listener.name=PLAINTEXT
log.dirs={work / f"data{i + 1}"}
offsets.topic.replication.factor=3
transaction.state.log.replication.factor=3
transaction.state.log.min.isr=2
min.insync.replicas=2
default.replication.factor=3
auto.create.topics.enable=false
group.initial.rebalance.delay.ms=0
""")
            subprocess.run(
                [str(binary / "kafka-storage.sh"), "format", "-t", cluster, "-c", str(config)],
                env=environment,
                check=True,
                stdout=subprocess.DEVNULL,
            )
            processes.append(start(i))
        deadline = time.monotonic() + 90
        while time.monotonic() < deadline:
            try:
                admin = KafkaAdminClient(
                    bootstrap_servers=[f"127.0.0.1:{p}" for p in ports],
                    request_timeout_ms=10000,
                    bootstrap_timeout_ms=10000,
                )
                if len(admin.describe_cluster()["brokers"]) == 3:
                    break
                admin.close()
                admin = None
            except Exception:
                time.sleep(1)
        if admin is None:
            raise RuntimeError("Three broker startup timeout")
        topic = "ops-native-" + uuid.uuid4().hex[:12]
        from kafka.errors import UnknownBrokerIdError, NotControllerError, KafkaConnectionError, TopicAlreadyExistsError

        deadline = time.monotonic() + 30
        while True:
            try:
                admin.list_topics()
                admin.create_topics([NewTopic(topic, 3, 3, topic_configs={"min.insync.replicas": "2"})])
                break
            except TopicAlreadyExistsError:
                break
            except (UnknownBrokerIdError, NotControllerError, KafkaConnectionError):
                if time.monotonic() >= deadline:
                    raise
                admin.close()
                time.sleep(1)
                admin = KafkaAdminClient(bootstrap_servers=[f"127.0.0.1:{p}" for p in ports])
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            meta = admin.describe_topics([topic])[0]
            if replicated(meta):
                break
            time.sleep(0.5)
        else:
            raise RuntimeError("ISR3 readiness timeout")
        producer = KafkaProducer(
            bootstrap_servers=[f"127.0.0.1:{p}" for p in ports],
            acks="all",
            retries=10,
            max_block_ms=30000,
            request_timeout_ms=10000,
        )
        expected = [
            json.dumps(
                {"id": i, "source_group": "simulation", "acquisition_method": "generated"}, sort_keys=True
            ).encode()
            for i in range(rows)
        ]
        started = time.monotonic()
        for value in expected[: rows // 2]:
            producer.send(topic, value=value).get(timeout=45)
        victim = meta["partitions"][0].get("leader", meta["partitions"][0].get("leader_id", -1)) - 1
        processes[victim].terminate()
        processes[victim].wait(timeout=30)
        outage = time.monotonic()
        for value in expected[rows // 2 :]:
            producer.send(topic, value=value).get(timeout=45)
        write_recovery = time.monotonic() - outage
        processes[victim] = start(victim)
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=[f"127.0.0.1:{p}" for p in ports],
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            group_id="ops-native-reader-" + uuid.uuid4().hex,
        )
        actual = []
        deadline = time.monotonic() + 60
        while len(set(actual)) < len(expected) and time.monotonic() < deadline:
            for batch in consumer.poll(timeout_ms=1000).values():
                actual.extend(row.value for row in batch)
        if set(actual) != set(expected):
            raise RuntimeError("Acknowledged payload loss or substitution")
        report = {
            "passed": True,
            "boundary": "three native Kafka3.7 brokers on one host; one broker/controller process stopped and restarted; fixture payloads only",
            "acknowledged": rows,
            "received": len(actual),
            "unique": len(set(actual)),
            "replication_factor": 3,
            "minimum_isr": 2,
            "write_recovery_seconds": write_recovery,
            "total_seconds": time.monotonic() - started,
            "topic": topic,
            "whole_host_failure_certified": False,
        }
        (work / "report.json").write_text(json.dumps(report, indent=2) + "\n")
        return report
    finally:
        for client in (consumer, producer, admin):
            if client:
                try:
                    client.close()
                except Exception:
                    pass
        for process in processes:
            if process.poll() is None:
                process.terminate()
        for process in processes:
            try:
                process.wait(timeout=30)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
        for log in logs:
            log.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--kafka-bin", type=Path, required=True)
    parser.add_argument("--work-dir", type=Path, required=True)
    parser.add_argument("--rows", type=int, default=200)
    parser.add_argument("--base-port", type=int, default=39094)
    args = parser.parse_args()
    print(json.dumps(kafka_drill(args.kafka_bin, args.work_dir, args.rows, args.base_port), indent=2))


if __name__ == "__main__":
    main()
