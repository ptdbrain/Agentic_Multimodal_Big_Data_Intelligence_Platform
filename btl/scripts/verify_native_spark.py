"""Owned loopback Spark workers: actual task-loss recovery, never serving or HA proof."""

from contextlib import contextmanager
import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import sys
import time
from urllib.request import urlopen

ROOT = Path(__file__).resolve().parents[1]


def verified_workers(snapshot, ports):
    workers = [row for row in snapshot.get("workers", []) if row.get("state") == "ALIVE"]
    if len(workers) != len(ports) or len({row["id"] for row in workers}) != len(workers):
        raise ValueError("Expected distinct live Spark workers")
    if {row.get("port") for row in workers} != set(ports) or any(row.get("host") != "127.0.0.1" for row in workers):
        raise ValueError("Spark workers do not belong to the isolated loopback ports")
    return workers


def verified_task_retries(records):
    attempts = {}
    for row in records:
        key = (row["stage_id"], row["partition_id"])
        attempts.setdefault(key, set()).add(row["task_attempt_id"])
    retried = sorted(key for key, values in attempts.items() if len(values) > 1)
    if not retried:
        raise ValueError("No actual task reexecution observed")
    return retried


def _write(path, value):
    with Path(path).open("x") as stream:
        json.dump(value, stream, indent=2, allow_nan=False)
        stream.flush()
        os.fsync(stream.fileno())


class NativeCluster:
    def __init__(self, spark_home, work, rpc_port=37077, ui_port=38080):
        self.spark_home = Path(spark_home).resolve()
        self.work = Path(work).resolve()
        self.rpc_port = rpc_port
        self.ui_port = ui_port
        self.processes = {}
        self.logs = []
        self.environment = dict(
            os.environ,
            SPARK_HOME=str(self.spark_home),
            SPARK_LOCAL_IP="127.0.0.1",
            SPARK_DAEMON_MEMORY="512m",
            SPARK_WORKER_MEMORY="2g",
            SPARK_PUBLIC_DNS="127.0.0.1",
        )
        self.url = f"spark://127.0.0.1:{rpc_port}"

    def start(self):
        if not (self.spark_home / "bin/spark-class").is_file():
            raise ValueError("Official Spark distribution required")
        if self.work.exists():
            raise ValueError("Fresh isolated work directory required")
        for port in [self.rpc_port + i for i in range(3)] + [self.ui_port + i for i in range(3)]:
            with socket.socket() as handle:
                handle.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
                handle.bind(("127.0.0.1", port))
        self.work.mkdir(parents=True, mode=0o700)
        self.spawn(
            "master",
            [
                "org.apache.spark.deploy.master.Master",
                "--host",
                "127.0.0.1",
                "--port",
                str(self.rpc_port),
                "--webui-port",
                str(self.ui_port),
            ],
        )
        self.wait(lambda: bool(self.snapshot()))
        self.scale(2)
        return self

    def spawn(self, name, args):
        log = (self.work / (name + ".log")).open("a")
        self.logs.append(log)
        self.processes[name] = subprocess.Popen(
            [str(self.spark_home / "bin/spark-class"), *args],
            cwd=ROOT,
            env=self.environment,
            stdin=subprocess.DEVNULL,
            stdout=log,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )

    def snapshot(self):
        with urlopen(f"http://127.0.0.1:{self.ui_port}/json/", timeout=3) as response:
            return json.load(response)

    def wait(self, condition, timeout=90):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if any(process.poll() is not None for process in self.processes.values()):
                raise RuntimeError("An owned Spark daemon exited; inspect its preserved log")
            try:
                result = condition()
                if result:
                    return result
            except (OSError, ValueError):
                pass
            time.sleep(0.2)
        raise TimeoutError("Owned Spark cluster did not reach the requested state")

    def stop_process(self, name):
        process = self.processes.pop(name, None)
        if process is None:
            return
        if process.poll() is None:
            os.killpg(process.pid, signal.SIGTERM)
            try:
                process.wait(timeout=15)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait(timeout=10)

    def scale(self, count):
        if count not in (1, 2):
            raise ValueError("Use one or two owned workers")
        for number in (1, 2):
            name = f"worker{number}"
            if number > count:
                self.stop_process(name)
                continue
            if name not in self.processes:
                self.spawn(
                    name,
                    [
                        "org.apache.spark.deploy.worker.Worker",
                        "--host",
                        "127.0.0.1",
                        "--port",
                        str(self.rpc_port + number),
                        "--webui-port",
                        str(self.ui_port + number),
                        "--cores",
                        "2",
                        "--memory",
                        "2g",
                        "--work-dir",
                        str(self.work / name),
                        self.url,
                    ],
                )
        return self.wait(lambda: verified_workers(self.snapshot(), {self.rpc_port + i for i in range(1, count + 1)}))

    def close(self):
        for name in list(reversed(self.processes)):
            self.stop_process(name)
        for log in self.logs:
            log.close()


@contextmanager
def isolated_cluster(spark_home, work, **kwargs):
    cluster = NativeCluster(spark_home, work, **kwargs)
    try:
        yield cluster.start()
    finally:
        cluster.close()


def workload(master, work, rows, output, driver_host="127.0.0.1", driver_bind="127.0.0.1"):
    from pyspark import TaskContext
    from pyspark.sql import SparkSession

    work = Path(work)
    ready = work / "ready"
    ready.mkdir(exist_ok=True)
    release = str(work / "release")
    spark = (
        SparkSession.builder.master(master)
        .appName("sentinel-owned-worker-fault")
        .config("spark.driver.host", driver_host)
        .config("spark.driver.bindAddress", driver_bind)
        .config("spark.executor.cores", "2")
        .config("spark.cores.max", "4")
        .config("spark.executor.memory", "1g")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )

    def gate(iterator):
        context = TaskContext.get()
        attempt = context.attemptNumber()
        evidence = {
            "stage_id": context.stageId(),
            "partition_id": context.partitionId(),
            "task_attempt_id": context.taskAttemptId(),
            "attempt_number": attempt,
            "pid": os.getpid(),
        }
        Path(
            str(ready), f"{context.stageId()}-{context.partitionId()}-{context.taskAttemptId()}-{os.getpid()}.json"
        ).write_text(json.dumps(evidence))
        deadline = time.monotonic() + 180
        while not Path(release).exists():
            if time.monotonic() > deadline:
                raise TimeoutError("Fault controller did not release the workload")
            time.sleep(0.05)
        for row in iterator:
            yield (row.id, attempt)

    try:
        started = time.monotonic()
        values = spark.range(rows, numPartitions=32).repartition(32).rdd.mapPartitions(gate).collect()
        identities = sorted(row[0] for row in values)
        expected = hashlib.sha256("\n".join(str(index) for index in range(rows)).encode()).hexdigest()
        actual = hashlib.sha256("\n".join(str(index) for index in identities).encode()).hexdigest()
        if actual != expected or len(set(identities)) != rows:
            raise RuntimeError("Spark task recovery changed output identities")
        _write(
            output,
            {
                "rows": rows,
                "unique": len(set(identities)),
                "business_sha256": actual,
                "max_task_attempt": max(row[1] for row in values),
                "seconds_including_fault_barrier": time.monotonic() - started,
            },
        )
    finally:
        spark.stop()


def worker_fault(spark_home, work, rows=100000):
    with isolated_cluster(spark_home, work) as cluster:
        control = cluster.work / "control"
        control.mkdir()
        output = control / "result.json"
        log = (control / "driver.log").open("x")
        environment = dict(cluster.environment, PYSPARK_PYTHON=sys.executable, PYSPARK_DRIVER_PYTHON=sys.executable)
        command = [
            sys.executable,
            str(Path(__file__).resolve()),
            "--action",
            "workload",
            "--master",
            cluster.url,
            "--work",
            str(control),
            "--rows",
            str(rows),
            "--output",
            str(output),
        ]
        driver = subprocess.Popen(
            command, cwd=ROOT, env=environment, stdout=log, stderr=subprocess.STDOUT, start_new_session=True
        )
        try:

            def tasks_active():
                if driver.poll() is not None:
                    raise RuntimeError("Workload ended before fault injection")
                snapshot = cluster.snapshot()
                workers = verified_workers(snapshot, {cluster.rpc_port + 1, cluster.rpc_port + 2})
                if len(list((control / "ready").glob("*"))) >= 4 and all(
                    row.get("coresused", 0) == 2 for row in workers
                ):
                    return snapshot
                return None

            before = cluster.wait(tasks_active, timeout=120)
            fault_at = datetime.now(timezone.utc).isoformat()
            stopped_pid = cluster.processes["worker1"].pid
            cluster.stop_process("worker1")
            (control / "release").write_text(fault_at)
            driver.wait(timeout=240)
            if driver.returncode:
                raise RuntimeError("Real Spark workload failed after worker loss; logs retained")
            result = json.loads(output.read_text())
            markers = [json.loads(path.read_text()) for path in (control / "ready").glob("*.json")]
            retried = verified_task_retries(markers)
            if "Lost executor" not in (control / "driver.log").read_text():
                raise RuntimeError("Driver did not observe an executor loss")
            restored = cluster.scale(2)
            report = dict(
                result,
                passed=True,
                retried_partitions=retried,
                source_group="simulation",
                acquisition_method="generated",
                fault_at=fault_at,
                stopped_owned_worker_pid=stopped_pid,
                workers_before=before["workers"],
                workers_after=restored,
                boundary="Two real single-host Spark standalone workers; one owned worker/executor process group lost during blocked tasks; exact shuffled ID reconciliation and observed task retries. Not ETL throughput, Docker, host loss or multi-host HA.",
            )
            _write(cluster.work / "verification.json", report)
            return report
        finally:
            if driver.poll() is None:
                os.killpg(driver.pid, signal.SIGTERM)
                try:
                    driver.wait(timeout=15)
                except subprocess.TimeoutExpired:
                    os.killpg(driver.pid, signal.SIGKILL)
                    driver.wait(timeout=10)
            log.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--action", choices=["fault", "workload"], default="fault")
    parser.add_argument("--spark-home", type=Path)
    parser.add_argument("--work", type=Path, required=True)
    parser.add_argument("--rows", type=int, default=100000)
    parser.add_argument("--master")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if not 100 <= args.rows <= 1000000:
        parser.error("Rows must be between 100 and 1000000")
    if args.action == "workload":
        if not args.master or not args.output:
            parser.error("Workload requires explicit master/output")
        workload(args.master, args.work, args.rows, args.output)
    else:
        if not args.spark_home:
            parser.error("Fault drill requires --spark-home")
        print(json.dumps(worker_fault(args.spark_home, args.work, args.rows), indent=2))


if __name__ == "__main__":
    main()
