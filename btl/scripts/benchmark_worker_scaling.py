"""Compare one/two real standalone workers using isolated full-path ETL cohorts.

Freeze a source snapshot before running so concurrent repository edits cannot
silently pool different implementations. Generated workloads remain labelled.
"""

import argparse, hashlib, json, os, shutil, subprocess, sys, time, threading
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts.verify_native_spark import isolated_cluster
from scripts.benchmark_operations import summarize


def freeze_source(snapshot):
    snapshot = Path(snapshot)
    snapshot.mkdir(parents=True, exist_ok=False)
    files = (
        subprocess.check_output(
            ["git", "ls-files", "--cached", "--others", "--exclude-standard", "-z", "--", "."], cwd=ROOT
        )
        .decode()
        .split("\0")
    )
    inventory = {}
    for name in sorted(set(files)):
        path = ROOT / name
        relative = Path(name)
        if (
            not path.is_file()
            or path.suffix not in {".py", ".json", ".sql", ".toml", ".txt", ".yml", ".yaml", ".sh", ".md"}
            or any(
                part in {"reports", ".git", ".runtime", "datalake", "captures", "minio_root"} for part in relative.parts
            )
        ):
            continue
        target = snapshot / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(path, target)
        inventory[name] = hashlib.sha256(path.read_bytes()).hexdigest()
    return inventory


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--spark-home", type=Path, required=True)
    parser.add_argument("--work", type=Path, required=True)
    parser.add_argument("--environment", type=Path, required=True)
    args = parser.parse_args()
    if args.work.exists():
        raise FileExistsError("Fresh benchmark directory required")
    args.work.mkdir(parents=True, mode=0o700)
    work = args.work.resolve()
    snapshot = work / "source"
    inventory = freeze_source(snapshot)
    (work / "source-manifest.json").write_text(json.dumps(inventory, indent=2))
    import shlex

    environment = dict(os.environ)
    for line in args.environment.read_text().splitlines():
        if not line.strip() or line.startswith("#"):
            continue
        key, value = line.split("=", 1)
        tokens = shlex.split(value)
        environment[key] = tokens[0] if tokens else ""
    environment.update(
        SPARK_CONNECTOR_PACKAGES="org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-aws:3.3.4",
        SPARK_LOCAL_IP="127.0.0.1",
        PYSPARK_PYTHON=sys.executable,
        PYSPARK_DRIVER_PYTHON=sys.executable,
        PYTHONPATH=str(snapshot),
    )
    summaries = []
    with isolated_cluster(args.spark_home, work / "cluster", rpc_port=47077, ui_port=48080) as cluster:
        for workers in (1, 2):
            cluster.scale(workers)
            environment["SPARK_MASTER"] = cluster.url
            environment["PYSPARK_SUBMIT_ARGS"] = (
                "--conf spark.executor.cores=2 --conf spark.cores.max="
                + str(workers * 2)
                + " --executor-memory 1g pyspark-shell"
            )
            for size in (10000, 100000):
                reports = []
                for trial in range(1, 4):
                    prefix = f"w{workers}-n{size}-trial{trial}"
                    path = work / (prefix + ".json")
                    log = work / (prefix + ".log")
                    seen = {}
                    stop = threading.Event()

                    def observe():
                        while not stop.wait(0.5):
                            try:
                                for row in cluster.snapshot().get("workers", []):
                                    if row.get("state") == "ALIVE" and row.get("coresused", 0) > 0:
                                        seen[row["id"]] = {
                                            "host": row["host"],
                                            "port": row["port"],
                                            "cores": row["coresused"],
                                        }
                            except OSError:
                                pass

                    thread = threading.Thread(target=observe, daemon=True)
                    thread.start()
                    try:
                        with log.open("x") as stream:
                            subprocess.run(
                                [
                                    sys.executable,
                                    str(snapshot / "scripts/benchmark_fullpath.py"),
                                    "--reviews",
                                    str(size),
                                    "--output",
                                    str(path),
                                ],
                                cwd=snapshot,
                                env=environment,
                                stdout=stream,
                                stderr=subprocess.STDOUT,
                                check=True,
                                timeout=3600,
                            )
                    finally:
                        stop.set()
                        thread.join(timeout=5)
                    report = json.loads(path.read_text())
                    if not report.get("completed") or len(seen) != workers:
                        raise RuntimeError("Full path or actual worker allocation not verified")
                    with (work / (prefix + "-workers.json")).open("x") as stream:
                        json.dump({"workers": workers, "observed_allocations": seen}, stream, indent=2)
                    reports.append(report)
                    print(prefix, "completed", flush=True)
                result = summarize(reports)
                result.update(workers=workers, cores=workers * 2, review_inputs=size)
                summaries.append(result)
    report = {
        "completed": True,
        "cohorts": summaries,
        "trials": 12,
        "source_snapshot_inventory": str(work / "source-manifest.json"),
        "boundary": "Actual standalone workers on one host, generated labelled 10k/100k full Kafka/MinIO/Spark/PostgreSQL/DashboardDB batch cohorts; not Docker throughput, per-event streaming latency or multi-host HA",
    }
    with (work / "summary.json").open("x") as stream:
        json.dump(report, stream, indent=2)
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
