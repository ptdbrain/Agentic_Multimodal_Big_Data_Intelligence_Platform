"""Read genuine scheduled Airflow cycles, hashed receipts and sampled monitoring history."""

import argparse, hashlib, json, math, os, sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import urlopen

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


def assess(jobs, start, now, runs, monitoring_coverage):
    elapsed = (now - start).total_seconds()
    counts = {
        job: len({
            row["run_id"] for row in runs
            if row["job"] == job and row["run_type"] == "scheduled" and row.get("run_id")
            and row["state"] == "success" and row["receipt_verified"]
        })
        for job in jobs
    }
    failures = [row for row in runs if row["run_type"] == "scheduled" and row["state"] == "failed"]
    partial = [row for row in runs if row["run_type"] == "scheduled" and row.get("coverage") == "partial"]
    unverified = [
        row for row in runs
        if row["run_type"] == "scheduled" and row["state"] == "success" and not row["receipt_verified"]
    ]
    operational = (
        elapsed >= 72 * 3600
        and bool(counts)
        and all(count >= 3 for count in counts.values())
        and not failures
        and not unverified
        and monitoring_coverage >= 0.99
    )
    return {
        "accepted": operational and not partial,
        "operational_acceptance": operational,
        "elapsed_hours": elapsed / 3600,
        "earliest_review_at": (start + timedelta(hours=72)).isoformat(),
        "scheduled_cycles_verified": counts,
        "failed_scheduled_runs": len(failures),
        "unverified_successful_scheduled_runs": len(unverified),
        "partial_scheduled_runs": len(partial),
        "monitoring_sample_coverage": monitoring_coverage,
        "required_monitoring_sample_coverage": 0.99,
        "boundary": "At least72 real elapsed hours and3successful scheduled cycles per source, verified durable receipts, >=99% observed up samples, no failed runs. Partial collection remains a separate acceptance gap. Manual runs never count; sampled availability is not proof of multi-host HA.",
    }


def expected_jobs(config_path: Path) -> list[str]:
    import yaml

    jobs = [entry["job_name"] for entry in yaml.safe_load(config_path.read_text())["scrape_configs"]]
    if not jobs or len(set(jobs)) != len(jobs):
        raise ValueError("Monitoring configuration requires distinct scrape jobs")
    return jobs


def monitoring_coverage(jobs: list[str], series: list[dict], expected: int) -> dict[str, float]:
    observed = {job: set() for job in jobs}
    for item in series:
        job = item["metric"]["job"]
        if job in observed:
            observed[job].update(t for t, value in item["values"] if float(value) == 1)
    return {job: len(times) / expected for job, times in observed.items()}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--window", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--prometheus", default="http://127.0.0.1:9090")
    default_config = "prometheus-application.yml" if os.environ.get("RUNNING_IN_DOCKER") == "true" else "prometheus-operations.yml"
    parser.add_argument("--prometheus-config", type=Path, default=ROOT / "monitoring" / default_config,
                        help="Scrape configuration defining every required monitoring job")
    args = parser.parse_args()
    import yaml
    from airflow.models import DagRun, TaskInstance
    from airflow.utils.session import create_session
    from orchestration.source_jobs import validate_config, write_atomic

    config = validate_config(yaml.safe_load(args.config.read_text()))
    start = datetime.fromisoformat(json.loads(args.window.read_text())["stability_window_started_at"])
    if start.tzinfo is None:
        raise ValueError("Explicit stability clock timezone required")
    now = datetime.now(timezone.utc)
    if start > now:
        raise ValueError("Stability window cannot start in the future")
    runs = []
    jobs = [job for job in config["jobs"] if job["owner"] == "airflow"]
    with create_session() as session:
        for job in jobs:
            dag = "sentinel_source_" + job["id"]
            for run in session.query(DagRun).filter(DagRun.dag_id == dag, DagRun.start_date >= start).all():
                run_type = str(getattr(run.run_type, "value", run.run_type))
                identity = job["id"] + "-" + hashlib.sha256(run.run_id.encode()).hexdigest()[:32]
                folder = Path(config["root"]) / job["id"] / identity
                path = folder / "manifest.json"
                receipt = json.loads(path.read_text()) if path.exists() else {}
                states = {
                    task.task_id: task.state
                    for task in session.query(TaskInstance)
                    .filter(TaskInstance.dag_id == dag, TaskInstance.run_id == run.run_id)
                    .all()
                }
                payload = folder / "capture.json"
                verified = (
                    receipt.get("run_id") == identity
                    and receipt.get("scheduler") == "airflow"
                    and receipt.get("source_group") == job["source_group"]
                    and receipt.get("state") in {"complete", "partial"}
                    and receipt.get("job_id") == job["id"]
                    and receipt.get("namespace") == job["namespace"]
                    and receipt.get("processing_state") == "complete"
                    and receipt.get("processed_at") is not None
                    and receipt.get("input_records", 0) >= job["minimum_records"]
                    and all(states.get(task) == "success" for task in ("capture", "process"))
                    and payload.is_file()
                    and hashlib.sha256(payload.read_bytes()).hexdigest() == receipt.get("capture_sha256")
                )
                runs.append(
                    {
                        "job": job["id"],
                        "run_id": run.run_id,
                        "run_type": run_type,
                        "state": run.state,
                        "receipt_verified": verified,
                        "coverage": receipt.get("state"),
                    }
                )
    end = now.timestamp()
    begin = start.timestamp()
    step = 60
    required = expected_jobs(args.prometheus_config)
    # Limit lookback to one sample interval. A lost exporter must not keep
    # earning availability from Prometheus's default five-minute lookback.
    query = urlencode({"query": "min by (job) (min_over_time(up[60s]))", "start": begin, "end": end, "step": step})
    with urlopen(args.prometheus + "/api/v1/query_range?" + query, timeout=20) as response:
        series = json.load(response)["data"]["result"]
    expected = math.floor((end - begin) / step) + 1
    coverage = monitoring_coverage(required, series, expected)
    report = assess([job["id"] for job in jobs], start, now, runs, min(coverage.values()))
    report["monitoring_coverage_by_job"] = coverage
    report["checked_at"] = now.isoformat()
    report["runs"] = runs
    write_atomic(args.output, report)
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
