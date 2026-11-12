"""Read genuine scheduled Airflow cycles, hashed receipts and sampled monitoring history."""

import argparse, hashlib, json, math, sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import urlopen

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


def assess(jobs, start, now, runs, monitoring_coverage):
    elapsed = (now - start).total_seconds()
    counts = {
        job: sum(
            row["job"] == job
            and row["run_type"] == "scheduled"
            and row["state"] == "success"
            and row["receipt_verified"]
            for row in runs
        )
        for job in jobs
    }
    failures = [row for row in runs if row["run_type"] == "scheduled" and row["state"] == "failed"]
    partial = [row for row in runs if row["run_type"] == "scheduled" and row.get("coverage") == "partial"]
    operational = (
        elapsed >= 72 * 3600
        and all(count >= 3 for count in counts.values())
        and not failures
        and monitoring_coverage >= 0.99
    )
    return {
        "accepted": operational and not partial,
        "operational_acceptance": operational,
        "elapsed_hours": elapsed / 3600,
        "earliest_review_at": (start + timedelta(hours=72)).isoformat(),
        "scheduled_cycles_verified": counts,
        "failed_scheduled_runs": len(failures),
        "partial_scheduled_runs": len(partial),
        "monitoring_sample_coverage": monitoring_coverage,
        "required_monitoring_sample_coverage": 0.99,
        "boundary": "At least72 real elapsed hours and3successful scheduled cycles per source, verified durable receipts, >=99% observed up samples, no failed runs. Partial collection remains a separate acceptance gap. Manual runs never count; sampled availability is not proof of multi-host HA.",
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--window", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--prometheus", default="http://127.0.0.1:9090")
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
                    receipt.get("job_id") == job["id"]
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
    query = urlencode(
        {"query": 'up{job=~"sentinel-(platform|dependencies|operations)"}', "start": begin, "end": end, "step": step}
    )
    with urlopen(args.prometheus + "/api/v1/query_range?" + query, timeout=20) as response:
        series = json.load(response)["data"]["result"]
    expected = math.floor((end - begin) / step) + 1
    # Missing targets and missing time samples reduce coverage rather than vanishing.
    by_job = {}
    for item in series:
        by_job.setdefault(item["metric"]["job"], set()).update(
            int(float(t)) for t, v in item["values"] if float(v) == 1
        )
    coverage = min(
        (
            len(by_job.get(name, set())) / expected
            for name in ("sentinel-platform", "sentinel-dependencies", "sentinel-operations")
        ),
        default=0,
    )
    report = assess([job["id"] for job in jobs], start, now, runs, min(1, coverage))
    report["checked_at"] = now.isoformat()
    report["runs"] = runs
    write_atomic(args.output, report)
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
