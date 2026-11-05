"""Repeated full-path batch latency; excludes browser rendering and event age."""

import argparse
import json
from pathlib import Path
import subprocess
import sys
import numpy as np

STAGES = ("kafka_bronze", "bronze_verify", "spark_silver_dq", "spark_gold_anomalies", "postgres_dashboard")


def summarize(reports):
    if len(reports) < 3:
        raise ValueError("At least three independent runs required")
    values = []
    fingerprints = set()
    for report in reports:
        if not report.get("completed"):
            raise ValueError("Incomplete full-path measurement")
        times = report["times_sec"]
        measured = [float(times[name]) for name in STAGES]
        if not all(np.isfinite(v) and v >= 0 for v in measured):
            raise ValueError("Invalid measured durations")
        values.append(sum(measured))
        fingerprints.add(report["source_fingerprint"]["sha256"])
    if len(fingerprints) != 1:
        raise ValueError("Cannot pool different code versions")
    return {
        "version": 1,
        "samples": len(values),
        "batch_latency_seconds": {
            "p50": float(np.percentile(values, 50)),
            "p95": float(np.percentile(values, 95)),
            "p99": float(np.percentile(values, 99)),
        },
        "individual_seconds": values,
        "source_fingerprint": fingerprints.pop(),
        "boundary": "independent full-path batch cohorts, from Kafka dispatch stage to SQL/DashboardDB visibility; not per-event streaming latency or browser rendering; no throughput SLA",
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--iterations", type=int, default=3)
    parser.add_argument("--reviews", type=int, default=1000)
    args = parser.parse_args()
    if not 3 <= args.iterations <= 30 or args.reviews < 100:
        parser.error("Use3–30 iterations and >=100 reviews")
    args.output_dir.mkdir(parents=True, exist_ok=False)
    reports = []
    for iteration in range(args.iterations):
        output = args.output_dir / f"run-{iteration + 1}.json"
        with (args.output_dir / f"run-{iteration + 1}.log").open("x") as log:
            subprocess.run(
                [
                    sys.executable,
                    str(Path(__file__).with_name("benchmark_fullpath.py")),
                    "--reviews",
                    str(args.reviews),
                    "--output",
                    str(output),
                ],
                check=True,
                stdout=log,
                stderr=subprocess.STDOUT,
            )
        reports.append(json.loads(output.read_text()))
    result = summarize(reports)
    (args.output_dir / "summary.json").write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
