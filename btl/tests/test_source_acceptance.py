"""Acceptance cannot be satisfied by manual runs, fabricated waiting or partial receipts."""

from datetime import datetime, timedelta, timezone
import pytest

pytestmark = pytest.mark.unit


def test_acceptance_requires_elapsed_window_and_three_real_scheduled_cycles_per_job():
    from scripts.verify_source_acceptance import assess

    start = datetime(2026, 11, 12, tzinfo=timezone.utc)
    runs = [
        {"job": "tiki", "run_id": f"scheduled__{i}", "run_type": "scheduled", "state": "success", "receipt_verified": True, "coverage": "complete"}
        for i in range(3)
    ]
    assert not assess(["tiki"], start, start + timedelta(hours=71), runs, 1.0)["accepted"]
    assert assess(["tiki"], start, start + timedelta(hours=72), runs, 1.0)["accepted"]
    runs[0]["run_type"] = "manual"
    assert not assess(["tiki"], start, start + timedelta(hours=72), runs, 1.0)["accepted"]
    runs[0]["run_type"] = "scheduled"
    runs[0]["receipt_verified"] = False
    assert not assess(["tiki"], start, start + timedelta(hours=72), runs, 1.0)["accepted"]
    runs[0]["receipt_verified"] = True
    runs[0]["coverage"] = "partial"
    result = assess(["tiki"], start, start + timedelta(hours=72), runs, 1.0)
    assert not result["accepted"] and result["operational_acceptance"]
    assert not assess(["tiki"], start, start + timedelta(hours=72), runs, 0.5)["operational_acceptance"]


def test_monitoring_coverage_uses_all_configured_docker_jobs_and_counts_missing_minutes():
    from pathlib import Path
    from scripts.verify_source_acceptance import expected_jobs, monitoring_coverage

    root = Path(__file__).resolve().parents[1]
    jobs = expected_jobs(root / "monitoring/prometheus-application.yml")
    assert set(jobs) == {"sentinel-dependencies", "sentinel-operations", "sentinel-metrics", "sentinel-datasets"}
    series = [{"metric": {"job": job}, "values": [[60, "1"], [120, "1"]]} for job in jobs]
    assert min(monitoring_coverage(jobs, series, 2).values()) == 1
    series.pop()
    assert min(monitoring_coverage(jobs, series, 2).values()) == 0
    series.append({"metric": {"job": jobs[-1]}, "values": [[60, "0"], [120, "1"]]})
    assert monitoring_coverage(jobs, series, 2)[jobs[-1]] == 0.5
    assert "sentinel-platform" in expected_jobs(root / "monitoring/prometheus-operations.yml")


def test_acceptance_requires_distinct_runs_and_a_nonempty_owner_set():
    from scripts.verify_source_acceptance import assess

    start = datetime(2026, 11, 12, tzinfo=timezone.utc)
    end = start + timedelta(hours=72)
    run = {"job": "web", "run_type": "scheduled", "run_id": "scheduled__one",
           "state": "success", "receipt_verified": True, "coverage": "complete"}
    assert not assess(["web"], start, end, [run] * 3, 1)["accepted"]
    assert not assess([], start, end, [], 1)["accepted"]
