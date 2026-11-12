"""Acceptance cannot be satisfied by manual runs, fabricated waiting or partial receipts."""

from datetime import datetime, timedelta, timezone
import pytest

pytestmark = pytest.mark.unit


def test_acceptance_requires_elapsed_window_and_three_real_scheduled_cycles_per_job():
    from scripts.verify_source_acceptance import assess

    start = datetime(2026, 11, 12, tzinfo=timezone.utc)
    runs = [
        {"job": "tiki", "run_type": "scheduled", "state": "success", "receipt_verified": True, "coverage": "complete"}
        for _ in range(3)
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
