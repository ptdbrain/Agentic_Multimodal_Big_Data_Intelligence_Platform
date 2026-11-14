"""Independent causal-statistics examples, explicitly generated test observations."""

from datetime import datetime, timedelta, timezone
import pytest

pytestmark = pytest.mark.unit


def windows(n=12):
    start = datetime(2026, 11, 13, tzinfo=timezone.utc)
    result = []
    for i in range(n):
        point = start + timedelta(minutes=i)
        result.append(
            {
                "product_id": "p",
                "source_group": "simulation",
                "window_seconds": 60,
                "window_start": point.isoformat(),
                "window_end": (point + timedelta(minutes=1)).isoformat(),
                "review_count": 2,
                "avg_rating": 4.5,
                "negative_count": 0,
            }
        )
    candidate = dict(
        result[-1],
        window_start=(start + timedelta(minutes=13)).isoformat(),
        window_end=(start + timedelta(minutes=14)).isoformat(),
        review_count=20,
        avg_rating=1.5,
        negative_count=18,
    )
    return result, candidate, start


def test_stream_burst_and_drop_use_closed_past_windows_only():
    from analytics.causal import stream_evaluations

    past, current, start = windows()
    watermark = (start + timedelta(minutes=13)).isoformat()
    evaluated = stream_evaluations(past + [current], dataset_id="fixture", generation="g", watermark=watermark)
    selected = [e for e in evaluated if e["window_start"] == current["window_start"]]
    assert {e["event_type"] for e in selected} == {"REVIEW_BOMBING", "RATING_DROP"}
    assert all(e["state"] == "firing" and e["evidence"]["baseline_windows"] == 12 for e in selected)
    assert all(e["evidence"]["provisional"] for e in selected)
    future = dict(
        current,
        window_start=(start + timedelta(minutes=15)).isoformat(),
        window_end=(start + timedelta(minutes=16)).isoformat(),
        review_count=100000,
        avg_rating=5.0,
    )
    again = stream_evaluations(
        past + [current, future],
        dataset_id="fixture",
        generation="g",
        watermark=(start + timedelta(minutes=20)).isoformat(),
    )
    candidate = [e for e in again if e["window_start"] == current["window_start"]]
    assert {e["event_id"] for e in candidate} == {e["event_id"] for e in selected}
    assert all(e["evidence"]["baseline_count_mean"] == 2 for e in candidate)


def test_insufficient_or_failed_quality_cannot_be_a_firing_incident():
    from analytics.causal import stream_evaluations

    past, current, start = windows(11)
    evaluated = stream_evaluations(
        past + [current], dataset_id="fixture", generation="g", watermark=(start + timedelta(minutes=13)).isoformat()
    )
    assert all(e["state"] == "insufficient_data" for e in evaluated)
    evaluated = stream_evaluations(
        windows()[0] + [current],
        dataset_id="fixture",
        generation="g",
        watermark=(start + timedelta(minutes=13)).isoformat(),
        quality_status="failed",
    )
    assert all(e["state"] == "insufficient_data" for e in evaluated)


def test_causal_prices_never_use_future_or_mix_currencies():
    from analytics.causal import price_evaluations

    rows = [
        {
            "product_id": "p",
            "source_group": "simulation",
            "date": f"2026-11-{day:02d}",
            "avg_price": 100,
            "currency": "VND",
        }
        for day in (1, 2, 3)
    ]
    rows.append(dict(rows[0], date="2026-11-04", avg_price=200))
    result = price_evaluations(rows, dataset_id="fixture", generation="g")
    fourth = [e for e in result if e["window_start"].startswith("2026-11-04")]
    assert any(e["state"] == "firing" for e in fourth)
    rows.extend(
        [
            dict(rows[0], date="2026-11-05", avg_price=100000),
            dict(rows[0], date="2026-11-03", avg_price=99999, currency="USD"),
        ]
    )
    same = [
        e
        for e in price_evaluations(rows, dataset_id="fixture", generation="g")
        if e["window_start"].startswith("2026-11-04") and e["evidence"]["currency"] == "VND"
    ]
    assert [(e["event_type"], e["state"], e["evidence"]["baseline_mean"]) for e in same] == [
        (e["event_type"], e["state"], e["evidence"]["baseline_mean"]) for e in fourth
    ]


def test_republishing_same_price_history_revises_the_same_incident_identity():
    from analytics.causal import price_evaluations

    rows = [
        {
            "product_id": "p",
            "source_group": "simulation",
            "date": f"2026-11-{day:02d}",
            "avg_price": value,
            "currency": "VND",
        }
        for day, value in [(1, 100), (2, 100), (3, 100), (4, 200)]
    ]
    first = price_evaluations(rows, dataset_id="fixture", generation="g1")
    second = price_evaluations(rows, dataset_id="fixture", generation="g2")
    assert [e["event_id"] for e in first] == [e["event_id"] for e in second]


def test_daily_price_dates_reject_timestamps_instead_of_changing_their_timezone():
    from analytics.causal import price_evaluations

    rows = [
        {
            "product_id": "p",
            "source_group": "simulation",
            "date": "2026-11-04T23:00:00-05:00",
            "avg_price": 100,
            "currency": "VND",
        }
    ]
    with pytest.raises(ValueError, match="daily|date"):
        price_evaluations(rows, dataset_id="fixture", generation="g")


def test_price_sources_cannot_collide_in_one_dataset_identity():
    from analytics.causal import price_evaluations

    base = {"product_id": "p", "date": "2026-11-04", "avg_price": 100, "currency": "VND"}
    with pytest.raises(ValueError, match="source"):
        price_evaluations(
            [{**base, "source_group": "simulation"}, {**base, "source_group": "marketplace"}],
            dataset_id="fixture",
            generation="g",
        )


def test_candidate_bound_avoids_revisiting_history_without_changing_its_baseline():
    from analytics.causal import stream_evaluations

    past, current, start = windows()
    watermark = (start + timedelta(minutes=13)).isoformat()
    all_results = stream_evaluations(past + [current], dataset_id="fixture", generation="g", watermark=watermark)
    bounded = stream_evaluations(
        past + [current],
        dataset_id="fixture",
        generation="g",
        watermark=watermark,
        candidate_from=current["window_start"],
    )
    assert bounded == [result for result in all_results if result["window_start"] == current["window_start"]]
    assert len(bounded) == 2 and all(result["evidence"]["baseline_windows"] == 12 for result in bounded)
