from pathlib import Path

import pandas as pd
import pytest

from analytics.trend.trends import TrendAnalyzer

pytestmark = pytest.mark.unit


def test_volume_window_uses_calendar_days_without_inventing_zero_observations():
    stats = pd.DataFrame(
        [
            {"date": "2026-10-01", "review_count": 8},
            {"date": "2026-10-01", "review_count": 12},
            {"date": "2026-10-10", "review_count": 10},
            {"date": "2026-10-11", "review_count": 20},
        ]
    )
    actual = TrendAnalyzer.daily_volume_trends(stats)
    assert actual["review_count"].tolist() == [20, 10, 20]
    assert actual["rolling_avg_count"].tolist() == [20, 10, 15]
    assert actual["observed_days"].tolist() == [1, 1, 2]
    assert actual["missing_days"].tolist() == [6, 6, 5]
    assert actual["trend_status"].tolist() == ["insufficient_data", "insufficient_data", "sufficient_data"]
    assert str(actual.iloc[1]["baseline_start"]) == "2026-10-04"
    assert str(actual.iloc[1]["baseline_end"]) == "2026-10-10"


def test_volume_window_excludes_invalid_counts_and_normalizes_utc():
    actual = TrendAnalyzer.daily_volume_trends(
        pd.DataFrame(
            [
                {"date": "2026-10-02T00:30:00+07:00", "review_count": 3},
                {"date": "invalid", "review_count": 10},
                {"date": "2026-10-01", "review_count": float("inf")},
                {"date": "2026-10-01", "review_count": -1},
            ]
        )
    )
    assert len(actual) == 1
    assert str(actual.iloc[0]["date"]) == "2026-10-01"
    assert actual.iloc[0]["review_count"] == 3


def test_overview_displays_coverage_for_sparse_daily_mart():
    from streamlit.testing.v1 import AppTest
    from database.warehouse import WarehouseManager

    wh = WarehouseManager(mode="test")
    wh.initialize_schema()
    wh.upsert_dataframe(
        "product_daily_stats",
        pd.DataFrame(
            [
                {"date": "2026-10-01", "product_id": "p1", "review_count": 20, "avg_rating": 4.0},
                {"date": "2026-10-10", "product_id": "p1", "review_count": 10, "avg_rating": 4.0},
            ]
        ),
        ["date", "product_id"],
    )
    wh.close()
    app = AppTest.from_file(str(Path(__file__).resolve().parents[1] / "dashboard/pages/1_Overview.py")).run(timeout=20)
    assert not app.exception
    assert any("Chưa đủ độ bao phủ theo ngày" in w.value for w in app.warning)
    coverage = next(frame.value for frame in app.dataframe if "observed_days" in frame.value)
    assert coverage["observed_days"].tolist() == [1, 1]
    assert coverage["missing_days"].tolist() == [6, 6]
