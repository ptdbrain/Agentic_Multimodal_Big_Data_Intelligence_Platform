import pandas as pd
import pytest
from analytics.trend.trends import TrendAnalyzer

pytestmark = pytest.mark.unit


def test_calendar_window_does_not_include_old_observed_days():
    rows = pd.DataFrame([
        {'review_id': 'r1', 'rating': 5, 'review_date': '2026-01-01'},
        {'review_id': 'r2', 'rating': 1, 'review_date': '2026-10-01'},
    ])
    daily = TrendAnalyzer.daily_review_trends(rows, rolling_window=7)
    row = daily.iloc[-1]
    assert row['rolling_avg_rating'] == 1
    assert row['observed_days'] == 1
    assert row['missing_days'] == 6
    assert row['trend_status'] == 'insufficient_data'


def test_rolling_rating_weights_reviews_and_reports_window_bounds():
    rows = [{'review_id': 'a', 'rating': 1, 'review_date': '2026-10-01'}]
    rows += [{'review_id': f'b{i}', 'rating': 5, 'review_date': '2026-10-02'} for i in range(9)]
    row = TrendAnalyzer.daily_review_trends(pd.DataFrame(rows), rolling_window=7).iloc[-1]
    assert row['rolling_avg_rating'] == 4.6
    assert row['rating_sample_size'] == 9
    assert row['rolling_rating_sample_size'] == 10
    assert str(row['baseline_start']) == '2026-09-26'
    assert str(row['baseline_end']) == '2026-10-02'
    assert row['trend_status'] == 'sufficient_data'
