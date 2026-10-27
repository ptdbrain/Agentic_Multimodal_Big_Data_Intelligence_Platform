import pandas as pd
import pytest
from analytics.anomaly.review_burst import ReviewBurstDetector
from analytics.anomaly.rating_anomaly import RatingAnomalyDetector

pytestmark = pytest.mark.unit


def test_future_reviews_cannot_erase_past_burst():
    rows=[]
    for h,n in [(0,1),(1,1),(2,10)]:
        rows.extend(dict(review_id=f'{h}_{i}',product_id='p1',rating=5,review_date=f'2026-10-01T{h:02}:00:00Z') for i in range(n))
    before=ReviewBurstDetector.detect_bursts(pd.DataFrame(rows))
    rows.extend(dict(review_id=f'later_{i}',product_id='p1',rating=5,review_date='2026-10-02T00:00:00Z') for i in range(100))
    after=ReviewBurstDetector.detect_bursts(pd.DataFrame(rows))
    past=[a for a in after if pd.Timestamp(a['timestamp']) < pd.Timestamp('2026-10-02',tz='UTC')]
    assert past == before
    assert len(before) == 1


def test_rating_baseline_uses_calendar_days():
    rows=[]
    for date,rating in [('2026-01-01',5),('2026-01-02',5),('2026-10-01',1)]:
        rows.extend(dict(review_id=f'{date}_{i}',product_id='p1',rating=rating,review_date=date) for i in range(3))
    assert RatingAnomalyDetector.detect_rating_drops(pd.DataFrame(rows),window_days=7) == []


def test_rating_baseline_weights_review_volume():
    rows=[]
    for date,rating,n in [('2026-10-01',1,1),('2026-10-02',5,9),('2026-10-03',4,3)]:
        rows.extend(dict(review_id=f'{date}_{i}',product_id='p1',rating=rating,review_date=date) for i in range(n))
    a=RatingAnomalyDetector.detect_rating_drops(pd.DataFrame(rows),drop_threshold=.5)
    assert len(a) == 1
    assert a[0]['baseline_rating'] == 4.6


def test_temporal_detectors_drop_invalid_dates_without_losing_valid_rows():
    rows = [
        dict(review_id='r1', product_id='p1', rating=5, review_date='2026-10-01'),
        dict(review_id='r2', product_id='p1', rating=5, review_date='2026-10-02'),
        dict(review_id='bad', product_id='p1', rating=5, review_date='bad-date'),
    ]
    assert RatingAnomalyDetector.detect_rating_drops(
        pd.DataFrame(rows), min_history_days=2
    ) == []
    assert ReviewBurstDetector.detect_bursts(
        pd.DataFrame(rows), min_history_points=2
    ) == []


def test_anomaly_events_carry_run_and_baseline_metadata():
    rows = []
    for h, n in [(0, 1), (1, 1), (2, 10)]:
        rows.extend(dict(review_id=f'{h}_{i}', product_id='p1', rating=5,
                         review_date=f'2026-10-01T{h:02}:00:00Z') for i in range(n))
    event = ReviewBurstDetector.detect_bursts(
        pd.DataFrame(rows), run_id='run-1', data_quality_status='valid'
    )[0]
    assert event['run_id'] == 'run-1'
    assert event['data_quality_status'] == 'valid'
    assert event['baseline_window_hours'] == 24
    assert event['baseline_sample_size'] >= 2
