"""Anomaly evidence must describe the actual historical sample and DQ decision."""
import pandas as pd
import pytest
from analytics.anomaly.rating_anomaly import RatingAnomalyDetector
from analytics.anomaly.review_burst import ReviewBurstDetector
from analytics.anomaly.price_anomaly import PriceAnomalyDetector

pytestmark = pytest.mark.unit


def rating_rows():
    return pd.DataFrame([
        {'review_id': f'{day}-{i}', 'product_id': 'p1', 'rating': value, 'review_date': day}
        for day, value, n in [('2026-10-01', 5, 4), ('2026-10-02', 5, 6), ('2026-10-03', 1, 3)]
        for i in range(n)
    ])


def burst_rows():
    return pd.DataFrame([
        {'review_id': f'{hour}-{i}', 'product_id': 'p1', 'rating': 5,
         'review_date': f'2026-10-01T{hour:02}:00:00Z'}
        for hour, n in [(0, 1), (1, 1), (2, 10)] for i in range(n)
    ])


def price_rows():
    return pd.DataFrame([
        {'product_id': 'p1', 'price': value, 'timestamp': f'2026-10-0{i+1}'}
        for i, value in enumerate([100., 101., 99., 102., 10.])
    ])


DETECTORS = [(RatingAnomalyDetector.detect_rating_drops, rating_rows),
             (ReviewBurstDetector.detect_bursts, burst_rows),
             (PriceAnomalyDetector.detect_zscore_anomalies, price_rows),
             (PriceAnomalyDetector.detect_iqr_anomalies, price_rows)]


def test_rating_baseline_size_counts_historical_reviews_not_candidate_day():
    event = RatingAnomalyDetector.detect_rating_drops(rating_rows(), run_id='run-1')[0]
    assert event['baseline_sample_size'] == 10
    assert event['current_sample_size'] == 3
    assert event['baseline_window_days'] == 7
    assert event['baseline_observed_days'] == 2


@pytest.mark.parametrize('detect,rows', DETECTORS)
@pytest.mark.parametrize('status', ['failed', 'stale', 'invalid'])
def test_failed_dq_cannot_produce_anomaly_success(detect, rows, status):
    assert detect(rows(), data_quality_status='valid')  # Fixture really triggers the detector.
    assert detect(rows(), data_quality_status=status) == []


@pytest.mark.parametrize('detect,rows', DETECTORS)
def test_standalone_analysis_does_not_claim_dq_was_verified(detect, rows):
    events = detect(rows())
    assert events
    assert {e['data_quality_status'] for e in events} == {'unverified'}


def test_burst_requires_observed_history_inside_configured_window():
    rows = burst_rows()
    rows.loc[rows['review_date'] == '2026-10-01T02:00:00Z', 'review_date'] = '2026-10-20T02:00:00Z'
    assert ReviewBurstDetector.detect_bursts(rows, baseline_window_hours=24) == []


@pytest.mark.parametrize('detect,rows,unit,method', [
    (RatingAnomalyDetector.detect_rating_drops, rating_rows, 'reviews', 'past_calendar_window'),
    (ReviewBurstDetector.detect_bursts, burst_rows, 'time_buckets', 'past_calendar_window'),
    (PriceAnomalyDetector.detect_zscore_anomalies, price_rows, 'price_observations', 'leave_one_out'),
    (PriceAnomalyDetector.detect_iqr_anomalies, price_rows, 'price_observations', 'leave_one_out'),
])
def test_baseline_metadata_describes_unit_and_method(detect, rows, unit, method):
    event = detect(rows(), run_id='r', data_quality_status='valid')[0]
    assert event['baseline_sample_unit'] == unit
    assert event['baseline_method'] == method
    assert pd.notna(pd.to_datetime(event['baseline_start'], utc=True))
    assert pd.notna(pd.to_datetime(event['baseline_end'], utc=True))
