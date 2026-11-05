import pytest
from spark.streaming.streaming_job import StreamingJob

pytestmark = pytest.mark.unit


def event(**fields):
    return dict(review_id='r',product_id='p',rating=4,timestamp='2026-01-01T00:00:00Z',**fields)


def test_boolean_rating_is_not_a_real_one_star_observation():
    job=StreamingJob(strict_validation=True)
    result=job.process_micro_batch([{'review_id':'r','product_id':'p','rating':True,'timestamp':'2026-01-01T00:00:00Z'}])
    assert result['batch_invalid_count'] == 1
    assert job.get_window_metrics() == []


def test_zero_epoch_can_reach_the_window_state():
    job=StreamingJob(strict_validation=True)
    result=job.process_micro_batch([{'review_id':'r','product_id':'p','rating':4,'timestamp':0}])
    assert result['batch_valid_count'] == 1
    assert job.get_window_metrics()[0]['window_start_epoch'] == 0


def test_collection_clock_is_not_substituted_for_missing_observation_time():
    job=StreamingJob(strict_validation=True)
    job.process_micro_batch([{'ingested_at':'2026-01-01T00:00:00Z',
                             'payload':{'review_id':'r','product_id':'p','rating':4}}])
    assert job.get_window_metrics() == []


def test_malformed_event_shape_is_quarantined_without_discarding_later_events():
    job=StreamingJob(strict_validation=True)
    result=job.process_micro_batch([None,event()])
    assert result['batch_invalid_count'] == result['batch_valid_count'] == 1
    assert job.get_window_metrics()[0]['review_count'] == 1
