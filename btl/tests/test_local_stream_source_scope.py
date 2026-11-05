"""Local review windows preserve source isolation and do not count price updates."""
import pytest
from spark.streaming.streaming_job import StreamingJob

pytestmark = pytest.mark.unit


def review(group='simulation',identifier='r1'):
    return {'review_id':identifier,'product_id':'p','rating':4,'timestamp':'2026-01-01T00:00:00Z',
            'source_group':group,'acquisition_method':'generated' if group=='simulation' else 'live'}


def test_mixed_source_batch_is_rejected_before_mutating_state():
    job=StreamingJob(strict_validation=True)
    with pytest.raises(ValueError,match='mixed'):
        job.process_micro_batch([review(),review('marketplace','r2')])
    assert job.valid_events_processed == 0
    assert job.get_window_metrics() == []


def test_recovered_windows_cannot_accumulate_a_different_source_class(tmp_path):
    folder=tmp_path/'checkpoint'
    job=StreamingJob(checkpoint_dir=str(folder),strict_validation=True)
    job.process_micro_batch([review()],batch_id='b1')
    recovered=StreamingJob(checkpoint_dir=str(folder),strict_validation=True)
    with pytest.raises(ValueError,match='source'):
        recovered.process_micro_batch([review('marketplace','r2')],batch_id='b2')
    assert recovered.valid_events_processed == 1
    assert recovered.get_window_metrics()[0]['source_group'] == 'simulation'


def test_root_envelope_provenance_is_retained_for_generated_review_events():
    item=review()
    group=item.pop('source_group');method=item.pop('acquisition_method')
    job=StreamingJob(strict_validation=True)
    result=job.process_micro_batch([{'event_type':'NEW_REVIEW','source_group':group,
                                    'acquisition_method':method,'payload':item}])
    assert result['source_group'] == 'simulation'
    assert job.get_window_metrics()[0]['source_group'] == 'simulation'


def test_price_and_catalog_rating_updates_are_not_counted_as_customer_reviews():
    job=StreamingJob()
    result=job.process_micro_batch([
        {'event_type':'PRICE_UPDATE','product_id':'p','timestamp':'2026-01-01T00:00:00Z','payload':{'new_price':100}},
        {'event_type':'RATING_UPDATE','product_id':'p','timestamp':'2026-01-01T00:00:00Z','payload':{'new_rating':4.5}},
        dict(review(),event_type='NEW_REVIEW')])
    assert result['batch_valid_count'] == 1
    assert result['batch_ignored_count'] == 2
    assert job.get_window_metrics()[0]['review_count'] == 1
