"""Local processor recovery must preserve ownership and reject corrupt state."""
from pathlib import Path
import pytest
from config.settings import settings
from spark.streaming.streaming_job import StreamingJob

pytestmark = pytest.mark.unit


def test_corrupt_existing_checkpoint_is_not_silently_replaced_by_empty_state(tmp_path):
    directory = tmp_path/'checkpoint';directory.mkdir()
    path = directory/'checkpoint_state.json';path.write_text('{broken')
    with pytest.raises(RuntimeError,match='checkpoint'):
        StreamingJob(checkpoint_dir=str(directory))
    assert path.read_text() == '{broken'


def test_failed_checkpoint_serialization_preserves_the_previous_committed_file(tmp_path):
    job = StreamingJob(checkpoint_dir=str(tmp_path/'checkpoint'))
    job.processed_batches = 3
    path = Path(job.save_checkpoint())
    before = path.read_bytes()
    job.window_state = {'unserializable':object()}
    with pytest.raises(TypeError):
        job.save_checkpoint()
    assert path.read_bytes() == before


def test_s3a_checkpoint_cannot_silently_write_to_a_different_configured_bucket():
    with pytest.raises(ValueError,match='bucket'):
        StreamingJob(checkpoint_dir='s3a://different-bucket/checkpoint')


def test_s3a_checkpoint_cannot_be_reported_as_remote_when_using_local_backend():
    with pytest.raises(ValueError,match='backend'):
        StreamingJob(checkpoint_dir=f's3a://{settings.storage.data_lake_bucket}/checkpoint')


def test_checkpoint_round_trip_retains_idempotence(tmp_path):
    folder = tmp_path/'checkpoint'
    first = StreamingJob(checkpoint_dir=str(folder),strict_validation=True)
    event = {'review_id':'r1','product_id':'p1','rating':4,'review_date':'2026-01-01T00:00:00Z'}
    first.process_micro_batch([event],batch_id='batch1')
    recovered = StreamingJob(checkpoint_dir=str(folder),strict_validation=True)
    result = recovered.process_micro_batch([event],batch_id='batch1')
    assert result['skipped_committed_batch'] is True
    assert recovered.valid_events_processed == 1
    assert recovered.get_window_metrics()[0]['review_count'] == 1


def test_existing_empty_json_object_is_not_a_valid_recovery_snapshot(tmp_path):
    folder=tmp_path/'checkpoint';folder.mkdir()
    (folder/'checkpoint_state.json').write_text('{}')
    with pytest.raises(RuntimeError,match='checkpoint'):
        StreamingJob(checkpoint_dir=str(folder))


def test_recovery_cannot_mix_windows_from_a_different_configuration(tmp_path):
    folder=tmp_path/'checkpoint'
    job=StreamingJob(checkpoint_dir=str(folder),window_seconds=60)
    job.process_micro_batch([{'review_id':'r','product_id':'p','rating':4,'timestamp':'2026-01-01T00:00:00Z'}])
    with pytest.raises(RuntimeError,match='configuration'):
        StreamingJob(checkpoint_dir=str(folder),window_seconds=120)


def test_corrupt_restart_does_not_discard_the_existing_memory_state(tmp_path):
    folder=tmp_path/'checkpoint'
    job=StreamingJob(checkpoint_dir=str(folder));job.processed_batches=3
    path=Path(job.save_checkpoint());path.write_text('{broken')
    with pytest.raises(RuntimeError,match='checkpoint'):
        job.restart()
    assert job.processed_batches == 3


def test_invalid_later_counter_does_not_partially_apply_a_checkpoint(tmp_path):
    import json
    job=StreamingJob(checkpoint_dir=str(tmp_path/'checkpoint'));job.processed_batches=3
    path=Path(job.save_checkpoint());data=json.loads(path.read_text())
    data.update(processed_batches=99,negative_reviews_detected='broken')
    path.write_text(json.dumps(data))
    with pytest.raises(RuntimeError,match='checkpoint'):
        job.load_checkpoint()
    assert job.processed_batches == 3


def test_failed_durable_save_does_not_commit_the_batch_in_memory(tmp_path):
    directory=tmp_path/'checkpoint'
    job=StreamingJob(checkpoint_dir=str(directory),strict_validation=True)
    directory.write_text('a file prevents creating the checkpoint directory')
    event={'review_id':'r1','product_id':'p','rating':4,'timestamp':'2026-01-01T00:00:00Z'}
    with pytest.raises(OSError):
        job.process_micro_batch([event],batch_id='b1')
    assert job.valid_events_processed == 0
    assert job.committed_batch_ids == []
    assert job.get_window_metrics() == []
    directory.unlink()
    retried=job.process_micro_batch([event],batch_id='b1')
    assert retried['skipped_committed_batch'] is False
    assert job.valid_events_processed == 1
