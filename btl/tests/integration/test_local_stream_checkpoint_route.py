"""Explicit local processor state, actually persisted and recovered from MinIO."""
import uuid
import pytest
from config.settings import settings
from storage.storage_manager import storage
from spark.streaming.streaming_job import StreamingJob

pytestmark = pytest.mark.integration


def test_local_processor_checkpoint_uses_the_actual_named_minio_bucket():
    bucket = 'local-state-' + uuid.uuid4().hex[:12]
    settings.storage.data_lake_bucket = bucket
    storage.set_mode('integration')
    backend = storage.backend
    try:
        uri = f's3a://{bucket}/checkpoints/fixture'
        job = StreamingJob(checkpoint_dir=uri,strict_validation=True)
        event = {'review_id':'fixture-r1','product_id':'fixture-p1','rating':4,
                 'review_date':'2026-01-01T00:00:00Z','source_group':'simulation','acquisition_method':'generated'}
        job.process_micro_batch([event],batch_id='fixture-batch')
        assert job.save_checkpoint() == uri + '/checkpoint_state.json'
        assert backend.client.bucket_exists(bucket)
        assert storage.read_json('checkpoints/fixture/checkpoint_state.json')['valid_events_processed'] == 1
        recovered = StreamingJob(checkpoint_dir=uri,strict_validation=True)
        result = recovered.process_micro_batch([event],batch_id='fixture-batch')
        assert result['skipped_committed_batch'] is True
        assert recovered.valid_events_processed == 1
        with pytest.raises(ValueError,match='bucket'):
            StreamingJob(checkpoint_dir='s3a://wrong-owner/checkpoint')
    finally:
        for item in backend.client.list_objects(bucket,recursive=True):
            backend.client.remove_object(bucket,item.object_name)
        backend.client.remove_bucket(bucket)
