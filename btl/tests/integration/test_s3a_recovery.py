"""Fault injection at publication with real Spark S3A and MinIO persistence."""
import uuid
import pytest
from spark.storage_safe import SafeParquetIO,ParquetPublishError
from config.settings import settings

pytestmark=[pytest.mark.integration,pytest.mark.spark]


def test_s3a_failed_promotion_preserves_original_rows(spark_session,monkeypatch):
    s=spark_session
    target=f's3a://{settings.storage.data_lake_bucket}/recovery/{uuid.uuid4().hex}/table'
    old=s.createDataFrame([(1,'one'),(2,'two')],['id','value'])
    SafeParquetIO.publish_parquet_atomic(s,old,target,expected_count=2)
    rename=SafeParquetIO._rename_storage_path
    def fail(spark,src,dst):
        if '__staging_' in src:raise OSError('injected S3A promotion failure')
        return rename(spark,src,dst)
    monkeypatch.setattr(SafeParquetIO,'_rename_storage_path',fail)
    with pytest.raises(ParquetPublishError) as exc:
        SafeParquetIO.publish_parquet_atomic(s,s.createDataFrame([(3,'new')],['id','value']),target,expected_count=1)
    assert not exc.value.recovery_required
    assert {(r.id,r.value) for r in s.read.parquet(target).collect()}=={(1,'one'),(2,'two')}
