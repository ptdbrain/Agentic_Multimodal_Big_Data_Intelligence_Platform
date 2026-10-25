"""Isolated S3 error classification; real concurrent clients are tested in integration."""
import pytest
from minio.error import S3Error
from storage.minio_backend import MinIOBackend

pytestmark = pytest.mark.unit


@pytest.mark.parametrize('code', ['AccessDenied', 'BucketAlreadyExists', 'BucketAlreadyOwnedByYou'])
def test_bucket_creation_error_cannot_report_health_without_readable_ownership(monkeypatch, code):
    backend = MinIOBackend(endpoint='127.0.0.1:1', bucket='fixture', enable_local_mirror=False)
    failure = S3Error(code, 'Injected S3 error response', '/fixture', 'request', 'host', None)
    monkeypatch.setattr(backend.client, 'bucket_exists', lambda name: False)

    def fail(name):
        raise failure

    monkeypatch.setattr(backend.client, 'make_bucket', fail)
    with pytest.raises(ConnectionError) as error:
        backend.health_check()
    assert error.value.__cause__ is failure
    assert not backend.is_connected
    assert not backend._bucket_verified
