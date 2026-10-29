"""These tests require real services. They deliberately do not skip on outage."""
import uuid
import pytest

pytestmark = pytest.mark.integration


def test_minio_concurrent_bucket_bootstrap_is_idempotent(monkeypatch):
    from concurrent.futures import ThreadPoolExecutor
    import threading
    from config.settings import settings
    from storage.storage_manager import StorageManager

    bucket = 'processing-bootstrap-' + uuid.uuid4().hex
    monkeypatch.setattr(settings.storage, 'data_lake_bucket', bucket)
    backends = [StorageManager(mode='integration').backend for _ in range(2)]
    barrier = threading.Barrier(2)
    for backend in backends:
        original = backend.client.bucket_exists

        def exists(name, _original=original):
            present = _original(name)
            if name == bucket and not present:
                # Both real clients observe absence before either may create it.
                barrier.wait(timeout=10)
            return present

        monkeypatch.setattr(backend.client, 'bucket_exists', exists)
    try:
        with ThreadPoolExecutor(max_workers=2) as pool:
            assert list(pool.map(lambda backend: backend.health_check(), backends)) == [True, True]
        assert backends[0].client.bucket_exists(bucket)
    finally:
        if backends[0].client.bucket_exists(bucket):
            backends[0].client.remove_bucket(bucket)


def test_elasticsearch_persists_between_clients():
    from storage.search_indexer import SearchIndexer
    index='sentinel-test-'+uuid.uuid4().hex
    writer=SearchIndexer(backend='elasticsearch',index=index)
    try:
        assert writer.health_check()
        writer.index_records([{'review_id':'r1','review_text':'unique camera review','rating':5}])
        reader=SearchIndexer(backend='elasticsearch',index=index)
        assert reader.search('camera')[0]['review_id']=='r1'
        writer.index_records([{'review_id':'r1','review_text':'updated camera review','rating':4}])
        assert reader.count==1
    finally:
        response=writer.session.delete(writer.endpoint+'/'+index,timeout=10)
        if response.status_code not in (200,404): response.raise_for_status()


def test_postgres_minio_lakehouse_transaction():
    import pandas as pd
    from config.settings import settings
    from storage.storage_manager import StorageManager
    from storage.lakehouse import LakehouseTable
    settings.set_runtime_mode('integration')
    mgr=StorageManager(mode='integration')
    assert mgr.health_check()
    t=LakehouseTable('integration_'+uuid.uuid4().hex,storage_backend=mgr)
    try:
        version=t.write(pd.DataFrame({'id':[1],'value':[10]}),operation_id='initial')
        t.upsert(pd.DataFrame({'id':[1],'value':[20]}),['id'])
        assert t.read(version)['value'].tolist()==[10]
        assert t.read()['value'].tolist()==[20]
    finally: t.close()
