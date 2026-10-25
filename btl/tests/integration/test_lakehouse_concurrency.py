"""Real PostgreSQL compare-and-swap and MinIO snapshot publication."""
from concurrent.futures import ThreadPoolExecutor
import threading
import uuid
import pandas as pd
import pytest
from storage.lakehouse import LakehouseTable
from storage.lakehouse_metadata import ConcurrentCommitError
from storage.storage_manager import StorageManager

pytestmark = pytest.mark.integration


def test_two_remote_writers_commit_only_one_snapshot():
    name='race_'+uuid.uuid4().hex
    tables=[LakehouseTable(name,storage_backend=StorageManager(mode='integration')) for _ in range(2)]
    barrier=threading.Barrier(2)
    for table in tables:
        original=table.metadata.commit
        def commit(*args,_original=original,**kwargs):
            barrier.wait(timeout=15)
            return _original(*args,**kwargs)
        table.metadata.commit=commit
    def write(index):
        try:
            return tables[index].write(pd.DataFrame({'id':[1],'value':[index]}))
        except ConcurrentCommitError:
            return 'conflict'
    try:
        with ThreadPoolExecutor(max_workers=2) as pool:
            results=list(pool.map(write,[0,1]))
        assert results.count(0)==1
        assert results.count('conflict')==1
        assert len(tables[0].get_history())==1
        assert len(tables[0].read())==1
    finally:
        for table in tables:table.close()
