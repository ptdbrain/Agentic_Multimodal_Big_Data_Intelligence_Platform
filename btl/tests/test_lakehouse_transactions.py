import os
import sys
from pathlib import Path
import pandas as pd
import pytest
from storage.storage_manager import StorageManager
from storage.lakehouse import LakehouseTable

pytestmark = pytest.mark.unit


def test_schema_mismatch_is_rejected_without_new_version(tmp_path):
    t=LakehouseTable('test',storage_backend=StorageManager(base_dir=tmp_path,mode='test'))
    t.write(pd.DataFrame({'id':[1],'value':[100]}))
    with pytest.raises(ValueError,match='schema'):
        t.write(pd.DataFrame({'other':['wrong']}))
    assert t.read().to_dict('records') == [{'id':1,'value':100}]
    assert len(t.get_history()) == 1


def test_operation_retry_is_idempotent_and_time_travel_survives(tmp_path):
    t=LakehouseTable('test',storage_backend=StorageManager(base_dir=tmp_path,mode='test'))
    data=pd.DataFrame({'id':[1],'value':[100]})
    v=t.write(data,operation_id='op1')
    assert t.write(data,operation_id='op1') == v
    t.upsert(pd.DataFrame({'id':[1],'value':[200]}),['id'])
    assert t.read(version=v)['value'].tolist() == [100]
    assert t.read()['value'].tolist() == [200]
    assert len(t.get_history()) == 2


def test_compare_and_swap_rejects_second_writer(tmp_path):
    from storage.lakehouse_metadata import MetadataStore, ConcurrentCommitError
    a=MetadataStore(sqlite_path=tmp_path/'meta.sqlite')
    b=MetadataStore(sqlite_path=tmp_path/'meta.sqlite')
    a.commit('t',-1,'a.parquet','{}','op1')
    with pytest.raises(ConcurrentCommitError):
        b.commit('t',-1,'b.parquet','{}','op2')
    assert a.get_commit('t')['data_file'] == 'a.parquet'


def test_two_process_writers_cannot_commit_same_version(tmp_path):
    import subprocess
    code='''
import sys
from storage.lakehouse_metadata import MetadataStore, ConcurrentCommitError
m=MetadataStore(sqlite_path=sys.argv[1])
try:
 m.commit('shared',-1,sys.argv[2],'{}',sys.argv[2])
except ConcurrentCommitError:
 sys.exit(3)
finally:
 m.close()
'''
    repo_root = str(Path(__file__).resolve().parents[1])
    child_env = dict(os.environ)
    child_env['PYTHONPATH'] = repo_root + os.pathsep + child_env.get('PYTHONPATH', '')
    a=subprocess.Popen([sys.executable,'-c',code,str(tmp_path/'catalog.sqlite'),'one'], cwd=repo_root, env=child_env)
    b=subprocess.Popen([sys.executable,'-c',code,str(tmp_path/'catalog.sqlite'),'two'], cwd=repo_root, env=child_env)
    assert sorted([a.wait(timeout=15),b.wait(timeout=15)]) == [0,3]


def test_metadata_idempotency_checks_intent_inside_transaction(tmp_path):
    from storage.lakehouse_metadata import MetadataStore
    m=MetadataStore(sqlite_path=tmp_path/'meta.sqlite')
    m.commit('t',-1,'one','{}','same',payload_hash='first',operation='WRITE',mode='append')
    with pytest.raises(ValueError,match='Operation'):
        m.commit('t',-1,'two','{}','same',payload_hash='different',operation='WRITE',mode='append')
    m.close()
