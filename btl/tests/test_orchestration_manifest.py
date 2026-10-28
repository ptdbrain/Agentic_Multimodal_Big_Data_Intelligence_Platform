import json
import subprocess
import sys
import pytest

pytestmark = pytest.mark.unit


def test_manifest_survives_process_boundary(tmp_path):
    from orchestration.run_manifest import RunManifest, save_manifest
    from config.settings import settings
    from storage.storage_manager import storage
    manifest=RunManifest(run_id='run-1',mode='test',required_datasets=['products','reviews'])
    path=save_manifest(manifest)
    full=storage.base_dir/path
    result=subprocess.run([sys.executable,'-c','import json,sys; print(json.load(open(sys.argv[1]))["run_id"])',str(full)],capture_output=True,text=True)
    assert result.returncode == 0
    assert result.stdout.strip() == 'run-1'
    assert json.loads(full.read_text())['mode'] == 'test'


def test_task_rejects_missing_manifest():
    from orchestration.dags.sentinel_daily_pipeline import task_spark_batch
    with pytest.raises(ValueError,match='manifest'):
        task_spark_batch()


def test_start_retry_preserves_existing_manifest():
    from orchestration.dags.sentinel_daily_pipeline import task_start
    from orchestration.run_manifest import load_manifest,save_manifest
    first=task_start(run_id='retry-run')
    m=load_manifest(first['manifest_path']);m.batches_by_topic={'raw.reviews':['bronze/confirmed.json']}
    save_manifest(m)
    again=task_start(run_id='retry-run')
    assert load_manifest(again['manifest_path']).batches_by_topic==m.batches_by_topic


def test_released_lease_cannot_be_reacquired_by_downstream():
    from orchestration.dags.sentinel_daily_pipeline import task_start,_manifest
    from orchestration.run_manifest import RunLease
    result=task_start(run_id='done-run')
    lease=RunLease('test');lease.release('done-run');lease.close()
    with pytest.raises(RuntimeError,match='owner'):
        _manifest({'manifest_path':result['manifest_path']})


def test_cli_respects_dag_canonical_lease(monkeypatch):
    from orchestration.run_manifest import RunLease
    import scripts.run_e2e_pipeline as pipeline
    lease=RunLease('test');lease.acquire('dag-owner');lease.close()
    monkeypatch.setattr(pipeline,'stage_1_ingest_sources',lambda **kw: pytest.fail('CLI bypassed DAG owner'))
    with pytest.raises(RuntimeError,match='owns canonical'):
        pipeline.run_end_to_end(mode='test')
