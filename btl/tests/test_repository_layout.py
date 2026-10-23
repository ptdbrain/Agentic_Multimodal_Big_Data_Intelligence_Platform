"""The source tree lives in btl while repository configuration remains above it."""
import importlib.util
from pathlib import Path
import yaml
import pytest

pytestmark = pytest.mark.unit


def test_ci_runs_commands_from_source_directory():
    root=Path(__file__).resolve().parents[2]
    workflow=yaml.safe_load((root/'.github/workflows/ci.yml').read_text())
    assert workflow['defaults']['run']['working-directory']=='btl'
    for job in workflow['jobs'].values():
        for step in job['steps']:
            if step.get('uses','').startswith('actions/setup-python@'):
                assert step['with']['cache-dependency-path']=='btl/requirements.txt'
    assert 'btl/orchestration/dags' in (root/'.github/workflows/ci.yml').read_text()


def test_environment_file_follows_moved_source_tree(tmp_path,monkeypatch):
    path=Path(__file__).resolve().parents[1]/'config/settings.py'
    spec=importlib.util.spec_from_file_location('layout_settings',path)
    module=importlib.util.module_from_spec(spec)
    import sys
    monkeypatch.setitem(sys.modules,'layout_settings',module)
    spec.loader.exec_module(module)
    root=tmp_path/'repository';root.mkdir();(root/'.git').mkdir();(root/'btl').mkdir()
    (root/'.env').write_text('LAYOUT_ONLY=yes\n')
    assert module.resolve_env_file(root/'btl')==root/'.env'
    (root/'btl'/'.env').write_text('LAYOUT_ONLY=override\n')
    assert module.resolve_env_file(root/'btl')==root/'btl'/'.env'


def test_relocated_lake_outputs_are_ignored():
    import subprocess
    root=Path(__file__).resolve().parents[2]
    result=subprocess.run(['git','check-ignore','--no-index',
                           'btl/storage/datalake/silver/test.parquet',
                           'btl/hadoop/jars/test.jar'],cwd=root,capture_output=True,text=True)
    assert len(result.stdout.splitlines())==2
