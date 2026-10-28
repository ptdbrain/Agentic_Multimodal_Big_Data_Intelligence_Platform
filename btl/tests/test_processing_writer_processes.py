"""Two OS processes compete for canonical output ownership before any write."""
import os
from pathlib import Path
import selectors
import subprocess
import sys

import pytest

pytestmark = pytest.mark.unit


def test_competing_pipeline_process_cannot_publish_while_another_owns_outputs(tmp_path):
    project = Path(__file__).resolve().parents[1]
    script = '''
import sys
from pathlib import Path
from config.settings import settings
settings.set_runtime_mode('test')
settings.database.sqlite_path = Path(sys.argv[1])
from orchestration.run_manifest import RunLease
lease = RunLease('test')
try:
    lease.acquire(sys.argv[2])
except RuntimeError:
    print('BLOCKED', flush=True)
    lease.close()
    raise SystemExit(0)
try:
    print('OWNED', flush=True)
    if sys.argv[2] == 'first':
        assert sys.stdin.readline().strip() == 'publish'
    with Path(sys.argv[3]).open('a') as stream:
        stream.write(sys.argv[2] + '\\n')
finally:
    lease.release(sys.argv[2])
    lease.close()
'''
    command = [sys.executable, '-u', '-c', script, str(tmp_path/'lease.sqlite')]
    evidence = tmp_path/'published.txt'
    first = subprocess.Popen(command + ['first', str(evidence)], cwd=project, env=os.environ.copy(),
                             stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    try:
        with selectors.DefaultSelector() as poll:
            poll.register(first.stdout, selectors.EVENT_READ)
            assert poll.select(timeout=15), 'First writer did not acquire its actual SQL lease'
        assert first.stdout.readline().strip() == 'OWNED'
        second = subprocess.run(command + ['second', str(evidence)], cwd=project, capture_output=True, text=True, timeout=15)
        assert second.returncode == 0, second.stderr
        assert second.stdout.strip() == 'BLOCKED'
        assert not evidence.exists()
        _, error = first.communicate('publish\n', timeout=15)
        assert first.returncode == 0, error
        third = subprocess.run(command + ['third', str(evidence)], cwd=project, capture_output=True, text=True, timeout=15)
        assert third.returncode == 0, third.stderr
        assert third.stdout.strip() == 'OWNED'
        assert evidence.read_text().splitlines() == ['first', 'third']
    finally:
        if first.poll() is None:
            first.kill()
            first.communicate()
