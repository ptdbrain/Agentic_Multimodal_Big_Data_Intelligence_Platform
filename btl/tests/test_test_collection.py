"""The pure unit gate must collect on a machine without PySpark."""
import os
import subprocess
import sys
from pathlib import Path
import pytest

pytestmark = pytest.mark.unit


def test_unit_collection_without_pyspark(tmp_path):
    (tmp_path / 'sitecustomize.py').write_text(
        'import sys\nclass NoSpark:\n'
        ' def find_spec(self, fullname, path=None, target=None):\n'
        '  if fullname == "pyspark" or fullname.startswith("pyspark."):\n'
        '   raise ModuleNotFoundError("PySpark deliberately unavailable")\n'
        'sys.meta_path.insert(0, NoSpark())\n'
    )
    env = dict(os.environ, PYTHONPATH=str(tmp_path))
    result = subprocess.run(
        [sys.executable, '-m', 'pytest', '--collect-only', '-q', '-m', 'unit and not spark'],
        cwd=Path(__file__).resolve().parents[1], env=env, capture_output=True, text=True,
    )
    assert result.returncode == 0, result.stdout + result.stderr
