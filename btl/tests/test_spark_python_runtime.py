"""A cached factory import must not leave new workers using system Python."""
import os
from pathlib import Path
import subprocess
import sys
import pytest

pytestmark = [pytest.mark.unit, pytest.mark.spark]


def test_new_session_reestablishes_python_defaults_after_environment_reset():
    code = '''
import os
from spark.spark_session import SparkSessionFactory
os.environ.pop('PYSPARK_PYTHON', None)
os.environ.pop('PYSPARK_DRIVER_PYTHON', None)
spark = SparkSessionFactory.get_spark_session(master='local[1]')
assert spark is not None
try:
    assert spark.sparkContext.parallelize([1]).map(lambda x: x + 1).collect() == [2]
finally:
    spark.stop()
'''
    result = subprocess.run([sys.executable, '-c', code], cwd=Path(__file__).resolve().parents[1],
                            env=dict(os.environ), capture_output=True, text=True, timeout=90)
    assert result.returncode == 0, result.stderr[-4000:]
