import pytest
from scripts.health_check import check_spark
from spark.spark_session import SparkSessionFactory

pytestmark = pytest.mark.unit


def test_spark_health_requires_successful_action(monkeypatch):
    monkeypatch.setattr(SparkSessionFactory,'get_spark_session',lambda **kw: None)
    assert check_spark() is False


def test_spark_health_does_not_hide_action_failure(monkeypatch):
    class Broken:
        def range(self,n): raise RuntimeError('JVM died')
        def stop(self): pass
    monkeypatch.setattr(SparkSessionFactory,'get_spark_session',lambda **kw: Broken())
    assert check_spark() is False


def test_preflight_rejects_failed_spark_health(monkeypatch):
    import scripts.health_check as health
    monkeypatch.setattr(health,'check_minio',lambda **kw: True)
    monkeypatch.setattr(health,'check_postgres',lambda **kw: True)
    monkeypatch.setattr(health,'check_spark',lambda: False)
    with pytest.raises(ConnectionError,match='Spark'):
        health.verify_integration_health()
