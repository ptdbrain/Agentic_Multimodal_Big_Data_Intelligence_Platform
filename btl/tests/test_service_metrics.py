"""Dependency failures are reported as unavailable, never as fabricated counts."""
import pytest

pytestmark = pytest.mark.unit


def test_failed_dependency_does_not_publish_a_zero_broker_count_or_error_secrets(monkeypatch):
    from monitoring.services_exporter import ServiceMetricsExporter

    def unavailable():
        raise RuntimeError('secret-example-must-not-be-exposed')

    monkeypatch.setattr(ServiceMetricsExporter,'probes',staticmethod(lambda:{'kafka':unavailable}))
    metrics = ServiceMetricsExporter.generate_prometheus_metrics()
    assert 'sentinel_dependency_up{service="kafka"} 0' in metrics
    assert 'sentinel_kafka_broker_count NaN' in metrics
    assert 'secret-example' not in metrics


def test_dependency_gauges_keep_observed_zero_distinct_from_failed_probe(monkeypatch):
    from monitoring.services_exporter import ServiceMetricsExporter
    monkeypatch.setattr(ServiceMetricsExporter,'probes',staticmethod(lambda:{
        'kafka':lambda:{'broker_count':2}, 'minio':lambda:{'configured_bucket_exists':False},
        'postgres':lambda:{'database_size_bytes':12345},'elasticsearch':lambda:{'health_code':1},
    }))
    metrics = ServiceMetricsExporter.generate_prometheus_metrics()
    for service in ['kafka','minio','postgres','elasticsearch']:
        assert f'sentinel_dependency_up{{service="{service}"}} 1' in metrics
    assert 'sentinel_kafka_broker_count 2' in metrics
    assert 'sentinel_minio_configured_bucket_exists 0' in metrics
    assert 'sentinel_postgres_database_size_bytes 12345' in metrics
    assert 'sentinel_elasticsearch_health_code 1' in metrics
