"""HTTP service gauges are backed by actual native/Docker dependency probes."""
import threading
import requests
import pytest
from monitoring.metrics_exporter import create_metrics_server
from monitoring.services_exporter import ServiceMetricsExporter

pytestmark = pytest.mark.integration


def test_actual_service_gauges_are_served_over_http():
    server = create_metrics_server(port=0,metrics_provider=ServiceMetricsExporter.generate_prometheus_metrics)
    thread = threading.Thread(target=server.serve_forever,daemon=True)
    thread.start()
    try:
        response = requests.get(f'http://127.0.0.1:{server.server_port}/metrics',timeout=15)
        assert response.status_code == 200
        for service in ['kafka','minio','postgres','elasticsearch']:
            assert f'sentinel_dependency_up{{service="{service}"}} 1' in response.text
        assert 'sentinel_kafka_broker_count 1' in response.text
        assert 'sentinel_postgres_database_size_bytes NaN' not in response.text
        assert 'sentinel_elasticsearch_health_code NaN' not in response.text
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=3)
