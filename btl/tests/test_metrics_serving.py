"""Metrics should expose valid numbers and a real scrape endpoint."""
import threading
from urllib.request import urlopen
import pytest
from monitoring import metrics_exporter

pytestmark = pytest.mark.unit


def test_empty_quality_is_nan_not_none_or_perfect_score():
    text = metrics_exporter.SentinelMetricsExporter.generate_prometheus_metrics(mode='test')
    quality = next(line for line in text.splitlines() if line.startswith('sentinel_data_quality_score '))
    assert quality.split()[-1] == 'NaN'
    assert 'sentinel_active_anomalies' not in text
    assert 'sentinel_recent_anomalies' in text


def test_http_endpoint_returns_metrics_and_404_for_unknown_path():
    with metrics_exporter.create_metrics_server('127.0.0.1', 0, mode='test') as server:
        worker = threading.Thread(target=server.serve_forever, daemon=True)
        worker.start()
        try:
            with urlopen(f'http://127.0.0.1:{server.server_port}/metrics', timeout=5) as response:
                assert response.status == 200
                assert response.headers['Content-Type'].startswith('text/plain')
                assert b'sentinel_warehouse_records' in response.read()
            from urllib.error import HTTPError
            with pytest.raises(HTTPError) as error:
                urlopen(f'http://127.0.0.1:{server.server_port}/unknown', timeout=5)
            assert error.value.code == 404
        finally:
            server.shutdown()
            worker.join(timeout=5)
