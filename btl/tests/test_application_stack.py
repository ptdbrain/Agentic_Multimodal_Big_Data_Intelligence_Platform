"""The application deployment must run complete, isolated vendor services."""

from pathlib import Path
import pytest, yaml

pytestmark = pytest.mark.unit
ROOT = Path(__file__).resolve().parents[1]


def test_complete_stack_has_app_stream_search_and_bounded_private_services():
    config = yaml.safe_load((ROOT / "docker-compose.yml").read_text())
    services = config["services"]
    required = {
        "kafka",
        "minio",
        "postgres",
        "elasticsearch",
        "spark-master",
        "spark-worker1",
        "spark-worker2",
        "airflow-init",
        "airflow",
        "dashboard",
        "streaming",
        "search-worker",
        "alert-worker",
        "prometheus",
        "alertmanager",
        "grafana",
        "metrics",
        "dependencies",
        "operations",
        "receiver",
    }
    assert required.issubset(services), required - set(services)
    for name, service in services.items():
        assert "container_name" not in service, "Project isolation requires Compose names"
        for port in service.get("ports", []):
            assert str(port).startswith("127.0.0.1:"), name

    def memory(value):
        value = str(value).lower()
        return float(value[:-1]) * (1024 if value.endswith("g") else 1)

    assert sum(memory(s["mem_limit"]) for s in services.values()) <= 16 * 1024
    assert "bitnami" not in str(config)
    assert services["dashboard"]["ports"] == ["127.0.0.1:${DASHBOARD_PORT:-18502}:8502"]
    assert services["streaming"].get("profiles") == ["streaming"]


def test_project_and_spark_workers_share_python_runtime():
    text = (ROOT / "docker/application.Dockerfile").read_text()
    assert "/opt/project-venv" in text and "/opt/airflow-venv" in text
    assert "PYSPARK_PYTHON=/opt/project-venv/bin/python" in text
    assert "java-17" in text and "apache/spark:3.5.0" in text


def test_fresh_application_initializes_namespaces_before_scheduler_and_dashboard():
    config = yaml.safe_load((ROOT / "docker-compose.yml").read_text())
    services = config["services"]
    assert "application-init" in services
    for name in ("airflow-init", "dashboard", "search-worker", "alert-worker", "streaming"):
        assert services[name]["depends_on"]["application-init"]["condition"] == "service_completed_successfully"
    assert services["airflow"]["environment"]["AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION"] == "true"


def test_executor_python_can_import_project_modules_outside_app_working_directory():
    dockerfile = (ROOT / 'docker/application.Dockerfile').read_text()
    assert 'PYTHONPATH=/app' in dockerfile
