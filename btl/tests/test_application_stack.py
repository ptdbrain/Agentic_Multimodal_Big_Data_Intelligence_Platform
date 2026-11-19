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
    dockerfile = (ROOT / "docker/application.Dockerfile").read_text()
    assert "PYTHONPATH=/app" in dockerfile


@pytest.mark.parametrize("service", ["dependencies", "airflow", "airflow-init"])
def test_dependency_exporter_probes_the_elasticsearch_service_endpoint(service):
    from urllib.parse import urlsplit

    services = yaml.safe_load((ROOT/'docker-compose.yml').read_text())['services']
    env = services[service]['environment']
    endpoint = env.get('ES_URL') or f"http://{env.get('ES_HOST','localhost')}:{env.get('ES_PORT','9200')}"
    assert urlsplit(endpoint).hostname == 'elasticsearch'
    assert urlsplit(endpoint).port == 9200


def test_continuous_stream_leaves_capacity_for_scheduled_batch():
    import shlex

    services = yaml.safe_load((ROOT / "docker-compose.yml").read_text())["services"]
    workers = [services[name]["command"] for name in ("spark-worker1", "spark-worker2")]
    capacity = sum(int(command[command.index("--cores") + 1]) for command in workers)
    args = shlex.split(services["streaming"]["environment"]["PYSPARK_SUBMIT_ARGS"])
    options = dict(token.split("=", 1) for token in args if token.startswith("spark."))
    executor = int(options["spark.executor.cores"])
    assert executor <= int(options["spark.cores.max"]) <= capacity - executor
