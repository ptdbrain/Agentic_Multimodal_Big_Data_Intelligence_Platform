"""Registry metrics must describe each real connection and preserve missing state."""

import sqlite3
import pytest

pytestmark = pytest.mark.unit


def test_registry_metrics_count_only_bound_provenance_and_actual_publication(tmp_path):
    from serving.registry import DatasetRegistry
    from serving.store import ServingStore
    from monitoring.serving_exporter import collect_binding, generate_metrics
    from tests.test_serving_registry import registry_file

    registry = DatasetRegistry.load(registry_file(tmp_path))
    binding = registry.datasets["tiki"]
    with sqlite3.connect(binding.sqlite_path) as connection:
        connection.execute("CREATE TABLE products(product_id TEXT,source_group TEXT,acquisition_method TEXT)")
        connection.execute("CREATE TABLE reviews(review_id TEXT,source_group TEXT,acquisition_method TEXT)")
        connection.executemany(
            "INSERT INTO products VALUES(?,?,?)",
            [("observed", "marketplace", "live"), ("wrong", "simulation", "generated")],
        )
        store = ServingStore(connection, binding.id, binding.source_group)
        store.initialize()
        store.stage_publication("g", "r", "commerce")
    result = collect_binding(binding)
    assert result["up"] == 1
    assert result["records"] == {"products": 1, "reviews": 0}
    assert result["publication_present"] == 1 and result["published_at"] > 0
    assert result["search_pending"] == 2
    assert result["lake_up"] == 0
    metrics = generate_metrics(registry).decode()
    assert 'sentinel_dataset_records{dataset_id="tiki",entity="products",source_group="marketplace"} 1.0' in metrics
    assert "POSTGRES_PASSWORD" not in metrics


def test_missing_dataset_does_not_create_a_database_or_invent_zero_records(tmp_path):
    from serving.registry import DatasetRegistry
    from monitoring.serving_exporter import collect_binding
    from tests.test_serving_registry import registry_file

    binding = DatasetRegistry.load(registry_file(tmp_path)).datasets["tiki"]
    assert not binding.sqlite_path.exists()
    result = collect_binding(binding)
    assert result["up"] == 0 and result["records"] == {}
    assert not binding.sqlite_path.exists()


def test_source_capture_failure_before_sql_publication_is_visible_and_recovery_uses_real_receipt(tmp_path):
    """Transport-free metadata test: real durable failure/recovery journals, no live collection claim."""
    import copy
    from monitoring.serving_exporter import collect_source_state
    from serving.registry import DatasetRegistry
    from tests.test_serving_registry import registry_file
    from orchestration.source_jobs import execute_job, validate_config, bind_target

    binding = DatasetRegistry.load(registry_file(tmp_path)).datasets["tiki"]
    config = validate_config(
        {
            "version": 1,
            "root": str(tmp_path / "jobs"),
            "jobs": [
                {
                    "id": "tiki",
                    "adapter": "tiki",
                    "source_group": "marketplace",
                    "namespace": "tiki",
                    "owner": "airflow",
                    "schedule": "0 9 * * *",
                    "minimum_records": 1,
                    "target": {"bucket": binding.bucket, "postgres_schema": binding.schema},
                }
            ],
        }
    )

    def preflight(config, job):
        return bind_target(
            config,
            job,
            {
                "lake_endpoint": "fixture:9000",
                "bucket": binding.bucket,
                "db_host": "fixture",
                "db_port": 5432,
                "db_name": "fixture",
                "db_schema": binding.schema,
            },
        )

    def failed_capture(*args):
        raise RuntimeError("Explicit failed capture fixture before any SQL publication")

    with pytest.raises(RuntimeError):
        execute_job(config, "tiki", "failed", capture=failed_capture, preflight=preflight)
    state = collect_source_state(binding, config)
    assert state["source_state_up"] == 1 and state["source_unresolved_failure"] == 1
    assert state["source_last_failure_timestamp_seconds"] > 0
    assert state["source_last_processed_timestamp_seconds"] is None
    execute_job(
        config,
        "tiki",
        "recovered",
        capture=lambda *args: {"status": "complete", "records": [{"product_id": "explicit-metadata-fixture"}]},
        process=lambda *args: {"completed": True},
        preflight=preflight,
    )
    recovered = collect_source_state(binding, config)
    assert recovered["source_unresolved_failure"] == 0
    assert recovered["source_last_processed_timestamp_seconds"] > recovered["source_last_failure_timestamp_seconds"]
    mismatched = copy.deepcopy(config)
    mismatched["jobs"][0]["target"]["bucket"] = "different-dataset"
    assert collect_source_state(binding, mismatched)["source_state_up"] == 0
