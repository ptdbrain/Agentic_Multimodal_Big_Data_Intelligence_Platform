"""Explicit dataset bindings must not mutate process configuration or mix sessions."""

import os
from pathlib import Path
import pytest, yaml

pytestmark = pytest.mark.unit


def registry_file(tmp_path):
    records = []
    for key, kind, group in [
        ("tiki", "commerce", "marketplace"),
        ("web", "web", "web_content"),
        ("simulation", "commerce", "simulation"),
    ]:
        env = tmp_path / (key + ".env")
        env.write_text("RUNTIME_MODE=test\nSQLITE_DB_PATH=" + str(tmp_path / (key + ".sqlite")) + "\n")
        env.chmod(0o600)
        records.append(
            {
                "id": key,
                "label": key.title(),
                "kind": kind,
                "source_group": group,
                "postgres_schema": key,
                "bucket": key + "-bucket",
                "environment_file": str(env),
            }
        )
    path = tmp_path / "datasets.yml"
    path.write_text(yaml.safe_dump({"version": 1, "datasets": records}))
    path.chmod(0o600)
    return path


def test_registry_explicit_local_bindings_are_session_isolated(tmp_path):
    from serving.registry import DatasetRegistry, ServingContext
    from dashboard.db_connector import DashboardDB

    path = registry_file(tmp_path)
    before = dict(os.environ)
    registry = DatasetRegistry.load(path)
    a = ServingContext(registry, commerce="tiki", articles="web", scope="real")
    b = ServingContext(registry, commerce="simulation", articles=None, scope="simulation")
    assert a.resolve("products").id == "tiki" and a.resolve("articles").id == "web"
    assert b.resolve("reviews").id == "simulation"
    assert dict(os.environ) == before
    with pytest.raises(ValueError, match="scope"):
        ServingContext(registry, commerce="simulation", articles=None, scope="real")
    first = DashboardDB(binding=a.resolve("products"), data_scope="real")
    second = DashboardDB(binding=b.resolve("reviews"), data_scope="simulation")
    try:
        first._conn.execute("CREATE TABLE independent(v TEXT)")
        first._conn.execute("INSERT INTO independent VALUES('tiki')")
        first._conn.commit()
        assert second._conn.execute("SELECT name FROM sqlite_master WHERE name='independent'").fetchall() == []
    finally:
        first.close()
        second.close()
    assert dict(os.environ) == before


def test_registry_refuses_shared_schema_and_insecure_secret_file(tmp_path):
    from serving.registry import DatasetRegistry

    path = registry_file(tmp_path)
    value = yaml.safe_load(path.read_text())
    for item in value["datasets"]:
        env = Path(item["environment_file"])
        env.write_text(
            "POSTGRES_HOST=host\nPOSTGRES_PORT=5432\nPOSTGRES_USER=reader\nPOSTGRES_PASSWORD=private-value\nPOSTGRES_DB=dw\n"
        )
        item["postgres_schema"] = "same"
    path.write_text(yaml.safe_dump(value))
    with pytest.raises(ValueError, match="shared|Shared"):
        DatasetRegistry.load(path)
    value["datasets"][1]["postgres_schema"] = "web"
    value["datasets"][2]["postgres_schema"] = "sim"
    path.write_text(yaml.safe_dump(value))
    Path(value["datasets"][0]["environment_file"]).chmod(0o644)
    with pytest.raises(ValueError, match="private|Private"):
        DatasetRegistry.load(path)


def test_dashboard_routes_web_and_commerce_without_opening_a_global_connection(tmp_path):
    import sqlite3
    from serving.registry import DatasetRegistry, ServingContext, DashboardSources

    registry = DatasetRegistry.load(registry_file(tmp_path))
    for key in ("tiki", "web"):
        with sqlite3.connect(registry.datasets[key].sqlite_path) as connection:
            table = "articles" if key == "web" else "products"
            connection.execute(f"CREATE TABLE {table}(source_group TEXT,acquisition_method TEXT,title TEXT)")
            connection.execute(
                f"INSERT INTO {table} VALUES(?,?,?)", ("web_content" if key == "web" else "marketplace", "live", key)
            )
    sources = DashboardSources(ServingContext(registry, "tiki", "web", "real"))
    try:
        assert sources.read_table("products")["title"].tolist() == ["tiki"]
        assert sources.read_table("articles")["title"].tolist() == ["web"]
        assert len(sources.connections) == 2
    finally:
        sources.close()


def test_stream_metadata_remains_visible_for_selected_observed_dataset(tmp_path):
    import sqlite3
    from serving.registry import DatasetRegistry
    from dashboard.db_connector import DashboardDB

    registry = DatasetRegistry.load(registry_file(tmp_path))
    binding = registry.datasets["tiki"]
    with sqlite3.connect(binding.sqlite_path) as c:
        c.execute("CREATE TABLE stream_deployments(dataset_id TEXT,generation TEXT,state TEXT)")
        c.execute("INSERT INTO stream_deployments VALUES('tiki','g','active')")
        c.execute("INSERT INTO stream_deployments VALUES('foreign','bad','active')")
    db = DashboardDB(binding=binding, data_scope="real")
    try:
        assert db.read_table("stream_deployments")["generation"].tolist() == ["g"]
    finally:
        db.close()


def test_diagnostic_scope_still_cannot_read_another_dataset_metadata(tmp_path):
    import sqlite3
    from serving.registry import DatasetRegistry
    from dashboard.db_connector import DashboardDB

    binding = DatasetRegistry.load(registry_file(tmp_path)).datasets["tiki"]
    with sqlite3.connect(binding.sqlite_path) as c:
        c.execute("CREATE TABLE stream_deployments(dataset_id TEXT,generation TEXT,state TEXT)")
        c.executemany(
            "INSERT INTO stream_deployments VALUES(?,?,?)", [("tiki", "good", "active"), ("foreign", "bad", "active")]
        )
    db = DashboardDB(binding=binding, data_scope="all")
    try:
        assert db.read_table("stream_deployments")["generation"].tolist() == ["good"]
    finally:
        db.close()


def test_direct_dataset_reference_cannot_bypass_sql_identity_validation(tmp_path):
    from dataclasses import replace
    from serving.registry import DatasetRegistry

    binding = DatasetRegistry.load(registry_file(tmp_path)).datasets["tiki"]
    with pytest.raises(ValueError, match="identity|ID"):
        replace(binding, id="x' OR TRUE --")
    with pytest.raises(ValueError, match="schema"):
        replace(binding, schema="public; DROP SCHEMA source_tiki")


def test_remote_lake_prefix_keeps_directory_boundary(tmp_path):
    from serving.lake import DatasetLake

    seen = []

    class Client:
        def list_objects(self, bucket, *, prefix, recursive):
            seen.append(prefix)
            return []

    lake = object.__new__(DatasetLake)
    lake.root = None
    lake.binding = type("Binding", (), {"bucket": "fixture"})()
    lake.client = Client()
    assert list(lake.objects("bronze/reviews/")) == []
    assert seen == ["bronze/reviews/"]


def test_absent_optional_table_is_not_queried_and_cannot_abort_read_snapshot(tmp_path, monkeypatch):
    from serving.registry import DatasetRegistry
    from dashboard.db_connector import DashboardDB

    binding = DatasetRegistry.load(registry_file(tmp_path)).datasets["tiki"]
    db = DashboardDB(binding=binding, data_scope="real")

    def query_absent_table(*args, **kwargs):
        raise RuntimeError("Undefined table would abort a PostgreSQL snapshot")

    monkeypatch.setattr(db, "query", query_absent_table)
    try:
        assert db.read_table("stream_deployments").empty
        assert db.read_table("stream_windows").empty
    finally:
        db.close()
