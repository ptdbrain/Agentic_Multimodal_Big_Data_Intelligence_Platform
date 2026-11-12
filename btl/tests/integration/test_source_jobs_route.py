"""Real PostgreSQL schema binding and MinIO namespace; no public source claim."""

import pytest
from tests.integration.test_processing_contracts import postgres_scope, remote_storage

pytestmark = pytest.mark.integration


def test_source_preflight_binds_actual_schema_and_closes_warehouse(tmp_path, remote_storage):
    from config.settings import settings
    from database.warehouse import WarehouseManager
    from orchestration.source_jobs import preflight_target

    warehouse = WarehouseManager(mode="integration")
    try:
        cursor = warehouse._conn.cursor()
        cursor.execute("SELECT current_schema()")
        schema = cursor.fetchone()[0]
    finally:
        warehouse.close()
    job = {
        "namespace": "preflight",
        "source_group": "web_content",
        "target": {"bucket": remote_storage, "postgres_schema": schema},
    }
    entry = preflight_target({"root": str(tmp_path / "registry")}, job)
    assert entry["target"]["db_schema"] == schema
    assert entry["target"]["bucket"] == remote_storage
    assert entry["target"]["db_port"] == settings.database.port
    job["target"]["postgres_schema"] = "wrong_schema"
    with pytest.raises(ValueError, match="schema target mismatch"):
        preflight_target({"root": str(tmp_path / "registry")}, job)


def test_recovery_identity_pins_actual_sql_schema_and_object_transport(remote_storage,monkeypatch):
    from operations.recovery import _backend_identity
    first=_backend_identity('integration')
    assert first['warehouse_schema'].startswith('processing_')
    assert first['secure'] is False
    monkeypatch.setenv('PGOPTIONS','-c search_path=public')
    second=_backend_identity('integration')
    assert second['warehouse_schema']=='public' and first!=second


def test_full_database_backup_refuses_a_lease_in_another_schema(remote_storage):
    import uuid
    from database.warehouse import WarehouseManager
    from operations.backup import maintenance_guard
    schema='backup_lease_'+uuid.uuid4().hex
    warehouse=WarehouseManager(mode='integration')
    try:
        warehouse.execute('CREATE SCHEMA '+schema)
        warehouse.execute('CREATE TABLE '+schema+'.pipeline_run_lease(resource TEXT PRIMARY KEY,run_id TEXT)')
        warehouse.execute("INSERT INTO "+schema+".pipeline_run_lease VALUES('canonical','interrupted-other-namespace')")
        with pytest.raises(ValueError,match='lease'):
            with maintenance_guard('integration',all_schemas=True):pass
    finally:
        warehouse.execute('DROP SCHEMA '+schema+' CASCADE');warehouse.close()
