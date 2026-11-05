"""Recovery artifacts must be complete, immutable and safe to restore."""

from contextlib import contextmanager
from pathlib import Path
import json
import pytest

pytestmark = pytest.mark.unit


@contextmanager
def quiet():
    yield {"writers_stopped": True, "drained": True, "lease_count": 0}


def components(tmp_path):
    result = []
    for role in ["warehouse", "lake", "checkpoints", "orchestration", "registry"]:
        path = tmp_path / ("source_" + role)
        path.mkdir()
        (path / "record").write_text(role)
        result.append({"name": role, "role": role, "kind": "directory", "path": str(path)})
    return result


def test_complete_roundtrip_is_verified_and_preserves_sources(tmp_path):
    from operations.backup import create_backup, verify_backup, restore_plan, restore_apply, restore_verify

    source = components(tmp_path)
    backup = create_backup(tmp_path / "backups", source, quiet)
    manifest = verify_backup(backup)
    assert manifest["schema_version"] == 1 and manifest["status"] == "complete"
    assert manifest["disaster_recovery_certified"] is False
    targets = {row["name"]: {"path": str(tmp_path / "restored" / row["name"])} for row in source}
    plan = restore_plan(backup, targets)
    assert restore_apply(backup, plan, quiet)["verified"] is True
    assert restore_verify(backup, targets)["verified"] is True
    for row in source:
        assert Path(row["path"], "record").read_text() == row["role"]
        assert (tmp_path / "restored" / row["name"] / "record").read_text() == row["role"]


def test_missing_guard_and_live_writer_prevent_backup(tmp_path):
    from operations.backup import create_backup

    source = components(tmp_path)

    @contextmanager
    def busy():
        yield {"writers_stopped": False, "drained": False, "lease_count": 1}

    for guard in [None, busy]:
        with pytest.raises(ValueError):
            create_backup(tmp_path / "backups", source, guard)


def test_corrupt_backup_is_rejected_before_restore(tmp_path):
    from operations.backup import create_backup, verify_backup, restore_plan

    backup = create_backup(tmp_path / "backups", components(tmp_path), quiet)
    next((backup / "payload").rglob("record")).write_text("tampered")
    with pytest.raises(ValueError, match="checksum"):
        verify_backup(backup)
    with pytest.raises(ValueError):
        restore_plan(backup, {})


def test_symlink_and_traversal_payloads_are_rejected(tmp_path):
    from operations.backup import create_backup, verify_backup

    source = components(tmp_path)
    Path(source[0]["path"], "escape").symlink_to(tmp_path / "source_lake" / "record")
    with pytest.raises(ValueError, match="symlink"):
        create_backup(tmp_path / "backups", source, quiet)
    assert not list((tmp_path / "backups").glob("backup-*"))
    Path(source[0]["path"], "escape").unlink()
    backup = create_backup(tmp_path / "backups", source, quiet)
    manifest = json.loads((backup / "manifest.json").read_text())
    manifest["components"][0]["files"][0]["path"] = "../../outside"
    (backup / "manifest.json").write_text(json.dumps(manifest))
    with pytest.raises(ValueError, match="path"):
        verify_backup(backup)


def test_restore_refuses_existing_destinations_before_first_write(tmp_path):
    from operations.backup import create_backup, restore_plan, restore_apply

    source = components(tmp_path)
    backup = create_backup(tmp_path / "backups", source, quiet)
    targets = {row["name"]: {"path": str(tmp_path / "restore" / row["name"])} for row in source}
    plan = restore_plan(backup, targets)
    Path(targets["lake"]["path"]).mkdir(parents=True)
    with pytest.raises(ValueError, match="exist"):
        restore_apply(backup, plan, quiet)
    assert not Path(targets["warehouse"]["path"]).exists()


def test_interrupted_copy_never_publishes_complete_backup(tmp_path, monkeypatch):
    import operations.backup as module

    source = components(tmp_path)

    def fail(*args):
        raise OSError("interrupted copy")

    monkeypatch.setattr(module, "_copy_directory", fail)
    with pytest.raises(OSError):
        module.create_backup(tmp_path / "backups", source, quiet)
    assert module.list_backups(tmp_path / "backups") == []


def test_retention_keeps_latest_daily_weekly_and_never_last_backup():
    from operations.backup import retention_plan

    assert retention_plan([]) == []
    assert retention_plan([{"path": "latest", "created_at": "2026-11-05T00:00:00+00:00"}]) == []
    from datetime import datetime, timedelta, timezone

    now = datetime(2026, 11, 5, tzinfo=timezone.utc)
    backups = [{"path": str(i), "created_at": (now - timedelta(days=i)).isoformat()} for i in range(60)]
    remove = retention_plan(backups)
    assert not set(map(str, range(7))).intersection(remove)
    assert "59" in remove


def test_maintenance_fence_excludes_writers_and_rejects_durable_lease():
    from operations.backup import maintenance_guard, writer_admission
    from database.warehouse import WarehouseManager

    with writer_admission("test"):
        with pytest.raises(RuntimeError, match="busy"):
            with maintenance_guard("test"):
                pass
    with maintenance_guard("test"):
        with pytest.raises(RuntimeError, match="busy"):
            with writer_admission("test"):
                pass
    warehouse = WarehouseManager(mode="test")
    warehouse.execute("CREATE TABLE pipeline_run_lease(resource TEXT PRIMARY KEY,run_id TEXT)")
    warehouse.execute("INSERT INTO pipeline_run_lease VALUES('canonical','crashed-run')")
    warehouse.close()
    with pytest.raises(ValueError, match="lease"):
        with maintenance_guard("test"):
            pass


def test_backup_rejects_duplicate_and_nested_restore_targets(tmp_path):
    from operations.backup import create_backup, restore_plan

    source = components(tmp_path)
    backup = create_backup(tmp_path / "backups", source, quiet)
    targets = {row["name"]: {"path": str(tmp_path / "destination")} for row in source}
    with pytest.raises(ValueError, match="Duplicate"):
        restore_plan(backup, targets)


def test_empty_directories_survive_restore(tmp_path):
    from operations.backup import create_backup, restore_plan, restore_apply

    source = components(tmp_path)
    Path(source[0]["path"], "empty").mkdir()
    backup = create_backup(tmp_path / "backups", source, quiet)
    targets = {row["name"]: {"path": str(tmp_path / "restore" / row["name"])} for row in source}
    restore_apply(backup, restore_plan(backup, targets), quiet)
    assert Path(targets["warehouse"]["path"], "empty").is_dir()


def test_maintenance_refuses_running_topic_registry_state():
    from operations.backup import maintenance_guard
    from database.warehouse import WarehouseManager

    warehouse = WarehouseManager(mode="test")
    warehouse.execute("CREATE TABLE ops_topic_registry(topic TEXT PRIMARY KEY,payload_json TEXT)")
    warehouse.execute("INSERT INTO ops_topic_registry VALUES('temporary','{\"state\":\"running\"}')")
    warehouse.close()
    with pytest.raises(ValueError, match="topic run"):
        with maintenance_guard("test"):
            pass


def test_retention_apply_rechecks_inventory_and_keeps_last(tmp_path):
    from operations.backup import create_backup, list_backups, retention_apply

    source = components(tmp_path)
    backup = create_backup(tmp_path / "backups", source, quiet)
    inventory = list_backups(tmp_path / "backups")
    plan = {"backup_target": str((tmp_path / "backups").resolve()), "inventory": inventory, "remove": [str(backup)]}
    with pytest.raises(ValueError, match="retention"):
        retention_apply(tmp_path / "backups", plan)
    assert backup.exists()
