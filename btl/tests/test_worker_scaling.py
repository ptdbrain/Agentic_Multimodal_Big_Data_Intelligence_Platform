"""Benchmark snapshots must retain storage code while excluding captured data."""

import pytest

pytestmark = pytest.mark.unit


def test_snapshot_keeps_storage_implementation_and_excludes_runtime_data(tmp_path):
    from scripts.benchmark_worker_scaling import freeze_source

    snapshot = tmp_path / "source"
    inventory = freeze_source(snapshot)
    assert "storage/storage_manager.py" in inventory
    assert (snapshot / "storage/storage_manager.py").is_file()
    assert not any(name.startswith(("storage/datalake/", "storage/captures/")) for name in inventory)
    assert (snapshot / "scripts/benchmark_fullpath.py").is_file()
