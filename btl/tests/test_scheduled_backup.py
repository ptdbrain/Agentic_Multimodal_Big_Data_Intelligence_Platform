"""Refuse incomplete or cross-service daily recovery bundles before pausing writers."""

import pytest

pytestmark = pytest.mark.unit


def test_daily_backup_refuses_uncovered_streams_and_foreign_services():
    from scripts.run_scheduled_backup import validate_scope

    base = {
        key: key
        for key in (
            "POSTGRES_HOST",
            "POSTGRES_PORT",
            "POSTGRES_DB",
            "MINIO_ENDPOINT",
            "MINIO_SECURE",
            "KAFKA_BOOTSTRAP_SERVERS",
        )
    }
    validate_scope({}, base, [base.copy()])
    with pytest.raises(ValueError, match="stream"):
        validate_scope({"active_streaming_queries": ["/live/checkpoint"]}, base, [base])
    foreign = dict(base, MINIO_ENDPOINT="other-host:9000")
    with pytest.raises(ValueError, match="MINIO_ENDPOINT"):
        validate_scope({}, base, [foreign])
    foreign = dict(base, POSTGRES_HOST="other-host")
    with pytest.raises(ValueError, match="POSTGRES_HOST"):
        validate_scope({}, base, [foreign])


def test_backup_metric_uses_verified_snapshot_not_pointer_mtime(tmp_path):
    import json
    from monitoring.source_metrics import backup_metrics

    pointer = tmp_path / "last-complete.json"
    assert backup_metrics(pointer).endswith(" NaN\n")
    backup = tmp_path / "bundle"
    backup.mkdir()
    snapshot = "2026-11-12T15:00:00+00:00"
    (backup / "manifest.json").write_text(json.dumps({"snapshot_at": snapshot}))
    pointer.write_text(json.dumps({"backup": str(backup), "snapshot_at": snapshot, "verified": True}))
    assert backup_metrics(pointer).endswith(" 1794495600\n")
    pointer.write_text(
        json.dumps({"backup": str(backup), "snapshot_at": "2026-11-12T16:00:00+00:00", "verified": True})
    )
    with pytest.raises(ValueError, match="snapshot"):
        backup_metrics(pointer)
