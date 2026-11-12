"""Read managed job manifests across isolated warehouses without changing settings."""

from datetime import datetime
import json
from pathlib import Path


def _clock(value):
    if not value:
        return None
    point = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if point.tzinfo is None:
        raise ValueError("Source telemetry clock requires a timezone")
    return point.timestamp()


def source_metrics(config, registered_at=None):
    from orchestration.source_jobs import validate_config

    config = validate_config(config)
    lines = []

    def emit(name, value, labels):
        rendered = "NaN" if value is None else format(float(value), ".15g")
        lines.append(f"sentinel_source_{name}{{{labels}}} {rendered}")

    for job in config["jobs"]:
        labels = f'source_job="{job["id"]}",source="{job["source_group"]}"'
        manifests = []
        for path in (Path(config["root"]) / job["id"]).glob("*/manifest.json"):
            value = json.loads(path.read_text())
            if value.get("job_id") != job["id"] or value.get("namespace") != job["namespace"]:
                raise ValueError("Source metric manifest identity mismatch")
            manifests.append(value)
        manifests.sort(key=lambda value: (_clock(value.get("started_at")) or 0, value["run_id"]))
        latest = manifests[-1] if manifests else {}
        for field, name in [("captured_at", "last_capture_timestamp"), ("processed_at", "last_process_timestamp")]:
            measured = [_clock(value.get(field)) for value in manifests if value.get(field)]
            emit(name, max(measured) if measured else None, labels)
        emit("expected_since_timestamp", registered_at, labels)
        emit("input_records", latest.get("input_records"), labels)
        emit("expected_min", job["minimum_records"], labels)
        emit(
            "pending_runs",
            sum(value.get("state") in ("new", "captured", "processing", "failed") for value in manifests),
            labels,
        )
        for state in ("new", "captured", "processing", "complete", "partial", "failed"):
            emit("state", int(latest.get("state") == state), labels + f',state="{state}"')
    return "\n".join(lines) + "\n"


def backup_metrics(pointer):
    """Last verified recovery point, independent of the dashboard warehouse schema."""
    pointer = Path(pointer)
    value = None
    if pointer.exists():
        record = json.loads(pointer.read_text())
        manifest = json.loads((Path(record["backup"]) / "manifest.json").read_text())
        if record.get("verified") is not True or record.get("snapshot_at") != manifest.get("snapshot_at"):
            raise ValueError("Backup pointer is not a verified matching snapshot")
        value = _clock(record["snapshot_at"])
    rendered = "NaN" if value is None else format(value, ".15g")
    return 'sentinel_ops_backup_last_complete_timestamp{pipeline="platform",source="system"} ' + rendered + "\n"
