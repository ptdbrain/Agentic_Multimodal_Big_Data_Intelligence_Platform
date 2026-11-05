"""Pinned-input replay and guarded promotion; never rewrite engine checkpoints."""

from contextlib import contextmanager
from datetime import datetime, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import tempfile
import uuid

COMPATIBILITY = {
    "envelope": {"read": ["1.0"], "write": "1.0"},
    "business": {"read": ["1.0"], "write": "1.0"},
    "simulator_checkpoint": {"resume": [3], "target": 3},
    "spark_checkpoint": {"upgrade": "new namespace + replay; opaque state never edited"},
}


def _json(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=False, default=str, allow_nan=False)


def _hash(value):
    return hashlib.sha256(_json(value).encode()).hexdigest()


def _safe(path):
    path = Path(path).absolute()
    if any(part.is_symlink() for part in [path, *path.parents]):
        raise ValueError("Symlink path is not a migration namespace")
    return path


def inventory(path):
    path = _safe(path)
    if not path.exists():
        raise ValueError("Missing migration input")
    files = sorted(path.rglob("*")) if path.is_dir() else [path]
    result = {}
    for file in files:
        if file.is_symlink():
            raise ValueError("Symlink in migration input")
        if file.is_file():
            result[str(file.relative_to(path)) if path.is_dir() else file.name] = hashlib.sha256(
                file.read_bytes()
            ).hexdigest()
    return result


def inspect_checkpoint(path):
    path = _safe(path)
    state = path / "checkpoint_state.json"
    report = {"version": 1, "path": str(path), "files": inventory(path), "compatibility": COMPATIBILITY}
    if state.exists():
        data = json.loads(state.read_text())
        report.update(
            engine="simulator",
            checkpoint_version=data.get("checkpoint_schema_version"),
            configuration=data.get("configuration"),
        )
    else:
        report.update(engine="spark", checkpoint_version="opaque")
    return report


def _write(path, value):
    path = _safe(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(dir=path.parent, prefix=".migration-")
    try:
        with os.fdopen(fd, "w") as stream:
            stream.write(_json(value) + "\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def make_plan(
    old_checkpoint,
    destination,
    inputs,
    *,
    writer_pids,
    engine="simulator",
    mode="test",
    old_output=None,
    old_quarantine=None,
    old_input_manifest=None,
    configuration=None,
):
    old = _safe(old_checkpoint)
    new = _safe(destination)
    if old == new or old in new.parents or new in old.parents or new.exists():
        raise ValueError("Destination must be a fresh isolated namespace")
    if engine not in {"simulator", "spark"}:
        raise ValueError("Unsupported engine")
    if not writer_pids or any(type(pid) is not int or pid <= 0 for pid in writer_pids):
        raise ValueError("Explicit old writer PIDs required")
    if not inputs:
        raise ValueError("Pinned Bronze input required; no latest-offset fallback")
    sources = [{"path": str(_safe(path)), "hashes": inventory(path)} for path in inputs]
    checkpoint = inspect_checkpoint(old)
    config = (
        configuration
        or checkpoint.get("configuration")
        or {
            "window_seconds": 60,
            "watermark_seconds": 600,
            "negative_threshold": 2.0,
            "strict_validation": True,
            "output_mode": "update",
        }
    )
    if old_input_manifest is None and (old.parent / "replay.json").is_file():
        old_input_manifest = old.parent / "replay.json"
    baseline = {}
    for kind, path in [("output", old_output), ("quarantine", old_quarantine), ("input_manifest", old_input_manifest)]:
        if path:
            baseline[kind] = {"path": str(_safe(path)), "hashes": inventory(path)}
    return {
        "version": 1,
        "migration_id": uuid.uuid4().hex,
        "engine": engine,
        "mode": mode,
        "old_checkpoint": checkpoint,
        "destination": str(new),
        "inputs": sources,
        "baseline": baseline,
        "configuration": config,
        "writer_pids": writer_pids,
    }


def _validate(plan):
    if plan.get("version") != 1:
        raise ValueError("Unsupported migration plan")
    old = _safe(plan["old_checkpoint"]["path"])
    new = _safe(plan["destination"])
    if old == new or old in new.parents or new in old.parents:
        raise ValueError("Old and new namespace must be isolated")
    if inventory(plan["old_checkpoint"]["path"]) != plan["old_checkpoint"]["files"]:
        raise ValueError("Old checkpoint hash changed; stop writer and create a new plan")
    for entry in [*plan["inputs"], *plan["baseline"].values()]:
        if inventory(entry["path"]) != entry["hashes"]:
            raise ValueError("Pinned input hash mismatch")


def _records(plan):
    rows = []
    for entry in plan["inputs"]:
        path = Path(entry["path"])
        files = sorted(path.rglob("*.json*")) if path.is_dir() else [path]
        for file in files:
            if file.suffix == ".jsonl":
                values = [json.loads(line) for line in file.read_text().splitlines() if line.strip()]
            else:
                values = json.loads(file.read_text())
                values = values if isinstance(values, list) else [values]
            if not all(isinstance(row, dict) for row in values):
                raise ValueError("Bronze must contain event objects")
            for value in values:
                if value.get("schema_version", "1.0") != "1.0":
                    raise ValueError("Unsupported envelope/business schema version")
            rows.extend(values)
    if not rows:
        raise ValueError("No replayable Bronze records")
    return rows


def _simulator(plan, records, checkpoint=None):
    from spark.streaming.streaming_job import StreamingJob

    job = StreamingJob(checkpoint_dir=str(checkpoint) if checkpoint else None, **plan["configuration"])
    job.process_micro_batch(records, batch_id="migration-replay")
    business = {
        key: getattr(job, key)
        for key in [
            "source_group",
            "valid_events_processed",
            "invalid_events_count",
            "duplicates_dropped",
            "late_events_dropped",
            "late_events_accepted",
            "seen_keys",
            "window_state",
        ]
    }
    return {"business": business, "windows": job.get_window_metrics(), "quarantine": job.quarantined_events}


def replay(plan):
    _validate(plan)
    destination = _safe(plan["destination"])
    if destination.exists():
        raise ValueError("Destination already exists; do not overwrite a replay")
    records = _records(plan)
    destination.mkdir(parents=True, exist_ok=False)
    try:
        if plan["engine"] == "simulator":
            output = _simulator(plan, records, destination / "checkpoint")
            _write(destination / "output.json", output)
            business = output["business"]
        else:
            from spark.streaming.spark_streaming_job import SparkStreamingJob
            from spark.spark_session import SparkSessionFactory

            incoming = destination / "input"
            incoming.mkdir()
            (incoming / "events.json").write_text(_json(records))
            spark = SparkSessionFactory.get_spark_session(master="local[2]")
            if spark is None:
                raise RuntimeError("Actual Spark engine unavailable")
            job = SparkStreamingJob(
                spark_session=spark,
                checkpoint_dir=str(destination / "checkpoint"),
                output_path=str(destination / "metrics"),
                quarantine_path=str(destination / "quarantine"),
            )
            try:
                query = job.start_review_stream(
                    source_path=str(incoming),
                    trigger_available_now=True,
                    window_duration=f"{plan['configuration']['window_seconds']} seconds",
                    watermark_delay=f"{plan['configuration']['watermark_seconds']} seconds",
                )
                job.await_termination(180)
                if query.isActive or (job._quarantine_query and job._quarantine_query.isActive):
                    raise RuntimeError("Spark replay timed out")
                if query.exception() or (job._quarantine_query and job._quarantine_query.exception()):
                    raise RuntimeError("Spark replay query failed")
            finally:
                job.stop()
            business = {"output": _parquet(destination / "metrics"), "quarantine": _parquet(destination / "quarantine")}
        result = {
            "version": 1,
            "plan_sha256": _hash(plan),
            "engine": plan["engine"],
            "input_sha256": _hash(records),
            "business": business,
            "artifacts": inventory(destination),
            "completed_at": datetime.now(timezone.utc).isoformat(),
        }
        _write(destination / "replay.json", result)
        return result
    except Exception:
        # Keep failed isolated namespace for diagnosis; never promote partial data.
        _write(destination / "failure.json", {"version": 1, "status": "failed"})
        raise


def _parquet(path):
    if not path.exists():
        return []
    import pandas as pd

    files = sorted(path.rglob("*.parquet"))
    rows = []
    for file in files:
        for row in pd.read_parquet(file).to_dict("records"):
            rows.append(
                {
                    key: (None if isinstance(value, float) and not math.isfinite(value) else value)
                    for key, value in row.items()
                    if key not in {"quarantined_at", "source_file"}
                }
            )
    return sorted([json.loads(_json(row)) for row in rows], key=_json)


def verify(plan, baseline=None):
    _validate(plan)
    destination = Path(plan["destination"])
    result = json.loads((destination / "replay.json").read_text())
    if result["plan_sha256"] != _hash(plan):
        raise ValueError("Replay plan mismatch")
    actual = inventory(destination)
    if set(actual) - set(result["artifacts"]) - {"replay.json", "verification.json", "promotion.json"}:
        raise ValueError("Unexpected replay output artifact")
    for name, digest in result["artifacts"].items():
        if actual.get(name) != digest:
            raise ValueError("Replay output hash mismatch")
    if "input_manifest" not in plan["baseline"]:
        raise ValueError("Pinned old input manifest required to reconcile exact event identities")
    old_input = json.loads(Path(plan["baseline"]["input_manifest"]["path"]).read_text())
    if old_input.get("input_sha256") != result["input_sha256"]:
        raise ValueError("Old/new input identity reconciliation failed")
    if plan["engine"] == "simulator":
        old_path = Path(plan["old_checkpoint"]["path"]) / "checkpoint_state.json"
        old = json.loads(old_path.read_text())
        if old.get("checkpoint_schema_version") != 3:
            raise ValueError(
                "Legacy simulator checkpoint requires independently captured v3-compatible baseline; cannot certify opaque old counters"
            )
        expected = {key: old[key] for key in result["business"]}
        if baseline is not None:
            # Arbitrary user summary can never replace the actual old checkpoint.
            supplied = json.loads(Path(baseline).read_text())
            if supplied.get("business") != expected:
                raise ValueError("Baseline reconciliation failed")
    else:
        if "output" not in plan["baseline"] or "quarantine" not in plan["baseline"]:
            raise ValueError("Pinned old Spark output and quarantine directories are required")
        expected = {kind: _parquet(Path(plan["baseline"][kind]["path"])) for kind in ["output", "quarantine"]}
    if result["business"] != expected:
        raise ValueError("Business reconciliation failed")
    proof = {
        "version": 1,
        "passed": True,
        "plan_sha256": _hash(plan),
        "replay_sha256": _hash(result),
        "expected_sha256": _hash(expected),
        "verified_artifacts": inventory(destination),
    }
    _write(destination / "verification.json", proof)
    return proof


def _stopped(plan):
    for pid in plan["writer_pids"]:
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            continue
        except PermissionError:
            raise RuntimeError("Old writer status cannot be verified") from None
        raise RuntimeError("Old writer is still active")


@contextmanager
def _guard(plan):
    from operations.backup import maintenance_guard

    with maintenance_guard(plan["mode"]):
        _stopped(plan)
        yield


def promote(plan, pointer):
    _validate(plan)
    destination = Path(plan["destination"])
    proof = json.loads((destination / "verification.json").read_text())
    if proof.get("plan_sha256") != _hash(plan) or not proof.get("passed"):
        raise ValueError("Verified plan mismatch")
    result = json.loads((destination / "replay.json").read_text())
    if proof["replay_sha256"] != _hash(result):
        raise ValueError("Verified replay changed")
    actual = inventory(destination)
    if set(actual) - set(proof["verified_artifacts"]) - {"verification.json", "promotion.json"}:
        raise ValueError("Unexpected verified output artifact")
    for name, digest in proof["verified_artifacts"].items():
        if actual.get(name) != digest:
            raise ValueError("Verified output changed")
    pointer = _safe(pointer)
    protected = [Path(plan["old_checkpoint"]["path"]), destination]
    protected += [Path(entry["path"]) for entry in [*plan["inputs"], *plan["baseline"].values()]]
    if any(pointer == path or path in pointer.parents for path in protected):
        raise ValueError("Serving pointer must be outside checkpoints, outputs and pinned evidence")
    with _guard(plan):
        _validate(plan)
        previous = json.loads(pointer.read_text()) if pointer.exists() else None
        change = {
            "version": 1,
            "migration_id": plan["migration_id"],
            "generation": str(destination),
            "previous": previous,
            "plan_sha256": _hash(plan),
        }
        if previous and previous.get("migration_id") == plan["migration_id"]:
            if previous.get("plan_sha256") != _hash(plan):
                raise ValueError("Serving pointer plan mismatch")
            return previous
        _write(destination / "promotion.json", change)
        _write(pointer, change)
    return change


def rollback(plan, pointer):
    pointer = _safe(pointer)
    with _guard(plan):
        _validate(plan)
        current = json.loads(pointer.read_text())
        if current.get("migration_id") != plan["migration_id"]:
            raise ValueError("Serving pointer has advanced; rollback refused")
        old = current.get("previous")
        if old is None:
            raise ValueError("No previous generation; rollback cannot invent one")
        _write(pointer, old)
    return old
