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
    kafka_source=None,
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
    plan = {
        "version": 1,
        "source_kind": "kafka" if kafka_source else "file",
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
    if kafka_source is not None:
        if engine != 'spark':
            raise ValueError('Kafka migration requires actual Spark engine')
        plan['kafka_source'] = json.loads(_json(validate_kafka_source(kafka_source)))
        observed = read_kafka_bounds(kafka_source)
        validate_kafka_bounds(kafka_source, observed, frozen=True)
        if not observed.get('cluster_id'):
            raise ValueError('Kafka cluster identity could not be pinned')
        plan['kafka_source']['cluster_id'] = observed['cluster_id']
        from spark.streaming.spark_streaming_job import SparkStreamingJob
        plan['kafka_schema_sha256'] = _hash(SparkStreamingJob.get_envelope_raw_schema().jsonValue())
        plan['kafka_expected'] = _kafka_identities(_records(plan), kafka_source)
        plan['kafka_input_sha256'] = _hash(plan['kafka_expected'])
    return plan


def _validate(plan):
    if plan.get("version") != 1:
        raise ValueError("Unsupported migration plan")
    old = _safe(plan["old_checkpoint"]["path"])
    new = _safe(plan["destination"])
    if old == new or old in new.parents or new in old.parents:
        raise ValueError("Old and new namespace must be isolated")
    if inventory(plan["old_checkpoint"]["path"]) != plan["old_checkpoint"]["files"]:
        raise ValueError("Old checkpoint hash changed; stop writer and create a new plan")
    if plan.get('source_kind') == 'kafka':
        from spark.streaming.spark_streaming_job import SparkStreamingJob
        if _hash(SparkStreamingJob.get_envelope_raw_schema().jsonValue()) != plan.get('kafka_schema_sha256'):
            raise ValueError('Kafka raw schema changed; create a new migration plan')
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

            kafka_source = plan.get('kafka_source')
            if kafka_source:
                validate_kafka_bounds(kafka_source, read_kafka_bounds(kafka_source), frozen=True)
                if _hash(_kafka_identities(records, kafka_source)) != plan['kafka_input_sha256']:
                    raise ValueError('Pinned Kafka raw input identities changed')
                _write(destination / 'stream-source.json', {
                    'source_kind': 'kafka', 'kafka_source': kafka_source,
                    'configuration': plan['configuration'], 'plan_sha256': _hash(plan),
                    'kafka_schema_sha256': plan['kafka_schema_sha256']})
                source_args = {'kafka_source': kafka_source, 'audit_path': str(destination / 'audit')}
            else:
                incoming = destination / "input"
                incoming.mkdir()
                (incoming / "events.json").write_text(_json(records))
                source_args = {'source_path': str(incoming)}
            spark = SparkSessionFactory.get_spark_session(master="local[2]")
            if spark is None:
                raise RuntimeError("Actual Spark engine unavailable")
            job = SparkStreamingJob(
                spark_session=spark,
                checkpoint_dir=str(destination / "checkpoint"),
                output_path=str(destination / "metrics"),
                quarantine_path=str(destination / "quarantine"),
                output_mode=plan['configuration']['output_mode'],
                negative_threshold=plan['configuration']['negative_threshold'],
            )
            try:
                query = job.start_review_stream(
                    **source_args,
                    trigger_available_now=True,
                    window_duration=f"{plan['configuration']['window_seconds']} seconds",
                    watermark_delay=f"{plan['configuration']['watermark_seconds']} seconds",
                )
                job.await_termination(180)
                if any(q is not None and q.isActive for q in job.queries):
                    raise RuntimeError("Spark replay timed out")
                if any(q is not None and q.exception() for q in job.queries):
                    raise RuntimeError("Spark replay query failed")
            finally:
                job.stop()
            business = {"output": _parquet(destination / "metrics"), "quarantine": _parquet(destination / "quarantine")}
            if kafka_source:
                validate_kafka_bounds(kafka_source, read_kafka_bounds(kafka_source), frozen=True)
                _check_kafka_checkpoints(destination, kafka_source)
                identities = _audit_identities(destination / 'audit')
                if identities != plan['kafka_expected']:
                    raise ValueError('Kafka replay differs from pinned Bronze event IDs/raw payloads')
        _validate(plan)
        result = {
            "version": 1,
            "plan_sha256": _hash(plan),
            "engine": plan["engine"],
            "source_kind": plan.get("source_kind", "file"),
            "kafka_input_sha256": plan.get("kafka_input_sha256"),
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
    if plan.get('source_kind') == 'kafka':
        source = plan['kafka_source']
        validate_kafka_bounds(source, read_kafka_bounds(source), frozen=True)
        _check_kafka_checkpoints(destination, source)
        if (_audit_identities(destination / 'audit') != plan['kafka_expected']
                or result.get('kafka_input_sha256') != plan['kafka_input_sha256']
                or old_input.get('kafka_input_sha256') != plan['kafka_input_sha256']):
            raise ValueError('Kafka Bronze event IDs/raw payload reconciliation failed')
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
        if plan.get('source_kind') == 'kafka':
            validate_kafka_bounds(plan['kafka_source'], read_kafka_bounds(plan['kafka_source']), frozen=True)
            _check_kafka_checkpoints(destination, plan['kafka_source'])
        previous = json.loads(pointer.read_text()) if pointer.exists() else None
        change = {
            "version": 1,
            "migration_id": plan["migration_id"],
            "generation": str(destination),
            "previous": previous,
            "plan_sha256": _hash(plan),
        }
        binding = destination / 'stream-source.json'
        if plan.get('source_kind') == 'kafka':
            change['source_binding_sha256'] = _hash(json.loads(binding.read_text()))
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


def validate_kafka_source(source):
    """Require a complete immutable partition range, never symbolic latest/earliest."""
    if not isinstance(source, dict) or source.get('version') != 1:
        raise ValueError('Explicit version 1 Kafka source manifest required')
    if not isinstance(source.get('bootstrap_servers'), str) or not source['bootstrap_servers'].strip():
        raise ValueError('Explicit Kafka bootstrap_servers required')
    if source.get('schema_version') != '1.0':
        raise ValueError('Unsupported Kafka envelope/business schema version')
    for key in ('starting_offsets', 'ending_offsets'):
        offsets = source.get(key)
        if not isinstance(offsets, dict) or not offsets:
            raise ValueError('Kafka offsets must be explicit topic/partition maps')
        for topic, parts in offsets.items():
            if not isinstance(topic, str) or not topic or not isinstance(parts, dict) or not parts:
                raise ValueError('Explicit Kafka topic/partition map required')
            for partition, offset in parts.items():
                if (not isinstance(partition, str) or not partition.isdigit() or str(int(partition)) != partition
                        or type(offset) is not int or offset < 0):
                    raise ValueError('Kafka partition and offsets must be explicit nonnegative integers')
    start, end = source['starting_offsets'], source['ending_offsets']
    if {t: set(p) for t, p in start.items()} != {t: set(p) for t, p in end.items()}:
        raise ValueError('Kafka start/end partition sets differ')
    if any(end[t][p] < value for t, parts in start.items() for p, value in parts.items()):
        raise ValueError('Kafka ending offset precedes starting offset')
    return source


def read_kafka_bounds(source):
    """Ask the broker for every partition; missing brokers/topics fail closed."""
    from kafka import KafkaConsumer
    from kafka.structs import TopicPartition

    validate_kafka_source(source)
    consumer = KafkaConsumer(bootstrap_servers=source['bootstrap_servers'], enable_auto_commit=False,
                             request_timeout_ms=60000)
    try:
        partitions = []
        for topic, expected in source['starting_offsets'].items():
            observed = consumer.partitions_for_topic(topic)
            if observed is None or {str(p) for p in observed} != set(expected):
                raise ValueError('Kafka partition topology changed; create a new migration plan')
            partitions.extend(TopicPartition(topic, int(p)) for p in expected)
        def convert(offsets):
            result = {}
            for part, offset in offsets.items():
                result.setdefault(part.topic, {})[str(part.partition)] = offset
            return result
        from kafka.admin import KafkaAdminClient
        admin = KafkaAdminClient(bootstrap_servers=source['bootstrap_servers'])
        try:
            cluster_id = admin.describe_cluster()['cluster_id']
        finally:
            admin.close()
        return {'beginning_offsets': convert(consumer.beginning_offsets(partitions)),
                'ending_offsets': convert(consumer.end_offsets(partitions)), 'cluster_id': cluster_id}
    finally:
        consumer.close()


def validate_kafka_bounds(source, bounds, *, frozen=False):
    validate_kafka_source(source)
    expected = source['starting_offsets']
    if source.get('cluster_id') and source['cluster_id'] != bounds.get('cluster_id'):
        raise ValueError('Kafka cluster identity changed')
    for key in ('beginning_offsets', 'ending_offsets'):
        actual = bounds.get(key, {})
        if {t: set(p) for t, p in actual.items()} != {t: set(p) for t, p in expected.items()}:
            raise ValueError('Kafka partition topology changed')
    for topic, parts in expected.items():
        for partition, start in parts.items():
            low = bounds['beginning_offsets'][topic][partition]
            high = bounds['ending_offsets'][topic][partition]
            end = source['ending_offsets'][topic][partition]
            if low > start or high < start:
                raise ValueError('Kafka offset loss: explicit Bronze backfill in a separate file-source migration required')
            if high < end or (frozen and high != end):
                raise ValueError('Kafka frozen frontier changed; stop producers and create a new plan')
    return bounds


def _kafka_identities(records, source):
    identities = []
    seen = set()
    for row in records:
        topic, part, offset = row.get('source_topic'), row.get('source_partition'), row.get('source_offset')
        raw = row.get('raw_value')
        if not isinstance(raw, str) or type(part) is not int or type(offset) is not int:
            raise ValueError('Kafka Bronze export requires exact raw_value and source topic/partition/offset')
        key = (topic, str(part), offset)
        start = source['starting_offsets'].get(topic, {}).get(str(part))
        end = source['ending_offsets'].get(topic, {}).get(str(part))
        if start is None or not start <= offset < end or key in seen:
            raise ValueError('Duplicate or out-of-range Kafka Bronze identity')
        seen.add(key)
        try:
            envelope = json.loads(raw)
        except ValueError:
            envelope = None
        event_id = envelope.get('event_id') if isinstance(envelope, dict) else None
        identities.append({'source_topic': topic, 'source_partition': part, 'source_offset': offset,
                           'event_id': event_id, 'raw_payload_sha256': hashlib.sha256(raw.encode('utf-8')).hexdigest()})
    return sorted(identities, key=_json)


def kafka_checkpoint_frontier(checkpoint):
    """Read committed source progress without modifying Spark's opaque state."""
    checkpoint = _safe(checkpoint)
    commits = sorted((int(p.name) for p in (checkpoint / 'commits').iterdir() if p.name.isdigit()))
    if not commits:
        raise ValueError('Missing committed Kafka checkpoint frontier')
    lines = (checkpoint / 'offsets' / str(commits[-1])).read_text().splitlines()
    sources = [json.loads(line) for line in lines[2:] if line.strip() and line != '-']
    if len(sources) != 1 or not isinstance(sources[0], dict):
        raise ValueError('Expected one actual Kafka checkpoint source')
    return sources[0]


def _check_kafka_checkpoints(destination, source, *, exact=True):
    for kind in ('reviews_window', 'reviews_quarantine', 'reviews_audit'):
        frontier = kafka_checkpoint_frontier(Path(destination) / 'checkpoint' / kind)
        if {t: set(p) for t, p in frontier.items()} != {
                t: set(p) for t, p in source['ending_offsets'].items()}:
            raise ValueError('Kafka checkpoint source differs from pinned partition assignment')
        for topic, parts in source['ending_offsets'].items():
            for partition, end in parts.items():
                if frontier[topic][partition] < end or (exact and frontier[topic][partition] != end):
                    raise ValueError('Kafka checkpoint did not consume the frozen frontier')


def resolve_stream_pointer(pointer, *, expected_topics=None):
    """Resolve opt-in startup to an actual Kafka checkpoint and its original source."""
    pointer = _safe(pointer)
    if not pointer.is_file():
        raise ValueError('Kafka serving pointer is missing')
    change = json.loads(pointer.read_text())
    generation = _safe(change['generation'])
    path = generation / 'stream-source.json'
    if not path.is_file():
        raise ValueError('Kafka source binding is required; file-source checkpoints cannot resume Kafka')
    binding = json.loads(path.read_text())
    if change.get('source_binding_sha256') is not None and change['source_binding_sha256'] != _hash(binding):
        raise ValueError('Promoted Kafka source binding changed')
    if binding.get('source_kind') != 'kafka':
        raise ValueError('Kafka startup cannot switch a file-source checkpoint to Kafka')
    source = validate_kafka_source(binding['kafka_source'])
    if binding.get('kafka_schema_sha256'):
        from spark.streaming.spark_streaming_job import SparkStreamingJob
        if binding['kafka_schema_sha256'] != _hash(SparkStreamingJob.get_envelope_raw_schema().jsonValue()):
            raise ValueError('Promoted Kafka schema differs from current reader')
    if expected_topics is not None and set(expected_topics.split(',')) != set(source['starting_offsets']):
        raise ValueError('Kafka topics differ from promoted checkpoint source')
    # Each query can be at a different position after a crash. All must still be
    # retained; startingOffsets is ignored by Spark once its checkpoint exists.
    _check_kafka_checkpoints(generation, source, exact=False)
    bounds = read_kafka_bounds(source)
    for kind in ('reviews_window', 'reviews_quarantine', 'reviews_audit'):
        resumed = dict(source, starting_offsets=kafka_checkpoint_frontier(generation / 'checkpoint' / kind),
                       ending_offsets=bounds['ending_offsets'])
        validate_kafka_bounds(resumed, bounds)
    return {'checkpoint_dir': str(generation / 'checkpoint'), 'output_path': str(generation / 'metrics'),
            'quarantine_path': str(generation / 'quarantine'), 'kafka_source': source,
            'configuration': binding['configuration'], 'audit_path': str(generation / 'audit')}


def _audit_identities(path):
    rows = []
    for file in sorted(Path(path).glob('batch-*.json')):
        rows.extend(json.loads(file.read_text()))
    keys = [(r['source_topic'], r['source_partition'], r['source_offset']) for r in rows]
    if len(keys) != len(set(keys)):
        raise ValueError('Duplicate Kafka audit coordinates')
    return sorted(rows, key=_json)
