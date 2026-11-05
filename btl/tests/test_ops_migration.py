"""Migration proves actual replay and preserves old state, never edits Spark internals."""

import json
from pathlib import Path
import pytest

pytestmark = pytest.mark.unit


def record_old_inputs(old, rows):
    import hashlib

    digest = hashlib.sha256(json.dumps(rows, sort_keys=True, ensure_ascii=False).encode()).hexdigest()
    (old.parent / "replay.json").write_text(json.dumps({"input_sha256": digest}))


def source(tmp_path):
    path = tmp_path / "bronze.jsonl"
    rows = [
        {
            "review_id": "r1",
            "product_id": "p1",
            "rating": 5,
            "review_text": "Actual fixture",
            "review_date": "2026-11-01T00:00:00Z",
            "source_group": "simulation",
            "acquisition_method": "generated",
        },
        {
            "review_id": "bad",
            "product_id": "p1",
            "rating": 8,
            "review_text": "Actual fixture",
            "review_date": "2026-11-01T00:00:00Z",
            "source_group": "simulation",
            "acquisition_method": "generated",
        },
    ]
    path.write_text("\n".join(json.dumps(row) for row in rows))
    return path


def test_replay_uses_engine_preserves_old_checkpoint_and_checks_baseline(tmp_path):
    from operations.migration import make_plan, replay, verify, inspect_checkpoint

    old = tmp_path / "old"
    old.mkdir()
    from spark.streaming.streaming_job import StreamingJob

    rows = [json.loads(line) for line in source(tmp_path).read_text().splitlines()]
    StreamingJob(checkpoint_dir=str(old), strict_validation=True).process_micro_batch(rows, batch_id="original")
    record_old_inputs(old, rows)
    original = (old / "checkpoint_state.json").read_bytes()
    plan = make_plan(old, tmp_path / "new", [source(tmp_path)], writer_pids=[99999999])
    result = replay(plan)
    assert inspect_checkpoint(tmp_path / "new/checkpoint")["checkpoint_version"] == 3
    assert result["business"]["valid_events_processed"] == 1
    assert result["business"]["invalid_events_count"] == 1
    assert original == (old / "checkpoint_state.json").read_bytes()
    baseline = tmp_path / "baseline.json"
    baseline.write_text(json.dumps(result))
    assert verify(plan, baseline)["passed"]
    result["business"]["valid_events_processed"] = 100
    baseline.write_text(json.dumps(result))
    with pytest.raises(ValueError, match="reconciliation"):
        verify(plan, baseline)


def test_tampered_input_and_existing_destination_are_rejected(tmp_path):
    from operations.migration import make_plan, replay

    old = tmp_path / "old"
    old.mkdir()
    src = source(tmp_path)
    plan = make_plan(old, tmp_path / "new", [src], writer_pids=[99999999])
    src.write_text("{}")
    with pytest.raises(ValueError, match="hash"):
        replay(plan)
    assert not (tmp_path / "new").exists()
    with pytest.raises(ValueError):
        make_plan(old, old, [src], writer_pids=[99999999])


def test_promote_requires_dead_writer_matching_verification_and_rollback(tmp_path):
    import os
    from operations.migration import make_plan, replay, verify, promote, rollback

    old = tmp_path / "old"
    old.mkdir()
    from spark.streaming.streaming_job import StreamingJob

    rows = [json.loads(line) for line in source(tmp_path).read_text().splitlines()]
    StreamingJob(checkpoint_dir=str(old), strict_validation=True).process_micro_batch(rows, batch_id="original")
    record_old_inputs(old, rows)
    plan = make_plan(old, tmp_path / "new", [source(tmp_path)], writer_pids=[os.getpid()])
    result = replay(plan)
    baseline = tmp_path / "baseline.json"
    baseline.write_text(json.dumps(result))
    verify(plan, baseline)
    pointer = tmp_path / "serving.json"
    pointer.write_text(json.dumps({"generation": str(old)}))
    with pytest.raises(RuntimeError, match="writer"):
        promote(plan, pointer)
    plan["writer_pids"] = [99999999]
    # Changing the plan invalidates the proof, even if the inputs are the same.
    with pytest.raises(ValueError, match="plan"):
        promote(plan, pointer)


@pytest.mark.spark
def test_actual_spark_replays_into_new_namespace_and_matches_old_output(tmp_path, spark_session):
    from operations.migration import make_plan, replay, verify

    empty = tmp_path / "empty"
    empty.mkdir()
    src = source(tmp_path)
    seed = make_plan(empty, tmp_path / "old", [src], writer_pids=[99999999], engine="spark")
    original = replay(seed)
    assert original["business"]["output"]
    plan = make_plan(
        tmp_path / "old/checkpoint",
        tmp_path / "new",
        [src],
        writer_pids=[99999999],
        engine="spark",
        old_output=tmp_path / "old/metrics",
        old_quarantine=tmp_path / "old/quarantine",
    )
    replay(plan)
    assert verify(plan)["passed"]


def test_verified_generation_promotes_and_rolls_back_under_maintenance_guard(tmp_path):
    from operations.migration import make_plan, replay, verify, promote, rollback
    from spark.streaming.streaming_job import StreamingJob

    old = tmp_path / "old"
    old.mkdir()
    src = source(tmp_path)
    rows = [json.loads(line) for line in src.read_text().splitlines()]
    StreamingJob(checkpoint_dir=str(old), strict_validation=True).process_micro_batch(rows)
    record_old_inputs(old, rows)
    plan = make_plan(old, tmp_path / "new", [src], writer_pids=[99999999])
    replay(plan)
    verify(plan)
    pointer = tmp_path / "serving.json"
    pointer.write_text(json.dumps({"generation": str(old)}))
    promoted = promote(plan, pointer)
    assert promoted["generation"] == str(tmp_path / "new")
    assert promote(plan, pointer) == promoted
    assert rollback(plan, pointer) == {"generation": str(old)}
    assert json.loads(pointer.read_text())["generation"] == str(old)


def test_added_unverified_output_file_blocks_verification(tmp_path):
    from operations.migration import make_plan, replay, verify
    from spark.streaming.streaming_job import StreamingJob

    old = tmp_path / "old"
    old.mkdir()
    src = source(tmp_path)
    rows = [json.loads(line) for line in src.read_text().splitlines()]
    StreamingJob(checkpoint_dir=str(old), strict_validation=True).process_micro_batch(rows)
    record_old_inputs(old, rows)
    plan = make_plan(old, tmp_path / "new", [src], writer_pids=[99999999])
    replay(plan)
    (tmp_path / "new/extra.parquet").write_bytes(b"unknown")
    with pytest.raises(ValueError, match="output"):
        verify(plan)
