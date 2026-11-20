"""Spark checkpoint offsets must describe committed work in every configured sink."""

import json
from pathlib import Path

import pytest


class CheckpointFiles:
    def __init__(self, root):
        self.root = root

    def objects(self, prefix):
        for path in (self.root / prefix).glob("*"):
            yield str(path.relative_to(self.root)), path.stat().st_size

    def read_bytes(self, key):
        return (self.root / key).read_bytes()


def write_batch(root, sink, batch, end, committed=True):
    for kind, content in [
        ("offsets", 'v1\n{"batchTimestampMs":0}\n' + json.dumps({"reviews": {"0": end}})),
        ("commits", 'v1\n{"nextBatchWatermarkMs":0}'),
    ]:
        if kind == "commits" and not committed:
            continue
        path = root / "generation" / sink / kind / str(batch)
        path.parent.mkdir(exist_ok=True, parents=True)
        path.write_text(content)


def verify(root: Path, sql_batch=2, end=45):
    from operations.stream_checkpoint import committed_frontiers

    return committed_frontiers(
        CheckpointFiles(root), "generation", ["reviews_window", "reviews_quarantine"], {"reviews": {"0": end}}, sql_batch
    )


def test_sinks_can_have_different_committed_batch_ids(tmp_path):
    write_batch(tmp_path, "reviews_window", 2, 45)
    write_batch(tmp_path, "reviews_quarantine", 0, 45)
    proof = verify(tmp_path)
    assert proof["reviews_window"]["batch_id"] == 2
    assert proof["reviews_quarantine"]["batch_id"] == 0


def test_uncommitted_offset_or_sql_behind_blocks_backup(tmp_path):
    write_batch(tmp_path, "reviews_window", 2, 45)
    write_batch(tmp_path, "reviews_quarantine", 1, 44)
    write_batch(tmp_path, "reviews_quarantine", 2, 45, committed=False)
    with pytest.raises(ValueError):
        verify(tmp_path)
    write_batch(tmp_path, "reviews_quarantine", 2, 45)
    with pytest.raises(ValueError):
        verify(tmp_path, sql_batch=1)


def test_missing_checkpoint_requires_untouched_source_and_sql(tmp_path):
    assert verify(tmp_path, sql_batch=None, end=0)["reviews_window"]["batch_id"] is None
    with pytest.raises(ValueError):
        verify(tmp_path, sql_batch=None, end=1)
    with pytest.raises(ValueError):
        verify(tmp_path, sql_batch=0, end=0)


def test_metadata_without_a_version_cannot_certify_offsets(tmp_path):
    write_batch(tmp_path, "reviews_window", 2, 45)
    write_batch(tmp_path, "reviews_quarantine", 1, 45)
    (tmp_path / "generation/reviews_quarantine/commits/1").write_text("{}")
    with pytest.raises(ValueError, match="format"):
        verify(tmp_path)


def test_same_total_on_a_different_partition_is_insufficient(tmp_path):
    write_batch(tmp_path, "reviews_window", 2, 45)
    write_batch(tmp_path, "reviews_quarantine", 1, 45)
    (tmp_path / "generation/reviews_window/offsets/2").write_text('v1\n{}\n{"reviews":{"1":45}}')
    with pytest.raises(ValueError, match="Kafka end"):
        verify(tmp_path)
