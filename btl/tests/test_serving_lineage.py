"""Evidence is returned only after checking the pinned bytes and exact record ID."""

import hashlib, json, sqlite3
from types import SimpleNamespace
import pytest

pytestmark = pytest.mark.unit


def fixture(tmp_path):
    from serving.store import ServingStore
    from serving.lineage import stage_lineage
    from serving.lake import DatasetLake

    raw = json.dumps(
        [
            {
                "payload": {
                    "review_id": "r",
                    "product_id": "p",
                    "rating": 4,
                    "review_date": "2026-11-13",
                    "email": "private@example.test",
                }
            }
        ]
    ).encode()
    key = "bronze/reviews/batch.json"
    path = tmp_path / key
    path.parent.mkdir(parents=True)
    path.write_bytes(raw)
    ref = {"path": key, "sha256": hashlib.sha256(raw).hexdigest(), "size": len(raw)}
    conn = sqlite3.connect(tmp_path / "lineage.sqlite")
    store = ServingStore(conn, "fixture", "simulation")
    with conn:
        store.initialize()
        stage_lineage(store, "g", {"run_id": "run", "status": "complete", "bronze": [ref], "silver": [], "gold": []})
    lake = object.__new__(DatasetLake)
    lake.root = tmp_path
    lake.binding = SimpleNamespace(id="fixture", bucket="fixture")
    return store, lake, path


def test_trace_verifies_bytes_and_record_identity_and_does_not_invent_legacy_links(tmp_path):
    from serving.lineage import trace_record

    store, lake, path = fixture(tmp_path)
    result = trace_record(store, lake, "g", "reviews", "r")
    assert result["status"] == "verified_input"
    assert result["records"][0]["record_index"] == 0
    assert result["records"][0]["payload"]["rating"] == 4
    assert "email" not in result["records"][0]["payload"]
    assert trace_record(store, lake, "g", "reviews", "missing")["status"] == "not_found"
    assert trace_record(store, lake, "legacy", "reviews", "r")["status"] == "unknown"
    path.write_text("[]")
    with pytest.raises(ValueError, match="checksum|bytes"):
        trace_record(store, lake, "g", "reviews", "r")
    store.conn.close()


def test_lineage_receipt_cannot_be_replaced_or_escape_bronze(tmp_path):
    from serving.lineage import stage_lineage

    store, lake, path = fixture(tmp_path)
    for key in ("../secret", "/etc/passwd", "gold/pretend.json"):
        with pytest.raises(ValueError):
            with store.conn:
                stage_lineage(
                    store,
                    "other",
                    {
                        "run_id": "run",
                        "bronze": [{"path": key, "sha256": "a" * 64, "size": 1}],
                        "silver": [],
                        "gold": [],
                    },
                )
    with pytest.raises(ValueError, match="changed|immutable"):
        with store.conn:
            stage_lineage(store, "g", {"run_id": "different", "bronze": [], "silver": [], "gold": []})
    store.conn.close()
