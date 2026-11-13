"""Publication and its search work must commit together, without downstream ES dependence."""

import sqlite3
import pytest

pytestmark = pytest.mark.unit


def test_publication_outbox_is_atomic_and_replay_keeps_one_job_per_entity(tmp_path):
    from serving.store import ServingStore

    c = sqlite3.connect(tmp_path / "publication.sqlite")
    store = ServingStore(c, "tiki", "marketplace")
    store.initialize()
    c.commit()
    with pytest.raises(RuntimeError):
        with c:
            store.stage_publication("g1", "run-one", "commerce", data_as_of="2026-11-13T10:00:00Z")
            raise RuntimeError("warehouse transaction rolled back")
    assert c.execute("SELECT COUNT(*) FROM search_jobs").fetchone()[0] == 0
    assert c.execute("SELECT COUNT(*) FROM serving_generation").fetchone()[0] == 0
    with c:
        store.stage_publication("g1", "run-one", "commerce", data_as_of="2026-11-13T10:00:00Z")
    with c:
        store.stage_publication("g1", "run-one", "commerce", data_as_of="2026-11-13T10:00:00Z")
    assert c.execute("SELECT COUNT(*) FROM search_jobs").fetchone()[0] == 2
    with c:
        store.stage_publication("g2", "run-two", "commerce")
    assert c.execute("SELECT generation FROM serving_generation").fetchone()[0] == "g2"
    assert c.execute("SELECT COUNT(*) FROM search_jobs WHERE generation='g1'").fetchone()[0] == 2
    c.close()


def test_article_publication_enqueues_articles_only(tmp_path):
    from serving.store import ServingStore

    c = sqlite3.connect(tmp_path / "publication.sqlite")
    s = ServingStore(c, "web", "web_content")
    s.initialize()
    c.commit()
    with c:
        s.stage_publication("g", "run", "web")
    assert c.execute("SELECT entity_kind FROM search_jobs").fetchall() == [("articles",)]
    c.close()
