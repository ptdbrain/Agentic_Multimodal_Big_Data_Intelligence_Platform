"""Explicit protocol-double unit tests; real Elasticsearch is a separate integration gate."""

import json, sqlite3
from pathlib import Path
import pytest

pytestmark = pytest.mark.unit


class SearchProtocol:
    def __init__(self, fail_bulk=False):
        self.indices = {}
        self.aliases = {}
        self.fail_bulk = fail_bulk
        self.alias_changes = 0

    def request(self, method, path, **kwargs):
        if path == "/_aliases":
            self.alias_changes += 1
            for action in kwargs["json"]["actions"]:
                if "remove" in action:
                    self.aliases.pop(action["remove"]["alias"], None)
                if "add" in action:
                    self.aliases[action["add"]["alias"]] = action["add"]["index"]
            return {"acknowledged": True}
        if path.startswith("/_alias/"):
            alias = path.split("/")[-1]
            return {self.aliases[alias]: {}} if alias in self.aliases else {}
        parts = path.strip("/").split("/")
        index = parts[0]
        if method == "PUT":
            self.indices[index] = {}
            return {"acknowledged": True}
        if path == "/_bulk?refresh=wait_for":
            if self.fail_bulk:
                raise RuntimeError("Explicit bulk failure")
            lines = kwargs["data"].splitlines()
            for i in range(0, len(lines), 2):
                meta = json.loads(lines[i])["index"]
                self.indices[meta["_index"]][meta["_id"]] = json.loads(lines[i + 1])
            return {"errors": False}
        if parts[-1] == "_count":
            return {"count": len(self.indices[index])}
        if parts[-1] == "_mget":
            return {
                "docs": [
                    {"found": key in self.indices[index], "_id": key, "_source": self.indices[index].get(key)}
                    for key in kwargs["json"]["ids"]
                ]
            }
        raise AssertionError((method, path))


def setup(tmp_path):
    from tests.test_serving_registry import registry_file
    from serving.registry import DatasetRegistry
    from serving.store import ServingStore

    binding = DatasetRegistry.load(registry_file(tmp_path)).datasets["tiki"]
    c = sqlite3.connect(binding.sqlite_path)
    c.execute("CREATE TABLE products(product_id TEXT PRIMARY KEY,product_name TEXT,source_group TEXT)")
    c.execute("INSERT INTO products VALUES('p','SQL source','marketplace')")
    c.execute("CREATE TABLE reviews(review_id TEXT PRIMARY KEY,review_text TEXT,source_group TEXT)")
    store = ServingStore(c, "tiki", "marketplace")
    store.initialize()
    with c:
        store.stage_publication("g", "run", "commerce")
    c.close()
    return binding


def test_generation_alias_published_only_after_count_and_content_verification(tmp_path):
    from serving.search import SearchWorker

    binding = setup(tmp_path)
    protocol = SearchProtocol()
    worker = SearchWorker(binding, protocol.request, page_size=1)
    assert worker.run_once() == 2
    assert protocol.alias_changes == 2
    index = protocol.aliases["sentinel-tiki-products-read"]
    assert len(protocol.indices[index]) == 1
    assert next(iter(protocol.indices[index].values()))["product_name"] == "SQL source"
    assert worker.run_once() == 0
    with sqlite3.connect(binding.sqlite_path) as c:
        assert c.execute("SELECT COUNT(*) FROM search_jobs WHERE state='complete'").fetchone()[0] == 2


def test_failed_bulk_keeps_previous_alias_and_work_recoverable(tmp_path):
    from serving.search import SearchWorker

    binding = setup(tmp_path)
    protocol = SearchProtocol(fail_bulk=True)
    with pytest.raises(RuntimeError):
        SearchWorker(binding, protocol.request).run_once()
    assert protocol.alias_changes == 0
    with sqlite3.connect(binding.sqlite_path) as c:
        assert c.execute("SELECT state FROM search_jobs WHERE entity_kind='products'").fetchone()[0] == "failed"
    protocol.fail_bulk = False
    assert SearchWorker(binding, protocol.request).run_once() == 2


def test_search_query_is_dataset_bound_and_uses_stable_index_pagination(tmp_path):
    from serving.search import search_snapshot

    binding = setup(tmp_path)
    calls = []

    def request(method, path, **kwargs):
        calls.append((method, path, kwargs))
        return {
            "hits": {
                "total": {"value": 2, "relation": "eq"},
                "hits": [
                    {
                        "_source": {"__dataset_id": binding.id, "__generation": "g", "product_id": "p"},
                        "sort": ["doc-id"],
                    }
                ],
            }
        }

    result = search_snapshot(
        binding,
        "products",
        "sentinel-tiki-products-test",
        "g",
        "phone",
        request=request,
        page_size=10,
        after=["previous"],
    )
    assert result["total"] == 2 and result["next_after"] == ["doc-id"]
    query = calls[0][2]["json"]
    assert calls[0][1] == "/sentinel-tiki-products-test/_search" and query["search_after"] == ["previous"]
    assert {"term": {"__dataset_id": "tiki"}} in query["query"]["bool"]["filter"]
    with pytest.raises(ValueError):
        search_snapshot(binding, "products", "../../foreign", "g", "phone", request=request)
