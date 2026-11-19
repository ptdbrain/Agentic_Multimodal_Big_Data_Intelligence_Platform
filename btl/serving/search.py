"""Generation-verified PostgreSQL/SQLite snapshot to real Elasticsearch publication."""

import hashlib
import json
import os
import uuid
from datetime import date, datetime
from decimal import Decimal
from typing import Protocol, TypedDict, Unpack

import requests

from serving.admission import writer_transaction
from serving.ownership import acquire_stream_owner
from serving.registry import DatasetRef
from serving.store import ServingStore, connect_dataset, now

ENTITIES = {"products": "product_id", "reviews": "review_id", "articles": "article_id"}


class RequestOptions(TypedDict, total=False):
    json: dict
    data: str
    headers: dict[str, str]


class SearchRequest(Protocol):
    def __call__(self, method: str, path: str, **kwargs: Unpack[RequestOptions]) -> dict: ...


def encode(value: object) -> str:
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, (bytes, memoryview)):
        return bytes(value).hex()
    raise TypeError("Unsupported SQL search value")


def canonical(value: object) -> str:
    return json.dumps(value, default=encode, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)


class Elasticsearch:
    def __init__(self, endpoint: str | None = None) -> None:
        self.endpoint = (endpoint or os.environ.get("ES_URL", "http://elasticsearch:9200")).rstrip("/")
        self.session = requests.Session()

    def request(self, method: str, path: str, **kwargs: Unpack[RequestOptions]) -> dict:
        response = self.session.request(method, self.endpoint + path, timeout=30, **kwargs)
        if response.status_code == 404 and path.startswith("/_alias/"):
            return {}
        response.raise_for_status()
        result = response.json()
        if not isinstance(result, dict):
            raise RuntimeError("Invalid Elasticsearch response")
        return result


class SearchWorker:
    def __init__(self, binding: DatasetRef, request: SearchRequest | None = None, *, page_size: int = 1000) -> None:
        if type(page_size) is not int or not 1 <= page_size <= 5000:
            raise ValueError("Bounded page size required")
        self.binding = binding
        self.request = request or Elasticsearch().request
        self.page_size = page_size

    def run_once(self) -> int:
        connection = connect_dataset(self.binding)
        store = ServingStore(connection, self.binding.id, self.binding.source_group)
        completed = 0
        try:
            with writer_transaction(connection):
                store.initialize()
            # Uses a separate identity from a live stream; serializes search workers only.
            from dataclasses import replace

            lock_binding = replace(self.binding, id=self.binding.id + "-search")
            with acquire_stream_owner(connection, lock_binding):
                generation = store.execute(
                    "SELECT generation FROM serving_generation WHERE dataset_id=?", (self.binding.id,)
                ).fetchone()
                connection.commit()
                if generation is None:
                    return 0
                current = generation[0]
                jobs = store.execute(
                    "SELECT generation,entity_kind,state FROM search_jobs WHERE dataset_id=? AND state!='complete' ORDER BY created_at,entity_kind",
                    (self.binding.id,),
                ).fetchall()
                connection.commit()
                for gen, entity, state in jobs:
                    if entity not in ENTITIES:
                        raise ValueError("Unsupported indexed entity")
                    if gen != current:
                        with writer_transaction(connection):
                            store.execute(
                                "UPDATE search_jobs SET state='superseded' WHERE dataset_id=? AND generation=? AND entity_kind=?",
                                (self.binding.id, gen, entity),
                            )
                        continue
                    try:
                        if self._publish(store, gen, entity):
                            completed += 1
                    except Exception as error:
                        with writer_transaction(connection):
                            store.execute(
                                "UPDATE search_jobs SET state='failed',error_kind=? WHERE dataset_id=? AND generation=? AND entity_kind=?",
                                (type(error).__name__, self.binding.id, gen, entity),
                            )
                        raise
            return completed
        finally:
            connection.close()

    def _publish(self, store: ServingStore, generation: str, entity: str) -> bool:
        read = connect_dataset(self.binding)
        index = "sentinel-" + self.binding.id + "-" + entity + "-" + uuid.uuid4().hex
        alias = "sentinel-" + self.binding.id + "-" + entity + "-read"
        try:
            if self.binding.sqlite_path is None:
                read.set_session(readonly=True, isolation_level="REPEATABLE READ")
            else:
                read.execute("BEGIN")
            source = ServingStore(read, self.binding.id, self.binding.source_group)
            current = source.execute(
                "SELECT generation FROM serving_generation WHERE dataset_id=?", (self.binding.id,)
            ).fetchone()
            if current is None or current[0] != generation:
                with writer_transaction(store.conn):
                    store.execute(
                        "UPDATE search_jobs SET state='superseded' WHERE dataset_id=? AND generation=? AND entity_kind=?",
                        (self.binding.id, generation, entity),
                    )
                return False
            with writer_transaction(store.conn):
                store.execute(
                    "UPDATE search_jobs SET state='building',attempts=attempts+1,error_kind=NULL,index_name=? WHERE dataset_id=? AND generation=? AND entity_kind=?",
                    (index, self.binding.id, generation, entity),
                )
            created = self.request(
                "PUT",
                "/" + index,
                json={
                    "settings": {
                        "number_of_shards": 1,
                        "number_of_replicas": 0,
                        "analysis": {
                            "analyzer": {
                                "sentinel_text": {"tokenizer": "standard", "filter": ["lowercase", "asciifolding"]}
                            }
                        },
                    },
                    "mappings": {
                        "date_detection": False,
                        "properties": {
                            "__document_id": {"type": "keyword"},
                            **{
                                name: {"type": "text", "analyzer": "sentinel_text"}
                                for name in ("product_name", "review_text", "review_title", "title", "body")
                            },
                            "__dataset_id": {"type": "keyword"},
                            "__generation": {"type": "keyword"},
                            "brand": {"type": "keyword"},
                            "category": {"type": "keyword"},
                            "source_name": {"type": "keyword"},
                            "source_group": {"type": "keyword"},
                            "rating": {"type": "double"},
                            "product_id": {"type": "keyword"},
                        },
                    },
                },
            )
            if not created.get("acknowledged"):
                raise RuntimeError("Search generation creation not acknowledged")
            cursor = (
                read.cursor(name="search_snapshot_" + uuid.uuid4().hex)
                if self.binding.sqlite_path is None
                else read.cursor()
            )
            cursor.execute("SELECT * FROM " + entity + " ORDER BY " + ENTITIES[entity])
            if self.binding.sqlite_path is None:
                cursor.fetchmany(0)  # initializes named cursor description on psycopg2
            columns = [column[0] for column in cursor.description]
            count = 0
            content = hashlib.sha256()
            while True:
                rows = cursor.fetchmany(self.page_size)
                if not rows:
                    break
                expected = {}
                lines = []
                for values in rows:
                    record = json.loads(canonical(dict(zip(columns, values))))
                    group = record.get("source_group") or "unverified"
                    if group != self.binding.source_group:
                        raise ValueError("SQL snapshot provenance differs from dataset")
                    key = record.get(ENTITIES[entity])
                    if key is None:
                        raise ValueError("Indexed logical ID missing")
                    document_id = hashlib.sha256((self.binding.id + "|" + entity + "|" + str(key)).encode()).hexdigest()
                    record.update(__dataset_id=self.binding.id, __generation=generation, __document_id=document_id)
                    encoded = canonical(record)
                    expected[document_id] = hashlib.sha256(encoded.encode()).hexdigest()
                    content.update(document_id.encode() + expected[document_id].encode())
                    count += 1
                    lines.extend([json.dumps({"index": {"_index": index, "_id": document_id}}), encoded])
                result = self.request(
                    "POST",
                    "/_bulk?refresh=wait_for",
                    data="\n".join(lines) + "\n",
                    headers={"Content-Type": "application/x-ndjson"},
                )
                if result.get("errors"):
                    raise RuntimeError("Elasticsearch bulk contains failed items")
                received = self.request("POST", "/" + index + "/_mget", json={"ids": list(expected)}).get("docs", [])
                if len(received) != len(expected) or any(
                    not doc.get("found")
                    or hashlib.sha256(canonical(doc["_source"]).encode()).hexdigest() != expected.get(doc["_id"])
                    for doc in received
                ):
                    raise RuntimeError("Indexed document content verification failed")
            cursor.close()
            actual = self.request("GET", "/" + index + "/_count").get("count")
            if actual != count:
                raise RuntimeError("Indexed generation count mismatch")
            with writer_transaction(store.conn):
                statement = "SELECT generation FROM serving_generation WHERE dataset_id=?" + (
                    "" if store.sqlite else " FOR UPDATE"
                )
                current = store.execute(statement, (self.binding.id,)).fetchone()
                if current is None or current[0] != generation:
                    store.execute(
                        "UPDATE search_jobs SET state='superseded' WHERE dataset_id=? AND generation=? AND entity_kind=?",
                        (self.binding.id, generation, entity),
                    )
                    return False
                existing = self.request("GET", "/_alias/" + alias)
                if any(not name.startswith("sentinel-" + self.binding.id + "-" + entity + "-") for name in existing):
                    raise ValueError("Search alias points outside owned namespace")
                actions = [{"remove": {"index": name, "alias": alias}} for name in existing]
                actions.append({"add": {"index": index, "alias": alias}})
                if not self.request("POST", "/_aliases", json={"actions": actions}).get("acknowledged"):
                    raise RuntimeError("Search alias publication not acknowledged")
                store.execute(
                    "UPDATE search_jobs SET state='complete',document_count=?,content_sha256=?,completed_at=? WHERE dataset_id=? AND generation=? AND entity_kind=?",
                    (count, content.hexdigest(), now(), self.binding.id, generation, entity),
                )
            return True
        finally:
            read.close()


def search_snapshot(
    binding: DatasetRef,
    entity: str,
    index_name: str,
    generation: str,
    query: str,
    *,
    request: SearchRequest | None = None,
    page_size: int = 20,
    after: list | None = None,
    filters: dict | None = None,
    min_rating: float | None = None,
) -> dict:
    import math
    import re

    if entity not in ENTITIES:
        raise ValueError("Unsupported search entity")
    prefix = "sentinel-" + binding.id + "-" + entity + "-"
    if not index_name.startswith(prefix) or not re.fullmatch(r"[a-z0-9_-]+", index_name):
        raise ValueError("Search index is outside dataset namespace")
    if type(page_size) is not int or not 1 <= page_size <= 100:
        raise ValueError("Search page size must be1..100")
    if len(query) > 500:
        raise ValueError("Query is too long")
    clauses = [{"term": {"__dataset_id": binding.id}}, {"term": {"__generation": generation}}]
    for key, value in (filters or {}).items():
        if key not in {"brand", "category", "source_name"}:
            raise ValueError("Unsupported search filter")
        clauses.append({"term": {key: str(value)}})
    if min_rating is not None:
        rating = float(min_rating)
        if not math.isfinite(rating) or not 1 <= rating <= 5:
            raise ValueError("Rating filter outside1..5")
        clauses.append({"range": {"rating": {"gte": rating}}})
    body = {
        "size": page_size,
        "track_total_hits": True,
        "sort": [{"__document_id": "asc"}],
        "query": {
            "bool": {
                "filter": clauses,
                "must": [
                    {
                        "multi_match": {
                            "query": query,
                            "fields": ["product_name", "review_text", "review_title", "title", "body"],
                        }
                    }
                ]
                if query.strip()
                else [],
            }
        },
    }
    if after is not None:
        if not isinstance(after, list) or len(after) != 1 or not isinstance(after[0], str):
            raise ValueError("Invalid search cursor")
        body["search_after"] = after
    response = (request or Elasticsearch().request)("POST", "/" + index_name + "/_search", json=body)
    hits = response["hits"]["hits"]
    for hit in hits:
        if hit["_source"].get("__dataset_id") != binding.id or hit["_source"].get("__generation") != generation:
            raise ValueError("Search response violates dataset/generation binding")
    return {
        "records": [hit["_source"] for hit in hits],
        "total": response["hits"]["total"]["value"],
        "next_after": hits[-1]["sort"] if hits else None,
    }
