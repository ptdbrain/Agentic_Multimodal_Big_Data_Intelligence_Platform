import os
import json
import requests
from config.settings import settings
from typing import List, Dict, Any, Optional

class SearchIndexer:
    """Elasticsearch REST search adapter with explicitly selected memory backend (Section 40).
    Generic and flexible across products, reviews, and arbitrary schemas.
    """

    def __init__(self, backend=None, session=None, endpoint=None, index="sentinel-reviews"):
        self.backend = backend or ("memory" if settings.is_local_mode else "elasticsearch")
        if self.backend not in ("memory", "elasticsearch"):
            raise ValueError("Unknown search backend")
        self.session = session or requests.Session()
        self.endpoint = (endpoint or os.environ.get("ES_URL") or f"http://{settings.search.es_host}:{settings.search.es_port}").rstrip("/")
        self.index = index
        self._memory_index: List[Dict[str, Any]] = []

    def index_reviews(self, reviews: List[Dict[str, Any]]):
        """Index reviews dataset (backward-compatible alias)."""
        self.index_records(reviews)

    def index_records(self, records: List[Dict[str, Any]]):
        """Generic indexing method for arbitrary record collections."""
        if self.backend == "elasticsearch":
            lines = []
            for record in records:
                key = record.get("review_id") or record.get("product_id") or record.get("event_id")
                if key is None:
                    raise ValueError("Elasticsearch document requires a stable logical ID")
                lines.extend([json.dumps({"index": {"_index": self.index, "_id": str(key)}}),
                              json.dumps(record, ensure_ascii=False, allow_nan=False)])
            if not lines:
                return
            result = self._request("POST", "/_bulk?refresh=wait_for", data="\n".join(lines) + "\n",
                                   headers={"Content-Type": "application/x-ndjson"})
            if result.get("errors"):
                raise RuntimeError(f"Elasticsearch bulk indexing failed: {result.get('items')}")
            return
        self._memory_index = list(records)
        print(f"SearchIndexer: Indexed {len(records)} records for search.")

    def search(
        self,
        query: str,
        search_fields: Optional[List[str]] = None,
        brand: Optional[str] = None,
        min_rating: Optional[float] = None,
        filters: Optional[Dict[str, Any]] = None,
        limit: int = 20
    ) -> List[Dict[str, Any]]:
        if limit < 1:
            return []
        if self.backend == "elasticsearch":
            terms = dict(filters or {})
            if brand:
                terms["brand"] = brand
            filter_clauses = [{"term": {f"{k}.keyword": v}} for k, v in terms.items()]
            if min_rating is not None:
                filter_clauses.append({"range": {"rating": {"gte": min_rating}}})
            must = [{"multi_match": {"query": query, "fields": search_fields or ["review_text", "review_title", "product_name"]}}] if query else []
            result = self._request("POST", f"/{self.index}/_search", json={
                "size": limit, "query": {"bool": {"must": must, "filter": filter_clauses}}})
            return [hit["_source"] for hit in result["hits"]["hits"]]
        q = (query or "").lower().strip()
        results = []
        combined_filters = dict(filters or {})
        if brand:
            combined_filters["brand"] = brand

        for r in self._memory_index:
            # 1. Attribute filtering
            match_filters = True
            for k, expected_v in combined_filters.items():
                actual_v = r.get(k)
                if actual_v is None or str(actual_v).lower() != str(expected_v).lower():
                    match_filters = False
                    break
            if not match_filters:
                continue

            # 2. Rating filter if applicable
            if min_rating is not None:
                rating = float(r.get("rating", 0) or 0)
                if rating < min_rating:
                    continue

            # 3. Text search
            if not q:
                results.append(r)
            else:
                fields_to_search = search_fields or [
                    k for k, v in r.items() if isinstance(v, str)
                ]
                match_text = False
                for f in fields_to_search:
                    val = str(r.get(f, "") or "").lower()
                    if q in val:
                        match_text = True
                        break
                if match_text:
                    results.append(r)

            if len(results) >= limit:
                break

        return results

    def _request(self, method, path, **kwargs):
        response = self.session.request(method, self.endpoint + path, timeout=10, **kwargs)
        response.raise_for_status()
        return response.json()

    def health_check(self):
        if self.backend == "memory":
            return True
        return self._request("GET", "/_cluster/health").get("status") in ("green", "yellow")

    def clear(self):
        if self.backend == "elasticsearch":
            self._request("POST", f"/{self.index}/_delete_by_query?refresh=true", json={"query": {"match_all": {}}})
        self._memory_index = []

    @property
    def count(self) -> int:
        if self.backend == "elasticsearch":
            return int(self._request("GET", f"/{self.index}/_count")["count"])
        return len(self._memory_index)

search_indexer = SearchIndexer()

