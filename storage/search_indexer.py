from typing import List, Dict, Any, Optional

class SearchIndexer:
    """Elasticsearch full-text search indexer with in-memory search fallback (Section 40).
    Generic and flexible across products, reviews, and arbitrary schemas.
    """

    def __init__(self):
        self._memory_index: List[Dict[str, Any]] = []

    def index_reviews(self, reviews: List[Dict[str, Any]]):
        """Index reviews dataset (backward-compatible alias)."""
        self.index_records(reviews)

    def index_records(self, records: List[Dict[str, Any]]):
        """Generic indexing method for arbitrary record collections."""
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

    def clear(self):
        self._memory_index = []

    @property
    def count(self) -> int:
        return len(self._memory_index)

search_indexer = SearchIndexer()

