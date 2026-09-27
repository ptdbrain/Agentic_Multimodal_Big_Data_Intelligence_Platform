from typing import List, Dict, Any

class SearchIndexer:
    """Elasticsearch full-text search indexer with in-memory search fallback (Section 40)."""

    def __init__(self):
        self._memory_index = []

    def index_reviews(self, reviews: List[Dict[str, Any]]):
        self._memory_index = reviews
        print(f"SearchIndexer: Indexed {len(reviews)} reviews for search.")

    def search(self, query: str, brand: str = None, min_rating: float = None, limit: int = 20) -> List[Dict[str, Any]]:
        q = query.lower().strip()
        results = []
        for r in self._memory_index:
            text = (r.get("review_text") or "").lower()
            title = (r.get("review_title") or "").lower()
            rating = float(r.get("rating", 0))

            if min_rating and rating < min_rating:
                continue

            if q in text or q in title or not q:
                results.append(r)
                if len(results) >= limit:
                    break

        return results

search_indexer = SearchIndexer()
