import time
import random
import requests
from typing import List, Dict, Any, Optional, Tuple
from config.settings import settings

class APICollector:
    """Simulated & live REST API Collector with pagination, retry logic, and rate limiting.
    Configurable endpoint, rate limits, inventory simulation, and mock/live modes.
    """
    
    def __init__(
        self,
        endpoint_url: Optional[str] = None,
        rate_limit_rps: Optional[float] = None,
        total_items: int = 1000,
        price_range: Tuple[int, int] = (500000, 15000000),
        mock_mode: bool = False,
        session=None,
        max_retries: int = 3,
    ):
        if not mock_mode and not endpoint_url:
            raise ValueError("Live API collection requires an explicit endpoint_url")
        self.endpoint_url = endpoint_url
        self.session = session or requests.Session()
        self.max_retries = max(1, max_retries)
        self.rate_limit_rps = rate_limit_rps if rate_limit_rps is not None else settings.ingestion.rate_limit_rps
        self._delay = 1.0 / self.rate_limit_rps if self.rate_limit_rps > 0 else 0.0
        self.total_items = total_items
        self.price_range = price_range
        self.mock_mode = mock_mode

    def fetch_page(self, page: int = 1, page_size: Optional[int] = None, category: Optional[str] = None) -> Dict[str, Any]:
        """Fetches a paginated batch of data."""
        p_size = page_size if page_size is not None else settings.ingestion.default_batch_size
        if self._delay > 0:
            time.sleep(self._delay)
        
        if page < 1 or p_size < 1:
            raise ValueError("page and page_size must be positive")
        if not self.mock_mode:
            params = {"page": page, "page_size": p_size}
            if category is not None:
                params["category"] = category
            for attempt in range(self.max_retries):
                try:
                    response = self.session.get(self.endpoint_url, params=params,
                                                timeout=settings.ingestion.request_timeout_sec)
                    if response.status_code == 429 or response.status_code >= 500:
                        if attempt + 1 < self.max_retries:
                            time.sleep(min(2 ** attempt, 4))
                            continue
                    response.raise_for_status()
                    body = response.json()
                    if (not isinstance(body, dict) or not isinstance(body.get("data"), list)
                            or any(not isinstance(item, dict) for item in body["data"])
                            or not isinstance(body.get("has_next"), bool)
                            or body.get("page") != page):
                        raise ValueError("API response must contain data records, page and boolean has_next")
                    return body
                except (requests.ConnectionError, requests.Timeout):
                    if attempt + 1 == self.max_retries:
                        raise
                    time.sleep(min(2 ** attempt, 4))

        # Explicit synthetic mode only.
        items = []
        start_idx = (page - 1) * p_size
        end_idx = min(start_idx + p_size, self.total_items)

        for i in range(start_idx, end_idx):
            item_id = f"api_prod_{page}_{i - start_idx + 1}"
            items.append({
                "item_id": item_id,
                "title": f"Smart Gadget {page}-{i - start_idx + 1}",
                "category": category or "Electronics",
                "price": random.randint(self.price_range[0], self.price_range[1]),
                "timestamp": time.time()
            })
            
        return {
            "page": page,
            "page_size": p_size,
            "total_items": self.total_items,
            "has_next": page * p_size < self.total_items,
            "data": items
        }

    def fetch_all(self, max_pages: Optional[int] = None, category: Optional[str] = None) -> List[Dict[str, Any]]:
        limit_pages = max_pages if max_pages is not None else settings.ingestion.max_api_pages
        results = []
        for page in range(1, limit_pages + 1):
            res = self.fetch_page(page=page, category=category)
            results.extend(res["data"])
            if not res["has_next"]:
                break
        return results

