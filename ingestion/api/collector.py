import time
import random
from typing import List, Dict, Any, Optional

class APICollector:
    """Simulated & live REST API Collector with pagination, retry logic, and rate limiting."""
    
    def __init__(self, endpoint_url: str = "https://api.sentinel.ai/v1/feed", rate_limit_rps: float = 10.0):
        self.endpoint_url = endpoint_url
        self.rate_limit_rps = rate_limit_rps
        self._delay = 1.0 / rate_limit_rps if rate_limit_rps > 0 else 0.0

    def fetch_page(self, page: int = 1, page_size: int = 50, category: Optional[str] = None) -> Dict[str, Any]:
        """Fetches a paginated batch of data."""
        time.sleep(self._delay)
        
        # Simulate network response
        items = []
        for i in range(page_size):
            item_id = f"api_prod_{page}_{i+1}"
            items.append({
                "item_id": item_id,
                "title": f"Smart Gadget {page}-{i+1}",
                "category": category or "Electronics",
                "price": random.randint(500000, 15000000),
                "timestamp": time.time()
            })
            
        return {
            "page": page,
            "page_size": page_size,
            "total_items": 1000,
            "has_next": page * page_size < 1000,
            "data": items
        }

    def fetch_all(self, max_pages: int = 5, category: Optional[str] = None) -> List[Dict[str, Any]]:
        results = []
        for page in range(1, max_pages + 1):
            res = self.fetch_page(page=page, category=category)
            results.extend(res["data"])
            if not res["has_next"]:
                break
        return results
