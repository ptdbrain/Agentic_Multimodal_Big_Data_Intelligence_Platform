import time
import requests
from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional
from config.settings import settings

class BaseEcommerceCrawler(ABC):
    """Abstract Base Class for E-Commerce Crawlers.
    Provides standardized session management, exponential backoff retries,
    rate limiting, and data normalization into SentinelAI canonical schemas.
    """

    def __init__(
        self,
        platform_name: str,
        base_url: str,
        user_agent: Optional[str] = None,
        timeout_sec: Optional[int] = None,
        rate_limit_rps: Optional[float] = None,
        max_retries: Optional[int] = None,
        retry_backoff_sec: Optional[float] = None,
        custom_headers: Optional[Dict[str, str]] = None
    ):
        self.platform_name = platform_name
        self.base_url = base_url.rstrip("/")
        self.user_agent = user_agent or settings.crawler.user_agent
        self.timeout_sec = timeout_sec if timeout_sec is not None else settings.crawler.request_timeout_sec
        self.rate_limit_rps = rate_limit_rps if rate_limit_rps is not None else settings.crawler.rate_limit_rps
        self.max_retries = max_retries if max_retries is not None else settings.crawler.max_retries
        self.retry_backoff_sec = retry_backoff_sec if retry_backoff_sec is not None else settings.crawler.retry_backoff_sec

        self._min_interval = 1.0 / self.rate_limit_rps if self.rate_limit_rps > 0 else 0.0
        self._last_request_time = 0.0

        self.session = requests.Session()
        default_headers = {
            "User-Agent": self.user_agent,
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7",
            "Connection": "keep-alive"
        }
        if custom_headers:
            default_headers.update(custom_headers)
        self.session.headers.update(default_headers)

    def _rate_limit_sleep(self):
        """Enforces rate limiting between consecutive outgoing HTTP requests."""
        if self._min_interval <= 0:
            return
        elapsed = time.time() - self._last_request_time
        if elapsed < self._min_interval:
            time.sleep(self._min_interval - elapsed)
        self._last_request_time = time.time()

    def _request_with_retry(self, method: str, url: str, **kwargs) -> Optional[requests.Response]:
        """Executes HTTP request with exponential backoff and retry logic."""
        if "timeout" not in kwargs:
            kwargs["timeout"] = self.timeout_sec

        for attempt in range(1, self.max_retries + 1):
            self._rate_limit_sleep()
            try:
                resp = self.session.request(method, url, **kwargs)
                if resp.status_code == 200:
                    return resp
                elif resp.status_code in [429, 500, 502, 503, 504]:
                    backoff = self.retry_backoff_sec * (2 ** (attempt - 1))
                    print(f"[{self.platform_name}] HTTP {resp.status_code} on {url}. Retrying {attempt}/{self.max_retries} in {backoff:.1f}s...")
                    time.sleep(backoff)
                else:
                    print(f"[{self.platform_name}] Non-retryable HTTP {resp.status_code} on {url}")
                    return resp
            except (requests.RequestException, Exception) as exc:
                backoff = self.retry_backoff_sec * (2 ** (attempt - 1))
                print(f"[{self.platform_name}] Connection error: {exc}. Retrying {attempt}/{self.max_retries} in {backoff:.1f}s...")
                time.sleep(backoff)
        return None

    @abstractmethod
    def search_products(self, query: str, limit: int = 20, page: int = 1) -> List[Dict[str, Any]]:
        """Search products on the platform by keyword."""
        raise NotImplementedError

    @abstractmethod
    def get_product_details(self, product_id: str) -> Optional[Dict[str, Any]]:
        """Fetch granular product specification and live seller pricing."""
        raise NotImplementedError

    @abstractmethod
    def get_product_reviews(self, product_id: str, limit: int = 50, page: int = 1) -> List[Dict[str, Any]]:
        """Fetch customer feedback and ratings for a given product."""
        raise NotImplementedError

    @abstractmethod
    def standardize_product(self, raw_item: Dict[str, Any]) -> Dict[str, Any]:
        """Maps platform-specific raw JSON to SentinelAI canonical Product schema."""
        raise NotImplementedError

    @abstractmethod
    def standardize_review(self, raw_review: Dict[str, Any], product_id: Optional[str] = None) -> Dict[str, Any]:
        """Maps platform-specific raw JSON to SentinelAI canonical Review schema."""
        raise NotImplementedError

    def crawl_category(
        self,
        query: str,
        max_products: int = 10,
        reviews_per_product: int = 10
    ) -> Dict[str, Any]:
        """Batch crawl products and their respective customer reviews by search query/category."""
        print(f"[{self.platform_name}] Starting crawl for '{query}' (max_products={max_products}, reviews_per_prod={reviews_per_product})...")
        raw_products = self.search_products(query, limit=max_products, page=1)
        
        standard_products = []
        all_reviews = []

        for p in raw_products[:max_products]:
            std_p = self.standardize_product(p)
            standard_products.append(std_p)

            # Extract platform product id to query reviews
            orig_id = str(p.get("id") or std_p.get("product_id"))
            if reviews_per_product > 0:
                raw_revs = self.get_product_reviews(orig_id, limit=reviews_per_product, page=1)
                for r in raw_revs:
                    all_reviews.append(self.standardize_review(r, product_id=std_p["product_id"]))

        print(f"[{self.platform_name}] Crawl completed: {len(standard_products)} products, {len(all_reviews)} reviews.")
        return {
            "platform": self.platform_name,
            "query": query,
            "products_count": len(standard_products),
            "reviews_count": len(all_reviews),
            "products": standard_products,
            "reviews": all_reviews
        }
