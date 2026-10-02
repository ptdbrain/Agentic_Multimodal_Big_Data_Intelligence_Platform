import datetime
from typing import List, Dict, Any, Optional
from config.settings import settings
from ingestion.crawler.base import BaseEcommerceCrawler

class TikiCrawler(BaseEcommerceCrawler):
    """Production-ready real-time crawler for Tiki.vn e-commerce marketplace.
    Extracts live catalog specs, pricing, and genuine customer feedback.
    """

    def __init__(
        self,
        base_url: Optional[str] = None,
        products_endpoint: Optional[str] = None,
        reviews_endpoint: Optional[str] = None,
        **kwargs
    ):
        b_url = base_url or settings.crawler.tiki_base_url
        super().__init__(platform_name="Tiki", base_url=b_url, **kwargs)
        self.products_endpoint = products_endpoint or settings.crawler.tiki_products_endpoint
        self.reviews_endpoint = reviews_endpoint or settings.crawler.tiki_reviews_endpoint

    def search_products(self, query: str, limit: int = 20, page: int = 1) -> List[Dict[str, Any]]:
        """Search products on Tiki by keyword."""
        url = f"{self.base_url}{self.products_endpoint}"
        params = {
            "q": query,
            "limit": limit,
            "page": page
        }
        resp = self._request_with_retry("GET", url, params=params)
        if resp and resp.status_code == 200:
            try:
                data = resp.json()
                return data.get("data", [])
            except Exception as e:
                print(f"[Tiki] JSON decode error: {e}")
                return []
        return []

    def get_product_details(self, product_id: str) -> Optional[Dict[str, Any]]:
        """Fetch granular product specification and live seller pricing from Tiki."""
        clean_id = str(product_id).replace("tiki_", "")
        url = f"{self.base_url}{self.products_endpoint}/{clean_id}"
        resp = self._request_with_retry("GET", url)
        if resp and resp.status_code == 200:
            try:
                return resp.json()
            except Exception:
                return None
        return None

    def get_product_reviews(self, product_id: str, limit: int = 50, page: int = 1) -> List[Dict[str, Any]]:
        """Fetch real customer feedback and ratings from Tiki reviews API."""
        clean_id = str(product_id).replace("tiki_", "")
        url = f"{self.base_url}{self.reviews_endpoint}"
        params = {
            "product_id": clean_id,
            "limit": limit,
            "page": page,
            "sort": "score|desc"
        }
        resp = self._request_with_retry("GET", url, params=params)
        if resp and resp.status_code == 200:
            try:
                data = resp.json()
                return data.get("data", [])
            except Exception as e:
                print(f"[Tiki] JSON decode error on reviews: {e}")
                return []
        return []

    def standardize_product(self, raw_item: Dict[str, Any]) -> Dict[str, Any]:
        """Maps Tiki raw product JSON to SentinelAI canonical Product schema."""
        raw_id = raw_item.get("id")
        p_id = f"tiki_{raw_id}" if raw_id else "tiki_unknown"
        now_iso = datetime.datetime.now(datetime.timezone.utc).isoformat().replace("+00:00", "Z")

        url_path = raw_item.get("url_path", "")
        prod_url = f"https://tiki.vn/{url_path}" if url_path else f"https://tiki.vn/p/{raw_id}.html"

        return {
            "product_id": p_id,
            "product_name": str(raw_item.get("name", "Unknown Product")).strip(),
            "brand": str(raw_item.get("brand_name") or "Generic").strip(),
            "category": str(raw_item.get("primary_category_name") or "Electronics").strip(),
            "subcategory": str(raw_item.get("primary_category_path") or "General").split("/")[-1].strip(),
            "price": float(raw_item.get("price") or raw_item.get("original_price") or 0.0),
            "currency": "VND",
            "rating": round(float(raw_item.get("rating_average") or 0.0), 1),
            "review_count": int(raw_item.get("review_count") or 0),
            "seller": str(raw_item.get("seller_name") or "Tiki Trading").strip(),
            "product_url": prod_url,
            "image_url": str(raw_item.get("thumbnail_url") or ""),
            "source": "tiki_crawler",
            "created_at": now_iso,
            "updated_at": now_iso
        }

    def standardize_review(self, raw_review: Dict[str, Any], product_id: Optional[str] = None) -> Dict[str, Any]:
        """Maps Tiki raw review JSON to SentinelAI canonical Review schema."""
        rev_id = raw_review.get("id")
        tiki_p_id = raw_review.get("product_id")
        resolved_pid = product_id or (f"tiki_{tiki_p_id}" if tiki_p_id else "unknown_product")
        
        # Parse timestamp
        created_at_val = raw_review.get("created_at")
        if isinstance(created_at_val, (int, float)):
            rev_time = datetime.datetime.fromtimestamp(created_at_val, tz=datetime.timezone.utc).isoformat().replace("+00:00", "Z")
        elif isinstance(created_at_val, str) and created_at_val:
            rev_time = created_at_val
        else:
            rev_time = datetime.datetime.now(datetime.timezone.utc).isoformat().replace("+00:00", "Z")

        title = str(raw_review.get("title") or "Đánh giá từ khách hàng").strip()
        text = str(raw_review.get("content") or title).strip()

        return {
            "review_id": f"tiki_rv_{rev_id}" if rev_id else f"tiki_rv_{int(datetime.datetime.now().timestamp() * 1000)}",
            "product_id": resolved_pid,
            "user_id": f"tiki_usr_{raw_review.get('customer_id') or 'anon'}",
            "rating": float(raw_review.get("rating") or 5.0),
            "review_title": title,
            "review_text": text,
            "review_date": rev_time,
            "verified_purchase": True,
            "helpful_count": int(raw_review.get("thank_count") or 0),
            "source": "tiki_crawler",
            "language": "vi",
            "created_at": rev_time
        }
