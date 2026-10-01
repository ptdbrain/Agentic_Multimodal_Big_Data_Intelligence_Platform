import time
import random
from typing import Dict, Any, List, Optional
from ingestion.crawler.base import BaseEcommerceCrawler

DEFAULT_MOCK_SELLERS = ["Official Flagship", "TechZone Store", "MobileHub", "ElectroWorld"]

class MockEcommerceCrawler(BaseEcommerceCrawler):
    """Mock E-Commerce Crawler for simulation, automated testing, and CI/CD environments."""

    def __init__(
        self,
        target_domain: str = "retail.sentinel.ai",
        sellers: Optional[List[str]] = None,
        warranty_options: Optional[List[int]] = None,
        max_shipping_days: int = 3,
        **kwargs
    ):
        super().__init__(platform_name="MockSimulator", base_url=f"https://{target_domain}", **kwargs)
        self.target_domain = target_domain
        self.sellers = sellers or DEFAULT_MOCK_SELLERS
        self.warranty_options = warranty_options or [12, 24]
        self.max_shipping_days = max(1, max_shipping_days)

    def scrape_product_page(self, product_id: str) -> Dict[str, Any]:
        """Extracts detailed product specs and live seller quote (backward-compatible method)."""
        return {
            "product_id": str(product_id),
            "scraped_url": f"https://{self.target_domain}/products/{product_id}",
            "scraped_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "seller": random.choice(self.sellers),
            "stock_status": "in_stock",
            "warranty_months": random.choice(self.warranty_options),
            "shipping_days": random.randint(1, self.max_shipping_days)
        }

    def scrape_batch(self, product_ids: List[str]) -> List[Dict[str, Any]]:
        return [self.scrape_product_page(pid) for pid in product_ids]

    def search_products(self, query: str, limit: int = 20, page: int = 1) -> List[Dict[str, Any]]:
        results = []
        for i in range(limit):
            pid = f"mock_{query.lower().replace(' ', '_')}_{page}_{i+1}"
            results.append({
                "id": pid,
                "name": f"{query.title()} Model {i+1}",
                "brand": "SentinelTech",
                "price": random.randint(10000000, 30000000),
                "rating_average": round(random.uniform(4.0, 5.0), 1),
                "review_count": random.randint(50, 500),
                "seller_name": random.choice(self.sellers)
            })
        return results

    def get_product_details(self, product_id: str) -> Optional[Dict[str, Any]]:
        return self.scrape_product_page(product_id)

    def get_product_reviews(self, product_id: str, limit: int = 50, page: int = 1) -> List[Dict[str, Any]]:
        reviews = []
        for i in range(limit):
            reviews.append({
                "id": f"mock_rv_{product_id}_{i+1}",
                "product_id": product_id,
                "customer_id": f"usr_{i+1}",
                "rating": random.choice([4.0, 5.0, 5.0, 3.0, 1.0]),
                "title": "Trải nghiệm sản phẩm",
                "content": "Sản phẩm dùng tốt, đúng như mô tả.",
                "created_at": time.time() - (i * 3600),
                "thank_count": random.randint(0, 10)
            })
        return reviews

    def standardize_product(self, raw_item: Dict[str, Any]) -> Dict[str, Any]:
        now_iso = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        return {
            "product_id": str(raw_item.get("id")),
            "product_name": str(raw_item.get("name")),
            "brand": str(raw_item.get("brand", "SentinelTech")),
            "category": "Electronics",
            "subcategory": "Gadgets",
            "price": float(raw_item.get("price", 0)),
            "currency": "VND",
            "rating": float(raw_item.get("rating_average", 4.5)),
            "review_count": int(raw_item.get("review_count", 10)),
            "seller": str(raw_item.get("seller_name", "Official Store")),
            "product_url": f"https://{self.target_domain}/products/{raw_item.get('id')}",
            "image_url": f"https://{self.target_domain}/img/{raw_item.get('id')}.jpg",
            "source": "mock_crawler",
            "created_at": now_iso,
            "updated_at": now_iso
        }

    def standardize_review(self, raw_review: Dict[str, Any], product_id: Optional[str] = None) -> Dict[str, Any]:
        now_iso = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        return {
            "review_id": str(raw_review.get("id")),
            "product_id": product_id or str(raw_review.get("product_id")),
            "user_id": str(raw_review.get("customer_id")),
            "rating": float(raw_review.get("rating", 5.0)),
            "review_title": str(raw_review.get("title", "Review")),
            "review_text": str(raw_review.get("content", "Good product")),
            "review_date": now_iso,
            "verified_purchase": True,
            "helpful_count": int(raw_review.get("thank_count", 0)),
            "source": "mock_crawler",
            "language": "vi",
            "created_at": now_iso
        }
