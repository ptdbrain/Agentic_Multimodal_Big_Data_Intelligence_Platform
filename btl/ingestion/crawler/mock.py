import time
import random
from typing import Dict, Any, List, Optional
from ingestion.crawler.base import BaseEcommerceCrawler
from ingestion.privacy import anonymize_user_id

DEFAULT_MOCK_SELLERS = ["Official Flagship", "TechZone Store", "MobileHub", "ElectroWorld"]

class MockEcommerceCrawler(BaseEcommerceCrawler):
    """Mock E-Commerce Crawler for simulation, automated testing, and CI/CD environments."""

    def __init__(
        self,
        target_domain: str = "retail.sentinel.ai",
        sellers: Optional[List[str]] = None,
        warranty_options: Optional[List[int]] = None,
        max_shipping_days: int = 3,
        seed: Optional[int] = 42,
        **kwargs
    ):
        super().__init__(platform_name="MockSimulator", base_url=f"https://{target_domain}", **kwargs)
        self.target_domain = target_domain
        self.sellers = sellers or DEFAULT_MOCK_SELLERS
        self.warranty_options = warranty_options or [12, 24]
        self.max_shipping_days = max(1, max_shipping_days)
        self.rng = random.Random(seed)

    def scrape_product_page(self, product_id: str) -> Dict[str, Any]:
        """Extracts detailed product specs and live seller quote (backward-compatible method)."""
        return {
            "product_id": str(product_id),
            "scraped_url": f"https://{self.target_domain}/products/{product_id}",
            "scraped_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "seller": self.rng.choice(self.sellers),
            "stock_status": "in_stock",
            "warranty_months": self.rng.choice(self.warranty_options),
            "shipping_days": self.rng.randint(1, self.max_shipping_days)
        }

    def scrape_batch(self, product_ids: List[str]) -> List[Dict[str, Any]]:
        return [self.scrape_product_page(pid) for pid in product_ids]

    def search_products(self, query: str, limit: int = 20, page: int = 1) -> List[Dict[str, Any]]:
        results = []
        brands = ["Apple", "Samsung", "Xiaomi", "Sony", "Dell"]
        categories = ["Smartphone", "Laptop", "Tablet", "Accessories"]
        for i in range(limit):
            pid = f"mock_{query.lower().replace(' ', '_')}_{page}_{i+1}"
            results.append({
                "id": pid,
                "name": f"{query.title()} Model {i+1}",
                "brand": brands[i % len(brands)],
                "category": categories[i % len(categories)],
                "price": self.rng.randint(10000000, 30000000),
                "rating_average": round(self.rng.uniform(4.0, 5.0), 1),
                "review_count": self.rng.randint(50, 500),
                "seller_name": self.rng.choice(self.sellers)
            })
        return results

    def get_product_details(self, product_id: str) -> Optional[Dict[str, Any]]:
        return self.scrape_product_page(product_id)

    def get_product_reviews(self, product_id: str, limit: int = 50, page: int = 1) -> List[Dict[str, Any]]:
        reviews = []
        base_ts = 1790236800.0  # Fixed deterministic base timestamp (2026-09-24T08:00:00Z)
        for i in range(limit):
            reviews.append({
                "id": f"mock_rv_{product_id}_{i+1}",
                "product_id": product_id,
                "customer_id": f"raw_cust_{product_id}_{i+1}",
                "customer_name": f"Nguyen Van {i+1}",
                "customer_email": f"customer{i+1}@example.com",
                "rating": self.rng.choice([4.0, 5.0, 5.0, 3.0, 2.0]),
                "title": "Trải nghiệm sản phẩm thực tế",
                "content": "Sản phẩm dùng tốt, đóng gói kỹ, giao hàng đúng hẹn.",
                "created_at": base_ts - (i * 3600),
                "thank_count": self.rng.randint(0, 10)
            })
        return reviews

    def standardize_product(self, raw_item: Dict[str, Any]) -> Dict[str, Any]:
        now_iso = "2026-09-24T08:00:00Z"
        return {
            "product_id": str(raw_item.get("id")),
            "product_name": str(raw_item.get("name")),
            "brand": str(raw_item.get("brand", "SentinelTech")),
            "category": str(raw_item.get("category", "Electronics")),
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
        created_at_val = raw_review.get("created_at")
        if isinstance(created_at_val, (int, float)):
            now_iso = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(created_at_val))
        else:
            now_iso = str(created_at_val or "2026-09-24T08:00:00Z")
        raw_uid = raw_review.get("customer_id") or raw_review.get("user_id")
        return {
            "review_id": str(raw_review.get("id")),
            "product_id": product_id or str(raw_review.get("product_id")),
            "user_id": anonymize_user_id(raw_uid, remove=self.remove_user_id),
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
