import time
import random
from typing import Dict, Any, List, Optional

DEFAULT_SELLERS = ["Official Flagship", "TechZone Store", "MobileHub", "ElectroWorld"]

class WebCrawler:
    """Mock Web Scraper capturing e-commerce product specs and real-time retail quotes.
    Configurable seller pool, warranty durations, and domain targets.
    """
    
    def __init__(
        self,
        target_domain: str = "retail.sentinel.ai",
        sellers: Optional[List[str]] = None,
        warranty_options: Optional[List[int]] = None,
        max_shipping_days: int = 3
    ):
        self.target_domain = target_domain
        self.sellers = sellers or DEFAULT_SELLERS
        self.warranty_options = warranty_options or [12, 24]
        self.max_shipping_days = max(1, max_shipping_days)

    def scrape_product_page(self, product_id: str) -> Dict[str, Any]:
        """Extracts detailed product specs and live seller quote."""
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

