import re
import json
import datetime
import hashlib
from typing import List, Dict, Any, Optional
from urllib.parse import urlparse
from ingestion.crawler.base import BaseEcommerceCrawler
from ingestion.privacy import anonymize_user_id, sanitize_record_pii

class UniversalProductScraper(BaseEcommerceCrawler):
    """Universal e-commerce web scraper extracting standard Schema.org JSON-LD
    and OpenGraph metadata from any e-commerce URL or HTML payload.
    """

    def __init__(self, **kwargs):
        super().__init__(platform_name="Universal", base_url="https://generic.store", **kwargs)

    def scrape_url(self, target_url: str) -> Optional[Dict[str, Any]]:
        """Scrapes a live e-commerce product webpage and extracts structured metadata."""
        resp = self._request_with_retry("GET", target_url)
        if not resp or resp.status_code != 200:
            return None

        html_text = resp.text
        return self._extract_metadata_from_html(html_text, target_url)

    def _extract_metadata_from_html(self, html: str, url: str) -> Dict[str, Any]:
        """Extracts JSON-LD Schema.org Product markup or OpenGraph tags."""
        now_iso = datetime.datetime.now(datetime.timezone.utc).isoformat().replace("+00:00", "Z")
        domain = urlparse(url).netloc

        # 1. Try to extract Schema.org JSON-LD
        json_ld_matches = re.findall(r'<script[^>]*type=["\']application/ld\+json["\'][^>]*>(.*?)</script>', html, re.DOTALL | re.IGNORECASE)
        for block in json_ld_matches:
            try:
                data = json.loads(block.strip())
                items = data if isinstance(data, list) else [data]
                for item in items:
                    if item.get("@type") in ["Product", "IndividualProduct"]:
                        offers = item.get("offers", {})
                        if isinstance(offers, list):
                            offers = offers[0] if offers else {}
                        agg_rating = item.get("aggregateRating", {})

                        raw_price = offers.get("price") or 0.0
                        return sanitize_record_pii({
                            "product_id": str(item.get("sku") or f"web_{hashlib.sha256(url.encode()).hexdigest()[:12]}"),
                            "product_name": str(item.get("name") or "Web Product").strip(),
                            "brand": str(item.get("brand", {}).get("name") if isinstance(item.get("brand"), dict) else item.get("brand") or domain).strip(),
                            "category": str(item.get("category") or "Retail").strip(),
                            "subcategory": "E-Commerce",
                            "price": float(raw_price),
                            "currency": str(offers.get("priceCurrency") or "VND"),
                            "rating": float(agg_rating.get("ratingValue") or 5.0),
                            "review_count": int(agg_rating.get("reviewCount") or 0),
                            "seller": str(offers.get("seller", {}).get("name") if isinstance(offers.get("seller"), dict) else domain),
                            "product_url": url,
                            "image_url": str(item.get("image") or ""),
                            "source": "web_crawler",
                            "created_at": now_iso,
                            "updated_at": now_iso
                        }, remove_user_id=self.remove_user_id)
            except Exception:
                continue

        # 2. OpenGraph Tag Fallback
        def get_meta(prop: str) -> str:
            match = re.search(rf'<meta[^>]*property=["\']{prop}["\'][^>]*content=["\'](.*?)["\']', html, re.IGNORECASE)
            if not match:
                match = re.search(rf'<meta[^>]*content=["\'](.*?)["\'][^>]*property=["\']{prop}["\']', html, re.IGNORECASE)
            return match.group(1).strip() if match else ""

        title = get_meta("og:title") or get_meta("twitter:title")
        if not title:
            t_match = re.search(r'<title>(.*?)</title>', html, re.IGNORECASE)
            title = t_match.group(1).strip() if t_match else "Scraped Product"

        price_str = get_meta("product:price:amount") or get_meta("og:price:amount") or "0"
        currency = get_meta("product:price:currency") or get_meta("og:price:currency") or "VND"
        image = get_meta("og:image") or ""

        try:
            price = float(price_str)
        except ValueError:
            price = 0.0

        return sanitize_record_pii({
            "product_id": f"web_{hashlib.sha256(url.encode()).hexdigest()[:12]}",
            "product_name": title,
            "brand": domain.replace("www.", "").split(".")[0].title(),
            "category": "Electronics",
            "subcategory": "Online Retail",
            "price": price,
            "currency": currency,
            "rating": 4.5,
            "review_count": 10,
            "seller": domain,
            "product_url": url,
            "image_url": image,
            "source": "web_crawler",
            "created_at": now_iso,
            "updated_at": now_iso
        }, remove_user_id=self.remove_user_id)

    def extract_reviews_from_html(self, html: str, product_id: str) -> List[Dict[str, Any]]:
        """Extracts Schema.org Review objects embedded in JSON-LD script blocks and hashes author PII."""
        reviews: List[Dict[str, Any]] = []
        now_iso = datetime.datetime.now(datetime.timezone.utc).isoformat().replace("+00:00", "Z")
        json_ld_matches = re.findall(
            r'<script[^>]*type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
            html,
            re.DOTALL | re.IGNORECASE,
        )
        for block in json_ld_matches:
            try:
                data = json.loads(block.strip())
                items = data if isinstance(data, list) else [data]
                for item in items:
                    raw_revs = item.get("review", [])
                    if isinstance(raw_revs, dict):
                        raw_revs = [raw_revs]
                    for idx, rv in enumerate(raw_revs):
                        author = rv.get("author", {})
                        raw_author = author.get("name") if isinstance(author, dict) else author
                        rating_obj = rv.get("reviewRating", {})
                        raw_rating = rating_obj.get("ratingValue") if isinstance(rating_obj, dict) else rv.get("rating", 5.0)
                        rev_date = str(rv.get("datePublished") or now_iso)
                        rev_id = str(rv.get("@id") or rv.get("review_id") or f"web_rv_{product_id}_{idx+1:04d}")
                        reviews.append(sanitize_record_pii({
                            "review_id": rev_id,
                            "product_id": product_id,
                            "user_id": anonymize_user_id(raw_author, remove=self.remove_user_id),
                            "rating": float(raw_rating) if raw_rating is not None else 5.0,
                            "review_title": str(rv.get("name") or "Web Review"),
                            "review_text": str(rv.get("reviewBody") or rv.get("description") or ""),
                            "review_date": rev_date,
                            "verified_purchase": bool(rv.get("verified_purchase", True)),
                            "helpful_count": int(rv.get("helpful_count", 0)),
                            "source": "web_crawler",
                            "language": str(rv.get("inLanguage", "vi"))[:2].lower(),
                            "created_at": rev_date,
                        }, remove_user_id=self.remove_user_id))
            except Exception:
                continue
        return reviews

    def search_products(self, query: str, limit: int = 20, page: int = 1) -> List[Dict[str, Any]]:
        return []

    def get_product_details(self, product_id: str) -> Optional[Dict[str, Any]]:
        return None

    def get_product_reviews(self, product_id: str, limit: int = 50, page: int = 1) -> List[Dict[str, Any]]:
        return []

    def standardize_product(self, raw_item: Dict[str, Any]) -> Dict[str, Any]:
        return sanitize_record_pii(raw_item, remove_user_id=self.remove_user_id)

    def standardize_review(self, raw_review: Dict[str, Any], product_id: Optional[str] = None) -> Dict[str, Any]:
        rec = dict(raw_review)
        if product_id and not rec.get("product_id"):
            rec["product_id"] = product_id
        return sanitize_record_pii(rec, remove_user_id=self.remove_user_id)
