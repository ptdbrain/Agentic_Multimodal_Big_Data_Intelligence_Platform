from __future__ import annotations

import datetime
import hashlib
import json
import logging
import math
import re
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple
from urllib.parse import parse_qsl, quote, urlencode, urljoin, urlsplit, urlunsplit
from urllib.robotparser import RobotFileParser

import requests
from bs4 import BeautifulSoup

from config.settings import settings
from ingestion.crawler.base import BaseEcommerceCrawler
from ingestion.crawler.validator import CrawlerDataValidator
from ingestion.privacy import anonymize_user_id, sanitize_record_pii

logger = logging.getLogger(__name__)

BOT_NAME = "SentinelAIResearchBot/0.1"
BASE_HOST = "tiki.vn"
PRODUCT_URL_RE = re.compile(r"-p(\d+)\.html(?:$|[?#])", re.IGNORECASE)

DEFAULT_SEEDS: Dict[str, str] = {
    "smartphones": "https://tiki.vn/dien-thoai-smartphone/c1795",
    "laptops": "https://tiki.vn/laptop/c8095",
    "bluetooth_headphones": "https://tiki.vn/tai-nghe-bluetooth/c1811",
}


def utc_now() -> str:
    """Return ISO-8601 formatted UTC timestamp string."""
    return datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def canonical_url(url: str) -> str:
    """Remove query parameters and fragments for stable deduplication."""
    parts = urlsplit(url)
    return urlunsplit((parts.scheme, parts.netloc, parts.path, "", ""))


def product_id_from_url(url: str) -> Optional[str]:
    """Extract canonical 'tiki_<id>' from Tiki product URL."""
    match = PRODUCT_URL_RE.search(urlsplit(url).path)
    return f"tiki_{match.group(1)}" if match else None


def walk_dicts(value: Any) -> Iterable[Dict[str, Any]]:
    """Recursively traverse arbitrary nested JSON objects and yield all dictionaries."""
    if isinstance(value, dict):
        yield value
        for child in value.values():
            yield from walk_dicts(child)
    elif isinstance(value, list):
        for child in value:
            yield from walk_dicts(child)


def normalize_type(value: Any) -> Set[str]:
    """Normalize Schema.org @type values to a lowercase set."""
    if isinstance(value, str):
        return {value.lower()}
    if isinstance(value, list):
        return {str(v).lower() for v in value}
    return set()


def pick(d: Optional[Dict[str, Any]], *keys: str) -> Any:
    """Case-insensitive dictionary lookup for first non-empty value among keys."""
    if not isinstance(d, dict):
        return None
    lowered = {str(k).lower(): v for k, v in d.items()}
    for key in keys:
        val = lowered.get(key.lower())
        if val not in (None, "", [], {}):
            return val
    return None


def parse_price(value: Any) -> Optional[float]:
    """Robust price parser handling int, float, dict, and formatted strings (e.g. 5.990.000 ₫)."""
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, (int, float)):
        if math.isnan(value) or math.isinf(value) or value < 0:
            return None
        return float(value)
    if isinstance(value, dict):
        return parse_price(pick(value, "price", "amount", "value", "lowPrice", "low_price"))
    if isinstance(value, list):
        for item in value:
            parsed = parse_price(item)
            if parsed is not None:
                return parsed
        return None
    text = str(value).strip()
    if not text:
        return None
    digits = re.sub(r"[^0-9]", "", text)
    if not digits:
        return None
    try:
        val = float(digits)
        return val if val >= 0 else None
    except ValueError:
        return None


def scalar_text(value: Any) -> Optional[str]:
    """Extract clean string from nested or scalar values."""
    if value is None:
        return None
    if isinstance(value, str):
        return value.strip() or None
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, dict):
        return scalar_text(pick(value, "name", "value", "label", "title"))
    if isinstance(value, list):
        for item in value:
            res = scalar_text(item)
            if res:
                return res
    return None


def find_json_product(soup: BeautifulSoup) -> Optional[Dict[str, Any]]:
    """Prefer official Schema.org Product JSON-LD; fall back to embedded Next.js/Nuxt blobs."""
    embedded: List[Any] = []
    for tag in soup.find_all("script"):
        text = tag.string or tag.get_text(strip=False)
        if not text or len(text) > 8_000_000:
            continue
        tag_type = (tag.get("type") or "").lower()
        tag_id = (tag.get("id") or "").lower()
        should_try = (
            "ld+json" in tag_type
            or "json" in tag_type
            or tag_id in {"__next_data__", "__nuxt_data__", "__initial_state__"}
        )
        if not should_try:
            continue
        try:
            embedded.append(json.loads(text))
        except (json.JSONDecodeError, TypeError):
            continue

    # 1. Official Schema.org Product node
    for blob in embedded:
        for obj in walk_dicts(blob):
            if "product" in normalize_type(obj.get("@type")):
                return obj

    # 2. Generic structured-data fallback
    name_keys = {"name", "product_name", "productname", "title", "displayname"}
    price_keys = {"price", "current_price", "sale_price", "saleprice", "final_price", "finalprice"}
    candidates: List[Tuple[int, Dict[str, Any]]] = []
    for blob in embedded:
        for obj in walk_dicts(blob):
            keys = {str(k).lower() for k in obj.keys()}
            if not (keys & name_keys) or not (keys & price_keys):
                continue
            score = 2
            if keys & {"product_id", "productid", "sku", "itemid", "master_id"}:
                score += 2
            if "brand" in keys or "brand_name" in keys:
                score += 1
            if keys & {"rating_average", "aggregaterating", "review_count", "reviewcount"}:
                score += 1
            candidates.append((score, obj))
    if candidates:
        return max(candidates, key=lambda item: item[0])[1]
    return None


def extract_page_fields(html: str, product_url: str, category_seed: str = "Electronics") -> Dict[str, Any]:
    """Extract metadata, pricing, rating, and seller specs from a Tiki product HTML page."""
    soup = BeautifulSoup(html, "html.parser")
    product = find_json_product(soup)

    h1 = soup.find("h1")
    og_title = soup.find("meta", attrs={"property": "og:title"})
    name = scalar_text(pick(product, "name", "product_name", "productName", "title", "displayName"))
    if not name and og_title:
        name = scalar_text(og_title.get("content"))
    if not name and h1:
        name = scalar_text(h1.get_text(" ", strip=True))

    offers = pick(product, "offers", "offer")
    price_value = pick(
        product,
        "price",
        "current_price",
        "currentPrice",
        "sale_price",
        "salePrice",
        "final_price",
        "finalPrice",
    )
    if price_value is None:
        price_value = pick(offers, "price", "lowPrice", "low_price", "amount") if isinstance(offers, dict) else offers
    currency = scalar_text(pick(product, "priceCurrency", "price_currency", "currency"))
    if isinstance(offers, dict) and not currency:
        currency = scalar_text(pick(offers, "priceCurrency", "price_currency", "currency"))

    if price_value is None:
        for selector in (
            'meta[property="product:price:amount"]',
            'meta[itemprop="price"]',
            '[itemprop="price"]',
        ):
            tag = soup.select_one(selector)
            if tag:
                price_value = tag.get("content") or tag.get_text(" ", strip=True)
                if price_value:
                    break

    price = parse_price(price_value)

    visible_text = soup.get_text(" ", strip=True)
    title_pos = visible_text.lower().find(name.lower()) if name else -1
    visible_region = visible_text[title_pos : title_pos + 1800] if title_pos >= 0 else visible_text[:1800]
    if price is None:
        price_match = re.search(
            r"(?<!\d)(\d{1,3}(?:[.,]\d{3})+|\d{5,})\s*(?:₫|đ|VNĐ|VND)",
            visible_region,
            re.IGNORECASE,
        )
        if not price_match:
            price_match = re.search(r"(?<!\d)(\d{1,3}(?:[.,]\d{3})+|\d{5,})(?!\d)", visible_region)
        if price_match:
            candidate_price = parse_price(price_match.group(1))
            if candidate_price is not None and 10_000 <= candidate_price <= 10_000_000_000:
                price = candidate_price

    brand = scalar_text(pick(product, "brand", "brand_name", "brandName", "manufacturer"))
    if not brand:
        brand_tag = soup.select_one('[itemprop="brand"]')
        if brand_tag:
            brand = scalar_text(brand_tag.get("content") or brand_tag.get_text(" ", strip=True))

    category = (
        scalar_text(pick(product, "category", "category_name", "categoryName", "primary_category_name"))
        or category_seed
    )
    seller = scalar_text(pick(product, "seller", "seller_name", "sellerName", "merchant"))
    image = scalar_text(pick(product, "image", "image_url", "imageUrl", "thumbnail_url"))

    aggregate = pick(product, "aggregateRating", "aggregate_rating", "rating_summary", "ratingSummary")
    rating = pick(product, "rating_average", "ratingAverage", "rating", "ratingValue")
    review_count = pick(product, "review_count", "reviewCount", "ratingCount", "rating_count", "num_reviews")
    if isinstance(aggregate, dict):
        rating = rating if rating is not None else pick(aggregate, "ratingValue", "rating_value", "rating")
        review_count = (
            review_count if review_count is not None else pick(aggregate, "reviewCount", "ratingCount", "review_count")
        )

    try:
        rating = float(rating) if rating not in (None, "") else None
    except (ValueError, TypeError):
        rating = None
    try:
        parsed_rc = parse_price(review_count)
        review_count = int(parsed_rc) if parsed_rc is not None else None
    except (ValueError, TypeError):
        review_count = None

    if rating is None or review_count is None:
        summary_match = re.search(r"(?<!\d)([0-5](?:[.,]\d)?)\s*\(\s*(\d[\d.,]*)\s*\)", visible_region)
        if summary_match:
            if rating is None:
                try:
                    rating = float(summary_match.group(1).replace(",", "."))
                except ValueError:
                    pass
            if review_count is None:
                parsed_count = parse_price(summary_match.group(2))
                review_count = int(parsed_count) if parsed_count is not None else None

    pid = product_id_from_url(product_url)
    clean_id = str(pid).replace("tiki_", "") if pid else ""

    return {
        "id": int(clean_id) if clean_id.isdigit() else clean_id,
        "product_id": pid,
        "product_name": name or "Unknown Tiki Product",
        "name": name or "Unknown Tiki Product",
        "brand": brand or "Generic",
        "brand_name": brand or "Generic",
        "category": category,
        "primary_category_name": category,
        "price": price if price is not None else 0.0,
        "currency": (currency or "VND").upper(),
        "rating": rating,
        "rating_average": rating,
        "review_count": review_count or 0,
        "seller": seller or "Tiki Trading",
        "seller_name": seller or "Tiki Trading",
        "image_url": image or "",
        "thumbnail_url": image or "",
        "product_url": product_url,
        "url_path": product_url.replace("https://tiki.vn/", ""),
        "source": "tiki_crawler",
        "category_seed": category_seed,
        "collected_at": utc_now(),
        "content_sha256": hashlib.sha256(html.encode("utf-8", errors="replace")).hexdigest(),
    }


class PoliteTikiCrawler:
    """Polite scraper for Tiki.vn complying with robots.txt and strict rate-limiting (delay >= 2.0s).
    Supports catalog scraping and genuine customer review retrieval without simulating data.
    """

    def __init__(self, delay_seconds: float = 2.0, timeout_seconds: int = 20):
        self.delay_seconds = max(2.0, delay_seconds)
        self.timeout_seconds = timeout_seconds
        self.last_request_at = 0.0
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": BOT_NAME,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "vi-VN,vi;q=0.9,en;q=0.6",
        })
        self.robots: Dict[str, RobotFileParser] = {}

    def _check_robots(self, url: str) -> None:
        parts = urlsplit(url)
        origin = f"{parts.scheme}://{parts.netloc}"
        parser = self.robots.get(origin)
        if parser is None:
            robots_url = f"{origin}/robots.txt"
            try:
                response = self.session.get(robots_url, timeout=self.timeout_seconds)
                if response.status_code == 200:
                    parser = RobotFileParser(robots_url)
                    parser.parse(response.text.splitlines())
                    self.robots[origin] = parser
                else:
                    logger.warning(f"Could not fetch robots.txt ({response.status_code}) at {robots_url}")
            except Exception as e:
                logger.warning(f"Robots check error: {e}")
        if parser and not parser.can_fetch(BOT_NAME, url):
            raise PermissionError(f"robots.txt disallows crawler access to URL: {url}")

    def fetch_html(self, url: str) -> str:
        self._check_robots(url)
        elapsed = time.monotonic() - self.last_request_at
        if elapsed < self.delay_seconds:
            time.sleep(self.delay_seconds - elapsed)

        response = self.session.get(url, timeout=self.timeout_seconds)
        self.last_request_at = time.monotonic()
        if response.status_code in (403, 429):
            raise RuntimeError(f"Tiki returned HTTP {response.status_code} for {url}. Crawl stopped.")
        if response.status_code >= 500:
            raise RuntimeError(f"Tiki returned HTTP {response.status_code} for {url}.")
        response.raise_for_status()
        response.encoding = response.apparent_encoding or response.encoding
        return response.text

    def discover_product_links(self, listing_url: str, max_products: int = 10) -> List[str]:
        html = self.fetch_html(listing_url)
        soup = BeautifulSoup(html, "html.parser")
        links: Dict[str, None] = {}
        for anchor in soup.select("a[href]"):
            href = anchor.get("href")
            if not href:
                continue
            full = urljoin(listing_url, href)
            parts = urlsplit(full)
            if parts.netloc and not (parts.netloc == BASE_HOST or parts.netloc.endswith("." + BASE_HOST)):
                continue
            if PRODUCT_URL_RE.search(parts.path):
                clean = canonical_url(full)
                links.setdefault(clean, None)
        return list(links.keys())[:max_products]

    def crawl_category_pages(
        self, category_seed: str, listing_url: str, max_products: int
    ) -> Tuple[List[Dict[str, Any]], List[Dict[str, str]]]:
        links = self.discover_product_links(listing_url, max_products=max_products)
        records: List[Dict[str, Any]] = []
        unparsed: List[Dict[str, str]] = []
        for url in links:
            try:
                html = self.fetch_html(url)
                record = extract_page_fields(html, url, category_seed)
                if not record.get("product_name") or record.get("price") is None:
                    unparsed.append({
                        "product_url": url,
                        "category_seed": category_seed,
                        "reason": "missing_product_name_or_price",
                        "collected_at": utc_now(),
                    })
                    continue
                records.append(record)
            except Exception as exc:
                unparsed.append({
                    "product_url": url,
                    "category_seed": category_seed,
                    "reason": str(exc),
                    "collected_at": utc_now(),
                })
        return records, unparsed

    def fetch_product_reviews(
        self, product_id: str | int, limit: int = 5, page: int = 1
    ) -> List[Dict[str, Any]]:
        """Politely query genuine customer reviews from Tiki's public review endpoint.
        Respects robots.txt and rate limits (delay >= 2.0s).
        NEVER invents or simulates mock customer feedback.
        """
        clean_id = str(product_id).replace("tiki_", "").strip()
        if not clean_id or not clean_id.isdigit():
            return []

        url = f"https://{BASE_HOST}/api/v2/reviews"
        params = {
            "product_id": clean_id,
            "limit": limit,
            "page": page,
            "sort": "score|desc",
        }
        full_url = f"{url}?{urlencode(params)}"
        self._check_robots(full_url)

        elapsed = time.monotonic() - self.last_request_at
        if elapsed < self.delay_seconds:
            time.sleep(self.delay_seconds - elapsed)

        headers = {
            "User-Agent": BOT_NAME,
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7",
        }
        try:
            resp = self.session.get(url, params=params, headers=headers, timeout=self.timeout_seconds)
            self.last_request_at = time.monotonic()
            if resp.status_code == 200:
                data = resp.json()
                items = data.get("data", [])
                if isinstance(items, list):
                    return items
            elif resp.status_code in (403, 429):
                logger.warning(f"Tiki review endpoint returned HTTP {resp.status_code} for product {clean_id}")
            else:
                logger.info(f"Tiki review query returned HTTP {resp.status_code} for product {clean_id}")
        except Exception as exc:
            logger.warning(f"Error fetching genuine reviews for product {clean_id}: {exc}")
        return []


class TikiCrawler(BaseEcommerceCrawler):
    """Production-ready real-time crawler for Tiki.vn e-commerce marketplace.
    Extracts live catalog specs, pricing, and genuine customer feedback.

    Strictly forbids synthetic mock data fallback in real crawl mode:
    when live queries fail or are blocked, raises or returns real results only.
    """

    def __init__(
        self,
        base_url: Optional[str] = None,
        products_endpoint: Optional[str] = None,
        reviews_endpoint: Optional[str] = None,
        allow_public_fallback: bool = False,
        use_polite_scraper: bool = True,
        delay_seconds: float = 2.0,
        **kwargs,
    ):
        b_url = base_url or settings.crawler.tiki_base_url
        super().__init__(platform_name="Tiki", base_url=b_url, **kwargs)
        self.products_endpoint = products_endpoint or settings.crawler.tiki_products_endpoint
        self.reviews_endpoint = reviews_endpoint or settings.crawler.tiki_reviews_endpoint
        self.allow_public_fallback = allow_public_fallback
        self.use_polite_scraper = use_polite_scraper
        self.polite_crawler = PoliteTikiCrawler(delay_seconds=delay_seconds)

    def search_products(self, query: str, limit: int = 20, page: int = 1) -> List[Dict[str, Any]]:
        """Search products on Tiki by keyword or category.
        Uses live API endpoint if available, with polite HTML scraping fallback.
        In real crawl mode (allow_public_fallback=False), NEVER injects synthetic mock products.
        """
        # 1. Try public JSON API
        url = f"{self.base_url}{self.products_endpoint}"
        params = {"q": query, "limit": limit, "page": page}
        resp = self._request_with_retry("GET", url, params=params)
        if resp and resp.status_code == 200:
            try:
                data = resp.json()
                items = data.get("data", [])
                if items:
                    return items
            except Exception as e:
                logger.warning(f"[Tiki] JSON decode error: {e}")

        # 2. Try polite HTML scraping of category seed or search page
        if self.use_polite_scraper:
            target_url = None
            clean_query = query.strip().lower()
            if clean_query in DEFAULT_SEEDS:
                target_url = DEFAULT_SEEDS[clean_query]
            else:
                target_url = f"https://tiki.vn/search?q={quote(query)}"

            try:
                scraped, _ = self.polite_crawler.crawl_category_pages(
                    category_seed=clean_query,
                    listing_url=target_url,
                    max_products=limit,
                )
                if scraped:
                    return scraped
            except Exception as exc:
                logger.warning(f"[Tiki] Polite HTML scraping failed for '{query}': {exc}")

        # 3. Guard against hardcoded fallback
        if not self.allow_public_fallback:
            logger.info(f"[Tiki] Real crawl completed with 0 items for '{query}' (synthetic fallback strictly disabled).")
            return []

        # Deprecated legacy mock fallback (strictly guarded by allow_public_fallback=True)
        logger.warning(
            "[Tiki] WARNING: Returning deprecated synthetic mock products catalog because allow_public_fallback=True."
        )
        base_catalog = [
            {
                "id": 271972435,
                "name": "Apple iPhone 15 Pro Max 256GB Titan Tự Nhiên - Hàng Chính Hãng VN/A",
                "brand_name": "Apple",
                "primary_category_name": "Smartphone",
                "primary_category_path": "Điện Thoại - Máy Tính Bảng/Điện thoại Smartphone",
                "price": 29490000,
                "rating_average": 4.9,
                "review_count": 1420,
                "seller_name": "Tiki Trading",
                "url_path": "apple-iphone-15-pro-max-256gb-p271972435.html",
                "thumbnail_url": "https://salt.tikicdn.com/cache/280x280/ts/product/iphone15promax.jpg",
            },
            {
                "id": 274018291,
                "name": "Điện Thoại Samsung Galaxy S24 Ultra 5G (12GB/256GB) - Hàng Chính Hãng",
                "brand_name": "Samsung",
                "primary_category_name": "Smartphone",
                "primary_category_path": "Điện Thoại - Máy Tính Bảng/Điện thoại Smartphone",
                "price": 27990000,
                "rating_average": 4.8,
                "review_count": 865,
                "seller_name": "Samsung Flagship Store",
                "url_path": "dien-thoai-samsung-galaxy-s24-ultra-p274018291.html",
                "thumbnail_url": "https://salt.tikicdn.com/cache/280x280/ts/product/s24ultra.jpg",
            },
            {
                "id": 273881004,
                "name": "MacBook Air M3 13 inch 2024 (8GB/256GB) - Hàng Chính Hãng Apple Việt Nam",
                "brand_name": "Apple",
                "primary_category_name": "Laptop",
                "primary_category_path": "Laptop - Máy Vi Tính/Laptop",
                "price": 26890000,
                "rating_average": 4.9,
                "review_count": 512,
                "seller_name": "Tiki Trading",
                "url_path": "macbook-air-m3-13-inch-2024-p273881004.html",
                "thumbnail_url": "https://salt.tikicdn.com/cache/280x280/ts/product/macbookairm3.jpg",
            },
            {
                "id": 184059211,
                "name": "Tai Nghe Chụp Tai Chống Ồn Sony WH-1000XM5 - Hàng Chính Hãng",
                "brand_name": "Sony",
                "primary_category_name": "Accessories",
                "primary_category_path": "Thiết Bị Số - Phụ Kiện Số/Tai Nghe",
                "price": 7490000,
                "rating_average": 4.8,
                "review_count": 640,
                "seller_name": "Sony Official Store",
                "url_path": "tai-nghe-sony-wh-1000xm5-p184059211.html",
                "thumbnail_url": "https://salt.tikicdn.com/cache/280x280/ts/product/wh1000xm5.jpg",
            },
            {
                "id": 275112098,
                "name": "Máy Tính Bảng Xiaomi Pad 6 (8GB/256GB) - Hàng Chính Hãng",
                "brand_name": "Xiaomi",
                "primary_category_name": "Tablet",
                "primary_category_path": "Điện Thoại - Máy Tính Bảng/Máy tính bảng",
                "price": 8990000,
                "rating_average": 4.7,
                "review_count": 320,
                "seller_name": "Xiaomi Official Store",
                "url_path": "may-tinh-bang-xiaomi-pad-6-p275112098.html",
                "thumbnail_url": "https://salt.tikicdn.com/cache/280x280/ts/product/xiaomipad6.jpg",
            },
        ]
        results = []
        for idx in range(limit):
            template = dict(base_catalog[idx % len(base_catalog)])
            if idx >= len(base_catalog):
                template["id"] = int(template["id"]) + idx
                template["name"] = f"{template['name']} (Edition {idx + 1})"
            results.append(template)
        return results

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
        """Fetch genuine customer feedback and ratings from Tiki reviews API.
        In real crawl mode (allow_public_fallback=False), NEVER injects synthetic mock feedback.
        """
        clean_id = str(product_id).replace("tiki_", "")

        # 1. Fetch genuine reviews via polite scraper
        real_reviews = self.polite_crawler.fetch_product_reviews(clean_id, limit=limit, page=page)
        if real_reviews:
            return real_reviews

        # 2. Secondary session query if polite scraper returned empty
        url = f"{self.base_url}{self.reviews_endpoint}"
        params = {"product_id": clean_id, "limit": limit, "page": page, "sort": "score|desc"}
        resp = self._request_with_retry("GET", url, params=params)
        if resp and resp.status_code == 200:
            try:
                data = resp.json()
                items = data.get("data", [])
                if items:
                    return items
            except Exception as e:
                logger.warning(f"[Tiki] JSON decode error on reviews: {e}")

        # 3. Guard against hardcoded fallback
        if not self.allow_public_fallback:
            return []

        logger.warning(
            "[Tiki] WARNING: Returning deprecated synthetic mock reviews because allow_public_fallback=True."
        )
        sample_feedback = [
            (5, "Cực kỳ hài lòng", "Hàng chính hãng Tiki Trading, đóng gói niêm phong kỹ, giao nhanh trong 2h."),
            (5, "Chất lượng tuyệt vời", "Máy chạy rất mượt, màn hình sắc nét, pin dùng thoải mái cả ngày."),
            (4, "Tốt trong tầm giá", "Sản phẩm đẹp, hiệu năng ổn định nhưng hộp hơi móp nhẹ khi vận chuyển."),
            (5, "Đáng đồng tiền", "Đã kiểm tra bảo hành chính hãng, rất yên tâm khi mua tại gian hàng."),
            (3, "Tạm ổn", "Máy dùng ổn nhưng nóng nhẹ khi cập nhật phần mềm lần đầu."),
        ]
        base_ts = 1790236800  # 2026-09-24T08:00:00Z
        fallback_reviews = []
        for i in range(limit):
            rating, title, content = sample_feedback[i % len(sample_feedback)]
            fallback_reviews.append({
                "id": f"{clean_id}_{page}_{i+1:04d}",
                "product_id": clean_id,
                "customer_id": f"tiki_raw_cust_{clean_id}_{i+1}",
                "created_by": {
                    "id": f"tiki_raw_cust_{clean_id}_{i+1}",
                    "name": f"Khach Hang {i+1}",
                    "full_name": f"Nguyen Thi Khach {i+1}",
                },
                "rating": rating,
                "title": title,
                "content": content,
                "created_at": base_ts - (i * 3600),
                "thank_count": (i * 3) % 15,
            })
        return fallback_reviews

    def standardize_product(self, raw_item: Dict[str, Any]) -> Dict[str, Any]:
        """Maps Tiki raw item (from API or HTML extraction) to SentinelAI canonical Product schema (raw.products)."""
        raw_id = raw_item.get("id") or raw_item.get("product_id")
        clean_id = str(raw_id).replace("tiki_", "") if raw_id else "unknown"
        p_id = f"tiki_{clean_id}"
        now_iso = utc_now()

        url_path = raw_item.get("url_path", "")
        if raw_item.get("product_url"):
            prod_url = raw_item["product_url"]
        elif url_path:
            prod_url = f"https://tiki.vn/{url_path}"
        else:
            prod_url = f"https://tiki.vn/p/{clean_id}.html"

        p_name = str(raw_item.get("name") or raw_item.get("product_name") or "Unknown Product").strip()
        p_brand = str(raw_item.get("brand") or raw_item.get("brand_name") or "Generic").strip()
        p_cat = str(raw_item.get("category") or raw_item.get("primary_category_name") or "Electronics").strip()
        p_subcat = str(raw_item.get("subcategory") or raw_item.get("primary_category_path") or p_cat).split("/")[-1].strip()

        price_val = float(raw_item.get("price") or raw_item.get("original_price") or 0.0)
        rating_val = raw_item.get("rating") if raw_item.get("rating") is not None else raw_item.get("rating_average")
        rc_val = raw_item.get("review_count")

        return {
            "product_id": p_id,
            "product_name": p_name,
            "brand": p_brand,
            "category": p_cat,
            "subcategory": p_subcat,
            "price": max(0.0, price_val),
            "currency": "VND",
            "rating": round(float(rating_val), 1) if rating_val is not None else 0.0,
            "review_count": int(rc_val) if rc_val is not None else 0,
            "seller": str(raw_item.get("seller") or raw_item.get("seller_name") or "Tiki Trading").strip(),
            "product_url": prod_url,
            "image_url": str(raw_item.get("image_url") or raw_item.get("thumbnail_url") or ""),
            "source": "tiki_crawler",
            "created_at": raw_item.get("collected_at") or now_iso,
            "updated_at": raw_item.get("collected_at") or now_iso,
        }

    def standardize_price(self, std_product: Dict[str, Any], index: int = 1) -> Dict[str, Any]:
        """Maps product details into SentinelAI canonical PriceHistory schema (raw.prices)."""
        pid = std_product["product_id"]
        ts = std_product.get("updated_at") or utc_now()
        clean_pid = pid.replace("tiki_", "")
        return {
            "price_id": f"pr_tiki_{clean_pid}_{index:04d}",
            "product_id": pid,
            "price": float(std_product.get("price", 0.0)),
            "currency": str(std_product.get("currency", "VND")),
            "seller": str(std_product.get("seller", "Tiki Trading")),
            "timestamp": ts,
            "source": "tiki_crawler",
        }

    def standardize_review(self, raw_review: Dict[str, Any], product_id: Optional[str] = None) -> Dict[str, Any]:
        """Maps genuine Tiki raw review JSON to SentinelAI canonical Review schema (raw.reviews).
        Hashes customer_id / created_by.id immediately at ingest for PII protection.
        Guarantees non-empty review_text and valid rating within [1.0, 5.0].
        """
        rev_id = raw_review.get("id")
        tiki_p_id = raw_review.get("product_id")
        clean_p_id = str(product_id or tiki_p_id or "unknown_product").replace("tiki_", "")
        resolved_pid = f"tiki_{clean_p_id}"

        created_at_val = raw_review.get("created_at")
        if isinstance(created_at_val, (int, float)):
            rev_time = (
                datetime.datetime.fromtimestamp(created_at_val, tz=datetime.timezone.utc)
                .isoformat()
                .replace("+00:00", "Z")
            )
        elif isinstance(created_at_val, str) and created_at_val:
            rev_time = created_at_val
        else:
            rev_time = utc_now()

        title = str(raw_review.get("title") or "Đánh giá từ khách hàng").strip()
        raw_content = str(raw_review.get("content") or "").strip()
        # Ensure review_text is non-empty to comply with schema minLength=1
        if raw_content:
            text = raw_content
        elif title and title != "Đánh giá từ khách hàng":
            text = title
        else:
            r_val = raw_review.get("rating", 5)
            text = f"Đánh giá {r_val} sao từ khách hàng Tiki."

        created_by = raw_review.get("created_by") if isinstance(raw_review.get("created_by"), dict) else {}
        raw_uid = raw_review.get("customer_id") or created_by.get("id") or raw_review.get("user_id")

        raw_rating = float(raw_review.get("rating") or 5.0)
        clamped_rating = max(1.0, min(5.0, raw_rating))

        verified = bool(
            raw_review.get("purchased")
            or raw_review.get("status") == "approved"
            or raw_review.get("verified_purchase", True)
        )
        helpful = int(raw_review.get("thank_count") or raw_review.get("helpful_count") or 0)

        return {
            "review_id": f"tiki_rv_{rev_id}" if rev_id else f"tiki_rv_{int(datetime.datetime.now().timestamp() * 1000)}",
            "product_id": resolved_pid,
            "user_id": anonymize_user_id(raw_uid, remove=self.remove_user_id),
            "rating": clamped_rating,
            "review_title": title,
            "review_text": text,
            "review_date": rev_time,
            "verified_purchase": verified,
            "helpful_count": max(0, helpful),
            "source": "tiki_crawler",
            "language": "vi",
            "created_at": rev_time,
        }

    def crawl_category(
        self,
        query: str,
        max_products: int = 10,
        reviews_per_product: int = 5,
        enforce_pre_spark_validation: bool = True,
    ) -> Dict[str, Any]:
        """Crawl products, price observations, and genuine customer reviews for a query or category.
        Standardizes products to raw.products, prices to raw.prices, and genuine reviews to raw.reviews.
        Validates data contracts before data moves toward Spark ETL.
        """
        raw_products = self.search_products(query, limit=max_products, page=1)

        standard_products: List[Dict[str, Any]] = []
        all_reviews: List[Dict[str, Any]] = []
        all_prices: List[Dict[str, Any]] = []

        for idx, p in enumerate(raw_products[:max_products], start=1):
            std_p = sanitize_record_pii(
                self.standardize_product(p),
                remove_user_id=self.remove_user_id,
            )
            standard_products.append(std_p)
            std_price = self.standardize_price(std_p, index=idx)
            all_prices.append(std_price)

            orig_id = str(p.get("id") or std_p["product_id"])
            if reviews_per_product > 0:
                raw_revs = self.get_product_reviews(orig_id, limit=reviews_per_product, page=1)
                for r in raw_revs:
                    std_rev = sanitize_record_pii(
                        self.standardize_review(r, product_id=std_p["product_id"]),
                        remove_user_id=self.remove_user_id,
                    )
                    all_reviews.append(std_rev)

        # Pre-Spark contract validation
        validation_report = None
        if enforce_pre_spark_validation and standard_products:
            validation_report = CrawlerDataValidator.validate_crawled_dataset(
                products=standard_products,
                prices=all_prices,
                reviews=all_reviews,
                enforce_tiki_id_format=True,
            )

        return {
            "platform": self.platform_name,
            "query": query,
            "products_count": len(standard_products),
            "reviews_count": len(all_reviews),
            "prices_count": len(all_prices),
            "products": standard_products,
            "reviews": all_reviews,
            "prices": all_prices,
            "validation_report": validation_report,
            "crawled_at": utc_now(),
        }
