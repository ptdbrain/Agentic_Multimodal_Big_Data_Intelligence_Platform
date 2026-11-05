from __future__ import annotations
from ingestion.identity import make_price_id

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
    from spark.etl.normalizer import EntityNormalizer, PRICE_TEXT_PATTERN
    if not re.fullmatch(PRICE_TEXT_PATTERN, text):
        return None
    try:
        val = EntityNormalizer.normalize_price(text)
        return val if math.isfinite(val) and val >= 0 else None
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


def observed_currency(raw_currency, raw_price):
    """Read a declared unit or explicit price token; never assume VND."""
    from spark.etl.normalizer import EntityNormalizer
    declared = EntityNormalizer.normalize_currency(raw_currency)
    tokens = set()
    if isinstance(raw_price,str):
        for token in re.findall(r"(?i)(?<![A-Za-z])(?:VND|VNĐ|USD|₫|đ|[$])(?![A-Za-z])",raw_price):
            tokens.add("USD" if token.upper() == "USD" or token == "$" else "VND")
    if len(tokens) > 1 or (declared and tokens and declared not in tokens):
        return None
    return declared or next(iter(tokens),None)


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
    if price is None and price_value is None:
        price_match = re.search(
            r"(?<![\d+-])([+-]?(?:\d{1,3}(?:[.,]\d{3})+|\d{5,}))\s*(?:₫|đ|VNĐ|VND)",
            visible_region,
            re.IGNORECASE,
        )
        if price_match:
            candidate_price = parse_price(price_match.group(1))
            if candidate_price is not None and 10_000 <= candidate_price <= 10_000_000_000:
                price = candidate_price
                price_value = price_match.group(0)

    brand = scalar_text(pick(product, "brand", "brand_name", "brandName", "manufacturer"))
    if not brand:
        brand_tag = soup.select_one('[itemprop="brand"]')
        if brand_tag:
            brand = scalar_text(brand_tag.get("content") or brand_tag.get_text(" ", strip=True))

    category = scalar_text(pick(product, "category", "category_name", "categoryName", "primary_category_name"))
    if not category:
        # Breadcrumb category links are observed page facts; the query seed
        # is never used as a substitute for a missing product category.
        categories = []
        for script in soup.select('script[type="application/ld+json"]'):
            try:
                document = json.loads(script.get_text())
            except (ValueError,TypeError):
                continue
            for node in walk_dicts(document):
                if "breadcrumblist" not in normalize_type(node.get("@type")):
                    continue
                entries = node.get("itemListElement",[])
                if not isinstance(entries,list):
                    continue
                for entry in entries:
                    if not isinstance(entry,dict):
                        continue
                    item = entry.get("item")
                    href = item.get("@id") or item.get("url") if isinstance(item,dict) else item
                    label = entry.get("name") or (item.get("name") if isinstance(item,dict) else None)
                    if not isinstance(href,str):
                        continue
                    parts = urlsplit(urljoin("https://tiki.vn/",href))
                    if parts.hostname == BASE_HOST and re.search(r"/c\d+/?$",parts.path) and scalar_text(label):
                        categories.append(scalar_text(label))
        category = categories[-1] if categories else None
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
        review_count = int(parsed_rc) if parsed_rc is not None and parsed_rc.is_integer() else None
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
        "product_name": name,
        "name": name,
        "brand": brand,
        "brand_name": brand,
        "category": category,
        "primary_category_name": category,
        "price": price,
        "price_raw": price_value,
        "currency": observed_currency(currency, price_value),
        "rating": rating,
        "rating_average": rating,
        "review_count": review_count,
        "seller": seller,
        "seller_name": seller,
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

    def __init__(self, delay_seconds: float = 2.0, timeout_seconds: int = 20, evidence=None):
        from ingestion.evidence import ResponseEvidence
        self.evidence = evidence or ResponseEvidence("tiki", "polite-tiki-html-api-v2")
        self._last_source_provenance = {}
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
        self._last_source_provenance = self.evidence.stamp([{}],response,url)[0]['_source_provenance']
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
                record['_source_provenance'] = self._last_source_provenance.copy()
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
                    return self.evidence.stamp(items,resp,full_url)
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

    PARSER_VERSION = 'tiki-api-html-v5'

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
        if allow_public_fallback:
            raise ValueError("Synthetic fallback removed: use MockEcommerceCrawler explicitly")
        b_url = base_url or settings.crawler.tiki_base_url
        super().__init__(platform_name="Tiki", base_url=b_url, **kwargs)
        self.products_endpoint = products_endpoint or settings.crawler.tiki_products_endpoint
        self.reviews_endpoint = reviews_endpoint or settings.crawler.tiki_reviews_endpoint
        self.allow_public_fallback = allow_public_fallback
        self.use_polite_scraper = use_polite_scraper
        self.polite_crawler = PoliteTikiCrawler(delay_seconds=delay_seconds, evidence=self.evidence)

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
                    observed = self.evidence.stamp(items,resp,url)
                    if self.use_polite_scraper:
                        for index,item in enumerate(observed):
                            if observed_currency(item.get('currency'),item.get('price')) is not None:
                                continue
                            path = item.get('product_url') or item.get('url_path')
                            if not path:
                                continue
                            product_url = urljoin(self.base_url + '/',path)
                            parts = urlsplit(product_url)
                            if parts.hostname != BASE_HOST or not PRODUCT_URL_RE.search(parts.path):
                                continue
                            try:
                                html = self.polite_crawler.fetch_html(product_url)
                                offer = extract_page_fields(html,product_url,query)
                                if offer.get('currency') and offer.get('price') is not None:
                                    # Price and unit share the same HTML evidence.
                                    offer['_source_provenance'] = self.polite_crawler._last_source_provenance.copy()
                                    observed[index] = offer
                            except Exception as error:
                                logger.warning("Tiki offer evidence unavailable (%s)",type(error).__name__)
                    return observed
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

        logger.info("[Tiki] No live products returned for %r", query)
        return []

    def get_product_details(self, product_id: str) -> Optional[Dict[str, Any]]:
        """Fetch granular product specification and live seller pricing from Tiki."""
        clean_id = str(product_id).replace("tiki_", "")
        url = f"{self.base_url}{self.products_endpoint}/{clean_id}"
        resp = self._request_with_retry("GET", url)
        if resp and resp.status_code == 200:
            try:
                item = resp.json()
                return self.evidence.stamp([item],resp,url)[0] if isinstance(item,dict) else None
            except Exception:
                return None
        return None

    def get_product_reviews(self, product_id: str, limit: int = 50, page: int = 1) -> List[Dict[str, Any]]:
        """Collect up to limit observed rows using Tiki's maximum20-row pages."""
        clean_id = str(product_id).replace("tiki_", "")
        if isinstance(limit,bool) or not isinstance(limit,int) or limit < 1:
            raise ValueError("Review limit must be a positive integer")
        if isinstance(page,bool) or not isinstance(page,int) or page < 1:
            raise ValueError("Review page must be a positive integer")
        page_size = min(limit,20)
        collected, seen = [], set()
        for current_page in range(page,page + math.ceil(limit / page_size)):
            items = (self.polite_crawler.fetch_product_reviews(clean_id,limit=page_size,page=current_page)
                     if self.use_polite_scraper else [])
            if not items:
                url = f"{self.base_url}{self.reviews_endpoint}"
                params = {"product_id":clean_id,"limit":page_size,"page":current_page,"sort":"score|desc"}
                response = self._request_with_retry("GET",url,params=params)
                if response is not None and response.status_code == 200:
                    try:
                        candidates = response.json().get("data",[])
                        if isinstance(candidates,list):
                            items = self.evidence.stamp(candidates,response,url)
                    except (ValueError,TypeError,AttributeError) as error:
                        logger.warning("Tiki review response could not be decoded (%s)",type(error).__name__)
            if not items:
                break
            added = 0
            for item in items:
                if not isinstance(item,dict):
                    continue
                identity = item.get("id")
                identity = str(identity) if identity is not None else None
                if identity is not None and identity in seen:
                    continue
                if identity is not None:
                    seen.add(identity)
                collected.append(item)
                added += 1
            if len(collected) >= limit or len(items) < page_size or not added:
                break
        return collected[:limit]

    def standardize_product(self, raw_item: Dict[str, Any]) -> Dict[str, Any]:
        """Map observed fields; unknown required facts stay invalid for validation."""
        from ingestion.crawler.generic_html import _number, _now
        raw_id = raw_item.get("id") or raw_item.get("product_id")
        clean_id = str(raw_id).replace("tiki_", "") if raw_id is not None else None
        collected_at = raw_item.get("collected_at") or _now()
        url_path = raw_item.get("url_path")
        product_url = raw_item.get("product_url") or (urljoin(self.base_url + '/', url_path) if url_path else None)
        brand = raw_item.get("brand") or raw_item.get("brand_name")
        brand = brand.get('name') if isinstance(brand, dict) else brand
        category = raw_item.get("category") or raw_item.get("primary_category_name")
        category = category.get('name') if isinstance(category, dict) else category
        rating = raw_item.get("rating") if raw_item.get("rating") is not None else raw_item.get("rating_average")
        record = {
            "product_id": f"tiki_{clean_id}" if clean_id else None,
            "product_name": raw_item.get("name") or raw_item.get("product_name"),
            "brand": brand, "category": category,
            "price": _number(raw_item.get("price")),
            "price_raw": raw_item.get("price_raw") if raw_item.get("price_raw") is not None else raw_item.get("price"),
            "currency": observed_currency(raw_item.get("currency"),
                raw_item.get("price_raw") if raw_item.get("price_raw") is not None else raw_item.get("price")),
            "source": "tiki_crawler", "source_group": "marketplace", "source_name": "tiki",
            "source_record_id": clean_id, "source_url": product_url,
            "acquisition_method": "live", "collected_at": collected_at,
        }
        optional = {
            "subcategory": raw_item.get("subcategory") or raw_item.get("primary_category_path"),
            "rating": _number(rating), "rating_raw": rating,
            "review_count": _number(raw_item.get("review_count"), True),
            "seller": raw_item.get("seller") or raw_item.get("seller_name"),
            "product_url": product_url,
            "image_url": raw_item.get("image_url") or raw_item.get("thumbnail_url"),
            "created_at": raw_item.get("created_at"), "updated_at": raw_item.get("updated_at"),
        }
        record.update({k: v for k, v in optional.items() if v is not None})
        from ingestion.provenance import inherit_source_metadata
        return inherit_source_metadata(record,raw_item.get('_source_provenance', {}))

    def standardize_price(self, std_product: Dict[str, Any], index: int = 1) -> Dict[str, Any]:
        from ingestion.crawler.generic_html import _now
        pid = std_product["product_id"]
        ts = std_product.get("collected_at") or _now()
        seller = std_product.get("seller")
        record = {
            "price_id": make_price_id("tiki_crawler", pid, str(seller or ""), ts),
            "product_id": pid, "price": std_product.get("price"),
            "price_raw": std_product.get("price_raw"), "currency": std_product.get("currency"),
            "timestamp": ts, "source": "tiki_crawler",
        }
        if seller is not None:
            record['seller'] = seller
        from ingestion.provenance import inherit_source_metadata
        return inherit_source_metadata(record, std_product)

    def standardize_review(self, raw_review: Dict[str, Any], product_id: Optional[str] = None) -> Dict[str, Any]:
        """Maps genuine Tiki raw review JSON to SentinelAI canonical Review schema (raw.reviews).
        Hashes customer_id / created_by.id immediately at ingest for PII protection.
        Missing/invalid raw fields remain invalid for the pre-Spark contract guard.
        """
        rev_id = raw_review.get("id")
        tiki_p_id = raw_review.get("product_id")
        original_product_id = product_id if product_id is not None else tiki_p_id
        clean_p_id = str(original_product_id).replace("tiki_", "") if original_product_id is not None else None
        resolved_pid = f"tiki_{clean_p_id}" if clean_p_id else None

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
            rev_time = None

        title = str(raw_review.get("title") or "").strip()
        raw_content = str(raw_review.get("content") or "").strip()
        # Preserve only text that actually exists in the source response.
        text = raw_content or title
        text_origin = "content" if raw_content else ("title" if text else "missing")

        created_by = raw_review.get("created_by") if isinstance(raw_review.get("created_by"), dict) else {}
        raw_uid = raw_review.get("customer_id") or created_by.get("id") or raw_review.get("user_id")

        raw_rating = raw_review.get("rating")
        if isinstance(raw_rating, str):
            try:
                raw_rating = float(raw_rating)
            except ValueError:
                pass
        from ingestion.crawler.generic_html import _boolean, _number, _now
        raw_verified = raw_review.get("purchased") if raw_review.get("purchased") is not None else raw_review.get("verified_purchase")
        verified = _boolean(raw_verified)

        helpful = _number(raw_review.get("thank_count") if raw_review.get("thank_count") is not None else raw_review.get("helpful_count"), True)

        record = {
            "review_id": f"tiki_rv_{rev_id}" if rev_id is not None else None,
            "product_id": resolved_pid,
            "user_id": anonymize_user_id(raw_uid, remove=self.remove_user_id),
            "rating": raw_rating,
            "review_title": title,
            "review_text": text,
            "review_text_origin": text_origin,
            "review_date": rev_time,
            "verified_purchase": verified,
            "helpful_count": helpful,
            "source": "tiki_crawler",
            "language": raw_review.get("language"),
            "created_at": rev_time,
            "source_group": "marketplace", "source_name": "tiki", "source_record_id": str(rev_id) if rev_id is not None else None,
            "source_url": f"{self.base_url}{self.reviews_endpoint}?product_id={clean_p_id}" if clean_p_id else None,
            "acquisition_method": "live", "collected_at": _now(),
        }
        for key in ('verified_purchase', 'helpful_count', 'language'):
            if record.get(key) is None:
                record.pop(key, None)
        from ingestion.provenance import inherit_source_metadata
        return inherit_source_metadata(record,raw_review.get('_source_provenance', {}))

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
