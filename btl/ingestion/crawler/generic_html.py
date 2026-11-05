"""Extract observed web metadata without filling missing business facts."""
import datetime
import hashlib
import json
import math
from typing import List, Dict, Any, Optional
from urllib.parse import urlparse

from bs4 import BeautifulSoup
from ingestion.crawler.base import BaseEcommerceCrawler
from ingestion.privacy import anonymize_user_id, sanitize_record_pii


def _objects(value):
    if isinstance(value, list):
        for item in value:
            yield from _objects(item)
    elif isinstance(value, dict):
        yield value
        if '@graph' in value:
            yield from _objects(value['@graph'])


def _documents(html):
    soup = BeautifulSoup(html, 'html.parser')
    for script in soup.find_all('script', type='application/ld+json'):
        try:
            yield from _objects(json.loads(script.string or script.get_text()))
        except (ValueError, TypeError):
            continue


def _number(value, integer=False):
    if value is None or isinstance(value, bool):
        return None
    try:
        parsed = float(value)
        if not math.isfinite(parsed) or (integer and not parsed.is_integer()):
            return None
        return int(parsed) if integer else parsed
    except (ValueError, TypeError, OverflowError):
        return None


def _boolean(value):
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return {'true': True, 'false': False}.get(value.strip().lower())
    return None


def _now():
    return datetime.datetime.now(datetime.timezone.utc).isoformat().replace('+00:00', 'Z')


class UniversalProductScraper(BaseEcommerceCrawler):
    """Parse JSON-LD Product/Review and OpenGraph product metadata.

    Absent required business fields intentionally remain absent: downstream
    validation must quarantine incomplete records, rather than invent facts.
    """

    def __init__(self, **kwargs):
        super().__init__(platform_name='Universal', base_url='https://generic.store', **kwargs)

    def scrape_url(self, target_url: str) -> Optional[Dict[str, Any]]:
        resp = self._request_with_retry('GET', target_url)
        if resp is None or resp.status_code != 200:
            return None
        return self._extract_metadata_from_html(resp.text, target_url) or None

    def _extract_metadata_from_html(self, html: str, url: str) -> Dict[str, Any]:
        domain = urlparse(url).netloc.lower()
        for item in _documents(html):
            types = item.get('@type', [])
            types = [types] if isinstance(types, str) else types
            if not isinstance(types, list) or not {'Product', 'IndividualProduct'}.intersection(types):
                continue
            offers = item.get('offers') or {}
            if isinstance(offers, list):
                offers = next((o for o in offers if isinstance(o, dict)), {})
            offers = offers if isinstance(offers, dict) else {}
            agg = item.get('aggregateRating') or {}
            agg = agg if isinstance(agg, dict) else {}
            brand = item.get('brand')
            brand = brand.get('name') if isinstance(brand, dict) else brand
            seller = offers.get('seller')
            seller = seller.get('name') if isinstance(seller, dict) else seller
            identity = str(item.get('sku') or item.get('@id') or url)
            record = {
                'product_id': 'web_' + hashlib.sha256(f'{domain}:{identity}'.encode()).hexdigest()[:24],
                'product_name': item.get('name'), 'brand': brand, 'category': item.get('category'),
                'price': _number(offers.get('price')), 'price_raw': offers.get('price'),
                'currency': offers.get('priceCurrency'), 'rating': _number(agg.get('ratingValue')),
                'rating_raw': agg.get('ratingValue'), 'review_count': _number(agg.get('reviewCount'), True),
                'seller': seller, 'image_url': item.get('image'), 'product_url': url,
                'source': 'web_crawler', 'collected_at': _now(),
                'created_at': item.get('datePublished'), 'updated_at': item.get('dateModified'),
            }
            return sanitize_record_pii({k: v for k, v in record.items() if v is not None}, remove_user_id=self.remove_user_id)

        soup = BeautifulSoup(html, 'html.parser')
        def meta(name):
            tag = soup.find('meta', attrs={'property': name}) or soup.find('meta', attrs={'name': name})
            return tag.get('content') if tag else None
        title = meta('og:title') or meta('twitter:title')
        if not title:
            return {}
        record = {
            'product_id': 'web_' + hashlib.sha256(url.encode()).hexdigest()[:24],
            'product_name': title, 'price': _number(meta('product:price:amount') or meta('og:price:amount')),
            'price_raw': meta('product:price:amount') or meta('og:price:amount'),
            'currency': meta('product:price:currency') or meta('og:price:currency'),
            'image_url': meta('og:image'), 'product_url': url,
            'source': 'web_crawler', 'collected_at': _now(),
        }
        return sanitize_record_pii({k: v for k, v in record.items() if v is not None}, remove_user_id=self.remove_user_id)

    def extract_reviews_from_html(self, html: str, product_id: str) -> List[Dict[str, Any]]:
        reviews = []
        for item in _documents(html):
            types = item.get('@type', [])
            types = [types] if isinstance(types, str) else types
            values = [item] if isinstance(types, list) and 'Review' in types else item.get('review', [])
            values = [values] if isinstance(values, dict) else values
            if not isinstance(values, list):
                continue
            for rv in values:
                if not isinstance(rv, dict):
                    continue
                author = rv.get('author')
                author = author.get('name') if isinstance(author, dict) else author
                rating = rv.get('reviewRating')
                raw_rating = rating.get('ratingValue') if isinstance(rating, dict) else rv.get('rating')
                user_id = anonymize_user_id(author, remove=self.remove_user_id)
                text = str(rv.get('reviewBody') or rv.get('description') or '')
                date = rv.get('datePublished')
                fingerprint = json.dumps([product_id, user_id, text, date, raw_rating], ensure_ascii=False, sort_keys=True)
                rev_id = rv.get('@id') or rv.get('review_id') or 'web_rv_' + hashlib.sha256(fingerprint.encode()).hexdigest()[:24]
                record = {
                    'review_id': str(rev_id), 'product_id': product_id, 'user_id': user_id,
                    'rating': _number(raw_rating), 'rating_raw': raw_rating,
                    'review_title': str(rv.get('name') or ''), 'review_text': text,
                    'review_date': date, 'created_at': date,
                    'verified_purchase': _boolean(rv.get('verified_purchase')),
                    'helpful_count': _number(rv.get('helpful_count'), True),
                    'language': rv.get('inLanguage'), 'source': 'web_crawler', 'collected_at': _now(),
                }
                reviews.append(sanitize_record_pii({k: v for k, v in record.items() if v is not None}, remove_user_id=self.remove_user_id))
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
        record = dict(raw_review)
        if product_id and not record.get('product_id'):
            record['product_id'] = product_id
        return sanitize_record_pii(record, remove_user_id=self.remove_user_id)
