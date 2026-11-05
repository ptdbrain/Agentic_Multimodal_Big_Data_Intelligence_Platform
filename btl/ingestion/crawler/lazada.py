"""Public Lazada listing adapter; no synthetic or snapshot fallback."""
from urllib.parse import urlencode, urljoin
from urllib.robotparser import RobotFileParser

from ingestion.crawler.base import BaseEcommerceCrawler
from ingestion.crawler.generic_html import _number, _now


class LazadaCrawler(BaseEcommerceCrawler):
    PARSER_VERSION = 'lazada-listing-v1'

    def __init__(self, **kwargs):
        super().__init__(platform_name='Lazada', base_url='https://www.lazada.vn', **kwargs)
        self._robots = None

    def search_products(self, query, limit=20, page=1):
        if limit < 0 or page < 1:
            raise ValueError('Invalid listing bounds')
        if self._robots is None:
            response = self._request_with_retry('GET', self.base_url + '/robots.txt')
            if response is None or response.status_code != 200:
                raise RuntimeError('Cannot verify Lazada robots policy')
            self._robots = RobotFileParser()
            self._robots.parse(response.text.splitlines())
        rows, seen = [], set()
        while len(rows) < limit:
            url = self.base_url + '/catalog/?' + urlencode({'ajax': 'true', 'q': query, 'page': page})
            if not self._robots.can_fetch(self.user_agent, url):
                raise RuntimeError('Lazada robots policy disallows listing URL')
            response = self._request_with_retry('GET', url)
            if response is None or response.status_code != 200:
                raise RuntimeError('Lazada listing request failed')
            payload = response.json()
            mods = payload.get('mods') if isinstance(payload, dict) else None
            if not isinstance(mods, dict) or not isinstance(mods.get('listItems'), list):
                raise ValueError('Lazada listing schema changed or access challenged')
            items = mods['listItems']
            new = [r for r in items if isinstance(r, dict) and r.get('itemId') not in seen]
            if not new:
                break
            new = self.evidence.stamp(new,response,url)
            collected = _now()
            for item in new:
                item = dict(item, collected_at=collected, source_url=url)
                seen.add(item.get('itemId'))
                rows.append(item)
                if len(rows) == limit:
                    break
            page += 1
        return rows

    def standardize_product(self, raw_item):
        raw_id = str(raw_item.get('itemId') or '')
        if not raw_id.isdigit():
            raise ValueError('Missing or invalid Lazada itemId')
        price_show = str(raw_item.get('priceShow') or '')
        categories = raw_item.get('categories')
        record = {
            'product_id': 'lazada_' + raw_id, 'product_name': raw_item.get('name'),
            'brand': raw_item.get('brandName'),
            'category': '/'.join(map(str, categories)) if isinstance(categories, list) and categories else None,
            'price': _number(raw_item.get('price')), 'price_raw': raw_item.get('price'),
            'currency': 'VND' if '₫' in price_show else None,
            'rating': _number(raw_item.get('ratingScore')),
            'review_count': _number(raw_item.get('review'), True), 'seller': raw_item.get('sellerName'),
            'product_url': urljoin(self.base_url, raw_item.get('itemUrl') or '/products/pdp-i' + raw_id + '.html'),
            'image_url': raw_item.get('image'), 'source': 'lazada_crawler',
            'source_name': 'lazada', 'source_record_id': raw_id, 'source_group': 'marketplace',
            'source_url': raw_item.get('source_url'), 'acquisition_method': 'live',
            'collected_at': raw_item.get('collected_at') or _now(),
        }
        from ingestion.provenance import inherit_source_metadata
        return inherit_source_metadata({k:v for k,v in record.items() if v is not None},
                                       raw_item.get('_source_provenance', {}))

    def get_product_details(self, product_id):
        raise NotImplementedError('Lazada adapter supports observed listing data only')

    def get_product_reviews(self, product_id, limit=50, page=1):
        # No review endpoint has been verified; never fabricate reviews.
        return []

    def standardize_review(self, raw_review, product_id=None):
        raise NotImplementedError('Lazada reviews are not supported')
