"""Daily Tiki capture: one frozen product watchlist, observed once per day into immutable files.

Collection needs only the network, so it can run on a timer while Kafka, MinIO and PostgreSQL
are stopped. Loading into the pipeline is a separate explicit step (``ingest_pending``) that
labels stored captures as snapshot replays; a capture is never relabelled as a live run.

Layout under the capture root::

    watchlist.json                    products frozen by the first successful discovery
    days/<YYYY-MM-DD>/<run_id>/       one attempt per run; never rewritten
        capture.json                  canonical products / prices / reviews
        manifest.json                 status, counts, fingerprint, policy, missing products
        ingested-<mode>.json          written only after a successful pipeline replay
    evidence/<run_id>/<sha256>.http   raw HTTP bodies referenced by every captured row
"""
from __future__ import annotations

import fcntl
import hashlib
import json
import tempfile
import uuid
from contextlib import contextmanager
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Iterator, List, Tuple
from urllib.parse import urljoin, urlsplit

import requests

from data.schemas.validator import SchemaValidator
from ingestion.crawler.tiki import BASE_HOST, TikiBlocked, product_id_from_url
from ingestion.privacy import sanitize_record_pii

COLLECTED = ('complete', 'partial')
REVIEW_SORT = 'id|desc'
MODES = ('integration', 'production', 'local', 'test')
DATASETS = (('products', 'product'), ('prices', 'price'), ('reviews', 'review'))


class CaptureBusy(RuntimeError):
    """Another capture or ingest currently holds the capture directory."""


class NoProductsDiscovered(RuntimeError):
    """The first search returned no usable Tiki product page."""


class NoProductsObserved(RuntimeError):
    """No watchlist product page could be observed in this run."""


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _write_new(path: Path, payload: Any, mode: int = 0o600) -> bytes:
    content = (json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + '\n').encode()
    with path.open('xb') as stream:
        stream.write(content)
    path.chmod(mode)
    return content


@contextmanager
def _locked(root: Path) -> Iterator[None]:
    root.mkdir(parents=True, exist_ok=True)
    with (root / '.lock').open('a') as handle:
        try:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise CaptureBusy(f'Capture directory is in use: {root}') from error
        try:
            yield
        finally:
            fcntl.flock(handle, fcntl.LOCK_UN)


def _attempts(root: Path) -> List[Tuple[Path, Dict[str, Any]]]:
    rows = [(path.parent, json.loads(path.read_text())) for path in (root / 'days').glob('*/*/manifest.json')]
    return sorted(rows, key=lambda row: (row[1]['day'], row[1]['finished_at']))


def _watchlist(root: Path, crawler, query: str, max_products: int) -> Dict[str, Any]:
    path = root / 'watchlist.json'
    if path.exists():
        return json.loads(path.read_text())
    products, seen = [], set()
    for item in crawler.search_products(query, limit=max_products, observe_offers=False):
        url = item.get('product_url') or (urljoin(crawler.base_url + '/', item['url_path'])
                                          if item.get('url_path') else None)
        product_id = product_id_from_url(url) if url else None
        if not product_id or urlsplit(url).hostname != BASE_HOST or product_id in seen:
            continue
        seen.add(product_id)
        products.append({'product_id': product_id, 'product_url': url, 'name': item.get('name')})
        if len(products) >= max_products:
            break
    if not products:
        raise NoProductsDiscovered(f'No Tiki product pages discovered for {query!r}')
    watchlist = {'query': query, 'created_at': _now(), 'discovery_run_id': crawler.evidence.run_id,
                 'products': products}
    _write_new(path, watchlist)
    return watchlist


def _observe(crawler, watchlist, query, reviews_per_product, missing) -> Dict[str, List[Dict[str, Any]]]:
    captured = {'products': [], 'prices': [], 'reviews': []}
    for index, entry in enumerate(watchlist['products'], start=1):
        try:
            offer = crawler.observe_product_page(entry['product_url'], query)
        except (TikiBlocked, PermissionError):
            raise
        except (requests.RequestException, RuntimeError) as error:
            missing.append({'product_id': entry['product_id'], 'reason': type(error).__name__})
            continue
        product = sanitize_record_pii(crawler.standardize_product(offer), remove_user_id=crawler.remove_user_id)
        captured['products'].append(product)
        captured['prices'].append(crawler.standardize_price(product, index=index))
        if reviews_per_product > 0:
            for raw in crawler.get_product_reviews(entry['product_id'], limit=reviews_per_product, sort=REVIEW_SORT):
                review = crawler.standardize_review(raw, product_id=entry['product_id'])
                captured['reviews'].append(sanitize_record_pii(review, remove_user_id=crawler.remove_user_id))
    if not captured['products']:
        raise NoProductsObserved('No watchlist product page was observed')
    return captured


def capture_day(root, day: date, crawler_factory: Callable[[str, Path], Any], *, query: str = 'iphone',
                max_products: int = 20, reviews_per_product: int = 10) -> Dict[str, Any]:
    """Observe the watchlist once for ``day``; a day already collected is skipped, not repeated.

    ``crawler_factory(run_id, evidence_dir)`` returns a TikiCrawler. Any error is recorded in a
    failed manifest and leaves the day open for a later retry in a new attempt directory.
    """
    root = Path(root)
    day_key = day.isoformat()
    with _locked(root):
        for attempt_dir, manifest in _attempts(root):
            if manifest['day'] == day_key and manifest['status'] in COLLECTED:
                return {'status': 'skipped', 'day': day_key, 'existing_run_id': manifest['collection_run_id'],
                        'attempt_dir': attempt_dir}
        run_id = uuid.uuid4().hex
        attempt_dir = root / 'days' / day_key / run_id
        attempt_dir.mkdir(parents=True, exist_ok=False, mode=0o700)
        crawler = crawler_factory(run_id, root / 'evidence')
        manifest = {'source': 'tiki', 'day': day_key, 'collection_run_id': run_id, 'query': query,
                    'started_at': _now(), 'missing': [], 'error': None, 'counts': None,
                    'policy': {'robots_txt': 'enforced', 'delay_seconds': crawler.polite_crawler.delay_seconds,
                               'max_products': max_products, 'reviews_per_product': reviews_per_product,
                               'review_sort': REVIEW_SORT}}
        try:
            watchlist = _watchlist(root, crawler, query, max_products)
            captured = _observe(crawler, watchlist, query, reviews_per_product, manifest['missing'])
        except Exception as error:
            manifest.update(status='failed', error=type(error).__name__, error_message=str(error)[:500])
        else:
            content = _write_new(attempt_dir / 'capture.json', captured, mode=0o400)
            manifest.update(
                status='partial' if manifest['missing'] else 'complete',
                capture_file='capture.json', capture_sha256=hashlib.sha256(content).hexdigest(),
                counts={name: len(captured[name]) for name, _ in DATASETS},
                schema_valid={name: sum(SchemaValidator.validate_record(row, schema)[0] for row in captured[name])
                              for name, schema in DATASETS})
        finally:
            crawler.session.close()
            crawler.polite_crawler.session.close()
        manifest['finished_at'] = _now()
        _write_new(attempt_dir / 'manifest.json', manifest, mode=0o400)
        return dict(manifest, attempt_dir=attempt_dir)


def _ingested(attempt_dir: Path, mode: str) -> Path:
    if mode not in MODES:
        raise ValueError(f'Unsupported runtime mode: {mode}')
    return attempt_dir / f'ingested-{mode}.json'


def pending_days(root, mode: str) -> List[Tuple[Path, Dict[str, Any]]]:
    """Collected attempts not yet replayed into ``mode``, oldest day first."""
    return [(attempt_dir, manifest) for attempt_dir, manifest in _attempts(Path(root))
            if manifest['status'] in COLLECTED and not _ingested(attempt_dir, mode).exists()]


def ingest_pending(root, mode: str, runner: Callable[..., bool]) -> List[Dict[str, Any]]:
    """Replay every collected, not-yet-loaded day through ``runner`` (``run_end_to_end``), oldest first."""
    root = Path(root)
    results = []
    with _locked(root):
        for attempt_dir, manifest in pending_days(root, mode):
            raw = (attempt_dir / manifest['capture_file']).read_bytes()
            if hashlib.sha256(raw).hexdigest() != manifest['capture_sha256']:
                raise ValueError(f"Capture fingerprint mismatch for {manifest['day']}; refusing to replay it")
            captured = json.loads(raw)
            with tempfile.TemporaryDirectory(prefix='tiki-replay-') as folder:
                paths = {}
                for name, _ in DATASETS:
                    rows = [dict(row, acquisition_method='snapshot_replay',
                                 original_acquisition_method=row.get('acquisition_method'))
                            for row in captured[name]]
                    path = Path(folder) / f'{name}.json'
                    path.write_text(json.dumps(rows, ensure_ascii=False))
                    paths[f'{name}_path'] = str(path)
                ok = runner(mode=mode, **paths)
            if not ok:
                raise RuntimeError(f"Pipeline replay failed for Tiki capture {manifest['day']} "
                                   f"({manifest['collection_run_id']})")
            _write_new(_ingested(attempt_dir, mode), {'mode': mode, 'ingested_at': _now(),
                                                       'collection_run_id': manifest['collection_run_id'],
                                                       'capture_sha256': manifest['capture_sha256']})
            results.append({'day': manifest['day'], 'collection_run_id': manifest['collection_run_id'],
                            'mode': mode, 'counts': manifest['counts']})
    return results


def capture_status(root) -> List[Dict[str, Any]]:
    """One row per day: the collected attempt if any, otherwise the latest failed one."""
    by_day: Dict[str, List[Tuple[Path, Dict[str, Any]]]] = {}
    for attempt_dir, manifest in _attempts(Path(root)):
        by_day.setdefault(manifest['day'], []).append((attempt_dir, manifest))
    rows = []
    for day, attempts in sorted(by_day.items()):
        collected = [attempt for attempt in attempts if attempt[1]['status'] in COLLECTED]
        attempt_dir, manifest = collected[0] if collected else attempts[-1]
        rows.append({'day': day, 'status': manifest['status'], 'collection_run_id': manifest['collection_run_id'],
                     'counts': manifest['counts'], 'missing': len(manifest['missing']), 'error': manifest['error'],
                     'attempts': len(attempts),
                     'ingested_modes': sorted(path.name[len('ingested-'):-len('.json')]
                                              for path in attempt_dir.glob('ingested-*.json'))})
    return rows
