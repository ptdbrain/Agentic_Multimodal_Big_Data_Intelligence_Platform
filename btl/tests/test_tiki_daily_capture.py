"""Daily Tiki capture: a real TikiCrawler over a controlled HTTP fixture, never the public site."""
import hashlib
import json
import re
from datetime import date
from urllib.parse import urlencode, urlsplit

import pytest
import requests

from ingestion import daily_capture
from ingestion.crawler import tiki as tiki_module
from ingestion.crawler.tiki import TikiCrawler

pytestmark = pytest.mark.unit

DAY = date(2026, 11, 11)
NEXT_DAY = date(2026, 11, 12)


def _response(url, status=200, body=b'', content_type='application/json'):
    response = requests.Response()
    response.url = url
    response.status_code = status
    response._content = body
    response.headers['Content-Type'] = content_type
    response.encoding = 'utf-8'
    return response


class FakeTiki:
    """Explicit HTTP fixture shaped like the observed Tiki search, product page and review responses."""

    def __init__(self, catalog, price_offset=0, blocked=(), missing=()):
        self.catalog = dict(catalog)
        self.price_offset = price_offset
        self.blocked = set(blocked)
        self.missing = set(missing)
        self.calls = []

    def handle(self, url, params=None):
        full_url = url + ('?' + urlencode(params) if params else '')
        self.calls.append(full_url)
        path = urlsplit(url).path
        if path == '/robots.txt':
            return _response(url, body=b'User-agent: *\nAllow: /\n', content_type='text/plain')
        if path == '/api/v2/products':
            rows = [{'id': pid, 'name': name, 'url_path': f'phone-{pid}-p{pid}.html'}
                    for pid, name in self.catalog.items()]
            return _response(full_url, body=json.dumps({'data': rows}).encode())
        if path == '/api/v2/reviews':
            pid = int(params['product_id'])
            rows = [{'id': pid * 100 + n, 'product_id': pid, 'content': 'Hàng tốt', 'rating': 5,
                     'created_at': 1794383103 - n, 'customer_id': 9000 + n} for n in range(2)]
            return _response(full_url, body=json.dumps({'data': rows[:int(params['limit'])]}).encode())
        match = re.search(r'-p(\d+)\.html$', path)
        if match:
            pid = int(match.group(1))
            if pid in self.blocked:
                return _response(url, status=429, body=b'slow down', content_type='text/plain')
            if pid in self.missing or pid not in self.catalog:
                return _response(url, status=404, body=b'not found', content_type='text/plain')
            product = {'@type': 'Product', 'name': self.catalog[pid], 'brand': 'Apple',
                       'category': 'Điện thoại Smartphone',
                       'offers': {'price': 1_000_000 * pid + self.price_offset, 'priceCurrency': 'VND'}}
            html = ('<html><head><script type="application/ld+json">' + json.dumps(product)
                    + f'</script></head><body><h1>{self.catalog[pid]}</h1></body></html>')
            return _response(url, body=html.encode(), content_type='text/html; charset=utf-8')
        return _response(url, status=404, body=b'unknown route', content_type='text/plain')

    def factory(self, run_id, evidence_dir):
        crawler = TikiCrawler(collection_run_id=run_id, evidence_dir=evidence_dir)
        crawler.session.request = lambda method, url, **kwargs: self.handle(url, kwargs.get('params'))
        crawler.polite_crawler.session.get = lambda url, **kwargs: self.handle(url, kwargs.get('params'))
        return crawler

    def product_page_calls(self):
        return [call for call in self.calls if re.search(r'-p\d+\.html$', urlsplit(call).path)]

    def search_calls(self):
        return [call for call in self.calls if urlsplit(call).path == '/api/v2/products']


@pytest.fixture(autouse=True)
def no_politeness_sleep(monkeypatch):
    monkeypatch.setattr(tiki_module.time, 'sleep', lambda seconds: None)


def _capture(root, fake, day=DAY, **options):
    options.setdefault('reviews_per_product', 2)
    return daily_capture.capture_day(root, day, fake.factory, query='iphone', max_products=3, **options)


def _load(manifest):
    return json.loads((manifest['attempt_dir'] / 'capture.json').read_text())


def test_first_capture_freezes_watchlist_and_records_observed_product_pages(tmp_path):
    fake = FakeTiki({101: 'Phone 101', 102: 'Phone 102', 103: 'Phone 103'})
    manifest = _capture(tmp_path, fake)

    assert manifest['status'] == 'complete'
    watchlist = json.loads((tmp_path / 'watchlist.json').read_text())
    assert [row['product_id'] for row in watchlist['products']] == ['tiki_101', 'tiki_102', 'tiki_103']
    captured = _load(manifest)
    assert [row['product_id'] for row in captured['products']] == ['tiki_101', 'tiki_102', 'tiki_103']
    assert [(row['price'], row['currency']) for row in captured['prices']] == [
        (101_000_000.0, 'VND'), (102_000_000.0, 'VND'), (103_000_000.0, 'VND')]
    assert manifest['counts'] == {'products': 3, 'prices': 3, 'reviews': 6}
    assert manifest['schema_valid'] == manifest['counts']
    assert len(fake.product_page_calls()) == 3


def test_captured_rows_carry_verifiable_live_evidence_and_hashed_reviewers(tmp_path):
    manifest = _capture(tmp_path, FakeTiki({101: 'Phone 101'}))
    captured = _load(manifest)
    raw = (manifest['attempt_dir'] / 'capture.json').read_bytes()

    assert manifest['capture_sha256'] == hashlib.sha256(raw).hexdigest()
    rows = captured['products'] + captured['prices'] + captured['reviews']
    assert {row['acquisition_method'] for row in rows} == {'live'}
    assert {row['collection_run_id'] for row in rows} == {manifest['collection_run_id']}
    for row in rows:
        evidence = (tmp_path / 'evidence' / manifest['collection_run_id'] / (row['evidence_sha256'] + '.http'))
        assert hashlib.sha256(evidence.read_bytes()).hexdigest() == row['evidence_sha256']
    assert all(row['user_id'].startswith('anon_') for row in captured['reviews'])
    assert not any('9000' in json.dumps(row) for row in captured['reviews'])


def test_reviews_are_requested_newest_first(tmp_path):
    fake = FakeTiki({101: 'Phone 101'})
    _capture(tmp_path, fake)
    review_calls = [call for call in fake.calls if '/api/v2/reviews' in call]
    assert review_calls and all('sort=id%7Cdesc' in call for call in review_calls)


def test_later_day_recollects_the_frozen_watchlist_without_searching(tmp_path):
    _capture(tmp_path, FakeTiki({101: 'Phone 101', 102: 'Phone 102'}))
    changed_search = FakeTiki({101: 'Phone 101', 102: 'Phone 102', 555: 'Newcomer'}, price_offset=7)
    manifest = _capture(tmp_path, changed_search, day=NEXT_DAY)

    captured = _load(manifest)
    assert changed_search.search_calls() == []
    assert [row['product_id'] for row in captured['products']] == ['tiki_101', 'tiki_102']
    assert [row['price'] for row in captured['prices']] == [101_000_007.0, 102_000_007.0]


def test_a_collected_day_is_not_collected_again(tmp_path):
    first = _capture(tmp_path, FakeTiki({101: 'Phone 101'}))
    repeat = FakeTiki({101: 'Phone 101'})
    result = _capture(tmp_path, repeat)

    assert result['status'] == 'skipped'
    assert result['existing_run_id'] == first['collection_run_id']
    assert repeat.calls == []


def test_missing_product_page_gives_partial_capture_of_the_rest(tmp_path):
    _capture(tmp_path, FakeTiki({101: 'Phone 101', 102: 'Phone 102'}))
    manifest = _capture(tmp_path, FakeTiki({101: 'Phone 101', 102: 'Phone 102'}, missing={102}), day=NEXT_DAY)

    assert manifest['status'] == 'partial'
    assert [row['product_id'] for row in manifest['missing']] == ['tiki_102']
    assert [row['product_id'] for row in _load(manifest)['products']] == ['tiki_101']


def test_rate_limit_stops_the_run_and_a_retry_keeps_the_failed_attempt(tmp_path):
    _capture(tmp_path, FakeTiki({101: 'Phone 101', 102: 'Phone 102'}))
    blocked = FakeTiki({101: 'Phone 101', 102: 'Phone 102'}, blocked={101})
    failed = _capture(tmp_path, blocked, day=NEXT_DAY)

    assert failed['status'] == 'failed'
    assert failed['error'] == 'TikiBlocked'
    assert len(blocked.product_page_calls()) == 1
    assert not (failed['attempt_dir'] / 'capture.json').exists()

    retried = _capture(tmp_path, FakeTiki({101: 'Phone 101', 102: 'Phone 102'}), day=NEXT_DAY)
    assert retried['status'] == 'complete'
    assert json.loads((failed['attempt_dir'] / 'manifest.json').read_text())['status'] == 'failed'


def test_failed_discovery_creates_no_watchlist(tmp_path):
    manifest = _capture(tmp_path, FakeTiki({}))
    assert manifest['status'] == 'failed'
    assert manifest['error'] == 'NoProductsDiscovered'
    assert not (tmp_path / 'watchlist.json').exists()


def _replay_runner(calls, ok=True):
    def runner(**kwargs):
        rows = {name: json.loads(open(kwargs[name + '_path']).read()) for name in ('products', 'reviews', 'prices')}
        calls.append({'mode': kwargs['mode'], **rows})
        return ok
    return runner


def test_ingest_replays_each_unloaded_day_with_honest_labels_once(tmp_path):
    _capture(tmp_path, FakeTiki({101: 'Phone 101'}))
    _capture(tmp_path, FakeTiki({101: 'Phone 101'}), day=NEXT_DAY)
    calls = []

    results = daily_capture.ingest_pending(tmp_path, 'integration', _replay_runner(calls))

    assert [row['day'] for row in results] == ['2026-11-11', '2026-11-12']
    assert [call['mode'] for call in calls] == ['integration', 'integration']
    rows = [row for call in calls for name in ('products', 'reviews', 'prices') for row in call[name]]
    assert {row['acquisition_method'] for row in rows} == {'snapshot_replay'}
    assert {row['original_acquisition_method'] for row in rows} == {'live'}
    assert daily_capture.ingest_pending(tmp_path, 'integration', _replay_runner(calls)) == []
    assert len(calls) == 2


def test_failed_ingest_leaves_the_day_pending(tmp_path):
    _capture(tmp_path, FakeTiki({101: 'Phone 101'}))
    with pytest.raises(RuntimeError, match='2026-11-11'):
        daily_capture.ingest_pending(tmp_path, 'integration', _replay_runner([], ok=False))
    assert len(daily_capture.ingest_pending(tmp_path, 'integration', _replay_runner([]))) == 1


def test_ingest_refuses_a_capture_whose_bytes_changed(tmp_path):
    manifest = _capture(tmp_path, FakeTiki({101: 'Phone 101'}))
    path = manifest['attempt_dir'] / 'capture.json'
    path.chmod(0o600)
    path.write_text(path.read_text().replace('Phone 101', 'Edited'))
    with pytest.raises(ValueError, match='fingerprint'):
        daily_capture.ingest_pending(tmp_path, 'integration', _replay_runner([]))


def test_failed_and_ingested_days_are_listed_in_status(tmp_path):
    _capture(tmp_path, FakeTiki({101: 'Phone 101'}))
    daily_capture.ingest_pending(tmp_path, 'integration', _replay_runner([]))
    _capture(tmp_path, FakeTiki({101: 'Phone 101'}, blocked={101}), day=NEXT_DAY)

    rows = daily_capture.capture_status(tmp_path)
    assert [(row['day'], row['status'], row['ingested_modes']) for row in rows] == [
        ('2026-11-11', 'complete', ['integration']), ('2026-11-12', 'failed', [])]


def test_cli_exit_code_tells_the_timer_whether_the_day_was_collected(tmp_path, capsys):
    from scripts import collect_tiki_daily as cli
    args = ['capture', '--root', str(tmp_path), '--day', '2026-11-11', '--max-products', '3']

    assert cli.main(args, crawler_factory=FakeTiki({}).factory) == 1
    assert cli.main(args, crawler_factory=FakeTiki({101: 'Phone 101'}).factory) == 0
    assert cli.main(args, crawler_factory=FakeTiki({101: 'Phone 101'}).factory) == 0
    output = capsys.readouterr().out
    assert 'failed' in output and 'complete' in output and 'skipped' in output


def test_cli_dry_run_lists_pending_days_without_running_the_pipeline(tmp_path, capsys):
    from scripts import collect_tiki_daily as cli
    _capture(tmp_path, FakeTiki({101: 'Phone 101'}))
    calls = []

    code = cli.main(['ingest-pending', '--root', str(tmp_path), '--mode', 'local', '--dry-run'],
                    runner=_replay_runner(calls))

    assert code == 0 and calls == []
    assert '2026-11-11' in capsys.readouterr().out
    assert len(daily_capture.ingest_pending(tmp_path, 'local', _replay_runner(calls))) == 1


def test_timer_units_run_the_capture_daily_and_catch_up_after_downtime():
    from pathlib import Path
    units = Path(__file__).resolve().parents[1] / 'orchestration' / 'systemd'
    service = (units / 'sentinel-tiki-capture.service').read_text()
    timer = (units / 'sentinel-tiki-capture.timer').read_text()
    assert 'scripts/collect_tiki_daily.py capture' in service
    assert 'Type=oneshot' in service and 'Restart=on-failure' in service
    assert 'OnCalendar=*-*-* 09:00:00' in timer and 'Persistent=true' in timer
    assert 'Unit=sentinel-tiki-capture.service' in timer
