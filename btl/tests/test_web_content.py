import pytest
from ingestion.web_content import parse_article

pytestmark = pytest.mark.unit


def test_article_keeps_own_schema_and_missing_time():
    row = parse_article('<article><h1>Observed title</h1><p>Actual published body.</p></article>', 'https://tinhte.vn/thread/one.123/', '2026-11-02T00:00:00Z')
    assert row['title'] == 'Observed title'
    assert 'Actual published body.' in row['body']
    assert row['published_at'] is None
    assert row['source_group'] == 'web_content'
    assert 'rating' not in row and 'product_id' not in row


def test_access_denied_is_not_an_article():
    with pytest.raises(ValueError):
        parse_article('<title>Access denied</title>', 'https://tinhte.vn/thread/one.123/', '2026-11-02T00:00:00Z')


def test_article_identity_stable_when_content_changes():
    url = 'https://tinhte.vn/thread/one.123/'
    a = parse_article('<article><h1>T</h1><p>Body A</p></article>', url, '2026-11-01T00:00:00Z')
    b = parse_article('<article><h1>T</h1><p>Body B</p></article>', url, '2026-11-02T00:00:00Z')
    assert a['article_id'] == b['article_id']
    assert a['content_sha256'] != b['content_sha256']


def test_collector_fetches_articles_and_retains_response_evidence(tmp_path):
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
    import threading
    from ingestion.web_content import collect_articles
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            body = (b'User-agent: *\nAllow: /' if self.path == '/robots.txt' else
                    b'<a href="/thread/one/">One</a>' if self.path == '/' else
                    b'<article><h1>Observed title</h1><p>Actual HTTP body</p></article>')
            self.send_response(200); self.end_headers(); self.wfile.write(body)
        def log_message(self, *args):
            pass
    with ThreadingHTTPServer(('127.0.0.1', 0), Handler) as server:
        thread = threading.Thread(target=server.serve_forever, daemon=True); thread.start()
        try:
            rows, manifest = collect_articles([f'http://127.0.0.1:{server.server_port}/'], limit=1, max_pages=3,
                                               evidence_dir=tmp_path, request_interval=0)
            assert len(rows) == 1
            assert rows[0]['body'].endswith('Actual HTTP body')
            assert rows[0]['acquisition_method'] == 'live'
            assert manifest['articles_collected'] == 1
            assert any(item.get('response_sha256') for item in manifest['requests'])
            assert list(tmp_path.glob('*.html'))
        finally:
            server.shutdown(); thread.join(timeout=5)
