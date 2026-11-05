"""HTTP evidence preserves bytes and ties one collection to canonical source metadata."""
import hashlib
from pathlib import Path
import pytest
import requests

pytestmark = pytest.mark.unit


def response(content=b'{"data":[{"id":1}]}'):
    result = requests.Response()
    result.status_code = 200
    result.url = 'https://source.example/products?q=phone'
    result._content = content
    return result


def test_response_evidence_records_exact_bytes_and_private_reproducible_reference(tmp_path):
    from ingestion.evidence import ResponseEvidence
    raw = response()
    collector = ResponseEvidence('fixture_marketplace','fixture-parser-v1',directory=tmp_path,run_id='test-run')
    stamped = collector.stamp([{'id':1}],raw)
    proof = stamped[0]['_source_provenance']
    assert proof['source_group'] == 'marketplace'
    assert proof['source_url'] == raw.url
    assert proof['acquisition_method'] == 'live'
    assert proof['collection_run_id'] == 'test-run'
    assert proof['parser_version'] == 'fixture-parser-v1'
    assert proof['schema_version'] == '1.0'
    assert proof['evidence_sha256'] == hashlib.sha256(b'{"data":[{"id":1}]}').hexdigest()
    evidence = Path(proof['evidence_ref'])
    assert evidence.read_bytes() == b'{"data":[{"id":1}]}'
    assert evidence.stat().st_mode & 0o077 == 0
    assert collector.stamp([{'id':2}],raw)[0]['_source_provenance']['evidence_ref'] == str(evidence)


def test_evidence_rejects_failed_http_and_does_not_bless_missing_response_bytes(tmp_path):
    from ingestion.evidence import ResponseEvidence
    collector = ResponseEvidence('fixture','parser',directory=tmp_path,run_id='test')
    failed = response();failed.status_code=403
    with pytest.raises(ValueError,match='HTTP'):
        collector.stamp([{'id':1}],failed)
    assert not list(tmp_path.rglob('*.http'))


def test_evidence_will_not_reuse_a_tampered_evidence_file(tmp_path):
    from ingestion.evidence import ResponseEvidence
    collector = ResponseEvidence('fixture','parser',directory=tmp_path,run_id='test')
    proof = collector.stamp([{'id':1}],response())[0]['_source_provenance']
    Path(proof['evidence_ref']).write_bytes(b'tampered')
    with pytest.raises(RuntimeError,match='evidence'):
        collector.stamp([{'id':2}],response())


@pytest.mark.parametrize('platform',['tiki','lazada'])
def test_live_adapter_propagates_response_evidence_to_product_review_and_price(tmp_path,monkeypatch,platform):
    import json
    from ingestion.crawler.tiki import TikiCrawler
    from ingestion.crawler.lazada import LazadaCrawler
    crawler = (TikiCrawler(use_polite_scraper=False,evidence_dir=tmp_path) if platform=='tiki'
               else LazadaCrawler(evidence_dir=tmp_path))

    def observed_http(method,url,**kwargs):
        if url.endswith('robots.txt'):
            raw = response(b'User-agent: *\nAllow: /\n')
        elif 'reviews' in url:
            raw = response(json.dumps({'data':[{'id':456,'product_id':123,'rating':4,'title':'Fixture',
                                               'content':'Explicit synthetic HTTP fixture','created_at':1793664000}]}).encode())
        elif platform=='tiki':
            raw = response(json.dumps({'data':[{'id':123,'name':'Fixture','price':100,'currency':'VND','brand_name':'A',
                                               'primary_category_name':'Phone','url_path':'fixture-p123.html'}]}).encode())
        else:
            raw = response(json.dumps({'mods':{'listItems':[{'itemId':'123','name':'Fixture','price':100,
                                                            'priceShow':'₫100','brandName':'A','categories':['Phone']}]}}).encode())
        raw.url = url
        return raw

    monkeypatch.setattr(crawler,'_request_with_retry',observed_http)
    if platform=='tiki':
        def disabled_polite(*args,**kwargs):
            raise AssertionError('Disabled polite path must not perform HTTP')
        monkeypatch.setattr(crawler.polite_crawler,'fetch_product_reviews',disabled_polite)
    result = crawler.crawl_category('fixture',max_products=1,reviews_per_product=1)
    rows = result['products'] + result['reviews'] + result['prices']
    assert len(result['products']) == len(result['prices']) == 1
    for row in rows:
        assert row['parser_version'] and row['schema_version'] == '1.0'
        assert row['collection_run_id']
        assert Path(row['evidence_ref']).exists()
        assert hashlib.sha256(Path(row['evidence_ref']).read_bytes()).hexdigest() == row['evidence_sha256']
    assert result['prices'][0]['collection_run_id'] == result['products'][0]['collection_run_id']

def test_tiki_underspecified_api_uses_an_observed_html_offer_without_guessing_currency(tmp_path,monkeypatch):
    import json
    from ingestion.crawler.tiki import TikiCrawler
    crawler=TikiCrawler(use_polite_scraper=True,evidence_dir=tmp_path)
    api=response(json.dumps({'data':[{'id':123,'name':'API fixture','price':999,
            'url_path':'fixture-p123.html'}]}).encode())
    api.url='https://tiki.vn/api/v2/products'
    monkeypatch.setattr(crawler,'_request_with_retry',lambda *a,**k:api)
    html='<script type="application/ld+json">'+json.dumps({
        '@type':'Product','name':'HTML fixture','brand':'Fixture','category':'Phone',
        'offers':{'price':'10.50','priceCurrency':'USD'}})+'</script>'
    raw=response(html.encode());raw.url='https://tiki.vn/fixture-p123.html'
    def observed_html(url):
        crawler.polite_crawler._last_source_provenance=crawler.evidence.stamp([{}],raw)[0]['_source_provenance']
        return html
    monkeypatch.setattr(crawler.polite_crawler,'fetch_html',observed_html)
    fetched=crawler.search_products('fixture',limit=1)[0]
    row=crawler.standardize_product(fetched)
    assert row['currency']=='USD' and row['price']==10.5
    assert row['product_name']=='HTML fixture'
    assert Path(row['evidence_ref']).read_bytes()==html.encode()
