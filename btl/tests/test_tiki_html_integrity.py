import json
import pytest
from ingestion.crawler.tiki import parse_price, extract_page_fields

pytestmark = pytest.mark.unit


def test_html_price_parser_does_not_collapse_arbitrary_digits():
    assert parse_price('Call 123 for model 456') is None
    assert parse_price('-100000 VND') is None
    assert parse_price('10.50 USD') == 10.5
    assert parse_price(25.555) == 25.555


def test_html_extraction_does_not_invent_business_fields():
    row = extract_page_fields('<h1>Observed phone</h1>', 'https://tiki.vn/phone-p123.html', category_seed='smartphones')
    for field in ['brand','category','price','review_count','seller']:
        assert row.get(field) is None, field
    assert row['product_name'] == 'Observed phone'


def test_html_rejects_unqualified_model_number_as_price():
    row = extract_page_fields('<h1>Phone 123456</h1>', 'https://tiki.vn/phone-p123.html')
    assert row.get('price') is None


def test_explicit_invalid_price_is_not_repaired_from_other_visible_digits():
    html = '<script type="application/ld+json">' + json.dumps({'@type':'Product','name':'Phone','offers':{'price':-100}}) + '</script><p>Warranty value 200000 VND</p>'
    row = extract_page_fields(html, 'https://tiki.vn/phone-p123.html')
    assert row.get('price') is None
    assert row['price_raw'] == -100


def test_api_review_missing_title_is_not_replaced_with_an_invented_title():
    from ingestion.crawler.tiki import TikiCrawler
    row = TikiCrawler().standardize_review({'id':1,'product_id':123,'rating':4,'content':'Observed body',
                                           'created_at':'2026-11-01T00:00:00Z'})
    assert row['review_title'] == ''
    assert row['review_text'] == 'Observed body'


def test_api_review_without_product_identity_cannot_acquire_a_fake_product_id():
    from ingestion.crawler.tiki import TikiCrawler
    row = TikiCrawler().standardize_review({'id':1,'rating':4,'title':'Observed title','content':'Observed body',
                                           'created_at':'2026-11-01T00:00:00Z'})
    assert row['product_id'] is None

@pytest.mark.parametrize('raw_currency',[{}, {'currency':None}, {'currency':''}])
def test_tiki_mapper_does_not_guess_an_unobserved_currency(raw_currency):
    from ingestion.crawler.tiki import TikiCrawler
    row=TikiCrawler().standardize_product(dict(id=123,name='Observed fixture',price=10,**raw_currency))
    assert row['currency'] is None


def test_tiki_html_uses_observed_price_suffix_instead_of_an_unseen_vnd_default():
    html='<script type="application/ld+json">'+json.dumps({
        '@type':'Product','name':'Observed fixture','offers':{'price':'10.50 USD'}})+'</script>'
    row=extract_page_fields(html,'https://tiki.vn/fixture-p123.html')
    assert row['currency']=='USD'
    assert row['price_raw']=='10.50 USD'


def test_tiki_html_keeps_currency_unknown_when_price_has_no_unit():
    html='<script type="application/ld+json">'+json.dumps({
        '@type':'Product','name':'Observed fixture','offers':{'price':100}})+'</script>'
    assert extract_page_fields(html,'https://tiki.vn/fixture-p123.html')['currency'] is None

def test_tiki_category_can_come_from_observed_breadcrumbs_not_the_query_seed():
    html='<h1>Observed phone</h1><script type="application/ld+json">'+json.dumps({
        '@type':'BreadcrumbList','itemListElement':[
            {'name':'Home','item':'https://tiki.vn/'},
            {'name':'Observed phones','item':'https://tiki.vn/phone/c123'},
            {'name':'Observed phone','item':'https://tiki.vn/phone-p123.html'}]})+'</script>'
    assert extract_page_fields(html,'https://tiki.vn/phone-p123.html',category_seed='unrelated query')['category']=='Observed phones'


def test_tiki_visible_price_retains_its_observed_unit_and_raw_token():
    row=extract_page_fields('<h1>Observed phone</h1><div>250000 VND</div>','https://tiki.vn/phone-p123.html')
    assert row['currency']=='VND'
    assert row['price_raw']=='250000 VND'
