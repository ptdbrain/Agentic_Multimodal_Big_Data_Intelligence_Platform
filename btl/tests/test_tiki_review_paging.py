"""Controlled HTTP fixtures test the observed API page bound, not live capture."""
import json
import pytest
import requests
from ingestion.crawler.tiki import TikiCrawler

pytestmark=pytest.mark.unit


@pytest.mark.parametrize('available,expected',[(100,45),(27,27)])
def test_requested_review_count_uses_bounded_pages_without_fabricating_missing_rows(tmp_path,monkeypatch,available,expected):
    crawler=TikiCrawler(use_polite_scraper=False,evidence_dir=tmp_path)
    calls=[]
    def bounded_server(method,url,**kwargs):
        params=kwargs['params'];calls.append(dict(params))
        response=requests.Response();response.url=url
        if params['limit']>20:
            response.status_code=400;data={'error':{'message':'Query limitation is: 20'}}
        else:
            response.status_code=200
            start=(params['page']-1)*params['limit']
            data={'data':[{'id':n+1,'content':'Explicit HTTP fixture'}
                          for n in range(start,min(start+params['limit'],available))]}
        response._content=json.dumps(data).encode()
        return response
    monkeypatch.setattr(crawler,'_request_with_retry',bounded_server)
    rows=crawler.get_product_reviews('tiki_123',limit=45)
    assert [row['id'] for row in rows]==list(range(1,expected+1))
    assert all(call['limit']==20 for call in calls)
    assert [call['page'] for call in calls]==list(range(1,len(calls)+1))
    assert all(row['_source_provenance']['evidence_sha256'] for row in rows)
