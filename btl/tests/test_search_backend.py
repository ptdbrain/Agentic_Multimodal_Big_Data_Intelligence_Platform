import json
import pytest
from storage.search_indexer import SearchIndexer

pytestmark = pytest.mark.unit


def test_elasticsearch_bulk_errors_are_not_reported_as_success():
    class Response:
        def raise_for_status(self): pass
        def json(self): return {'errors':True,'items':[{'index':{'status':400}}]}
    class Session:
        def request(self,*a,**kw): return Response()
    with pytest.raises(RuntimeError,match='bulk'):
        SearchIndexer(backend='elasticsearch',session=Session()).index_records([{'review_id':'r1'}])


def test_elasticsearch_search_uses_service_result():
    calls=[]
    class Response:
        def raise_for_status(self): pass
        def json(self): return {'hits':{'hits':[{'_source':{'review_id':'persisted'}}]}}
    class Session:
        def request(self,method,url,**kw): calls.append((method,url,kw)); return Response()
    result=SearchIndexer(backend='elasticsearch',session=Session()).search('phone',min_rating=4,limit=3)
    assert result == [{'review_id':'persisted'}]
    assert calls[0][2]['json']['size'] == 3
