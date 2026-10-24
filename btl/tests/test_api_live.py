import pytest
from ingestion.api.collector import APICollector

pytestmark = pytest.mark.unit


def test_live_requires_explicit_endpoint():
    with pytest.raises(ValueError,match='endpoint'):
        APICollector(mock_mode=False)


def test_live_never_falls_back_to_generated_data():
    class Session:
        def get(self,*args,**kw): raise ConnectionError('offline')
    with pytest.raises(ConnectionError):
        APICollector(endpoint_url='http://127.0.0.1:1', mock_mode=False, rate_limit_rps=0, session=Session()).fetch_page()


def test_live_pagination_passes_parameters():
    seen=[]
    class Response:
        status_code=200
        def raise_for_status(self): pass
        def json(self): return {'data':[{'id':'real'}],'page':2,'has_next':False}
    class Session:
        def get(self,url,**kw): seen.append(kw); return Response()
    result=APICollector(endpoint_url='https://source.test',session=Session(),rate_limit_rps=0).fetch_page(page=2,page_size=3,category='phones')
    assert result['data'] == [{'id':'real'}]
    assert seen[0]['params'] == {'page':2,'page_size':3,'category':'phones'}
    assert seen[0]['timeout'] > 0


@pytest.mark.integration
def test_live_http_pagination_with_real_local_server():
    import json,threading
    from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
    from urllib.parse import parse_qs,urlparse
    seen=[]
    class Handler(BaseHTTPRequestHandler):
        def log_message(self,*args): pass
        def do_GET(self):
            params=parse_qs(urlparse(self.path).query)
            seen.append(params)
            self.send_response(200); self.send_header('Content-Type','application/json'); self.end_headers()
            self.wfile.write(json.dumps({'data':[{'item_id':'from-http'}],'page':int(params['page'][0]),'has_next':False}).encode())
    server=ThreadingHTTPServer(('127.0.0.1',0),Handler)
    worker=threading.Thread(target=server.serve_forever,daemon=True);worker.start()
    try:
        result=APICollector(endpoint_url=f'http://127.0.0.1:{server.server_port}',rate_limit_rps=0).fetch_page(page=2,page_size=3,category='phones')
        assert result['data']==[{'item_id':'from-http'}]
        assert seen==[{'page':['2'],'page_size':['3'],'category':['phones']}]
    finally:
        server.shutdown();server.server_close();worker.join(timeout=2)
