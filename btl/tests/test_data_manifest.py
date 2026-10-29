import json
import pytest

pytestmark = pytest.mark.unit


def test_self_declared_source_does_not_verify_provenance(tmp_path):
    from scripts.verify_data_manifest import verify_manifest
    f=tmp_path/'reviews.json'
    f.write_text(json.dumps([{'review_id':'tiki_rv_1','source':'tiki_crawler'}]))
    import hashlib
    m=tmp_path/'manifest.json'
    m.write_text(json.dumps({'datasets':[{'dataset_id':'t','source_kind':'unverified','files':[{'path':'reviews.json','sha256':hashlib.sha256(f.read_bytes()).hexdigest()}]}]}))
    result=verify_manifest(m)
    assert result['content_verified'] is True
    assert result['datasets'][0]['provenance_status'] == 'unverified'


def test_format_comparison_detects_changed_values(tmp_path):
    import pandas as pd
    from scripts.sync_sample_formats import compare_formats
    a=tmp_path/'a.json'; b=tmp_path/'a.parquet'
    a.write_text('[{"product_id":"p1","price":100}]')
    pd.DataFrame([{'product_id':'p1','price':200}]).to_parquet(b,index=False)
    assert compare_formats([a,b],'product_id') is False
