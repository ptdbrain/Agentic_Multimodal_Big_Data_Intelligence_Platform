"""Delimited sources must keep identities and surface the original parse failure."""
import pandas as pd
import pytest
from ingestion.file_loader.loader import FileLoader

pytestmark = pytest.mark.unit


def test_malformed_csv_reports_parser_error_instead_of_retrying_unrelated_encodings(tmp_path):
    path = tmp_path / 'broken.csv'
    path.write_text('product_id,name\n1,one\n2,two,extra\n')
    with pytest.raises(pd.errors.ParserError):
        FileLoader.load_file(path, delimiter=',')


def test_explicit_encoding_does_not_silently_fall_back(tmp_path):
    path = tmp_path / 'latin.csv'
    path.write_bytes('product_id,name\n001,café\n'.encode('cp1252'))
    with pytest.raises(UnicodeDecodeError):
        FileLoader.load_file(path, encoding='utf-8', delimiter=',')
    records = FileLoader.load_records(path, encoding='cp1252', delimiter=',', anonymize_pii=False)
    assert records == [{'product_id':'001', 'name':'café'}]


def test_csv_identity_and_missing_optional_values_survive_transport_loading(tmp_path):
    path = tmp_path / 'source.csv'
    path.write_text('review_id,product_id,rating,review_title\n0001,0007,5,Good\n0002,0008,4,\n')
    rows = FileLoader.load_records(path, delimiter=',', anonymize_pii=False)
    assert rows == [{'review_id':'0001','product_id':'0007','rating':5,'review_title':'Good'},
                    {'review_id':'0002','product_id':'0008','rating':4}]
