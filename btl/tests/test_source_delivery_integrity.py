"""Mixed live schemas must not acquire NaN fields or hide DLQ losses."""
import json
import pytest
from ingestion.file_loader.loader import FileLoader
from scripts.run_e2e_pipeline import stage_2_kafka_produce_and_consume

pytestmark = pytest.mark.unit


@pytest.mark.parametrize('suffix', ['.json', '.jsonl'])
def test_json_records_preserve_absent_optional_fields_and_integer_types(tmp_path, suffix):
    records = [{'id':9007199254740993,'name':'A','optional':'present'}, {'id':1,'name':'B'}]
    path = tmp_path / ('source' + suffix)
    path.write_text(json.dumps(records) if suffix == '.json' else '\n'.join(map(json.dumps, records)))
    assert FileLoader.load_records(path, anonymize_pii=False) == records


def test_source_contract_failures_cannot_disappear_from_quality_gate():
    good = {'product_id':'p1','product_name':'Observed','brand':'A','category':'Phone','price':1,'currency':'VND','source':'synthetic_fixture'}
    bad = dict(good, product_id='p2', price=None)
    review = {'review_id':'r1','product_id':'p1','user_id':'anon_redacted','rating':5,
              'review_title':'Observed','review_text':'Observed text','review_date':'2026-11-01'}
    with pytest.raises(RuntimeError, match='Source contract'):
        stage_2_kafka_produce_and_consume([good,bad], [review], [], effective_mode='test')


def test_equal_transport_counts_cannot_substitute_a_different_payload(monkeypatch):
    from kafka.producers.stream_producer import StreamProducer
    original = StreamProducer.send_message

    # Inject corruption at the transport seam; run real envelopes, validation,
    # Bronze writes, counters and the source-content gate below it.
    def altered(self, topic, key, value, **kwargs):
        if kwargs.get('event_type') == 'NEW_PRODUCT':
            value = dict(value, product_name='Stale other observation')
        return original(self, topic, key, value, **kwargs)

    monkeypatch.setattr(StreamProducer, 'send_message', altered)
    product = {'product_id':'p1','product_name':'Expected observation','brand':'A','category':'Phone',
               'price':1,'currency':'VND','source':'synthetic_fixture'}
    review = {'review_id':'r1','product_id':'p1','user_id':'anon_redacted','rating':5,
              'review_title':'Observed','review_text':'Observed text','review_date':'2026-11-01'}
    with pytest.raises(RuntimeError, match='content'):
        stage_2_kafka_produce_and_consume([product], [review], [], effective_mode='test')


def test_source_content_accounts_for_durable_dlq_without_hiding_accepted_rows():
    products = [{'product_id':f'p{i}','product_name':'Observed','brand':'A','category':'Phone',
                 'price':1,'currency':'VND','source':'synthetic_fixture'} for i in range(10)]
    products.append(dict(products[0], product_id='bad', price=None))
    review = {'review_id':'r1','product_id':'p0','user_id':'anon_redacted','rating':5,
              'review_title':'Observed','review_text':'Observed text','review_date':'2026-11-01'}
    result = stage_2_kafka_produce_and_consume(products, [review], [], effective_mode='test')
    assert result['records_valid'] == 11
    assert result['records_dlq'] == 1
    assert result['source_contracts']['products']['content_verified'] is True
