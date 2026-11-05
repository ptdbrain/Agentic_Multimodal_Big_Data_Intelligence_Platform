"""Native Spark parsing and sinks must preserve provenance and observation clocks."""
import json
from datetime import datetime
import pytest
from spark.streaming.spark_streaming_job import SparkStreamingJob
from config.settings import settings

pytestmark = pytest.mark.spark


def payload():
    return {'review_id':'fixture-r','product_id':'fixture-p','rating':4,'review_text':'Explicit parser fixture',
            'review_date':'2026-01-01T00:00:00Z','source':'fixture_record',
            'source_group':'marketplace','source_name':'fixture','source_record_id':'upstream-r',
            'source_url':'https://source.example/review/1','acquisition_method':'snapshot_replay',
            'collected_at':'2026-01-02T00:00:00Z','evidence_sha256':'a'*64,'evidence_ref':'private-fixture-reference',
            'parser_version':'fixture-v1','schema_version':'1.0','collection_run_id':'fixture-run'}


def job(spark,tmp_path):
    return SparkStreamingJob(spark_session=spark,checkpoint_dir=str(tmp_path/'checkpoint'),
                             output_path=str(tmp_path/'metrics'),quarantine_path=str(tmp_path/'quarantine'))


def envelope(row):
    return {'event_id':'fixture-event','event_type':'NEW_REVIEW','source':'fixture_transport',
            'ingested_at':'2026-01-02T00:00:00Z','payload':row}


@pytest.mark.parametrize('shape',['kafka','payload','flat'])
def test_native_parser_retains_source_provenance_for_every_input_shape(spark_session,tmp_path,shape):
    row=payload()
    if shape=='kafka':
        raw=spark_session.createDataFrame([(json.dumps(envelope(row)),settings.kafka.topic_reviews)],'value string,topic string')
    elif shape=='payload':
        raw=spark_session.read.json(spark_session.sparkContext.parallelize([json.dumps(envelope(row))]))
    else:
        raw=spark_session.createDataFrame([row])
    processor=job(spark_session,tmp_path)
    valid,invalid=processor.validate_review_stream(processor.parse_review_stream(raw))
    observed=valid.first().asDict()
    for field in ['source','source_group','source_name','source_record_id','source_url','acquisition_method',
                  'collected_at','evidence_sha256','evidence_ref','parser_version','schema_version','collection_run_id']:
        assert observed.get(field) == row[field]
    assert invalid.count() == 0


def test_native_stream_does_not_replace_missing_observation_clock_with_ingestion_time(spark_session,tmp_path):
    row=payload();row.pop('review_date')
    raw=spark_session.createDataFrame([(json.dumps(envelope(row)),)],'value string')
    processor=job(spark_session,tmp_path)
    valid,invalid=processor.validate_review_stream(raw)
    assert valid.count() == 0
    assert invalid.first()['quarantine_reason'] == 'MISSING_EVENT_TIME'


def test_native_flat_source_does_not_invent_review_body(spark_session,tmp_path):
    row=payload();row.pop('review_text')
    processor=job(spark_session,tmp_path)
    valid,invalid=processor.validate_review_stream(spark_session.createDataFrame([row]))
    assert valid.count() == 0
    assert invalid.first()['quarantine_reason'] == 'MISSING_REVIEW_TEXT'


def test_contradictory_native_source_metadata_is_quarantined(spark_session,tmp_path):
    row=payload();row['acquisition_method']='generated'
    processor=job(spark_session,tmp_path)
    valid,invalid=processor.validate_review_stream(spark_session.createDataFrame([row]))
    assert valid.count() == 0
    assert invalid.first()['quarantine_reason'] == 'INVALID_SOURCE_METADATA'


def metric(spark,group):
    return spark.createDataFrame([(datetime(2026,1,1),datetime(2026,1,1,0,1),'fixture-p',group,1,4.0,0)],
        'window_start timestamp,window_end timestamp,product_id string,source_group string,review_count int,avg_rating double,negative_count int')


def test_native_sink_cannot_replace_a_simulation_generation_with_real_rows(spark_session,tmp_path):
    processor=job(spark_session,tmp_path)
    processor._upsert_window_metrics_batch(metric(spark_session,'simulation'),0)
    with pytest.raises(ValueError,match='source'):
        processor._upsert_window_metrics_batch(metric(spark_session,'marketplace'),1)
    assert spark_session.read.parquet(str(tmp_path/'metrics')).first()['source_group']=='simulation'


def test_native_sink_rejects_a_mixed_incoming_generation(spark_session,tmp_path):
    processor=job(spark_session,tmp_path)
    mixed=metric(spark_session,'simulation').unionByName(metric(spark_session,'marketplace'))
    with pytest.raises(ValueError,match='source'):
        processor._upsert_window_metrics_batch(mixed,0)
    assert not (tmp_path/'metrics').exists()


def test_native_quarantine_keeps_different_failed_payloads_but_deduplicates_retries(spark_session,tmp_path):
    processor=job(spark_session,tmp_path)
    def rejected(raw_rating,clock):
        return spark_session.createDataFrame([('same-event','same-review','INVALID_TYPE_RATING',raw_rating,
            'simulation',clock)],'event_id string,review_id string,quarantine_reason string,rating_raw string,source_group string,quarantined_at string')
    processor._write_quarantine_batch(rejected('bad-A','2026-01-01T00:00:00Z'),0)
    processor._write_quarantine_batch(rejected('bad-A','2026-01-01T00:01:00Z'),1)
    processor._write_quarantine_batch(rejected('bad-B','2026-01-01T00:02:00Z'),2)
    rows=spark_session.read.parquet(str(tmp_path/'quarantine')).collect()
    assert len(rows) == 2
    assert {row['rating_raw'] for row in rows} == {'bad-A','bad-B'}

@pytest.mark.parametrize('entry',['kafka','file'])
def test_flat_review_is_parsed_at_actual_json_entry_points(spark_session,tmp_path,entry):
    processor=job(spark_session,tmp_path)
    encoded=json.dumps(payload())
    if entry=='kafka':
        raw=spark_session.createDataFrame([(encoded,settings.kafka.topic_reviews)],'value string,topic string')
    else:
        folder=tmp_path/'input';folder.mkdir();(folder/'row.json').write_text(encoded)
        raw=processor.create_file_stream(str(folder))
    valid,invalid=processor.validate_review_stream(raw)
    if entry=='file':
        valid.writeStream.format('memory').queryName('flat_json_review').trigger(availableNow=True).start().awaitTermination()
        assert spark_session.table('flat_json_review').count()==1
    else:
        assert valid.count()==1
        assert invalid.count()==0


def test_selected_custom_kafka_topic_is_not_silently_filtered(spark_session,tmp_path):
    processor=job(spark_session,tmp_path)
    raw=spark_session.createDataFrame([(json.dumps(envelope(payload())),'custom-review-topic')],'value string,topic string')
    valid,invalid=processor.validate_review_stream(raw)
    assert valid.count()+invalid.count()==1
    assert valid.count()==1


def test_quarantine_identity_is_bound_before_projection_discards_fields(spark_session,tmp_path):
    processor=job(spark_session,tmp_path)
    rows=[]
    for user in ['fixture-A','fixture-B','fixture-A']:
        row=dict(payload(),rating=99,user_id=user,review_title='Explicit title')
        rows.append((json.dumps(envelope(row)),))
    raw=spark_session.createDataFrame(rows,'value string')
    _,invalid=processor.validate_review_stream(raw)
    processor._write_quarantine_batch(invalid,0)
    assert spark_session.read.parquet(str(tmp_path/'quarantine')).count()==2

def test_quarantine_keeps_repairable_payload_with_user_identifiers_sanitized(spark_session,tmp_path):
    row=dict(payload(),rating=99,user_id='fixture-clear-user',customer_email='fixture@example.invalid',
             review_title='Observed rejected title',verified_purchase=True)
    raw=spark_session.createDataFrame([(json.dumps(envelope(row)),)],'value string')
    processor=job(spark_session,tmp_path)
    _,invalid=processor.validate_review_stream(raw)
    observed=invalid.first().asDict()
    recovered=json.loads(observed['quarantine_payload_json'])['payload']
    assert recovered['review_title']==row['review_title']
    assert recovered['verified_purchase'] is True
    assert recovered['user_id'].startswith('anon_') and recovered['user_id']!=row['user_id']
    assert 'customer_email' not in recovered
    assert observed['raw_payload_sha256']

def test_quarantine_retry_identity_excludes_kafka_delivery_coordinates(spark_session,tmp_path):
    processor=job(spark_session,tmp_path)
    encoded=json.dumps(envelope(dict(payload(),rating=99)))
    raw=spark_session.createDataFrame([(encoded,'reviews-fixture',0,1),(encoded,'reviews-fixture',0,2)],
        'value string,topic string,partition int,offset long')
    _,invalid=processor.validate_review_stream(raw)
    processor._write_quarantine_batch(invalid,0)
    assert spark_session.read.parquet(str(tmp_path/'quarantine')).count()==1
