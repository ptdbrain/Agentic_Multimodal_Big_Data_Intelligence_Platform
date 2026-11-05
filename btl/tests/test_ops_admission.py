import pytest

pytestmark = pytest.mark.unit


def test_maintenance_blocks_operational_metric_writes():
    from operations.status import OperationStatus
    from operations.backup import maintenance_guard

    with OperationStatus(mode="test") as status:
        with maintenance_guard("test"):
            with pytest.raises(RuntimeError, match="busy"):
                status.set_gauge("checkpoint_errors", 1)
        assert 'checkpoint_errors{pipeline="platform",source="system"} NaN' in status.metrics()


def test_empty_kafka_metadata_is_never_ready_for_failover():
    from scripts.verify_native_resilience import replicated

    assert not replicated({"partitions": []})
    assert not replicated({"partitions": [{"isr": [1, 2, 3]}]})
    assert replicated({"partitions": [{"isr": [1, 2, 3]}] * 3})
    assert not replicated({"partitions": [{"isr": [1, 2]}] * 3})


def test_maintenance_blocks_topic_lifecycle_writes():
    from operations.kafka_lifecycle import TopicRegistry
    from operations.backup import maintenance_guard

    registry = TopicRegistry("test")
    try:
        with maintenance_guard("test"):
            with pytest.raises(RuntimeError, match="busy"):
                registry.register("raw.articles.admission", "r", "web_content", "g")
        assert not registry.entries()
    finally:
        registry.close()

def test_batch_latency_uses_complete_same_binary_cohorts_only():
    from scripts.benchmark_operations import summarize,STAGES
    rows=[{'completed':True,'times_sec':dict.fromkeys(STAGES,n),'source_fingerprint':{'sha256':'code'}} for n in (1,2,3)]
    report=summarize(rows)
    assert report['individual_seconds']==[5,10,15]
    assert report['batch_latency_seconds']['p50']==10
    rows[1]['source_fingerprint']['sha256']='other'
    with pytest.raises(ValueError,match='different code'):summarize(rows)
    rows[1]['source_fingerprint']['sha256']='code'
    rows[0]['completed']=False
    with pytest.raises(ValueError,match='Incomplete'):summarize(rows)

def test_maintenance_blocks_registry_schema_initialization():
    from operations.backup import maintenance_guard
    from operations.kafka_lifecycle import TopicRegistry
    with maintenance_guard('test'):
        with pytest.raises(RuntimeError,match='busy'):
            TopicRegistry('test')
