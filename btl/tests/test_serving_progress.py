import pytest

pytestmark = pytest.mark.unit


def test_stream_heartbeat_waits_for_every_sink_at_source_frontier():
    from serving.progress import committed_serving_progress

    def progress(offset):
        return {
            "batchId": 1,
            "sources": [{"description": "KafkaSource[reviews]", "endOffset": {"reviews": {"0": offset}}}],
        }

    window = progress(10)
    assert committed_serving_progress(window, [progress(9)]) is None
    assert committed_serving_progress(window, [{}]) is None
    assert committed_serving_progress(window, [progress(10)]) == window
    assert committed_serving_progress(window, [progress(11), progress(10)]) == window
