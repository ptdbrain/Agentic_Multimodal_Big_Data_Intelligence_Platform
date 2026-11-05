"""A rate ceiling includes acknowledgement time and uses a monotonic clock."""
from types import SimpleNamespace
import pytest
from kafka.producers.stream_producer import StreamProducer

pytestmark = pytest.mark.unit


class Clock:
    def __init__(self):
        self.now = 0.0

    def sleep(self, seconds):
        self.now += seconds


def producer_with_ack(monkeypatch, latency):
    import kafka.producers.stream_producer as module
    clock = Clock()
    monkeypatch.setattr(module, 'time', SimpleNamespace(time=lambda:10000 + clock.now,
                        perf_counter=lambda:clock.now,sleep=clock.sleep))
    producer = StreamProducer(enable_offline_buffer=True)
    original = producer.send_message

    def acknowledged(*args, **kwargs):
        result = original(*args, **kwargs)
        clock.sleep(latency)
        return result

    monkeypatch.setattr(producer, 'send_message', acknowledged)
    return producer, clock


def test_acknowledgement_time_is_part_of_interval_not_added_to_it(monkeypatch):
    producer, clock = producer_with_ack(monkeypatch, .002)
    sent, actual_rate = producer.produce_batch('raw.reviews', [{'review_id':str(i)} for i in range(10)], rate=100)
    assert sent == 10
    assert clock.now == pytest.approx(.1)
    assert actual_rate == pytest.approx(100)


def test_duration_window_stops_dispatch_even_if_acknowledgements_are_slower_than_target(monkeypatch):
    producer, clock = producer_with_ack(monkeypatch, .015)
    sent, actual_rate = producer.produce_batch('raw.reviews', [{'review_id':str(i)} for i in range(100)],
                                             rate=100,max_duration_sec=.05)
    assert sent == 4  # starts at 0, .015, .030, .045; last ack arrives at .060
    assert clock.now == pytest.approx(.06)
    assert actual_rate == pytest.approx(4/.06)
    assert [item['sent_perf_counter'] for item in producer.virtual_queue] == pytest.approx([0,.015,.03,.045])


@pytest.mark.parametrize('duration', [0, -1, float('nan'), float('inf')])
def test_invalid_window_is_rejected_before_dispatch(monkeypatch, duration):
    producer, _ = producer_with_ack(monkeypatch, .002)
    with pytest.raises(ValueError):
        producer.produce_batch('raw.reviews', [{'review_id':'1'}],rate=100,max_duration_sec=duration)
    assert producer.virtual_queue == []


def test_velocity_duration_limits_dispatch_and_reports_a_shortfall_when_delivery_is_slow(monkeypatch):
    import time
    from scripts.benchmark import benchmark_velocity
    original = StreamProducer.send_message

    def slow_ack(self, *args, **kwargs):
        result = original(self, *args, **kwargs)
        time.sleep(.01)
        return result

    monkeypatch.setattr(StreamProducer,'send_message',slow_ack)
    row = benchmark_velocity(rates=[200],effective_mode='test',duration_sec=.1,dirty_ratio=0)[0]
    assert row['messages_sent'] < 20  # Cannot sustain 200/s with a 10 ms ack.
    assert row['requested_duration_sec'] == .1
    assert row['messages_consumed'] == row['messages_sent']
    assert row['accounting_reconciled'] is True
    assert row['actual_produce_rate_mps'] < 110


def test_velocity_failure_stops_consumer_and_lag_monitor_threads(monkeypatch):
    import threading
    import time
    from scripts.benchmark import benchmark_velocity
    before = {thread.ident for thread in threading.enumerate()}

    def failed_delivery(*args, **kwargs):
        raise RuntimeError('Injected transport outage')

    monkeypatch.setattr(StreamProducer,'produce_batch',failed_delivery)
    with pytest.raises(RuntimeError,match='Injected transport outage'):
        benchmark_velocity(rates=[50],effective_mode='test',messages_per_rate=10)
    deadline = time.monotonic() + 1
    remaining = []
    while time.monotonic() < deadline:
        remaining = [thread for thread in threading.enumerate()
                     if thread.ident not in before and thread.name.startswith('velocity-')]
        if not remaining:
            break
        time.sleep(.01)
    assert remaining == []
