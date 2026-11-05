"""The local window helper must not invent clocks or average invalid ratings."""
import os
import time
import pytest
from spark.streaming.window_aggregations import WindowAggregator

pytestmark = pytest.mark.unit


def test_epoch_zero_is_preserved_in_an_envelope():
    rows = WindowAggregator.aggregate_tumbling_window([{'timestamp':0,'payload':{'rating':4}}])
    assert rows[0]['window_start'] == '1970-01-01T00:00:00+00:00'
    assert rows[0]['event_count'] == 1


def test_seconds_and_millisecond_encodings_resolve_to_the_same_window():
    rows = WindowAggregator.aggregate_tumbling_window([
        {'timestamp':value,'rating':4} for value in [1767225600,1767225600000,'1767225600000']])
    assert len(rows) == 1
    assert rows[0]['window_start'] == '2026-01-01T00:00:00+00:00'
    assert rows[0]['event_count'] == 3


def test_invalid_epochs_do_not_crash_or_generate_synthetic_windows():
    rows = WindowAggregator.aggregate_tumbling_window([
        {'timestamp':value,'rating':4} for value in [float('nan'),float('inf'),True,'NaN',1e300]] +
        [{'timestamp':'2026-01-01T00:00:00Z','rating':4}])
    assert len(rows) == 1 and rows[0]['event_count'] == 1


def test_naive_supported_clock_uses_the_explicit_utc_contract_on_any_host():
    previous = os.environ.get('TZ')
    try:
        os.environ['TZ'] = 'Asia/Ho_Chi_Minh'
        if hasattr(time,'tzset'):
            time.tzset()
        result = WindowAggregator.aggregate_tumbling_window([{'timestamp':'2026-01-01T00:00:00','rating':4}])
        assert result[0]['window_start'] == '2026-01-01T00:00:00+00:00'
    finally:
        if previous is None:
            os.environ.pop('TZ',None)
        else:
            os.environ['TZ'] = previous
        if hasattr(time,'tzset'):
            time.tzset()


def test_rating_mean_excludes_invalid_values_and_declares_its_sample_size():
    rows = WindowAggregator.aggregate_tumbling_window([
        {'timestamp':'2026-01-01T00:00:00Z','rating':value} for value in [4,float('nan'),float('inf'),True,6]])
    assert rows[0]['event_count'] == 5
    assert rows[0]['avg_rating'] == 4
    assert rows[0]['value_sample_size'] == 1


def test_custom_value_field_is_not_mislabelled_as_a_star_rating():
    row = WindowAggregator.aggregate_tumbling_window([
        {'timestamp':'2026-01-01T00:00:00Z','temperature':20},
        {'timestamp':'2026-01-01T00:00:20Z','temperature':22}],value_field='temperature')[0]
    assert row['avg_value'] == 21
    assert row['value_field'] == 'temperature'
    assert 'avg_rating' not in row
