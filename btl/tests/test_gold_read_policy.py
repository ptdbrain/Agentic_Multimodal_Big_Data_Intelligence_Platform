from types import SimpleNamespace
import pandas as pd
import pytest
from spark.analytics.gold_builder import SparkGoldBuilder

pytestmark = pytest.mark.unit


class Reader:
    def __init__(self, error):
        self.error = error

    def parquet(self, path):
        raise self.error


def builder(error, **kwargs):
    return SparkGoldBuilder(spark=SimpleNamespace(read=Reader(error)), **kwargs)


def test_local_read_does_not_hide_permission_failure():
    with pytest.raises(PermissionError):
        builder(PermissionError('denied'))._ensure_spark_df('s3a://bucket/reviews', 'reviews')


def test_local_missing_path_requires_explicit_fallback():
    with pytest.raises(FileNotFoundError):
        builder(FileNotFoundError('absent'))._ensure_spark_df('s3a://bucket/reviews', 'reviews')


def test_explicit_local_fallback_returns_empty_missing_dataset(monkeypatch):
    from storage.storage_manager import storage
    monkeypatch.setattr(storage, 'read_silver_parquet', lambda _: pd.DataFrame())
    assert builder(FileNotFoundError('absent'), allow_local_fallback=True)._ensure_spark_df(object(), 'reviews') is None


def test_integration_rejects_local_fallback_opt_in(monkeypatch):
    monkeypatch.setenv('RUNTIME_MODE', 'integration')
    with pytest.raises(ValueError, match='local'):
        builder(FileNotFoundError('absent'), allow_local_fallback=True)


def test_price_mart_propagates_supplied_path_read_failure():
    with pytest.raises(PermissionError):
        builder(PermissionError('denied')).build_price_daily_stats('s3a://bucket/prices')
