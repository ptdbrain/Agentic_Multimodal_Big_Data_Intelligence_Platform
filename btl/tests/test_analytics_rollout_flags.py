import json
import os
from pathlib import Path
import subprocess
import sys
from types import SimpleNamespace

import pandas as pd
import pytest

from config.settings import settings
from spark.analytics.gold_builder import SparkGoldBuilder
from spark.etl.gold_aggregator import GoldAggregator


@pytest.mark.unit
@pytest.mark.parametrize(('strict', 'fallback'), [('true', 'false'), ('false', 'true')])
def test_analytics_flags_are_read_from_runtime_environment(strict, fallback):
    env = dict(os.environ, STRICT_ANALYTICS_TIME=strict, ALLOW_LOCAL_ANALYTICS_FALLBACK=fallback)
    completed = subprocess.run([
        sys.executable, '-c',
        'import json; from config.settings import settings; '
        'print(json.dumps([settings.analytics.strict_analytics_time, settings.analytics.allow_local_analytics_fallback]))',
    ], cwd=Path(__file__).resolve().parents[1], env=env, text=True, capture_output=True, check=True)
    assert json.loads(completed.stdout) == [strict == 'true', fallback == 'true']


@pytest.mark.unit
def test_environment_local_fallback_opt_in_cannot_enable_integration(monkeypatch):
    monkeypatch.setattr(settings.analytics, 'allow_local_analytics_fallback', True, raising=False)
    monkeypatch.setenv('RUNTIME_MODE', 'integration')
    with pytest.raises(ValueError, match='local'):
        SparkGoldBuilder(spark=SimpleNamespace())


@pytest.mark.parametrize('engine', ['pandas', pytest.param('spark', marks=pytest.mark.spark)])
def test_strict_time_excludes_ingestion_only_rows_and_compatibility_is_explicit(engine, request, monkeypatch):
    reviews = pd.DataFrame([{
        'product_id': 'p1', 'review_id': 'r1', 'rating': 5.0, 'ingested_at': '2026-10-01',
    }])
    products = pd.DataFrame([{'product_id': 'p1', 'brand': 'A', 'category': 'Phone', 'price': 10.0}])

    def result():
        if engine == 'pandas':
            marts = GoldAggregator.build_gold_marts(products, reviews, save_to_storage=False)
            return marts.get('product_daily_stats', pd.DataFrame())
        spark = request.getfixturevalue('spark_session')
        return SparkGoldBuilder(spark).build_all(reviews, products, [], save_gold=False)['product_daily_stats'].toPandas()

    # Safe default is verified in a fresh process above; both paths must obey the same flag.
    monkeypatch.setattr(settings.analytics, 'strict_analytics_time', True, raising=False)
    assert len(result()) == 0
    monkeypatch.setattr(settings.analytics, 'strict_analytics_time', False)
    compatible = result()
    assert len(compatible) == 1
    assert not compatible.iloc[0]['time_available']
    assert pd.isna(compatible.iloc[0]['data_as_of'])
