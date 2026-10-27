from types import SimpleNamespace

import pandas as pd
import pytest

from spark.analytics.gold_builder import SparkGoldBuilder
from spark.quality.spark_dq import DataQualityGateError
from storage.storage_manager import storage

pytestmark = pytest.mark.unit


@pytest.mark.parametrize('mode', ['production', 'integration'])
def test_direct_gold_call_requires_current_run_in_service_modes(mode, monkeypatch):
    monkeypatch.setenv('RUNTIME_MODE', mode)
    with pytest.raises(DataQualityGateError, match='run_id'):
        SparkGoldBuilder(spark=SimpleNamespace()).build_all([], [], [], save_gold=False)


@pytest.mark.parametrize('mode', ['production', 'integration'])
def test_direct_gold_call_cannot_disable_quality_gate_in_service_modes(mode, monkeypatch):
    monkeypatch.setenv('RUNTIME_MODE', mode)
    monkeypatch.setattr(storage, 'read_gold_parquet', lambda _: pd.DataFrame())
    with pytest.raises(DataQualityGateError):
        SparkGoldBuilder(spark=SimpleNamespace()).build_all(
            [], [], [], save_gold=False, run_id='current-run', enforce_dq_gate=False,
        )


def test_single_metric_dict_cannot_bypass_run_id_requirement():
    builder = SparkGoldBuilder(spark=SimpleNamespace())
    with pytest.raises(DataQualityGateError, match='run_id'):
        builder.build_all([], [], [], save_gold=False, dq_metrics={
            'dataset_name': 'reviews', 'run_id': 'old-run', 'records_received': 10,
            'dq_score': 100, 'records_duplicate': 0, 'records_missing': 0,
        }, required_datasets=['reviews'])
