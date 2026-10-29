import datetime
from decimal import Decimal
import json
from pathlib import Path
import subprocess
import sys

import pandas as pd
import pytest

pytestmark = pytest.mark.unit


def test_snapshot_comparison_is_order_and_numeric_representation_independent():
    from scripts.compare_processing_snapshots import compare_table
    before = pd.DataFrame([
        {'date': '2026-10-01', 'product_id': 'p1', 'avg_price': Decimal('10.00'), 'run_id': 'old'},
        {'date': '2026-10-01', 'product_id': 'p2', 'avg_price': None, 'run_id': 'old'},
    ])
    after = pd.DataFrame([
        {'date': datetime.date(2026, 10, 1), 'product_id': 'p2', 'avg_price': float('nan'), 'run_id': 'new'},
        {'date': datetime.date(2026, 10, 1), 'product_id': 'p1', 'avg_price': 10.0, 'run_id': 'new'},
    ])
    report = compare_table(before, after, ['date', 'product_id'])
    assert report['status'] == 'equal'
    assert report['before']['rows'] == report['after']['rows'] == 2
    assert report['before']['content_sha256'] == report['after']['content_sha256']
    assert report['keys_added'] == report['keys_removed'] == report['rows_changed'] == 0


def test_comparison_reports_changed_metrics_new_removed_keys_and_schema():
    from scripts.compare_processing_snapshots import compare_table
    before = pd.DataFrame([
        {'product_id': 'p1', 'avg_rating': 4.0}, {'product_id': 'p2', 'avg_rating': 5.0},
    ])
    after = pd.DataFrame([
        {'product_id': 'p1', 'avg_rating': 3.0, 'rating_sample_size': 5},
        {'product_id': 'p3', 'avg_rating': 5.0, 'rating_sample_size': 1},
    ])
    report = compare_table(before, after, ['product_id'])
    assert report['status'] == 'changed'
    assert report['keys_added'] == report['keys_removed'] == report['rows_changed'] == 1
    assert report['columns_added'] == ['rating_sample_size']
    assert report['changed_columns'] == {'avg_rating': 1}
    assert report['added_key_examples'] == [['p3']]
    assert report['removed_key_examples'] == [['p2']]


@pytest.mark.parametrize('rows', [[{'product_id': 'p1'}, {'product_id': 'p1'}], [{'product_id': None}], [{'product_id': ''}]])
def test_comparison_rejects_ambiguous_or_missing_grain(rows):
    from scripts.compare_processing_snapshots import compare_table
    with pytest.raises(ValueError, match='grain'):
        compare_table(pd.DataFrame(rows), pd.DataFrame([{'product_id': 'p1'}]), ['product_id'])


def test_anomaly_diff_uses_event_identity_and_ignores_run_only_changes():
    from scripts.compare_processing_snapshots import compare_table
    before = pd.DataFrame([{'event_id': 'a1', 'score': 3.0, 'run_id': 'old'}, {'event_id': 'a2', 'score': 4.0, 'run_id': 'old'}])
    after = pd.DataFrame([{'event_id': 'a1', 'score': 3.0, 'run_id': 'new'}, {'event_id': 'a3', 'score': 5.0, 'run_id': 'new'}])
    report = compare_table(before, after, ['event_id'])
    assert report['keys_added'] == report['keys_removed'] == 1
    assert report['rows_changed'] == 0


def test_cli_only_writes_new_report_and_refuses_missing_table(tmp_path):
    before, after = tmp_path/'before', tmp_path/'after'
    before.mkdir(); after.mkdir()
    snapshot = pd.DataFrame([{'date': '2026-10-01', 'product_id': 'p1', 'avg_rating': 4.0}])
    for folder in [before, after]:
        snapshot.to_parquet(folder/'product_daily_stats.parquet', index=False)
    original = {p: p.read_bytes() for p in tmp_path.rglob('*.parquet')}
    script = Path(__file__).resolve().parents[1]/'scripts/compare_processing_snapshots.py'
    report = tmp_path/'report.json'
    command = [sys.executable, str(script), '--before', str(before), '--after', str(after),
               '--tables', 'product_daily_stats', '--output', str(report)]
    done = subprocess.run(command, capture_output=True, text=True)
    assert done.returncode == 0, done.stderr
    assert json.loads(report.read_text())['tables']['product_daily_stats']['status'] == 'equal'
    assert subprocess.run(command, capture_output=True).returncode != 0
    assert all(path.read_bytes() == value for path, value in original.items())
    bad = subprocess.run(command[:-3] + ['price_daily_stats', '--output', str(tmp_path/'missing.json')], capture_output=True)
    assert bad.returncode != 0
    assert not (tmp_path/'missing.json').exists()
