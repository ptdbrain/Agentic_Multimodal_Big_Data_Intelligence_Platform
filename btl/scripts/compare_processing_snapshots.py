"""Read-only backfill comparison. No SQL connections, publication or data migration."""
import argparse
import datetime
from decimal import Decimal
import hashlib
import json
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

GRAINS = {
    'product_daily_stats': ['date', 'product_id'],
    'brand_daily_stats': ['date', 'brand'],
    'category_daily_stats': ['date', 'category'],
    'price_daily_stats': ['date', 'product_id'],
    'anomaly_events': ['event_id'],
}
IGNORED_METADATA = {'run_id', 'source_run_id', 'created_at', 'quarantined_at'}


def canonical_value(value: Any) -> Any:
    if isinstance(value, np.ndarray):
        value = value.tolist()
    if isinstance(value, (list, tuple)):
        return [canonical_value(item) for item in value]
    if isinstance(value, dict):
        return {str(key): canonical_value(item) for key, item in sorted(value.items())}
    if isinstance(value, np.generic):
        value = value.item()
    if value is None or pd.isna(value):
        return None
    if isinstance(value, (datetime.date, datetime.datetime, pd.Timestamp)):
        return value.isoformat()
    if isinstance(value, (int, float, Decimal)) and not isinstance(value, bool):
        number = Decimal(str(value))
        if not number.is_finite():
            raise ValueError('Snapshot contains a non-finite metric')
        return format(number.normalize(), 'f')
    return value


def _json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(',', ':'), allow_nan=False)


def _indexed(frame: pd.DataFrame, grain: list[str], columns: list[str]) -> dict:
    if set(grain) - set(frame.columns):
        raise ValueError(f'Missing grain columns: {sorted(set(grain) - set(frame.columns))}')
    result = {}
    for record in frame.to_dict(orient='records'):
        key = [canonical_value(record[name]) for name in grain]
        if any(value is None or (isinstance(value, str) and not value.strip()) for value in key):
            raise ValueError('Snapshot contains null/empty grain')
        encoded = _json(key)
        if encoded in result:
            raise ValueError('Snapshot contains duplicate grain')
        result[encoded] = {name: canonical_value(record[name]) for name in columns}
    return result


def compare_table(before: pd.DataFrame, after: pd.DataFrame, grain: list[str]) -> dict:
    """Compare business values on common columns; expose schema additions separately."""
    before_cols = set(before.columns) - IGNORED_METADATA
    after_cols = set(after.columns) - IGNORED_METADATA
    columns = sorted(before_cols & after_cols)
    left, right = _indexed(before, grain, columns), _indexed(after, grain, columns)
    added, removed = sorted(right.keys() - left.keys()), sorted(left.keys() - right.keys())
    changed = [key for key in sorted(left.keys() & right.keys()) if left[key] != right[key]]
    changed_columns = {name: sum(left[key][name] != right[key][name] for key in changed) for name in columns}

    def summary(index: dict) -> dict:
        digest = hashlib.sha256(_json({key: index[key] for key in sorted(index)}).encode()).hexdigest()
        return {'rows': len(index), 'unique_keys': len(index), 'content_sha256': digest}

    schema_changed = before_cols != after_cols
    return {
        'status': 'changed' if added or removed or changed or schema_changed else 'equal',
        'grain': grain, 'compared_columns': columns, 'ignored_metadata': sorted(IGNORED_METADATA),
        'before': summary(left), 'after': summary(right),
        'columns_added': sorted(after_cols - before_cols), 'columns_removed': sorted(before_cols - after_cols),
        'keys_added': len(added), 'keys_removed': len(removed), 'rows_changed': len(changed),
        'changed_columns': {name: count for name, count in changed_columns.items() if count},
        'added_key_examples': [json.loads(key) for key in added[:10]],
        'removed_key_examples': [json.loads(key) for key in removed[:10]],
        'changed_key_examples': [json.loads(key) for key in changed[:10]],
    }


def compare_snapshots(before: Path, after: Path, tables: list[str]) -> dict:
    report = {'mode': 'read_only_comparison', 'acceptance': 'requires_review',
              'before': str(before.resolve()), 'after': str(after.resolve()), 'tables': {}}
    for table in tables:
        if table not in GRAINS:
            raise ValueError(f'Unsupported analytical table: {table}')
        frames = []
        for folder in (before, after):
            # Accept pandas files and Spark directories, but never infer absence as zero rows.
            candidates = [path for path in [folder/f'{table}.parquet', folder/table] if path.exists()]
            if len(candidates) != 1:
                raise ValueError(f'Exactly one snapshot is required for {table} in {folder}')
            frames.append(pd.read_parquet(candidates[0]))
        report['tables'][table] = compare_table(*frames, GRAINS[table])
    report['status'] = 'equal' if all(item['status'] == 'equal' for item in report['tables'].values()) else 'changed'
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--before', type=Path, required=True)
    parser.add_argument('--after', type=Path, required=True)
    parser.add_argument('--tables', nargs='+', choices=list(GRAINS), default=list(GRAINS))
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    if any(args.output.resolve().is_relative_to(folder.resolve()) for folder in (args.before, args.after)):
        parser.error('Report output must be outside both snapshot directories')
    if args.output.exists():
        parser.error('Report already exists; choose a new output path')
    report = compare_snapshots(args.before, args.after, args.tables)
    with args.output.open('x', encoding='utf-8') as stream:
        json.dump(report, stream, ensure_ascii=False, indent=2, allow_nan=False)
        stream.write('\n')


if __name__ == '__main__':
    main()
