"""Plan identity migration from retained raw batches without modifying the lake."""
import argparse
from collections import defaultdict
import json
from pathlib import Path
import sys
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ingestion.identity import make_event_id, make_price_id


def plan_migration(records: list[Any]) -> dict[str, Any]:
    price_mappings = []
    event_mappings = []
    unresolved = []
    legacy = defaultdict(set)

    for index, raw in enumerate(records):
        if not isinstance(raw, dict):
            unresolved.append({'index': index, 'reason': 'Raw record is not an object'})
            continue
        payload = raw.get('payload', raw)
        if not isinstance(payload, dict):
            unresolved.append({'index': index, 'reason': 'Payload is not an object'})
            continue
        payload = dict(payload)
        if 'price_id' in payload:
            try:
                for field in ('price_id', 'source', 'product_id', 'timestamp'):
                    if not isinstance(payload.get(field), str) or not payload[field].strip():
                        raise ValueError(f'Missing or invalid {field}; identity cannot be inferred')
                old = payload['price_id']
                new = make_price_id(
                    payload['source'], payload['product_id'],
                    str(payload.get('seller', '')), payload['timestamp'],
                )
                price_mappings.append({
                    'old_id': old, 'new_id': new, 'observation_time': payload['timestamp'],
                })
                legacy[old].add(new)
                payload['price_id'] = new
            except (KeyError, ValueError, TypeError) as exc:
                unresolved.append({'index': index, 'reason': str(exc)})
                continue
        if isinstance(raw.get('payload'), dict):
            try:
                new = make_event_id(raw['event_type'], raw['source'], payload)
                event_mappings.append({'old_id': raw.get('event_id'), 'new_id': new})
            except (KeyError, ValueError, TypeError) as exc:
                unresolved.append({'index': index, 'reason': str(exc)})

    return {
        'price_mappings': price_mappings,
        'event_mappings': event_mappings,
        'legacy_collisions': sorted(old for old, ids in legacy.items() if len(ids) > 1),
        'unresolved_count': len(unresolved),
        'unresolved': unresolved,
        'apply_policy': 'Rebuild and reconcile in a new namespace; this tool never overwrites existing data',
    }


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('batch', nargs='+')
    parser.add_argument('--output', help='Create a new report; existing files are never overwritten')
    args = parser.parse_args(argv)
    records = []
    for name in args.batch:
        path = Path(name)
        content = path.read_text(encoding='utf-8')
        if path.suffix.lower() == '.jsonl':
            value = [json.loads(line) for line in content.splitlines() if line.strip()]
        else:
            value = json.loads(content)
        records.extend(value if isinstance(value, list) else [value])
    report = json.dumps(plan_migration(records), ensure_ascii=False, indent=2)
    if args.output:
        with Path(args.output).open('x', encoding='utf-8') as stream:
            stream.write(report + '\n')
    else:
        print(report)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
