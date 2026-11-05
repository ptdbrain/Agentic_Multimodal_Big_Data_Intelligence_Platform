"""Collect public articles or replay a labelled snapshot through serving."""
import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import sys
import uuid

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from ingestion.web_content import collect_articles
from orchestration.article_pipeline import run_article_pipeline


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--mode', choices=['integration', 'local', 'test'], default='integration')
    parser.add_argument('--input', type=Path, help='Existing article snapshot; explicitly labelled snapshot_replay')
    parser.add_argument('--seed-url', action='append', help='Public article index; defaults to Tinhte')
    parser.add_argument('--max-articles', type=int, default=50)
    parser.add_argument('--max-pages', type=int, default=150)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists():
        parser.error('Output already exists; historical reports must not be overwritten')
    if args.input and args.seed_url:
        parser.error('Choose snapshot input or live seeds')
    run_id = str(uuid.uuid4())
    if args.input:
        content = args.input.read_bytes()
        records = json.loads(content)
        if not isinstance(records, list) or not all(isinstance(row, dict) for row in records):
            parser.error('Snapshot must contain a JSON list of article records')
        records = [dict(row, acquisition_method='snapshot_replay',
                        original_acquisition_method=row.get('original_acquisition_method',row.get('acquisition_method')))
                   for row in records]
        collection = {'acquisition_method': 'snapshot_replay', 'source_path': str(args.input),
                      'sha256': hashlib.sha256(content).hexdigest(), 'articles_collected': len(records)}
    else:
        records, collection = collect_articles(args.seed_url or ['https://tinhte.vn/'],
            limit=args.max_articles, max_pages=args.max_pages,
            evidence_dir=ROOT.parent / '.runtime' / 'source-evidence' / run_id,collection_run_id=run_id)
    report = {'run_id': run_id, 'executed_at': datetime.now(timezone.utc).isoformat(), 'collection': collection,
              'mode': args.mode, 'passed': False}
    try:
        if len(records) < args.max_articles:
            raise RuntimeError(f'Only {len(records)}/{args.max_articles} articles collected; no acceptance claimed')
        report['pipeline'] = run_article_pipeline(records, mode=args.mode, run_id=run_id)
        report['passed'] = True
    except Exception as error:
        report['error_type'] = type(error).__name__
        raise
    finally:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        with args.output.open('x') as stream:
            json.dump(report, stream, ensure_ascii=False, indent=2)
            stream.write('\n')
    print(f'Article route PASS: {len(records)} records; report={args.output}')


if __name__ == '__main__':
    main()
