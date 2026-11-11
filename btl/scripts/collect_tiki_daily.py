"""Daily Tiki capture CLI, run by the sentinel-tiki-capture systemd user timer.

  capture         observe the frozen watchlist once for a day (network only; services may be down)
  ingest-pending  replay collected, not-yet-loaded days through the canonical pipeline (needs the stack)
  status          one line per day: collected, partial or failed, and where it was loaded

Captures default to storage/captures/tiki (override with --root or TIKI_CAPTURE_DIR).
Exit code 1 means the day was not collected, so the timer's Restart=on-failure retries later.
"""
import argparse
import json
import os
import sys
from datetime import date
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ingestion import daily_capture  # noqa: E402

DEFAULT_ROOT = REPO_ROOT / 'storage' / 'captures' / 'tiki'
EX_TEMPFAIL = 75


def tiki_crawler_factory(delay_seconds: float):
    def factory(run_id, evidence_dir):
        from ingestion.crawler.tiki import TikiCrawler
        return TikiCrawler(collection_run_id=run_id, evidence_dir=evidence_dir,
                           delay_seconds=delay_seconds, rate_limit_rps=1.0 / delay_seconds)
    return factory


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    commands = parser.add_subparsers(dest='command', required=True)
    capture = commands.add_parser('capture', help='Observe the watchlist once for a day')
    capture.add_argument('--day', type=date.fromisoformat, default=None, help='YYYY-MM-DD (default: today)')
    capture.add_argument('--query', default='iphone', help='Search used only to create the first watchlist')
    capture.add_argument('--max-products', type=int, default=20)
    capture.add_argument('--reviews-per-product', type=int, default=10, help='Newest reviews per product')
    capture.add_argument('--delay', type=float, default=8.0, help='Seconds between requests (minimum 2)')
    ingest = commands.add_parser('ingest-pending', help='Replay collected days through the pipeline')
    ingest.add_argument('--mode', choices=['integration', 'local', 'test'], default=None)
    ingest.add_argument('--dry-run', action='store_true', help='List pending days without running anything')
    status = commands.add_parser('status', help='List captured days')
    for command in (capture, ingest, status):
        command.add_argument('--root', type=Path, default=None, help='Capture directory')
    return parser


def main(argv=None, crawler_factory=None, runner=None) -> int:
    parser = _parser()
    args = parser.parse_args(argv)
    root = args.root or Path(os.environ.get('TIKI_CAPTURE_DIR') or DEFAULT_ROOT)

    if args.command == 'capture':
        if args.delay < 2.0:
            parser.error('--delay must be at least 2 seconds')
        if args.max_products < 1 or args.reviews_per_product < 0:
            parser.error('--max-products must be positive and --reviews-per-product non-negative')
        try:
            manifest = daily_capture.capture_day(
                root, args.day or date.today(), crawler_factory or tiki_crawler_factory(args.delay),
                query=args.query, max_products=args.max_products, reviews_per_product=args.reviews_per_product)
        except daily_capture.CaptureBusy as error:
            print(f'[tiki-capture] busy: {error}')
            return EX_TEMPFAIL
        summary = {key: manifest.get(key) for key in ('counts', 'missing', 'error', 'error_message')}
        print(f"[tiki-capture] {manifest['day']} {manifest['status']} "
              f"run={manifest.get('collection_run_id') or manifest.get('existing_run_id')} "
              f"{json.dumps(summary, ensure_ascii=False)}")
        return 1 if manifest['status'] == 'failed' else 0

    if args.command == 'ingest-pending':
        from config.settings import settings
        mode = args.mode or settings.runtime_mode
        if args.dry_run:
            for _, manifest in daily_capture.pending_days(root, mode):
                print(f"[tiki-ingest] pending {manifest['day']} {manifest['status']} {json.dumps(manifest['counts'])}")
            return 0
        if runner is None:
            from scripts.run_e2e_pipeline import run_end_to_end as runner
        for row in daily_capture.ingest_pending(root, mode, runner):
            print(f"[tiki-ingest] loaded {row['day']} into {row['mode']} {json.dumps(row['counts'])}")
        return 0

    for row in daily_capture.capture_status(root):
        print(json.dumps(row, ensure_ascii=False))
    return 0


if __name__ == '__main__':
    sys.exit(main())
