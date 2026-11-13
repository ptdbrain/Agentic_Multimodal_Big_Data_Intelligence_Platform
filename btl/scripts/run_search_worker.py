"""Run the real generation-indexing outbox worker for every registered dataset."""

import argparse, logging, sys, time, os
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from serving.registry import DatasetRegistry
from serving.search import SearchWorker, Elasticsearch


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--registry", type=Path, required=True)
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--interval", type=float, default=5)
    args = parser.parse_args()
    if args.interval < 1:
        parser.error("Interval must be at least one second")
    request = Elasticsearch(os.environ.get("ES_URL")).request
    while True:
        failed = False
        for binding in DatasetRegistry.load(args.registry).datasets.values():
            try:
                SearchWorker(binding, request).run_once()
            except Exception as error:
                failed = True
                logging.error("Search dataset %s failed: %s", binding.id, type(error).__name__)
        if args.once:
            if failed:
                raise SystemExit(1)
            return
        time.sleep(args.interval)


if __name__ == "__main__":
    main()
