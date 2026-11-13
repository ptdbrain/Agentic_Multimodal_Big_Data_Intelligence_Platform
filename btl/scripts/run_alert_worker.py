"""Deliver committed statistical alert transitions from dataset outboxes."""

import argparse, logging, sys, time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from serving.registry import DatasetRegistry
from serving.notifications import deliver_notifications


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--registry", type=Path, required=True)
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args()
    while True:
        failed = False
        for binding in DatasetRegistry.load(args.registry).datasets.values():
            try:
                deliver_notifications(binding)
            except Exception as error:
                failed = True
                logging.error("Alert delivery for%s failed:%s", binding.id, type(error).__name__)
        if args.once:
            if failed:
                raise SystemExit(1)
            return
        time.sleep(5)


if __name__ == "__main__":
    main()
