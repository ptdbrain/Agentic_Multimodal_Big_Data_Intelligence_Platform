"""Publish the verified retail daily mart from staged Kafka and Spark receipts."""

import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from serving.retail import publish_verified_mart


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("archive_receipt", type=Path)
    parser.add_argument("spark_receipt", type=Path)
    args = parser.parse_args()
    archive = json.loads(args.archive_receipt.read_text(encoding="utf-8"))
    reconciled = json.loads(args.spark_receipt.read_text(encoding="utf-8"))
    publish_verified_mart(archive, reconciled)
    print(f"Published retail generation {archive['run_id']} with {archive['kafka_acks']} transaction lines")


if __name__ == "__main__":
    main()
