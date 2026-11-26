"""Deliver committed statistical alert transitions from dataset outboxes."""

import argparse, logging, os, sys, time
from contextlib import ExitStack, closing
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from serving.registry import DatasetRegistry
from serving.notifications import deliver_notifications


def worker_loop(args, price_source=None):
    while True:
        failed = False
        if price_source is not None:
            from scripts.run_price_alert_stream import consume_price_batch

            outcomes = consume_price_batch(*price_source, timeout_ms=200)
            if outcomes:
                logging.info("Kafka price batch outcomes: %s", dict(outcomes))
        for binding in DatasetRegistry.load(args.registry).datasets.values():
            try:
                deliver_notifications(binding)
            except Exception as error:
                failed = True
                logging.error("Alert delivery for %s failed: %s", binding.id, type(error).__name__)
        if args.once:
            if failed:
                raise SystemExit(1)
            return
        time.sleep(5)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--registry", type=Path, required=True)
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--price-dataset", default=os.environ.get("PRICE_ALERT_DATASET"), help="Optional dataset Kafka price topic")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)
    if not args.price_dataset:
        worker_loop(args)
        return
    from scripts.run_price_alert_stream import open_price_source
    from serving.admission import writer_transaction
    from serving.price_stream import initialize_price_stream
    from serving.store import ServingStore, connect_dataset

    binding = DatasetRegistry.load(args.registry).datasets[args.price_dataset]
    with ExitStack() as stack:
        consumer, source = open_price_source(binding)
        stack.enter_context(closing(consumer))
        connection = stack.enter_context(closing(connect_dataset(binding)))
        store = ServingStore(connection, binding.id, binding.source_group)
        with writer_transaction(connection):
            initialize_price_stream(store)
        # A price-processing error ends the process. Restarting rejoins from the
        # last committed offsets without accidentally advancing a failed batch.
        worker_loop(args, (store, consumer, source))


if __name__ == "__main__":
    main()
