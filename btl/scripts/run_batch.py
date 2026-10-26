"""SentinelAI Batch Pipeline CLI Runner.
Synchronized with the canonical Spark Batch -> Silver / Quarantine / DQ -> Spark Gold -> Warehouse -> Dashboard path
defined in scripts/run_e2e_pipeline.py. Zero Pandas ETL or GoldAggregator fallback paths.
"""
import sys
import argparse
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.run_e2e_pipeline import (
    configure_pipeline_mode,
    run_end_to_end,
    stage_4_spark_batch_silver_dq,
    stage_5_spark_gold_marts,
    stage_6_warehouse_and_dashboard,
)


from orchestration.run_manifest import canonical_writer


@canonical_writer
def run_from_bronze(mode=None):
    effective_mode=configure_pipeline_mode(mode)
    s4=stage_4_spark_batch_silver_dq(batches_by_topic=None,include_prices=True)
    metrics=[s4["etl_res"][key] for key in ("products_dq","reviews_dq","prices_dq") if s4["etl_res"].get(key)]
    stage_5_spark_gold_marts(dq_metrics=metrics,run_id=s4["etl_res"]["run_id"],required_datasets=["products","reviews","prices"])
    return stage_6_warehouse_and_dashboard(effective_mode=effective_mode)


def main():
    parser = argparse.ArgumentParser(description="SentinelAI Canonical Batch Pipeline Runner")
    parser.add_argument(
        "--mode",
        choices=["integration", "local", "test"],
        default=None,
        help="Execution mode: 'integration' (default) or 'local'/'test'"
    )
    parser.add_argument("--products", default=None, help="Path to products source file")
    parser.add_argument("--reviews", default=None, help="Path to reviews source file")
    parser.add_argument("--prices", default=None, help="Path to prices source file")
    parser.add_argument(
        "--from-bronze",
        action="store_true",
        help="Run Stage 4-6 directly from existing Bronze layer on MinIO/storage"
    )
    args = parser.parse_args()

    try:
        if args.from_bronze:
            run_from_bronze(mode=args.mode)
        else:
            ok = run_end_to_end(
                products_path=args.products,
                reviews_path=args.reviews,
                prices_path=args.prices,
                mode=args.mode
            )
            if not ok:
                sys.exit(1)
    except Exception as exc:
        print(f"\n[BATCH EXECUTION FAILED] {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
