"""SentinelAI Bootstrap Script.
One-command script to initialize sample datasets (if missing) and execute
the canonical 6-stage End-to-End pipeline via scripts/run_e2e_pipeline.py.
"""
import sys
import json
import argparse
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.run_e2e_pipeline import run_end_to_end


def ensure_sample_datasets(regenerate: bool = False) -> Path:
    """Ensures canonical sample datasets exist in data/sample without overwriting deterministic data unless requested."""
    sample_dir = REPO_ROOT / "data" / "sample"
    sample_dir.mkdir(parents=True, exist_ok=True)

    prods_file = sample_dir / "products.json"
    revs_file = sample_dir / "reviews.json"
    prices_file = sample_dir / "prices.json"

    if regenerate or not prods_file.exists() or not revs_file.exists():
        from data.generator import generate_products, generate_reviews, generate_prices
        products = generate_products()
        reviews = generate_reviews(products, count=500)
        with open(prods_file, "w", encoding="utf-8") as f:
            json.dump(products, f, ensure_ascii=False, indent=2)
        with open(revs_file, "w", encoding="utf-8") as f:
            json.dump(reviews, f, ensure_ascii=False, indent=2)
        if hasattr(sys.modules["data.generator"], "generate_prices") and (regenerate or not prices_file.exists()):
            prices = generate_prices(products)
            with open(prices_file, "w", encoding="utf-8") as f:
                json.dump(prices, f, ensure_ascii=False, indent=2)
        print(f"[Bootstrap] Generated sample datasets in {sample_dir}")
    else:
        print(f"[Bootstrap] Using existing sample datasets in {sample_dir}")

    return sample_dir


def main():
    parser = argparse.ArgumentParser(description="SentinelAI Pipeline Bootstrap")
    parser.add_argument(
        "--mode",
        choices=["integration", "local", "test"],
        default=None,
        help="Execution mode: 'integration' (default) or 'local'/'test'"
    )
    parser.add_argument(
        "--regenerate-sample",
        action="store_true",
        help="Regenerate synthetic sample data before running the pipeline"
    )
    args = parser.parse_args()

    print("=" * 60)
    print("  SENTINELAI PIPELINE BOOTSTRAP (CANONICAL E2E PATH)")
    print("=" * 60)

    sample_dir = ensure_sample_datasets(regenerate=args.regenerate_sample)

    try:
        ok = run_end_to_end(
            products_path=str(sample_dir / "products.json"),
            reviews_path=str(sample_dir / "reviews.json"),
            prices_path=str(sample_dir / "prices.json"),
            mode=args.mode
        )
        if not ok:
            sys.exit(1)
    except Exception as exc:
        print(f"\n[BOOTSTRAP FAILED] {exc}", file=sys.stderr)
        sys.exit(1)

    print("\n" + "=" * 60)
    print("  BOOTSTRAP COMPLETE! You can now start the dashboard:")
    print("  streamlit run dashboard/app.py")
    print("=" * 60)


if __name__ == "__main__":
    main()
