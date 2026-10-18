"""Small, polite Tiki product, price & genuine review snapshot crawler for SentinelAI.

Collects public product catalog specs, price observations over time, and genuine
customer reviews directly via Tiki's public review endpoint (complying with robots.txt
and rate limits >= 2.0s). Never simulates or invents fake customer feedback.
Standardizes data into SentinelAI canonical schemas (raw.products, raw.prices, raw.reviews)
and enforces strict pre-Spark contract validation.
"""
from __future__ import annotations

import argparse
import csv
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

# Configure UTF-8 encoding on Windows terminal
if sys.stdout and hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
if sys.stderr and hasattr(sys.stderr, "reconfigure"):
    try:
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

REPO_ROOT = Path(__file__).resolve().parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ingestion.crawler.tiki import (
    DEFAULT_SEEDS,
    PoliteTikiCrawler,
    TikiCrawler,
    extract_page_fields,
    utc_now,
)
from ingestion.crawler.validator import CrawlerDataValidator, validate_crawler_payloads
from ingestion.privacy import sanitize_record_pii


def write_jsonl(path: Path, records: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record, ensure_ascii=False) + "\n")


def write_csv(path: Path, records: List[Dict[str, Any]], fieldnames: List[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8-sig") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(records)


def main() -> int:
    parser = argparse.ArgumentParser(description="SentinelAI Polite Tiki Product, Price & Review Crawler")
    parser.add_argument("--max-products", type=int, default=8, help="Maximum products per category (default: 8)")
    parser.add_argument(
        "--reviews-per-product",
        type=int,
        default=3,
        help="Maximum genuine customer reviews to fetch per product (default: 3, 0 to disable)",
    )
    parser.add_argument("--delay", type=float, default=2.0, help="Minimum seconds between requests (never below 2.0s)")
    parser.add_argument("--output-dir", default="data/external/tiki", help="Output directory for snapshot files")
    parser.add_argument(
        "--categories",
        nargs="+",
        choices=sorted(DEFAULT_SEEDS.keys()),
        default=sorted(DEFAULT_SEEDS.keys()),
        help="Categories to crawl",
    )
    parser.add_argument(
        "--validate-pre-spark",
        action="store_true",
        default=True,
        help="Strictly validate field names, types, IDs, prices, and reviews before exiting",
    )
    parser.add_argument(
        "--save-datalake",
        action="store_true",
        default=False,
        help="Save raw batches into Bronze Data Lake (sentinel-data/bronze/)",
    )
    parser.add_argument(
        "--push-kafka",
        action="store_true",
        default=False,
        help="Produce live events to Kafka topics raw.products, raw.prices, and raw.reviews",
    )

    args = parser.parse_args()
    if args.max_products < 1:
        parser.error("--max-products must be at least 1")

    effective_delay = max(2.0, args.delay)
    print(f"[*] Initializing Polite Tiki Crawler (delay={effective_delay:.1f}s, seeds={args.categories})...")

    crawler = TikiCrawler(delay_seconds=effective_delay, allow_public_fallback=False)
    polite = crawler.polite_crawler

    all_products: List[Dict[str, Any]] = []
    all_prices: List[Dict[str, Any]] = []
    all_reviews: List[Dict[str, Any]] = []
    all_unparsed: List[Dict[str, str]] = []

    price_counter = 1
    for category in args.categories:
        seed_url = DEFAULT_SEEDS[category]
        print(f"\n[{category.upper()}] Crawling listing from: {seed_url}")
        links = polite.discover_product_links(seed_url, max_products=args.max_products)
        print(f"[{category}] Discovered {len(links)} product links.")

        for idx, url in enumerate(links, start=1):
            print(f"  [{idx}/{len(links)}] Fetching: {url}")
            try:
                html = polite.fetch_html(url)
                raw_extracted = extract_page_fields(html, url, category_seed=category)
                if not raw_extracted.get("product_name") or raw_extracted.get("price") is None:
                    all_unparsed.append({
                        "product_url": url,
                        "category_seed": category,
                        "reason": "missing_product_name_or_price",
                        "collected_at": utc_now(),
                    })
                    print("     -> Skipped: missing product_name or price.")
                    continue

                # Standardize to raw.products and raw.prices
                std_prod = crawler.standardize_product(raw_extracted)
                std_price = crawler.standardize_price(std_prod, index=price_counter)
                price_counter += 1

                all_products.append(std_prod)
                all_prices.append(std_price)
                print(
                    f"     -> [OK] {std_prod['product_id']}: {std_prod['product_name'][:50]}... "
                    f"| {std_prod['price']:,.0f} {std_prod['currency']} | Rating: {std_prod['rating']}"
                )

                # Fetch genuine customer reviews directly via public reviews endpoint
                clean_id = std_prod["product_id"].replace("tiki_", "")
                if args.reviews_per_product > 0 and clean_id.isdigit():
                    raw_revs = polite.fetch_product_reviews(clean_id, limit=args.reviews_per_product, page=1)
                    prod_revs = []
                    for r in raw_revs:
                        std_rev = sanitize_record_pii(
                            crawler.standardize_review(r, product_id=std_prod["product_id"]),
                            remove_user_id=crawler.remove_user_id,
                        )
                        prod_revs.append(std_rev)
                    all_reviews.extend(prod_revs)
                    print(
                        f"     -> [Reviews] Fetched {len(prod_revs)} genuine customer reviews "
                        f"(0 simulated, PII anonymized)."
                    )
            except Exception as exc:
                print(f"     -> Error fetching/parsing {url}: {exc}")
                all_unparsed.append({
                    "product_url": url,
                    "category_seed": category,
                    "reason": str(exc),
                    "collected_at": utc_now(),
                })

    print("\n" + "=" * 60)
    print("      PRE-SPARK DATA VALIDATION & CONTRACT AUDIT")
    print("=" * 60)
    if args.validate_pre_spark and all_products:
        report = CrawlerDataValidator.validate_crawled_dataset(
            products=all_products,
            prices=all_prices,
            reviews=all_reviews,
            enforce_tiki_id_format=True,
        )
        print(f"[*] Validation Status: {'PASSED' if report['is_valid'] else 'FAILED'}")
        print(f"    - Total Products Checked: {report['stats']['products_total']}")
        print(f"    - Valid Products:         {report['stats']['products_valid']}")
        print(f"    - Invalid Products:       {report['stats']['products_invalid']}")
        print(f"    - Total Prices Checked:   {report['stats']['prices_total']}")
        print(f"    - Valid Prices:           {report['stats']['prices_valid']}")
        print(f"    - Invalid Prices:         {report['stats']['prices_invalid']}")
        print(f"    - Total Reviews Checked:  {report['stats']['reviews_total']}")
        print(f"    - Valid Reviews:          {report['stats']['reviews_valid']}")
        print(f"    - Invalid Reviews:        {report['stats']['reviews_invalid']}")

        if report["errors"]:
            print(f"[*] Validation Errors ({len(report['errors'])}):")
            for err in report["errors"][:10]:
                print(f"    ! {err}")

        if not report["is_valid"]:
            print("[CRITICAL] Pre-Spark validation guard failed. Refusing to forward invalid data downstream.")
            return 1
        print("[SUCCESS] All product IDs, names, numeric prices, genuine reviews, and schemas passed verification!")

    # Output persistence
    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    prod_jsonl = out_dir / f"tiki_products_snapshot_{stamp}.jsonl"
    prices_jsonl = out_dir / f"tiki_prices_snapshot_{stamp}.jsonl"
    reviews_jsonl = out_dir / f"tiki_reviews_snapshot_{stamp}.jsonl"
    unparsed_jsonl = out_dir / f"tiki_unparsed_{stamp}.jsonl"
    prod_csv = out_dir / "tiki_products_latest.csv"
    prices_csv = out_dir / "tiki_prices_latest.csv"
    reviews_csv = out_dir / "tiki_reviews_latest.csv"

    write_jsonl(prod_jsonl, all_products)
    write_jsonl(prices_jsonl, all_prices)
    write_jsonl(reviews_jsonl, all_reviews)
    write_jsonl(unparsed_jsonl, all_unparsed)

    prod_fields = [
        "product_id",
        "product_name",
        "brand",
        "category",
        "subcategory",
        "price",
        "currency",
        "rating",
        "review_count",
        "seller",
        "product_url",
        "image_url",
        "source",
        "created_at",
        "updated_at",
    ]
    price_fields = ["price_id", "product_id", "price", "currency", "seller", "timestamp", "source"]
    review_fields = [
        "review_id",
        "product_id",
        "user_id",
        "rating",
        "review_title",
        "review_text",
        "review_date",
        "verified_purchase",
        "helpful_count",
        "source",
        "language",
        "created_at",
    ]

    write_csv(prod_csv, all_products, prod_fields)
    write_csv(prices_csv, all_prices, price_fields)
    write_csv(reviews_csv, all_reviews, review_fields)

    # Optional Data Lake / Kafka dispatch
    if args.save_datalake and all_products:
        from kafka.consumers.raw_consumer import RawConsumer
        from config.settings import settings

        consumer = RawConsumer()
        p_path = consumer.save_raw_batch(settings.kafka.topic_products, all_products)
        pr_path = consumer.save_raw_batch(settings.kafka.topic_prices, all_prices)
        if all_reviews:
            r_path = consumer.save_raw_batch(settings.kafka.topic_reviews, all_reviews)
            print(f"[Bronze Lake] Saved {len(all_reviews)} genuine reviews to: {r_path}")
        print(f"[Bronze Lake] Saved {len(all_products)} products to: {p_path}")
        print(f"[Bronze Lake] Saved {len(all_prices)} prices to: {pr_path}")

    if args.push_kafka and all_products:
        from kafka.producers.stream_producer import StreamProducer
        from config.settings import settings

        producer = StreamProducer()
        producer.produce_batch(settings.kafka.topic_products, all_products, event_type="NEW_PRODUCT")
        producer.produce_batch(settings.kafka.topic_prices, all_prices, event_type="PRICE_UPDATE")
        if all_reviews:
            producer.produce_batch(settings.kafka.topic_reviews, all_reviews, event_type="NEW_REVIEW")
        producer.close()
        print(
            f"[Kafka] Pushed {len(all_products)} products, {len(all_prices)} prices, and "
            f"{len(all_reviews)} genuine reviews to stream."
        )

    print("\n" + "=" * 60)
    print("      CRAWL EXECUTION SUMMARY")
    print("=" * 60)
    print(f"Products Extracted (raw.products): {len(all_products)}")
    print(f"Prices Recorded   (raw.prices):   {len(all_prices)}")
    print(f"Reviews Collected (raw.reviews):  {len(all_reviews)} (100% genuine, 0 simulated)")
    print(f"Unparsed Pages:                   {len(all_unparsed)}")
    print(f"Products Latest CSV:              {prod_csv}")
    print(f"Prices Latest CSV:                {prices_csv}")
    print(f"Reviews Latest CSV:               {reviews_csv}")
    print(f"Products JSONL Snapshot:          {prod_jsonl}")
    print(f"Prices JSONL Snapshot:            {prices_jsonl}")
    print(f"Reviews JSONL Snapshot:           {reviews_jsonl}")
    print("=" * 60 + "\n")

    return 0 if all_products else 2


if __name__ == "__main__":
    raise SystemExit(main())
