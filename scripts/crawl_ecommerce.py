import sys
import argparse
from pathlib import Path

# Configure UTF-8 output encoding for Windows terminal
if sys.stdout and hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from ingestion.crawler.registry import CrawlerRegistry
from ingestion.crawler.validator import CrawlerDataValidator
from ingestion.privacy import is_anonymized_user_id
from kafka.consumers.raw_consumer import RawConsumer
from kafka.producers.stream_producer import StreamProducer
from scripts.run_e2e_pipeline import run_end_to_end
from config.settings import settings

def main():
    parser = argparse.ArgumentParser(description="SentinelAI Live E-Commerce Crawler CLI")
    parser.add_argument("--platform", default="tiki", choices=CrawlerRegistry.list_available(),
                        help=f"Target marketplace ({', '.join(CrawlerRegistry.list_available())})")
    parser.add_argument("--query", default="iphone 15", help="Search keyword or product category")
    parser.add_argument("--max-products", type=int, default=5, help="Maximum number of products to crawl")
    parser.add_argument("--reviews-per-prod", type=int, default=5, help="Number of customer reviews per product")
    parser.add_argument("--save-datalake", action=argparse.BooleanOptionalAction, default=True, help="Save raw JSON batch to Bronze Data Lake")
    parser.add_argument("--push-kafka", action="store_true", default=False, help="Push live events into Kafka topics")
    parser.add_argument("--validate-pre-spark", action=argparse.BooleanOptionalAction, default=True, help="Validate field names, types, IDs, and prices before downstream processing")
    parser.add_argument("--run-pipeline", action="store_true", default=False, help="Run the full 6-stage canonical E2E pipeline on crawled data")
    parser.add_argument("--mode", choices=["integration", "local", "test"], default=None, help="Pipeline runtime mode when --run-pipeline is set")
    parser.add_argument("--remove-user-id", action="store_true", default=False, help="Redact user_id completely instead of hashing")
    parser.add_argument("--url", default=None, help="Direct product URL for universal scraper")

    args = parser.parse_args()

    if args.run_pipeline:
        ok = run_end_to_end(
            mode=args.mode,
            source_type="crawler",
            crawler_platform=args.platform,
            crawler_query=args.query,
            crawler_max_products=args.max_products,
            crawler_reviews_per_product=args.reviews_per_prod,
            remove_user_id=args.remove_user_id,
        )
        if not ok:
            sys.exit(1)
        return

    print("==========================================================")
    print(f"      SENTINELAI E-COMMERCE LIVE CRAWLER ({args.platform.upper()})")
    print("==========================================================")

    crawler = CrawlerRegistry.get(args.platform, remove_user_id=args.remove_user_id)

    if args.url and hasattr(crawler, "scrape_url"):
        print(f"[*] Scraping direct URL: {args.url}")
        prod_data = crawler.scrape_url(args.url)
        if prod_data:
            print(f"[OK] Successfully scraped: {prod_data.get('product_name')} - {prod_data.get('price')} {prod_data.get('currency')}")
            crawl_result = {"products": [prod_data], "reviews": [], "prices": []}
        else:
            print("[FAIL] Could not extract metadata from URL.")
            return
    else:
        crawl_result = crawler.crawl_category(
            query=args.query,
            max_products=args.max_products,
            reviews_per_product=args.reviews_per_prod
        )

    products = crawl_result.get("products", [])
    reviews = crawl_result.get("reviews", [])
    prices = crawl_result.get("prices", [])

    for r in reviews:
        if not is_anonymized_user_id(r.get("user_id")):
            raise RuntimeError(f"PII violation: crawled review user_id '{r.get('user_id')}' was not anonymized.")

    # Pre-Spark contract validation
    if args.validate_pre_spark and (products or prices):
        report = CrawlerDataValidator.validate_crawled_dataset(
            products=products,
            prices=prices,
            enforce_tiki_id_format=(args.platform == "tiki"),
        )
        if not report["is_valid"]:
            print(f"[ERROR] Pre-Spark validation failed: {len(report['errors'])} errors detected.")
            for err in report["errors"][:5]:
                print(f"  ! {err}")
            raise ValueError(f"Pre-Spark contract guard rejected crawled data for platform '{args.platform}'.")
        print(f"[OK] Pre-Spark contract validation passed ({len(products)} products, {len(prices)} prices verified).")

    # 1. Save to Bronze Data Lake
    if args.save_datalake:
        consumer = RawConsumer()
        if products:
            p_path = consumer.save_raw_batch(settings.kafka.topic_products, products)
            print(f"[Data Lake] Products raw batch saved: {p_path}")
        if reviews:
            r_path = consumer.save_raw_batch(settings.kafka.topic_reviews, reviews)
            print(f"[Data Lake] Reviews raw batch saved: {r_path}")
        if prices:
            pr_path = consumer.save_raw_batch(settings.kafka.topic_prices, prices)
            print(f"[Data Lake] Prices raw batch saved: {pr_path}")

    # 2. Push to Kafka (if enabled)
    if args.push_kafka:
        producer = StreamProducer()
        if products:
            producer.produce_batch(settings.kafka.topic_products, products, event_type="NEW_PRODUCT")
        if reviews:
            producer.produce_batch(settings.kafka.topic_reviews, reviews, event_type="NEW_REVIEW")
        if prices:
            producer.produce_batch(settings.kafka.topic_prices, prices, event_type="PRICE_UPDATE")
        producer.close()
        print("[Kafka] Live stream ingestion completed.")

    print("==========================================================\n")

if __name__ == "__main__":
    main()
