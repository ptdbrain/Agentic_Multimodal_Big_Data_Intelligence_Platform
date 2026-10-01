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
from kafka.consumers.raw_consumer import RawConsumer
from kafka.producers.stream_producer import StreamProducer
from config.settings import settings

def main():
    parser = argparse.ArgumentParser(description="SentinelAI Live E-Commerce Crawler CLI")
    parser.add_argument("--platform", default="tiki", choices=CrawlerRegistry.list_available(),
                        help=f"Target marketplace ({', '.join(CrawlerRegistry.list_available())})")
    parser.add_argument("--query", default="iphone", help="Search keyword or product category")
    parser.add_argument("--max-products", type=int, default=5, help="Maximum number of products to crawl")
    parser.add_argument("--reviews-per-prod", type=int, default=5, help="Number of customer reviews per product")
    parser.add_argument("--save-datalake", action="store_true", default=True, help="Save raw JSON batch to Bronze Data Lake")
    parser.add_argument("--push-kafka", action="store_true", default=False, help="Push live events into Kafka topics")
    parser.add_argument("--url", default=None, help="Direct product URL for universal scraper")

    args = parser.parse_args()

    print("==========================================================")
    print(f"      SENTINELAI E-COMMERCE LIVE CRAWLER ({args.platform.upper()})")
    print("==========================================================")

    crawler = CrawlerRegistry.get(args.platform)

    if args.url and hasattr(crawler, "scrape_url"):
        print(f"[*] Scraping direct URL: {args.url}")
        prod_data = crawler.scrape_url(args.url)
        if prod_data:
            print(f"[OK] Successfully scraped: {prod_data.get('product_name')} - {prod_data.get('price')} {prod_data.get('currency')}")
            crawl_result = {"products": [prod_data], "reviews": []}
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

    print(f"\n[Summary] Crawled {len(products)} products and {len(reviews)} reviews.")

    # 1. Save to Bronze Data Lake
    if args.save_datalake:
        consumer = RawConsumer()
        if products:
            p_path = consumer.save_raw_batch(settings.kafka.topic_products, products)
            print(f"[Data Lake] Products raw batch saved: {p_path}")
        if reviews:
            r_path = consumer.save_raw_batch(settings.kafka.topic_reviews, reviews)
            print(f"[Data Lake] Reviews raw batch saved: {r_path}")

    # 2. Push to Kafka (if enabled)
    if args.push_kafka:
        producer = StreamProducer()
        if products:
            producer.produce_batch(settings.kafka.topic_products, products)
        if reviews:
            producer.produce_batch(settings.kafka.topic_reviews, reviews)
        print("[Kafka] Live stream ingestion completed.")

    print("==========================================================\n")

if __name__ == "__main__":
    main()
