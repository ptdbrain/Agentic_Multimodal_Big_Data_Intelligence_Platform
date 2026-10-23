"""SentinelAI Run Streaming CLI Script.
Launches Spark Structured Streaming or micro-batch simulator from Kafka.
"""
import sys
import argparse
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from config.settings import settings

def main():
    parser = argparse.ArgumentParser(description="SentinelAI Streaming Processor")
    parser.add_argument("--mode", choices=["spark", "simulator"], default="simulator",
                        help="Streaming execution engine")
    parser.add_argument("--window", default="1 minute", help="Tumbling window duration")
    parser.add_argument("--watermark", default="10 minutes", help="Event-time watermark delay")
    parser.add_argument("--topic", default=None, help="Kafka input topic")
    args = parser.parse_args()

    print("=" * 60)
    print("      SENTINELAI STREAMING PROCESSOR")
    print(f"  Mode:      {args.mode}")
    print(f"  Window:    {args.window}")
    print(f"  Watermark: {args.watermark}")
    print(f"  Broker:    {settings.kafka.bootstrap_servers}")
    print("=" * 60)

    if args.mode == "spark":
        try:
            from spark.streaming.spark_streaming_job import SparkStreamingJob
            job = SparkStreamingJob()
            print("[Spark Streaming] Initializing streaming query from Kafka...")
            query = job.start_review_stream(
                window_duration=args.window,
                watermark_delay=args.watermark
            )
            print("[Spark Streaming] Query active. Awaiting termination...")
            query.awaitTermination()
        except Exception as e:
            print(f"[Spark Streaming Error] Could not start streaming query: {e}")
            sys.exit(1)
    else:
        print("[Simulator] Running StreamingJob micro-batch simulator...")
        from spark.streaming.streaming_job import StreamingJob
        from data.generator import generate_products, generate_streaming_events
        
        products = generate_products()
        events = generate_streaming_events(products, count=50)
        
        job = StreamingJob()
        result = job.process_micro_batch(events)
        print(f"[Simulator Result] Micro-batch processed: {result}")

if __name__ == "__main__":
    main()
