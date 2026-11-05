"""SentinelAI Run Streaming CLI Script.
Launches Spark Structured Streaming from Kafka or a labelled synthetic simulator.
"""
import sys
import argparse
import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from config.settings import settings


def simulator_duration(value, allow_zero=False):
    match = re.fullmatch(r'(\d+)\s*(seconds?|minutes?|hours?|days?)',value.strip().lower())
    if match is None:
        raise ValueError('Simulator duration requires whole seconds/minutes/hours/days')
    multiplier = {'second':1,'minute':60,'hour':3600,'day':86400}[match.group(2).rstrip('s')]
    seconds = int(match.group(1)) * multiplier
    if seconds == 0 and not allow_zero:
        raise ValueError('Simulator window must be positive')
    return seconds

def main():
    parser = argparse.ArgumentParser(description="SentinelAI Streaming Processor")
    parser.add_argument("--mode", choices=["spark", "simulator"], default="simulator",
                        help="Streaming execution engine")
    parser.add_argument("--window", default="1 minute", help="Tumbling window duration")
    parser.add_argument("--watermark", default="10 minutes", help="Event-time watermark delay")
    parser.add_argument("--topic", default=None, help="Kafka input topic")
    parser.add_argument('--seed',type=int,default=None,help='Simulator generator seed (default 42)')
    args = parser.parse_args()
    if args.mode == 'simulator':
        try:
            window_seconds = simulator_duration(args.window)
            watermark_seconds = simulator_duration(args.watermark,allow_zero=True)
        except ValueError as error:
            parser.error(str(error))
        if args.topic:
            parser.error('--topic requires --mode spark; simulator uses generated input')
        args.seed = args.seed if args.seed is not None else 42
    elif args.seed is not None:
        parser.error('--seed applies only to --mode simulator')

    print("=" * 60)
    print("      SENTINELAI STREAMING PROCESSOR")
    print(f"  Mode:      {args.mode}")
    print(f"  Window:    {args.window}")
    print(f"  Watermark: {args.watermark}")
    if args.mode == 'spark':
        print(f"  Broker:    {settings.kafka.bootstrap_servers}")
    else:
        print(f'  Source:    synthetic generator, seed={args.seed}')
    print("=" * 60)

    if args.mode == "spark":
        try:
            from spark.streaming.spark_streaming_job import SparkStreamingJob
            job = SparkStreamingJob()
            print("[Spark Streaming] Initializing streaming query from Kafka...")
            query = job.start_review_stream(
                window_duration=args.window,
                watermark_delay=args.watermark,
                topics=args.topic,
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
        
        products = generate_products(seed=args.seed)
        events = generate_streaming_events(products, count=50,seed=args.seed)
        
        job = StreamingJob(window_seconds=window_seconds,watermark_seconds=watermark_seconds)
        result = job.process_micro_batch(events)
        print(f"[Simulator Result] Micro-batch processed: {result}")

if __name__ == "__main__":
    main()
