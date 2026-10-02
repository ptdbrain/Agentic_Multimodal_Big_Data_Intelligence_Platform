"""SentinelAI 3V Benchmark Tool.
Measures Volume, Velocity, and Variety processing capabilities.
"""
import time
import json
import sys
from pathlib import Path
from typing import Optional

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from data.generator import generate_products, generate_reviews, generate_price_history
from spark.batch.batch_etl_job import BatchETLJob
from config.settings import settings

class Benchmark:
    
    @staticmethod
    def benchmark_volume(record_counts: list = None) -> dict:
        """Benchmark processing at different data volumes."""
        counts = record_counts or [100, 1000, 5000, 10000]
        results = []
        products = generate_products()
        
        for count in counts:
            reviews = generate_reviews(products, count=count, inject_anomalies=False)
            
            t0 = time.time()
            etl_result = BatchETLJob.run_pipeline(products, reviews, save_silver=False)
            duration = time.time() - t0
            
            throughput = count / max(duration, 0.001)
            results.append({
                "records": count,
                "duration_sec": round(duration, 3),
                "throughput_rps": round(throughput, 1),
                "products_processed": etl_result.get("products_processed", len(products)),
                "reviews_processed": etl_result.get("reviews_processed", count),
                "dq_score": etl_result.get("reviews_dq", {}).get("dq_score", 0.0) if "reviews_dq" in etl_result else 0.0
            })
            print(f"Volume Benchmark: {count:>6} records -> {duration:.3f}s ({throughput:.1f} rec/s)")
        
        return {"benchmark_type": "volume", "results": results}
    
    @staticmethod  
    def benchmark_velocity(rates: list = None) -> dict:
        """Benchmark Kafka producer throughput at different rates."""
        from kafka.producers.stream_producer import StreamProducer
        
        target_rates = rates or [10, 50, 100, 500]
        results = []
        products = generate_products()
        reviews = generate_reviews(products, count=100, inject_anomalies=False)
        
        for rate in target_rates:
            producer = StreamProducer(enable_offline_buffer=True)
            
            t0 = time.time()
            sent, actual_rate = producer.produce_batch(
                settings.kafka.topic_reviews, reviews[:min(100, len(reviews))], rate=rate
            )
            duration = time.time() - t0
            
            results.append({
                "target_rate": rate,
                "actual_rate": round(actual_rate, 1),
                "messages_sent": sent,
                "duration_sec": round(duration, 3)
            })
        
        return {"benchmark_type": "velocity", "results": results}
    
    @staticmethod
    def benchmark_variety() -> dict:
        """Benchmark processing of different file formats."""
        from ingestion.file_loader.loader import FileLoader
        
        formats = []
        sample_dir = REPO_ROOT / "data" / "sample"
        
        for fmt, files in [
            ("JSON", ["products.json", "reviews.json"]),
            ("CSV", ["products.csv", "reviews.csv"]),
            ("Parquet", ["products.parquet", "reviews.parquet"])
        ]:
            loaded = 0
            t0 = time.time()
            for f in files:
                fpath = sample_dir / f
                if fpath.exists():
                    records = FileLoader.load_records(fpath)
                    loaded += len(records)
            duration = time.time() - t0
            
            formats.append({
                "format": fmt,
                "files_processed": len(files),
                "records_loaded": loaded,
                "duration_sec": round(duration, 3)
            })
        
        return {"benchmark_type": "variety", "results": formats}
    
    @staticmethod
    def run_full_benchmark() -> dict:
        """Run all 3V benchmarks and generate report."""
        print("=" * 60)
        print("  SENTINELAI 3V BENCHMARK SUITE")
        print("=" * 60)
        
        print("\n--- Volume Benchmark ---")
        vol = Benchmark.benchmark_volume()
        
        print("\n--- Velocity Benchmark ---")
        vel = Benchmark.benchmark_velocity()
        
        print("\n--- Variety Benchmark ---")
        var = Benchmark.benchmark_variety()
        
        report = {
            "volume": vol,
            "velocity": vel,
            "variety": var
        }
        
        # Save report
        report_path = REPO_ROOT / "docs" / "reports" / "benchmark_3v.json"
        report_path.parent.mkdir(parents=True, exist_ok=True)
        with open(report_path, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        
        print(f"\nBenchmark report saved to: {report_path}")
        return report

if __name__ == "__main__":
    Benchmark.run_full_benchmark()
