"""SentinelAI 3V Benchmark Tool (Spark Native).
Measures Volume, Velocity, and Variety processing capabilities across the Big Data pipeline.
Generates comprehensive JSON & Markdown performance evidence.
"""
import time
import json
import sys
import os
from pathlib import Path
from typing import Optional, List, Dict, Any

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from data.generator import generate_products, generate_reviews, generate_price_history
from spark.batch.spark_etl_job import SparkBatchETLJob
from config.settings import settings

class Benchmark:
    
    @staticmethod
    def benchmark_volume(record_counts: Optional[List[int]] = None) -> Dict[str, Any]:
        """Benchmark Spark Batch ETL engine at different data volumes."""
        counts = record_counts or [100, 1000, 5000, 10000]
        results = []
        products = generate_products()
        etl_job = SparkBatchETLJob()
        
        for count in counts:
            reviews = generate_reviews(products, count=count, inject_anomalies=False)
            
            t0 = time.time()
            etl_result = etl_job.run_pipeline(products, reviews)
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
    def benchmark_velocity(rates: Optional[List[int]] = None) -> Dict[str, Any]:
        """Benchmark Kafka producer throughput and message delivery rates."""
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
            print(f"Velocity Benchmark: Target {rate:>4} msg/s -> Actual {actual_rate:.1f} msg/s ({sent} sent in {duration:.3f}s)")
        
        return {"benchmark_type": "velocity", "results": results}
    
    @staticmethod
    def benchmark_variety() -> Dict[str, Any]:
        """Benchmark processing diversity across different file formats (CSV, JSON, Parquet)."""
        from ingestion.file_loader.loader import FileLoader
        
        formats = []
        sample_dir = REPO_ROOT / "data" / "sample"
        
        for fmt, files in [
            ("JSON", ["products.json", "reviews.json", "prices.json"]),
            ("CSV", ["products.csv", "reviews.csv", "prices.csv"]),
            ("Parquet", ["products.parquet", "reviews.parquet", "prices.parquet"])
        ]:
            loaded = 0
            t0 = time.time()
            for f in files:
                fpath = sample_dir / f
                if fpath.exists():
                    records = FileLoader.load_records(fpath)
                    loaded += len(records)
            duration = time.time() - t0
            throughput = loaded / max(duration, 0.001)
            
            formats.append({
                "format": fmt,
                "files_processed": len(files),
                "records_loaded": loaded,
                "duration_sec": round(duration, 3),
                "throughput_rps": round(throughput, 1)
            })
            print(f"Variety Benchmark: {fmt:>7} -> {loaded:>5} records loaded in {duration:.3f}s ({throughput:.1f} rec/s)")
        
        return {"benchmark_type": "variety", "results": formats}
    
    @staticmethod
    def run_full_benchmark() -> Dict[str, Any]:
        """Run all 3V benchmarks and generate comprehensive JSON and Markdown reports."""
        print("=" * 60)
        print("  SENTINELAI 3V BENCHMARK SUITE (SPARK PIPELINE)  ")
        print("=" * 60)
        
        print("\n--- 1. Volume Benchmark (Spark Batch ETL) ---")
        vol = Benchmark.benchmark_volume()
        
        print("\n--- 2. Velocity Benchmark (Kafka Streaming) ---")
        vel = Benchmark.benchmark_velocity()
        
        print("\n--- 3. Variety Benchmark (Multi-Format Ingestion) ---")
        var = Benchmark.benchmark_variety()
        
        report = {
            "volume": vol,
            "velocity": vel,
            "variety": var
        }
        
        # Save JSON report
        report_json_path = REPO_ROOT / "docs" / "reports" / "benchmark_3v.json"
        report_json_path.parent.mkdir(parents=True, exist_ok=True)
        with open(report_json_path, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        
        # Generate Markdown Report
        md_content = f"""# SentinelAI 3V Benchmark Report

**Generated At:** {time.strftime("%Y-%m-%d %H:%M:%S")}
**Engine:** Apache Spark Batch & Kafka Ingestion

## 1. Volume Benchmark (Spark Batch ETL)
| Record Count | Processing Time (s) | Throughput (rec/s) | DQ Score |
| :--- | :--- | :--- | :--- |
"""
        for r in vol["results"]:
            md_content += f"| {r['records']:,} | {r['duration_sec']}s | {r['throughput_rps']:,} | {r['dq_score']}% |\n"

        md_content += """
## 2. Velocity Benchmark (Kafka Stream Producer)
| Target Rate (msg/s) | Actual Rate (msg/s) | Messages Sent | Duration (s) |
| :--- | :--- | :--- | :--- |
"""
        for r in vel["results"]:
            md_content += f"| {r['target_rate']:,} | {r['actual_rate']:,} | {r['messages_sent']:,} | {r['duration_sec']}s |\n"

        md_content += """
## 3. Variety Benchmark (Multi-Format Ingestion)
| Format | Files Processed | Records Loaded | Load Time (s) | Throughput (rec/s) |
| :--- | :--- | :--- | :--- | :--- |
"""
        for r in var["results"]:
            md_content += f"| {r['format']} | {r['files_processed']} | {r['records_loaded']:,} | {r['duration_sec']}s | {r['throughput_rps']:,} |\n"

        report_md_path = REPO_ROOT / "docs" / "reports" / "benchmark_report.md"
        with open(report_md_path, "w", encoding="utf-8") as f:
            f.write(md_content)

        print(f"\nBenchmark reports saved to:\n- {report_json_path}\n- {report_md_path}")
        return report

if __name__ == "__main__":
    Benchmark.run_full_benchmark()
