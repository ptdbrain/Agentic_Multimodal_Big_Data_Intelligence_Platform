"""SentinelAI Dataset Replay & 3V Ingestion Simulator.

Simulates pushing pre-existing datasets (CSV, JSON, JSONL, Parquet) into the
Big Data pipeline while demonstrating the 3Vs (Variety, Volume, Velocity) and
executing core data processing (Clean, Normalize, Validate, Deduplicate, Aggregate).
"""
import sys
import time
import copy
import random
import argparse
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List, Dict, Any

if sys.stdout and hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from config.settings import settings
from ingestion.file_loader.loader import FileLoader
from kafka.producers.stream_producer import StreamProducer
from kafka.consumers.raw_consumer import RawConsumer
from data.schemas.validator import SchemaValidator
from spark.batch.spark_etl_job import SparkBatchETLJob
from spark.analytics.gold_builder import SparkGoldBuilder
from storage.storage_manager import storage


def load_multi_format_datasets(data_dir: Path) -> Dict[str, Any]:
    """1. VARIETY: Load pre-existing datasets from heterogeneous file formats (JSON, CSV, Parquet)."""
    print("\n" + "=" * 70)
    print("[1. VARIETY] Đọc dữ liệu từ các định dạng và cấu trúc khác nhau")
    print("=" * 70)

    # Read each entity from a different file format to prove multi-format ingestion
    format_plan = [
        ("products", "JSON", data_dir / "products.json"),
        ("reviews", "CSV", data_dir / "reviews.csv"),
        ("prices", "Parquet", data_dir / "prices.parquet"),
    ]

    datasets: Dict[str, List[Dict[str, Any]]] = {"products": [], "reviews": [], "prices": []}
    variety_stats = []

    for entity, fmt_label, file_path in format_plan:
        if not file_path.exists():
            # Fallback to generator if sample files are not yet initialized
            from data.generator import build_all_sample_datasets
            build_all_sample_datasets()

        t0 = time.time()
        records = FileLoader.load_records(file_path)
        elapsed = time.time() - t0
        datasets[entity] = records
        variety_stats.append({
            "entity": entity,
            "format": fmt_label,
            "file": file_path.name,
            "records": len(records),
            "elapsed_sec": round(elapsed, 4),
        })
        print(
            f"  + [{fmt_label:<7}] Đọc file '{file_path.name:<18}' -> "
            f"{len(records):>5} bản ghi ({entity}) trong {elapsed:.4f}s"
        )

    return {"datasets": datasets, "stats": variety_stats}


def scale_dataset_volume(
    base_records: List[Dict[str, Any]],
    target_volume: int,
    id_field: str,
    prefix: str,
    inject_dirty_ratio: float = 0.01,
) -> List[Dict[str, Any]]:
    """2. VOLUME: Amplify/replay pre-existing dataset to reach target volume + inject noisy records."""
    if not base_records:
        return []

    scaled: List[Dict[str, Any]] = []
    base_len = len(base_records)
    base_time = datetime(2026, 10, 1, 8, 0, 0, tzinfo=timezone.utc)

    for i in range(target_volume):
        template = copy.deepcopy(base_records[i % base_len])
        # Assign deterministic sequential ID for new replayed events
        if i >= base_len:
            template[id_field] = f"{prefix}_sim_{i:07d}"
            event_dt = base_time + timedelta(seconds=i * 2)
            if "review_date" in template:
                template["review_date"] = event_dt.isoformat().replace("+00:00", "Z")
            elif "timestamp" in template:
                template["timestamp"] = event_dt.isoformat().replace("+00:00", "Z")

        # Inject a small fraction of dirty/malformed data to test Processing & Quarantine
        if inject_dirty_ratio > 0 and i > 0 and (i % int(1 / inject_dirty_ratio) == 0):
            if "rating" in template:
                template["rating"] = 99.0  # Invalid rating (> 5.0)
            elif "price" in template:
                template["price"] = -50000.0  # Invalid negative price

        scaled.append(template)

    # Also inject a few exact duplicates to verify Deduplication in Processing
    if len(scaled) >= 5:
        scaled.append(copy.deepcopy(scaled[0]))
        scaled.append(copy.deepcopy(scaled[1]))

    return scaled


def simulate_velocity_push(
    products: List[Dict[str, Any]],
    reviews: List[Dict[str, Any]],
    prices: List[Dict[str, Any]],
    rate_rps: float,
    micro_batch_size: int = 500,
) -> Dict[str, Any]:
    """3. VELOCITY: Simulate streaming push via Producer -> Kafka/Queue -> Consumer -> Bronze Lake."""
    print("\n" + "=" * 70)
    print(
        f"[3. VELOCITY] Mô phỏng đẩy luồng dữ liệu (Target rate: "
        f"{'MAX BURST' if rate_rps <= 0 else f'{rate_rps} msg/s'}, Micro-batch: {micro_batch_size})"
    )
    print("=" * 70)

    try:
        producer = StreamProducer(enable_offline_buffer=False)
        mode_str = f"Real Kafka Broker ({settings.kafka.bootstrap_servers})"
    except Exception:
        producer = StreamProducer(enable_offline_buffer=True)
        mode_str = "Simulated Stream Queue (Offline Buffer Mode)"

    print(f"  * Chế độ kết nối Producer: {mode_str}")

    t_start = time.time()
    p_sent, p_rate = producer.produce_batch(
        settings.kafka.topic_products, products, rate=rate_rps, event_type="NEW_PRODUCT", source="dataset_simulator"
    )
    r_sent, r_rate = producer.produce_batch(
        settings.kafka.topic_reviews, reviews, rate=rate_rps, event_type="NEW_REVIEW", source="dataset_simulator"
    )
    pr_sent, pr_rate = producer.produce_batch(
        settings.kafka.topic_prices, prices, rate=rate_rps, event_type="PRICE_UPDATE", source="dataset_simulator"
    )
    total_sent = p_sent + r_sent + pr_sent
    total_stream_time = max(time.time() - t_start, 0.001)
    overall_rate = total_sent / total_stream_time

    # Consumer reads streamed envelopes, validates contract, and writes micro-batches to Bronze
    consumer = RawConsumer()
    bronze_paths = {"products": [], "reviews": [], "prices": []}

    for entity_key, topic_name, raw_list, schema_name in [
        ("products", settings.kafka.topic_products, products, "product"),
        ("reviews", settings.kafka.topic_reviews, reviews, "review"),
        ("prices", settings.kafka.topic_prices, prices, "price"),
    ]:
        # Slice into micro-batches to simulate continuous Consumer polling
        for idx in range(0, len(raw_list), micro_batch_size):
            chunk = raw_list[idx : idx + micro_batch_size]
            out_path = consumer.save_raw_batch(topic_name, chunk)
            bronze_paths[entity_key].append(str(out_path))

    print(
        f"  * Tổng kết đẩy luồng: Đã đẩy {total_sent} bản ghi trong {total_stream_time:.3f}s "
        f"(Tốc độ thực tế: {overall_rate:,.1f} msg/s)"
    )
    print(
        f"  * Tầng Bronze ({type(storage.backend).__name__}): Đã ghi "
        f"{len(bronze_paths['products']) + len(bronze_paths['reviews']) + len(bronze_paths['prices'])} micro-batch files."
    )

    return {
        "total_messages_sent": total_sent,
        "duration_sec": round(total_stream_time, 3),
        "actual_velocity_mps": round(overall_rate, 1),
        "bronze_batches": {k: len(v) for k, v in bronze_paths.items()},
    }


def run_core_processing(
    products: List[Dict[str, Any]],
    reviews: List[Dict[str, Any]],
    prices: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """4. PROCESSING: Clean -> Normalize -> Validate (Quarantine) -> Deduplicate -> Aggregate."""
    print("\n" + "=" * 70)
    print("[4. PROCESSING] Thực thi xử lý dữ liệu (Spark Batch ETL & Gold Aggregation)")
    print("=" * 70)

    # Step 4.1: Bronze -> Silver (Clean, Normalize, Validate, Quarantine, Deduplicate)
    etl_job = SparkBatchETLJob()
    etl_summary = etl_job.run_pipeline(
        products_source=products,
        reviews_source=reviews,
        prices_source=prices,
    )

    print("  [A. Tầng Silver - Làm sạch, Chuẩn hóa, Kiểm tra & Khử trùng lặp]")
    print(f"    - Products hợp lệ ghi vào Silver : {etl_summary['products_processed']}")
    print(
        f"    - Reviews hợp lệ ghi vào Silver  : {etl_summary['reviews_processed']} "
        f"(Bị loại/Quarantine & Dedup: {len(reviews) - etl_summary['reviews_processed']})"
    )
    print(f"    - Prices hợp lệ ghi vào Silver   : {etl_summary['prices_processed']}")
    print(
        f"    - Điểm chất lượng dữ liệu (DQ)   : "
        f"Products={etl_summary['products_dq']['dq_score']}% | "
        f"Reviews={etl_summary['reviews_dq']['dq_score']}% | "
        f"Prices={etl_summary['prices_dq']['dq_score']}%"
    )

    # Step 4.2: Silver -> Gold (Basic Descriptive Aggregations: COUNT, AVG, MIN, MAX)
    gold_builder = SparkGoldBuilder(spark=etl_job.spark)
    marts = gold_builder.build_all()

    print("\n  [B. Tầng Gold - Tổng hợp thống kê cơ bản (GROUP BY Ngày, Sản phẩm, Thương hiệu, Ngành hàng)]")
    for mart_name, df in marts.items():
        print(f"    - Bảng Gold '{mart_name:<22}': {len(df):>5} dòng tổng hợp")

    return {
        "silver_counts": {
            "products": etl_summary["products_processed"],
            "reviews": etl_summary["reviews_processed"],
            "prices": etl_summary["prices_processed"],
        },
        "gold_marts": {k: len(v) for k, v in marts.items()},
    }


def main():
    parser = argparse.ArgumentParser(
        description="Chương trình mô phỏng đẩy Dataset có sẵn lên hệ thống Big Data (3V Simulator)"
    )
    parser.add_argument(
        "--data-dir",
        type=str,
        default=str(REPO_ROOT / "data" / "sample"),
        help="Thư mục chứa các file dataset có sẵn (.json, .csv, .parquet)",
    )
    parser.add_argument(
        "--volume",
        type=int,
        default=3000,
        help="Số lượng bản ghi đánh giá (reviews) cần mô phỏng để kiểm tra Volume (VD: 3000, 50000, 100000)",
    )
    parser.add_argument(
        "--rate",
        type=float,
        default=0.0,
        help="Tốc độ đẩy message/giây vào Kafka để kiểm tra Velocity (0 = đẩy tốc độ tối đa / burst mode)",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=1000,
        help="Kích thước mỗi micro-batch khi Consumer ghi xuống tầng Bronze",
    )
    parser.add_argument(
        "--skip-processing",
        action="store_true",
        help="Chỉ mô phỏng đẩy dữ liệu vào Kafka/Bronze, bỏ qua bước Spark ETL",
    )
    parser.add_argument(
        "--mode",
        type=str,
        default=None,
        choices=["integration", "production", "local", "test"],
        help="Chế độ chạy: 'integration' (mặc định - bắt buộc MinIO thật) hoặc 'local' (khai báo tường minh cho thử nghiệm offline)",
    )
    args = parser.parse_args()

    resolved_mode = (args.mode or settings.runtime_mode or "integration").strip().lower()
    is_local = resolved_mode in ("local", "test")
    settings.set_runtime_mode("local" if is_local else "integration")
    storage.set_mode("local" if is_local else "integration")

    print("#" * 70)
    print(f"   SENTINELAI — CHƯƠNG TRÌNH MÔ PHỎNG ĐẨY DATASET & XỬ LÝ 3V (MODE: {resolved_mode.upper()})")
    print("#" * 70)

    if not is_local:
        print("\n[PRE-FLIGHT] Kiểm tra kết nối dịch vụ thực tế (MinIO) trước khi chạy chế độ Integration...")
        storage.health_check()
        print("  -> [OK] MinIO kết nối thành công.")

    # 1. VARIETY: Load multi-format files
    loaded_info = load_multi_format_datasets(Path(args.data_dir))
    base_products = loaded_info["datasets"]["products"]
    base_reviews = loaded_info["datasets"]["reviews"]
    base_prices = loaded_info["datasets"]["prices"]

    # 2. VOLUME: Scale dataset to requested volume
    print("\n" + "=" * 70)
    print(f"[2. VOLUME] Mở rộng quy mô tập dữ liệu lên mục tiêu {args.volume:,} reviews")
    print("=" * 70)
    scaled_reviews = scale_dataset_volume(
        base_reviews, target_volume=args.volume, id_field="review_id", prefix="rv", inject_dirty_ratio=0.01
    )
    scaled_prices = scale_dataset_volume(
        base_prices, target_volume=max(len(base_prices), args.volume // 3), id_field="price_id", prefix="pr", inject_dirty_ratio=0.0
    )
    print(
        f"  + Quy mô dữ liệu chuẩn bị đẩy: "
        f"{len(base_products):,} products | {len(scaled_reviews):,} reviews | {len(scaled_prices):,} prices "
        f"(Tổng cộng: {len(base_products) + len(scaled_reviews) + len(scaled_prices):,} bản ghi)"
    )

    # 3. VELOCITY: Push records through StreamProducer -> Consumer -> Bronze
    vel_stats = simulate_velocity_push(
        products=base_products,
        reviews=scaled_reviews,
        prices=scaled_prices,
        rate_rps=args.rate,
        micro_batch_size=args.batch_size,
    )

    # 4. PROCESSING: Run Spark Batch ETL & Gold Builder
    if not args.skip_processing:
        proc_stats = run_core_processing(
            products=base_products,
            reviews=scaled_reviews,
            prices=scaled_prices,
        )

    print("\n" + "#" * 70)
    print("   HOÀN TẤT MÔ PHỎNG 3V & XỬ LÝ DỮ LIỆU THÀNH CÔNG!")
    print("#" * 70)


if __name__ == "__main__":
    main()
