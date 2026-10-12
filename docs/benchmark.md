# SentinelAI — Báo Cáo Benchmark "3V" (Big Data Verification)

Tài liệu này được **sinh tự động trực tiếp** từ lệnh chạy `python scripts/benchmark.py` trên đường chạy thực (Kafka -> RawConsumer -> MinIO Bronze -> SparkBatchETLJob -> Silver/Quarantine Parquet). Mọi số liệu dưới đây khớp 100% với `docs/reports/benchmark_3v.json` và `docs/reports/benchmark_report.md`.

- **Thời điểm chạy (Timestamp)**: `2026-10-12 05:01:02 UTC`
- **Chế độ chạy (Runtime Mode)**: `integration` (`MinIOBackend`)
- **Seed cố định (Deterministic Seed)**: `42`

---

## 1. Kết Quả Volume (Bronze -> Spark Batch ETL -> Silver & Quarantine)

Kịch bản chạy thực tế với các cỡ dữ liệu `[100, 1000, 5000, 10000]` bản ghi (có tiêm tỷ lệ bản ghi lỗi `dirty_ratio=0.05` và bản ghi trùng `duplicate_ratio=0.03`). Không ngoại suy hay quảng cáo số liệu 1 triệu bản ghi khi kịch bản đó không nằm trong danh sách chạy.

| Seed | Số Bản Ghi Đầu Vào | Hợp Lệ (Silver) | Quarantine | Trùng Lặp | Điểm DQ | Thời Gian (s) | Throughput (rec/s) | Dung Lượng Bronze (B) | Dung Lượng Silver (B) | Dung Lượng Quarantine (B) |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| 142 | 100 | 92 | 5 | 3 | 92.0% | 17.9701s | 5.6 | 64,262 | 11,424 | 5,708 |
| 1042 | 1,000 | 920 | 50 | 30 | 92.0% | 18.0859s | 55.3 | 642,052 | 52,327 | 7,801 |
| 5042 | 5,000 | 4,600 | 250 | 150 | 92.0% | 19.1923s | 260.5 | 3,213,561 | 222,331 | 16,057 |
| 10042 | 10,000 | 9,200 | 500 | 300 | 92.0% | 21.0059s | 476.1 | 6,433,642 | 412,904 | 26,277 |

---

## 2. Kết Quả Velocity (Kafka Producer -> Broker -> RawConsumer -> Bronze / DLQ)

| Seed | Transport | Target Rate (msg/s) | Produce Rate (msg/s) | E2E Consume Rate (msg/s) | Đã Gửi | Đã Nhận | Hợp Lệ (Bronze) | DLQ | Thời Gian E2E (s) | Dung Lượng Bronze (B) |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| 92 | real_kafka | 50 | 44.2 | 22.2 | 100 | 100 | 96 | 4 | 4.5136s | 71,211 |
| 242 | real_kafka | 200 | 148.1 | 78.4 | 200 | 200 | 192 | 8 | 2.5525s | 142,986 |
| 542 | real_kafka | 500 | 277.2 | 153.0 | 500 | 500 | 480 | 20 | 3.2681s | 356,559 |
| 1042 | real_kafka | 1,000 | 349.6 | 171.4 | 500 | 500 | 480 | 20 | 2.9168s | 357,692 |

---

## 3. Kết Quả Variety (Parse -> PII Hash -> Bronze -> Spark Validate/Transform -> Silver)

| Seed | Định Dạng / Nguồn | Dung Lượng Nguồn (B) | Đầu Vào | Hợp Lệ (Silver) | Quarantine | Trùng Lặp | Điểm DQ | Thời Gian (s) | Throughput (rec/s) | Dung Lượng Silver (B) | Dung Lượng Quarantine (B) |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| 42 | JSON | 215,893 | 500 | 465 | 25 | 10 | 93.0% | 18.1009s | 27.6 | 30,035 | 6,739 |
| 42 | JSONL | 215,893 | 500 | 465 | 25 | 10 | 93.0% | 17.5559s | 28.5 | 30,035 | 6,739 |
| 42 | CSV | 106,765 | 500 | 473 | 17 | 10 | 94.6% | 17.6744s | 28.3 | 30,324 | 6,509 |
| 42 | TSV | 106,041 | 500 | 473 | 17 | 10 | 94.6% | 17.7482s | 28.2 | 30,324 | 6,509 |
| 42 | Parquet | 31,177 | 500 | 465 | 25 | 10 | 93.0% | 17.9053s | 27.9 | 30,035 | 6,739 |
| 42 | HTML_JSONLD_Crawler | 163,779 | 500 | 465 | 25 | 10 | 93.0% | 17.8881s | 28.0 | 28,902 | 6,562 |

---

## 4. Lệnh Chạy Tái Hiện Trực Tiếp

```bash
python scripts/benchmark.py --mode integration --seed 42
```
