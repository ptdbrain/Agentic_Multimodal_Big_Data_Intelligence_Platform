# SentinelAI — Báo Cáo Benchmark "3V" (Big Data Verification)

Tài liệu này được **sinh tự động trực tiếp** từ lệnh chạy `python scripts/benchmark.py` trên đường chạy thực (Kafka -> RawConsumer -> MinIO Bronze -> SparkBatchETLJob -> Silver/Quarantine Parquet). Mọi số liệu dưới đây khớp 100% với `docs/reports/benchmark_3v.json` và `docs/reports/benchmark_report.md`.

- **Thời điểm chạy (Timestamp)**: `2026-10-20 07:11:43 UTC`
- **Chế độ chạy (Runtime Mode)**: `integration` (`MinIOBackend`)
- **Seed cố định (Deterministic Seed)**: `42`
- **Số lần chạy lặp lại (Iterations)**: `1` (báo cáo giá trị median)

---

## 1. Kết Quả Volume (Bronze -> Spark Batch ETL -> Silver & Quarantine)

> [!NOTE]
> **Ranh giới đo lường:** Giai đoạn 1 ghi Bronze Storage + Giai đoạn 2 Spark Batch ETL (đọc Bronze -> chuẩn hóa kiểu -> kiểm định & cách ly -> khử trùng lặp -> ghi Silver Parquet). Không bao gồm Kafka.

Kịch bản chạy thực tế với các cỡ dữ liệu `[100, 1000, 10000]` bản ghi (có tiêm tỷ lệ bản ghi lỗi `dirty_ratio=0.05` và bản ghi trùng `duplicate_ratio=0.03`). Không ngoại suy hay quảng cáo số liệu 1 triệu bản ghi khi kịch bản đó không nằm trong danh sách chạy.

| Seed | Đầu Vào | Hợp Lệ (Silver) | Quarantine | Trùng Lặp | Điểm DQ | Ghi Bronze (s) | Spark ETL (s) | Tổng Thời Gian (s) | Spark Rate (rec/s) | Tổng Rate (rec/s) | CPU % | RAM (MB) | Dung Lượng Bronze (B) | Dung Lượng Silver (B) | Dung Lượng Quarantine (B) |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| 142 | 100 | 92 | 5 | 3 | 92.0% | 0.0127s | 2.8036s | 2.8162s | 35.7 | 35.5 | 2.2% | 148.3 | 64,262 | 11,424 | 5,708 |
| 1042 | 1,000 | 920 | 50 | 30 | 92.0% | 0.0387s | 2.8737s | 2.9124s | 348.0 | 343.4 | 5.4% | 149.2 | 642,052 | 52,327 | 7,801 |
| 10042 | 10,000 | 9,200 | 500 | 300 | 92.0% | 0.2214s | 3.7355s | 3.9568s | 2,677.0 | 2,527.3 | 4.7% | 158.4 | 6,433,642 | 412,904 | 26,277 |

---

## 2. Kết Quả Velocity (Kafka Producer -> Broker -> RawConsumer -> Bronze / DLQ)

> [!NOTE]
> **Transport Mode:** `real_kafka` kết nối trực tiếp cụm Kafka KRaft; chế độ integration không dùng hàng đợi offline giả lập. Đo đạc độ trễ p50/p95/p99 từ lúc phát đến lúc commit.

| Seed | Transport | Mục Tiêu (msg/s) | Đã Gửi | Đã Nhận | Hợp Lệ (Bronze) | DLQ | Lag | Tốc Độ Gửi (msg/s) | Tốc Độ Nhận (msg/s) | Độ Trễ p50 (ms) | Độ Trễ p95 (ms) | Độ Trễ p99 (ms) | Thời Gian E2E (s) | Throughput E2E (msg/s) | Dung Lượng Bronze (B) |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| 92 | real_kafka | 50 | 100 | 100 | 96 | 4 | 0 | 42.4 | 399.8 | 2611.14ms | 2611.15ms | 2611.15ms | 2.6112s | 38.3 | 75,407 |
| 242 | real_kafka | 200 | 400 | 400 | 384 | 16 | 0 | 131.6 | 1,119.2 | 3396.96ms | 3397.01ms | 3397.01ms | 3.3970s | 117.8 | 302,720 |

---

## 3. Kết Quả Variety (Parse -> PII Hash -> Bronze -> Spark Validate/Transform -> Silver)

> [!NOTE]
> Phân biệt rõ HTML fixture tổng hợp Schema.org với dữ liệu crawler Tiki thực tế. Toàn bộ các định dạng đều tuân thủ schema chuẩn, ẩn danh PII và qua Spark ETL.

| Seed | Định Dạng / Nguồn | Phân Loại | Dung Lượng Nguồn (B) | Đầu Vào | Hợp Lệ (Silver) | Quarantine | Trùng Lặp | Điểm DQ | Thời Gian (s) | Throughput (rec/s) | Dung Lượng Silver (B) | Dung Lượng Quarantine (B) |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| 42 | JSON | synthetic_file | 43,124 | 100 | 93 | 5 | 2 | 93.0% | 2.2851s | 43.8 | 11,478 | 5,699 |
| 42 | JSONL | synthetic_file | 43,124 | 100 | 93 | 5 | 2 | 93.0% | 2.3577s | 42.4 | 11,478 | 5,699 |
| 42 | CSV | synthetic_file | 21,418 | 100 | 94 | 4 | 2 | 94.0% | 2.8175s | 35.5 | 11,441 | 5,819 |
| 42 | TSV | synthetic_file | 21,272 | 100 | 94 | 4 | 2 | 94.0% | 2.4892s | 40.2 | 11,441 | 5,819 |
| 42 | Parquet | synthetic_file | 14,359 | 100 | 93 | 5 | 2 | 93.0% | 2.5648s | 39.0 | 11,478 | 5,699 |
| 42 | HTML_Synthetic_JSONLD | synthetic_html | 33,220 | 100 | 93 | 5 | 2 | 93.0% | 2.3616s | 42.3 | 10,994 | 5,719 |
| 42 | HTML_Tiki_Crawler | real_crawler_html | 3,042 | 6 | 6 | 0 | 0 | 100.0% | 1.7480s | 3.4 | 6,419 | 0 |

---

## 4. Lệnh Chạy Tái Hiện Trực Tiếp

```bash
# Chạy nhanh smoke-test:
python scripts/benchmark.py --mode integration --seed 42 --volume-sizes 100,1000,10000 --velocity-rates 50,200 --variety-count 100

# Chạy tải lớn có lặp lại tính median:
python scripts/benchmark.py --mode integration --seed 42 --volume-sizes 10000,100000,500000 --volume-iterations 3 --velocity-rates 500,1000 --velocity-messages 2000 --variety-count 500
```
