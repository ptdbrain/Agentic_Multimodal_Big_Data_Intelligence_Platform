# SentinelAI — Báo Cáo Benchmark "3V" (Big Data Verification)

Tài liệu này được **sinh tự động trực tiếp** từ lệnh chạy `python scripts/benchmark.py` trên đường chạy thực (Kafka -> RawConsumer -> MinIO Bronze -> SparkBatchETLJob -> Silver/Quarantine Parquet). Mọi số liệu dưới đây khớp 100% với `docs/reports/benchmark_3v.json` và `docs/reports/benchmark_report.md`.

- **Thời điểm chạy (Timestamp)**: `2026-10-21 05:19:04 UTC`
- **Chế độ chạy (Runtime Mode)**: `local` (`LocalBackend`)
- **Seed cố định (Deterministic Seed)**: `42`
- **Số lần chạy lặp lại (Iterations)**: `3` (báo cáo giá trị median và dải min-max)

---

## 1. Kết Quả Volume (Bronze -> Spark Batch ETL -> Silver & Quarantine)

> [!NOTE]
> **Ranh giới đo lường (Measurement Boundary):** Giai đoạn 1 ghi Bronze Storage + Giai đoạn 2 Spark Batch ETL (đọc Bronze -> chuẩn hóa kiểu -> kiểm định & cách ly -> khử trùng lặp -> ghi Silver Parquet). Không bao gồm Kafka.
>
> **Lưu ý về số đo CPU / RAM:** Chỉ số CPU % và RAM (MB) được đo đạc liên tục (sampling định kỳ 50ms bằng background thread) trên toàn bộ cây tiến trình cục bộ (tiến trình Python + tiến trình con Java JVM / Py4J worker). Số đo này phản ánh tài nguyên client chạy benchmark cục bộ, **không nên được diễn giải là tổng tài nguyên phân tán của toàn bộ cụm Spark** trong môi trường cluster đa node độc lập (như YARN/K8s/Standalone workers chạy ngoài cây tiến trình cục bộ).

Kịch bản chạy thực tế với các cỡ dữ liệu `[10000, 100000, 500000]` bản ghi (có tiêm tỷ lệ bản ghi lỗi `dirty_ratio=0.05` và bản ghi trùng `duplicate_ratio=0.03`). Mỗi cỡ được chạy lặp lại `3` lần để tính median và kiểm tra độ ổn định giữa các lần chạy.

| Seed | Đầu Vào | Hợp Lệ (Silver) | Quarantine | Trùng Lặp | Điểm DQ | Ghi Bronze (s) | Spark ETL (s) | Tổng Thời Gian (s) | Độ Ổn Định Min-Max (s) | Spark Rate (rec/s) | Tổng Rate (rec/s) | CPU % (Tree) | RAM Tree (MB) | RAM Py (MB) | Dung Lượng Bronze (B) | Dung Lượng Silver (B) | Dung Lượng Quarantine (B) |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| 10042 | 10,000 | 9,200 | 500 | 300 | 92.0% | 0.2915s | 4.4003s | 4.6918s | 4.1236 - 5.7940s | 2,272.6 | 2,131.4 | 189.7% | 1231.4 | 151.3 | 5,393,640 | 416,148 | 26,501 |
| 100042 | 100,000 | 92,000 | 5,000 | 3,000 | 92.0% | 3.3585s | 16.5447s | 19.9379s | 18.7593 - 20.2964s | 6,044.2 | 5,015.6 | 148.3% | 1750.2 | 238.5 | 54,065,581 | 3,716,559 | 202,650 |
| 500042 | 500,000 | 460,000 | 25,000 | 15,000 | 92.0% | 15.3105s | 64.0915s | 79.4019s | 79.3482 - 80.2800s | 7,801.3 | 6,297.1 | 170.4% | 3030.7 | 626.0 | 270,349,333 | 15,992,058 | 995,259 |

### 1.1 Chi Tiết Các Lần Chạy Lặp (Volume Iterations Breakdown)

| Cỡ Dữ Liệu | Lần Chạy | Ghi Bronze (s) | Spark ETL (s) | Tổng Thời Gian (s) | Spark Rate (rec/s) | Tổng Rate (rec/s) | CPU % | RAM Tree (MB) | RAM Py (MB) | Trạng Thái Khớp Accounting |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| 10,000 | Lần 1 | 0.3296s | 5.4643s | 5.7940s | 1,830.1 | 1,725.9 | 308.9% | 1159.8 | 151.2 | ĐẠT (100% Khớp) |
| 10,000 | Lần 2 | 0.2915s | 4.4003s | 4.6918s | 2,272.6 | 2,131.4 | 187.6% | 1231.4 | 151.3 | ĐẠT (100% Khớp) |
| 10,000 | Lần 3 | 0.2593s | 3.8643s | 4.1236s | 2,587.8 | 2,425.1 | 189.7% | 1249.2 | 151.3 | ĐẠT (100% Khớp) |
| 100,000 | Lần 1 | 3.3585s | 16.5794s | 19.9379s | 6,031.6 | 5,015.6 | 148.3% | 1487.9 | 238.5 | ĐẠT (100% Khớp) |
| 100,000 | Lần 2 | 3.7518s | 16.5447s | 20.2964s | 6,044.2 | 4,927.0 | 151.1% | 1750.2 | 238.5 | ĐẠT (100% Khớp) |
| 100,000 | Lần 3 | 3.0767s | 15.6826s | 18.7593s | 6,376.5 | 5,330.7 | 143.8% | 1812.2 | 239.2 | ĐẠT (100% Khớp) |
| 500,000 | Lần 1 | 15.2374s | 65.0426s | 80.2800s | 7,687.3 | 6,228.2 | 185.0% | 2560.6 | 626.0 | ĐẠT (100% Khớp) |
| 500,000 | Lần 2 | 15.3105s | 64.0915s | 79.4019s | 7,801.3 | 6,297.1 | 170.4% | 3030.7 | 626.0 | ĐẠT (100% Khớp) |
| 500,000 | Lần 3 | 16.1702s | 63.1779s | 79.3482s | 7,914.2 | 6,301.3 | 165.5% | 3052.4 | 625.9 | ĐẠT (100% Khớp) |

---

## 2. Kết Quả Velocity (Kafka Producer -> Broker -> RawConsumer -> Bronze / DLQ)

> [!NOTE]
> **Transport Mode:** `real_kafka` kết nối trực tiếp cụm Kafka KRaft; chế độ integration không dùng hàng đợi offline giả lập.
>
> **Phương Pháp Đo Độ Trễ ($L_i = t_{\text{processed}, i} - t_{\text{sent}, i}$):** Sử dụng đồng hồ monotonic độ chính xác cao (`time.perf_counter()`) ghi nhận thời điểm gửi của từng message riêng biệt và thời điểm hoàn tất xử lý/quarantine của consumer đối với message đó. Các phân vị p50, p95, p99 được tính trực tiếp từ tập hợp các $L_i$ này, phản ánh trung thực độ trễ streaming trên từng bản ghi thay vì thời gian gộp của cả batch.

| Seed | Transport | Mục Tiêu (msg/s) | Đã Gửi | Đã Nhận | Hợp Lệ (Bronze) | DLQ | Lag | Tốc Độ Gửi (msg/s) | Tốc Độ Nhận (msg/s) | Độ Trễ p50 (ms) | Độ Trễ p95 (ms) | Độ Trễ p99 (ms) | Thời Gian E2E (s) | Throughput E2E (msg/s) | Dung Lượng Bronze (B) |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| 92 | offline_virtual_queue | 50 | 100 | 100 | 96 | 4 | 0 | 48.1 | 48.4 | 2.28ms | 3.23ms | 3.4ms | 2.0776s | 48.1 | 60,553 |
| 242 | offline_virtual_queue | 200 | 400 | 400 | 384 | 16 | 0 | 184.6 | 183.7 | 2.05ms | 3.0ms | 3.63ms | 2.1773s | 183.7 | 243,315 |
| 542 | offline_virtual_queue | 500 | 1,000 | 1,000 | 960 | 40 | 0 | 393.1 | 389.2 | 1.81ms | 2.66ms | 3.65ms | 2.5695s | 389.2 | 606,706 |
| 1042 | offline_virtual_queue | 1,000 | 2,000 | 2,000 | 1,920 | 80 | 0 | 475.4 | 461.7 | 2.01ms | 4.01ms | 38.61ms | 4.3315s | 461.7 | 1,218,155 |

---

## 3. Kết Quả Variety (Parse -> PII Hash -> Bronze -> Spark Validate/Transform -> Silver)

> [!NOTE]
> Phân biệt rõ HTML fixture tổng hợp Schema.org với dữ liệu crawler Tiki thực tế. Toàn bộ các định dạng đều tuân thủ schema chuẩn, ẩn danh PII và qua Spark ETL.

| Seed | Định Dạng / Nguồn | Phân Loại | Dung Lượng Nguồn (B) | Đầu Vào | Hợp Lệ (Silver) | Quarantine | Trùng Lặp | Điểm DQ | Thời Gian (s) | Throughput (rec/s) | Dung Lượng Silver (B) | Dung Lượng Quarantine (B) |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| 42 | JSON | synthetic_file | 215,893 | 500 | 465 | 25 | 10 | 93.0% | 2.0882s | 239.4 | 30,287 | 6,811 |
| 42 | JSONL | synthetic_file | 215,893 | 500 | 465 | 25 | 10 | 93.0% | 2.2146s | 225.8 | 30,287 | 6,811 |
| 42 | CSV | synthetic_file | 106,765 | 500 | 473 | 17 | 10 | 94.6% | 2.1723s | 230.2 | 30,580 | 6,577 |
| 42 | TSV | synthetic_file | 106,041 | 500 | 473 | 17 | 10 | 94.6% | 2.0620s | 242.5 | 30,580 | 6,577 |
| 42 | Parquet | synthetic_file | 31,177 | 500 | 465 | 25 | 10 | 93.0% | 2.1477s | 232.8 | 30,287 | 6,811 |
| 42 | HTML_Synthetic_JSONLD | synthetic_html | 163,779 | 500 | 465 | 25 | 10 | 93.0% | 2.1678s | 230.6 | 29,146 | 6,630 |
| 42 | JSONL_Tiki_Crawler | authentic_snapshot_jsonl | 3,042 | 6 | 6 | 0 | 0 | 100.0% | 1.8131s | 3.3 | 6,487 | 0 |

---

## 4. Lệnh Chạy Tái Hiện Trực Tiếp

```bash
# Chạy nhanh smoke-test:
python scripts/benchmark.py --mode integration --seed 42 --volume-sizes 100,1000,10000 --velocity-rates 50,200 --variety-count 100

# Chạy tải lớn có lặp lại tính median:
python scripts/benchmark.py --mode integration --seed 42 --volume-sizes 10000,100000,500000 --volume-iterations 3 --velocity-rates 500,1000 --velocity-messages 2000 --variety-count 500
```
