# SentinelAI — Báo Cáo Benchmark "3V" (Big Data Verification)

Tài liệu này tổng hợp kết quả đo kiểm hiệu năng theo 3 đặc trưng cốt lõi của Big Data (Volume, Velocity, Variety) được thực thi bởi kịch bản tự động `scripts/benchmark.py` và `SparkBatchETLJob`.

---

## 1. Tóm Tắt Kết Quả 3V

```
                ┌──────────────────────────────────────────────┐
                │             SENTINELAI 3V BENCHMARK           │
                ├──────────────────────────────────────────────┤
                │ Volume   │ 100K - 1M bản ghi                 │
                │          │ Throughput lên đến 163,792 rec/s  │
                ├──────────────────────────────────────────────┤
                │ Velocity │ Ingestion 10 - 1,000 msg/s        │
                │          │ Measured Rate: 410.6 msg/s        │
                ├──────────────────────────────────────────────┤
                │ Variety  │ CSV, JSON, JSONL, Parquet         │
                │          │ Multi-schema: products, reviews,  │
                │          │ prices, raw envelopes             │
                └──────────────────────────────────────────────┘
```

---

## 2. Chi Tiết Kiểm Thử Từng Chiều

### 2.1 Volume (Khối Lượng Dữ Liệu)
- **Kịch bản**: Đo thời gian nạp và xử lý qua Spark Batch ETL Job với quy mô 100K bản ghi và ngoại suy 1M bản ghi.
- **Kết quả đo lường**:
  - **100,000 bản ghi**:
    - Thời gian xử lý: **0.61 giây**
    - Throughput tính toán: **163,792 bản ghi / giây**
    - Bộ nhớ tiêu thụ: **44.9 MB**
  - **1,000,000 bản ghi**:
    - Thời gian xử lý: **5.80 giây**
    - Throughput tính toán: **172,413 bản ghi / giây**
    - Bộ nhớ tiêu thụ: **450.0 MB**

### 2.2 Velocity (Tốc Độ Dòng Dữ Liệu)
- **Kịch bản**: Bơm dữ liệu stream liên tục qua Stream Producer vào Kafka topic theo các mức target rate (10, 50, 200, 500, 1000 msg/s).
- **Kết quả đo lường**:
  - Target: **500 msg/s**
  - Actual Throughput: **410.6 msg/s**
  - Độ trễ trung bình: **2.43 ms / message**
  - Tỷ lệ mất gói / thất thoát: **0% (0 dropped)**

### 2.3 Variety (Đa Dạng Định Dạng & Cấu Trúc)
- Đã kiểm chứng việc đọc, xác thực và chuẩn hoá trên 4 định dạng phổ biến:
  1. **JSON Line (JSONL)**: Dữ liệu stream từ Kafka Bronze.
  2. **JSON**: Danh mục API sản phẩm và reviews.
  3. **CSV**: Dữ liệu lịch sử bảng giá và catalog tĩnh.
  4. **Parquet**: Lưu trữ nén cột tại tầng Silver và Gold.
- Tất cả định dạng đều được chuyển hoá thành công về lược đồ dữ liệu thống nhất (Canonical Schema) với 100% tỷ lệ thành công.

---

## 3. Cách Tái Hiện Benchmark

Để chạy lại toàn bộ benchmark và sinh báo cáo tự động:

```bash
# Chạy benchmark tự động qua Makefile
make benchmark

# Hoặc chạy trực tiếp script benchmark
python scripts/benchmark.py
```

Kết quả chi tiết được tự động cập nhật vào:
- `docs/reports/benchmark_report.md`
- `docs/reports/benchmark_3v.json`
- `docs/reports/3v_evidence.md`
