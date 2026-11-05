> Checkpoint lịch sử03/11, trước review/sửa cuối. Tiến độ hiện hành: [báo cáo hoàn tất04/11](source_integrity_completion_2026-11-04.md). Các số đo dưới đây không phải cohort Volume cuối.

# Đánh giá 3V và ranh giới bằng chứng — 03/11/2026

Hệ thống có bằng chứng thực nghiệm cho cả ba đặc tính ở quy mô demo một máy. Chưa đủ cơ sở gọi đây là triển khai Big Data sản xuất hoặc cam kết SLA.

## Variety: đa dạng nguồn và đa dạng dữ liệu

| Nhóm | Dữ liệu thực tế | Xử lý |
|---|---|---|
| Tiki/Lazada thật | Catalog/giá có cấu trúc, JSON API bán cấu trúc, review Tiki có điểm và văn bản tự do | Mapper theo sàn, schema/DQ, namespace identity, raw provenance, Spark/Gold |
| Giả lập | Catalog/giá/review/events do generator sinh | simulation/generated, seed/config/input hash; namespace riêng |
| Web độc lập | HTML Tinhte, tiêu đề/nội dung tự do, ngày xuất bản khi quan sát được | Schema article riêng, pandas ETL, lake/SQL/trang Web Content riêng |

**Chỉ thu từ sàn thương mại điện tử có thể thể hiện Variety**, nếu thực sự xử lý các dạng và ngữ nghĩa khác nhau như danh mục, chuỗi giá, JSON/HTML và văn bản review. Tuy nhiên, nhiều sàn chủ yếu chứng minh nhiều nhà cung cấp; cùng bảng đổi CSV/JSON/Parquet chỉ chứng minh định dạng. Chỉ có vài bảng sản phẩm giống nhau từ nhiều sàn chưa đủ chứng minh Variety mạnh.

Bài viết độc lập bổ sung ngữ cảnh biên tập/thảo luận ngoài thương mại. Generator phục vụ kiểm thử và tạo tải, không được tính thành nguồn quan sát thật thứ ba. Cách mô tả chính xác là **ba nhóm nguồn, trong đó hai nhóm quan sát thật và một nhóm giả lập**.

Tiki có hai lần thu HTTP mới, mỗi lần20 sản phẩm/20 giá/65 review. Lazada có hai lần thu thật ngày02/11, mỗi lần20 sản phẩm/20 giá; hiện challenge nên chưa xác nhận thu mới. Tinhte có hai lần50 bài thật. [Kiểm chứng cuối](source_native_verified_source_routes_2026-11-03.json) replay có nhãn và so hash bằng chứng; replay không phải lần crawl mới.

## Volume: đủ phép đo, phân biệt input và quan sát thật

Toàn tuyến JSONL → Kafka → MinIO Bronze → Spark Silver/DQ/dedup/quarantine → Gold/anomaly → PostgreSQL → DashboardDB,9/9 lượt hoàn tất:

| Review input/lượt | Silver | Quarantine | Duplicate | Median thời gian3 lượt | Peak cây tiến trình lớn nhất |
|---:|---:|---:|---:|---:|---:|
| 10.000 | 9.200 | 500 | 300 | 36,2946s | 2.130,82MiB |
| 100.000 | 92.000 | 5.000 | 3.000 | 115,0115s | 2.996,15MiB |
| 1.000.000 | 920.000 | 50.000 | 30.000 | 913,0989s | 8.387,02MiB |

Thêm100 sản phẩm và một quan sát giá mỗi10 review input. Dữ liệu đo tải là giả lập có nhãn; ba lần lặp cùng input hash không tạo thêm quan sát thị trường độc lập. Số đo loại startup/generation và RAM daemon dịch vụ; Spark local[2], không có bằng chứng scale nhiều máy. [Chi tiết9 lượt](source_native_volume_summary_2026-11-03.json).

## Velocity: tốc độ đo thực khác mục tiêu cấu hình

Kafka/MinIO thật, mỗi mức120 giây ×3, workload4% lỗi schema phân bố đều:

| Mục tiêu msg/s | Tốc độ gửi thực tế3 lượt | DLQ mỗi lượt | End lag |
|---:|---|---|---|
| 50 | 49,8 /49,8 /49,6 | 238 /238 /238 | 0 |
| 200 | 197,2 /197,0 /197,3 | 946 /945 /947 | 0 |
| 500 | 479,2 /479,5 /479,4 | 2.299 /2.301 /2.301 | 0 |

Cả9 lượt không mất bản tin đối soát. ACK nằm trong chu kỳ phát; không gán tốc độ cấu hình thành tốc độ đạt được. Consumer drain sau cửa sổ phát và latency chứa thời gian gom batch, p95 có thể xấp xỉ120 giây. Đường Velocity này không bao gồm Spark/SQL/UI, vì vậy không dùng nó chứng nhận độ trễ streaming toàn hệ thống. [Chi tiết9 lượt](source_native_velocity_summary_2026-11-03.json).

## Kết luận có điều kiện

Demo đáp ứng mô hình3V bằng đường dữ liệu và phép đo cụ thể, nhưng dữ liệu thị trường thật còn nhỏ; khả năng thu liên tục Lazada và browser rendering hiện chưa được nghiệm thu. Kết quả không chứng minh SLA500msg/s, dữ liệu thị trường triệu bản ghi hay khả năng phân tán. Việc tối ưu validator và PG được kiểm bằng business hash riêng, không gán thành speedup toàn hệ thống.

Tham chiếu khái niệm: [IBM — What is big data?](https://www.ibm.com/think/topics/big-data).
