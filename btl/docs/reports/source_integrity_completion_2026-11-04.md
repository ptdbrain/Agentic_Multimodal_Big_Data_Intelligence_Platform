# Hoàn tất kế hoạch rà soát, sửa lỗi và3V — 04/11/2026

Đã hoàn tất các gate của kế hoạch trong phạm vi demo học thuật. **692 test qua,0 lỗi,0 skip**:630 local,60 integration,1 Tiki live,1 Airflow. Airflow có bằng chứng đủ7 task thực thi mới. Một reviewer độc lập tìm7 Important; tất cả đã sửa và có hồi quy RED→GREEN. Hai lỗi thực tế bổ sung ở pagination Tiki và test Airflow no-op cũng đã sửa.

## Bằng chứng theo yêu cầu

| Yêu cầu | Kết quả hiện hành |
|---|---|
| Rà soát logic/code/test/hardcode | [Audit](source_integrity_audit_2026-11-03.md) và [review cuối, quyết định, giới hạn](source_final_review_2026-11-04.md) |
| Sửa các lỗi đã xác nhận | Gate630/60/live/Airflow trong source_final_certified_*.xml; immutable evidence, source/currency guard, streaming JSON/topic/quarantine, checkpoint và provenance |
| Hai sàn thật | Tiki hai bộ20 sản phẩm/20 giá/65 review có raw HTTP hash; Lazada hai bản thu thật20 sản phẩm/20 giá ngày02/11 |
| Nguồn web độc lập | Hai bộ50 bài Tinhte, HTML hash kiểm chứng; schema/bảng/article ETL riêng, không biến thành review |
| Nguồn giả lập | Nhãn simulation/generated, seed/config và namespace riêng; mặc định real không hiển thị giả lập |
| Tối ưu có đo | Validator và PG upsert có before/after, cùng hash kết quả nghiệp vụ; không suy ra speedup toàn pipeline |
| Dashboard/monitoring |9 trang AppTest với PG/MinIO thật; người dùng xác nhận browser trang chính/Web Content và scope real; hai HTTP exporter thật hoạt động |
| Volume/Velocity |9 lượt Volume cuối cùng trên cùng source fingerprint;9 lượt Kafka/MinIO120s với workload lỗi4% phân bố đều |
| Tài liệu/review | [Runbook](../runbook.md), [review và toàn bộ rulings](source_final_review_2026-11-04.md), báo cáo này và status JSON có thể kiểm tra máy |

Luồng nguồn cuối qua Kafka → MinIO → Spark → Gold → PostgreSQL phục vụ40 sản phẩm,65 đánh giá và40 dòng giá mỗi lượt; Silver giữ80 quan sát giá sau hai lượt. Bài viết qua Kafka/MinIO/pandas/SQL,50 bài/lượt. Đây là **replay có nhãn** của dữ liệu đã thu; không đổi nhãn thành lần crawl mới. Lazada hiện challenge; không tuyên bố khả năng thu mới liên tục hoặc bịa raw HTTP chưa lưu của bản cũ.

## Volume trên code cuối

| Review input/lượt | Silver/lượt | Median3 lượt(s) | Peak cây tiến trình(MiB) |
|---:|---:|---:|---:|
| 10,000 | 9,200 | 36.6184 | 2250.08 |
| 100,000 | 92,000 | 115.3000 | 3017.21 |
| 1,000,000 | 920,000 | 921.7107 | 8283.47 |

Mỗi1M input có920.000 Silver,50.000 quarantine và30.000 duplicate. Tải là giả lập có nhãn; các lần lặp không phải quan sát thị trường độc lập. Thời gian loại generation/startup; RAM không gồm daemon dịch vụ. Source fingerprint cuối: f19cfa1472f39d8f9d59bb081c023eb795a1629988d5bd583beaf0da150d9d77.

## Velocity thật

| Mục tiêu msg/s | Tốc độ gửi thực3 lượt | End lag |
|---:|---|---:|
| 50 | 49.8 / 49.8 / 49.6 | 0 |
| 200 | 197.2 / 197.0 / 197.3 | 0 |
| 500 | 479.2 / 479.5 / 479.4 | 0 |

Các lượt đối soát đủ bản tin, không mất dữ liệu theo phạm vi đo. Consumer batching làm latency lớn; không chứng nhận SLA500msg/s. Velocity đo Kafka/MinIO, không gồm Spark/SQL/UI.

## Trả lời câu hỏi Variety

Chỉ thu từ sàn thương mại **có thể thể hiện Variety** nếu xử lý thực sự catalog/chuỗi giá có cấu trúc, JSON/HTML bán cấu trúc và văn bản review tự do. Nhiều sàn chỉ trực tiếp chứng minh nhiều nhà cung cấp; đổi cùng bảng sang CSV/JSON/Parquet không tự tạo đa dạng ngữ nghĩa. Dự án hiện có ba nhóm: commerce thật, giả lập, web độc lập thật; chỉ hai nhóm là quan sát thật.

## Giới hạn và bàn giao

Dịch vụ native thật Kafka/MinIO/PostgreSQL/Elasticsearch, Spark local[2]; Docker được truy cập lại nhưng không dùng để đổi nhãn bằng chứng. Prometheus/Grafana deployment, layout browser toàn9 trang, scale phân tán, SLA sản xuất, FX và model training không được chứng nhận. Checkpoint v3/state source_group cần namespace mới; giữ bản cũ và replay có đối soát. Toàn bộ quyết định và chi phí nếu sai được ghi trong review cuối; không có deferred minor.

Dashboard: http://127.0.0.1:8502. Metrics: http://127.0.0.1:8000/metrics và http://127.0.0.1:8001/metrics. Không chỉnh AGENTS.md, không commit/push. Các báo cáo thất bại/checkpoint cũ được giữ nguyên; dùng status JSON04/11 làm chỉ mục hiện hành.
