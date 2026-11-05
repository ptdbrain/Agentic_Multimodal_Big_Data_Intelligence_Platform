# Đánh giá3V trên bản sửa cuối — 04/11/2026

## Variety

Dự án có ba nhóm: commerce thật(Tiki/Lazada), giả lập có nhãn, và bài viết web
độc lập thật(Tinhte). Chỉ hai nhóm là quan sát thật. Catalog/giá có cấu trúc,
JSON API/HTML bán cấu trúc và văn bản review/bài viết tự do có schema/xử lý khác
nhau. Chỉ lấy từ sàn thương mại vẫn có thể thể hiện Variety nếu xử lý các dạng
và ngữ nghĩa này; nhiều sàn hoặc đổi CSV/JSON/Parquet tự nó chưa đủ.

Tiki có hai bộ20 sản phẩm/20 giá/65 review với raw HTTP hash và unit quan sát
được. Lazada có hai capture thật20/20 ngày02/11, hiện challenge. Tinhte có hai
bộ50 bài từ HTML gốc. Luồng cuối replay có nhãn, không gọi là crawl mới.
Generator không được tính thành quan sát thị trường thật.

## Volume

| Review input/lượt | Silver/lượt | Median3 lượt(s) | Peak cây tiến trình(MiB) |
|---:|---:|---:|---:|
| 10,000 | 9,200 | 36.6184 | 2250.08 |
| 100,000 | 92,000 | 115.3000 | 3017.21 |
| 1,000,000 | 920,000 | 921.7107 | 8283.47 |

Đủ9 lượt toàn tuyến nguồn/Kafka/MinIO/Spark/Gold/PG/DashboardDB. Mỗi1M input
có920k Silver,50k quarantine,30k duplicate; thêm100 sản phẩm và100k giá. Dữ liệu
đo tải là simulation, ba lần lặp cùng input hash không tạo ba bộ quan sát thật.
Cohort cuối có cùng fingerprint nguồn và khớp code hiện hành. Không gồm
generation/startup hay tài nguyên daemon dịch vụ; host dùng chung, không có
chứng nhận scale phân tán.

## Velocity

| Mục tiêu msg/s | Gửi thực3 lượt | End lag |
|---:|---|---:|
| 50 | 49.8 / 49.8 / 49.6 | 0 |
| 200 | 197.2 / 197.0 / 197.3 | 0 |
| 500 | 479.2 / 479.5 / 479.4 | 0 |

Đủ9 lượt120s qua Kafka/MinIO thật;4% lỗi schema phân bố đều, đối soát đủ, không
còn lag khi kết thúc. ACK nằm trong chu kỳ gửi, actual dưới target được giữ
nguyên. Consumer gom batch làm latency cao; đường đo không gồm Spark/SQL/UI.
Không tuyên bố SLA500msg/s. Các module sửa cuối không nằm trên đường Velocity;
báo cáo lưu fingerprint thực của từng lượt, không gọi cả repository là cùng
binary với cohort Volume cuối.

## Kết luận và bằng chứng

Demo có bằng chứng cho cả3V ở phạm vi đã đo; chưa chứng minh triệu quan sát
thị trường thật hay hệ thống Big Data sản xuất. Xem
[Volume cuối](source_native_volume_final_summary_2026-11-04.json),
[Velocity](source_native_velocity_summary_2026-11-03.json) và
[báo cáo hoàn tất](source_integrity_completion_2026-11-04.md).
Khái niệm tham chiếu: [IBM — What is big data?](https://www.ibm.com/think/topics/big-data).
