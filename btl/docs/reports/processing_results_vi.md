# Báo cáo kết quả xử lý và phân tích

Cập nhật ngày 29/10/2026. Code và các luồng kiểm thử dưới đây đã có bằng chứng thực thi. Phần triển khai và nghiệm thu chức năng theo kế hoạch đã hoàn tất; gate Docker đã đạt sau khi quyền socket được khôi phục.

## Kết quả kiểm chứng

| Phạm vi | Kết quả | Bằng chứng |
| --- | --- | --- |
| Local, gồm Spark JVM | **434 đạt**, 48 ngoài bộ chọn; không lỗi/bỏ qua | [JUnit local](remediation_processing_local.xml) |
| Bộ integration trên Docker | **46 đạt**; không lỗi/bỏ qua | [JUnit Docker](processing_docker_integration.xml) |
| Pipeline trên Docker, hai lượt | 560 thông điệp/lượt; hash nội dung Silver và cả bốn mart Gold khớp; DQ history 3→6 | [Replay Docker](processing_docker_pipeline_replay.json) |
| Airflow LocalExecutor | Hai DAG run, 14 task thành công trong các tiến trình riêng | [Airflow](processing_native_airflow.json) |
| Dashboard với PostgreSQL thật | Bảy trang AppTest; oracle 15 sản phẩm, 20 review, rating 4,55 | [Dashboard Docker](processing_docker_dashboard.json) |
| Tải toàn tuyến qua dịch vụ thật | 100.000 review đầu vào; đối soát đúng 92.000 review phục vụ | [Phép đo](processing_native_fullpath_100k.json) |

Bộ integration gồm 15 ca trực tiếp dùng Kafka/MinIO/PostgreSQL/Elasticsearch, cùng các ca HTTP, nhánh local và kiểm tra lỗi. Các báo cáo thành phần có phạm vi chồng nhau; không cộng chúng thành tổng test độc lập.

## Những lỗi đã sửa

- Chuẩn hóa giữ giá trị gốc; giá/thời gian sai được đánh dấu hoặc cách ly. DQ phải đủ phép đo và khớp run/nguồn trước khi tạo Gold.
- Bốn mart kiểm tra grain, miền giá trị và thông tin nguồn trước khi xuất bản. Warehouse thay các snapshot ngày trong một giao dịch; dashboard đọc các truy vấn liên quan trong cùng snapshot.
- Thống kê dùng đúng mẫu, trọng số và ngày lịch. Cảnh báo lưu phương pháp, đơn vị, cỡ mẫu và trạng thái chất lượng; thiếu dữ liệu được thể hiện rõ.
- PostgreSQL: tránh `ALTER TABLE` không cần thiết gây chặn phiên đọc đang hoạt động.
- Script nghiệm thu Gold: yêu cầu Silver/DQ có bằng chứng producer thật; bỏ tự nạp dữ liệu mẫu khi Silver rỗng.
- Kafka: tiếp tục đọc các batch còn lại khi lần fetch đầu hết cửa sổ thời gian. Lỗi ban đầu đọc 62.104/110.100 thông điệp đã được tái hiện và sửa bằng regression dùng broker thật.
- MinIO: hai writer cùng tạo bucket được xử lý đúng khi bucket đã thuộc tài khoản và đọc được; lỗi quyền hoặc bucket không thuộc tài khoản vẫn bị từ chối.
- Giá tùy chọn: pipeline chấp nhận snapshot giá rỗng hợp lệ, xóa dữ liệu giá phục vụ cũ và vẫn hoàn tất với sản phẩm/review hợp lệ.

## Phép đo 100.000 review

Tổng 110.100 thông điệp qua Kafka gồm 100 sản phẩm, 100.000 review và 10.000 quan sát giá. Kết quả review: **92.000 Silver + 5.000 quarantine + 3.000 trùng**. Bốn mart có lần lượt 2.000/100/80/500 dòng, PostgreSQL và truy vấn dashboard đối soát đúng.

Thời gian **151.68 giây**, RAM đỉnh Python và cây tiến trình con **3206.13 MiB** (~3.13 GiB). Một lượt đo trên một máy, Spark `local[2]`; không tính bộ nhớ dịch vụ chạy riêng, tạo input, khởi động JVM hay tải trình duyệt. Đây là bằng chứng nghiệm thu trong phạm vi đo, chưa phải SLA production.

Fixture được gắn nhãn synthetic. Lần đo này dùng 5.000 timestamp lỗi; phép đo local cũ dùng rating lỗi, nên không coi hai phép đo là cùng workload để so sánh hiệu năng.

## Giới hạn và bước triển khai sau nghiệm thu

- **Docker đã nghiệm thu:** stack cách ly mới gồm Kafka, MinIO, PostgreSQL và Elasticsearch. Hai worker trong các container có IP riêng xác nhận chặn tranh chấp quyền ghi và nhận quyền sau khi owner giải phóng. Stack kiểm thử được giữ chạy để xem lại.
- Replay và AppTest trên Docker dùng source cuối cùng. Bằng chứng Airflow hai run/14 task thuộc lượt native trước các guard cuối; code handoff/lease không đổi. Các guard có regression và bộ kiểm thử cuối. Phép đo100k phía trên vẫn là native, không phải benchmark Docker.
- Backfill fixture đã được đối chiếu; việc áp dụng dữ liệu lịch sử production chưa thực hiện. Giá bất thường vẫn được ghi rõ là phân tích hồi cứu.
- Cảnh báo dependency và thuộc tính Compose `version` cũ còn hiển thị trong log.

Xem [nghiệm thu Docker và audit từng yêu cầu](processing_docker_acceptance.json) hoặc [tổng hợp bằng chứng dạng JSON](processing_service_acceptance.json) và [audit kế hoạch](processing_plan_remaining.md) để kiểm tra phạm vi, fingerprint và các giới hạn.
