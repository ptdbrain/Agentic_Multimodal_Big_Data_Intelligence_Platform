# Hoàn thiện hệ thống không dùng AI

Trạng thái ngày 14/11/2026: đang triển khai. Chưa nghiệm thu toàn bộ kế hoạch. Giữ lại dữ liệu và triển khai native đang chạy; cấu hình Docker mới là môi trường staging độc lập.

## Những phần đã triển khai

- Registry kết nối trực tiếp từng dataset; lựa chọn thương mại/web độc lập theo phiên người dùng. Mỗi nguồn có snapshot SQL riêng, không đổi biến môi trường dùng chung giữa các phiên.
- Sink SQL cho cửa sổ Spark có generation, receipt theo batch và kiểm tra nội dung khi replay. Tiến độ phục vụ chờ các sink đạt cùng frontier nguồn.
- Cảnh báo thống kê dùng cửa sổ quá khứ đã đóng, tối thiểu 12 cửa sổ quan sát trong 24 giờ. Thiếu mẫu được ghi `insufficient_data`. Giá dùng quan sát ngày và tiền tệ gốc; chỉ phát cảnh báo khi có receipt DQ hợp lệ cho run và đầu vào giá.
- Cảnh báo lưu bằng chứng, phiên bản cấu hình, lịch sử chỉnh sửa và outbox. Receiver HTTP nội bộ lưu khóa chống gửi trùng qua lần khởi động lại.
- Outbox tìm kiếm nằm trong giao dịch xuất warehouse. Worker đọc snapshot có phân trang, xác minh từng tài liệu và tổng số trước khi đổi alias Elasticsearch. Giao diện đọc chỉ mục đã xác nhận, báo độ trễ so với SQL.
- Truy vết lưu snapshot Silver/Gold bất biến theo generation, kế thừa các tham chiếu Bronze có checksum. Giao diện đối chiếu sự kiện, giá trị và thời gian của bản ghi Silver với vị trí bản ghi Bronze; dữ liệu cũ thiếu định danh giữ trạng thái một phần. Recipe kiểm tra thành viên tổng hợp, số mẫu, trung bình, độ lệch chuẩn mẫu ổn định, tỷ lệ theo ngưỡng được lưu, giá catalog/quan sát và tiền tệ gốc; kết quả có phân trang và CSV. Các trường thiếu cấu hình lịch sử được liệt kê chưa đối soát, không tự điền ngưỡng cũ.
- Compose staging có hai Spark worker, app/stream/search/alert worker, Airflow, Kafka, MinIO, PostgreSQL, Elasticsearch và monitoring. Tổng giới hạn RAM cấu hình không vượt 16 GiB; đây chưa phải số RAM thực đo. Dịch vụ init chuẩn bị schema/bucket; DAG mới được tạo ở trạng thái paused.

## Bằng chứng đã chạy

- Bộ local không cần Spark/dịch vụ: **756 passed** ở lượt mới nhất (188 deselected); bộ này loại các test Spark/integration. Cần chạy lại sau sửa lỗi review nếu có.
- Bộ Spark local: **119 passed**; có JVM và sink Parquet thật.
- Gate native Kafka → Spark → Parquet → PostgreSQL: **2 passed**, gồm loại bản ghi sai, receipt SQL, tiến độ, replay checkpoint, late event trong watermark, loại quá hạn, và 12 cửa sổ nền → cảnh báo thống kê → receiver HTTP thật không gửi trùng. Dữ liệu workload được ghi nhãn giả lập.
- Gate PostgreSQL/Elasticsearch thật: [kết quả](reports/nonai_search_2026-11-13.json), gồm bulk, đối soát nội dung, alias, tìm kiếm và `search_after`. Ngoài tình huống tiêm lỗi transport, đã [dừng và khởi động lại tiến trình ES riêng](reports/nonai_search_outage_2026-11-13.json): alias cũ được giữ, outbox phục hồi từ 2 lên 3 sản phẩm và replay không tạo thêm công việc.
- Giám sát [SQL/MinIO/nhật ký ba nguồn qua Prometheus thật](reports/nonai_monitoring_2026-11-13.json) đã khớp truy vấn SQL độc lập; 7 rule được `promtool` xác nhận. Dashboard Grafana theo dataset đã cấu hình, chưa chứng nhận render trong Docker. Lỗi capture trước khi có SQL publication được đọc từ nhật ký có identity/config/target hợp lệ.
- [Chuỗi truy vết dữ liệu Tiki thật](reports/nonai_lineage_2026-11-14.json): sao chép nguyên bytes vào namespace nghiệm thu riêng, xuất SQL, đối soát bản ghi Silver/Bronze và kiểm tra tổng hợp với SQL gốc. Đây không phải một lần thu mới hoặc chu kỳ lịch mới.
- Đọc SQL ba nguồn thực tế: [kết quả](reports/nonai_serving_2026-11-13.json). Đây là kiểm tra routing, không phải chứng nhận đủ dữ liệu cho mọi phép phân tích.
- [AppTest 12 trang trên PostgreSQL thật](reports/nonai_dashboard_2026-11-13.json), cùng kiểm tra thao tác nút truy vết/recipe trên fixture local; kiểm thử HTTP loopback kiểm tra firing/resolved và chống gửi trùng. Đã kill tiến trình worker con sau khi receiver fsync nhưng trước HTTP response/xác nhận SQL: chạy lại chỉ lưu một receipt. Phép thử crash này dùng SQLite riêng, chưa phải PostgreSQL/Docker.
- [Smoke benchmark Kafka → Spark → MinIO/SQL](reports/nonai_stream_smoke_2026-11-14_retry3.json): target 10/s, warmup 2 giây và đo 5 giây, 70 ACK khớp 70 SQL, 50 mẫu đã thấy ở frontier cả window/quarantine. P95 khoảng 19,7 giây là cận trên do polling trên native đang chạy đồng thời gate Spark; không dùng làm chứng nhận hiệu năng Docker. Ba lần thất bại trước được giữ nguyên: [import](reports/nonai_stream_smoke_2026-11-14.json), [timeout](reports/nonai_stream_smoke_2026-11-14_retry1.json), [đối soát](reports/nonai_stream_smoke_2026-11-14_retry2.json). Bộ đo đã sửa import và điều kiện đòi audit tùy chọn.
- `ruff check .`, `git diff --check` và `docker compose ... config --quiet` đạt. Kiểm tra Compose không cần daemon và không thay thế build/start container.

Dữ liệu phát sinh trong kiểm thử được ghi nhãn giả lập và dùng namespace riêng. Các kết quả trên không chứng nhận 100.000 sự kiện/giây, HA nhiều máy hoặc độ trễ hiển thị trình duyệt.

## Khởi động staging Docker

Chạy từ thư mục `btl`, dùng Python 3.11 đã cài dependencies:

```bash
python scripts/prepare_application.py --directory ../.runtime/nonai-docker-private
# Bỏ qua bước tạo nếu thư mục này đã được chuẩn bị; lệnh từ chối ghi đè.
docker compose --env-file ../.runtime/nonai-docker-private/compose.env \
  -p sentinel-nonai config --quiet
docker compose --env-file ../.runtime/nonai-docker-private/compose.env \
  -p sentinel-nonai up -d --build
```

Dashboard staging: `http://127.0.0.1:18502`; Grafana staging: `http://127.0.0.1:13000`. Thư mục cấu hình có quyền 0700, tệp có quyền 0600. Không commit, sao chép vào báo cáo hoặc in nội dung các tệp bí mật. Streaming chỉ chạy khi bật profile `streaming`; DAG thu thập vẫn paused đến khi xác nhận quyền sở hữu nguồn và kế hoạch chuyển đổi.

## Điều kiện còn thiếu trước nghiệm thu

1. Docker socket hiện vẫn từ chối truy cập trong phiên agent. Cần build/start thật, kiểm tra init, restart, tài nguyên thực tế và cấu hình monitoring theo từng dataset.
2. Chứng nhận chi phí/quy mô truy vết trên chiến dịch lớn và xác định chính sách giữ snapshot. Winner, recipe, phân trang và đối chiếu Bronze thật đã có bằng chứng; cấu hình lịch sử không được ghi nhận vẫn để chưa xác định. Snapshot bất biến tăng dung lượng lưu trữ theo số generation.
3. Mở rộng thử crash cảnh báo sang PostgreSQL/Docker; kiểm tra bảo trì/khôi phục khi worker đang ghi và lặp lại late-update toàn tuyến trong Docker. Thử outage ES native và crash worker với SQLite đã đạt, cần lặp lại trong staging Docker.
4. Chạy lại chiến dịch batch 10k/100k/1m × 3; streaming 50/200/500 × 3 với warmup 30 giây, đo 120 giây, drain và đối soát. Tách độ trễ SQL khỏi độ trễ browser; ghi cả lần thất bại.
5. Diễn tập drain/fence → backup → restore Docker → resume, một owner duy nhất và rollback có đối soát dữ liệu sau cutover. Không chép offset/checkpoint qua cluster Kafka khác.
6. Fresh review toàn nhánh, các gate integration còn lại và tài liệu vận hành cuối cùng. Sau cutover mới đo **72 giờ và 3 chu kỳ lịch thực**, không thay bằng chạy ép hoặc chỉnh thời gian.

Backup ngoài máy và HA nhiều máy cần tài nguyên riêng; chưa được chứng nhận bởi stack một máy này.

## Đo streaming qua SQL

`scripts/benchmark_stream_serving.py` yêu cầu cấu hình Kafka/MinIO/PostgreSQL/Spark thật và `RUNTIME_MODE=integration` hoặc `production`. Mỗi lần tự tạo schema, bucket, topic và cấu hình riêng; không ghi vào dataset nguồn. Tài nguyên được giữ để kiểm tra/replay, topic có retention một ngày; cần dọn namespace benchmark sau khi nghiệm thu. Thư mục báo cáo ma trận phải chưa tồn tại.

```bash
python scripts/benchmark_stream_serving.py --output-dir /benchmark-reports/stream-campaign
# Mặc định: 50/200/500 reviews/s × 3; warmup 30s, đo 120s, drain tối đa 120s.
```

Chạy trong môi trường ứng dụng có cấu hình private và giới hạn tài nguyên Docker đã nghiệm thu. Script xác nhận ACK, SQL theo từng product/window và offset của mọi query được cấu hình, ghi cả thất bại. Latency là từ enqueue producer đến lần polling SQL xác nhận; tài nguyên chỉ tính client Python/JVM con, không phải toàn cluster. Không dùng `benchmark_fullpath.py --velocity-rates` để thay thế phép đo này vì lệnh đó không đo visibility SQL Spark.
