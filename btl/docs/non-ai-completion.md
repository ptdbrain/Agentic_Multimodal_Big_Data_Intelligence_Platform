# Hoàn thiện hệ thống không dùng AI

Trạng thái ngày 19/11/2026: đang triển khai. Chưa nghiệm thu toàn bộ kế hoạch. Giữ lại dữ liệu và triển khai native đang chạy; cấu hình Docker mới là môi trường staging độc lập.

## Những phần đã triển khai

- Registry kết nối trực tiếp từng dataset; lựa chọn thương mại/web độc lập theo phiên người dùng. Mỗi nguồn có snapshot SQL riêng, không đổi biến môi trường dùng chung giữa các phiên.
- Sink SQL cho cửa sổ Spark có generation, receipt theo batch và kiểm tra nội dung khi replay. Các phiên bản đang chờ nằm ở `stream_pending_windows`; chỉ chuyển phiên bản phù hợp sang `stream_windows` cùng giao dịch xác nhận frontier và đánh giá cảnh báo. Batch chờ không ghi đè cửa sổ đang phục vụ. Các phiên bản đã chuyển được dọn khỏi vùng chờ.
- Cảnh báo thống kê dùng cửa sổ quá khứ đã đóng, tối thiểu 12 cửa sổ quan sát trong 24 giờ. Thiếu mẫu được ghi `insufficient_data`. Giá dùng quan sát ngày và tiền tệ gốc; chỉ phát cảnh báo khi có receipt DQ hợp lệ cho run và đầu vào giá.
- Cảnh báo lưu bằng chứng, phiên bản cấu hình, lịch sử chỉnh sửa và outbox. Receiver HTTP nội bộ lưu khóa chống gửi trùng qua lần khởi động lại; tự bỏ phần append cuối chưa có newline, giữ nguyên và báo lỗi nếu bản ghi đã hoàn chỉnh bị hỏng, fsync trước cả phản hồi nhận mới và nhận trùng.
- Outbox tìm kiếm nằm trong giao dịch xuất warehouse. Worker đọc snapshot có phân trang, xác minh từng tài liệu và tổng số trước khi đổi alias Elasticsearch. Giao diện đọc chỉ mục đã xác nhận, báo độ trễ so với SQL.
- Truy vết lưu snapshot Silver/Gold bất biến theo generation, kế thừa các tham chiếu Bronze có checksum. Giao diện đối chiếu sự kiện, giá trị bắt buộc, tiền tệ gốc và thời gian của bản ghi Silver với vị trí bản ghi Bronze; dữ liệu cũ thiếu định danh giữ trạng thái một phần. Recipe kiểm tra thành viên tổng hợp, số mẫu, trung bình, độ lệch chuẩn mẫu ổn định, tỷ lệ theo ngưỡng được lưu, giá catalog/quan sát và tiền tệ gốc; kết quả có phân trang và CSV. Các trường thiếu cấu hình lịch sử được liệt kê chưa đối soát, không tự điền ngưỡng cũ.
- Compose staging có hai Spark worker, app/stream/search/alert worker, Airflow, Kafka, MinIO, PostgreSQL, Elasticsearch và monitoring. Tổng giới hạn RAM cấu hình không vượt 16 GiB; đây chưa phải số RAM thực đo. Dịch vụ init chuẩn bị schema/bucket; DAG mới được tạo ở trạng thái paused.

## Bằng chứng đã chạy

- [Gate cuối ngày 19/11](reports/nonai_validation_2026-11-19.json): **957 passed, 1 deselected** trong môi trường dự án, gồm Spark/native integration; test Airflow được chạy riêng và đạt **1 passed**. Lượt cuối không ghi nhận failure. Có cảnh báo deprecation từ dependency, chưa phải lỗi logic. Dữ liệu fixture và các namespace kiểm thử được tách khỏi nguồn đang phục vụ.

- Bộ local không cần Spark/dịch vụ: **770 passed** ở lượt mới nhất (188 deselected); bộ này loại các test Spark/integration. Lượt này đã chạy sau các bản sửa review.
- Bộ Spark local: **119 passed**; có JVM và sink Parquet thật.
- Gate native Kafka → Spark → Parquet → PostgreSQL: **2 passed**, gồm loại bản ghi sai, receipt SQL, tiến độ, replay checkpoint, late event trong watermark, loại quá hạn, và 12 cửa sổ nền → cảnh báo thống kê → receiver HTTP thật không gửi trùng. Dữ liệu workload được ghi nhãn giả lập.
- Gate PostgreSQL/Elasticsearch thật: [kết quả](reports/nonai_search_2026-11-13.json), gồm bulk, đối soát nội dung, alias, tìm kiếm và `search_after`. Ngoài tình huống tiêm lỗi transport, đã [dừng và khởi động lại tiến trình ES riêng](reports/nonai_search_outage_2026-11-13.json): alias cũ được giữ, outbox phục hồi từ 2 lên 3 sản phẩm và replay không tạo thêm công việc.
- Giám sát [SQL/MinIO/nhật ký ba nguồn qua Prometheus thật](reports/nonai_monitoring_2026-11-13.json) đã khớp truy vấn SQL độc lập; 7 rule được `promtool` xác nhận. Chrome đã hiển thị dashboard Grafana theo dataset, trang thương mại với 20 sản phẩm/262 đánh giá và thư viện 260 bài web. [Kiểm tra giao diện trên Docker](reports/nonai_docker_render_2026-11-19.json). Lỗi capture trước khi có SQL publication được đọc từ nhật ký có identity/config/target hợp lệ.
- [Chuỗi truy vết dữ liệu Tiki thật](reports/nonai_lineage_2026-11-19.json): sao chép nguyên bytes vào namespace nghiệm thu riêng, xuất SQL, đối soát bản ghi Silver/Bronze và kiểm tra tổng hợp với SQL gốc. Đây không phải một lần thu mới hoặc chu kỳ lịch mới.
- Đọc SQL ba nguồn thực tế: [kết quả](reports/nonai_serving_2026-11-13.json). Đây là kiểm tra routing, không phải chứng nhận đủ dữ liệu cho mọi phép phân tích.
- [AppTest 12 trang trên PostgreSQL thật](reports/nonai_dashboard_2026-11-13.json), cùng kiểm tra thao tác nút truy vết/recipe trên fixture local; kiểm thử HTTP loopback kiểm tra firing/resolved và chống gửi trùng. Đã kill tiến trình worker con sau khi receiver fsync nhưng trước HTTP response/xác nhận SQL: chạy lại chỉ lưu một receipt. Phép thử crash này dùng SQLite riêng, chưa phải PostgreSQL/Docker.
- [Smoke benchmark Kafka → Spark → MinIO/SQL](reports/nonai_stream_smoke_2026-11-14_retry3.json): target 10/s, warmup 2 giây và đo 5 giây, 70 ACK khớp 70 SQL, 50 mẫu đã thấy ở frontier cả window/quarantine. P95 khoảng 19,7 giây là cận trên do polling trên native đang chạy đồng thời gate Spark; không dùng làm chứng nhận hiệu năng Docker. Ba lần thất bại trước được giữ nguyên: [import](reports/nonai_stream_smoke_2026-11-14.json), [timeout](reports/nonai_stream_smoke_2026-11-14_retry1.json), [đối soát](reports/nonai_stream_smoke_2026-11-14_retry2.json). Bộ đo đã sửa import và điều kiện đòi audit tùy chọn.
- [Airflow LocalExecutor native](reports/nonai_airflow_2026-11-19.json): gate chẩn đoán đạt 1/1, bảy task thành công ngay lần thử đầu ở lượt mới. Metadata PostgreSQL riêng; payload task chạy `mode=test` với fixture phát sinh và file/SQLite local. Không tính là chu kỳ lịch thực hoặc chứng nhận DAG production/Docker.
- [Smoke MinIO/SQL sau sửa lỗi ngày 19/11](reports/nonai_stream_smoke_2026-11-19.json): 70 ACK khớp 70 SQL, 50 mẫu, p95 polling khoảng 8,7 giây. Đây vẫn là workload ngắn 10/s trong khi có kiểm thử khác chạy đồng thời; không dùng để so sánh tối ưu hiệu năng với lượt 14/11 hoặc thay chiến dịch chuẩn.
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

## Kiểm chứng Docker ngày 19/11

Staging đã khôi phục hai cơ sở dữ liệu, ba bucket và ba thư mục từ cùng bản sao lưu. PostgreSQL và Elasticsearch khớp số lượng ở từng nguồn. Tiki có 20 sản phẩm và 262 đánh giá, nguồn web có 260 bài, nguồn giả lập có 10 sản phẩm và 160 đánh giá. Bốn target Prometheus phản hồi và Spark đã thực thi trên cả hai worker. [Báo cáo khôi phục](reports/nonai_docker_restore_2026-11-19.json) · [Đối soát dịch vụ](reports/nonai_docker_functional_2026-11-19.json)

Trong lượt thử batch đầu, producer nhận 11.100 ACK nhưng consumer hết thời gian lúc chờ Kafka cấp phân vùng. Sau khi tách thời hạn cấp phân vùng và thời hạn đọc, lượt chạy lại hoàn tất trong 63,3 giây đo. PostgreSQL có 9.200 đánh giá sau khi loại 300 bản trùng và cách ly 500 bản lỗi. [Hai lượt thử 10.000 đánh giá giả lập](reports/nonai_docker_batch_pilot_2026-11-19.json)

Cấu hình mới tạo mã Kafka riêng cho mỗi triển khai. Với volume của image `apache/kafka:3.7.0` đã có sẵn, giữ nguyên mã bootstrap khi nâng cấp. Nếu metadata ghi `Some(<id>)`, biến `KAFKA_CLUSTER_ID` cần giá trị bên trong ngoặc. Sau khi khởi động lại, đối chiếu mã giao thức và offset từng phân vùng với trước khi nâng cấp.

### Hiệu năng batch trên Docker

| Đánh giá đầu vào mỗi lượt | Số lượt đạt | Đánh giá đã lưu mỗi lượt | Thời gian trung vị |
|---:|---:|---:|---:|
| 10.000 | 3/3 | 9.200 | 66,1 giây |
| 100.000 | 3/3 | 92.000 | 176,1 giây |
| 1.000.000 | 3/3 | 920.000 | 1.167,6 giây |

Mỗi lượt sử dụng nguồn giả lập có checksum, database, topic và bucket riêng. Từng lượt đối soát nội dung Kafka/Bronze, 3% bản trùng, 5% bản cách ly, số đánh giá ở Silver, các mart Gold và số bản ghi dashboard đọc từ SQL. Thời gian tính từ đọc nguồn đến đối soát SQL, sau khi khởi tạo JVM. Tổng bộ nhớ cao nhất quan sát được trên 19 container là 10,76 GiB, tính cả cache tệp. Trong 896 mẫu cgroup, bộ đếm tiến trình bị hệ điều hành dừng do thiếu RAM tăng 0. [Chín lượt đo và tài nguyên Docker](reports/nonai_docker_batch_matrix_2026-11-19.json)

### Hiệu năng streaming trên Docker

| Mục tiêu (đánh giá/giây) | Tốc độ phát thực tế trung vị | Độ trễ SQL p95¹ | ACK khớp SQL qua 3 lượt |
|---:|---:|---:|---:|
| 50 | 50,00 | 32,51 giây | 22.500 |
| 200 | 200,00 | 33,14 giây | 89.977 |
| 500 | 499,81 | 33,41 giây | 224.812 |

¹ Trung vị của p95 ở ba lượt. Phép đo bắt đầu khi producer đưa sự kiện vào hàng đợi và kết thúc ở lần đọc SQL đầu tiên thấy số đếm đã được tất cả các sink xác nhận. Spark dùng trigger 30 giây. Thời gian tải trang trình duyệt nằm ngoài phép đo.

Cả 9 lượt đã drain đến hết offset của từng phân vùng. Tổng cộng 337.289 đánh giá giả lập được Kafka xác nhận và PostgreSQL ghi nhận, gồm cả thời gian làm nóng. Đỉnh tổng bộ nhớ container quan sát được là 7,76 GiB qua 324 mẫu cgroup. [Kết quả từng lượt và phạm vi đo tài nguyên](reports/nonai_docker_stream_matrix_2026-11-19.json)

### Khôi phục và phát lại trên Docker

Ngày 19/11, nhóm kiểm thử đã dừng tiến trình gửi cảnh báo sau khi receiver ghi bền vững thông báo đầu tiên và trước khi PostgreSQL nhận xác nhận. Bản sao lưu gồm hai database, bốn bucket và ba thư mục. Sau khi khôi phục sang hai database mới và một MinIO riêng, cả hai đầu ra Spark đọc tiếp từ offset 45 lên 47. Kho gốc vẫn có 45 đánh giá và hai thông báo chờ. Bản khôi phục ghi nhận đúng hai khóa thông báo duy nhất, lần gửi lại sau đó xử lý 0 mục. [Bằng chứng khôi phục và chạy tiếp](reports/nonai_docker_resume_2026-11-19.json)

Khi container Elasticsearch dừng, SQL ghi nhận lỗi lập chỉ mục. Sau khi container khởi động lại, alias cũ vẫn trả hai sản phẩm. Worker xử lý lại hai tác vụ, tìm thấy sản phẩm thứ ba và lần chạy tiếp theo xử lý 0 tác vụ. Phép thử dùng dataset giả lập riêng. [Bằng chứng sự cố Elasticsearch](reports/nonai_docker_search_outage_2026-11-19.json)

Bộ test toàn dự án chạy tối 19/11 đạt **973 passed, 1 deselected**. Bài được tách sang môi trường Airflow LocalExecutor đạt **1 passed**. Hai lượt có lần lượt 716 và 1 cảnh báo.

## Điều kiện còn thiếu trước nghiệm thu

1. Hai chiến dịch tải Docker đã có số đo tài nguyên. Cần kiểm tra cảnh báo khi nguồn ngừng cập nhật trên hệ thống sau chuyển đổi.
2. Chứng nhận chi phí/quy mô truy vết trên chiến dịch lớn và xác định chính sách giữ snapshot. Winner, recipe, phân trang và đối chiếu Bronze thật đã có bằng chứng; cấu hình lịch sử không được ghi nhận vẫn để chưa xác định. Snapshot bất biến tăng dung lượng lưu trữ theo số generation.
3. Docker đã qua phép thử crash worker gửi HTTP, khôi phục cửa sổ đang mở, xử lý hai sự kiện đến sau và thử outage Elasticsearch. Cần xác nhận sao lưu định kỳ của hệ thống chính sau chuyển đổi.
4. Batch 10k/100k/1m × 3 đã đạt trên bản build xác định bởi fingerprint mã nguồn của từng lượt. Streaming 50/200/500 × 3 đã hoàn tất với warmup 30 giây, đo 120 giây và đối soát toàn bộ ACK/SQL/offset. Độ trễ trình duyệt đầu cuối còn chưa đo.
5. Diễn tập drain/fence → backup → restore Docker → resume, một owner duy nhất và rollback có đối soát dữ liệu sau cutover. Không chép offset/checkpoint qua cluster Kafka khác.
6. Hoàn tất các gate Docker còn lại. Lượt kiểm tra native sau sửa lỗi review đã đạt. Review độc lập đã nêu 5 lỗi Important (đã sửa và có regression) cùng 1 Minor về type annotation (đã bổ sung hợp đồng kiểu cho các interface serving). Sau cutover mới đo **72 giờ và 3 chu kỳ lịch thực**, không thay bằng chạy ép hoặc chỉnh thời gian.

Backup ngoài máy và HA nhiều máy cần tài nguyên riêng; chưa được chứng nhận bởi stack một máy này.

## Đo streaming qua SQL

`scripts/benchmark_stream_serving.py` yêu cầu cấu hình Kafka/MinIO/PostgreSQL/Spark thật và `RUNTIME_MODE=integration` hoặc `production`. Mỗi lần tự tạo schema, bucket, topic và cấu hình riêng; không ghi vào dataset nguồn. Tài nguyên được giữ để kiểm tra/replay, topic có retention một ngày; cần dọn namespace benchmark sau khi nghiệm thu. Thư mục báo cáo ma trận phải chưa tồn tại.

```bash
python scripts/benchmark_stream_serving.py --output-dir /benchmark-reports/stream-campaign
# Mặc định: 50/200/500 reviews/s × 3; warmup 30s, đo 120s, drain tối đa 120s.
```

Chạy trong môi trường ứng dụng có cấu hình private và giới hạn tài nguyên Docker đã nghiệm thu. Script xác nhận ACK, SQL theo từng product/window và offset của mọi query được cấu hình, ghi cả thất bại. Latency là từ enqueue producer đến lần polling SQL xác nhận; tài nguyên chỉ tính client Python/JVM con, không phải toàn cluster. Không dùng `benchmark_fullpath.py --velocity-rates` để thay thế phép đo này vì lệnh đó không đo visibility SQL Spark.

## Sửa lỗi sau review ngày 19/11

[Review và phản hồi](reports/nonai_code_review_2026-11-19.md) ghi rõ phạm vi và giới hạn. Đã sửa: snapshot streaming hiển thị sớm/bỏ sót cảnh báo, thay đổi đường dẫn quarantine trong cùng generation, truy vết nhầm bản ghi thiếu giá trị/sai tiền tệ, journal receiver bị ghi dở và endpoint Elasticsearch sai trong Compose. Gate native còn phát hiện cache batch number sau restart khiến bỏ qua xác nhận SQL, cùng mã run Airflow chứa `+` bị từ chối; cả hai có kiểm thử hồi quy.

CLI hiện pin `serving_protocol=3`, mã cụm do Kafka trả về, mọi đường dẫn sink, output mode, ngưỡng và cấu hình nguồn/audit. Generation đăng ký theo cấu hình cũ phải được drain/retire và migration/replay có kiểm chứng sang generation mới; không tự sửa checkpoint hoặc coi những phiên bản đã bị ghi đè trước đây là có thể khôi phục. Giao diện lọc theo frontier cả với dữ liệu legacy. Không xóa checkpoint gốc để vượt qua lỗi cấu hình.

Các test Kafka lifecycle/E2E đã đọc tên topic từ cấu hình thay cho `raw.articles`/`raw.products`. Đây là sửa tính độc lập môi trường của kiểm thử; không thay đầu vào hoặc điều kiện đạt nghiệp vụ. Lượt bare pytest ban đầu có 937 đạt/8 lỗi; kết quả đó không được coi là xanh. Lượt cuối được tách theo hai môi trường Python của ứng dụng và Airflow, phù hợp cách đóng gói Docker.
