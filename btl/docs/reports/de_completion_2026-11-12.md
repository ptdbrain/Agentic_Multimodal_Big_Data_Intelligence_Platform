# Nghiệm thu Data Engineering — 12/11/2026

## Kết luận hiện tại

Các phần triển khai và diễn tập bên dưới đã có bằng chứng chạy thật. Chưa đủ điều kiện tuyên bố hoàn thiện 100%: cần đủ 72 giờ vận hành cùng ít nhất 3 chu kỳ lịch thật cho mỗi nguồn; thu thập Tiki/web còn trạng thái partial do giới hạn phân trang/hàng đợi; bản sao lưu hiện cùng máy, chưa có đích ngoài máy hoặc diễn tập mất cả máy.

## Thay đổi và bằng chứng

| Phần | Kết quả đã xác minh |
|---|---|
| Ba nguồn độc lập | Tiki HTTP thật: 2 sản phẩm, 71 đánh giá; web thật: 2 bài; giả lập có nhãn: 10 sản phẩm, 160 đánh giá. Namespace, schema, bucket và lịch riêng. [Chi tiết](de_sources_2026-11-12.json). |
| Thu thập tăng dần | Cursor/backlog bền vững, chỉ cập nhật sau khi lưu; lỗi HTTP không trở thành trang rỗng thành công; retry thiếu sản phẩm và hàng đợi URL riêng. Không thay nguồn bị chặn bằng dữ liệu giả. |
| Bài viết | Current tích lũy, phiên bản nội dung bất biến, thống kê ngày bị ảnh hưởng; quan sát cũ không ghi đè nội dung mới. |
| Điều phối/phục hồi | Airflow LocalExecutor quản lý capture/process; nhận diện owner/PID thật, stage receipts và hash đầu ra; lease không bị thu hồi chỉ vì quá thời gian. |
| Kafka/checkpoint | Consumer dùng group cấu hình; chuyển group giữ đúng offset đã xác minh. Replay bằng KafkaSource, kiểm tra retention, restart/promote/rollback; mặc định failOnDataLoss. |
| Kafka chịu lỗi | 3 broker Docker, RF3/minISR2; 200 bản ghi được xác nhận, 200 nhận đủ và duy nhất khi một broker dừng. [Bằng chứng](de_docker_kafka_2026-11-12.json). |
| Spark chịu lỗi | 2 worker Docker, dừng một worker khi task thực sự đang chạy; 100.000 ID đúng và bằng chứng task chạy lại. [Bằng chứng](de_docker_spark_2026-11-12.json). |
| Consumer/storage/driver | MinIO dừng thì không commit offset; restart consumer thu đủ 4 bản ghi. Dừng cưỡng bức driver rồi resume checkpoint: 2 → 4, không trùng. [Bằng chứng](de_pipeline_faults_2026-11-12.json). |
| Giám sát | 20 rule; promtool kiểm tra firing/resolved/unknown/disabled. Prometheus/Alertmanager/webhook thật xác minh 5 loại cảnh báo phát sinh/phục hồi và silence bảo trì. [Bằng chứng](de_alert_delivery_2026-11-12.json). |
| Sao lưu/khôi phục | Toàn bộ warehouse gồm 3 schema nguồn, metadata Airflow thật, 3 bucket, checkpoint, registry và HTTP capture; khôi phục vào đích trống, giải mã Fernet bằng key giữ riêng, chạy LocalExecutor, resume Kafka/Spark 2 → 3. 17,421 giây cho toàn bộ restore/verify/resume trên tập nhỏ. [Bằng chứng](de_full_restore_generic_2026-11-12.json). |
| Vận hành định kỳ | 3 dịch vụ lõi và 7 dịch vụ giám sát có user systemd ownership; linger đã bật. Backup có fence 02:00, retention topic 03:00, cập nhật nghiệm thu mỗi giờ. Bản backup đầu đã xác minh; retention đầu có 0 topic đủ tuổi và không xóa gì. |

## Đo mở rộng trên một máy

12 lượt độc lập, mỗi cấu hình/kích thước 3 lượt, cùng snapshot mã và đầu vào giả lập có nhãn. Đã xác minh số worker thực sự được cấp task.

| Worker / CPU core | 10.000 đánh giá, trung vị | 100.000 đánh giá, trung vị |
|---|---:|---:|
| 1 / 2 | 41,38 giây | 116,04 giây |
| 2 / 4 | 44,73 giây | 118,51 giây |

[Báo cáo đầy đủ](de_worker_scaling_2026-11-12.json). Tăng worker chưa tạo cải thiện trong phép đo này. Khoảng đo từ Kafka dispatch đến SQL/DashboardDB thấy kết quả; không phải độ trễ từng sự kiện, thời gian render trình duyệt hoặc bằng chứng nhiều máy. Chỉ 3 mẫu mỗi nhóm nên p95/p99 chưa đủ làm SLA. Tài nguyên đo của driver không đại diện RAM toàn cụm.

## Kiểm thử hồi quy

Phiên bản mã cuối: **785 passed**, 67 deselected trong 281,87 giây cho bộ local, gồm Spark. Nhóm marker integration: **7 passed**, 9 deselected trong 17,94 giây, dùng PostgreSQL/MinIO và KafkaSource thật. Airflow LocalExecutor: **1 passed**, xác minh đủ 7 task mới trong 57,88 giây, chạy thủ công có nhãn rõ ràng. Ruff, kiểm tra cú pháp, promtool và git diff check đều đạt. [Bằng chứng kiểm thử](de_validation_2026-11-12.json), [Airflow](de_airflow_executor_2026-11-12.json).

Rà soát độc lập tìm thấy lỗi tiến trình chết giữa lúc lưu capture và cập nhật manifest. Đã tái hiện rồi sửa bằng biên nhận nguyên tử chứa đầu vào, nhận diện run/config và checksum. Hai kiểm thử ngắt ở hai thời điểm đều phục hồi không thu thập lại; ba kiểm thử thay đổi identity/nội dung/payload đều bị từ chối. Kiểm thử Airflow cũng được sửa để thực sự chạy DAG thủ công qua executor, thay cho backfill không còn phù hợp. Còn cảnh báo deprecation từ thư viện và cảnh báo định dạng thuộc tính JUnit; không có kiểm thử thất bại.

## Những điều cần chờ hoặc cấu hình thêm

- Cửa sổ theo dõi mới bắt đầu 22:36:04 ngày 12/11, giờ Việt Nam. Mốc xem xét sớm nhất 22:36:04 ngày 15/11. Báo cáo hiện 0 chu kỳ lịch thật cho cả 3 nguồn; lượt manual/diễn tập không được tính. [Trạng thái ban đầu](de_source_acceptance_2026-11-12.json).
- Lịch đầu tiên dự kiến 09:00 Tiki, 10:00 web, 11:00 giả lập ngày 13/11, múi giờ Asia/Ho_Chi_Minh. Máy và kết nối nguồn phải hoạt động. Báo cáo nghiệm thu đọc metadata Airflow, task state, hash capture, số đầu vào và mẫu up thật từ Prometheus.
- Thu thập partial vẫn hiển thị/cảnh báo đúng và chặn nghiệm thu đầy đủ; không nâng trạng thái thành complete để đạt chỉ tiêu.
- RPO mục tiêu 24 giờ, RTO mục tiêu 60 phút. Kết quả 17,421 giây chỉ cho diễn tập local nhỏ; cần bản sao ngoài máy, giữ key riêng an toàn và diễn tập mất broker/máy trước khi chứng nhận disaster recovery.
- Nguồn managed hiện dùng Kafka native một broker; cụm 3 broker Docker là môi trường diễn tập riêng. Cụm chịu lỗi Docker và benchmark standalone đều chạy trên cùng một máy; chưa chứng minh HA nhiều máy.

## Lệnh kiểm tra vận hành

```bash
systemctl --user status sentinel-source-scheduler.service
systemctl --user list-timers 'sentinel-*'
systemctl --user status sentinel-managed-backup.service sentinel-source-maintenance.service
journalctl --user -u sentinel-source-acceptance.service -n30
```

Hướng dẫn triển khai: [nguồn](../managed-source-jobs.md), [native services](../native-operations.md), [sao lưu](../operations-backup.md), [Kafka](../operations-kafka.md), [giám sát](../operations-monitoring.md), [phục hồi run](../run-recovery.md).


## Đánh giá 3V

Variety hiện có thương mại điện tử quan sát thật, bài viết web quan sát thật và luồng giả lập được gắn nhãn riêng; dữ liệu gồm sản phẩm, giá, đánh giá và nội dung bài viết. Nhiều bảng từ cùng một sàn chưa tự chứng minh đa dạng nhà cung cấp độc lập. Luồng giả lập phục vụ kiểm thử và đo tải, không được tính thành nguồn quan sát thật thứ ba.

Volume đã được đo với 100.000 đánh giá giả lập trong mỗi lượt tải lớn; lượng dữ liệu quan sát thật hiện nhỏ. Velocity có đường batch theo lịch và đường Kafka/Spark streaming đã thử checkpoint/chịu lỗi. Chưa có phép đo tải streaming liên tục dài ngày hoặc SLA dữ liệu thật, nên chưa kết luận đủ quy mô Big Data vận hành chỉ từ các diễn tập này.
