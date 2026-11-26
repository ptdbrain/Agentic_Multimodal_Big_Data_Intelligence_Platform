# Nghiệm thu Giai đoạn 1 tối 26/11/2026

## Giá Tiki qua Kafka

Worker `c3ddca2` đọc topic `docker-tiki-prices` của hệ thống đang chạy. Lượt đầu ghi 120 quan sát VND thu từ Tiki, từ ngày 21 đến 26/11. Giá đến muộn được lưu làm dữ liệu nền. Giá đủ mới được xét theo cùng sản phẩm, nhà bán và tiền tệ, với tối thiểu ba ngày có quan sát trong bảy ngày trước. Điều kiện xét cảnh báo là quan sát thuộc ngày UTC hiện tại và được nhận trong vòng một giờ. Lịch thu thập của Tiki vẫn là lịch hằng ngày.

Kiểm thử trên Kafka, PostgreSQL và HTTP receiver riêng tạo đúng hai cảnh báo thống kê từ dữ liệu giả lập có nhãn. Phát lại cùng sự kiện giữ nguyên hai thông báo. Tại thời điểm kiểm tra, toàn bộ 120 quan sát Tiki thuộc nhóm lịch sử và có 0 cảnh báo giá mới trên hệ thống đang phục vụ.

Bản sao sau khi bật worker khôi phục được tám thành phần vào đích riêng. Hai bảng quan sát và offset giá có 120 hàng mỗi bảng, với checksum dữ liệu trùng kho đang phục vụ. Ba DAG phục hồi được giữ ở trạng thái tạm dừng. [Biên nhận phục hồi](nonai_native_handback_backup-20261126T122931-b0fbb725ba58.json) · [Số liệu đối chiếu](project_completion_evening_2026-11-26.json)

### Bật lại worker khi tái tạo container

Dựng image ứng dụng từ phiên bản có mã worker, đặt `PRICE_ALERT_IMAGE` thành tag đã dựng và chọn `PRICE_ALERT_DATASET=tiki`. Chạy Compose với cùng project và tệp môi trường của hệ thống đang vận hành. Ví dụ từ `btl/`:

```bash
PRICE_ALERT_DATASET=tiki PRICE_ALERT_IMAGE="$sentinel_release_image" \
  docker compose --env-file "$sentinel_compose_env" -p "$sentinel_project" \
  -f docker-compose.yml -f docker-compose.price-alerts.yml \
  up -d --no-deps --no-build alert-worker
```

Xem `docker compose logs alert-worker` sau khi worker tham gia consumer group. So số quan sát mới trong `price_stream_observations` và kết quả xử lý trong `price_stream_events`. Phần triển khai hiện tại dùng một worker cho một nguồn, với giới hạn bộ nhớ 256 MiB.

## Phiên bản Silver và Gold

Đọc lại hai phiên xuất bản của mỗi nguồn Tiki, web và giả lập từ MinIO. Đối chiếu SHA-256 của biên nhận, kích thước và checksum của từng đối tượng Parquet trước khi đếm. Mart giá Tiki đọc được 262 dòng ở phiên ngày 25/11 và 282 dòng ở phiên ngày 26/11. Bằng chứng này bao phủ định dạng Parquet có manifest. [Phiên bản, checksum và số dòng từng bảng](silver_gold_versions_2026-11-26.json)

Thử nghiệm riêng với Spark 3.5.0 và Delta Lake 3.1.0 trên MinIO staging. Hai bảng `silver.prices` và `gold.price_daily_stats` đọc đúng 262/282 dòng qua `versionAsOf` và khớp dữ liệu nguồn theo cả hai chiều. Phát lại cùng `txnAppId` và `txnVersion` giữ nguyên lịch sử. Cả hai bảng từ chối thêm cột theo mặc định, chỉ chấp nhận khi đặt `mergeSchema=true` và vẫn đọc được schema ở phiên cũ. Phép thử dùng một Spark driver. [Biên nhận thử nghiệm](delta_minio_pilot_2026-11-26.json) · [Tài liệu cấu hình lưu trữ Delta](https://docs.delta.io/delta-storage/)

Thử nghiệm mở rộng đã đối soát **39 phiên xuất bản** được lưu trước lượt phát lại thủ công của web. Cả **195 phiên bảng trên 15 bảng** có cùng schema và dữ liệu với bản sao đã kiểm tra. Mỗi bảng đọc được theo từng phiên và phát lại phiên cuối giữ nguyên lịch sử. Spark 3.5 gặp lỗi với timestamp nano giây của Parquet bài web. Phép thử chuyển đơn vị về micro giây trên bản sao, so khớp từng giá trị khi đổi ngược, và chỉ mở rộng schema khi cột `seller` xuất hiện ở phiên 8 của hai bảng Tiki. [Kết quả từng bảng và mã phiên](delta_history_shadow_2026-11-26.json)

## Truy vết trên dashboard

Commit `34884de` sửa việc đối chiếu giờ thu bài web khi Silver lưu UTC dưới dạng thiếu thông tin múi giờ. Mã lấy giờ gốc có múi giờ làm bằng chứng và yêu cầu giờ Bronze, giờ lưu Silver và giờ gốc khớp nhau. Bản đã triển khai truy vết được một review Tiki, một bài web và một review giả lập, đồng thời đối soát thành viên của một dòng mart giá Tiki.

Phiên web ban đầu xác minh được **498/500** bài. Hai bài ngày 12/11 khớp tệp Bronze gốc nhưng thiếu tệp này trong biên nhận. Sau khi thử trên staging, hệ thống phát lại hai bài qua Kafka với nhãn `snapshot_replay` và nguồn gốc `live`. Phiên mới truy vết được **500/500** bài. Nội dung, thời gian thu thập và 174 dòng tổng hợp Gold khớp phiên trước. Lượt phát lại thủ công được ghi riêng với các lượt thu hằng ngày. Sau sao lưu mới, cả hai bài vẫn truy vết được từ MinIO và PostgreSQL phục hồi. [Biên nhận phát lại](web_lineage_replay_2026-11-26.json) · [Biên nhận phục hồi](nonai_native_handback_backup-20261126T125447-5078b0008ded.json)

Mở trang [Truy vết dữ liệu](http://127.0.0.1:18502/lineage), chọn `articles` và nhập `article_75c88b4180ac7f07ef13867f` để xem một trường hợp đã xác minh. [Biên nhận truy vết ban đầu](lineage_live_2026-11-26.json)

## Hiệu năng và kiểm thử

- Mart lịch sử tổng hợp **1.033.036 dòng giao dịch thật** thành 604 dòng theo ngày. Trong 60 lần đọc có mở kết nối PostgreSQL, p50 là **10,4 ms** và p95 là **13,8 ms**. Trong 14 lượt mở trang bằng Chrome trên cùng máy, p50 là **515 ms** và p95 là **707 ms** đến khi KPI và nút CSV hiện. Phép đo dùng mart theo ngày sau một lượt làm nóng. [Số đo](project_completion_evening_2026-11-26.json)
- Commit `c3ddca2` vượt qua **73 kiểm thử integration** trên Docker staging và **120 kiểm thử Spark** với Java 17. Bộ integration bao gồm replay cảnh báo giá và cửa sổ trượt, với phạm vi loại ba nhóm `airflow`, `live` và `docker`. Commit `34884de` vượt qua lint và **760 kiểm thử unit**. Việc chạy CI trên GitHub Actions cho đúng bản phát hành vẫn chưa được xác nhận. [Kết quả và checksum JUnit](project_completion_evening_2026-11-26.json)
- Báo cáo chất lượng gần nhất của Tiki ghi nhận 20/20 giá, 20/20 sản phẩm và 263/263 review hợp lệ, không có bản ghi cách ly trong các lô đó. Dữ liệu giả lập có các tử số và mẫu số riêng. [Biên nhận chất lượng](project_completion_evening_2026-11-26.json)

## Đối chiếu 3V

| Tiêu chí | Bằng chứng | Phạm vi |
|---|---|---|
| Volume | 1.033.036 dòng giao dịch thực phân biệt của Online Retail II đi qua Kafka, Bronze, Spark và mart SQL. | Dữ liệu lịch sử của nhà bán lẻ tại Anh. |
| Velocity | Đo tải Kafka–Bronze với đầu vào giả lập, đạt khoảng 479 msg/s khi đặt mục tiêu 500 msg/s. Kiểm thử riêng xác nhận cửa sổ review trượt, watermark và replay. | Tiki và web được thu hằng ngày. Độ trễ khi có giá Tiki mới cần đo riêng. |
| Variety | Catalog, giá, review Tiki, bài web độc lập, luồng giả lập có nhãn và giao dịch lịch sử. Cùng một đầu vào kiểm chứng được ở CSV, JSON, JSONL và Parquet. | Nhiều sàn làm đa dạng nhà cung cấp. Nếu chỉ thu từ sàn, vẫn có thể chứng minh đa dạng kiểu dữ liệu qua catalog, thời gian giá và review, còn hợp đồng nguồn web và giả lập của dự án cần bằng chứng riêng. |

[Bằng chứng tải](three_v_assessment_2026-11-04.md) · [Đối soát giao dịch và định dạng](online_retail_ii_end_to_end_2026-11-26.json)

## Các điều kiện còn mở

- Báo cáo 19:15 của cửa sổ trước có năm chu kỳ hợp lệ mỗi nguồn, web đạt 50 bài/ngày và coverage giám sát **53,09%**. Backlog web tiếp tục có số đo riêng. Một cửa sổ bổ sung bắt đầu **19:32:18 ngày 26/11**, đủ 72 giờ sớm nhất **19:32:18 ngày 29/11** (giờ Việt Nam). Bộ xác minh ghi báo cáo mỗi giờ và giữ cả ba cửa sổ trước. Tiêu chí gồm ít nhất 99% mẫu giám sát, ba chu kỳ lịch hợp lệ mỗi nguồn và đủ quota web.
- Độ trễ từ sự kiện giá Tiki mới đến PostgreSQL cần đo ở chu kỳ thật tiếp theo. Kiểm thử worker với dữ liệu giả lập chứng minh đường xử lý, gửi HTTP và khử trùng.
- Chuyển hệ thống đang phục vụ sang Delta còn cần đưa đường xuất bản mới vào pipeline, kiểm soát các writer, diễn tập khôi phục và chứng minh có thể chuyển lại đường đọc cũ. Thử nghiệm 39 phiên dùng một Spark driver trên MinIO staging.
- Giai đoạn AI sẽ tiếp nối sau khi nghiệm thu Giai đoạn 1.
