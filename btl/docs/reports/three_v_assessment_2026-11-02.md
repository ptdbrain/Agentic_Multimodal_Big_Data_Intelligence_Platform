# Đánh giá 3V — trạng thái triển khai hiện tại

Báo cáo này phân biệt bằng chứng lịch sử với các lượt chạy mới ngày02/11/2026. Đây chưa phải báo cáo benchmark cuối kế hoạch.

## Đính chính cách đọc báo cáo cũ

Trong `3v_evidence.md`, dòng Velocity vừa gọi phép đo là “Real Kafka”, vừa ghi `offline_virtual_queue`. Phép đo đó không chứng minh throughput/latency của broker Kafka thật. Giữ nguyên báo cáo lịch sử để truy vết; không dùng dòng đó làm bằng chứng Kafka. Các báo cáo integration và phép đo dịch vụ thật khác phải được đọc riêng theo phạm vi ghi trong từng artifact.

## Volume: mới có bằng chứng lịch sử trong phạm vi giới hạn

Lần đo native đã lưu trước đây dùng100.000review đầu vào, ghi rõ synthetic, một máy và Spark local[2]. Điều này không chứng minh có100.000review thu trực tiếp từ sàn. Đợt nguồn mới hiện chỉ có các bộ thu nhỏ; chưa chạy ma trận10k/100k/1M×3của kế hoạch. Chưa xác nhận năng lực phân tán hoặc SLA.

## Velocity: chưa có phép đo mới cho nguồn mới

Hai lượt crawl hoặc lấy dữ liệu nhanh không thay cho benchmark streaming. Cần Kafka thật, tốc độ producer thực đo, tốc độ commit bền vững, lag, latency và đối soát sau drain. Các mức50/200/500message/giây là tải mục tiêu, không phải kết quả đạt được. Docker hiện bị lỗi quyền socket nên chưa chạy được gate dịch vụ mới.

## Variety: phải xét nội dung, cấu trúc và nguồn gốc riêng

- Sản phẩm/giá là dữ liệu có cấu trúc.
- JSONAPI và HTML là đầu vào bán cấu trúc; phải xử lý thực tế, không chỉ đổi tên file.
- Nội dung review/bài viết là văn bản tự do, dù được đặt trong một trường củaJSON.
- Tiki và Lazada là hai nguồn sàn độc lập đã thu được sản phẩm/giá. Nguồn bài viếtTinhte đang được hoàn thiện. Generator là nguồn giả lập riêng, không phải quan sát thực.

Chỉ thu thập từ sàn TMĐT vẫn có thể thể hiện Variety nếu thực sự xử lý dữ liệu có cấu trúc, bán cấu trúc và văn bản tự do khác nhau. Ngược lại, nhiều sàn cùng schema hoặc xuất cùng bảng raCSV/JSON/Parquet không tự động chứng minh Variety cao. Cần ma trận trường/schema, parser, nghiệp vụ và bằng chứng xuyên tuyến.

Tham chiếu khái niệm: [IBM — What is big data?](https://www.ibm.com/think/topics/big-data).

## Điều kiện chốt báo cáo cuối

Chỉ kết luận từngVtheo phạm vi có kết quả đo; công bố thất bại và giới hạn. Nguồn mới phải hoàn tấtKafka→Bronze→Silver/Quarantine→Gold→serving. Hiện chưa được tuyên bố hoàn tất3Vtheo kế hoạch mới.
