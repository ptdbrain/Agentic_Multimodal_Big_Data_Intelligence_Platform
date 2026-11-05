# Review độc lập và sửa hồi quy cuối — 04/11/2026

Một reviewer mới, không kế thừa lịch sử hội thoại, đọc toàn bộ thay đổi tracked/untracked và tự tái hiện trong thư mục tạm. Không thay đổi checkout, dịch vụ hoặc Git. Kết quả: **0 Critical, 7 Important, 0 Minor**. Không có lượt re-review thứ hai; từng sửa được xác minh bằng RED→GREEN và gate rộng.

## Phát hiện và xử lý

| Phát hiện Important | Sửa | Bằng chứng hồi quy |
|---|---|---|
| Retry article run đã commit sửa quarantine/quality lịch sử | Kiểm tra run ID trước mọi ghi evidence, khóa theo run trên local/PG; failed attempt có namespace UUID riêng | Article duplicate valid/invalid, failed attempts, race hai writer; khóa bị bỏ tạm làm test race thất bại |
| SQL bỏ qua class NULL/rỗng/unverified đang tồn tại | Coi legacy unknown là class có dữ liệu, chặn class khác vào cùng namespace | Ba trường hợp class + PG/MinIO thật; không nới guard để chiều fixture cũ |
| Guard tiền tệ bỏ qua NULL và mart chỉ có avg_price | Kiểm tra catalog giữ lại, mart giữ lại và monetary frames mới trong transaction; bỏ dòng hoàn toàn không có số tiền | Unknown-unit/Gold-only RED→GREEN, actual PG rollback giữ snapshot |
| JSON flat bị decoder bỏ ID/rating/body | Schema nhận root và payload; chọn theo từng dòng, payload object hiện diện là nguồn business authoritative | Kafka-shaped JSON và JSON file stream thật, không chỉ DataFrame đã decode |
| Topic tùy chọn bị lọc mất cả valid lẫn invalid | Subscription quyết định topic; bỏ filter hardcode raw.reviews | Unit raw custom topic; integration Kafka/restart không monkeypatch default để che lỗi |
| Quarantine hash sau projection làm mất phiên bản | Fingerprint trước projection; lưu payload sửa được theo chính sách PII, tọa độ Kafka/file; retry không đổi identity vì offset | User-ID-only variants, sanitized repair payload, repeated delivery dedup |
| Tiki tự gán VND cho USD hoặc currency thiếu | Chỉ đọc unit khai báo/token; unknown giữ None; offer HTML bổ sung có giá/unit cùng response; category từ breadcrumb quan sát được | Unknown/suffix/visible token/HTML evidence tests; hai collection20/20/65 hợp lệ |

Hai phát hiện bổ sung từ chạy thật được sửa trong cùng fix pass:

- Tiki trả HTTP400 “Query limitation is: 20” khi client dùng mặc định50. Client chia trang tối đa20, chỉ trả các dòng quan sát được; test45 yêu cầu/45 có và45 yêu cầu/27 có đều qua, không lấp thiếu.
- Airflow backfill ngày cố định có thể trả0 mà không chạy task mới. Test cũ bị tái hiện RED bằng timestamp. Test mới chỉ reset trong metadata được xác nhận disposable, dùng lake/SQLite riêng cho mỗi lần, kiểm tra DAG run mới và đủ7 task success với start time mới. JUnit ghi executor, số task và thời gian thực thi.

Các fixture cũ dựa vào VND mặc định hoặc trộn source class được sửa thành input có unit/class rõ ràng. Không thay assert nghiệp vụ bằng assert lỏng hơn; giữ số đếm, privacy và kết quả cuối.

## Các giới hạn reviewer không tự kết luận và quyết định

1. **Browser9 trang:** chấp nhận AppTest9 trang và người dùng xác nhận trực tiếp trang chính/Web Content40 sản phẩm/65 review/50 bài, scope real. Không tuyên bố đã có screenshot hay review layout cả9 trang. Rủi ro còn lại là bố cục các trang chưa xem trực quan.
2. **Prometheus/Grafana triển khai:** acceptance ở mức exporter thật và cấu hình; deployment/rendering không thuộc gate này. Khi triển khai vẫn phải cấu hình địa chỉ exporter/datasource đúng mạng.
3. **Lazada và bằng chứng cũ:** giữ hai canonical capture thật đã thu; không bịa raw HTTP/parser chưa lưu. Website hiện challenge, nên đường cuối là replay có nhãn. Cần lần thu mới khi website cho phép nếu muốn xác nhận truy cập liên tục.
4. **Scale/SLA/FX:** ngoài phạm vi demo học thuật. Không suy ra scale phân tán, SLA500msg/s hay chuyển đổi tiền tệ tự động.
5. **Simulator lâu dài:** CLI xử lý batch hữu hạn; checkpoint chỉ bền counters/windows/dedup/config/source class, không đảm bảo lưu toàn bộ quarantine trong bộ nhớ qua restart. Muốn giữ payload giả lập phải xuất trước khi dừng. Kafka/Spark thật có quarantine bền riêng.

## Các quyết định triển khai khác

- Dùng branch riêng tại checkout hiện tại để giữ đường dẫn runtime; không chỉnh main/merge/push. Đổi lại, không có cách ly filesystem bằng worktree thứ hai.
- Một đơn vị tiền tệ cho mỗi namespace; dữ liệu nhiều unit phải tách hoặc có thiết kế FX sau này. Không điền unit cho tiền quan sát được nhưng chưa biết đơn vị.
- Dùng Kafka/MinIO/PG/ES native thật khi Docker mất quyền. Docker đã truy cập lại được, nhưng evidence native không đổi nhãn thành Docker. Dịch vụ native cần tài nguyên và dọn theo nhu cầu sau demo.
- Các cohort trước có fingerprint toàn repository khác nhau do sửa crawler/monitoring độc lập; không suy ra cùng binary hay speedup toàn tuyến. Ma trận Volume cuối được chạy lại sau guard SQL mới; Velocity không gọi các module sửa cuối.
- Checkpoint localv3 và state Spark có source_group dùng namespace mới. Giữ checkpoint cũ và replay có đối soát, không tự xóa hoặc tắt schema check; chi phí là cần bước migration.

**Deferred minors:** không có. Các quyết định trên là ranh giới công khai, không phải phát hiện lỗi bị giấu. Gate rộng và ma trận cuối được ghi riêng trong audit/status; chưa dùng báo cáo review này tự thay thế kết quả chạy.
