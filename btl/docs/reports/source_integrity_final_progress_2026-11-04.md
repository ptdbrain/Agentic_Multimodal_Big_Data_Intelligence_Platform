> Đã hoàn tất. Bản tiến độ dưới đây được giữ làm lịch sử; xem [báo cáo cuối](source_integrity_completion_2026-11-04.md) và [status máy](source_integrity_completion_status_2026-11-04.json).

# Kiểm chứng cuối — đang chờ đủ ma trận Volume

Code đã đóng sau một lượt review độc lập và sửa hồi quy. Các kiểm thử hiện hành
là **630 local +60 integration +1 live +1 Airflow =692 passed,0 failed,0 skipped**.
Airflow kiểm tra đủ7 task mới, tránh false-positive khi backfill cũ đã hoàn thành.

Hai bộ Tiki đã hoàn tất20 sản phẩm/20 giá/65 review, schema và raw HTTP hash hợp lệ;
unit từ source field/token/HTML, không mặc định VND. Hai bản Lazada thật20/20 được
giữ từ02/11; hiện challenge và đường kiểm chứng là replay có nhãn. Hai bộ50 bài
Tinhte được reparse từ HTML hash-verified, giữ clock nguồn gốc.

Sau sửa, hai lượt nguồn thật qua Kafka/MinIO/Spark/Gold/PG đúng40 sản phẩm/65 review/
40 dòng Gold giá; Silver có80 quan sát giá sau hai lượt. Hai lượt article50 bản
qua Kafka/MinIO/pandas/PG. All9 page AppTest, HTTP dashboard8502 và exporter8000/8001
đều qua; người dùng đã xác nhận browser trang chính/Web Content và mặc định real.

**Còn chờ:** đủ9 lượt Volume cuối trên code có guard SQL mới, rồi kiểm tra đối chiếu
từng yêu cầu và xuất báo cáo hoàn tất/status JSON. Bộ9 Velocity thật120s đã hoàn
thành, actual49,6–49,8 /197,0–197,3 /479,2–479,5, không gán target thành actual.
Đọc trạng thái từng run trong source_native_volume_final_matrix_2026-11-03;
file có mặt không có nghĩa đã đạt.

[Review, sửa lỗi và mọi quyết định/giới hạn](source_final_review_2026-11-04.md).
Chưa tuyên bố toàn bộ kế hoạch hoàn tất trước khi đủ gate đo tải cuối.
Không sửa AGENTS.md, không commit/push.
