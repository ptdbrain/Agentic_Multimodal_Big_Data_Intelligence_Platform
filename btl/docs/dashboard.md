# Sử dụng dashboard

Chạy từ thư mục `btl/`, cài `requirements.txt`, cấu hình kho dữ liệu theo `.env.example`, sau đó:

```bash
streamlit run dashboard/app.py --server.port 8502
```

Mở http://127.0.0.1:8502. Trong môi trường integration, PostgreSQL phải hoạt động; trang Luồng dữ liệu còn đọc MinIO. Chọn chế độ local một cách tường minh nếu dùng kho SQLite theo hướng dẫn cấu hình dự án.

- Dùng thanh bên để chuyển giữa tổng quan, phân tích và vận hành.
- **Phạm vi dữ liệu** mặc định là dữ liệu thật. Bản phát lại giữ nhãn và thời gian gốc. Dữ liệu giả lập và chưa kiểm chứng có phạm vi riêng.
- Trang Sản phẩm hỗ trợ tìm không dấu, phối hợp bộ lọc và phân trang. Xuất CSV chứa toàn bộ kết quả phù hợp, không chỉ trang đang xem.
- Trang Đánh giá và Nội dung web cho phép tìm kiếm, lọc, xuất CSV và mở nội dung đầy đủ. Thư viện web giới hạn tối đa 1.000 bài gần nhất.
- Bấm **Làm mới dữ liệu** để đọc lại dữ liệu đã lưu; thao tác này không khởi chạy thu thập.
- Mở phần thông tin chất lượng/cửa sổ quan sát khi đọc xu hướng. Giá trị chưa biết giữ nguyên; ngày thiếu không tự trở thành 0.
- Biểu đồ giá chia theo đơn vị tiền tệ và không tự quy đổi. Trạng thái tích cực/tiêu cực của đánh giá được phân theo điểm số.

Thiết kế có quy tắc thích ứng màn hình hẹp. Kiểm chứng chức năng và giới hạn kiểm tra hình ảnh được ghi trong [báo cáo giao diện](reports/dashboard_ui_completion_2026-11-05.md).

## Phân tích cơ bản

Chọn **Phân tích dữ liệu → Phân tích cơ bản**, hoặc mở `/basic-analytics`.

- Chọn một nhóm nguồn, thương hiệu và danh mục. Các thống kê, biểu đồ và CSV cùng dùng bộ lọc này. Dữ liệu thật, giả lập và chưa kiểm chứng được tính riêng.
- Thẻ tổng hợp hiển thị số sản phẩm hiện tại, số đánh giá đã thu, điểm trung bình và tỷ lệ điểm 4–5. Điểm trung bình chỉ dùng mẫu hợp lệ 1–5; số đánh giá trên sàn không thay thế số đánh giá đã thu. Tỷ lệ điểm 1–2 có trong phần phương pháp.
- **So sánh & thống kê**: số sản phẩm, số mẫu điểm và điểm trung bình theo thương hiệu/danh mục; bảng giá trung bình/trung vị/min/max tách từng tiền tệ. Xếp hạng trong mẫu có ngưỡng số đánh giá tối thiểu do người dùng chọn.
- **Đánh giá & xu hướng**: phân bố sao và điểm theo ngày; trung bình trượt 7 ngày có trọng số theo số mẫu, kèm số ngày quan sát/thiếu và trạng thái đủ dữ liệu. Điểm lẻ không tự làm tròn vào cột sao nguyên và được thông báo riêng.
- **Lịch sử giá**: chọn sản phẩm để xem giá trung bình ngày, số quan sát nếu có và thay đổi giữa ngày đầu/cuối quan sát trong khoảng chọn. Mỗi tiền tệ có thang giá riêng. Một ngày giữ biến động là chưa có dữ liệu. CSV giá chỉ xuất sản phẩm đang xem.
- Bật **Lọc theo khoảng thời gian** để lọc ngày đánh giá/ngày giá, tính cả hai ngày biên theo UTC. Danh mục sản phẩm và giá danh mục vẫn là trạng thái hiện tại; không phải số tồn kho hoặc doanh số lịch sử. Bản ghi thiếu ngày không tham gia khoảng ngày/đồ thị, được báo trong phần độ bao phủ.

Trang đọc `products`, `reviews`, `price_daily_stats` từ cùng một snapshot kết nối warehouse, không ghi dữ liệu hay tự nạp fixture. Bản báo cáo chỉ phản ánh namespace warehouse đang cấu hình; không tự hợp nhất các schema nguồn độc lập. Module tính toán là `analytics/basic.py`; phương pháp giữ giá trị chưa biết, không nội suy ngày thiếu hoặc tự quy đổi tiền tệ.

### Chạy bằng user service

Template `orchestration/systemd/sentinel-dashboard.service` chạy dashboard tại `127.0.0.1:8502`. Private `~/.config/sentinel/dashboard.env` (mode 0600) chứa `SENTINEL_PROJECT_DIR`, `SENTINEL_PROJECT_PYTHON`, cấu hình PostgreSQL/runtime và `DASHBOARD_DATA_SCOPE=real`. Sau khi cài template:

```bash
systemctl --user daemon-reload
systemctl --user enable --now sentinel-dashboard.service
systemctl --user status sentinel-dashboard.service
```

Giữ mật khẩu ngoài Git và không chạy đồng thời hai tiến trình trên cùng cổng. Đối chiếu namespace trước khi cấu hình `PGOPTIONS` hoặc đổi warehouse.
