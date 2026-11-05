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
