# Hoàn thiện giao diện SentinelAI — 05/11/2026

## Phạm vi triển khai

Giao diện Streamlit gồm trang chủ và tám trang chức năng dùng chung điều hướng, bảng màu, kiểu chữ và bố cục. Thiết kế dùng nền sáng, thanh bên tối và màu nhấn tím/xanh ngọc. Các trang dùng tiếng Việt, có hướng dẫn khi chưa có dữ liệu hoặc mất kết nối.

- Sản phẩm: tìm không dấu, lọc thương hiệu/danh mục/nguồn/điểm, xóa bộ lọc, phân trang 20 bản ghi và xuất CSV. Biểu đồ tách từng đơn vị tiền tệ, dùng thang giá riêng và loại giá thiếu đơn vị khỏi so sánh.
- Đánh giá: tìm nội dung, lọc điểm, phân bố điểm, xác minh mua hàng và đọc nội dung đầy đủ. Giá trị xác minh `0/1` từ cơ sở dữ liệu được hiểu đúng; giá trị thiếu giữ trạng thái chưa biết.
- Nội dung web: tìm bài, lọc nguồn, đọc nội dung và mở liên kết gốc. Thống kê lịch sử theo nguồn được phân biệt với kết quả tìm trong tối đa 1.000 bài gần nhất.
- Tổng quan, xu hướng, bất thường, chất lượng và luồng dữ liệu giữ thông tin nguồn, cửa sổ quan sát và dữ liệu thiếu. Cơ cấu danh mục lấy ngày thống kê gần nhất để tránh cộng lặp lịch sử.
- CSV dùng UTF-8 BOM và vô hiệu hóa chuỗi công thức khi mở bằng bảng tính. Các thay đổi xuất không sửa dữ liệu lưu trữ.

Phạm vi mặc định là **Dữ liệu thật**; dữ liệu giả lập/chưa kiểm chứng/tất cả vẫn được chọn rõ ràng. Không bổ sung số liệu mẫu để lấp khoảng trống. Cần Streamlit `>=1.66.0,<2.0`.

## Kiểm chứng

- 55 kiểm thử liên quan đạt; một kiểm thử Spark không thuộc phạm vi giao diện được loại bằng marker. Xem `dashboard_ui_tests_2026-11-05.xml`.
- AppTest chạy cả chín trang trên PostgreSQL và MinIO thực, dùng dữ liệu đã lưu: **40 sản phẩm, 65 đánh giá, 50 bài viết**. Xem `dashboard_ui_real_smoke_2026-11-05.json`. Không chạy lại thu thập hoặc thay thế dữ liệu.
- Ruff và kiểm tra khoảng trắng Git đạt.
- Dashboard phục vụ tại `http://127.0.0.1:8502`; endpoint sức khỏe và đường dẫn sản phẩm trả HTTP 200.

Công cụ trình duyệt báo `browser access host unavailable`, vì vậy chưa xác nhận bằng ảnh chụp hoặc thao tác chuột trên desktop/mobile. AppTest kiểm tra chức năng, không chứng minh bố cục pixel. Lượt review bằng agent riêng không chạy được do lỗi xác thực công cụ; đã rà soát trực tiếp mã giao diện.
