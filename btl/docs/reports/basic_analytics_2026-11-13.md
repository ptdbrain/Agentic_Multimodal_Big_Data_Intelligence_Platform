# Phân tích cơ bản — 13/11/2026

## Kết quả triển khai

Đã thêm trang **Phân tích cơ bản** trong nhóm Phân tích dữ liệu, đường dẫn `/basic-analytics`. Trang sử dụng Streamlit hiện có, đọc một snapshot warehouse và không thay đổi dữ liệu nguồn.

- Tổng số sản phẩm, đánh giá đã thu, số mẫu điểm hợp lệ, điểm trung bình và tỷ lệ điểm cao/thấp.
- So sánh thương hiệu/danh mục bằng số sản phẩm, số mẫu đánh giá và điểm trung bình có trọng số theo mẫu.
- Giá danh mục trung bình, trung vị, thấp nhất/cao nhất và cỡ mẫu, tách theo tiền tệ.
- Xếp hạng điểm trong mẫu với số đánh giá tối thiểu do người dùng chọn.
- Phân bố sao và xu hướng điểm theo ngày, tái sử dụng trung bình trượt 7 ngày và thông tin độ bao phủ.
- Lịch sử giá trung bình ngày theo sản phẩm/tiền tệ; thay đổi giữa ngày đầu/cuối thực sự quan sát, chỉ khi có ít nhất hai ngày.
- Bộ lọc nhóm nguồn, thương hiệu, danh mục và khoảng ngày UTC; xuất CSV theo đúng kết quả đang xem.

## Phương pháp và giới hạn

Đây là thống kê mô tả trên dữ liệu đã thu. Chưa sử dụng AI, mô hình ngôn ngữ hay dự báo. Số đánh giá không được xem là số đơn bán; xếp hạng trong mẫu không đại diện toàn thị trường.

Ngày được lọc cả hai biên theo UTC; bản ghi thiếu ngày được báo riêng. Danh mục và giá danh mục thể hiện trạng thái hiện tại của kho, còn đánh giá/lịch sử giá chịu bộ lọc ngày. Thiếu mẫu giữ giá trị chưa có dữ liệu. Tiền tệ thiếu, giá không hợp lệ và ngày thiếu không được dùng để tạo biến động giá. Không tự điền lịch sử hoặc quy đổi tiền tệ.

Trang đọc `products`, `reviews`, `price_daily_stats` từ namespace warehouse đang cấu hình, không tự hợp nhất các schema nguồn độc lập. Giá lịch sử là giá trung bình từng ngày, không phải tick giá cuối ngày. Điểm lẻ tham gia tính trung bình nhưng không được làm tròn vào các cột sao nguyên; giao diện thông báo số mẫu này.

## Kiểm chứng

- Kiểm thử tính toán dùng fixture có nhãn rõ trong test và oracle số học độc lập: trọng số, dữ liệu thiếu, nguồn, tiền tệ, ngày UTC, giá không hợp lệ và một ngày lịch sử.
- AppTest kiểm tra điều hướng, bộ lọc thương hiệu/ngày, dữ liệu trống và CSV khớp sản phẩm đang xem.
- Rà soát độc lập phát hiện hai lỗi: tiền tệ `NaN` bị hiểu thành mã `NAN`, CSV biến động giá xuất cả các sản phẩm khác. Cả hai đã có regression tái hiện thất bại trước sửa và đạt sau sửa. Số dòng giá thiếu ngày cũng được hiển thị rõ.
- Nhóm giao diện/phân tích liên quan sau sửa: **38 passed**, gồm 11 kiểm thử mới. Bộ local toàn dự án: **796 passed**, 67 deselected, 321,89 giây, gồm Spark JVM. Ruff, compileall và kiểm tra khoảng trắng đạt. Còn 526 cảnh báo deprecation từ dependency. [Bằng chứng kiểm thử](basic_analytics_tests_2026-11-13.json).
- PostgreSQL thật, giao dịch chỉ đọc: **40 sản phẩm, 65 mẫu điểm**, trung bình **4,8153846154** khớp phép tính SQL độc lập; trang render qua AppTest không lỗi. Có 40 dòng giá theo ngày cho 40 lịch sử sản phẩm; mỗi lịch sử một ngày nên chưa có phần trăm biến động. [Bằng chứng](basic_analytics_real_2026-11-13.json).
- Browser host của phiên không khả dụng; chưa xác nhận bố cục pixel hoặc thao tác chuột. AppTest xác minh hành vi ứng dụng, HTTP kiểm tra dịch vụ hoạt động.

## Mở và vận hành

Mở http://127.0.0.1:8502/basic-analytics, hoặc chọn **Phân tích cơ bản** trong thanh bên. User service `sentinel-dashboard.service` đã được bật, bind loopback 8502; cấu hình PostgreSQL/private environment nằm ngoài Git.

```bash
systemctl --user status sentinel-dashboard.service
journalctl --user -u sentinel-dashboard.service -n 30
```

Xem [hướng dẫn dashboard](../dashboard.md), [thiết kế](../superpowers/specs/2026-11-13-basic-analytics-design.md) và [kế hoạch](../superpowers/plans/2026-11-13-basic-analytics.md).
