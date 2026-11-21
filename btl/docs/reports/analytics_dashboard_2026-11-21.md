# Phân tích mẫu dữ liệu trên dashboard ngày 21/11/2026

Kết quả được đọc từ PostgreSQL của dashboard Docker lúc 11:58 giờ Việt Nam. Tại thời điểm đó, lần thu gần nhất trong danh mục Tiki và thư viện web là ngày 20/11. Dashboard tính lại các chỉ số theo dữ liệu và bộ lọc khi mở trang.

## Đánh giá Tiki

Trong 20 sản phẩm, 12 sản phẩm có điểm đánh giá hợp lệ. Điểm trung bình của 262 lượt chấm là **4,74/5**. Có **250/262 lượt (95,4%)** đạt từ 4 sao trở lên. Ba sản phẩm nhận nhiều lượt chấm nhất đóng góp **215/262 lượt (82,1%)**, nên điểm chung phản ánh chủ yếu trải nghiệm của người đánh giá ba sản phẩm này.

## Diễn biến giá

Cả 20 sản phẩm đều có ít nhất hai ngày giá bằng VND. So sánh giá trung bình ở ngày quan sát đầu và cuối của từng sản phẩm cho thấy **2 sản phẩm tăng, 18 có cùng mức giá ở hai đầu và 0 giảm**. Apple iPhone 17 Pro Max tăng từ **31.990.000 đồng** ngày 12/11 lên **32.500.000 đồng** ngày 20/11, tương đương **1,59%**. Có thể chọn sản phẩm trong thẻ **Lịch sử giá** để xem các ngày ở giữa.

## Nội dung web và dữ liệu giả lập

Thư viện có **302 bài**. Toàn bộ đến từ **tinhte.vn** và có ngày xuất bản. Mẫu này mô tả nội dung của một website. Tỷ trọng nguồn và số bài có ngày xuất bản được tính lại khi tìm theo từ khóa hoặc chọn nguồn.

Dataset giả lập có **10 sản phẩm và 160 lượt chấm**, điểm trung bình **3,23/5** tại thời điểm đối soát. Chọn phạm vi **Dữ liệu giả lập** trên thanh bên để xem độc lập với Tiki.

## Kiểm chứng

Đã so sánh kết quả Python với truy vấn SQL độc lập cho cỡ mẫu, điểm trung bình, tỷ lệ điểm cao, mức tập trung đánh giá, biến động giá và phân bố nguồn web. Chrome đã mở cả hai trang trên cổng 18502, chuyển sang thẻ lịch sử giá và kiểm tra các số hiển thị. Bộ kiểm thử liên quan đạt **40/40** trong container độc lập của chính image đã triển khai.
