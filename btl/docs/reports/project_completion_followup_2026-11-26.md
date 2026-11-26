# Kết quả kiểm chứng SentinelAI ngày 26/11/2026

## Giao dịch lịch sử trên dashboard

- Kafka xác nhận **1.033.036** dòng giao dịch Online Retail II. MinIO lưu 104 đối tượng Bronze. Từng lô được đọc lại và so toàn bộ dấu vân tay của payload, sau đó xác nhận offset cuối của ba partition. Thời gian truyền và xác minh Bronze là 335,911 giây.
- Cụm Spark Standalone đọc Bronze và so hai chiều với Parquet đã chuẩn hóa. Silver và tổng các ngày Gold đều khớp 1.033.036 dòng. Phép thử định dạng dùng 4.000 dòng cùng nguồn, với kết quả canonical giống nhau ở CSV, JSON, JSONL và Parquet. [Biên nhận Kafka và Spark](online_retail_ii_end_to_end_2026-11-26.json)
- Bảng PostgreSQL phục vụ 604 ngày gốc trong khoảng 01/12/2009–09/12/2011, có **19.104 dòng với mã hóa đơn hủy**. Xuất bản cùng phiên lần thứ hai không thêm dữ liệu. Dashboard cổng 18502 hiển thị GBP, có bộ lọc ngày và CSV. Chrome kiểm tra số liệu tổng và AppTest kiểm tra ngày 01/12/2009 có **3.186 dòng**.
- Pipeline lịch sử chạy trong Compose riêng. Kho đang phục vụ đọc mart lịch sử trong schema riêng. Dữ liệu gốc và đối tượng Bronze/Silver/Gold lịch sử nằm trong MinIO của staging. Bản sao kho live chứa mart SQL. [Nguồn Online Retail II của UCI](https://archive.ics.uci.edu/dataset/502/online+retail+ii)

Đường chạy gồm `scripts/prepare_online_retail_ii.py`, `scripts/archive_online_retail_ii.py`, `scripts/reconcile_retail_bronze.py` và `scripts/publish_retail_mart.py`. Lệnh cuối đọc hai biên nhận Kafka và Spark rồi xuất bản mart vào kết nối PostgreSQL cấu hình cho lần chạy.

## Nghiệm thu theo lịch và sao lưu

- Cửa sổ đo bắt đầu tối 21/11 đã tích lũy **5 chu kỳ hợp lệ mỗi nguồn**. Cả 5 lượt web đạt 50 bài phân biệt với biên nhận hợp lệ. Lượt mới nhất có 19 bài mới, 31 bài giữ nguyên, 1.022 URL chờ và 10 URL hết lượt thử. Kho web hiện có 500 bài. Tiki hiện có 20/20 sản phẩm có nhà bán.
- Báo cáo lúc 17:15 ghi **52,28%** mẫu giám sát đạt, dưới ngưỡng 99%. Trong đó có các khoảng ngắt dài 7,90–10,37 giờ. Mốc thời gian và biên nhận cũ được giữ để đối chiếu. Điều kiện còn thiếu là 72 giờ quan sát trên môi trường duy trì giám sát đủ liên tục.
- Bản sao mới được tạo sau khi các DAG đã rảnh. Diễn tập phục hồi xác minh tám thành phần và lịch sử sáu chu kỳ của từng nguồn kể từ lúc chuyển đổi. Truy vấn trên đích phục hồi xác nhận mart UCI có 604 ngày, 1.033.036 dòng và 19.104 dòng có mã hủy. Sao lưu định kỳ được cấu hình thử lại sau 3 phút khi thất bại, tối đa bốn lần khởi động trong 30 phút. [Biên nhận phục hồi](nonai_native_handback_backup-20261126T102647-8bfea4654ca9.json)

Các phần còn mở trước khi chốt Giai đoạn 1 gồm chứng minh cửa sổ trượt Kafka–SQL qua replay, cảnh báo giá mới theo đúng phạm vi thời gian thực, kiểm chứng đầy đủ phiên bản Silver/Gold của pipeline chính và phép đo giám sát 72 giờ đạt ngưỡng.
