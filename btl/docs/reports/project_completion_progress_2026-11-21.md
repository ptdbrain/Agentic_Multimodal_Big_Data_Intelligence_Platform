# Tiến độ triển khai kế hoạch hoàn thiện ngày 21/11/2026

## Vận hành và các chu kỳ thực

- Sau reboot, lỗi gắn tệp cấu hình trên ổ NTFS khiến Prometheus và Alertmanager dừng với mã 127. Hai cấu hình hiện được đưa vào image qua Dockerfile riêng, có kiểm tra bằng promtool và amtool khi build. Hai container đã chạy với volume lịch sử ban đầu. Trong mạng cô lập, cùng hai image khởi động khi thiếu thư mục cấu hình trên host và gửi được cả trạng thái firing lẫn resolved tới receiver. [Biên nhận diễn tập](monitoring_config_restart_2026-11-21.json)
- Bốn job systemd do người dùng quản lý hiện khởi chạy từ thư mục home và đợi workspace, Python và Docker sẵn sàng. Nếu sau 120 giây vẫn thiếu phụ thuộc, journal ghi phụ thuộc bị thiếu. File cấu hình riêng của các dịch vụ ứng dụng vẫn nằm trên ổ dữ liệu. Nghiệm thu boot của toàn hệ thống cần diễn tập trên host hoặc staging tương đương.
- Sau khi bốn target đều up, cửa sổ quan sát mới bắt đầu lúc **23:43:37 ngày 21/11** giờ Việt Nam. Mốc đủ 72 giờ sớm nhất là **23:43:37 ngày 24/11**, còn phụ thuộc ba chu kỳ lịch của mỗi nguồn và ≥99% mẫu giám sát. Hai cửa sổ trước tiếp tục ghi nhận sự cố và kết quả theo tiêu chí cũ. Tiêu chí web 50 bài được dùng cho riêng cửa sổ mới theo quyết định ngày 21/11.
- Một lượt lịch thực của mỗi nguồn đã được xác minh trước cửa sổ mới. Bản sao sau các lượt đó đã khôi phục vào tám đích native biệt lập. Kho phục hồi có 335 bài web, 262 đánh giá Tiki và 160 đánh giá giả lập. Ba DAG phục hồi đều giữ trạng thái tạm dừng. [Biên nhận phục hồi](nonai_native_handback_backup-20261121T164818-f685ac8364db.json)

## Tiêu chí web và nhà bán Tiki

- Người dùng chọn 50 bài/ngày với backlog theo dõi riêng. Báo cáo nghiệm thu mới kiểm tra số ID bài phân biệt trong payload đã băm, số đầu vào, số Kafka giao và số Kafka đọc. Trạng thái `partial`, số URL chờ và retry hết lượt vẫn xuất hiện. Các lượt mới còn ghi số bài mới, bài đổi nội dung, bài giữ nguyên và quan sát cũ được giữ trong lịch sử.
- 20 phản hồi HTTP gốc Tiki ngày 21/11 được xác minh SHA-256 và phân tích lại. Bộ parser mới tìm được 20 nhà bán theo sản phẩm và giá quan sát, giữ đúng giá và tiền tệ. Snapshot SQL cũ sẽ thay đổi từ lần xuất bản tiếp theo có dữ liệu nhà bán. Dashboard hỗ trợ lọc và nhóm theo nhà bán hiện tại, đồng thời hiển thị số sản phẩm chưa biết nhà bán.

## Dữ liệu giao dịch lịch sử

Online Retail II của Daqing Chen, UCI Machine Learning Repository, cấp phép CC BY 4.0. Workbook chính thức chứa **1.067.371** dòng. Chuẩn hóa giữ **1.033.036** dòng phân biệt và loại **34.335** dòng trùng theo các trường nghiệp vụ sau khi bỏ Customer ID. Giá giữ GBP, giờ hóa đơn giữ độ chính xác gốc và bản hủy vẫn được gắn cờ. Đã kiểm tra cùng số dòng và tổng tiền bằng Spark và PostgreSQL từ tệp Parquet có checksum xác minh qua MinIO. Đường chạy dành cho dữ liệu lịch sử đã chứng minh bước chuẩn hóa, lưu và đối soát. Bằng chứng Kafka, Spark phân tán và dashboard cho dataset này vẫn cần bổ sung. [Nguồn UCI](https://archive.ics.uci.edu/dataset/502/online+retail+ii) · [Biên nhận kiểm tra dữ liệu](online_retail_ii_volume_2026-11-21.json)

Chuẩn bị lại tệp Parquet trong Python 3.11 có PyArrow và OpenPyXL bằng lệnh `python scripts/prepare_online_retail_ii.py /path/online-retail-ii.zip /path/new-output`. Giữ workbook trong thư mục dữ liệu riêng và dùng phần kết quả đã loại Customer ID cho các bước phía sau.

## Tiếp tục nghiệm thu

Cần kiểm tra lượt Tiki/web sau khi đổi mã, chạy ba chu kỳ theo lịch trong cửa sổ mới và nghiệm thu khả năng khởi động đầy đủ. Các mục còn mở gồm đường Kafka cho nguồn lịch sử, hiệu năng dashboard trên mẫu lớn, kiểm chứng hợp đồng cửa sổ trượt và cảnh báo giá, đường ghi phiên bản Silver/Gold và bản bàn giao cài sạch có kết quả CI. Người dùng chọn nghiệm thu Giai đoạn 1 trước khi mở pha AI.

## Dashboard và streaming trong bản cập nhật

- Trình duyệt Chrome đã mở các trang Phân tích cơ bản, Xu hướng và Nội dung web trên cổng 18502. Trang Xu hướng có chuỗi giá theo đồng tiền, số sản phẩm và thay đổi thành phần mẫu, cùng bộ lọc nhãn từ danh mục hiện tại. Dữ liệu thực ở thời điểm kiểm chứng hiển thị 335 bài web.
- Spark đã xử lý một tệp sự kiện thử có review trùng qua cửa sổ dài năm phút, trượt một phút. Tệp kết quả có đúng năm cửa sổ, mỗi cửa sổ chứa một review. Cấu hình tùy chọn đặt bằng `--window "5 minutes" --slide "1 minute"` và bước trượt được lưu trong cấu hình generation. Cụm đang phục vụ dùng cửa sổ cũ. Còn kiểm chứng cửa sổ trượt qua Kafka, SQL và resume trong namespace riêng.
- Tại thời điểm kiểm tra, bộ hồi quy phân tích và vận hành có 86 test đạt. Lượt unit toàn repo có 751 test đạt. Repo đã đổi các mục Makefile chạy pytest sang `python -m pytest` để dùng đúng môi trường Python khi đường dẫn môi trường cũ không còn.

- Đã tạo project Compose, volume và cấu hình riêng từ script khởi tạo. Cả 20 dịch vụ chạy, bốn target giám sát up, ba DAG nguồn tạm dừng trước khi thử. Một lần Airflow DAG test có nhãn manual đã xuất 10 sản phẩm và 160 review giả lập từ Kafka qua MinIO và Spark tới SQL. AppTest kiểm tra dashboard hiển thị đúng hai số trong phạm vi giả lập. [Biên nhận môi trường sạch](sentinel_fresh_stack_2026-11-21.json)
- Bộ kiểm thử Spark qua Makefile có 119 test đạt. Nhóm kiểm thử streaming, binding và Kafka migration có 36 test đạt khi chạy với Java 17 và Spark Kafka connector. Hai lần gọi ban đầu đã phát hiện thiếu runtime Java và connector ở câu lệnh kiểm chứng. Kết quả đạt được lấy từ lần chạy lại với cấu hình đúng.
- Kiểm tra điều hướng phát hiện lựa chọn phạm vi bị đặt lại khi chuyển trang. Giao diện hiện ghi lựa chọn theo phiên. Kiểm thử hồi quy chọn dữ liệu giả lập rồi chuyển sang trang Xu hướng xác nhận phạm vi được giữ.
