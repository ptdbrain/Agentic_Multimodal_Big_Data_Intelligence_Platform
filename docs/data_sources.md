# SentinelAI — Nguồn Dữ Liệu & Hướng Dẫn Thu Thập (Data Sources & Ingestion Guide)

Tài liệu này quy định danh mục các nguồn dữ liệu, nguyên tắc kỹ thuật, khung pháp lý & đạo đức và chính sách bảo mật/ẩn danh hoá được áp dụng trong nền tảng **SentinelAI (Phase 1)**.

---

## 1. Danh Mục Nguồn Dữ Liệu (Data Sources Inventory)

| Nguồn | Định dạng | Entity Ingested | Mục đích sử dụng | Phương thức thu thập |
|---|---|---|---|---|
| **Tiki Vietnam** | JSON API / HTML | `products`, `reviews`, `prices` | Theo dõi giá, danh mục điện tử & đánh giá người dùng | `ingestion/crawler/tiki.py`, `TikiCrawler` |
| **Diễn đàn Công nghệ (Voz / Tinhte)** | HTML / Post streams | `posts`, `reviews` | Thảo luận công nghệ, lỗi thiết bị, xu hướng thảo luận | `ingestion/crawler/generic_html.py` |
| **Tập dữ liệu Công khai (Public Datasets)** | CSV / JSONL / Parquet | `products`, `reviews`, `prices` | Kiểm thử tải lớn (Volume ≥ 1M bản ghi), benchmark 3V | `ingestion/file_loader/`, `data/generator.py` |
| **Replay Stream Engine** | Stream messages | Envelope canonical events | Thử nghiệm áp lực (Velocity 10 - 1,000 msg/s), DLQ replay | `scripts/benchmark.py`, `scripts/replay_dlq.py` |

---

## 2. Tiêu Chuẩn Kỹ Thuật Khi Thu Thập (Technical Crawling Guidelines)

### 2.1 Tôn trọng Robots.txt & Thân Thiện với Máy Chủ (Polite Crawling)
1. **Tuân thủ `robots.txt`**: Mọi crawler trước khi chạy đều phân tích file `robots.txt` của tên miền đích. Các đường dẫn bị `Disallow` tuyệt đối không được crawl.
2. **Định danh User-Agent minh bạch**:
   Mọi HTTP request đều mang User-Agent định danh mục đích nghiên cứu:
   ```http
   User-Agent: SentinelAI-Academic-Research/1.0 (+https://github.com/ptdbrain/Agentic_Multimodal_Big_Data_Intelligence_Platform; contact@sentinelai.local)
   ```
3. **Giới hạn tốc độ (Rate Limiting) & Jitter**:
   - Tốc độ mặc định không vượt quá **2 requests/giây** trên mỗi IP đích đối với live websites.
   - Sử dụng cơ chế Random Jitter (`0.5s - 1.5s`) giữa các request liên tiếp để tránh tạo xung đột tải đột ngột.
4. **Retry có Backoff hàm mũ (Exponential Backoff)**:
   - Khi gặp HTTP `429 (Too Many Requests)` hoặc `503 (Service Unavailable)`, crawler tự động lùi bước: `delay = base_delay * (2 ** retry_count) + random_jitter`.
   - Giới hạn tối đa 3 lần thử; nếu tiếp tục lỗi, đánh dấu failed và ghi log cảnh báo, không làm gián đoạn toàn bộ batch.
5. **Crawler Lũy kế (Incremental Crawling)**:
   - Crawler lưu trạng thái `last_crawled_id` và `last_crawled_timestamp`.
   - Chỉ lấy dữ liệu mới sinh ra hoặc cập nhật kể từ lần chạy gần nhất, không crawl lại toàn bộ dữ liệu lịch sử.

---

## 3. Chính Sách Bảo Mật, Quyền Riêng Tư & Ẩn Danh Hoá (Privacy & Anonymization)

Để bảo vệ quyền riêng tư người dùng theo nguyên tắc Privacy by Design:

1. **Băm định danh người dùng (`user_id`)**:
   - Tất cả mã tài khoản hoặc tên người dùng được mã hoá một chiều:
     $$\text{user\_id} = \text{sha256}(\text{raw\_user\_id} + \text{salt})[:16]$$
   - Tuyệt đối không lưu tên thật, số điện thoại, email, địa chỉ giao hàng hoặc thông tin cá nhân (PII) vào bất kỳ tầng nào (Bronze, Silver, Gold, Warehouse).
2. **Không đưa dữ liệu nhị phân media (Binary Bytes) vào Kafka**:
   - Ảnh đại diện, avatar người đánh giá bị loại bỏ hoàn toàn tại Ingestion.
   - Đối với ảnh sản phẩm hoặc ảnh minh chứng lỗi: Crawler tải và lưu vào MinIO Object Storage (`bronze/images/{sha256}.jpg`). Message gửi vào Kafka chỉ chứa:
     - `image_uri`: Đường dẫn file trên MinIO
     - `sha256`: Mã băm kiểm tra toàn vẹn
     - `size_bytes`: Kích thước file
3. **Lưu trữ Snapshot Phục vụ Khắc phục sự cố (Debugging)**:
   - Lưu trữ bản chụp định dạng text JSON/HTML tại `bronze/snapshots/` trong thời hạn tối đa 7 ngày để debug parser khi giao diện website thay đổi cấu trúc.

---

## 4. Khuôn Khổ Pháp Lý & Phạm Vi Sử Dụng (Legal & Ethical Framework)

1. **Mục đích học thuật và nghiên cứu phi thương mại**:
   - Dự án được phát triển phục vụ mục đích nghiên cứu học thuật về Xử lý Dữ liệu lớn (Big Data Engineering) và Phân tích thị trường TMĐT.
   - Không khai thác dữ liệu phục vụ mục đích thương mại trái phép, không bán dữ liệu cho bên thứ ba.
2. **Không vượt rào bảo mật (No Paywalls/Bypass)**:
   - Chỉ thu thập các thông tin được công khai tự do trên Internet (Publicly Accessible Data).
   - Không sử dụng các biện pháp bẻ khoá, khai thác lỗ hổng bảo mật hay bypass CAPTCHA thương mại trái phép.
3. **Quyền sở hữu trí tuệ của nội dung**:
   - Toàn bộ thương hiệu, logo, tên sản phẩm và đánh giá nguyên gốc thuộc quyền sở hữu của các nền tảng và người sáng tạo nội dung tương ứng.
