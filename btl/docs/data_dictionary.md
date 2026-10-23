# SentinelAI — Data Dictionary & Entity Reference

Tài liệu này tổng hợp toàn bộ từ điển dữ liệu (Data Dictionary) cho các tầng Medallion Architecture (Bronze Raw Envelopes, Silver Cleaned Parquet, Gold Analytics Marts, và PostgreSQL Data Warehouse).

---

## 1. Canonical Envelope (Kafka & Bronze Storage)

Mọi bản ghi đi qua Kafka đều được đóng gói theo định dạng phong bì chuẩn (Canonical Event Envelope):

| Trường | Kiểu dữ liệu | Bắt buộc | Mô tả | Ví dụ |
|---|---|---|---|---|
| `event_id` | String | Có | Mã băm tất định `evt_sha256(source\|entity_id\|event_time)[:24]` | `evt_a8f9c011e582...` |
| `event_type` | String | Có | Loại sự kiện: `NEW_PRODUCT`, `NEW_REVIEW`, `PRICE_UPDATE`, `POST` | `NEW_REVIEW` |
| `schema_version` | String | Có | Phiên bản lược đồ hợp đồng (Contract Version) | `1.0` |
| `source` | String | Có | Nguồn phát sinh dữ liệu (`tiki`, `voz`, `tinhte`, `benchmark`) | `tiki` |
| `event_time` | String (ISO-8601) | Có | Thời gian xảy ra sự kiện tại nguồn | `2026-10-01T10:00:00Z` |
| `ingested_at` | String (ISO-8601) | Có | Thời điểm hệ thống tiếp nhận vào Kafka | `2026-10-01T10:00:02Z` |
| `payload` | Object | Có | Nội dung chi tiết của bản ghi nghiệp vụ | `{ ... }` |

---

## 2. Các Thực Thể Dữ Liệu Tầng Silver (Silver Entities)

### 2.1 Bảng `silver.products`
- **Đường dẫn**: `silver/products/` (Partitioned by `year`, `month`, `day`)
- **Khoá chính (Primary Key)**: `product_id`

| Cột | Kiểu | Nullable | Mô tả & Ràng buộc nghiệp vụ |
|---|---|---|---|
| `product_id` | String | Không | SKU định danh duy nhất của sản phẩm |
| `product_name` | String | Không | Tên hiển thị đầy đủ của sản phẩm |
| `brand` | String | Không | Thương hiệu đã chuẩn hoá chữ hoa/thường (e.g., Apple, Samsung) |
| `category` | String | Không | Danh mục ngành hàng cấp 1 |
| `subcategory` | String | Có | Phân loại ngành hàng chi tiết |
| `price` | Double | Không | Giá niêm yết hiện tại (VND, phải > 0) |
| `currency` | String | Không | Đơn vị tiền tệ (mặc định `VND`) |
| `rating` | Double | Có | Điểm đánh giá trung bình danh mục [1.0, 5.0] |
| `review_count` | Long | Có | Tổng số lượng đánh giá tích luỹ |
| `seller` | String | Có | Tên gian hàng bán lẻ |
| `source` | String | Không | Nguồn thu thập |
| `ingested_at` | String | Có | Thời gian nạp dữ liệu |

### 2.2 Bảng `silver.reviews`
- **Đường dẫn**: `silver/reviews/` (Partitioned by `year`, `month`, `day`)
- **Khoá chính (Primary Key)**: `review_id`

| Cột | Kiểu | Nullable | Mô tả & Ràng buộc nghiệp vụ |
|---|---|---|---|
| `review_id` | String | Không | Mã định danh duy nhất của đánh giá |
| `product_id` | String | Không | Mã sản phẩm được đánh giá (Khoá ngoại tham chiếu `products`) |
| `user_id` | String | Không | Mã định danh người dùng đã băm bảo mật (`sha256`) |
| `rating` | Double | Không | Điểm số xếp hạng của khách hàng trong khoảng $[1.0, 5.0]$ |
| `review_title` | String | Có | Tiêu đề tóm tắt đánh giá |
| `review_text` | String | Không | Nội dung phản hồi chi tiết của khách hàng |
| `review_date` | String | Có | Thời điểm đánh giá (ISO-8601 UTC) |
| `verified_purchase` | Boolean | Có | Cờ xác nhận người mua đã thanh toán đơn hàng |
| `helpful_count` | Long | Có | Số lượt bình chọn hữu ích từ cộng đồng |
| `source` | String | Không | Nguồn thu thập |

### 2.3 Bảng `silver.price_history`
- **Đường dẫn**: `silver/prices/` (Partitioned by `year`, `month`, `day`)
- **Khoá chính (Primary Key)**: `price_id`

| Cột | Kiểu | Nullable | Mô tả & Ràng buộc nghiệp vụ |
|---|---|---|---|
| `price_id` | String | Không | Mã định danh sự kiện biến động giá |
| `product_id` | String | Không | Mã sản phẩm |
| `price` | Double | Không | Giá bán thực tế (VND, phải > 0) |
| `currency` | String | Không | Đơn vị tiền tệ (`VND`) |
| `seller` | String | Có | Đơn vị bán buôn/bán lẻ |
| `timestamp` | String | Không | Thời điểm quan sát giá biến động |
| `source` | String | Không | Nguồn thu thập |

---

## 3. Các Data Mart Tầng Gold & PostgreSQL Data Warehouse

Các bảng Gold được tổng hợp hàng ngày bởi `SparkGoldBuilder` và lưu trữ dưới dạng Parquet trên MinIO cũng như đồng bộ vào PostgreSQL DW với cơ chế Upsert Idempotent (`ON CONFLICT`).

### 3.1 Data Mart `product_daily_stats`
- **Grain**: Một dòng tương ứng với một sản phẩm trên một ngày (`stat_date`, `product_id`).
- **Primary Key**: `(product_id, stat_date)`

| Cột | Kiểu | Mô tả |
|---|---|---|
| `product_id` | VARCHAR(100) | Mã định danh sản phẩm |
| `stat_date` | DATE | Ngày quan sát thống kê |
| `review_count` | INT | Số lượng đánh giá mới phát sinh trong ngày |
| `avg_rating` | NUMERIC(3,2) | Điểm đánh giá trung bình trong ngày |
| `rating_std` | NUMERIC(4,3) | Độ lệch chuẩn phân phối điểm đánh giá |
| `avg_price` | NUMERIC(15,2) | Giá trung bình quan sát được trong ngày |
| `min_price` | NUMERIC(15,2) | Mức giá thấp nhất trong ngày |
| `max_price` | NUMERIC(15,2) | Mức giá cao nhất trong ngày |
| `high_rating_ratio` | NUMERIC(4,3) | Tỷ lệ đánh giá tích cực ($\ge 4.0$) |
| `low_rating_ratio` | NUMERIC(4,3) | Tỷ lệ đánh giá tiêu cực ($\le 2.0$) |

### 3.2 Data Mart `brand_daily_stats`
- **Grain**: Một thương hiệu trên một ngày.
- **Primary Key**: `(brand, stat_date)`

| Cột | Kiểu | Mô tả |
|---|---|---|
| `brand` | VARCHAR(100) | Tên thương hiệu |
| `stat_date` | DATE | Ngày quan sát |
| `total_products` | INT | Tổng số SKU sản phẩm thuộc thương hiệu |
| `total_reviews` | INT | Tổng số lượng đánh giá tích luỹ trong ngày |
| `avg_rating` | NUMERIC(3,2) | Điểm đánh giá trung bình toàn thương hiệu |
| `avg_price` | NUMERIC(15,2) | Mức giá sản phẩm trung bình |

### 3.3 Data Mart `category_daily_stats`
- **Grain**: Một ngành hàng trên một ngày.
- **Primary Key**: `(category, stat_date)`

| Cột | Kiểu | Mô tả |
|---|---|---|
| `category` | VARCHAR(100) | Tên danh mục ngành hàng |
| `stat_date` | DATE | Ngày quan sát |
| `total_products` | INT | Tổng số SKU sản phẩm thuộc ngành hàng |
| `total_reviews` | INT | Tổng số lượt đánh giá |
| `avg_rating` | NUMERIC(3,2) | Điểm đánh giá trung bình |
| `avg_price` | NUMERIC(15,2) | Giá bán trung bình của ngành hàng |

### 3.4 Data Mart `price_daily_stats`
- **Grain**: Thống kê lịch sử giá một sản phẩm trên một ngày.
- **Primary Key**: `(product_id, stat_date)`

| Cột | Kiểu | Mô tả |
|---|---|---|
| `product_id` | VARCHAR(100) | Mã sản phẩm |
| `stat_date` | DATE | Ngày quan sát |
| `avg_price` | NUMERIC(15,2) | Giá bán trung bình trong ngày |
| `min_price` | NUMERIC(15,2) | Giá thấp nhất |
| `max_price` | NUMERIC(15,2) | Giá cao nhất |
| `price_std` | NUMERIC(15,2) | Độ lệch chuẩn biến động giá |

### 3.5 Bảng `anomaly_events`
- **Grain**: Mỗi sự kiện dị biệt được phát hiện (Review Burst, Price Drop, Rating Drop).
- **Primary Key**: `anomaly_id`

| Cột | Kiểu | Mô tả |
|---|---|---|
| `anomaly_id` | VARCHAR(100) | Mã băm sự kiện dị biệt |
| `anomaly_type` | VARCHAR(50) | Phân loại: `REVIEW_BURST`, `PRICE_DROP`, `RATING_PLUNGE` |
| `entity_id` | VARCHAR(100) | Mã đối tượng (`product_id`) |
| `detected_at` | TIMESTAMP | Thời điểm phát hiện |
| `score` | FLOAT | Mức độ nghiêm trọng / Z-Score |
| `parameters` | JSONB | Bộ tham số baseline và ngưỡng kiểm tra |
| `severity` | VARCHAR(20) | `LOW`, `MEDIUM`, `HIGH`, `CRITICAL` |
