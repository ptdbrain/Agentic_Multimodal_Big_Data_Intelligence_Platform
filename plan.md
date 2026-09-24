PLAN CHI TIẾT — GIAI ĐOẠN 1
SentinelAI — Data Foundation & Analytics

Mục tiêu của Giai đoạn 1:

Xây dựng được một hệ thống Big Data hoàn chỉnh có khả năng thu thập dữ liệu thật từ nhiều nguồn → streaming qua Kafka → lưu Data Lake → xử lý bằng Spark → kiểm tra chất lượng → tạo dữ liệu phân tích → lưu Data Warehouse/Search → xây Dashboard → phát hiện trend/anomaly và tạo báo cáo.

Chưa cần Agent, RAG, LLM hay VLM ở giai đoạn này. Nhưng kiến trúc phải được thiết kế để Giai đoạn 2–3 có thể cắm AI vào mà không phải xây lại từ đầu.

0. Kiến trúc tổng thể Giai đoạn 1

Mình đề xuất chốt architecture như sau:

                         SENTINELAI — PHASE 1
                  DATA FOUNDATION & ANALYTICS
                              │
                              ▼
┌──────────────────────────────────────────────────────────────┐
│                    1. DATA SOURCES                           │
│                                                              │
│  E-commerce     Social/Forum      News/API      Local Files  │
│  Product        Reviews           Price         Images       │
└──────────┬─────────────┬──────────────┬─────────────┬────────┘
           │             │              │             │
           └─────────────┴──────────────┴─────────────┘
                              │
                              ▼
┌──────────────────────────────────────────────────────────────┐
│                 2. DATA INGESTION                            │
│                                                              │
│   Web/API Collector ──┐                                      │
│   File Collector ─────┼──► Kafka Topics                      │
│   Stream Producer ────┘                                      │
│                                                              │
│   products | reviews | prices | events | images              │
└──────────────────────────┬───────────────────────────────────┘
                           │
                           ▼
┌──────────────────────────────────────────────────────────────┐
│                 3. DATA LAKE                                 │
│                                                              │
│                     MinIO                                   │
│                       │                                      │
│        ┌──────────────┼──────────────┐                       │
│        ▼              ▼              ▼                       │
│       RAW          CLEANED       PROCESSED                   │
│      JSON/CSV       Parquet        Parquet                    │
└──────────────────────────┬───────────────────────────────────┘
                           │
                           ▼
┌──────────────────────────────────────────────────────────────┐
│                 4. SPARK PROCESSING                          │
│                                                              │
│       Spark Batch             Spark Streaming                │
│            │                         │                       │
│            └──────────┬──────────────┘                       │
│                       ▼                                      │
│              ETL / Data Quality                              │
│                                                              │
│   Cleaning → Dedup → Normalize → Validate → Enrich           │
└──────────────────────────┬───────────────────────────────────┘
                           │
                           ▼
┌──────────────────────────────────────────────────────────────┐
│              5. ANALYTICS DATA LAYER                         │
│                                                              │
│  PostgreSQL                Elasticsearch                      │
│  ├── product_stats        ├── product search                 │
│  ├── review_stats         ├── review search                  │
│  ├── daily_metrics        └── event search                   │
│  └── anomaly_results                                         │
└──────────────────────────┬───────────────────────────────────┘
                           │
                           ▼
┌──────────────────────────────────────────────────────────────┐
│                 6. ANALYTICS                                │
│                                                              │
│  Volume Trend │ Rating Trend │ Price Trend │ Anomaly         │
│  Product      │ Category     │ Brand       │ Time Series     │
└──────────────────────────┬───────────────────────────────────┘
                           │
                           ▼
┌──────────────────────────────────────────────────────────────┐
│                 7. DASHBOARD                                 │
│                                                              │
│  Overview │ Products │ Reviews │ Trends │ Anomalies │ Data   │
│                                                              │
│              Streamlit / Grafana / Metabase                  │
└──────────────────────────────────────────────────────────────┘
1. Chốt phạm vi dữ liệu

Đây là quyết định quan trọng nhất trước khi code.

1.1. Domain

Mình khuyên chỉ chọn một domain trong Phase 1.

Ví dụ:

Consumer Electronics / Smartphone & Technology Products

Thay vì:

mọi sản phẩm trên Internet

hãy làm:

Smartphones
Laptops
Tablets
Accessories

Lý do:

dễ tìm dataset;
có product;
có review;
có rating;
có price;
có brand;
có timestamp;
có category;
có thể mở rộng sang image ở Phase 2;
phù hợp với NLP + CV sau này.
2. Xác định Data Model

Không được bắt đầu bằng việc tải một đống CSV rồi mới nghĩ xem lưu như thế nào.

Trước tiên định nghĩa schema.

2.1. Product
product_id
product_name
brand
category
subcategory
price
currency
rating
review_count
seller
product_url
image_url
source
created_at
updated_at

Ví dụ:

{
  "product_id": "iphone_15_128",
  "product_name": "iPhone 15 128GB",
  "brand": "Apple",
  "category": "Smartphone",
  "price": 18990000,
  "currency": "VND",
  "rating": 4.6,
  "review_count": 12543,
  "source": "..."
}
3. Review Data Model

Đây sẽ là dataset quan trọng nhất.

review_id
product_id
user_id
rating
review_title
review_text
review_date
verified_purchase
helpful_count
source
language
created_at

Ví dụ:

{
  "review_id": "rv_001",
  "product_id": "iphone_15_128",
  "user_id": "u_8123",
  "rating": 2,
  "review_title": "Màn hình có vấn đề",
  "review_text": "Sau vài tháng sử dụng màn hình xuất hiện sọc...",
  "review_date": "2026-08-12",
  "verified_purchase": true,
  "helpful_count": 35,
  "source": "..."
}
4. Price History

Để sau này làm trend/anomaly.

price_id
product_id
price
currency
seller
timestamp
source

Một product có:

Day 1    20,000,000
Day 7    19,500,000
Day 14   18,900,000
Day 21   22,000,000

Sau này có thể phát hiện:

Price anomaly.

5. Event Data

Đây là thứ giúp chứng minh Velocity.

Ví dụ:

event_id
event_type
product_id
timestamp
source
payload

Các event:

NEW_REVIEW
PRICE_UPDATE
RATING_UPDATE
PRODUCT_UPDATE

Ví dụ:

{
  "event_id": "evt_123",
  "event_type": "NEW_REVIEW",
  "product_id": "iphone_15_128",
  "timestamp": "2026-09-29T14:30:21",
  "payload": {
      "rating": 2,
      "review_id": "rv_999"
  }
}
6. Chứng minh 3V

Đây phải là một mục riêng trong báo cáo.

Volume

Ví dụ mục tiêu:

Products       ≥ 50,000
Reviews        ≥ 1,000,000
Price records  ≥ 500,000
Events         ≥ 1,000,000

Không nhất thiết phải đạt chính xác những con số này ngay từ đầu.

Có thể bắt đầu:

100K reviews

rồi scale lên.

Variety

Có:

CSV
JSON
Parquet
Text
Metadata
Images
Time-series

Ví dụ:

products.csv
reviews.json
prices.json
images/
events.json
Velocity

Dataset Kaggle bản chất là static.

Không nên nói:

"Kaggle là real-time data."

Thay vào đó:

Historical Data
      ↓
Replay Engine
      ↓
Kafka
      ↓
Spark Structured Streaming

Ví dụ:

10 events/sec
50 events/sec
100 events/sec

Dataset lịch sử được replay như stream.

Đây là cách rất hợp lý cho project môn học.

7. Thiết kế Data Sources

Nên có ít nhất 3 nhóm.

Source A — Product/Review Dataset

Nguồn lớn nhất.

Ví dụ:

Products
Reviews
Ratings

Có thể sử dụng Kaggle hoặc public datasets.

Source B — Price Data
product
price
seller
timestamp

Nếu không có historical price đủ tốt:

price snapshots

cũng được.

Ví dụ:

09:00 → 20M
12:00 → 19.8M
15:00 → 19.5M
18:00 → 20.1M
Source C — Streaming Data

Không cần website thật.

Tạo:

Kafka Producer

đọc historical data và replay:

review_001
review_002
review_003
...

với tốc độ:

10 msg/s
50 msg/s
100 msg/s
8. Cấu trúc project

Ngay từ đầu nên tổ chức repository như sau:

sentinel-ai/
│
├── README.md
├── docker-compose.yml
├── .env
│
├── config/
│   ├── kafka.yaml
│   ├── spark.yaml
│   └── storage.yaml
│
├── data/
│   ├── sample/
│   └── schemas/
│
├── ingestion/
│   ├── api/
│   ├── crawler/
│   ├── file_loader/
│   └── kafka_producer/
│
├── kafka/
│   ├── topics/
│   ├── producers/
│   └── consumers/
│
├── storage/
│   ├── raw/
│   ├── processed/
│   └── analytics/
│
├── spark/
│   ├── batch/
│   ├── streaming/
│   ├── etl/
│   └── quality/
│
├── analytics/
│   ├── descriptive/
│   ├── trend/
│   ├── anomaly/
│   └── time_series/
│
├── database/
│   ├── schema.sql
│   └── migrations/
│
├── dashboard/
│
├── tests/
│
├── docs/
│   ├── architecture/
│   ├── data_dictionary/
│   └── reports/
│
└── notebooks/
9. GIAI ĐOẠN 1A — Infrastructure
Mục tiêu

Dựng được toàn bộ infrastructure trước khi xử lý AI.

Docker Compose

Ban đầu:

Kafka
Zookeeper/KRaft
MinIO
Spark
PostgreSQL
Elasticsearch

Dashboard:

Streamlit

Sau đó có thể thêm:

Prometheus
Grafana
10. Infrastructure milestone

Kết quả cần đạt:

docker compose up

và toàn bộ service chạy:

Kafka       ✓
MinIO       ✓
Spark       ✓
PostgreSQL  ✓
Elastic     ✓
Dashboard   ✓

Tạo health check.

Ví dụ:

/check/kafka
/check/minio
/check/spark
/check/postgres
11. GIAI ĐOẠN 1B — Data Ingestion

Đây là phần cần làm rất kỹ.

Pipeline
SOURCE
  ↓
Collector
  ↓
Validation
  ↓
Normalization
  ↓
Kafka Producer
  ↓
Kafka Topic
12. Kafka Topics

Tạo:

products
reviews
prices
events

Nếu muốn rõ hơn:

raw.products
raw.reviews
raw.prices
raw.events

Kafka partition:

reviews → 3 partitions
products → 2 partitions
prices → 2 partitions
events → 3 partitions

Không cần quá nhiều partition.

Mục đích là demo scalability.

13. Producer

Producer phải hỗ trợ:

python producer.py \
    --topic reviews \
    --rate 50

Tức:

50 records/sec

Có thể thay:

10
50
100
500
14. Consumer

Consumer nhận:

Kafka
 ↓
Consumer
 ↓
MinIO

Raw data được lưu nguyên bản.

Ví dụ:

s3://sentinelai/raw/reviews/

partition theo:

year/
month/
day/
hour/

Ví dụ:

raw/
└── reviews/
    └── year=2026/
        └── month=09/
            └── day=29/
                └── hour=15/
15. Tại sao phải lưu RAW?

Rất quan trọng.

Không được:

Kafka → clean → bỏ raw

Mà:

Source
  ↓
RAW
  ↓
Clean
  ↓
Processed

Nếu cleaning sai:

RAW
 ↓
reprocess

không cần tải dữ liệu lại.

Đây là tư duy Data Engineering rất quan trọng.

16. Data Lake Architecture

Chia:

RAW
BRONZE
SILVER
GOLD

Có thể giải thích:

Bronze

Dữ liệu gần nguyên bản:

JSON
CSV
Silver

Dữ liệu đã:

clean
dedup
normalize
validate
Gold

Dữ liệu phục vụ analytics:

daily_product_stats
daily_review_stats
price_statistics
anomaly_results

Architecture:

                DATA LAKE

Source
  ↓
BRONZE
  ↓
SILVER
  ↓
GOLD
17. GIAI ĐOẠN 1C — Data Cleaning

Spark đảm nhiệm phần lớn.

17.1. Missing value

Ví dụ:

rating = NULL
price = NULL
review_text = NULL

Xử lý theo từng field.

Không được:

dropna()

một cách máy móc.

Ví dụ:

review_text NULL
→ loại review

price NULL
→ giữ review nhưng không dùng trong price analysis

rating NULL
→ giữ nếu review text vẫn hữu ích
18. Chuẩn hóa

Ví dụ:

Apple
APPLE
apple
Apple Inc.

→

Apple

Price:

19.999.000
19,999,000
19999000

→

19999000

Timestamp:

UTC
Vietnam Time

→ chuẩn hóa về một timezone.

19. Deduplication

Một review có thể xuất hiện:

2 lần
3 lần

Dùng:

review_id

hoặc hash:

hash(product_id + user_id + review_text + timestamp)

để phát hiện duplicate.

20. Data Quality Framework

Đây là phần mình đặc biệt khuyên bạn làm kỹ vì nó làm project trông giống hệ thống thật hơn.

Mỗi batch phải có:

records_received
records_valid
records_invalid
records_duplicate
records_missing
processing_time

Ví dụ dashboard:

Incoming:       1,000,000
Valid:            982,430
Invalid:           8,120
Duplicate:         9,450
Missing fields:      0,000
21. Data Quality Rules

Ví dụ:

Review
review_id != NULL
product_id != NULL
rating BETWEEN 1 AND 5
review_date valid
Product
product_id != NULL
price >= 0
rating BETWEEN 0 AND 5
Price
price > 0
timestamp valid
22. Data Quality Score

Có thể tạo:

DQ Score =
valid_records / total_records

Ví dụ:

982,430 / 1,000,000
= 98.24%

Dashboard:

Data Quality
███████████████████░ 98.24%
23. GIAI ĐOẠN 1D — Spark Batch

Đây là phần quan trọng nhất để chứng minh Big Data Processing.

Input:

MinIO Bronze

Spark:

Read
 ↓
Parse
 ↓
Clean
 ↓
Normalize
 ↓
Deduplicate
 ↓
Validate
 ↓
Enrich
 ↓
Write

Output:

MinIO Silver
24. Spark Streaming

Pipeline:

Kafka
 ↓
Spark Structured Streaming
 ↓
Transformation
 ↓
Aggregation
 ↓
MinIO / PostgreSQL

Ví dụ mỗi 1 phút:

reviews_received
average_rating
negative_review_count
price_updates
25. Window Analytics

Đây là phần rất đáng đưa vào báo cáo.

Ví dụ:

1-minute window
5-minute window
1-hour window
1-day window

Ví dụ:

5-minute review count

Spark:

window(timestamp, "5 minutes")

Kết quả:

10:00–10:05 → 132 reviews
10:05–10:10 → 421 reviews
10:10–10:15 → 980 reviews

→ có thể phát hiện spike.

26. GIAI ĐOẠN 1E — Analytics

Chưa cần AI.

Nhưng analytics phải đủ sâu.

Chia thành:

Descriptive Analytics
Trend Analytics
Comparative Analytics
Anomaly Analytics
Time-series Analytics
27. Descriptive Analytics

Tính:

total products
total reviews
total brands
total categories
average rating
median rating
min/max price
review count
28. Product Analytics

Mỗi product:

review_count
average_rating
rating_std
average_price
price_std
positive_ratio
negative_ratio

Phase 1 có thể chưa dùng sentiment model.

Thay bằng:

5-star ratio
4-star ratio
1-star ratio
2-star ratio

Sau này Phase 2 thay bằng sentiment.

29. Brand Analytics

Ví dụ:

Brand
Products
Reviews
Average Rating
Average Price
Rating Distribution

Dashboard:

Apple
Samsung
Xiaomi
Google
...

Không nên đưa "best brand" nếu mục tiêu học thuật là phân tích trung lập; chỉ hiển thị metrics và cho người dùng tự so sánh.

30. Category Analytics

Ví dụ:

Smartphone
Laptop
Tablet
Headphone
Smartwatch

Phân tích:

review volume
average rating
average price
price volatility
review growth
31. Temporal Analytics

Đây là một phần rất quan trọng.

Ví dụ:

Daily review volume
Weekly review volume
Monthly review volume

Biểu đồ:

Reviews
  │
  │             ╭───╮
  │        ╭────╯   ╰──╮
  │   ╭────╯            ╰──
  └──────────────────────────
              Time
32. Rating Trend

Ví dụ:

Average rating by day

Phát hiện:

4.6
4.5
4.5
4.4
4.1  ← sudden drop
4.0

Đây sẽ trở thành input rất tốt cho Phase 2.

33. Price Trend

Theo:

product
brand
category
time

Ví dụ:

Product A

Price
20M ─────────╮
             │
19M          ╰──────╮
                    │
18M                 ╰────
34. Price Anomaly

Có thể dùng thống kê trước khi dùng ML.

Z-score
z = (x - μ) / σ

Nếu:

|z| > 3

→ đánh dấu anomaly.

Hoặc:

IQR

để robust hơn.

35. Review Spike Detection

Ví dụ:

Normal:

100
110
120
105
115

Spike:

100
110
120
105
2,500  ← anomaly

Có thể dùng:

rolling mean
rolling std
z-score
36. Rating Anomaly

Ví dụ:

Daily average:

4.5
4.6
4.5
4.6
2.1 ← anomaly
4.5

Có thể đánh dấu:

rating_anomaly = TRUE
37. Review Burst

Đây là một insight thú vị.

Ví dụ:

Product A

08:00 → 20 reviews
09:00 → 18 reviews
10:00 → 21 reviews
11:00 → 19 reviews
12:00 → 1,250 reviews

Hệ thống:

detect burst

Sau này Phase 2 có thể hỏi:

Những review trong burst này nói về vấn đề gì?

và NLP sẽ trả lời.

38. GIAI ĐOẠN 1F — Gold Data

Tạo các bảng/data mart.

product_daily_stats
date
product_id
review_count
avg_rating
min_price
max_price
avg_price
rating_std
brand_daily_stats
date
brand
product_count
review_count
avg_rating
avg_price
category_daily_stats
date
category
review_count
avg_rating
avg_price
anomaly_events
event_id
entity_type
entity_id
anomaly_type
score
timestamp
description

Ví dụ:

PRODUCT
iphone_15
REVIEW_BURST
8.7
2026-09-29
39. PostgreSQL

PostgreSQL không phải nơi chứa toàn bộ Big Data.

Đây là điểm cần làm rõ trong architecture.

Millions of raw records
        ↓
      MinIO
        ↓
     Parquet
        ↓
   Spark Analytics
        ↓
Small aggregated datasets
        ↓
    PostgreSQL

PostgreSQL phục vụ:

dashboard
API
queries
aggregated metrics
40. Elasticsearch

Không nhất thiết phải dùng từ ngày đầu.

Nếu dùng:

review search
product search
event search

Ví dụ dashboard:

Search:
"battery problem"

→ tìm review liên quan.

Phase 2 có thể thay bằng semantic search.

41. GIAI ĐOẠN 1G — Dashboard

Dashboard phải chứng minh toàn bộ pipeline.

Page 1 — Overview

Hiển thị:

Total Products
Total Reviews
Total Events
Total Brands
Data Volume
Data Quality

Ví dụ:

┌────────────┬────────────┬────────────┬────────────┐
│ 1.2M       │ 54K        │ 8.3K       │ 98.7%      │
│ Reviews    │ Products   │ Brands     │ DQ Score   │
└────────────┴────────────┴────────────┴────────────┘
42. Dashboard — Data Pipeline

Hiển thị:

Kafka throughput
Spark processing
records/sec
records/min
processing latency
failed records

Ví dụ:

Kafka

████████████████ 820 msg/s
43. Dashboard — Product

Filter:

Brand
Category
Date
Price range

Hiển thị:

Product
Review Count
Rating
Price
Rating Trend
Review Trend
44. Dashboard — Review

Biểu đồ:

Reviews per day
Rating distribution
Reviews per category
Reviews per brand
45. Dashboard — Trend

Ví dụ:

Review Volume Trend
Rating Trend
Price Trend

Có filter:

Product
Brand
Category
Time range
46. Dashboard — Anomaly

Đây sẽ là một trong những màn hình đẹp nhất.

ANOMALY CENTER

┌────────────┬──────────────┬──────────┬────────────┐
│ Product    │ Type         │ Score    │ Time       │
├────────────┼──────────────┼──────────┼────────────┤
│ Product A  │ Review Burst │ 8.9      │ 12:30      │
│ Product B  │ Price Spike  │ 7.8      │ 14:20      │
│ Product C  │ Rating Drop  │ 7.4      │ 16:10      │
└────────────┴──────────────┴──────────┴────────────┘

Click vào:

Anomaly
 ↓
Product
 ↓
Time period
 ↓
Raw reviews
47. GIAI ĐOẠN 1H — Monitoring

Đây là phần nhiều sinh viên thường bỏ qua.

Bạn nên có:

Prometheus
+
Grafana

Theo dõi:

Kafka throughput
Kafka lag
Spark jobs
CPU
RAM
Storage
Processing latency
Error rate
48. Logging

Mỗi component phải có log.

Ví dụ:

[INGESTION]
2026-09-29 15:20
Received 10,000 records

[KAFKA]
Published 10,000 messages

[SPARK]
Processed 10,000 records

[QUALITY]
Valid: 9,872
Invalid: 128

[STORAGE]
Written: 9,872 records
49. Orchestration

Sau khi từng module chạy ổn, thêm:

Airflow

Pipeline:

      START
        │
        ▼
   Collect Data
        │
        ▼
   Validate Data
        │
        ▼
   Spark ETL
        │
        ▼
   Data Quality
        │
        ▼
   Analytics
        │
        ▼
 Update PostgreSQL
        │
        ▼
 Refresh Dashboard

Airflow DAG:

sentinel_daily_pipeline
50. Testing

Không được chỉ test bằng cách:

"Chạy thấy không lỗi là xong."

Cần test:

Unit Test
parser
normalizer
validator
deduplication
Integration Test
Producer
 ↓
Kafka
 ↓
Consumer
 ↓
MinIO
Data Test
1000 input
→ 980 valid
→ 20 invalid

phải đúng.

51. End-to-End Test

Đây là test quan trọng nhất.

Cho:

10,000 reviews

vào hệ thống.

Kiểm tra:

Source
 ↓
Producer
 ↓
Kafka
 ↓
MinIO
 ↓
Spark
 ↓
PostgreSQL
 ↓
Dashboard

Tất cả phải chạy.

52. Test Velocity

Chạy:

10 events/sec

sau đó:

100 events/sec

sau đó:

500 events/sec

Ghi:

throughput
latency
Kafka lag
Spark processing time

Đây là bằng chứng cho Velocity.

53. Test Volume

Chạy thử:

10K
100K
500K
1M

records.

So sánh:

processing time
memory
CPU
storage

Có thể làm chart:

Processing Time
      │
      │                ╭──
      │          ╭─────╯
      │     ╭────╯
      │─────╯
      └────────────────────
          Data Volume
54. Test Variety

Cho vào:

CSV
JSON
Parquet

và kiểm tra pipeline chuẩn hóa về:

Unified Schema

Đây là evidence cho Variety.

55. Data Lineage

Một tính năng rất đáng có.

Cho mỗi metric, có thể truy ngược:

Dashboard
   ↓
Gold Table
   ↓
Spark Job
   ↓
Silver
   ↓
Bronze
   ↓
Kafka
   ↓
Source

Ví dụ:

Average Rating = 4.21

Có thể giải thích:

4.21
 ↓
product_daily_stats
 ↓
Spark aggregation
 ↓
clean reviews
 ↓
raw reviews
56. Metadata / Data Catalog

Tạo một Data Dictionary.

Ví dụ:

Field	Type	Description
product_id	string	Product identifier
review_id	string	Review identifier
rating	float	Rating 1–5
review_date	timestamp	Review time
price	float	Product price
brand	string	Product brand

Đây sẽ giúp report rất chuyên nghiệp.

57. Schema Versioning

Nên thiết kế:

schema_v1
schema_v2

Ví dụ ban đầu:

review_text
rating

sau đó thêm:

language
verified_purchase
helpful_count

Không phá pipeline cũ.

58. Những thứ KHÔNG nên làm trong Phase 1

Rất quan trọng.

Không nên cùng lúc làm:

Kafka
Spark
Flink
Hadoop
Hive
Airflow
Kubernetes
LLM
VLM
RAG
Agent
YOLO
Vector DB
...

sẽ dễ thành project "có tất cả nhưng không cái nào hoàn chỉnh".

Phase 1 chỉ cần:

Kafka
MinIO
Spark
PostgreSQL
Elasticsearch (optional)
Airflow
Streamlit/Grafana
Docker
59. Roadmap thực hiện thực tế
Milestone 1 — Dataset

Mục tiêu:

Dataset ≥ 100K records

Hoàn thành:

 Chọn domain
 Tìm datasets
 Download
 Khảo sát schema
 Data dictionary
 Data profiling
 Xác định 3V
Milestone 2 — Infrastructure
 Docker
 Kafka
 MinIO
 Spark
 PostgreSQL
 Streamlit
 Health check

Output:

docker compose up

→ toàn bộ hệ thống chạy.

60. Milestone 3 — Ingestion
 File collector
 API collector
 Kafka producer
 Kafka topics
 Partition
 Consumer
 Raw storage
 Replay engine

Output:

Dataset
 ↓
Kafka
 ↓
MinIO
61. Milestone 4 — Data Lake
 Bronze
 Silver
 Gold
 Partitioning
 Parquet
 Metadata
 Data dictionary
62. Milestone 5 — Spark
 Spark batch
 Spark streaming
 ETL
 Cleaning
 Deduplication
 Validation
 Aggregation
 Window processing
63. Milestone 6 — Data Quality
 Missing values
 Invalid values
 Duplicate
 Schema validation
 DQ score
 Quality report
64. Milestone 7 — Analytics
 Descriptive statistics
 Product statistics
 Brand statistics
 Category statistics
 Rating trend
 Price trend
 Review trend
 Review burst
 Price anomaly
 Rating anomaly
65. Milestone 8 — Data Warehouse / Search
 PostgreSQL schema
 Gold tables
 ETL → PostgreSQL
 Elasticsearch indexing nếu cần
 Search API
66. Milestone 9 — Dashboard

Phải có ít nhất:

1. Overview
2. Data Pipeline
3. Product Analytics
4. Review Analytics
5. Trend Analytics
6. Anomaly Center
7. Data Quality
67. Milestone 10 — Monitoring
 Kafka metrics
 Spark metrics
 Processing latency
 Throughput
 Error rate
 Storage
 Grafana
68. Milestone 11 — Airflow

Cuối cùng mới orchestration.

Airflow
   │
   ├── ingestion
   ├── validation
   ├── spark_etl
   ├── quality
   ├── analytics
   └── warehouse_update
69. Milestone 12 — Final End-to-End Demo

Đây là demo mình nghĩ rất hợp để thuyết trình.

Bạn bắt đầu:

100,000 historical reviews

↓

Collector

↓

Kafka

↓

MinIO RAW

↓

Spark

↓

Cleaning
Dedup
Validation

↓

Silver

↓

Spark Analytics

↓

Daily statistics
Trend
Anomaly

↓

Gold

↓

PostgreSQL

↓

Dashboard

70. Sau đó bật streaming

Ví dụ:

Producer = 100 events/sec

Dashboard realtime bắt đầu thay đổi:

Reviews/min
      ↑
      │             ╭───
      │       ╭─────╯
      │───────╯
      └──────────────────

Hệ thống phát hiện:

⚠ Review Burst Detected

Product: X
Normal: 80 reviews/hour
Current: 1,240 reviews/hour
Anomaly Score: 8.7

Đây là lúc bạn có một Big Data demo thực sự, thay vì chỉ có notebook phân tích dataset.

71. Các deliverables cuối Phase 1

Cuối giai đoạn này bạn phải có 8 sản phẩm đầu ra.

1. Data
≥ 100K–1M records
2. Data Lake
Bronze
Silver
Gold
3. Streaming Pipeline
Source → Kafka → Spark Streaming
4. Batch Pipeline
Data Lake → Spark → Gold
5. Data Quality System
Validation
Dedup
DQ Score
Quality Report
6. Analytics Engine
Trend
Statistics
Time Series
Anomaly
7. Dashboard

Có thể nhìn thấy:

Data
Pipeline
Metrics
Trends
Anomalies
8. Architecture Documentation

Phải có:

System Architecture
Data Flow
Data Dictionary
Database Schema
Kafka Design
Spark Jobs
Data Quality
Analytics Method
3V Evidence
Performance Test
72. Definition of Done

Mình đề xuất không chuyển sang Phase 2 cho tới khi 10 điều này đạt:

[✓] Data thật từ nhiều nguồn
[✓] ≥ 3 loại data format
[✓] Volume đủ lớn
[✓] Kafka streaming chạy được
[✓] MinIO Data Lake chạy được
[✓] Spark Batch chạy được
[✓] Spark Streaming chạy được
[✓] Data Quality có kiểm soát
[✓] Analytics có trend + anomaly
[✓] Dashboard hiển thị kết quả

Nếu đạt đủ 10 cái này thì Phase 1 đã là một project Big Data hoàn chỉnh, kể cả bạn chưa làm một dòng LLM nào.

73. Sau Phase 1, dữ liệu sẽ sẵn sàng cho Phase 2 như thế nào?

Đây mới là điểm quan trọng của kiến trúc.

Phase 1 tạo:

                    GOLD DATA
                       │
        ┌──────────────┼───────────────┐
        ▼              ▼               ▼
   Reviews         Products         Images
        │              │               │
        ▼              ▼               ▼
     Phase 2        Phase 2         Phase 2
      NLP              ML              CV

Cụ thể:

reviews
   ↓
Sentiment
ABSA
Topic Modeling
NER
products + prices
   ↓
Forecasting
Recommendation
Potential Analysis
images
   ↓
CLIP/DINO
Similarity
Clustering
VLM

Và cuối cùng:

Phase 1
DATA FOUNDATION
       ↓
Phase 2
AI INTELLIGENCE
       ↓
Phase 3
AGENTIC INTELLIGENCE
Mình sẽ chốt kiến trúc chiến lược như sau
┌─────────────────────────────────────────────────────────────┐
│                    PHASE 1                                  │
│                                                             │
│        DATA IS THE PRODUCT                                  │
│                                                             │
│  Collect → Kafka → Data Lake → Spark → Analytics →         │
│                                           Dashboard         │
└─────────────────────────────┬───────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                    PHASE 2                                  │
│                                                             │
│        AI UNDERSTANDS THE DATA                              │
│                                                             │
│          NLP + CV + ML + Embeddings                         │
└─────────────────────────────┬───────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                    PHASE 3                                  │
│                                                             │
│        AI REASONS ABOUT THE DATA                             │
│                                                             │
│          RAG + LLM/VLM + Agent + Tools                     │
└─────────────────────────────────────────────────────────────┘