Mục tiêu cuối cùng sẽ là:

                         SENTINELAI PHASE 1

 REAL SOURCES
      │
      ├── Historical Dataset
      ├── API
      └── E-commerce Crawler
               │
               ▼
        DATA INGESTION
               │
               ▼
        ┌───────────────┐
        │     KAFKA     │
        │               │
        │ raw.products  │
        │ raw.reviews   │
        │ raw.prices    │
        │ raw.events    │
        └───────┬───────┘
                │
        ┌───────┴────────┐
        ▼                ▼
 Kafka Consumer     Spark Streaming
        │                │
        ▼                ▼
   MINIO BRONZE     Realtime Metrics
        │
        ▼
   SPARK BATCH ETL
        │
  ┌─────┼─────────┐
  ▼     ▼         ▼
Clean Normalize  DQ
  └─────┼─────────┘
        ▼
    MINIO SILVER
        │
        ▼
  SPARK ANALYTICS
        │
        ▼
     MINIO GOLD
        │
        ├───────────────┐
        ▼               ▼
   PostgreSQL       Analytics
        │
        └───────┬───────┘
                ▼
           DASHBOARD

Điểm quan trọng nhất:

Kafka, MinIO, Spark, PostgreSQL phải thực sự nằm trên đường đi của dữ liệu.

Không còn chuyện có container nhưng code lại chạy bằng Pandas/SQLite/local filesystem.

I. Nguyên tắc sửa repo

Trước khi đi vào từng bước, mình muốn đặt 6 nguyên tắc.

1. Không rewrite từ đầu

Giữ lại:

data/
ingestion/
kafka/
spark/
storage/
analytics/
dashboard/
database/
tests/

Các module hiện tại sẽ được nâng cấp.

2. Không fallback im lặng

Hiện tại nhiều đoạn:

except Exception:
    return None

hoặc:

Kafka lỗi
→ virtual queue

Điều này phải bỏ khỏi đường chạy chính.

Ta muốn:

Kafka lỗi
→ pipeline FAIL
→ log rõ nguyên nhân

chứ không phải:

Kafka lỗi
→ giả lập
→ vẫn báo SUCCESS
3. Python/Pandas chỉ dùng ở tầng phù hợp

Pandas vẫn được phép dùng cho:

small local debugging
tests
dashboard-side processing

Nhưng:

Big Data ETL
Big Data aggregation
Streaming

phải do Spark đảm nhiệm.

4. Local filesystem chỉ dành cho test

Production/dev pipeline:

MinIO

không phải:

storage/datalake/

Local fallback chỉ dành cho:

unit test
offline test
5. PostgreSQL thật sự là serving/warehouse layer

Không dùng:

SQLite

trong pipeline chính.

SQLite chỉ dành cho test.

6. Chỉ gọi "Lakehouse" khi thực sự có table format

Hiện tại repo có:

MinIO + Parquet

→ đây là Data Lake.

Chưa phải Lakehouse.

Sau này nếu thêm:

Iceberg / Delta Lake

thì mới gọi là Lakehouse.

II. PHASE 0 — Đóng băng trạng thái hiện tại
Mục tiêu

Có một baseline trước khi sửa.

Tạo branch:

refactor/phase1-real-pipeline

Sau đó chạy:

pytest

Ghi lại:

number of tests
passed
failed
execution time

Tạo file:

docs/reports/baseline.md

ghi:

Current repository status
Current architecture
Known limitations
Baseline tests
Không sửa code ở bước này.
III. PHASE 1 — Chuẩn hóa dependencies

Hiện tại requirements.txt quá ít.

Repo đang có code sử dụng những thứ như:

Kafka
Spark
PostgreSQL
MinIO
Elasticsearch

nhưng dependencies không phản ánh đầy đủ.

Sửa requirements.txt

Tối thiểu cần xác định rõ:

pandas
pyarrow
pydantic
pyyaml
requests

kafka-python

pyspark

minio

psycopg2-binary

jsonschema

streamlit
plotly

pytest

Nếu dùng Elasticsearch thật:

elasticsearch

Nhưng Elasticsearch chưa phải critical path.

IV. PHASE 2 — Chuẩn hóa configuration

Hiện tại có nhiều configuration khác nhau:

config/kafka.yaml
config/storage.yaml
config/database.yaml
config/spark.yaml
.env.example
settings.py

Ý tưởng đúng nhưng đang có nguy cơ cấu hình chồng chéo.

Thiết kế mới
.env.example
ENV=development

# Kafka
KAFKA_BOOTSTRAP_SERVERS_HOST=localhost:9094
KAFKA_BOOTSTRAP_SERVERS_DOCKER=kafka:9092

# MinIO
MINIO_ENDPOINT_HOST=localhost:9000
MINIO_ENDPOINT_DOCKER=minio:9000

MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin

# PostgreSQL
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=sentinel_dw
POSTGRES_USER=sentinel
POSTGRES_PASSWORD=sentinelpass

# Spark
SPARK_MASTER=spark://localhost:7077

# Data
DATA_LAKE_BUCKET=sentinel-data
V. Sửa vấn đề Kafka networking

Hiện tại Docker Kafka có:

kafka:9092
localhost:9094

đó là đúng hướng, nhưng application config đang dùng:

localhost:9092

Sai trong trường hợp Python chạy ngoài Docker.

Chốt:

Host machine
localhost:9094
Docker network
kafka:9092
VI. Tạo một config loader thống nhất

config/settings.py trở thành nơi duy nhất application đọc config.

Ví dụ:

settings.kafka.bootstrap_servers
settings.storage.endpoint
settings.database.host
settings.spark.master

Không hard-code:

localhost:9000

ở các module khác.

VII. PHASE 3 — Kafka thật

Đây là milestone quan trọng nhất.

1. Sửa StreamProducer

File:

kafka/producers/stream_producer.py
Bỏ
virtual_queue

khỏi production path.

Thay bằng:

KafkaProducer
    ↓
send
    ↓
flush
    ↓
delivery result
2. Producer phải có metadata

Mỗi message nên có:

event_id
event_type
source
ingested_at
payload

Ví dụ:

{
  "event_id": "evt_123",
  "event_type": "NEW_REVIEW",
  "source": "tiki",
  "ingested_at": "2026-10-02T10:00:00Z",
  "payload": {
    "review_id": "rv_001",
    "product_id": "p001",
    "rating": 2
  }
}
VIII. Kafka topic architecture

Chốt:

raw.products
raw.reviews
raw.prices
raw.events

Partitions:

raw.products → 2
raw.reviews  → 3
raw.prices   → 2
raw.events   → 3

Replication:

1

cho local single-node.

Sau này báo cáo phải giải thích:

Production có thể tăng replication/partitions; local project dùng 1 broker để phù hợp tài nguyên.

IX. Kafka key

Đây là kiến thức quan trọng.

Ví dụ review:

key = product_id

Như vậy các event của cùng product có xu hướng đi cùng partition.

Ví dụ:

P001 → Partition 0
P002 → Partition 1
P003 → Partition 2

Giúp giữ ordering tương đối theo entity.

X. Kafka Consumer thật

File:

kafka/consumers/raw_consumer.py

phải đổi hoàn toàn logic.

Hiện tại:

records
 ↓
save_raw_batch()

Mới phải là:

KafkaConsumer
      ↓
poll()
      ↓
parse message
      ↓
validate envelope
      ↓
batch records
      ↓
MinIO Bronze
XI. Consumer Group

Dùng:

sentinel-bronze-writer

Consumer:

raw.products
raw.reviews
raw.prices
raw.events

Có thể dùng group riêng nếu muốn tách workload.

Ví dụ:

sentinel-bronze-writer
sentinel-stream-analytics
XII. Offset management

Không dùng:

enable_auto_commit=True

một cách tùy tiện.

Pipeline:

poll
 ↓
write successful
 ↓
commit offset

Nếu MinIO write thất bại:

DO NOT COMMIT

→ lần sau consumer đọc lại.

Đây là một điểm rất đáng nói khi bảo vệ.

XIII. Bronze Layer — MinIO thật

Đây là milestone thứ hai.

Bucket

Mình khuyên chỉ dùng một bucket Data Lake:

sentinel-data

bên trong:

bronze/
silver/
gold/

thay vì nhất thiết phải:

sentinel-raw
sentinel-silver
sentinel-gold

Một bucket cũng dễ quản lý lifecycle và lineage hơn.

XIV. Bronze path

Ví dụ:

bronze/reviews/
year=2026/
month=10/
day=02/
hour=10/

Mỗi file:

batch_20261002_100001.json

Lưu raw data gần nguyên bản.

XV. Sửa StorageManager

File:

storage/storage_manager.py

phải chuyển từ:

Path(...)

sang abstraction:

StorageBackend

Có thể:

MinIOBackend
LocalBackend

Nhưng:

default = MinIO
XVI. API của StorageManager

Ví dụ:

write_json(...)
read_json(...)
write_parquet(...)
read_parquet(...)
list_objects(...)
get_object(...)

Application không cần biết bên dưới là:

MinIO

hay:

local filesystem
XVII. Một điểm phải sửa: Bronze không được biến đổi

Bronze:

raw data

Không:

normalize brand
fix rating
remove duplicate

Các việc đó thuộc Silver.

Pipeline:

SOURCE
 ↓
BRONZE
 ↓
CLEAN
 ↓
SILVER
XVIII. PHASE 4 — Data contract / schema validation

Repo đã có:

data/schemas/*.json

Đây là phần tốt.

Nhưng hiện tại schema chưa thực sự nằm trên ingestion path.

Ta sửa:

Producer
 ↓
Schema validation
 ↓
Kafka

hoặc ít nhất:

Kafka
 ↓
Consumer
 ↓
Schema validation
 ↓
Bronze

Mình thích cách thứ hai vì Bronze vẫn có thể giữ raw payload, còn invalid data được đưa vào:

bronze/dead_letter/
XIX. Dead Letter Queue

Tạo topic:

dead-letter

Nếu record lỗi schema:

Kafka
 ↓
Consumer
 ↓
Invalid
 ↓
DLQ

Thay vì vứt dữ liệu.

Ví dụ:

{
  "error_type": "SCHEMA_VALIDATION_ERROR",
  "topic": "raw.reviews",
  "partition": 1,
  "offset": 932,
  "payload": {...},
  "error": ["rating out of range"]
}

Đây là một nâng cấp rất đáng giá.

XX. PHASE 5 — Sửa Data Cleaning

File:

spark/etl/cleaner.py

Hiện tại có lỗi logic:

rating = 10

bị biến thành:

5

Điều này không tốt.

Thay bằng:

Raw rating = 10
        ↓
Validation
        ↓
INVALID
XXI. Data Cleaning phải tách thành 3 bước
VALIDATE
   ↓
CLEAN
   ↓
NORMALIZE

Không trộn hết vào một hàm.

Ví dụ:

Validate
rating = 10
→ invalid
Clean
"   iphone 15   "
→ "iphone 15"
Normalize
apple
APPLE
Apple Inc.
→ Apple
XXII. Invalid data

Không được chỉ:

continue

rồi mất dữ liệu.

Phải thống kê:

valid
invalid
duplicate
repaired

và lưu invalid records:

silver/quarantine/

hoặc:

bronze/dead_letter/
XXIII. PHASE 6 — Spark Batch thật

Đây là thay đổi lớn nhất.

File:

spark/batch/batch_etl_job.py

hiện đang:

Pandas

Phải chuyển thành:

Spark DataFrame
XXIV. Batch architecture mới
MinIO Bronze
       ↓
Spark.read.json()
       ↓
Spark DataFrame
       ↓
Schema enforcement
       ↓
Cleaning
       ↓
Normalization
       ↓
Deduplication
       ↓
Quality
       ↓
MinIO Silver
XXV. Không đưa toàn bộ dữ liệu vào Python List

Hiện tại:

records = [...]

Điều này không scale.

Không muốn:

1 million records
 ↓
Python List
 ↓
RAM

Muốn:

1 million records
 ↓
Spark DataFrame
 ↓
distributed processing
XXVI. Spark partitioning

Ví dụ:

1M reviews

Spark có thể chia:

Partition 0
Partition 1
Partition 2
...

sau đó worker xử lý song song.

Bạn sẽ dùng:

df.repartition(...)

một cách có chủ đích.

Không lạm dụng repartition() vì shuffle rất tốn tài nguyên.

XXVII. Spark transformations

Bạn cần biến các logic hiện tại thành:

filter
select
withColumn
join
groupBy
agg
dropDuplicates

Thay vì vòng:

for record in records:
XXVIII. Silver format

Output:

Parquet

partition theo:

year
month
day

Ví dụ:

silver/reviews/
year=2026/
month=10/
day=02/
part-00000.parquet
XXIX. PHASE 7 — Deduplication thật

Hiện tại Deduplicator dùng Python set.

Điều đó ổn cho unit test.

Nhưng Spark pipeline phải dùng:

dropDuplicates(...)

và nếu cần deterministic rule:

partitionBy(...)
orderBy(...)
row_number()

Ví dụ:

same review_id
        ↓
sort by ingested_at
        ↓
keep latest
XXX. Data Quality trên Spark

DataQualityEvaluator hiện tại dùng:

List[Dict]

Cần tạo:

SparkDataQualityEvaluator

Ví dụ:

total_count
valid_count
invalid_count
null_count
duplicate_count
dq_score

Tất cả tính bằng Spark.

XXXI. Data Quality report

Mỗi pipeline run phải tạo:

batch_id
dataset
received
valid
invalid
duplicates
missing
dq_score
processing_time

và ghi thành:

gold/data_quality/

hoặc trực tiếp warehouse.

XXXII. PHASE 8 — Price Pipeline

Đây là phần repo hiện còn thiếu.

Hiện có:

data/sample/prices.*

nhưng chưa đi hết pipeline.

Phải làm:

prices
 ↓
Kafka raw.prices
 ↓
Bronze
 ↓
Spark
 ↓
Silver
 ↓
Gold
XXXIII. Price Silver

Schema:

price_id
product_id
price
currency
seller
timestamp
source

Partition:

year
month
day
XXXIV. PHASE 9 — Spark Streaming thật

File:

spark/streaming/streaming_job.py

hiện tại là simulation.

Phải đổi thành:

SparkSession
    ↓
readStream
    ↓
Kafka
XXXV. Pipeline streaming
Kafka
 ↓
Spark Structured Streaming
 ↓
Parse JSON
 ↓
Schema
 ↓
Filter
 ↓
Watermark
 ↓
Window
 ↓
Aggregation
 ↓
Sink
XXXVI. Window

Bắt đầu đơn giản:

1-minute tumbling window

sau đó:

5-minute tumbling

Không cần làm sliding window ngay.

XXXVII. Metrics realtime

Ví dụ:

window_start
window_end
product_id
review_count
avg_rating
negative_count

Ví dụ:

10:00–10:01
Product A
review_count = 82
avg_rating = 2.8
XXXVIII. Event Time

Đây là thứ bạn nên học thật kỹ.

Không lấy:

processing time

làm thời gian review nếu event đã có timestamp.

Dùng:

review_date
timestamp

để tính event time.

XXXIX. Watermark

Vì event có thể tới muộn.

Ví dụ:

10:05 event
10:03 event tới muộn

Spark cần biết:

Tôi còn chờ dữ liệu trễ bao lâu?

Ví dụ:

withWatermark("timestamp", "10 minutes")

Đây sẽ là một kiến thức Big Data rất đẹp để trình bày.

XL. Streaming checkpoint

Phải có:

checkpoints/

Ví dụ:

MinIO:
checkpoints/reviews_stream/

Không đặt checkpoint trong thư mục tạm rồi mất khi restart.

XLI. Streaming sink

MVP có thể ghi:

Gold streaming metrics

vào MinIO.

Sau đó thêm:

PostgreSQL

để dashboard đọc.

Không cần viết trực tiếp từng event vào Postgres.

XLII. PHASE 10 — Gold Analytics thật bằng Spark

File:

spark/etl/gold_aggregator.py

hiện tại dùng Pandas.

Phải chuyển sang Spark.

XLIII. Gold tables

Giữ:

product_daily_stats
brand_daily_stats
category_daily_stats

thêm:

price_daily_stats
anomaly_events
data_quality_metrics
pipeline_metrics
XLIV. Sửa logic price

Hiện tại:

min_price
max_price
avg_price

đều lấy từ product hiện tại.

Sai về mặt ý nghĩa.

Phải:

Silver price history
        ↓
groupBy(product_id, date)
        ↓
min(price)
max(price)
avg(price)
stddev(price)
XLV. Product Daily Stats

Mẫu:

date
product_id
review_count
avg_rating
rating_std
avg_price
min_price
max_price
positive_ratio
negative_ratio

Lưu ý:

positive_ratio hiện đang dựa vào star rating.

Trong Phase 1 gọi là:

high_rating_ratio
low_rating_ratio

sẽ chính xác hơn.

Phase 2 mới đổi sang sentiment thật.

XLVI. PHASE 11 — PostgreSQL Warehouse thật

database/schema.sql đã có nền tốt nhưng cần xác định rõ:

PostgreSQL không chứa toàn bộ raw Big Data.

Kiến trúc:

MinIO
 ├── Bronze
 ├── Silver
 └── Gold
       ↓
PostgreSQL

PostgreSQL chứa:

Gold analytics
DQ metrics
pipeline metrics
anomaly events
serving data
XLVII. Schema PostgreSQL

Mình khuyên giữ:

products
product_daily_stats
brand_daily_stats
category_daily_stats
anomaly_events
data_quality_metrics
pipeline_metrics

Có thể thêm:

price_daily_stats

Không cần đẩy toàn bộ 1M raw reviews vào PostgreSQL trong Phase 1.

XLVIII. Sửa export_gold.py

Không dùng:

if_exists="replace"

Thay:

staging
 ↓
validate
 ↓
UPSERT

Ví dụ:

gold.product_daily_stats

khóa:

(date, product_id)

Nếu record tồn tại:

UPDATE

nếu chưa:

INSERT
XLIX. PostgreSQL phải được kiểm chứng

Sau pipeline:

SELECT COUNT(*) FROM product_daily_stats;

phải cho ra số đúng với Gold.

Kiểm tra thêm:

SELECT *
FROM product_daily_stats
ORDER BY date DESC
LIMIT 20;
L. PHASE 12 — Dashboard chuyển sang PostgreSQL

Hiện tại Dashboard:

Parquet

→ cần sửa.

Architecture mới:

Dashboard
    ↓
PostgreSQL
    ↓
Gold tables
LI. Dashboard Overview

KPI:

Products
Reviews
Brands
Average Rating
Data Quality

Nguồn:

PostgreSQL

không hard-code:

98.4%
LII. Dashboard Pipeline

Hiển thị thật:

Kafka throughput
records ingested
Spark processing time
DQ score
storage size

Không gọi:

"real-time"

nếu dữ liệu thực tế không realtime.

LIII. Dashboard Trend

Query:

SELECT date, SUM(review_count)
FROM product_daily_stats
GROUP BY date
ORDER BY date;

Sau đó vẽ.

LIV. Dashboard Anomaly

Nguồn:

PostgreSQL.anomaly_events

Không tự chạy Python anomaly detector mỗi lần người dùng mở dashboard.

Sai architecture:

Open dashboard
 ↓
re-run analytics

Đúng:

Pipeline
 ↓
compute anomaly
 ↓
save result
 ↓
Dashboard reads result
LV. PHASE 13 — Anomaly engine

Giữ 3 detector:

Review Burst
Price Anomaly
Rating Drop

Nhưng refactor để input/output rõ ràng.

Ví dụ:

Input:
gold/product_daily_stats

Output:
gold/anomaly_events
LVI. Review Burst

Pipeline:

review_count by time window
       ↓
rolling baseline
       ↓
compare current vs baseline
       ↓
burst score

Không nên dùng dữ liệu toàn cục bằng Pandas nếu dataset lớn.

Tính bằng Spark trước.

LVII. Price Anomaly

Có thể làm 2 tầng:

Statistical baseline
Z-score
IQR
Phase 2
Isolation Forest

Phase 1 chỉ cần statistical.

LVIII. Rating Drop

Tính:

current average
vs
historical average

Ví dụ:

previous 7 days = 4.6
current day = 3.9

drop = -0.7

→ anomaly.

LIX. PHASE 14 — Real data ingestion

Đây là phần mình muốn bạn đặc biệt sửa.

Hiện tại repo có:

generator.py

Nhưng generator không được coi là data source chính.

Ta có:

Source 1

Historical dataset.

Source 2

Live crawler/API.

Source 3

Replay engine.

Architecture:

Historical dataset ─────┐
                        │
Live API/Crawler ───────┼──→ Canonical Schema
                        │
Synthetic replay ───────┘
                              │
                              ▼
                           Kafka
LX. Data generator dùng làm gì?

Không xóa.

Dùng cho:

load testing
anomaly injection
stream simulation
failure testing
integration tests

Không gọi nó là:

real data source.

LXI. Crawler architecture

Giữ:

BaseEcommerceCrawler
CrawlerRegistry
TikiCrawler
UniversalProductScraper
MockEcommerceCrawler

Đây là kiến trúc tốt.

Nhưng crawler phải trả về:

canonical Product
canonical Review
canonical Price

sau đó mới Kafka.

LXII. Sửa Universal crawler

Có một vấn đề kỹ thuật nhỏ:

hash(url)

không nên dùng làm ID lâu dài vì Python hash không đảm bảo ổn định giữa process.

Thay bằng:

SHA-256(url)

hoặc:

stable URL hash

Ví dụ:

web_<sha256_prefix>
LXIII. Sửa timestamp

Một số code hiện tại tạo kiểu:

2026-09-30T10:00:00+00:00Z

Không nên tạo timestamp kiểu này.

Chuẩn hóa thành một trong:

2026-09-30T10:00:00Z

hoặc:

2026-09-30T10:00:00+00:00

Một format thống nhất.

LXIV. PHASE 15 — Orchestration

Airflow hiện tại mới là:

print(...)

Không đủ.

Nhưng chưa sửa ngay từ đầu.

Chỉ làm sau khi pipeline thật chạy được.

LXV. Airflow DAG thật

Thiết kế:

start
  ↓
collect
  ↓
publish_kafka
  ↓
spark_batch
  ↓
data_quality
  ↓
gold_analytics
  ↓
warehouse_upsert
  ↓
finish

Streaming không nhất thiết nằm trong DAG daily.

Streaming là service chạy liên tục.

LXVI. Batch và Streaming phải tách

Đừng làm:

Airflow
 ↓
stream forever

Airflow phù hợp:

scheduled batch jobs

Kafka + Spark Streaming:

long-running service
LXVII. PHASE 16 — Monitoring

Sau khi core pipeline chạy được.

Thêm:

Prometheus
Grafana

Nhưng trước tiên hãy làm application metrics.

LXVIII. Pipeline metrics

Mỗi run:

run_id
stage
start_time
end_time
duration
records_in
records_out
error_count
LXIX. Kafka metrics

Cần đo:

messages/sec
consumer lag
messages produced
messages consumed

Đây là evidence cho Velocity.

LXX. Spark metrics

Theo dõi:

input records
output records
processing duration
shuffle
failed task
LXXI. Data Lake metrics

Đo:

Bronze size
Silver size
Gold size
file count
records
compression ratio
LXXII. PHASE 17 — Benchmark Volume

Đây là phần cực kỳ quan trọng cho báo cáo.

Không chỉ nói:

Hệ thống hỗ trợ Big Data.

Phải có bằng chứng.

Chạy:

10K
100K
500K
1M

records.

Đo:

execution time
throughput
memory
CPU
storage
LXXIII. Benchmark Velocity

Replay:

10 events/s
50 events/s
100 events/s
500 events/s

Ghi:

producer throughput
consumer throughput
Spark latency
Kafka lag
LXXIV. Benchmark Variety

Chạy:

CSV
JSON
JSONL
Parquet

qua ingestion.

Kiểm tra tất cả converge về:

Canonical Schema
LXXV. PHASE 18 — Data Lineage

Repo đã có:

docs/architecture/data_lineage.md

nhưng cần biến thành lineage thực.

Mỗi dataset phải trả lời được:

Source
 ↓
Kafka topic
 ↓
Bronze object
 ↓
Spark job
 ↓
Silver
 ↓
Gold
 ↓
PostgreSQL
 ↓
Dashboard
LXXVI. Data Catalog

Tạo:

docs/data_dictionary/

cho từng dataset.

Ví dụ:

products
reviews
prices
events
product_daily_stats
anomaly_events

Mỗi field:

name
type
nullable
meaning
source
transformation
LXXVII. PHASE 19 — Test lại toàn bộ

Test phải chia 4 tầng.

Unit
normalizer
validator
deduplicator
anomaly logic
Integration
Producer
 ↓
Kafka
 ↓
Consumer
 ↓
MinIO
Data pipeline
Bronze
 ↓
Spark
 ↓
Silver
 ↓
Gold
E2E
Source
 ↓
Kafka
 ↓
MinIO
 ↓
Spark
 ↓
Postgres
 ↓
Dashboard
LXXVIII. Sửa test E2E hiện tại

Hiện tại test_e2e.py chỉ kiểm tra:

Silver exists
Gold exists

Chưa chứng minh:

Kafka
MinIO
Spark
Postgres

Phải thêm:

Kafka topic exists
records published > 0
Bronze object exists
Silver parquet exists
Gold parquet exists
Postgres rows > 0
LXXIX. Thêm failure tests

Ví dụ:

Kafka down
Kafka unavailable
→ pipeline fail clearly
Bad schema
bad event
→ DLQ
Duplicate
duplicate review
→ deduplicated
Spark failed
job failure
→ no false SUCCESS
PostgreSQL unavailable
warehouse stage fails
→ earlier data remains intact
LXXX. Target repository sau khi hoàn thành

Mình muốn structure cuối gần như:

sentinel-ai/
│
├── config/
│   ├── settings.py
│   ├── kafka.yaml
│   ├── spark.yaml
│   ├── storage.yaml
│   └── database.yaml
│
├── data/
│   ├── sample/
│   ├── schemas/
│   └── generator.py
│
├── ingestion/
│   ├── api/
│   ├── crawler/
│   └── file_loader/
│
├── kafka/
│   ├── producers/
│   ├── consumers/
│   ├── topics/
│   └── dlq/
│
├── storage/
│   ├── storage_manager.py
│   └── minio_backend.py
│
├── spark/
│   ├── batch/
│   ├── streaming/
│   ├── etl/
│   ├── quality/
│   └── analytics/
│
├── database/
│   ├── schema.sql
│   ├── migrations/
│   └── warehouse.py
│
├── analytics/
│   ├── descriptive/
│   ├── trend/
│   └── anomaly/
│
├── dashboard/
│   ├── app.py
│   └── pages/
│
├── orchestration/
│   └── dags/
│
├── monitoring/
│   ├── prometheus/
│   └── grafana/
│
├── scripts/
│   ├── bootstrap.py
│   ├── provision_topics.py
│   ├── run_batch.py
│   ├── run_streaming.py
│   ├── benchmark.py
│   └── health_check.py
│
├── tests/
│
└── docs/
LXXXI. Thứ tự code chính xác

Đây là phần quan trọng nhất. Không làm lung tung.

Sprint 1 — Infrastructure
1. requirements.txt
2. .env.example
3. settings.py
4. docker-compose.yml
5. health_check.py

Kết quả:

Kafka ✓
MinIO ✓
Spark ✓
PostgreSQL ✓
Sprint 2 — Kafka
1. StreamProducer
2. topic provisioning
3. Kafka consumer
4. offset handling
5. DLQ
6. integration tests

Kết quả:

Producer
 ↓
Kafka
 ↓
Consumer
Sprint 3 — MinIO
1. MinIO client
2. StorageManager
3. Bronze
4. object naming
5. partition path
6. read/write tests

Kết quả:

Kafka
 ↓
Consumer
 ↓
MinIO Bronze
Sprint 4 — Data Contract
1. JSON schemas
2. validation
3. quarantine
4. DLQ
5. schema tests
Sprint 5 — Spark Batch
1. SparkSession
2. read Bronze
3. clean
4. normalize
5. dedup
6. DQ
7. Silver Parquet

Kết quả:

Bronze
 ↓
Spark
 ↓
Silver
Sprint 6 — Price Pipeline
prices
 ↓
Kafka
 ↓
Bronze
 ↓
Spark
 ↓
Silver
Sprint 7 — Gold Analytics
Silver
 ↓
Spark
 ↓
Gold
Sprint 8 — PostgreSQL
Gold
 ↓
UPSERT
 ↓
PostgreSQL
Sprint 9 — Streaming
Kafka
 ↓
Spark Structured Streaming
 ↓
Window
 ↓
Realtime Gold
Sprint 10 — Dashboard
PostgreSQL
 ↓
Streamlit
Sprint 11 — Monitoring + Airflow
Airflow
+
Prometheus
+
Grafana
Sprint 12 — Benchmark + Documentation
3V
Performance
Lineage
Architecture
Demo
LXXXII. Definition of Done mới

Sau khi sửa, README không được tick [x] chỉ vì module tồn tại.

Mỗi chức năng chỉ được đánh dấu hoàn thành khi có evidence.

Ví dụ:

Kafka
[x] Kafka broker reachable
[x] Producer sends real messages
[x] Consumer reads real messages
[x] Offset commit works
[x] Partitioning demonstrated
Data Lake
[x] MinIO used in actual pipeline
[x] Bronze contains raw data
[x] Silver contains Parquet
[x] Gold contains analytical marts
Spark
[x] Batch ETL uses Spark DataFrame
[x] Streaming uses Spark Structured Streaming
[x] Window aggregation works
Warehouse
[x] PostgreSQL receives Gold
[x] Dashboard queries PostgreSQL
3V
[x] Volume benchmark
[x] Velocity benchmark
[x] Variety benchmark
LXXXIII. Kiến trúc cuối cùng cần đạt

Đây là architecture mà mình muốn bạn bảo vệ trước giáo viên:

┌────────────────────────────────────────────────────────────────┐
│                       DATA SOURCES                             │
│                                                                │
│ Historical Dataset │ API │ E-commerce │ Replay Simulator      │
└────────────────────────────┬───────────────────────────────────┘
                             │
                             ▼
                    ┌────────────────┐
                    │ KAFKA INGESTION│
                    │                │
                    │ products       │
                    │ reviews        │
                    │ prices         │
                    │ events         │
                    └───────┬────────┘
                            │
                ┌───────────┴────────────┐
                ▼                        ▼
        ┌──────────────┐       ┌────────────────────┐
        │ Kafka        │       │ Spark Structured   │
        │ Consumer     │       │ Streaming          │
        └──────┬───────┘       └─────────┬──────────┘
               │                         │
               ▼                         ▼
        ┌──────────────┐          Realtime Metrics
        │ MINIO        │
        │ BRONZE       │
        └──────┬───────┘
               │
               ▼
        ┌─────────────────────┐
        │ SPARK BATCH ETL     │
        │                     │
        │ Validate            │
        │ Clean               │
        │ Normalize           │
        │ Deduplicate         │
        │ Data Quality        │
        └──────────┬──────────┘
                   │
                   ▼
             MINIO SILVER
              Parquet
                   │
                   ▼
        ┌─────────────────────┐
        │ SPARK ANALYTICS     │
        │                     │
        │ Statistics          │
        │ Trends              │
        │ Time Series         │
        │ Anomalies           │
        └──────────┬──────────┘
                   │
                   ▼
              MINIO GOLD
                   │
            ┌──────┴───────┐
            ▼              ▼
      PostgreSQL       Data Lake
       Warehouse          /
            │           Gold
            │
            ▼
       STREAMLIT
       DASHBOARD
LXXXIV. Sau khi Phase 1 hoàn thành

Bạn sẽ có một nền rất sạch cho Phase 2:

                    PHASE 1
               DATA FOUNDATION
                       │
        ┌──────────────┼──────────────┐
        ▼              ▼              ▼
      Reviews       Products        Images
        │              │              │
        ▼              ▼              ▼
      NLP             ML             CV
        │              │              │
        └──────────────┼──────────────┘
                       ▼
                    PHASE 2
               AI INTELLIGENCE
                       │
                       ▼
               Embeddings / RAG
                       │
                       ▼
                    PHASE 3
               AGENTIC INTELLIGENCE
LXXXV. Một thay đổi rất quan trọng đối với README hiện tại

README đang nói khá nhiều thứ đã hoàn thành, ví dụ:

Spark Structured Streaming
PostgreSQL
Prometheus/Grafana
Airflow
100% test pass

trong khi code thực tế chưa đạt những mức đó.

Nên tạm thời sửa README để phản ánh đúng trạng thái, không nên để README đi trước implementation.

Ví dụ:

Phase 1 Status: Under Active Development

Completed:
- Schema design
- Data generation
- Crawler abstraction
- Analytics prototypes
- Dashboard prototype

In Progress:
- Real Kafka pipeline
- MinIO Data Lake
- Spark Batch
- Spark Streaming
- PostgreSQL Warehouse

Sau khi có evidence mới tick [x].

Điều này quan trọng cho cả GitHub portfolio lẫn khi demo.

LXXXVI. Thứ tự ưu tiên nếu bạn thiếu thời gian

Nếu môn học sắp đến deadline, ưu tiên tuyệt đối:

1. Kafka thật
2. MinIO thật
3. Spark Batch thật
4. PostgreSQL thật
5. Spark Streaming thật
6. Dashboard
7. Data Quality
8. Analytics
9. Benchmark 3V
10. Airflow
11. Monitoring
12. Elasticsearch
13. Lakehouse

Tức là:

Kafka
 ↓
MinIO
 ↓
Spark
 ↓
PostgreSQL

phải hoàn chỉnh trước.

Elasticsearch, Airflow, Grafana và Lakehouse là lớp nâng cấp; chúng không được phép làm chậm việc xây core pipeline.

Mốc hoàn thành quan trọng nhất

Mình muốn bạn coi đây là checkpoint số 1:

python / crawler
       ↓
Kafka
       ↓
Consumer
       ↓
MinIO Bronze
       ↓
Spark Batch
       ↓
MinIO Silver
       ↓
Spark Gold
       ↓
PostgreSQL
       ↓
Streamlit

Khi bạn chạy được một record thật đi hết đường này, rồi mới scale lên:

1 record
 ↓
1K
 ↓
100K
 ↓
1M

và tiếp tục:

Kafka
 ↓
Spark Streaming
 ↓
Window Analytics

Đó là cách mình muốn sửa repo: không chạy theo số lượng công nghệ, mà lấy một đường dữ liệu end-to-end làm “xương sống”, sau đó từng công nghệ phải thực sự tham gia vào đường đó.

Bước code đầu tiên

Bắt đầu từ Sprint 1 → Sprint 2, cụ thể là:

docker-compose.yml
→ requirements.txt
→ settings.py
→ Kafka listener
→ StreamProducer
→ RawConsumer thật

Sau milestone này, test bắt buộc phải chứng minh được:

Python Producer
      ↓
Kafka topic raw.reviews
      ↓
Kafka Consumer
      ↓
console/log

Chưa cần Spark, chưa cần dashboard.

Khi Kafka thật đã thông, mới nối Kafka → MinIO Bronze. Đây là thứ tự an toàn nhất để bạn vừa học Data Engineering vừa sửa project mà không bị “ngợp” vì 10 công nghệ cùng lúc