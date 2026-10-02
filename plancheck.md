# SentinelAI — Kế hoạch hoàn thiện dự án

> Tài liệu này là roadmap sống (living document). Cập nhật checkbox và mục "Nhật ký tiến độ" sau mỗi tuần.
> Quy ước: `[ ]` chưa làm · `[~]` đang làm · `[x]` xong · **(P0)** bắt buộc · **(P1)** nên có · **(P2)** nếu còn thời gian.

---

## 0. Tóm tắt

| Mục | Nội dung |
|---|---|
| Bài toán | Theo dõi giá, uy tín và ý kiến người dùng về sản phẩm điện tử trên các sàn TMĐT Việt Nam (Tiki, ...) kết hợp diễn đàn công nghệ (Voz, Tinhte). |
| Giai đoạn 1 (tập trung) | Thu thập → lưu trữ → xử lý dữ liệu lớn → chất lượng dữ liệu → phục vụ phân tích. **Chưa có AI.** |
| Giai đoạn 2+ | NLP tiếng Việt → đa phương thức → agent/RAG (chỉ phác thảo ở cuối tài liệu). |
| Nguyên tắc | Làm sâu từng tầng, chứng minh bằng số liệu, mọi thứ chạy lại được bằng 1 lệnh. |

### Định nghĩa "hoàn thành giai đoạn 1" (Definition of Done)
- [x] `make up && make demo` dựng toàn bộ hạ tầng và chạy pipeline thật (Kafka → Bronze → Silver → Gold → Postgres → Dashboard) trên máy mới, không cần sửa tay.
- [x] Có dataset **≥ 1 triệu bản ghi** (crawl + dataset công khai) đã chạy qua pipeline.
- [x] Chạy lại bất kỳ job nào **không làm trùng hoặc sai** dữ liệu (idempotent).
- [x] Có báo cáo benchmark (throughput, latency, thời gian ETL theo kích thước dữ liệu).
- [x] Có integration test chạy trên Docker và CI xanh.
- [x] Tài liệu: kiến trúc, data dictionary, hướng dẫn vận hành, hạn chế đã biết.
- [x] Không còn secrets trong repo.

---

## 1. Kiến trúc mục tiêu giai đoạn 1

```
Nguồn (Tiki crawler, forum crawler, dataset công khai, replay)
   │
   ▼
Kafka (KRaft)  ── topics: raw.products / raw.reviews / raw.prices / raw.posts
   │                      └─► DLQ: dlq.<topic>
   ├──► Raw Consumer ──► Bronze (MinIO, JSON/Parquet, append-only, partition theo event_date/hour)
   │
   └──► Spark Structured Streaming ──► Gold realtime (cửa sổ 1/5 phút) ──► Postgres

Bronze ──► Spark Batch ETL ──► Silver (Parquet/Delta, đã làm sạch, dedup, validate)
                                  │              └─► Quarantine (bản ghi lỗi + lý do)
                                  ▼
                           Data Quality (Great Expectations / Soda)
                                  ▼
                           Gold marts ──► Postgres (upsert idempotent) ──► Streamlit / Grafana

Airflow điều phối batch · Prometheus + Grafana giám sát · MinIO catalog: Hive Metastore hoặc bỏ qua (dùng Delta path)
```

### Quyết định công nghệ (chốt để khỏi lan man)
| Hạng mục | Chọn | Lý do / ghi chú |
|---|---|---|
| Object storage | MinIO | Chạy local, tương thích S3 |
| Table format | **Delta Lake** (hoặc Iceberg) cho Silver/Gold | ACID, schema evolution, time travel; thay cho Parquet thuần |
| Warehouse phục vụ | PostgreSQL | Bỏ SQLite fallback ở môi trường chính |
| Search | Elasticsearch **chỉ khi** có use case full-text; nếu chưa có thì tạm bỏ | Tránh thành phần "trang trí" |
| Catalog | Bỏ Hive/Glue ở giai đoạn 1 | Giảm độ nặng; xem lại ở giai đoạn 3 |
| Schema | Schema Registry (Avro) **(P1)**, tối thiểu là JSON Schema có version | Chống vỡ hợp đồng dữ liệu |
| DQ | Great Expectations hoặc Soda **(P1)** | Thay cho evaluator tự viết, hoặc dùng song song |
| Điều phối | Airflow | Đã có DAG |

> Việc cần làm: tạo `docs/architecture.md` với 2 sơ đồ — **Hiện tại** và **Mục tiêu** — đánh dấu rõ phần nào đã làm.

---

## 2. Rà soát hiện trạng (làm trước tiên)

Mục tiêu: biết chính xác lỗi đang có trước khi thêm tính năng. Mỗi mục là giả thuyết cần kiểm chứng trong code.

### 2.1 Bảo mật & vệ sinh repo (P0)
- [x] `git ls-files | grep -E "\.env$"` — nếu `.env` bị commit: đổi toàn bộ mật khẩu/key, thêm vào `.gitignore`, xoá khỏi lịch sử (git filter-repo / BFG).
- [x] Quét secrets: `gitleaks detect` hoặc `trufflehog`.
- [x] Bỏ thông tin cá nhân không cần thiết khỏi README.
- [x] Thêm description, topics, LICENSE cho repo.
- [x] Không hardcode mật khẩu trong `docker-compose.yml`; dùng `${VAR}` từ `.env`.

### 2.2 Checklist kiểm tra logic từng tầng
**Kafka producer/consumer**
- [x] `event_id` có tất định không? Cần `hash(source + entity_id + observed_at)`, không dùng uuid4 ngẫu nhiên.
- [x] Partition key là `product_id`/`entity_id` chưa?
- [x] Consumer: **ghi MinIO thành công → mới commit offset**. Kiểm tra thứ tự.
- [x] DLQ chứa: lý do lỗi, topic/partition/offset gốc, payload gốc, timestamp, số lần thử.
- [x] Envelope có `schema_version`, `event_time`, `ingested_at` chưa?
- [x] Có xử lý shutdown êm (flush buffer, commit offset) khi nhận SIGTERM chưa?

**Bronze/Storage**
- [x] Gom batch trước khi ghi (theo số bản ghi/kích thước/thời gian) để tránh small files.
- [x] Partition theo `event_time` (hoặc ghi cả 2: `event_date` và `ingest_date`).
- [x] Fallback MinIO → local có **log cảnh báo** và chỉ bật bằng cờ cấu hình; mặc định phải fail rõ ràng.
- [x] Bronze là **append-only, bất biến**.

**Silver/ETL**
- [x] Thứ tự đúng: Clean → Normalize → **Validate → Dedup (giữ bản mới nhất)**.
- [x] Dedup xuyên batch (so với Silver hiện có), không chỉ trong batch hiện tại.
- [x] Bản ghi invalid vào **Quarantine** kèm lý do, không bị drop im lặng.
- [x] Múi giờ thống nhất (đề xuất lưu UTC, hiển thị `Asia/Ho_Chi_Minh`).
- [x] Giá: xử lý null/0/âm, chuẩn hoá VND (kiểu số nguyên), giữ **lịch sử giá**.

**Spark**
- [x] Phiên bản `pyspark` = Spark trong Docker = gói `spark-sql-kafka` (cùng bản Spark + Scala).
- [x] Streaming có sink rõ ràng? (hiện sơ đồ không có đích) — nối vào Postgres/Delta.
- [x] `checkpointLocation` nằm ở nơi bền vững (MinIO/volume).
- [x] Watermark + output mode hợp lý với cửa sổ 1 phút/5 phút.
- [x] Không tính cùng một chỉ số ở 2 nơi theo 2 cách (batch vs streaming) → chọn nguồn sự thật duy nhất cho mỗi chỉ số.
- [x] Kiểm tra `run_e2e_pipeline.py` đang chạy Spark/Kafka/MinIO thật hay chế độ local/mock.

**Postgres/Gold**
- [x] Mỗi mart có `PRIMARY KEY`/`UNIQUE` đúng grain (vd. `product_id, stat_date`).
- [x] Upsert qua staging table + `INSERT ... ON CONFLICT` trong một transaction.
- [x] Chạy lại cùng ngày không cộng dồn/nhân đôi.
- [x] Lưu thành phần của aggregate (sum, count) thay vì chỉ lưu trung bình.
- [x] Index cho truy vấn dashboard (theo ngày, product_id, brand).

**Anomaly detection**
- [x] Baseline **loại trừ** điểm đang xét.
- [x] Ngưỡng số mẫu tối thiểu (tránh báo sai với sản phẩm mới/ít review).
- [x] Lưu kết quả anomaly vào bảng riêng (kèm tham số, score, thời điểm phát hiện).
- [x] Ghi rõ đây là phát hiện theo ngày (batch) hay gần thời gian thực.

**Airflow**
- [x] Task dùng `data_interval_start/end`, không dùng `now()`.
- [x] `retries`, `retry_delay`, `catchup`, `max_active_runs`, SLA/alert khi fail.
- [x] Mỗi task idempotent.

> Cách làm: mỗi lỗi tìm thấy → tạo GitHub Issue với nhãn `bug`/`data-correctness`, kèm test tái hiện, rồi mới sửa.

---

## 3. Workstream chi tiết

### WS1 — Thu thập dữ liệu (Ingestion)
**Mục tiêu:** nguồn dữ liệu ổn định, hợp pháp, đủ lớn, có thể replay.

- [x] **(P0)** Chốt danh sách nguồn: Tiki (đã có), 1 diễn đàn (Voz hoặc Tinhte), 1 dataset công khai cho volume.
- [x] **(P0)** Crawler: rate limit, retry có backoff + jitter, timeout, User-Agent rõ ràng, tôn trọng `robots.txt`.
- [x] **(P0)** Crawler **incremental**: nhớ trạng thái (last crawled id/time) để không crawl lại toàn bộ.
- [x] **(P0)** Chuẩn hoá đầu ra crawler về cùng envelope (xem WS2).
- [x] **(P1)** Lưu snapshot HTML/JSON gốc để debug khi parser hỏng.
- [x] **(P1)** Phát hiện thay đổi cấu trúc trang (parser trả 0 trường bắt buộc → cảnh báo).
- [x] **(P1)** Replay engine: phát lại dữ liệu lịch sử với tốc độ cấu hình (10–500+ msg/s) để benchmark.
- [x] **(P1)** Ẩn danh hoá người dùng: hash `user_id`, bỏ tên/avatar/thông tin liên hệ ngay lúc ingest.
- [x] **(P1)** Ảnh: **không đưa byte ảnh vào Kafka**; tải ảnh về MinIO (`bronze/images/...`), Kafka chỉ chứa `image_uri`, `sha256`, kích thước.
- [ ] **(P2)** Thêm nguồn thứ hai (Shopee/Lazada) nếu khả thi về pháp lý và kỹ thuật.
- [x] Ghi chú pháp lý/đạo đức trong `docs/data_sources.md`: điều khoản từng nguồn, phạm vi sử dụng (học tập), chính sách dữ liệu cá nhân.

**Tiêu chí hoàn thành:** crawl ≥ N sản phẩm/ngày ổn định 7 ngày liên tục không can thiệp tay; parser lỗi → có cảnh báo, không mất dữ liệu im lặng.

### WS2 — Hợp đồng dữ liệu & Kafka
- [x] **(P0)** Envelope chuẩn:
  ```json
  {
    "event_id": "sha256(source|entity_id|event_time)",
    "event_type": "review|price|product|post",
    "schema_version": "1.0",
    "source": "tiki",
    "event_time": "2026-10-01T10:00:00Z",
    "ingested_at": "2026-10-01T10:00:02Z",
    "payload": {}
  }
  ```
- [x] **(P0)** JSON Schema cho từng `event_type`, có version, đặt trong `config/schemas/`, test tương thích ngược.
- [x] **(P0)** Topic: số partition, replication factor, retention, compression (`lz4`/`zstd`) được ghi trong `kafka/topics.yml` và tạo tự động khi `make up`.
- [x] **(P0)** Producer: `acks=all`, `enable.idempotence=true`, key = entity_id, flush khi shutdown.
- [x] **(P0)** Consumer: at-least-once có chủ đích, batch + commit sau khi ghi thành công.
- [x] **(P0)** DLQ + **công cụ replay DLQ** (`scripts/replay_dlq.py`).
- [x] **(P1)** Schema Registry + Avro / JSON Schema validation.
- [x] **(P1)** Theo dõi consumer lag, xuất lên Prometheus.
- [x] **(P1)** Tài liệu `docs/data_contracts.md`: trường, kiểu, ý nghĩa, ví dụ.

### WS3 — Lưu trữ & Medallion (MinIO + Delta)
- [x] **(P0)** Bố cục bucket:
  ```
  s3://lake/bronze/{event_type}/event_date=YYYY-MM-DD/hour=HH/
  s3://lake/silver/{entity}/
  s3://lake/quarantine/{entity}/dt=YYYY-MM-DD/
  s3://lake/gold/{mart}/
  s3://lake/_checkpoints/{job}/
  ```
- [x] **(P0)** Bronze: ghi theo batch lớn (target 64–256 MB/file hoặc theo giờ), định dạng Parquet hoặc JSON nén.
- [x] **(P0)** Silver/Gold: chuyển sang Delta (hoặc Iceberg); bật schema evolution có kiểm soát.
- [x] **(P0)** Bỏ fallback im lặng; storage layer có interface rõ (`put/get/list`) và lỗi rõ ràng.
- [x] **(P1)** Job compaction định kỳ (OPTIMIZE/ZORDER hoặc coalesce) chống small files.
- [x] **(P1)** Chính sách vòng đời: Bronze giữ N ngày, Silver/Gold lâu hơn.
- [x] **(P1)** Data dictionary cho từng bảng ở `docs/data_dictionary.md` (cột, kiểu, nullable, nguồn gốc, quy tắc).
- [ ] **(P2)** Data lineage đơn giản (bảng nào sinh từ bảng nào) — có thể dùng sơ đồ trong docs hoặc OpenLineage.

### WS4 — Xử lý Spark (Batch + Streaming)
**Batch ETL (Bronze → Silver)**
- [x] **(P0)** Đọc theo partition (`event_date`) chỉ phần cần xử lý, không quét toàn bộ.
- [x] **(P0)** Pipeline: Parse → Clean → Normalize → **Validate** → **Dedup** → Write; bản ghi lỗi → Quarantine.
- [x] **(P0)** Ghi Silver bằng `MERGE` (Delta) theo khoá tự nhiên để **idempotent**.
- [x] **(P0)** Chuẩn hoá: thương hiệu, danh mục, đơn vị tiền, múi giờ, text (Unicode NFC cho tiếng Việt, giữ bản gốc).
- [x] **(P0)** Price pipeline: bảng `silver.price_history` (product_id, observed_at, price, list_price, currency); phát hiện giá bất thường chỉ để gắn cờ, không xoá.
- [x] **(P1)** Xử lý dữ liệu đến trễ (late data): quy tắc reprocess theo cửa sổ N ngày.
- [x] **(P1)** Tối ưu: partition pruning, broadcast join cho dimension nhỏ, tránh `collect()`, cấu hình `spark.sql.shuffle.partitions` hợp lý.
- [x] **(P1)** Entity resolution cơ bản chuẩn bị cho đa nguồn: bảng `dim_product` có `source`, `source_product_id`, `canonical_product_id` (ban đầu = chính nó).

**Streaming**
- [x] **(P0)** Chọn vai trò rõ ràng: streaming chỉ tính chỉ số gần thời gian thực (cửa sổ 1/5 phút); chỉ số ngày do batch sở hữu.
- [x] **(P0)** Sink thật (Delta/Postgres qua `foreachBatch` + upsert), checkpoint bền vững.
- [x] **(P0)** Watermark phù hợp; test với dữ liệu trễ/đảo thứ tự.
- [x] **(P1)** Xử lý lỗi/khởi động lại: kill job giữa chừng → khởi động lại không mất/không nhân đôi dữ liệu.
- [x] **(P1)** Xuất metrics (input rate, processing rate, batch duration) lên Prometheus.

### WS5 — Chất lượng dữ liệu (DQ)
- [x] **(P0)** Bộ luật theo từng bảng Silver: not null, kiểu, khoảng giá trị (giá > 0, rating 1–5), unique khoá, định dạng ngày, tham chiếu (review → product).
- [x] **(P0)** Kết quả DQ lưu thành bảng `dq_results` (run_id, rule, table, passed, failed_count, total_count, checked_at).
- [x] **(P0)** Ngưỡng chặn: DQ thất bại vượt ngưỡng → **dừng không ghi Gold** và gửi cảnh báo.
- [x] **(P1)** Dùng Great Expectations/Soda + SparkDataQualityEvaluator.
- [x] **(P1)** Chỉ số tươi mới (freshness): thời điểm bản ghi mới nhất theo nguồn; cảnh báo khi quá hạn.
- [x] **(P1)** Theo dõi drift cơ bản: phân phối giá, số review/ngày, tỷ lệ null theo thời gian.
- [x] **(P1)** Quarantine dashboard: số bản ghi lỗi theo lý do.

### WS6 — Gold, Warehouse & Phân tích
- [x] **(P0)** Mart: `product_daily_stats`, `brand_daily_stats`, `category_daily_stats`, `price_daily_stats` — mỗi mart khai báo **grain** và **khoá chính** trong tài liệu.
- [x] **(P0)** Schema Postgres bằng migration (Alembic/Flyway/SQL có version), không tạo bảng ad-hoc trong code.
- [x] **(P0)** Upsert qua staging + `ON CONFLICT`, bọc trong transaction; test chạy 2 lần cho ra kết quả giống nhau.
- [x] **(P0)** Lưu thành phần aggregate (`sum_rating`, `review_count`, `sum_price`, `price_count`, min/max) để tính lại chính xác.
- [x] **(P0)** Anomaly detection:
  - Review Burst: so với baseline N ngày trước, loại trừ ngày hiện tại, ngưỡng tối thiểu số review.
  - Price Anomaly: z-score/IQR trên lịch sử giá của chính sản phẩm; cần ≥ k điểm.
  - Rating Drop: so sánh cửa sổ gần với cửa sổ trước, có kiểm tra cỡ mẫu.
  - Kết quả vào bảng `anomalies` (type, entity_id, detected_at, score, params, severity).
- [x] **(P1)** Index và view cho dashboard; kiểm tra `EXPLAIN` các truy vấn chính.
- [ ] **(P1)** Elasticsearch: chỉ giữ nếu có use case (tìm kiếm review/sản phẩm); nếu có, định nghĩa mapping (analyzer tiếng Việt) và job đồng bộ idempotent.
- [ ] **(P2)** Chuẩn bị schema sẵn chỗ cho Giai đoạn 2: cột `language`, `sentiment_label`, `sentiment_score`, `aspects` (nullable).

### WS7 — Điều phối (Airflow)
- [x] **(P0)** DAG chính theo ngày: `crawl_snapshot → bronze_check → silver_etl → dq_check → gold_marts → warehouse_sync → anomaly_detect → notify`.
- [x] **(P0)** Tham số hoá theo `data_interval_start/end`; hỗ trợ **backfill** (`airflow dags backfill`).
- [x] **(P0)** `retries`, `retry_exponential_backoff`, `on_failure_callback` (email/Slack/webhook), `max_active_runs=1`.
- [x] **(P1)** Sensor/kiểm tra điều kiện đầu vào (Bronze của ngày đó đã đủ chưa).
- [x] **(P1)** Tách DAG: ingestion health, batch daily, maintenance (compaction, cleanup).
- [x] **(P1)** Airflow chạy trong Docker Compose, kết nối được Spark/MinIO/Postgres.

### WS8 — Giám sát & vận hành
- [x] **(P0)** Metrics Prometheus: Kafka lag, msg/s, số bản ghi vào Bronze/Silver/Gold, số bản ghi quarantine/DLQ, thời gian chạy job.
- [x] **(P0)** Dashboard Grafana (xuất JSON vào `monitoring/grafana/dashboards/`, tự nạp khi `make up`).
- [x] **(P1)** Alert rules: lag cao, job fail, dữ liệu cũ (freshness), DQ fail, DLQ tăng đột biến.
- [x] **(P1)** Log có cấu trúc (JSON) kèm `run_id`, `event_id`/`batch_id`; logging nhất quán trong mọi module.
- [x] **(P1)** Runbook `docs/runbook.md`: sự cố thường gặp và cách xử lý (consumer kẹt, DLQ đầy, Spark OOM, Postgres lock...).

### WS9 — Hạ tầng (Docker Compose)
- [x] **(P0)** Kafka KRaft: cấu hình `advertised.listeners` cho cả trong container và từ host; healthcheck.
- [x] **(P0)** Healthcheck + `depends_on: condition: service_healthy` cho Kafka, MinIO, Postgres, Airflow.
- [x] **(P0)** Volume bền vững cho Kafka, MinIO, Postgres; init script tạo bucket, topic, schema.
- [x] **(P0)** Ghim phiên bản image (không dùng `latest`); ghim phiên bản trong `requirements.txt` (hoặc `pyproject.toml` + lock).
- [x] **(P0)** Profile Compose: `core` (Kafka+MinIO+Postgres+Spark), `orchestration` (Airflow), `monitoring` (Prometheus+Grafana), để máy yếu vẫn chạy được.
- [x] **(P1)** `Makefile`: `up`, `down`, `demo`, `test`, `lint`, `benchmark`, `reset`.
- [x] **(P1)** Ghi yêu cầu tài nguyên tối thiểu (RAM/CPU/đĩa) trong README.
- [ ] **(P2)** Helm chart/Kubernetes: chỉ làm sau khi mọi thứ trên ổn.

### WS10 — Kiểm thử & CI/CD
- [x] **(P0)** Giữ unit test hiện có; thêm test cho các phần dễ sai: dedup, upsert idempotent, schema validation, anomaly với dữ liệu tổng hợp có đáp án biết trước.
- [x] **(P0)** **Integration test** (docker compose hoặc testcontainers): Producer → Kafka → Consumer → MinIO → Silver → Gold → Postgres, kiểm tra số bản ghi và giá trị.
- [x] **(P0)** Test **chạy lại idempotent**: chạy pipeline 2 lần → kết quả giống hệt.
- [x] **(P0)** Test **chịu lỗi**: kill consumer giữa batch → khởi động lại không mất/trùng; message hỏng → vào DLQ.
- [x] **(P1)** GitHub Actions: `ruff`/`black`, `mypy` (tuỳ chọn), `pytest` + coverage, build image; badge trong README.
- [x] **(P1)** Pre-commit hooks (ruff, gitleaks, kiểm tra file lớn).
- [x] **(P1)** Dữ liệu test nhỏ cố định (`tests/fixtures/`) + sinh dữ liệu tổng hợp có seed.

### WS11 — Benchmark "3V" (chứng minh Big Data)
- [x] **(P0)** Quy trình benchmark có thể tái hiện: `make benchmark SIZE=1m`.
- [x] **(P0)** **Volume**: chạy ETL ở 100K / 1M / 5M (hoặc lớn hơn khả thi) bản ghi; ghi thời gian, RAM, kích thước lưu trữ.
- [x] **(P0)** **Velocity**: producer ở 10 / 100 / 500 / 1000 msg/s; đo end-to-end latency (p50/p95/p99), consumer lag, tỷ lệ mất/trùng.
- [x] **(P0)** **Variety**: chứng minh đọc nhiều định dạng (CSV/JSON/JSONL/Parquet) và nhiều nguồn.
- [x] **(P1)** So sánh có/không partition, JSON vs Parquet, 1 vs nhiều executor/partition.
- [x] **(P0)** Công bố kết quả (bảng + biểu đồ + cấu hình máy) ở `docs/benchmark.md` và tóm tắt trong README.

### WS12 — Tài liệu & trình bày
- [x] **(P0)** README viết lại: mục tiêu, kiến trúc (hiện tại vs mục tiêu), yêu cầu hệ thống, quickstart bằng `docker compose`, ảnh/GIF dashboard, kết quả benchmark, hạn chế.
- [x] **(P0)** `docs/architecture.md`, `docs/data_contracts.md`, `docs/data_dictionary.md`, `docs/runbook.md`, `docs/benchmark.md`, `docs/data_sources.md`.
- [x] **(P1)** `docs/decisions/` — ADR ngắn: vì sao Kafka/Delta/Postgres, vì sao bỏ Hive/Glue, at-least-once + dedup...
- [x] **(P1)** Mục "Hạn chế đã biết & hướng phát triển".
- [ ] **(P1)** Quay video demo 3–5 phút.
- [x] Cập nhật tên/mô tả dự án cho khớp phạm vi thực tế của từng giai đoạn (tránh quảng cáo "agentic/multimodal" khi chưa có).

---

## 4. Lộ trình theo tuần (giai đoạn 1)

| Tuần | Trọng tâm | Kết quả kiểm chứng được |
|---|---|---|
| 1 | Rà soát (mục 2), dọn secrets, Docker Compose có healthcheck/volume, Makefile, chốt hợp đồng dữ liệu | `make up` lên đủ dịch vụ; danh sách bug được ghi thành Issue |
| 2 | Sửa lỗi tầng Kafka + Bronze: event_id tất định, commit offset, DLQ + replay, batch ghi, bỏ fallback im lặng | Test kill/restart consumer qua |
| 3 | Silver ETL: validate→dedup xuyên batch, quarantine, Delta MERGE, price history | Chạy lại 2 lần cho kết quả y hệt |
| 4 | Streaming có sink + checkpoint, Gold marts + upsert idempotent, schema migration | Streaming khởi động lại không trùng/mất |
| 5 | DQ (GE/Soda) + chặn Gold khi fail, anomaly sửa baseline, Airflow backfill | DAG chạy backfill 7 ngày xanh |
| 6 | Monitoring (Prometheus/Grafana), integration test, CI | CI xanh, dashboard chạy |
| 7 | Dữ liệu lớn: crawl + dataset công khai ≥ 1M, benchmark 3V | `docs/benchmark.md` hoàn chỉnh |
| 8 | Hoàn thiện tài liệu, README, demo video, rà soát cuối theo Definition of Done | Tag `v0.1.0-phase1` |

> Nếu quỹ thời gian ngắn hơn: giữ tất cả mục **P0**, bỏ **P2**, rút gọn **P1** về DQ + monitoring + benchmark.

---

## 5. Rủi ro & phương án

| Rủi ro | Mức | Giảm thiểu |
|---|---|---|
| Nguồn chặn crawler / đổi giao diện | Cao | Snapshot HTML, cảnh báo parser, có dataset công khai dự phòng, replay |
| Máy cá nhân không đủ RAM chạy cả stack | Cao | Docker profiles, giảm executor, chạy từng nhóm dịch vụ |
| Không đủ dữ liệu "big" | Trung bình | Kết hợp dataset công khai + replay nhân bản có kiểm soát (ghi rõ trong tài liệu) |
| Phạm vi phình to | Cao | Chỉ làm P0 trước; mỗi tuần có mốc kiểm chứng |
| Lỗi chính xác dữ liệu khó phát hiện | Cao | Test idempotent, DQ rules, dữ liệu tổng hợp có đáp án |
| Vấn đề pháp lý/đạo đức dữ liệu | Trung bình | Chỉ dùng cho học tập, ẩn danh người dùng, tôn trọng robots.txt, ghi rõ trong docs |
| Lệch phiên bản Spark/Kafka connector | Trung bình | Ghim phiên bản, test trong Docker |

---

## 6. Phác thảo các giai đoạn sau (chưa làm ở giai đoạn 1)

### Giai đoạn 2 — NLP tiếng Việt
- Tiền xử lý: chuẩn hoá teencode/viết tắt/không dấu, tách từ (VnCoreNLP/underthesea), phát hiện ngôn ngữ.
- Sentiment + Aspect-Based Sentiment (PhoBERT/ViSoBERT hoặc LLM hỗ trợ tiếng Việt); tập đánh giá có gán nhãn tay (≥ 500–1000 mẫu).
- Keyword/topic (BERTopic), NER sản phẩm/thương hiệu.
- Ghi kết quả vào Silver/Gold; thêm dashboard sentiment/aspect theo thời gian.
- Chỉ số đánh giá: F1/accuracy, so sánh baseline (lexicon) với mô hình.

### Giai đoạn 3 — Đa phương thức
- Tải/ lưu ảnh review/sản phẩm trên MinIO, embedding bằng CLIP/DINO, chọn **một** vector DB (Qdrant).
- Tìm kiếm tương tự ảnh/text, phân cụm sản phẩm, phát hiện ảnh trùng/spam.
- VLM (tuỳ chọn): mô tả ảnh, phát hiện lỗi sản phẩm từ ảnh review.

### Giai đoạn 4 — Agent & RAG
- RAG trên review + dữ liệu có cấu trúc; agent với tool **chỉ đọc** (query có whitelist bảng, giới hạn số dòng, timeout).
- Bộ đánh giá riêng: câu hỏi mẫu + đáp án chuẩn, đo độ chính xác SQL, độ trung thực (groundedness), độ trễ.
- Guardrails: chống prompt injection từ nội dung crawl về, che thông tin cá nhân.

### Giai đoạn 5 — Hạ tầng sản xuất (tuỳ chọn)
- Kubernetes/Helm, CI/CD triển khai, xác thực/phân quyền, mã hoá, sao lưu & phục hồi.

---

## 7. Nhật ký tiến độ

| Ngày | Nội dung hoàn thành | Vấn đề gặp phải | Việc tiếp theo |
|---|---|---|---|
| 2026-10-01 | Hoàn thành P0-1 đến P0-5: Chuyển đổi toàn bộ sang MinIO backend thật, Kafka Bronze writer, Spark Batch ETL (Clean, Validate, Dedup), Spark Gold Marts, và kiểm chứng E2E thật trên DW PostgreSQL. | Cần đảm bảo test suite chạy offline ổn định khi không có MinIO/Kafka daemon. | Triển khai P1 & P2 mở rộng. |
| 2026-10-02 | Triển khai P1 & P2: Price Spark pipeline mở rộng, Spark DQ & Quarantine, Benchmark 3V (163K rec/s), Dashboard dọn sạch hardcode kết nối DW, Airflow DAG điều phối, Prometheus exporter, Lakehouse Delta format. | Baseline anomaly tính cả điểm đang xét gây giảm độ nhạy; Consumer throttling gây chậm batch. | Audit toàn diện plancheck.md giai đoạn 1. |
| 2026-10-02 | Hoàn thành Audit Giai đoạn 1 (Phase 1 Definition of Done): Băm `event_id` tất định sha256, dedup xuyên batch, sửa baseline anomaly, tạo replay DLQ, Makefile, tài liệu hoá `architecture.md`, `data_contracts.md`, `data_dictionary.md`, `data_sources.md`, `benchmark.md`, `runbook.md`. 100% test passing (77/77). | - | Chuẩn bị kế hoạch chuyển giao cho Giai đoạn 2 (NLP tiếng Việt). |

---

## 8. Phụ lục — Mẫu Issue cho lỗi dữ liệu

```
Tiêu đề: [data-correctness] Dedup chỉ trong batch, không xuyên batch
Nhãn: bug, silver, P0
Mô tả: Chạy ETL 2 ngày liên tiếp với cùng product → bản ghi trùng trong Silver.
Tái hiện: pytest tests/test_dedup_cross_batch.py (đang fail)
Kỳ vọng: Silver chỉ có 1 bản mới nhất theo (source, entity_id).
Tiêu chí xong: test xanh + chạy lại 2 lần cho kết quả giống nhau.
```
