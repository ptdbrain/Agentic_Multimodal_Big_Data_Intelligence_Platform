# Rà soát code và tính đúng đắn — 2026-10-25

## Phạm vi và kết quả kiểm tra

Rà soát phiên bản workspace hiện tại, gồm cả các thay đổi chưa commit. Không sửa code ứng dụng hoặc dữ liệu. Đã đọc các luồng ingestion, Kafka, batch/streaming, storage, Gold, warehouse, dashboard, orchestration và các test liên quan.

- Parse cú pháp thành công **120 file Python**.
- `ruff check .`: **All checks passed**. Cấu hình hiện chỉ bật E9, F63, F7, F82; kết quả này không chứng minh logic nghiệp vụ đúng.
- Chạy trên bản sao `/tmp/sentinel-audit-repo`, với môi trường riêng `/tmp/sentinel-audit-venv`:

  ```bash
  python -m pytest tests/test_schemas.py tests/test_data_quality.py \
    tests/test_analytics.py tests/test_advanced.py tests/test_ingestion.py \
    tests/test_crawler_validator.py -q --disable-warnings
  ```

  Kết quả: **85 passed, 2 warnings**.
- Full unit collection chưa hoàn tất: `tests/test_spark_etl.py:6` import PySpark trực tiếp và môi trường chưa có PySpark. Không chạy Spark JVM, Kafka/MinIO/PostgreSQL thật, Airflow executor thật, crawler trực tiếp hoặc toàn bộ benchmark 3V. Python kiểm tra là 3.14, trong khi CI dùng 3.11.
- Những kiểm chứng bổ sung bên dưới gọi code thật với input nhỏ. Kiểm chứng rollback có chủ động chèn lỗi promotion và dùng đối tượng I/O tối giản; không phải một lần chạy Spark/S3A thật.

## Phát hiện ưu tiên cao

### A01 — Rollback Parquet không xử lý exception lúc promotion

**Vị trí:** `spark/storage_safe.py:243–265`.

Sau khi chuyển target sang backup, lời gọi chuyển staging sang target không nằm trong `try/except`. Nếu rename ném exception, nhánh rollback không chạy. Khi chèn `OSError` tại bước promotion, kết quả thực tế là `target.exists() == False`, còn thư mục backup và staging. Dữ liệu cũ chưa mất vật lý nhưng bảng không còn ở đường dẫn phục vụ bình thường.

Ngoài ra, rollback khi rename trả `False` không kiểm tra kết quả khôi phục. Hai bước rename cũng tạo khoảng trống ngay cả trên local; gọi đây là “atomic swap” với độc giả đồng thời là quá mạnh. Nhánh S3A dùng `fs.rename`, không có transaction/pointer commit trong code này.

**Hướng sửa:** bắt cả exception lẫn kết quả `False`, kiểm tra khôi phục; dùng snapshot/version và cơ chế publish phù hợp nếu cần nhất quán với độc giả đồng thời. Test cả promotion failure và rollback failure.

### A02 — Crawler thay đổi sự thật của review trước validation

**Vị trí:** `ingestion/crawler/tiki.py:755–793`.

Đã tái hiện:

| Raw input | Kết quả chuẩn hóa |
| --- | --- |
| `rating=0, purchased=False` | `rating=5.0, verified_purchase=True` |
| `rating=99` | `rating=5.0` |
| Chỉ có `id`, không có rating/content | 5 sao, verified, tự sinh câu review; qua JSON Schema |

Nguyên nhân: `rating or 5.0`, clamp 1–5, `verified_purchase` mặc định `True` và tự tạo nội dung. DQ phía sau thấy dữ liệu hợp lệ dù raw không có đủ thông tin.

**Hướng sửa:** giữ giá trị thiếu/sai để quarantine, chỉ xác nhận mua hàng khi có bằng chứng rõ ràng; phân biệt nội dung do hệ thống tạo với nội dung khách hàng.

### A03 — `price_id` không định danh một lần quan sát giá

**Vị trí:** `ingestion/crawler/tiki.py:721–732`; `spark/batch/spark_etl_job.py:42–52, 610–619`.

Hai giá 100 và 200 của `tiki_1` ở hai ngày khác nhau đều nhận `pr_tiki_1_0001`. Silver deduplicate theo `price_id`, giữ timestamp mới nhất, vì vậy lần crawl sau có thể ghi đè lịch sử giá. `BaseEcommerceCrawler.crawl_category` cũng tạo ID từ sản phẩm và thứ tự trong danh sách.

**Hướng sửa:** định danh observation bằng nguồn, sản phẩm, người bán và thời điểm/snapshot ID; test nhiều lần crawl và replay cùng snapshot.

### A04 — DQ gate chặn replay và update hợp lệ

**Vị trí:** `spark/batch/spark_etl_job.py:680`; `spark/quality/spark_dq.py:450–474`.

`records_duplicate` cộng cả trùng trong batch và khóa đã tồn tại ở Silver. Khi chạy lại cùng dữ liệu, hoặc cập nhật catalog sản phẩm hiện có, tỷ lệ này có thể là 100%. Gate mặc định chỉ cho 5%.

Đã gọi gate với 10 bản ghi hợp lệ, điểm 100, 10 khóa tồn tại từ trước: kết quả `passed=False`, `duplicate_ratio=1.0`. Đây là kiểm chứng công thức và truy vết code, chưa chạy hai lượt E2E Spark thật.

**Hướng sửa:** tách replay/update hợp lệ khỏi duplicate lỗi; áp dụng ngưỡng chất lượng cho đúng loại duplicate. Bổ sung chạy E2E hai lần với gate bật.

### A05 — Trang Products lỗi với schema catalog thực tế

**Vị trí:** `analytics/descriptive/stats.py:96–98`; `dashboard/pages/3_Products.py:23`.

Catalog đã có `review_count`; bảng aggregate cũng có cột này. Merge sinh `review_count_x` và `review_count_y`, sau đó truy cập `review_count` gây `KeyError`. Đã tái hiện bằng chính `data/sample/products.parquet` và `reviews.parquet`.

**Hướng sửa:** đặt tên riêng cho số review từ catalog và số review đã ingest; chọn rõ cột dashboard hiển thị. Test với đầy đủ schema catalog.

### A06 — Đường Kafka của Spark Streaming gọi method không tồn tại

**Vị trí:** `spark/streaming/spark_streaming_job.py:123`; `config/settings.py:31–40`.

Gọi `settings.kafka.get_bootstrap_servers()`, nhưng `KafkaSettings` chỉ có thuộc tính `bootstrap_servers`. Kiểm tra runtime xác nhận `hasattr(...) == False`. Khi Spark reader khởi tạo được, đường này sẽ gặp `AttributeError` trước khi đọc Kafka.

**Hướng sửa:** thống nhất API cấu hình và test đường Kafka thật. Script acceptance hiện truyền `source_path` để đọc file, nên không đi qua lời gọi này.

### A07 — Airflow DAG phụ thuộc bộ nhớ của một process

**Vị trí:** `orchestration/dags/sentinel_daily_pipeline.py:77–144`.

Các task truyền mode, đường dẫn Bronze và kết quả Spark qua `_DAG_RUN_STATE`. Khi task chạy process/worker riêng, trạng thái không được truyền theo run. `spark_batch` có thể nhận `None` và đọc toàn bộ topic Bronze thay vì batch của run hiện tại. Hàm này còn trả `etl_res` chứa Spark DataFrame qua return của PythonOperator, không phù hợp XCom JSON mặc định.

**Hướng sửa:** truyền run ID, object paths và metrics JSON qua XCom/manifest; không truyền Spark/Pandas DataFrame. Test bằng executor thật hoặc ít nhất tách process. Standalone runner cùng process không kiểm chứng được điều này.

### A08 — `event_id` có thể trùng giữa hai review khác nhau

**Vị trí:** `kafka/producers/stream_producer.py:69–71`.

Hash dùng `source|entity_key|event_time`, trong đó entity key thường là `product_id`, cũng là Kafka partition key. Đã gửi hai review `r1`, `r2` của cùng sản phẩm, cùng `review_date`: hai envelope có cùng `event_id`. ID cũng không bao gồm event type.

**Hướng sửa:** tách partition key khỏi event identity; dùng review/price observation ID và version phù hợp. Test tính duy nhất giữa các sự kiện và tính ổn định khi replay.

## Những lỗi và khoảng trống còn lại

| Mã | Phát hiện và bằng chứng | Hướng xử lý |
| --- | --- | --- |
| A09 | `ingestion/api/collector.py:17–43`: `mock_mode=False` không thay đổi hành vi. Dùng endpoint `127.0.0.1:1` vẫn trả Smart Gadget với giá ngẫu nhiên, không có HTTP request. | Implement live request/pagination/retry hoặc báo chưa hỗ trợ live. |
| A10 | `scripts/health_check.py:71–78`: health check Spark chỉ kiểm tra chuỗi cấu hình rồi trả True. Đã trả OK khi chưa có PySpark. | Kiểm tra dependency, tạo session và chạy action tối thiểu; tách “configured” khỏi “healthy”. |
| A11 | `spark/analytics/gold_builder.py:360–375`: gate bỏ qua khi metrics rỗng hoặc đọc metrics lỗi. Đã kiểm chứng nhánh điều khiển với builder con bỏ qua aggregation: `enforce_dq_gate=True` vẫn hoàn tất khi metrics rỗng. | Fail khi metrics bắt buộc thiếu/lỗi; yêu cầu metrics thuộc đúng run/dataset. |
| A12 | `analytics/anomaly/review_burst.py:57`: baseline lấy mọi cửa sổ khác, gồm cả tương lai. Khi thêm 100 review ngày sau, burst 10 review đã được báo ở ngày trước biến mất. | Nếu dùng cảnh báo theo thời gian, chỉ lấy lịch sử trước thời điểm đang xét; bổ sung cửa sổ không có review. |
| A13 | `analytics/anomaly/rating_anomaly.py:52`: `rolling(window=w_days)` đếm số dòng ngày có dữ liệu, không phải số ngày lịch. Review cách nhau nhiều tháng vẫn vào “7-day baseline”. | Dùng time-based window và quy định trọng số theo số review. |
| A14 | `spark/etl/normalizer.py:66`: regex loại dấu âm. Đã tái hiện `normalize_price('-100 VND') == 100.0`. | Giữ dấu âm để validation từ chối; không đổi dữ liệu sai thành giá hợp lệ. |
| A15 | `spark/quality/spark_dq.py:520`: thiếu thời gian thực thì báo `processing_time_ms=10`. Batch pipeline không đưa timing thực vào report. Batch ID cố định còn ghi đè lịch sử DQ. | Đo thực tế hoặc ghi null/unknown; tách latest view khỏi lịch sử theo run ID. |
| A16 | `storage/search_indexer.py:3–35`: mô tả Elasticsearch nhưng implementation chỉ lưu list và tìm substring trong RAM. Không có Elasticsearch client/network path. | Ghi rõ đây là local search hoặc thực hiện tích hợp ES và acceptance test riêng. |
| A17 | `storage/lakehouse.py:63–95`: schema chỉ được ghi vào log, không được enforce. Đã append DataFrame có schema khác hoàn toàn và được chấp nhận. Version lấy max+1, không có lock/CAS, nên chưa đủ cơ sở cho tuyên bố ACID/concurrent atomic merge. | Dùng table format có transaction, hoặc thu hẹp cam kết và thêm schema/concurrency control. |

## Hardcode, dữ liệu giả và độ tin cậy của test

### Phần mô phỏng được khai báo rõ

`data/generator.py`, `MockEcommerceCrawler`, buffer Kafka offline và `StreamingJob` simulator hữu ích cho demo/unit test. CLI `run_streaming.py` mặc định simulator và có in mode. Các phần này không tự chứng minh gian lận test; cần giữ nhãn simulator rõ khi demo hoặc báo cáo hiệu năng.

### Các điểm có thể gây hiểu nhầm

- API live, review Tiki tự điền, Spark health check và DQ timing cố định là các hành vi đã xác nhận, có thể làm người xem đánh giá sai chức năng hoặc chất lượng dữ liệu.
- `data/public/ecommerce_public_reviews.jsonl` có **80 bản ghi nhưng chỉ 2 nội dung review khác nhau**, user/email theo số thứ tự và nhãn `public_ecommerce_dataset`. Chưa tìm thấy bằng chứng nguồn tải, giấy phép, manifest hay raw export để xác nhận đây là dữ liệu public thực. Nên gọi là fixture cho tới khi có provenance; mẫu lặp không đủ để kết luận toàn bộ dữ liệu là giả.
- `scripts/benchmark.py:916–934` gọi nguồn là authentic dựa trên ID/source/rating và hash file hiện tại. Đây là kiểm tra hình dạng/nhãn, không chứng thực nơi dữ liệu được thu thập; record tự tạo với nhãn đúng có thể qua.
- JSON và Parquet mẫu có cùng số dòng nhưng khác nội dung: products khác giá; reviews khác rating; prices khác giá/timestamp. Vì vậy các đường đọc khác định dạng có thể cho analytics khác nhau. Kiểm tra ingestion hiện chỉ đối chiếu số dòng nên vẫn pass. Chưa kết luận sự khác biệt là chủ ý hay artifact chưa đồng bộ.
- Factory Spark chưa thấy khai báo load Kafka connector hoặc `hadoop/jars` vào classpath. Workspace có hai JAR Hadoop/AWS nhưng chưa xác nhận đường chạy cài đặt sạch có đủ connector; cần kiểm tra tích hợp trên Python/Java giống CI, không suy ra chỉ từ file JAR tồn tại.

### Vì sao 85 test pass vẫn có lỗi

- `tests/test_ingestion.py:103–107` tính lại đúng cùng công thức hash như implementation, nhưng không thử hai review khác ID cùng product/time. Test đang củng cố quy tắc gây collision.
- `tests/test_analytics.py:76–95` kiểm tra `GoldAggregator` Pandas, trong khi pipeline chính dùng `SparkGoldBuilder`; pass không chứng minh kết quả production builder đúng.
- Các fixture catalog tối giản không có `review_count`, che lỗi merge của trang Products.
- `scripts/verify_streaming_dq_acceptance.py:459–464` đọc stream từ file, không kiểm chứng khởi tạo Kafka stream.
- `tests/test_spark_etl.py` có nhánh skip nếu tạo Spark không thành công; cần báo skipped riêng. Import PySpark ở đầu file vẫn có thể làm collection lỗi trước khi tới skip.
- Chưa thấy cơ sở kết luận có chủ ý làm giả toàn bộ test. Có test thật kiểm tra giá trị cụ thể, lỗi dữ liệu và storage. Vấn đề chính là phạm vi kiểm tra lệch luồng chạy thật, một số assertion sao chép implementation và các lỗi chưa có regression test.

## Thứ tự khắc phục đề xuất

1. A01–A08: tính toàn vẹn dữ liệu, dữ liệu review/giá, replay và những đường chạy đang lỗi.
2. A09–A11, A15–A16: làm đúng hoặc khai báo rõ chức năng live, health, DQ và search.
3. A12–A14, A17: thống nhất ý nghĩa thuật toán, validation và cam kết lakehouse.
4. Đồng bộ fixtures; bổ sung regression test cho từng tình huống trên; chạy E2E hai lượt có DQ gate, Kafka streaming, Airflow tách worker và fault injection storage trên hạ tầng thật.

Không nên dùng số test pass hoặc các acceptance/report hiện tại để khẳng định toàn bộ hệ thống đã sẵn sàng production. Báo cáo này phân biệt lỗi đã tái hiện, lỗi xác định bằng truy vết code và các điểm còn cần kiểm tra tích hợp.
