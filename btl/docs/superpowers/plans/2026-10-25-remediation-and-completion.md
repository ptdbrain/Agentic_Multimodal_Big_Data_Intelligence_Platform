# SentinelAI Remediation and Completion Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking. Direct execution in this session is the proposed method; no delegation is required.

**Goal:** Sửa 17 phát hiện trong báo cáo rà soát, hoàn thiện các chức năng đang mô phỏng dưới tên chức năng thật, và cung cấp bằng chứng kiểm thử cho các luồng được hỗ trợ.

**Architecture:** Giữ pipeline ingestion → Kafka → Bronze → Spark → Silver/Gold → warehouse/dashboard. Tách định danh sự kiện, dữ liệu quan sát và trạng thái theo run; kiểm tra chất lượng trước khi publish dữ liệu phục vụ. Dùng snapshot bất biến và metadata commit cho Lakehouse; giao tiếp task bằng manifest JSON.

**Tech Stack:** Python 3.11, Java 17, Spark 3.5.0 như Docker hiện tại, pytest, Kafka, MinIO, PostgreSQL, SQLite cho local/test, Streamlit, Elasticsearch qua REST và thư viện `requests` hiện có.

**Spec:** [Báo cáo 17 phát hiện](../../reports/code_audit_2026-10-25.md). Các quyết định bổ sung và phạm vi nghiệm thu nằm trong phần Thiết kế đề xuất bên dưới.

**Status:** Người dùng đã duyệt ngày 2026-10-25; phần triển khai và kiểm chứng local đang được hoàn tất. Xem `../../reports/remediation_status.md` để phân biệt bằng chứng local với các gate service bên ngoài còn chờ.

**Repository layout (2026-10-26):** Source đã chuyển vào `btl/`. Các đường dẫn code trong kế hoạch tính từ `btl/`; Git, `.github/workflows/ci.yml` và cấu hình `.env` chung vẫn ở thư mục cha.

## Tiến độ xác minh ngày 2026-10-26

| Nhóm task | Trạng thái |
| --- | --- |
| 1–6, 11 | Đã triển khai và kiểm chứng local; rollout/migration production chưa tự apply. |
| 7, 8, 9, 10 | Code và kiểm tra local đã có; service thực/Kafka/Airflow/PostgreSQL-MinIO/Elasticsearch còn chờ môi trường. |
| 12 | 188 test local pass, HTTP thật 1 test pass, lint/hash pass; evidence và CI gates đã lưu. Benchmark integration/Tiki live chưa nghiệm thu. |

Các checkbox bên dưới là checklist thiết kế từng bước, không được đồng loạt đánh dấu complete khi integration evidence chưa có. Kết quả chi tiết nằm ở báo cáo nghiệm thu.

## Thiết kế đề xuất

### Phương án và lựa chọn đề xuất

1. Chỉ vá exception và chỉnh nhãn demo: ít thay đổi nhưng không hoàn thiện API live, search và tính nhất quán của Lakehouse.
2. **Sửa theo từng thành phần, giữ kiến trúc hiện tại:** thêm regression test, hoàn thiện live adapters, kiểm soát transaction và xác minh E2E. Đây là phương án đề xuất vì xử lý đủ các phát hiện và tận dụng hạ tầng đang có.
3. Thay pipeline/lakehouse bằng một nền tảng khác: phạm vi migration lớn, khó phân biệt lỗi cũ với lỗi tích hợp mới; chưa đề xuất trong đợt sửa này.

Các quyết định ở đây do người lập kế hoạch đề xuất, chưa phải các lựa chọn đã được người dùng duyệt.

### Hợp đồng hành vi cần đạt

- Dữ liệu thiếu/sai phải bị từ chối hoặc quarantine; không tự bịa rating, review text, verified purchase hay latency.
- Cùng logical event khi replay có ID ổn định; các review và price observations khác nhau có ID khác nhau. Partition key không quyết định event identity.
- Replay không tăng số logical rows, không bị coi là duplicate chất lượng kém. Update thực sự vẫn cập nhật bản mới nhất.
- DQ gate bắt buộc phải có metrics đúng run và đủ dataset; thiếu metrics hoặc storage lỗi phải chặn publish Gold.
- Mọi telemetry phải là giá trị đo, hoặc `null` có lý do. Lưu lịch sử theo run và cung cấp latest view riêng.
- API live và Elasticsearch phải giao tiếp service thực; simulator chỉ hoạt động khi chọn rõ local/mock.
- Không khẳng định directory rename là atomic trên S3A. Publish Parquet được bảo vệ rollback và giới hạn một writer; Lakehouse có transaction metadata riêng để hỗ trợ nhiều writer/reader.
- Không dùng binary Parquet hoặc báo cáo benchmark cũ làm bằng chứng cho phiên bản code mới.

### Transaction và tương thích dữ liệu

- Parquet truyền thống: staged write → validate → backup → promote → verify; bắt cả `False` và exception. Nếu recovery thất bại, giữ backup và báo trạng thái recovery-required với đường dẫn cụ thể. Không tự xóa backup khi chưa xác nhận target đúng.
- Lakehouse: snapshot path duy nhất theo UUID, commit metadata qua PostgreSQL khi integration/production và SQLite khi local/test. Mỗi commit dùng expected version và transaction để phát hiện conflict; không ghi đè version của writer khác. Read/time travel chọn snapshot chỉ từ commit metadata đã thành công.
- Dùng schema fingerprint và kiểm tra schema trước write. Mặc định từ chối thay đổi tên/type/nullability; schema evolution không thuộc đợt này.
- Legacy Lakehouse log chỉ được import theo lệnh migration tường minh sau khi kiểm tra snapshot. Không tự chuyển đổi/xóa dữ liệu hiện có khi import module hoặc khởi động app.
- Silver/Gold hiện tại vẫn đọc được. Đổi event/price ID và DQ schema cần manifest migration, chạy thử trong namespace riêng rồi đối chiếu; không reset lake hoặc warehouse để làm test pass.

## Global Constraints

- Python 3.11, Java 17, Spark 3.5.0 như Docker hiện tại.
- Không sửa `AGENTS.md`; giữ nguyên những thay đổi chưa commit của người dùng.
- Mỗi task: viết regression test → xác nhận fail đúng nguyên nhân → sửa → xác nhận pass → review diff.
- Chỉ stage file/hunk của task; không dùng `git add .`, reset hoặc tự commit phần thay đổi có sẵn. Mỗi task là một đơn vị commit sau khi tách được diff an toàn.
- Unit test không cần dịch vụ ngoài, dùng `tmp_path` và khôi phục settings/environment; không ghi vào lake/DB đang sử dụng.
- Integration test thiếu dependency/service phải fail với lý do; không skip test bắt buộc rồi công bố pass. Network crawler test tách khỏi kiểm tra deterministic.
- Không tự ý đổi ngưỡng DQ để che lỗi. Không thay kết quả mong đợi chỉ nhằm làm test xanh.
- Báo cáo tiến độ phân biệt implemented, unit verified, integration verified và blocked.

## Review Focus

1. Promotion và rollback cùng lỗi: backup vẫn tồn tại, lỗi có recovery path, không báo publish thành công — Task 2.
2. Replay, sửa review cùng timestamp và hai price observations cùng sản phẩm: ID ổn định/khác nhau đúng nghĩa, không mất lịch sử — Task 3.
3. Thiếu một dataset hoặc metrics của run khác: DQ gate đóng, Gold hiện tại không bị thay thế — Task 5.
4. Retry Airflow ở worker mới và hai run đồng thời: không lẫn manifest, không gửi DataFrame qua XCom — Task 8.
5. Hai Lakehouse writers cùng expected version và reader đang đọc: chỉ commit được snapshot hợp lệ, conflict rõ ràng, history cũ đọc được — Task 10.

## Ma trận bao phủ

| Phát hiện | Task xử lý | Bằng chứng đóng lỗi |
| --- | --- | --- |
| A01 rollback Parquet | 2 | Fault injection local + MinIO/S3A, kiểm tra dữ liệu cũ |
| A02 review giả hóa; A14 giá âm | 4 | Raw invalid không trở thành valid; boolean đúng |
| A03 price ID; A08 event ID | 3 | Replay/update/multiple observations |
| A04 replay DQ; A11 gate mở; A15 timing/history | 5 | E2E hai lần, missing/stale metrics, clock đo thật |
| A05 dashboard merge | 6 | Full catalog schema, Streamlit smoke |
| A06 Kafka streaming; A10 health | 7 | Kafka source thật, Spark action, outage |
| A07 Airflow state | 8 | Task tách process + Airflow executor |
| A09 live API | 9 | Local HTTP server và lỗi HTTP, không random fallback |
| A16 Elasticsearch | 9 | ES REST index/search/persistence/outage |
| A12 burst; A13 rating window | 11 | Không dùng tương lai; cửa sổ theo lịch |
| A17 Lakehouse | 10 | Schema enforcement, conflict, time travel |
| Fixtures/provenance/test coverage/classpath | 1, 7, 12 | So nội dung, manifest nguồn, đúng production builder |

## Task 1: Môi trường tái lập và phân loại test

**Files:** Modify `requirements.txt`, `pyproject.toml`, `conftest.py`, `Makefile`, `.github/workflows/ci.yml`, `tests/test_spark_etl.py`; create `tests/conftest.py`.

**Interfaces:** Các marker `unit`, `spark`, `integration`, `live`; `spark_session` fixture chỉ khởi tạo khi test yêu cầu; settings/lake/DB được isolate theo test.

- [ ] Viết test `test_unit_collection_without_pyspark` chạy subprocess collection cho nhóm unit thuần Python; assert return code 0, không import PySpark ở cấp module của test thuần Python.
- [ ] Chạy test và ghi lỗi collection hiện tại. Phân loại các test tạo JVM là `spark`, không gắn đồng thời marker unit/integration gây chọn nhầm.
- [ ] Đồng bộ dependency Spark/Python với stack trên; tạo lệnh `make test-unit`, `make test-spark`, `make test-integration`, `make test-live`. Unit thuần không bao gồm Spark; job Spark riêng là bắt buộc trong CI.
- [ ] Chạy collection từng nhóm; xác nhận test bắt buộc không bị skip vì session lỗi. Lưu danh sách dependency thực tế và platform vào artifact CI.
- [ ] Review diff; chuẩn bị commit `test: isolate unit spark and integration environments`.

## Task 2: Publish Parquet có recovery kiểm chứng được — A01

**Files:** Modify `spark/storage_safe.py`, `spark/batch/spark_etl_job.py`, `spark/streaming/spark_streaming_job.py`; create `tests/test_storage_recovery.py`.

**Interfaces:** Giữ `SafeParquetIO.publish_parquet_atomic(...) -> None` để tương thích caller, đánh dấu rõ tên legacy; thêm `ParquetPublishError` chứa `target_path`, `backup_path`, `stage_path`, `recovery_required: bool`.

- [ ] Viết `test_publish_exception_restores_original`, `test_publish_false_restores_original`, `test_rollback_failure_preserves_backup`, `test_count_mismatch_does_not_replace_target`. Assert nội dung/PK cũ, không chỉ sự tồn tại của thư mục; rollback lỗi phải giữ backup.
- [ ] Run: `pytest tests/test_storage_recovery.py -q`; xác nhận fail tại các nhánh hiện chưa bảo vệ.
- [ ] Bao transaction recovery quanh backup/promote/verify; xử lý exception và False; chỉ dọn backup sau xác minh target. Serialize writer local bằng cơ chế lock hiện thực tại lớp storage; remote classic Parquet yêu cầu một orchestrated writer, không quảng cáo multiwriter atomic.
- [ ] Chạy unit và Spark recovery; thêm integration fault injection S3A với batch dữ liệu thật. Xác nhận không xóa backup khi recovery thất bại.
- [ ] Review; chuẩn bị commit `fix(storage): recover failed parquet publication safely`.

## Task 3: Event identity và price observations — A03, A08

**Files:** Create `ingestion/identity.py`, `tests/test_event_identity.py`; modify `kafka/producers/stream_producer.py`, `ingestion/crawler/base.py`, `ingestion/crawler/tiki.py`, `ingestion/crawler/mock.py`, `tests/test_ingestion.py`.

**Interfaces:** `make_event_id(event_type: str, source: str, payload: dict) -> str`; `make_price_id(source: str, product_id: str, seller: str, observed_at: str) -> str`. Canonical UTC timestamp; stable sorted JSON serialization; source namespace bắt buộc.

- [ ] Viết test assert hai review khác ID/cùng product/time có event ID khác; replay cùng payload giữ ID; sửa nội dung cùng review/time tạo event mới; đổi partition key không đổi identity; timestamp cùng instant khác timezone cho cùng identity.
- [ ] Viết test assert observation khác thời điểm/seller có price ID khác, replay observation giữ ID; chạy `pytest tests/test_event_identity.py -q` và xác nhận fail trước sửa.
- [ ] Sinh event ID từ source, type và fingerprint canonical payload; ưu tiên logical ID trong payload, không dùng routing key. Price ID lấy fingerprint observation, không dùng vị trí trong trang. Thiếu observation time phải reject thay vì lấy now mỗi lần replay.
- [ ] Chạy unit và Spark test `test_price_history_survives_recrawl_and_replay`: hai observations tồn tại, replay không tăng số hàng. Lập migration mapping từ legacy ID dựa trên raw Bronze; dữ liệu không truy hồi được phải ghi rõ.
- [ ] Review; chuẩn bị commit `fix(ingestion): separate event identity from routing and snapshots`.

## Task 4: Giữ trung thực raw review và giá — A02, A14

**Files:** Modify `ingestion/crawler/tiki.py`, `ingestion/crawler/validator.py`, `spark/etl/normalizer.py`, `data/schemas/review.json`; create `tests/test_ingestion_integrity.py`.

**Interfaces:** Giữ `standardize_review(raw_review, product_id=None) -> dict`; trả giá trị thiếu/sai để validator phân loại. `verified_purchase` là true chỉ với `purchased is True` hoặc `verified_purchase is True`; không suy ra từ approved. Missing/false thành false và giữ lý do unknown trong metadata nguồn.

- [ ] Test parametrized rating `None`, `0`, `99`, `-1`, `NaN`, bool, chuỗi sai: không trở thành 5 sao hợp lệ; review không content/title không được tự sinh text. Test title-only giữ đúng text có thật và đánh dấu origin. `purchased=False` không bị biến thành true.
- [ ] Test `normalize_price('-100 VND') == -100.0`, giữ các định dạng VN/US đang hỗ trợ, giá âm bị quality reject. Run `pytest tests/test_ingestion_integrity.py -q` và xác nhận fail.
- [ ] Bỏ clamp/default rating và câu review tự tạo; parse numeric hữu hạn; preserve raw provenance sau PII sanitization. Chuỗi `"false"` không được coi true. Giữ dấu âm trong price parser và reject input mơ hồ theo validation rõ ràng.
- [ ] Chạy crawler/schema/quality tests; xác nhận invalid records có reason và raw reference để audit, không đi vào Silver hợp lệ. Cập nhật expectation cũ đang chấp nhận sửa dữ liệu sai.
- [ ] Review; chuẩn bị commit `fix(crawler): preserve invalid review and price evidence`.

## Task 5: DQ cho replay, gate bắt buộc, timing/history thật — A04, A11, A15

**Files:** Modify `spark/batch/spark_etl_job.py`, `spark/quality/spark_dq.py`, `spark/analytics/gold_builder.py`, `scripts/run_e2e_pipeline.py`, `database/schema.sql`, `database/export_gold.py`, `dashboard/db_connector.py`, `monitoring/metrics_exporter.py`; create `tests/test_dq_contract.py`, `tests/test_pipeline_replay.py`.

**Interfaces:** Metrics giữ `intra_batch_duplicate_count`, `cross_batch_duplicate_count` và thêm `run_id`, `batch_id`, `processing_time_ms: int | None`. `check_dq_gate(..., required_datasets, run_id)` chỉ tính duplicate ratio chất lượng từ intra-batch; báo cross-batch riêng. Report ID là `dq:{run_id}:{dataset_name}`; retry cùng run không nhân bản history.

- [ ] Test replay 10/10 khóa cũ, 0 lỗi: gate pass; batch 10 dòng có duplicate lỗi vượt 5%: gate fail. Thiếu products/reviews hoặc metrics run khác: fail; ép storage read lỗi: fail trước khi bất kỳ Gold writer nào chạy.
- [ ] Test duration bằng clock inject cho đúng 125 ms, thiếu duration thành null có reason; hai run có hai report, retry run cũ giữ số report. Run `pytest tests/test_dq_contract.py -q`, ghi các failure.
- [ ] Tách duplicate counters; kiểm tra metrics completeness; đưa run ID xuyên E2E. Đo bằng `perf_counter`; persist report history và latest-per-dataset query. Kiểm tra DQ trước publish Gold; dataset optional chỉ bắt buộc khi run khai báo có dữ liệu.
- [ ] Chạy Spark/E2E hai lượt với gate bật. Assert đầy đủ PK và giá trị latest, không chỉ count; batch invalid bị chặn Gold và warehouse, dữ liệu Gold cũ còn nguyên. Migration DQ schema phải chạy lặp an toàn trên DB hiện có.
- [ ] Review; chuẩn bị commit `fix(quality): distinguish replay and enforce run-scoped evidence`.

## Task 6: Dashboard dùng schema thực và số liệu đúng — A05

**Files:** Modify `analytics/descriptive/stats.py`, `dashboard/pages/3_Products.py`, `dashboard/db_connector.py`, `dashboard/utils.py`; create `tests/test_dashboard_contract.py`.

**Interfaces:** `compute_product_metrics(...) -> pd.DataFrame` giữ `review_count` là số review ingest; catalog count đổi thành `catalog_review_count`. KPI rating tổng = tổng rating có trọng số theo review count, không `AVG` các daily averages không trọng số.

- [ ] Test catalog có `review_count`, review rỗng, product không có review; assert không KeyError, không mất product, `catalog_review_count=420` và `review_count=2` cùng tồn tại.
- [ ] Test summary hai nhóm: 1 review 1 sao + 9 review 5 sao → 4.6; run `pytest tests/test_dashboard_contract.py -q`, xác nhận failure phù hợp.
- [ ] Đặt alias trước merge, chọn metric rõ ràng trên UI; sửa KPI query weighted average. Loaders phải đóng DB connection sau query; local fallback hiển thị nguồn sample/silver thực thay vì chỉ nhãn SQLite.
- [ ] Chạy test với fixtures đủ schema và Streamlit smoke cho Products/Overview/Trends; integration query phải khớp SQL oracle độc lập.
- [ ] Review; chuẩn bị commit `fix(dashboard): handle catalog counters and weighted metrics`.

## Task 7: Spark Kafka thật, connector và health thật — A06, A10

**Files:** Modify `config/settings.py`, `spark/spark_session.py`, `spark/streaming/spark_streaming_job.py`, `scripts/health_check.py`, `scripts/run_streaming.py`, `scripts/verify_streaming_dq_acceptance.py`, `.github/workflows/ci.yml`; create `tests/test_spark_health.py`, `tests/integration/test_kafka_streaming.py`.

**Interfaces:** Dùng `settings.kafka.bootstrap_servers` nhất quán. `check_spark() -> bool` chỉ true sau Spark action `range(1).count() == 1`; CLI exit khác 0 khi health fail. Config connector dùng phiên bản Spark trên, không phụ thuộc JAR thủ công không được load.

- [ ] Test thiếu PySpark, JVM lỗi, master không truy cập được đều health false; Kafka reader nhận đúng endpoint. Run `pytest tests/test_spark_health.py -q`, xác nhận fail trước sửa.
- [ ] Khai báo/load connector Kafka và S3A phù hợp runtime; thêm kiểm tra classpath trước query. Endpoint host/Docker chọn tường minh, không suy từ `spark://` rằng driver nằm trong Docker.
- [ ] Sửa CLI truyền `--topic` thực vào query, mode mặc định/nhãn engine rõ ràng. Health không tạo session mới cho mỗi lần UI render; cung cấp timeout và đóng session nếu check tự sở hữu session.
- [ ] Kafka integration: gửi 2 review hợp lệ + 1 sai → đúng aggregate và quarantine; gửi duplicate, late record; restart checkpoint không tăng metric. Đọc source Kafka, không thay bằng file. CI fail nếu Spark/connector thiếu hoặc query exception.
- [ ] Review; chuẩn bị commit `fix(streaming): verify kafka source and spark readiness`.

## Task 8: Airflow theo run, không truyền bộ nhớ/DataFrame — A07

**Files:** Create `orchestration/run_manifest.py`, `tests/test_orchestration_manifest.py`, `tests/integration/test_airflow_execution.py`; modify `orchestration/dags/sentinel_daily_pipeline.py`, `scripts/run_e2e_pipeline.py`, `database/schema.sql`; add `requirements-airflow.txt`.

**Interfaces:** `RunManifest` gồm `run_id`, `mode`, `source_type`, `batches_by_topic`, `required_datasets`, `silver_paths`, `dq_report_ids`. `save_manifest(manifest) -> str`, `load_manifest(path) -> RunManifest`. XCom chỉ chứa manifest URI và JSON metrics nhỏ.

- [ ] Test `json.dumps(task_result)` thành công; 2 subprocess đọc cùng manifest cho kết quả giống nhau; 2 run có path riêng; retry giữ run ID; thiếu manifest phải fail, không fallback đọc cả Bronze.
- [ ] Run `pytest tests/test_orchestration_manifest.py -q`; xác nhận lỗi state-sharing/serialization hiện tại.
- [ ] Bỏ `_DAG_RUN_STATE`, truyền manifest URI bằng XCom/context; mỗi task load mode trước storage/session. Silver/Gold run output dùng namespace staging riêng; publish canonical output được serialize để tránh hai E2E writers ghi đè nhau.
- [ ] Chạy task trong process riêng và Airflow LocalExecutor đúng version dependency đã khóa; test retry task ở process mới, failure downstream không đánh dấu run success. Standalone dùng cùng manifest API.
- [ ] Review; chuẩn bị commit `fix(orchestration): persist serializable run manifests`.

## Task 9: Hoàn thiện API live và Elasticsearch — A09, A16

**Files:** Modify `ingestion/api/collector.py`, `storage/search_indexer.py`, `config/settings.py`, `.env.example`, `scripts/health_check.py`; create `tests/test_api_live.py`, `tests/test_search_backend.py`, `tests/integration/test_elasticsearch.py`.

**Interfaces:** `APICollector(..., mock_mode: bool = False, session=None)` yêu cầu endpoint khi live; response chuẩn `{data: list[dict], has_next: bool, page: int}`. `SearchIndexer(backend=None)` chọn `elasticsearch` ở integration/production, `memory` khi local/test tường minh; không fallback ngầm.

- [ ] Dùng HTTP server local thật: assert request có page/page_size/category, headers và timeout; 429/5xx retry hữu hạn, 4xx/schema lỗi fail; live endpoint chết không trả Smart Gadget. Mock test phải truyền `mock_mode=True`.
- [ ] Test ES REST request/response, bulk partial failure phải raise, limit/rating/filter đúng; run `pytest tests/test_api_live.py tests/test_search_backend.py -q`, xác nhận fail trên implementation cũ.
- [ ] Implement requests session, retry/backoff có giới hạn và schema response validation. Implement ES `_bulk`, `_search`, health, stable document ID; cấu hình endpoint/auth ngoài code. Giữ memory backend phục vụ unit/demo với nhãn rõ.
- [ ] Integration với Elasticsearch container hiện có: index, refresh, tìm bằng instance mới, update cùng ID, mất kết nối phải báo lỗi; không dùng in-memory result để chứng minh ES hoạt động.
- [ ] Review; chuẩn bị commit `feat(integrations): implement live api and elasticsearch adapters`.

## Task 10: Lakehouse schema và commit transaction — A17

**Files:** Create `storage/lakehouse_metadata.py`, `database/lakehouse_schema.sql`, `scripts/migrate_lakehouse_metadata.py`, `tests/test_lakehouse_transactions.py`; modify `storage/lakehouse.py`, `database/warehouse.py`; create `tests/integration/test_lakehouse_concurrency.py`.

**Interfaces:** `MetadataStore.commit(table: str, expected_version: int, snapshot_path: str, schema_json: str, operation_id: str) -> int`; `get_commit(table: str, version: int | None) -> dict`; conflict raises `ConcurrentCommitError`. Local SQLite và remote PostgreSQL dùng cùng transaction contract.

- [ ] Test append schema khác bị reject trước publish; overwrite schema khác cũng reject; time travel giữ snapshot cũ; retry operation ID không tạo commit mới; commit metadata lỗi không lộ snapshot chưa commit.
- [ ] Test hai process cùng expected version: đúng một commit thành công, writer còn lại nhận conflict rõ ràng; reader không bao giờ thấy snapshot mới trước commit. Run `pytest tests/test_lakehouse_transactions.py -q`, xác nhận thiếu enforcement/concurrency hiện tại.
- [ ] Dùng UUID snapshot file bất biến, transaction metadata với unique `(table, version)` và operation ID. Kiểm tra expected version dưới lock transaction. Append/upsert tạo snapshot hoàn chỉnh rồi commit; conflict không tự overwrite. Chỉ garbage-collect orphan sau retention bằng lệnh riêng.
- [ ] Migration dry-run liệt kê version/schema/path/record count, apply idempotent và giữ legacy log. PostgreSQL+MinIO integration chạy multiwriter và fault injection; chỉ công bố isolation thực sự đã test, không gọi log này là Delta protocol.
- [ ] Review; chuẩn bị commit `fix(lakehouse): enforce schema and transactional snapshot commits`.

## Task 11: Analytics theo thời gian thực sự — A12, A13

**Files:** Modify `analytics/anomaly/review_burst.py`, `analytics/anomaly/rating_anomaly.py`, `config/settings.py`; create `tests/test_temporal_anomalies.py`.

**Interfaces:** Giữ public detector signatures. Burst baseline dùng các bucket trước candidate trong `baseline_window_hours` cấu hình; mặc định đề xuất 24h, bỏ candidate và tương lai. Rating baseline là `[t-window_days, t)`, tính weighted mean theo số review; min-history đo ngày có quan sát thực.

- [ ] Test thêm dữ liệu tương lai không thay kết quả candidate cũ; cửa sổ giờ thiếu được điền count=0 nhưng không tạo rating giả; input đảo thứ tự không đổi kết quả.
- [ ] Test review cách nhau 30 ngày không nằm trong 7-day baseline; ngày có 1 và 9 review phải weighted; timezone khác nhau được chuẩn UTC; timestamp sai có số lượng reject rõ. Run `pytest tests/test_temporal_anomalies.py -q`, xác nhận fail trước sửa.
- [ ] Implement prior-only, bounded baseline; rolling theo datetime index và exclusion current window. Đảm bảo event IDs deterministic sau chuẩn UTC; không âm thầm trả danh sách rỗng khi cả dataset parse lỗi.
- [ ] Chạy analytics unit và integration đối chiếu SQL/Python oracle độc lập; ghi thay đổi ý nghĩa threshold vào tài liệu.
- [ ] Review; chuẩn bị commit `fix(analytics): use causal calendar-based anomaly baselines`.

## Task 12: Fixtures, provenance, production-path tests và nghiệm thu

**Files:** Create `data/manifest.json`, `scripts/verify_data_manifest.py`, `scripts/sync_sample_formats.py`, `tests/test_data_manifest.py`, `tests/test_spark_gold_builder.py`, `docs/reports/remediation_status.md`; modify `data/generator.py`, `scripts/benchmark.py`, `tests/test_analytics.py`, `tests/test_ingestion.py`, `.github/workflows/ci.yml`, `README.md`, `docs/architecture/data_lineage.md`, `docs/benchmark.md`.

**Interfaces:** Manifest có `dataset_id`, `source_kind` (`synthetic`, `unverified`, `captured_live`), `source_url`, `collected_at`, `license`, `sha256`, schema version và format paths. `verify_manifest(path) -> dict` trả riêng `content_verified` và `provenance_status`, không suy ra authentic từ chuỗi ID.

- [ ] Test dữ liệu tự tạo có `source=tiki_crawler` vẫn không thành verified provenance; fixture thiếu nguồn ghi unverified/synthetic; JSON/CSV/Parquet khác giá/rating bị phát hiện dù số dòng giống nhau.
- [ ] Test SparkGoldBuilder trên dataset oracle: 4 review rating `5,4,1,2` cho mean=3, positive/negative ratio=0.5; prices `24M,26M` cho mean=25M. So nội dung/grain của cả 4 marts; không gọi Pandas GoldAggregator làm substitute. Run các test và xác nhận failure/gap trước sửa.
- [ ] Lập manifest từ dữ liệu hiện có, không tạo provenance bịa; bổ sung script format sync có dry-run/diff và backup. Sample JSON là nguồn canonical đề xuất cho sample; dữ liệu `data/public` hiện tại giữ nguyên bytes nhưng gắn unverified cho tới khi có chứng cứ nguồn. Chỉ regenerate derived fixtures sau xem diff, không thay raw crawler captures.
- [ ] Chạy toàn bộ acceptance matrix bên dưới trong namespace/DB riêng. Thêm CI service MinIO thật và ES thật; server S3 tự viết nếu còn dùng phải có job riêng và ghi đúng loại service. Live crawler kiểm tra riêng, không dùng fallback synthetic khi bị chặn mạng.
- [ ] Xuất report mới gắn Git revision, dirty-state fingerprint, command, dependencies, service images, source provenance và timestamps thật. Báo cáo cũ giữ nguyên làm lịch sử; không sửa tay số benchmark. Review từng A01–A17 và chuẩn bị commit `test: verify production paths and dataset provenance`.

## Thứ tự thực hiện và phụ thuộc

`1 → 2 → 3 → 4 → 5 → 6 → 7 → 8 → 9 → 10 → 11 → 12`.

Task 3–4 định nghĩa input cho Task 5; Task 5 định nghĩa run/report contract cho Task 8; Task 7 tạo integration runtime cho Task 10 và 12. Thứ tự tuần tự là phương án thực thi đề xuất để giảm xung đột trên workspace đang có nhiều thay đổi.

## Acceptance matrix và điều kiện hoàn tất

| Gate | Lệnh/kịch bản | Điều kiện đạt |
| --- | --- | --- |
| Static | `make lint` | Compile/Ruff pass; không thay scope linter để né lỗi |
| Pure unit | `make test-unit` | Không cần service/JVM; không collection error; regression A01–A17 có test chủ sở hữu |
| Spark | `make test-spark` | Batch/Gold/file-stream/storage tests chạy JVM thực, không skip bắt buộc |
| Real integration | `make test-integration` | Kafka, MinIO, PostgreSQL, Elasticsearch thực; lỗi service làm test fail đúng lý do |
| Replay | E2E hai lượt cùng fixture, DQ bật | PK/value đúng, không duplicate logical rows, report history phân biệt run |
| Updates | Hai crawl snapshots và review edit cùng timestamp | Giữ đủ price history, event IDs đúng, latest entity đúng |
| Recovery | Lỗi stage/promote/rollback/metadata commit | Không báo success giả, backup/snapshot cũ đọc được hoặc recovery-required rõ ràng |
| Orchestration | Process riêng, retry và hai DAG runs | Manifest không lẫn, XCom JSON, output publish được serialize |
| Live | `make test-live` | Trạng thái crawl thực được báo; blocked/failure không chuyển thành synthetic success |
| Benchmarks | CLI 3V integration với seed và kích thước ghi trong artifact | Report được sinh từ lần chạy thật; timing/resource measurement boundaries rõ; không yêu cầu đạt performance số cũ |
| UI | Streamlit smoke và query oracle | Products không crash; nguồn dữ liệu và KPI đúng; connection được đóng |

Một task chưa có integration evidence vẫn ghi “unit verified”, không đánh dấu “integration verified”. Hoàn tất toàn bộ chỉ khi ma trận bắt buộc pass, các migration dry-run đã được xem, và tất cả phát hiện có code/test/evidence liên kết. Mạng chặn live crawler phải ghi limitation riêng, không được biến thành kết luận crawler đã hoạt động trên Tiki.

## Rủi ro chuyển đổi và rollback

- Đổi ID không thể khôi phục observations đã bị legacy dedup làm mất nếu raw Bronze không còn; báo số observation truy hồi được, không sinh lịch sử giả.
- DQ/history schema migration phải chạy trong transaction, có backup và script kiểm tra version; chỉ cập nhật latest view sau khi lịch sử mới hợp lệ.
- Thuật toán causal có thể cho ít/nhiều anomaly khác trước; chấp nhận theo oracle mới, không ép bằng số anomaly demo cũ.
- Trước rollout: chạy namespace/DB thử nghiệm, so content checksum/PK/count; giữ mapping legacy → new ID và snapshot cũ. Rollback bằng code version và metadata/snapshot đã lưu, không `make reset`.

## Checklist tự kiểm tra kế hoạch

- [x] A01–A17 có task, file và bằng chứng đóng lỗi.
- [x] Năm failure modes trong Review Focus có test sở hữu.
- [x] Public interfaces giữa identity, DQ, orchestration và metadata được nêu rõ.
- [x] Giữ nguyên thay đổi của người dùng và phân biệt raw data với generated fixture.
- [x] Phân biệt implementation, unit evidence, real integration evidence và live-network limitation.
- [x] Các lựa chọn mở rộng — transaction metadata, live API contract, window baseline — được ghi là đề xuất cần duyệt trước triển khai.

## Kết quả thực thi cuối (2026-10-26)

A01–A17 đã có implementation và regression. Lượt cuối: 189 local passed, 36 integration passed, 1 Tiki live passed; lint/compile pass. Airflow LocalExecutor 1 passed và real-service E2E hai lượt có bằng chứng trước reboot. Dockerfile MinIO checksum-pinned đã build trực tiếp và chạy container kiểm thử. Benchmark 3V đã sinh lại đầy đủ sau sửa provenance, accounting khớp ở acceptance workload. Xem `docs/reports/remediation_status.md`, JUnit và JSON cùng thư mục.

Kế hoạch sửa lỗi và nghiệm thu acceptance đã thực hiện; checklist ở từng task là trình tự ban đầu, kết quả thực tế được ghi tại báo cáo và execution ledger. Không tự commit/merge, apply migration hoặc deployment. Production-scale load/soak và kết quả GitHub Actions chưa nằm trong bằng chứng đã chạy.
