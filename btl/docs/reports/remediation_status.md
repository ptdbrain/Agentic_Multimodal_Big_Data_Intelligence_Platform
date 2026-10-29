> **Latest processing status2026-10-29: complete.** Docker46/46,current-source content replay and seven-page PostgreSQL AppTest passed; network-separated worker ownership verified. See `processing_docker_acceptance.json`. Earlier blocked/access statements below are historical.

# Kết quả sửa lỗi và nghiệm thu SentinelAI

Cập nhật ngày **2026-10-26**, sau khi source chuyển vào `btl/`. Git, `.github/` và cấu hình `.env` chung vẫn ở thư mục cha. Đây là kết quả trên workspace hiện tại, chưa phải một release đã commit hoặc triển khai production.

## Kết quả đã chạy

| Kiểm tra | Kết quả |
| --- | --- |
| Unit thuần Python và dashboard smoke | **177 passed** |
| Toàn bộ local: unit + Spark JVM + E2E replay + streaming checkpoint | **189 passed, 0 failed, 0 skipped**; 38 test khác không thuộc nhóm local |
| API live qua HTTP server loopback thực, chạy riêng | **1 passed** |
| Migration ID | **4 passed**, đã nằm trong bộ local; không phải 4 test cộng thêm |
| Thích ứng đường dẫn sau khi chuyển `btl/` | **3 passed**, đã nằm trong bộ local |
| `make lint` và kiểm tra Ruff riêng cho `storage/` | Pass |
| Workflow YAML | Parse thành công; cấu hình working directory, pip cache và Airflow DAG folder trỏ vào `btl/` |
| Hash các artifact trong `data/manifest.json` | Khớp nội dung hiện có |
| Dry-run migration giá từ canonical sample | 525 mappings, 0 unresolved, 0 legacy collisions; **không apply vào lake đang dùng** |

Runtime kiểm chứng: **Python 3.11.17, Java 17.0.21, Spark 3.5.0, pandas 2.3.3, NumPy 1.26.4, PyArrow 14.0.2**. Bộ local có 218 `DeprecationWarning` từ `kafka-python`/`importlib.resources`; không bị ẩn hoặc tính thành lỗi ứng dụng.

Bằng chứng lưu cùng source:

- [Thông tin runtime, lệnh chạy, fingerprint source và phạm vi chưa kiểm chứng](remediation_validation.json).
- [JUnit của 189 test local](remediation_local.xml).
- [JUnit HTTP thực](remediation_http.xml).
- [Báo cáo phát hiện ban đầu](code_audit_2026-10-25.md).
- [Kế hoạch triển khai](../superpowers/plans/2026-10-25-remediation-and-completion.md).

## Đối chiếu các lỗi

| Mã | Thay đổi chính | Mức kiểm chứng hiện tại |
| --- | --- | --- |
| A01 | Backup bằng copy có kiểm tra trước khi xóa target; xử lý lỗi promote/rollback, giữ recovery path; khóa writer theo path. Gold bỏ overwrite/fallback phá hủy. | Fault injection local và Spark pass; S3A/MinIO fault injection: 1 passed (JUnit đã lưu). |
| A02 | Bỏ mặc định/clamp rating, tự tạo review text và mặc định verified. Giá trị thiếu/sai giữ nguyên để validator từ chối. | Unit crawler/schema pass; Tiki live: 1 passed (JUnit đã lưu). |
| A03 | Price ID dựa trên observation, không dựa thứ tự trong danh sách; có công cụ lập mapping legacy và báo collision/unresolved. | Unit identity/migration và Spark replay pass. |
| A04 | Tách duplicate trong batch khỏi khóa đã tồn tại, không phạt replay/update hợp lệ. | DQ unit và E2E hai lượt với gate bật pass. |
| A05 | Tách `catalog_review_count` khỏi `review_count` ingest; KPI rating có trọng số. Hiển thị nguồn dữ liệu thực, gắn nhãn sample là synthetic. | Unit và Streamlit Products AppTest pass. |
| A06 | Dùng thuộc tính Kafka endpoint có thật; truyền topic CLI tới stream; khai báo connector theo stack Spark đã chốt. | Local Spark/file-stream pass; Kafka source thật và checkpoint: 1 passed. |
| A07 | Bỏ trạng thái global; dùng manifest JSON lưu bền vững, retry giữ trạng thái; downstream kiểm tra lease, CLI và DAG cùng cơ chế sở hữu output. | Unit, handoff qua process, retry và CLI lease pass; Airflow LocalExecutor: 1 passed; metadata PostgreSQL, pipeline local. |
| A08 | Tách event identity khỏi partition key; replay ổn định, review/update khác nhau có ID khác. | Unit identity và ingestion pass. |
| A09 | API live gọi HTTP thật với pagination, timeout, retry hữu hạn và kiểm tra response; mock chỉ khi chọn rõ. | Unit và HTTP loopback thật pass; endpoint kinh doanh cụ thể chưa được cung cấp. |
| A10 | Spark health phải chạy được JVM action; preflight fail khi action lỗi. Local TopicManager không thử bootstrap Kafka. | Unit và Spark JVM pass. |
| A11 | Gate đóng khi thiếu/stale metrics, NaN/Inf hoặc giá trị không hợp lệ; kiểm tra dataset/run trước Gold. | Unit và E2E local pass. |
| A12 | Burst baseline chỉ lấy lịch sử trước candidate, có cửa sổ giới hạn và giờ không có review. | Unit causal/invariance pass. |
| A13 | Rating baseline dùng ngày lịch, loại hiện tại và tính trọng số theo số review. | Unit khoảng cách ngày, trọng số và regression pass. |
| A14 | Price parser giữ dấu âm để validation từ chối. | Unit normalization/integrity pass. |
| A15 | Bỏ timing mặc định 10 ms; đo thời gian thực hoặc ghi unavailable; DQ history theo run, migration schema giữ lịch sử cũ. | Unit migration và E2E history hai lượt pass. |
| A16 | Có adapter Elasticsearch REST thật; memory backend được chọn tường minh, không fallback khi service lỗi. | Unit request/error behavior pass; Elasticsearch persistence/search thật pass. |
| A17 | Snapshot bất biến, schema enforcement và transaction catalog SQLite/PostgreSQL; kiểm tra version và intent của operation ID bên trong transaction. Có dry-run/import log legacy. | SQLite, time travel, schema, replay và race hai process pass; PostgreSQL/MinIO và concurrent writers thật pass. |

## Công việc vừa hoàn thiện sau khi tiếp tục

- `scripts/plan_identity_migration.py` đã hoàn chỉnh, không bị dở ở phần đọc JSON/JSONL. Test trước đây “2 failed → 2 passed” là chu trình tái hiện rồi sửa, không phải hai lỗi đang tồn tại. Đã bổ sung kiểm tra raw không phải object và thiếu source; cả 4 test hiện pass. Công cụ không bịa source hoặc observation time, không ghi đè report/lake.
- CI chạy lệnh tại `btl/`; artifact và pip cache path đúng vị trí mới. Python ưu tiên `btl/.env`, sau đó mới dùng `.env` của Git root. Docker Compose dùng cùng thứ tự ưu tiên.
- Cache bytecode cũ mang theo đường dẫn trước khi di chuyển đã được làm mới. Phân loại Spark dùng marker tường minh, không phụ thuộc `inspect.getsource()`.
- Quy tắc ignore cho lake outputs và JAR đã được bổ sung tại source root mới. Không di chuyển ngược source hoặc ghi đè dữ liệu raw cũ.

## Nghiệm thu dịch vụ thực

Sau khi người dùng cấp lại quyền Docker, stack cô lập `sentinel-verify-20261026` đã khởi động lại. Lượt mới nhất sau sửa provenance: **189 local passed**, **36 integration passed**, **1 Tiki live passed**, không failure hoặc skip. Một số test cũ trong gate integration tự chọn local mode; test dịch vụ chuyên biệt kiểm tra Kafka Structured Streaming/checkpoint, PostgreSQL/MinIO, concurrent writers, S3A recovery và Elasticsearch.

- [JUnit integration](remediation_integration_all.xml), [Tiki live](remediation_tiki_live.xml). [Airflow LocalExecutor: 1 passed](remediation_airflow.xml) là bằng chứng lượt trước reboot; không chạy lại Airflow ở lượt cuối. Airflow dùng PostgreSQL metadata, các stage pipeline dùng Spark/SQLite local.
- [E2E replay qua dịch vụ thực](remediation_integration_replay.json): 15 products, 20 reviews, 525 prices; content hashes Silver giữ nguyên qua hai lượt, DQ history tăng 3 → 6. Đây là bằng chứng lượt trước reboot; gate integration được chạy lại sau reboot.
- [Benchmark 3V đã sinh lại đầy đủ](remediation_3v_integration.json): Volume 100/1.000 records, một iteration; Velocity rates 50/200 messages/s, mỗi lượt gửi/nhận 100; Variety sáu định dạng synthetic × 100 records và 9 review Tiki live. Tất cả accounting khớp. Đây là acceptance workload, chưa phải kiểm thử tải production.
- MinIO thực được build từ [upstream release source](https://github.com/minio/minio/tree/RELEASE.2024-03-30T09-41-56Z), sau khi registry từ chối pull. Recipe `docker/minio.Dockerfile` khóa SHA256 source; Compose/CI dùng recipe này. **Build trực tiếp recipe đã pass**; container được tạo lại từ image đó và health HTTP 200. Image: `sha256:52638615045eedbb62e934a5609eb3a94458c28b3e81b71ede7b8d170b6c9aff`.

Phát hiện bổ sung: `benchmark_variety` hardcode `provenance_verified=true`. Đã thay bằng `source_verified`, thêm `content_verified` và `provenance_status`. Regression đã quan sát RED→GREEN, nằm trong 189 local test. Báo cáo mới kiểm tra mọi dòng Variety: `provenance_verified=false`, `content_verified=true`; dữ liệu sinh giữ nhãn `synthetic`, Tiki giữ `unverified`. Hash xác nhận nội dung, không chứng thực nguồn.

Runtime `/tmp` đã khôi phục. Bản sao venv và Ivy cache lưu trong `.runtime/remediation-venv-backup.tar.gz` và `.runtime/sentinel-ivy-backup.tar.gz` (được ignore). Stack kiểm thử hiện giữ chạy để tiếp tục kiểm tra; không đụng tới stack hoặc volume khác.

Chưa thực hiện production-scale load/soak, triển khai production hoặc apply migration dữ liệu người dùng. Không coi acceptance workload là chứng nhận production ready. Không có kết quả GitHub Actions; bằng chứng ở đây là lệnh chạy trực tiếp tại workspace.

## Các quyết định triển khai và giới hạn

- Giữ workspace và các thay đổi sẵn có, không tự commit/merge. Baseline `/tmp` từ phiên cũ đã mất sau reboot; fingerprint source hiện tại được ghi trong JSON evidence, không giả định rằng HEAD đại diện cho code đã test.
- Serialize classic pipeline bằng SQL lease xuyên suốt run, kết hợp khóa per-path. Đổi lại, một DAG crash giữ lease để phục hồi tường minh; không tự chiếm lease hết hạn khi worker cũ có thể còn chạy. Classic Parquet có thể có khoảng trống với concurrent reader; không gọi đó là atomic multi-reader transaction.
- Lakehouse dùng catalog SQL riêng, không nhận là Delta protocol. Schema evolution mặc định bị từ chối. Writer xung đột phải retry từ snapshot mới, không tự ghi chồng.
- Giữ file sample/public/raw đã có; tạo canonical fixtures riêng. Hash xác nhận bytes, không chứng thực nguồn. Dataset public/crawler cũ vẫn được ghi **unverified**; không nâng nhãn chỉ vì record chứa `tiki_crawler`.
- Burst baseline tính cả giờ trống trong khoảng lịch sử, nên một regression fixture thay kỳ vọng `3.0` bằng `30/11`. Đây là thay đổi nghĩa thuật toán theo thiết kế đã duyệt, không ép số anomaly bằng báo cáo demo cũ.
- Chốt stack pandas/NumPy/PyArrow phù hợp Spark 3.5.0. Python 3.14/Java 25 của máy không được dùng làm runtime kiểm chứng Spark.
- Workspace filesystem từ chối thư mục `pyarrow.` trong wheel; venv/cache chuyển về `/tmp`, Python/Java giữ trong `.runtime` được ignore. Nếu reboot xóa `/tmp`, có thể giải nén bản sao runtime vào `/tmp`; evidence trong repository không mất.
- Review độc lập trước khi chuyển thư mục đã phát hiện 1 Critical và 6 Important; các sửa tương ứng có regression local. Lượt tiếp tục này kiểm chứng lại source sau di chuyển, không được mô tả là một lần review độc lập thứ hai.
- Minor còn hoãn: warning dependency từ `kafka-python`. Không sửa hoặc ẩn code dependency chỉ để log xanh; các giới hạn tải production và deployment được ghi rõ ở trên.

Các bước migration hiện cung cấp dry-run/mapping và kiểm tra schema; không tự apply vào dữ liệu production. Lịch sử giá đã mất do dedup trước đây chỉ có thể khôi phục khi raw Bronze còn giữ observation đó, không thể tái tạo bằng dữ liệu sinh.


## Cập nhật xử lý và phân tích — 2026-10-27

Đã triển khai các điểm còn thiếu trong kế hoạch xử lý:

- Gold không còn dùng `current_date()` khi review/price thiếu observation time; bản ghi thiếu ngày bị loại khỏi daily mart và không tạo số liệu giả.
- `price_daily_stats` không biến catalog price thành một observation của ngày chạy. `product_daily_stats` đánh dấu rõ `price_source=observed|catalog_fallback|unavailable`.
- Spark read errors ở integration/production được raise nguyên trạng; fallback pandas chỉ còn cho local/test.
- Normalizer giữ `price_raw` và `normalization_errors` cho giá thiếu/sai; Spark product standardization giữ `price_raw` trước khi cast.
- Test subprocess transaction được cố định để không phụ thuộc working directory/PYTHONPATH sau khi source chuyển vào `btl/`.

Bằng chứng mới: 5 Gold regression pass, 16 analytics/unit pass, toàn bộ local gate **194 passed, 0 failed, 0 skipped** (JUnit: `remediation_processing_local.xml`), Ruff và compile pass. Một lượt chạy từ Git root từng lộ lỗi import subprocess; đây là lỗi cách chạy, đã được cố định và xác nhận lại từ `btl/`.

Lượt integration cuối sau thay đổi xử lý chưa thể hoàn tất vì Docker socket hiện bị từ chối quyền; stack cô lập đã dừng nên 5 test dịch vụ (MinIO/Kafka/Elasticsearch/PostgreSQL) fail do `connection refused`, trong khi 31 test còn lại pass. Đây là giới hạn môi trường hiện tại, không dùng kết quả này để tuyên bố integration xanh. Cần cấp lại ACL `/var/run/docker.sock` và chạy lại `pytest -m 'integration and not airflow and not live'`.

Cập nhật analytics 2026-10-27:

- Trend review loại riêng timestamp/rating không hợp lệ thay vì làm mất toàn bộ batch; thêm trạng thái `data_quality_status`.
- Price trajectory loại riêng timestamp/price lỗi, giữ các observation hợp lệ.
- Descriptive rating chỉ tính rating 1–5 và trả `rating_sample_size` để dashboard biết độ tin cậy mẫu.

Local gate mới sau toàn bộ thay đổi: **197 passed, 0 failed, 0 skipped**; analytics test **8 passed**. Integration chưa chạy lại được vì Docker socket chưa có quyền và stack đang dừng.

Cập nhật comparative analytics 2026-10-27:

- Brand/category comparison loại rating ngoài miền 1–5 khỏi review count và average rating.
- Product metrics áp dụng cùng bộ lọc rating hợp lệ; catalog review count vẫn tách biệt với review ingest.
- Giá được ép numeric trước khi tính average/min/max.

Analytics + dashboard contract tests: **13 passed**.

Cập nhật temporal anomalies và pandas Gold 2026-10-27:

- Pandas Gold loại riêng review/price có timestamp lỗi hoặc rating/price không hợp lệ, giữ lại observation hợp lệ.
- Rating-drop và review-burst detector dùng `errors=coerce`, lọc timestamp/rating invalid thay vì fail cả batch.

Local processing gate mới nhất: **200 passed, 0 failed, 0 skipped**. Integration Docker vẫn cần ACL/socket và stack đang chạy lại trước khi nghiệm thu dịch vụ.

Completion audit correction: the approved processing plan remains incomplete beyond Docker integration. Outstanding requirements are tracked in processing_plan_remaining.md. Latest price fix excludes nonfinite/nonpositive values from anomaly/volatility and corrects the small-sample IQR regression; local JUnit now reports 202 tests, 0 failures.

Ingestion time contract: all three Spark datasets preserve invalid/missing ingested_at as null, add ingested_at_raw and ingestion_time_available; quarantine time remains processing time. Six regressions RED→GREEN. Replay ignores only ingestion metadata, preserving business-field comparison. Fresh local gate:208 passed, zero failures/errors/skips. Remaining approved work is tracked in processing_plan_remaining.md.

Gold read policy continuation: explicit constructor opt-in required for local/test fallback, typed missing-path errors only; permission/schema/S3A failures propagate, including optional price/catalog inputs. Integration rejects fallback opt-in. Five regressions RED→GREEN. Local gate 213 passed, zero failures/errors/skips. Docker info rechecked: socket permission denied; full plan remains incomplete per processing_plan_remaining.md.

Gold lineage/time continuation: GoldBuilder now adds run_id, source_run_id, data_as_of and time_available to every built mart. Observation date uses per-row coalescing across date/review_date/timestamp/created_at/ingested_at with UTC parsing; invalid rows remain undated and are excluded by daily marts. Two lineage/time regressions RED→GREEN. Optional missing price Silver input is allowed; required review/product inputs remain strict. Fresh local gate:215 passed, zero failures/errors/skips.

Gold publish validation continuation: before Gold publication, validator now rejects duplicate mart grain, non-finite numeric metrics, ratio values outside 0..1 and avg_rating outside 1..5. Regression observed RED→GREEN; fake sink contract remains compatible. Fresh local gate:216 passed, zero failures/errors/skips.

Dashboard quality panel and 100k benchmark continuation: panel wired into Overview/Products/Trends/DQ. Missing metadata is reported unknown; explicit missing-time/catalog-fallback/zero-rating-sample warnings verified with Streamlit AppTest. Dashboard8 passed, full local219 passed/0 failed/0 skipped; targeted Ruff and compile pass.

100k benchmark completed in isolated local namespace (synthetic, seed42, one measured iteration, warmed Spark local[2]):100000 input =92000 Silver +5000 quarantine +3000 duplicates. Bronze write7.471s, Spark ETL56.613s, measured total64.084s, peak process-tree RAM1556.3MB. Artifact:processing_volume_100k.json. Boundary excludes Kafka, Gold aggregation and dashboard; this is not a production SLA or full-pipeline load claim. Full approved plan remains incomplete per processing_plan_remaining.md.

Anomaly lineage continuation: burst, rating-drop, Z-score and IQR events now emit run_id, data_quality_status, baseline window and sample size. Relevant anomaly/analytics17 passed; full local gate223 passed/0 failed/0 skipped. Event persistence through Gold/warehouse and Docker integration remain unverified.

Anomaly warehouse lineage: additive schema migration and SQLite round-trip preserve detector run_id, DQ status and baseline sample; Anomalies page displays these fields. Dashboard/warehouse contract tests:8 passed. PostgreSQL/Docker integration remains unverified.

Gold validation final continuation: null grain/count/price checks are in the pre-publish validator; full local gate after this change **226 passed**, zero failures/errors/skips.

Current-run DQ continuation: Gold refuses lineage-bearing DQ metrics without explicit current run_id, preventing stale metrics from authorizing marts. DQ2 regression pass; full local gate 227 passed, zero failures/errors/skips.


## Lượt hoàn thiện xử lý/phân tích — 2026-10-28

Bộ local mới nhất: **324 passed, 38 deselected, 0 failures/errors/skips**, 102.23s; Ruff và compileall pass. 300 cảnh báo dependency không bị ẩn. JUnit: `remediation_processing_local.xml`; fingerprint và phạm vi kiểm chứng: `remediation_validation.json` → `processing_plan_update`. Các kết quả cũ phía trên là lịch sử.

- Chuẩn hóa pandas/Spark giữ raw trước trim, invalid thành null kèm đầy đủ mã lỗi; phân biệt thời gian quan sát và thu thập. 32 trường hợp đối chiếu gồm giá locale/scientific, UTC/epoch/slash dates, alias brand/category/currency. Sửa cả lỗi chuyển Double sang chuỗi exponent gây sai giá.
- ETL pandas cũ bỏ clamp rating, giá0 giả và việc loại dữ liệu trước khi đếm input; có7 regression. Báo cáo pipeline giữ quarantine, thiếu thời gian và lỗi theo nhóm qua Gold/SQLite/dashboard; kiểm thử thực5 input =2 valid +3 quarantine.
- Gold hai engine thống nhất nguồn giá/cỡ mẫu/rating có trọng số. Brand/category daily average catalog price theo mỗi sản phẩm có review trong ngày, không theo số review; cần backfill comparison trước rollout vì số lịch sử có thể đổi.
- Overview dùng cửa sổ7 ngày lịch, hiển thị thiếu ngày/cỡ quan sát và ẩn điểm làm mượt chưa đủ dữ liệu. Cờ strict time mặc định true; local fallback mặc định false.
- Anomaly run/DQ, đúng mẫu lịch sử và đơn vị/bounds/method được kiểm tra xuyên detector→Gold→SQLite→dashboard, bao gồm replay2 lượt. Legacy metadata chưa biết giữ null.

**Chưa hoàn tất toàn bộ kế hoạch.** Docker socket vẫn denied; PostgreSQL và dịch vụ thật chưa được kiểm chứng lại sau thay đổi. Còn backfill comparison, một số hợp đồng dữ liệu/serving, concurrency và full-path load. Danh sách hiện hành ở `processing_plan_remaining.md`; quy tắc vận hành ở `../processing_analytics.md`. Không apply migration/backfill vào dữ liệu production của người dùng.


## Cập nhật xử lý/phân tích — 2026-10-29

Bộ local hiện tại: **393 passed,38 deselected,0 failures/errors/skips**,123.55s; Ruff/compileall pass.390 cảnh báo dependency giữ nguyên. XML và machine report đã cập nhật. Toàn bộ implementation đang được review độc lập; chưa đánh dấu hoàn tất mục tiêu.

Sửa thêm: thiếu cả cột bắt buộc không được bỏ qua; mọi validator từ chối giá+Inf; metadata Silver giữ qua warehouse; production Gold không thể tắt DQ/current-run gate. Std/volatility một mẫu giữ null; catalog keys phải duy nhất. Export nhiều bảng dùng một transaction, từ chối các mart khác/không rõ run; dashboard giữ read snapshot; cạnh tranh giữa hai process được kiểm tra bằng SQL lease thật. Sửa KPI thiếu mẫu/đơn vị% và Gold anomaly rỗng không dùng lại kết quả cũ.

Đo tải bằng script dùng lại được `scripts/benchmark_processing.py`:100000 review =92000 Silver+5000 quarantine+3000 trùng; thêm100 products/10000 prices. Bronze→Spark ETL/Gold/anomalies→SQLite→DashboardDB:66.132s,2209.05MiB RAM đỉnh;92000 review phục vụ. Synthetic,1 iteration; không gồm Kafka/MinIO/PostgreSQL/UI/startup. Lượt đầu đã phát hiện JSONL chỉ đọc1 dòng, có2 regression Spark sửa trước khi phép đo thành công. Artifact:`processing_fullpath_100k_current.json`.

Backfill đối chiếu fixture canonical trên cùng Silver hợp lệ với code Git baseline giữ nguyên4 bộ khóa mart và cho new replay hash bằng nhau. Giá/std/anomaly lịch sử thay đổi được liệt kê trong `processing_backfill_comparison.json`; không tự merge event ID cũ vì đổi định dạng thời gian UTC. Hướng dẫn comparison/rollback trong `../processing_analytics.md`.

Docker vẫn denied, sudo-n cần mật khẩu; các cổng dịch vụ mặc định/lần kiểm thử cũ đều chưa truy cập được. Không dùng kết quả local để nhận PostgreSQL/MinIO/Kafka đã đạt. Các gate đang chờ ở `processing_plan_remaining.md`.


## Nghiệm thu local sau review — 2026-10-29

**430 passed,38 deselected,0 failures/errors/skips**,155.11s; Ruff/compileall pass.526 cảnh báo dependency được giữ nguyên. Số liệu local trước đó trong tài liệu này là lịch sử. Bản hiện hành có fingerprint ở `remediation_validation.json` và XML `remediation_processing_local.xml`.

Review độc lập phát hiện8 Important/noCritical; đã sửa bằng regression rồi kiểm tra toàn bộ. Daily warehouse thay snapshot hoàn chỉnh, không giữ khóa cũ; Gold xuất đủ4mart kể cả rỗng, có manifest/khóa chung và từ chối export snapshot thiếu/chưa hoàn tất. Giá kiểm tra grammar trước parse, giữ decimal numeric và rounding half-up thống nhất. DQ không chấp nhận phép đo thiếu/mâu thuẫn, nguồn giá bị loại không tự nạp lại; nguồn canonical có producer manifest và gate theo mọi dataset được dùng. Source chưa biết giữ null. Products/DQ dùng chung read snapshot; các thống kê dùng cùng tập review đủ điều kiện. Lịch sử anomaly/DQ giữ nguyênrun gốc.

Load sau review:`processing_fullpath_100k_final.json`,100000review=92000Silver+5000quarantine+3000duplicates;71.1536s,2375.02MiB peak process-tree. Phạm vi SparkETL/Gold/anomalies→SQLite→DashboardDB, thêm100products/10000prices; loại Kafka/MinIO/PostgreSQL/UI/startup,synthetic1iteration. Artifact ghi rõ phiên bản đo; hai guard lỗi nguồn/input rỗng bổ sung sau đo được kiểm tra trong suite cuối. Backfill mới:`processing_backfill_comparison_final.json`, replay hash bằng nhau, input không đổi.

Mục tiêu vẫn chờ kiểm chứng dịch vụ thật: Docker socket hiện root:docker660, tài khoản chạy không thuộc nhómdocker; sudo-n cần mật khẩu, cổng dịch vụ chưa truy cập được. Đã yêu cầu người dùng khôi phục ACL trên `/var/run/docker.sock`, không hỏi lại quyền logic đã cấp. Không deploy/backfill vào production. Báo cáo review và ràng buộc: `processing_review_2026-10-29.md`; phần còn lại: `processing_plan_remaining.md`.


## Service acceptance update2026-10-29

Latest final local:434passed/48deselected,0failures/errors/skips,162.44s. Latest full integration:46passed/436deselected,0failures/errors/skips,141.48s;15direct actual-service cases plus31othermode/HTTP/errorcases. Ruff/compileall pass. Final source fingerprint92481e950144a2f37d27fca461e5265fcad89f18b01815a8756ec0ee93a9ae73 (199selectedsourcefiles). The older numbers above are historical.

Actual native Kafka/MinIO/Spark S3A/PostgreSQL100k load passed:110100transport messages,92000Silver/5000quarantine/3000duplicate reviews;92000served,4marts2000/100/80/500.151.6761s,3206.13MiBpeakPython/childtree;single synthetichostiteration,notDocker/browser/multihost/SLA. Measured source hash retained separately. Initial partial-consume bug, optional-empty-price verifier rejection and cold owned-bucket race have RED→GREEN regressions and fresh whole-suite verification.

Two canonical real-service replay runs, seven actual-PostgreSQL AppTest pages and two AirflowLocalExecutor runs/14separateprocess tasks have completed artifacts. Earlier Airflow/replay/UI execution predates the final boundary guards; saved scope is explicit. Native services/runtime restored after /tmp loss. Current evidence index:`processing_service_acceptance.json`; readable Vietnamese report:`processing_results_vi.md`.

Docker socket access remains denied; Compose config validation passed with its existing obsolete-version warning. Required Docker runtime acceptance is still outstanding, so the full goal remains active. No production backfill/event-ID rewrite or deployment was applied.

FinalDockercompletion2026-10-29T13:29:43.819211+00:00: requiredDockerblockingconditionresolved;434local/46Dockerintegration pass,0fail/error/skip,two-run Silver/Gold business hashes equal,7AppTestpages,remote-worker ownership verified. Source92481e950144a2f37d27fca461e5265fcad89f18b01815a8756ec0ee93a9ae73; no code edits in this Docker pass. Native100k measurement and all scope limitations remain explicitly separate.
