> Checkpoint lịch sử03/11, trước review/sửa cuối. Tiến độ hiện hành: [báo cáo hoàn tất04/11](source_integrity_completion_2026-11-04.md). Các số đo dưới đây không phải cohort Volume cuối.

# Rà soát và hoàn thiện tính đúng của hệ thống — 03/11/2026

Kế hoạch đang ở bước kiểm chứng cuối và review độc lập. Báo cáo này thay cho các checkpoint trong ngày; bằng chứng JSON/XML cũ được giữ nguyên, không cộng số test chồng lấp.

## Phạm vi và kiến trúc

Đã rà soát ingestion file/API/crawler, generator, schema/PII, Kafka/DLQ, lake/identity, Spark batch/streaming, analytics/Gold, SQL, dashboard, orchestration và monitoring. Các hồi quy tập trung vào tác động nghiệp vụ, đối soát nội dung và lỗi vận hành; fixture có kiểm soát không được tính là bằng chứng nguồn thật.

Luồng commerce: nguồn → schema/PII → Kafka → MinIO Bronze → Spark Silver/DQ/dedup/quarantine → Gold/anomaly → PostgreSQL → DashboardDB/Streamlit. Bài viết dùng schema và bảng riêng, xử lý pandas có giới hạn; không biến thành review. Local/test dùng backend riêng theo khai báo; integration không tự chuyển sang giả lập khi dịch vụ lỗi.

## Các lỗi đã tái hiện và sửa

| Nhóm | Lỗi và tác động | Hồi quy chính |
|---|---|---|
| HTTP/API/HTML | Tự điền rating/count/brand/time/verified/title, fallback Tiki sinh dữ liệu giả; flag API vẫn gọi scraper | Tiki/HTML/HTTP evidence/simulation tests; fallback giả đã xóa, trường thiếu giữ unknown hoặc quarantine |
| Nguồn gốc | Mất source metadata và raw evidence khi chuyển tầng; giả lập mang nhãn nguồn thật | provenance contracts, HTTP SHA256, inheritance, Silver/SQL/source scope tests |
| JSON/CSV | NaN thêm vào object JSON, mã 0001 mất số 0, lỗi parser/encoding bị che | source delivery/delimited tests; giữ object và integer lớn |
| Kafka/Bronze | Đủ số lượng nhưng nội dung có thể bị thay bằng bản cũ; DLQ không được tính chất lượng | So multiset SHA256 nguồn với Bronze + DLQ; gate chất lượng trước serving |
| Replay DLQ | Luôn offline nhưng báo đã replay; nhận nhầm price/product, bỏ Parquet và ngày lỗi | dry-run tách eligible/replayed; real chỉ đếm DELIVERED ACK; unit và Kafka/MinIO integration |
| Spark batch | price_raw tồn tại ở một nguồn làm mất giá nguồn khác; trộn class giữa các thế hệ | Per-row coalesce, source class guard; partial raw/source separation tests |
| Tiền tệ | Gộp USD/VND không FX; thiếu đơn vị bị gán VND kể cả SQL default; cảnh báo USD ghi VND | Currency guards pandas/Spark/SQL; unknown giữ NULL, namespace đơn vị riêng |
| SQL/upsert | iterrows làm tròn ID 9007199254740993; mỗi row một RPC; thiếu count thành 0 | Tuple iteration + execute_batch1000; large-ID/null/conflict/order/rollback tests trên PG thật |
| Thời gian bài viết | Mất offset/precision; ngày không giờ thành timestamp chính xác | raw clocks/UTC/precision; source offset và metadata qua SQL |
| Native streaming | Bỏ provenance, tạo body/observation time từ ingestion, dedup/window trộn nguồn | 9 Spark regressions, source-checked sink cả append/update, Kafka/restart thật |
| Quarantine streaming | Hai payload lỗi khác nhau chung ID/reason bị gộp mất bằng chứng | Hash nội dung, retry cùng payload idempotent, giữ các phiên bản lỗi khác nhau |
| Local streaming | Epoch0 mất, bool/NaN/timezone sai, rating NaN gây hỏng mean, CLI bỏ window/watermark/seed | Window/input/CLI tests; giữ ranh giới simulator rõ ràng |
| Checkpoint local | s3a sai bucket, JSON hỏng âm thầm reset, ghi nửa file, áp dụng state từng phần | v3 typed/config/source checks, atomic replace, staged restore, rollback khi save lỗi; MinIO restart thật |
| Generator | Config rỗng; giá USD nhỏ làm tròn 0, đơn vị bị đổi VND | Config/seed/input hash, unit-aware price, từ chối đổi đơn vị không FX |
| Dashboard | Mặc định lẫn dữ liệu chưa kiểm chứng/giả lập, trạng thái hardcode | UI mặc định real, selector rõ, query scope, Bronze sample có lọc |
| Monitoring | Scrape DB/broker như HTTP; thiếu DQ thành số đo; service status giả | HTTP exporter thật, protocol probes, unknown NaN, failures503/up0 |

Không có ngưỡng coverage số học được cấu hình. Các test hồi quy kiểm tra đầu ra, identity, lỗi, rollback và transport thật; test HTTP fixture chỉ xác minh parser, không chứng nhận live crawl.

## Nguồn thật và bằng chứng hiện hành

- **Tiki:** hai lần HTTP mới trên parser v4, mỗi lần 20 sản phẩm, 20 giá, 65 review; mọi bản ghi có hash/ref HTTP kiểm chứng. Xem [bản thu Tiki](tiki_current_http_v4_capture_2026-11-03.json).
- **Lazada:** hai bản thu HTTP thật ngày02/11, mỗi bản 20 sản phẩm/20 giá, không có review. Ngày03/11 trả challenge. Replay giữ hash canonical capture; không bịa raw HTTP/parser version chưa lưu.
- **Tinhte:** hai lượt thu trực tiếp trước đó, mỗi lượt50 bài; lần kiểm chứng hiện tại reparse 100 HTML gốc, so hash/article ID, giữ collection clock/run gốc.
- **Luồng cuối:** [native verified source routes](source_native_verified_source_routes_2026-11-03.json) qua hai lượt commerce40 sản phẩm/65 review/40 dòng Gold giá. Silver giá lượt2 có80 quan sát. Hai lượt bài viết50 bản/lượt qua Kafka/MinIO/pandas/PG. Đây là replay bản thu thật, không phải crawl mới.
- **Giả lập:** ma trận Volume dùng namespace riêng từng lượt, nhãn simulation/generated, input xác định và scope real thấy0 review giả lập. Không tính các lần lặp thành quan sát thị trường độc lập.

Bản chạy source đầu có lỗi cleanup trong private harness sau khi business assertions đã qua. Giữ report và log cũ, sửa harness rồi chạy lại report mới nêu trên với exit0; không xóa lỗi lịch sử.

## Hiệu năng đã đo

- Cache validator: median CPU 0,10375702 → 0,00892895 giây trên300 bản ghi giả lập ×3, hash kết quả/lỗi giữ nguyên.
- PostgreSQL upsert10.000 dòng ×3: median wall1,459773561 → 0,330561076 giây; sáu hash nghiệp vụ giống nhau. Đây là microbenchmark, không suy ra mức tăng tốc toàn pipeline.
- Volume10k/100k/1M ×3: đủ9 lượt toàn tuyến, median36,2946 /115,0115 /913,0989 giây. Mỗi1M input có920k Silver,50k quarantine,30k duplicate.
- Kafka50/200/500 msg/s ×120s ×3: đủ9 lượt workload lỗi4% phân bố đều, actual49,6–49,8 /197,0–197,3 /479,2–479,5. Tất cả đối soát đủ và end lag0. Consumer batching tạo tail latency lớn; không có chứng nhận SLA500msg/s.

Xem [Volume summary](source_native_volume_summary_2026-11-03.json), [Velocity summary](source_native_velocity_summary_2026-11-03.json) và [đánh giá3V](three_v_assessment_2026-11-03.md). Cohort cũ có run1M bị ngắt và workload Velocity dồn lỗi ở cuối được giữ làm lịch sử, loại khỏi tổng hợp cuối.

## Vận hành và giới hạn kiểm chứng

Dịch vụ chạy thật native: Kafka3.7, MinIO, PostgreSQL16.15, Elasticsearch8.13; Spark3.5 local[2], Python3.11/Java17. Docker đã truy cập lại được nhưng hiện không chạy container; không gọi native evidence là Docker evidence.

Dashboard AppTest thực thi9 trang với PG/MinIO thật, kiểm tra scope real và simulation; oracle40 sản phẩm/65 review/50 bài viết. Server HTTP khỏe tại127.0.0.1:8502. Công cụ trình duyệt trả “browser access host unavailable”: **chưa xác nhận layout/screenshot bằng browser**. AppTest không thay thế bằng chứng render.

Exporter dependency kiểm tra giao thức thật; Prometheus/Grafana cấu hình đã sửa nhưng chưa chứng nhận deployment/rendering. Checkpoint v3 và Spark source_group state cần namespace mới; hướng dẫn migration trong [runbook](../runbook.md). Một namespace commerce chỉ hỗ trợ một currency đã biết; muốn đa đơn vị cần tách namespace hoặc thiết kế FX riêng.

Gate local hiện có **603 passed, 57 deselected, 0 failed, 0 skipped** (203,77s); native integration **56 passed, 605 deselected** (142,75s); live Tiki **1 passed**; Airflow LocalExecutor với PG metadata riêng **1 passed** (43,22s). Các selector này tách nhau; không cộng với các checkpoint lịch sử. Gate integration có cả kiểm tra dịch vụ thật và compatibility local được khai báo rõ trong test.

Test local cũ từng kỳ vọng public/unknown và generated vào cùng Silver đã được tách thành hai fixture namespace, giữ exact counts/privacy/classification; guard sản phẩm không bị nới. Test mới dùng classifier cho legacy thiếu source_group thay vì bịa cột để test qua. Hai XML thất bại trung gian được giữ nguyên.

Một reviewer độc lập đang rà toàn bộ diff, gồm source/test chưa tracked; chưa chốt nghiệm thu cho đến khi các phát hiện cần sửa được kiểm chứng. Không sửa AGENTS.md, không commit/push trong kế hoạch này.
