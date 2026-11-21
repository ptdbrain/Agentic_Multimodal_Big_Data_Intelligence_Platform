# Kế hoạch hoàn thiện và nghiệm thu SentinelAI

**Mục tiêu** — Bàn giao hệ thống Big Data có thể dựng lại, chạy tự động và chứng minh mọi kết luận trên dashboard bằng dữ liệu nguồn. Bàn giao Giai đoạn 1 trước. NLP, đa phương thức và trợ lý dữ liệu có mốc nghiệm thu riêng khi mở phạm vi AI.

**Cơ sở** — [Đề xuất môn học, mục 4–7](../../BAO_CAO_DE_XUAT_DU_AN.md), [thiết kế Phase 1](../../../plan.md), [định nghĩa hoàn thành và các pha sau](../../../plancheck.md), [các quyết định đã duyệt ngày 13/11](../specs/2026-11-13-non-ai-completion-design.md). Khi triển khai theo từng mục, dùng quy trình superpowers:executing-plans.

**Kiến trúc giữ lại** — Nguồn thật và giả lập có định danh riêng. Kafka → MinIO Bronze → Spark Silver/Gold → PostgreSQL → Streamlit. Elasticsearch phục vụ tìm kiếm; Airflow giữ lịch; Prometheus, Alertmanager và Grafana theo dõi vận hành. Bài viết dùng schema riêng. Phân tích tiền tệ theo đơn vị gốc.

## 1. Đối chiếu thiết kế với hiện trạng

Đối chiếu tại commit `9851863`, khoảng 12:03 ngày 21/11/2026, giờ Việt Nam. Số dưới đây là ảnh chụp tại thời điểm đọc. Kiểm tra lần này dùng mã, báo cáo đã lưu, trạng thái container, metadata Airflow, manifest thu thập và truy vấn PostgreSQL chỉ đọc. Kết quả kiểm thử trước đây giữ nguyên ngày đo.

| Yêu cầu ban đầu | Hiện trạng và phần cần đóng |
|---|---|
| Ba nhóm nguồn, dữ liệu thật có xuất xứ | Tiki, web Tinhte và giả lập đã có đường ghi/đọc riêng. Lúc kiểm tra, Tiki có 20 sản phẩm, 262 đánh giá; web có 335 bài từ một website; giả lập có 10 sản phẩm, 160 đánh giá. Lazada có bằng chứng capture cũ, chưa đủ cơ sở gọi là nguồn live định kỳ. Tiki và Tinhte đủ cho hai loại nguồn thật theo thiết kế hiện hành. |
| Dữ liệu công khai quy mô lớn, nạp CSV/JSON/JSONL/Parquet | Đã có loader, manifest và bài đo tải. Benchmark một triệu dùng dữ liệu sinh có nhãn. Manifest `public` hiện không ghi giấy phép. Chưa có bằng chứng đạt mục “≥1 triệu bản ghi crawl + dataset công khai” đã được đánh dấu xong trong `plancheck.md`. |
| Kafka, ETL, Silver/Gold, kho SQL và tìm kiếm | Đã có hợp đồng thông điệp, DLQ/replay, đối soát offset, quarantine, bốn mart ngày, phiên xuất bản SQL, chỉ mục ES và đổi alias. Cần bộ minh chứng cho cùng một phiên bản bàn giao, gồm trường hợp replay, lỗi giữa chừng và khôi phục. |
| Lakehouse, time travel và lineage | Có bảng phiên bản nội bộ, kiểm thử đồng thời, archive và truy vết bản ghi đã xác minh. Tài liệu còn mô tả Delta/Iceberg, ACID và tiết kiệm khoảng 75%. Cần chứng minh đường ghi chính dùng cơ chế nào, ranh giới nguyên tử ở đâu, bảng nào đọc lại được phiên bản cũ. |
| Bất thường và streaming | Có bộ phát hiện giá, bão đánh giá, sụt điểm, baseline quá khứ, sink SQL và webhook. Cấu hình streaming đang triển khai gắn với dataset giả lập; Tiki và web đang thu theo lịch. Đường review dùng watermark và gọi `window(event_time, window_duration)`. Chưa chứng minh đầy đủ cửa sổ trượt 5 phút và cập nhật **giá** từ Kafka đến cảnh báo SQL đúng như cách đề xuất có thể được hiểu. |
| BI nghiệp vụ | Đã có cả bảy trang gốc và các trang mở rộng về web, thống kê cơ bản, streaming, tìm kiếm, lineage. Có độ bao phủ mẫu, tập trung đánh giá và biến động giá theo tiền tệ. Trong dữ liệu Tiki hiện tại **0/20 sản phẩm có seller**. Trang Trends chủ yếu đọc đánh giá. Chưa có đầy đủ trải nghiệm so sánh giá/điểm theo nhà bán, thương hiệu và danh mục qua thời gian. |
| Đo 3V | Docker đã đo batch 10k/100k/1M và streaming 50/200/500 sự kiện/s, mỗi mức ba lượt. Ở 1M, trung vị 1.167,6 giây, 920k đánh giá được lưu mỗi lượt. Ba mức streaming có p95 đến SQL khoảng 32,5–33,4 giây. Mục tiêu >100.000 bản ghi/s trong đề xuất đã được thay bằng báo cáo hiệu năng đo thực theo thiết kế duyệt ngày 13/11. Giữ cả hai mốc trong hồ sơ. Benchmark phân biệt đầu vào, bản trùng và quarantine. Đã có API/HTML, dữ liệu cấu trúc và văn bản. Nhiều định dạng của một mẫu không tự tạo thêm nguồn độc lập. |
| Chạy theo lịch, cảnh báo, sao lưu | Sau reboot, dashboard đã phục hồi. Lúc kiểm tra, Prometheus **và** Alertmanager đều dừng với mã 127, Docker báo lỗi gắn tệp cấu hình trên ổ dữ liệu. Airflow ngày 21/11 ghi simulation và web thành công, Tiki đang chạy. **Web thu 50/50 bài, 689 URL chờ, 0 URL hết lượt thử; manifest vẫn là partial.** |
| Nghiệm thu độ bền, tái lập trên máy mới | Có bản sao đã xác minh, diễn tập restore, resume Spark, retry ES và sự cố worker. Báo cáo giờ cuối đọc được ghi lúc 22:15 ngày 20/11. Chưa chứng minh đủ 72 giờ, ≥99% mẫu giám sát, ba chu kỳ thật mỗi nguồn và restore sau chu kỳ mới. Có workflow CI và kết quả test local/Docker; cần kết quả CI đúng commit phát hành và diễn tập khởi tạo sạch. |
| AI và triển khai sản xuất | Giai đoạn 1 của đề xuất dùng thống kê. Các pha NLP, ảnh và RAG nằm trong roadmap; chưa thấy pipeline mô hình đã đánh giá và triển khai. HA vật lý nhiều máy, bản sao ngoài máy và triển khai công khai cần hạ tầng và tiêu chí riêng. |

Bằng chứng dùng để đối chiếu gồm [nguồn và 3V](../../reports/three_v_assessment_2026-11-04.md), [batch Docker](../../reports/nonai_docker_batch_matrix_2026-11-19.json), [streaming tới SQL](../../reports/nonai_docker_stream_matrix_2026-11-19.json), [lineage có tải](../../reports/nonai_managed_lineage_load_2026-11-20.json), [kiểm thử toàn hệ thống gần nhất](../../reports/nonai_validation_2026-11-20.json), [khôi phục giám sát](../../reports/nonai_monitoring_recovery_2026-11-20.json) và [phân tích đã triển khai](../../reports/analytics_dashboard_2026-11-21.md). Báo cáo BI lúc 11:58 ghi 302 bài; sau lượt web vừa xử lý, SQL có 335 bài. Một lượt thu 50 không đồng nghĩa thêm 50 bài mới.

## 2. Kế hoạch Giai đoạn 1

Thứ tự đề xuất là A → B; bắt đầu quan sát D ngay khi hệ thống đủ điều kiện. C, E và F có thể làm trong thời gian chờ lịch thật. G là bước bàn giao. Ước lượng là ngày công kỹ thuật cho một người; thời gian chờ nguồn, phê duyệt quyền sử dụng dữ liệu và 72 giờ quan sát tính riêng.

### A · Khôi phục giám sát bền qua reboot — P0, 1–2 ngày

- [ ] Lưu lỗi mount, các volume, mốc scrape cuối và trạng thái lịch trước can thiệp. Kiểm tra thứ tự mount ổ dữ liệu, Docker và user service. Đưa cấu hình giám sát vào vị trí có sẵn khi boot hoặc ràng buộc startup với mount thực tế. Kiểm tra loại tệp, khả năng đọc registry và quyền socket trước khi khởi động job. Điểm sửa chính là `docker-compose.yml`, cấu hình khởi động và runbook vận hành.
- [ ] Thử tình huống ổ cấu hình đến muộn trên môi trường cô lập. Khi tệp thiếu, hệ thống phải báo chính xác nguyên nhân. Khi ổ sẵn sàng, tự khôi phục trên volume cũ. Xác nhận cả bốn target qua Grafana và gửi được một cảnh báo có cặp firing/resolved đến receiver.
- [ ] Nghiệm thu bằng mẫu lịch sử trước/sau restart, báo cáo giờ mới và bằng chứng tác vụ backup/maintenance chạy theo lịch. Diễn tập reboot máy đang phục vụ cần cửa sổ bảo trì đã thống nhất.

### B · Đóng hợp đồng thu thập và nguồn dữ liệu lịch sử — P0, 2–4 ngày

- [ ] Trong `ingestion/web_capture.py`, `orchestration/source_jobs.py` và bộ xác minh, báo riêng số yêu cầu, số thu, số mới/cập nhật, backlog, lỗi hết retry và trạng thái xuất bản. Giữ nguyên tiêu chí hiện hành cho đến khi chủ dự án duyệt thay đổi.
- [ ] Nếu cần **bao phủ hết**, đóng tập URL đầu kỳ, xử lý hết tập đó và chuyển URL mới phát hiện sang kỳ kế tiếp với biên nhận riêng. Nếu mục tiêu là **50 bài/ngày**, nghiệm thu quota và sức khỏe backlog bằng hai chỉ tiêu độc lập. Kiểm thử qua restart, URL trùng, lỗi HTTP và hàng đợi tăng. Không sửa ngược kết quả `partial` cũ.
- [ ] Chọn dataset thương mại lịch sử có quyền sử dụng rõ ràng, giữ URL/phiên bản, giấy phép, checksum, khóa tự nhiên và quy tắc loại PII. Nạp vào namespace lịch sử riêng qua loader hiện có. Chạy lại cùng input phải cho cùng số liệu và fingerprint. Nếu giữ yêu cầu một triệu bản ghi quan sát thật, báo số bản ghi quan sát phân biệt và số còn lại từng tầng cho tới khi đủ. Nếu thiếu quyền hoặc dung lượng, ghi mục này chưa đạt.
- [ ] Kiểm tra chính sách robots, giới hạn tốc độ, retry, bí mật băm định danh và khả năng lộ PII qua payload lỗi, log, CSV. `ingestion/privacy.py` có salt mặc định cố định; kiểm tra caller production, quy định xử lý ID cũ trước khi đổi. Dùng cùng bộ input để xác minh bốn định dạng cho kết quả canonical tương đương.

### C · Hoàn thiện phân tích nghiệp vụ trên dữ liệu có thật — P1, 3–5 ngày

- [ ] Lấy mẫu được phép từ Tiki, đối chiếu seller ID/tên trong payload với schema sản phẩm và quan sát giá. Bổ sung ánh xạ ở crawler → Silver → PostgreSQL khi trường thật sự tồn tại. Dashboard công bố tỷ lệ có seller và nhóm chưa biết. Nếu nguồn không cung cấp, ghi giới hạn và giữ trống.
- [ ] Mở rộng `analytics/basic.py`, `dashboard/pages/3_Products.py` và `dashboard/pages/5_Trends.py`. Cho phép phân nhóm nhà bán, thương hiệu, danh mục; tách tiền tệ, so điểm theo số lượt chấm, hiển thị số sản phẩm và số ngày thực sự quan sát. Với giá nhóm, báo thay đổi thành phần mẫu hoặc dùng tập sản phẩm ghép cặp giữa hai mốc. Không gán thương hiệu hiện tại cho lịch sử khi dữ liệu không chứng minh.
- [ ] Đối soát năm loại cảnh báo trong mục 7 của đề xuất, gồm điều kiện kích hoạt, mức độ, baseline, cỡ mẫu, thời điểm và bằng chứng. Dùng tình huống có nhãn và tình huống âm để kiểm thử. Nếu cần kết luận “khuyến mãi ảo”, phải có giá niêm yết/giá bán và lịch sử của cùng offer; hiện chỉ nên hiển thị tín hiệu biến động.
- [ ] Giảm việc tải toàn bộ bảng reviews/price history vào Pandas cho số tổng hợp. Đẩy lọc, nhóm và phân trang xuống PostgreSQL, giữ snapshot nhất quán và cùng điều kiện khi xuất CSV. Đối soát SQL độc lập cho mẫu nhỏ, xếp hạng đồng điểm và dữ liệu thiếu; thử tải 1M, ghi p50/p95 truy vấn và thời gian render trình duyệt riêng.

### D · Nghiệm thu theo lịch và phục hồi sau khi có dữ liệu mới — P0, ít nhất 72 giờ thực

- [ ] Giữ cả cửa sổ chuyển đổi và cửa sổ sau khôi phục. Sau sự cố ngày 21/11, tính lại coverage từ Prometheus và ghi rõ khoảng mất mẫu. Nếu cần một cửa sổ 72 giờ mới, công bố mốc bắt đầu trước khi đo và lưu kết quả các cửa sổ trước.
- [ ] Dùng `scripts/verify_source_acceptance.py` đối chiếu tối thiểu ba run theo lịch cho **từng** nguồn với task capture/process, hash payload, biên nhận xuất bản, SQL và trạng thái coverage. Tiêu chí vận hành gồm ≥99% mẫu giám sát theo cấu hình và không có run lịch lỗi hoặc thành công thiếu bằng chứng. Nghiệm thu toàn phần còn cần đạt hợp đồng nguồn ở B.
- [ ] Sau chu kỳ lịch mới, chạy backup có fence bằng `scripts/backup_docker_owner.py`; khôi phục vào database, bucket, checkpoint và metadata riêng. So fingerprint SQL, checksum object, offset từng partition, trạng thái cả hai sink, outbox và lịch DAG. Bằng chứng phải cho thấy dữ liệu mới xuất hiện ở đích và chỉ một scheduler được phép ghi.
- [ ] Gắn biên nhận cho ba lần bảo trì: backup, dọn topic đủ điều kiện và restore. Nếu máy nghỉ hoặc job lỡ lịch, ghi thời gian thực và lý do. Không tính chạy tay vào ba chu kỳ.

### E · Đóng khoảng cách lưu trữ và streaming — P1, 2–4 ngày

- [ ] Dùng `storage/lakehouse.py`, metadata store và đường xuất Silver/Gold thật để chứng minh đọc phiên bản N sau khi xuất N+1, replay, đụng độ writer và phục hồi giữa commit. Lập bảng theo từng loại dữ liệu về nơi giữ phiên bản, thông tin schema và thời hạn giữ. Chỉ tuyên bố tương thích Delta/Iceberg nếu trình đọc chuẩn đọc được. Nếu hợp đồng bắt buộc định dạng mở, làm pilot trên namespace riêng trước migration.
- [ ] Đối chiếu cấu hình `spark/streaming/spark_streaming_job.py` với yêu cầu 1 và 5 phút. Kiểm thử event sát biên, đến muộn, trùng và resume. Nếu giữ yêu cầu cửa sổ trượt 5 phút, khai báo cả độ dài lẫn bước trượt, dùng generation/checkpoint mới và đối chiếu kết quả SQL bằng phép tính độc lập. Nếu cần cảnh báo trên nguồn thật ngay khi nhận sự kiện, thêm binding cho nguồn thật, checkpoint và generation riêng, đo riêng độ trễ từ lúc sàn phát sinh đến lúc thu và từ Kafka đến SQL. Xác định rõ giá chạy batch hay cần thêm luồng `raw.prices` với bằng chứng end-to-end.
- [ ] Tái hiện một cảnh báo từ dashboard về mart, manifest và Bronze có checksum. Điểm DQ phải có tử số, mẫu số, số cách ly và trạng thái thiếu dữ liệu. Đo nén trên cùng input nếu báo cáo giữ con số 75%.

### F · Dọn các tuyên bố cũ và kiểm tra cài đặt sạch — P1, 1–2 ngày

- [ ] Cập nhật `README.md`, `docs/benchmark.md`, `docs/architecture.md`, `docs/data_sources.md` và `plancheck.md` bằng bảng yêu cầu gốc, quyết định thay thế và đường dẫn bằng chứng. Giữ nguyên tiền tệ gốc; ghi nhận DQ đo được, lịch theo từng nguồn, tốc độ phát thực đạt và ranh giới đo Kafka → SQL → trình duyệt.
- [ ] Chạy quy trình khởi tạo trên môi trường sạch, project Compose và volume mới, với bộ cấu hình mẫu không chứa bí mật. Ghi lệnh chính xác thay cho giả định `make up && make demo` đủ dùng trong mọi môi trường. Chứng minh từ khởi tạo tới dữ liệu và trang BI mà không cần tệp riêng có sẵn của máy phát triển.
- [ ] Chạy `make lint`, `make test-unit`, `make test-spark` và `make test-integration` với đúng dịch vụ, cùng gate scheduler thật và AppTest. Lưu mã commit, digest image, phiên bản phụ thuộc, JUnit và kết quả CI của đúng bản phát hành. Kiểm thử fixture chạy trong namespace cô lập.

### G · Bàn giao và quyết định nghiệm thu — 1 ngày

- [ ] Demo một review thật và một bài web từ thu thập đến dashboard, một mẫu giả lập có nhãn, một bản bị cách ly, một cảnh báo, một lần replay và một lần restore. Đính kèm hướng dẫn vận hành, từ điển dữ liệu, truy vấn đối soát và báo cáo 3V.
- [ ] Chốt riêng bốn kết quả: chức năng, chất lượng dữ liệu nguồn, hiệu năng đã đo và độ bền vận hành. Chủ dự án duyệt mọi yêu cầu được thay thế. Các mục phụ thuộc dữ liệu hoặc chưa đủ thời gian đo giữ trạng thái mở.

## 3. Lộ trình hoàn thiện các pha AI và sản xuất

Các pha sau nằm trong thiết kế toàn dự án. Chỉ bắt đầu triển khai khi chủ dự án mở phạm vi tương ứng. Các mốc thời gian dưới đây là ước lượng sau khi có dữ liệu, nhãn và tài nguyên.

| Pha | Việc thực hiện | Điều kiện nghiệm thu đề xuất |
|---|---|---|
| 2 · NLP tiếng Việt, 2–4 tuần | Chuẩn hóa văn bản, gán nhãn cảm xúc và khía cạnh, tạo tập đánh giá 500–1.000 mẫu theo roadmap. Chia tập theo sản phẩm/thời gian, giữ riêng bản trùng. So baseline từ điển với PhoBERT/ViSoBERT. Bổ sung từ khóa/chủ đề và thực thể sản phẩm/thương hiệu sau khi baseline ổn định. Lưu phiên bản mô hình, nhãn, độ tin cậy và kết quả theo review. | Chốt ngưỡng trước khi huấn luyện. Báo macro-F1, kết quả từng khía cạnh, số mẫu và lỗi điển hình trên tập giữ lại; so với baseline. Dashboard lọc được theo thời gian, khía cạnh và phiên bản. Sao đánh giá không thay thế nhãn văn bản. |
| 3 · Văn bản và hình ảnh, 2–4 tuần | Kiểm kê quyền dùng ảnh và liên kết ảnh–sản phẩm. Lưu ảnh/metadata trong MinIO, tạo embedding có phiên bản, index Qdrant. Thêm tìm ảnh tương tự, truy vấn chữ–ảnh và phát hiện ảnh trùng. VLM mô tả ảnh là mở rộng tùy chọn. | Có tập truy vấn và kết quả liên quan do người đánh giá; công bố Recall@K, chất lượng phát hiện trùng và p95. Kiểm thử ảnh hỏng, mất quyền sử dụng, xóa khỏi chỉ mục và xây lại index. |
| 4 · Trợ lý RAG/Text-to-SQL, 2–4 tuần | Lập bộ câu hỏi BI có đáp án SQL chuẩn. Truy xuất review/bài và các mart theo đúng dataset. Dùng vai trò PostgreSQL chỉ đọc, danh sách bảng được phép, giới hạn hàng và timeout; mỗi câu trả lời dẫn nguồn, thời điểm và phiên bản. | Đối chiếu số với SQL chuẩn; đo độ đúng, tỷ lệ câu có căn cứ, p95. Thử câu ngoài phạm vi, nhầm tiền tệ, dữ liệu thiếu và chỉ dẫn lẫn trong nội dung crawl. Chứng minh tài khoản ứng dụng không ghi được dữ liệu. |
| 5 · Hạ tầng sản xuất, tùy phạm vi | Nếu triển khai ngoài máy đồ án, chốt người dùng, RPO/RTO, công suất và ngân sách; bổ sung xác thực, phân quyền, secrets, mã hóa, backup ngoài máy và hạ tầng nhiều máy. Kubernetes/Helm là một lựa chọn sau khi chốt nhu cầu. | Diễn tập mất máy thật và phục hồi từ bản sao ngoài máy, đo RPO/RTO, thử quyền truy cập và rollback phát hành. Kết quả nhiều container trên cùng một host chỉ chứng minh phạm vi một host. |

**Các quyết định cần chốt** — (1) web cần vét hết tập URL hay đạt quota có quản lý backlog; (2) mục một triệu quan sát thật giữ trong nghiệm thu môn học hay chỉ giữ bài đo công suất; (3) yêu cầu Delta chuẩn và luồng giá/cửa sổ trượt có bắt buộc hay chấp nhận kiến trúc tương đương đã chứng minh. Quyết định về AI và triển khai nhiều máy có thể để sau Giai đoạn 1.

## Quyết định ngày 21/11

Đủ 50 bài web phân biệt mỗi ngày và theo dõi backlog riêng. Nghiệm thu Giai đoạn 1 trước khi triển khai AI. [Tiến độ và bằng chứng](../../reports/project_completion_progress_2026-11-21.md)
