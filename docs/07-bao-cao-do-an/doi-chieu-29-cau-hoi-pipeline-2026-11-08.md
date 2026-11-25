# Đối chiếu đồ án với 29 câu hỏi về data pipeline

**Sau hoàn thiện ngày 08/11:** [134 test đạt, backfill dữ liệu thật, telemetry và cảnh báo nội bộ](hoan-thien-pipeline-2026-11-08.md). Các hàng schema/backfill/quan sát dưới đây đã cập nhật theo bằng chứng mới. Power BI do sinh viên tự làm; lịch ML tự động và các công nghệ ngoài phạm vi không được tự đánh dấu đã triển khai.

Ngày đánh giá: 08/11/2026. Căn cứ mục tiêu sinh viên xác nhận: xây dựng một luồng dữ liệu end-to-end, có các cơ chế cần thiết của pipeline. Mô hình dự báo là một thành phần tiêu thụ dữ liệu để kiểm chứng MLOps; tối đa hóa độ chính xác dự báo không phải mục tiêu chính của đồ án.

Đợt này đọc mã, cấu hình và bằng chứng đã lưu; không chạy lại stack hoặc phát thêm dữ liệu. Các số đo Docker dưới đây thuộc lần kiểm thử 04/11, không phải trạng thái dịch vụ được đo lại ngày 08/11.

## Kết luận theo đúng mục tiêu

Đồ án đã có bằng chứng cho luồng dữ liệu cốt lõi từ nguồn mô phỏng qua Kafka, MinIO, làm sạch/kiểm định, PostgreSQL đến nguồn dữ liệu hạ nguồn và API. Đã kiểm tra phát lại không nhân đôi fact, cách ly dữ liệu lỗi, xử lý bản cập nhật đến muộn, khôi phục sau gián đoạn dịch vụ và theo dõi sức khỏe. Nhánh MLOps đã có bằng chứng tạo đặc trưng, huấn luyện, đăng ký phiên bản, nạp lại và quay lui trong môi trường thử nghiệm cô lập.

Chưa đủ căn cứ mô tả toàn bộ hệ thống là một vòng tự động liên tục từ mọi sự kiện mới đến huấn luyện lại và Power BI Desktop tự cập nhật. Phải công bố ranh giới của từng nhánh. Sai số dự báo cao giới hạn ứng dụng nhập hàng; nó không tự phủ định việc pipeline chạy đúng và có thể truy vết.

Bộ tài liệu được gửi là 29 câu hỏi kiến thức, gồm cả công nghệ và bài toán nằm ngoài đồ án. Không coi mỗi ví dụ Spark, Debezium, Airflow, dbt, Lakehouse, clickstream hoặc RAG trong tài liệu là một yêu cầu phải triển khai thêm.

## Những bằng chứng quyết định

| Kiểm tra | Bằng chứng đã lưu | Điều được chứng minh |
|---|---|---|
| Dữ liệu bình thường và lỗi | 12 đơn hợp lệ → 12 raw → 12 fact; lô 13 bản tin → 12 fact và 1 cách ly | Có đường nhận, lưu, kiểm định và phân luồng lỗi |
| Phát lại cùng lô | ACK/raw tăng từ 13 lên 26; fact giữ 12 | Idempotence tại đích nghiệp vụ trong ca thử; raw vẫn có thể chứa bản phát lại |
| Phiên bản đến muộn | Lô AUTO giữ 2 dòng đơn, tổng quantity 7; snapshot cũ đến sau không ghi đè stock mới 50 | Có quy tắc cập nhật trạng thái và đối soát đầu ra |
| Tải hỗn hợp 10 phút | 600 Kafka ACK; đọc lại đủ 600 Kafka, 600 raw trong 56 file; 56 file checkpoint; 600 fact; không thiếu/thừa/trùng | Luồng dữ liệu gia tăng chạy xuyên suốt tại mức tải đã thử |
| Nhánh phục vụ | 2.400/2.400 HTTP đúng tiêu chí; API đọc model Registry và lịch sử warehouse; P95 HTTP 42,201 ms | Đầu ra ứng dụng hoạt động trong phép thử; không phải P95 độ trễ sự kiện đầu cuối |
| Tái huấn luyện và đổi phiên bản | Trigger ép tay, Registry/API cô lập; tạo 42.768 dòng đặc trưng; huấn luyện, đăng ký, reload và rollback | Đường chức năng MLOps đã được kiểm tra; chưa chứng minh lịch tự động hoặc lô streaming mới đi qua toàn nhánh ML |
| Nguồn BI | 7 query đúng schema, tổng summary khớp fact | Nguồn PostgreSQL phục vụ BI có đối soát; Desktop Apply/Refresh chưa nghiệm thu |

Nguồn chính: [báo cáo E2E ngày 04/11](../../output/e2e-2026-11-04/Bao-cao-kiem-thu-end-to-end.md), [đọc lại Kafka/MinIO/checkpoint](../../output/e2e-2026-11-04/soak-integrity.json), [đối soát nguồn BI](../../output/e2e-2026-11-04/check_powerbi_import.json).

## Ranh giới luồng đang có

```text
Nhánh dữ liệu sự kiện
Simulator đơn hàng → Producer → Kafka → Consumer → MinIO Parquet
    → Chuẩn hóa/kiểm định/cách ly/chống trùng → PostgreSQL → SQL phục vụ BI
Simulator tồn kho → Kafka → Consumer → PostgreSQL → API tồn kho

Nhánh MLOps đã kiểm thử
CSV lịch sử → dữ liệu lịch sử trong warehouse → đặc trưng → huấn luyện
    → MLflow Registry → reload FastAPI → đầu ra dự báo

Quan sát: trạng thái worker + FastAPI /metrics → Prometheus → Grafana
```

Manifest hiện hành giới hạn mô hình vào nguồn `public_anonymized_historical_csv` và hai fingerprint dữ liệu. Worker tự chạy ingestion và ETL; không tự chạy training, registration hoặc drift/retraining theo lịch. Power BI phải refresh để nhận dữ liệu mới. Vì vậy, không dùng sơ đồ một đường thẳng để suy ra mọi lô simulator tự động tạo mô hình mới hoặc cập nhật mọi visual.

## Đối chiếu từng câu hỏi

Các mức đánh giá dưới đây nói về phần triển khai/bằng chứng của đồ án, không chấm khả năng trả lời miệng của sinh viên. Các câu ngoài phạm vi vẫn có thể xuất hiện dưới dạng hỏi lý thuyết khi bảo vệ.

| Câu | Chủ đề | Đồ án trả lời bằng phần nào | Kết luận và giới hạn |
|---:|---|---|---|
| 1 | Thành phần data pipeline | Nguồn simulator/CSV; Kafka ingestion; MinIO; ETL/DQ; PostgreSQL; BI/ML/API; giám sát | Đã có các thành phần cốt lõi và bằng chứng E2E. Phân biệt hai nguồn và hai nhánh ở trên. |
| 2 | ETL và ELT | Lưu raw vào MinIO trước; ETL Python chuyển đổi/kiểm định trước khi nạp PostgreSQL; SQL view tổng hợp sau nạp | Giải thích được bằng kiến trúc thực tế. Không gọi việc lưu raw là đã triển khai một ELT warehouse thuần túy. |
| 3 | Idempotence | Khóa dòng đơn, upsert, phiên bản nguồn, transaction; replay giữ nguyên số fact | Đã kiểm chứng tại sink nghiệp vụ trong ca phát lại. Không tuyên bố mọi tầng, đặc biệt raw/quarantine, chỉ có một bản ghi vật lý. |
| 4 | Warehouse, Lake, Lakehouse | MinIO lưu object/Parquet; PostgreSQL quản lý bảng phân tích | Đã áp dụng Lake + Warehouse. Không có bằng chứng triển khai Delta/Iceberg hoặc giao dịch bảng Lakehouse. |
| 5 | Medallion | Có thể ánh xạ raw MinIO → chuẩn hóa/kiểm định → fact/dimension/view | Tương đương về vai trò các tầng. Đây không phải ba bộ bảng Bronze/Silver/Gold trong một Lakehouse đã triển khai. |
| 6 | Parquet | Consumer ghi Parquet; ETL đọc qua PyArrow/Pandas | Đã áp dụng định dạng. Chưa có benchmark riêng chứng minh mức tiết kiệm I/O/nén hoặc predicate pushdown của truy vấn cụ thể. |
| 7 | Partition key | Object path phân theo platform/year/month/day; ETL chọn ngày và danh sách object | Đã áp dụng partition theo nguồn/ngày. Cần giải thích business timezone, quy mô tệp và khả năng tăng số object. |
| 8 | SCD | Upsert dimension sản phẩm/shop theo khóa hiện tại | Có cập nhật theo kiểu Type 1 ở các dimension liên quan. Chưa có SCD Type 2 theo effective_from/effective_to; chỉ cần bổ sung nếu bài toán yêu cầu truy vấn lịch sử thuộc tính. |
| 9 | Star schema và grain | Fact dòng đơn, các dimension và view `bi`; tồn kho/forecast có grain riêng | Đã áp dụng. Phải nói rõ dòng hàng khác cấp đơn và phân biệt snapshot tồn kho hiện tại với lịch sử. |
| 10 | Schema evolution | Hợp đồng orders v1; chấp nhận trường mới và 1.x; cách ly thiếu trường, sai kiểu hoặc major lạ | Đã triển khai và kiểm thử cả trong bộ hồi quy lẫn lô Kafka thật. Không cần Schema Registry để đạt phạm vi hợp đồng hiện tại. |
| 11 | CDC | Nguồn hiện tại phát sự kiện qua producer, không đọc WAL/binlog | Ngoài phần triển khai hiện tại. Không gọi producer là CDC; không bắt buộc thêm Debezium khi nguồn là sự kiện. |
| 12 | Topic/partition/offset/consumer group | Topic đơn/tồn, message key theo order/SKU, consumer group, manual offset commit | Đã áp dụng và có thể giải thích theo cấu hình/mã. Thứ tự chỉ được dựa vào phạm vi partition, không toàn topic. |
| 13 | Delivery semantics | Commit Kafka sau lưu MinIO/commit DB; replay; upsert ở sink | Thiết kế at-least-once và sink có chống trùng đã kiểm thử. Không tuyên bố exactly-once toàn hệ thống hoặc chống được mất volume chỉ từ ca restart. |
| 14 | Event time/processing time/watermark | Thời gian sự kiện, source_updated_at, loaded_at và chặn cập nhật cũ | Có xử lý thời gian và sự kiện đến muộn. Chưa có watermark đóng streaming window; version guard không phải watermark. |
| 15 | Tumbling/sliding/session window | Có gom micro-batch theo lượng/thời gian và rolling feature theo ngày | Chưa triển khai engine event-time window. Micro-batch và rolling feature không tự chứng minh tumbling/sliding/session window của xử lý luồng. Không bắt buộc có session window cho đơn hàng. |
| 16 | Spark shuffle | Luồng hiện tại dùng Python/Pandas/SQL, không có Spark executor/stage | Ngoài phạm vi công nghệ đã chọn; cần hiểu lý thuyết, không tính là lỗi chức năng pipeline. |
| 17 | Spark data skew | Không có phép thử Spark phân tán | Ngoài triển khai. Không được lấy phân bố SKU hoặc nhiều số 0 làm bằng chứng đã xử lý Spark skew. |
| 18 | Broadcast join | Join/tra dimension trong Python/SQL; không dùng Spark broadcast | Ngoài triển khai. Không cần thêm Spark chỉ để có một Broadcast Join. |
| 19 | Small files | Consumer gom batch theo số lượng/thời gian; ca soak tạo 56 object cho 600 đơn | Đáp ứng một phần. Có giảm số file bằng micro-batch nhưng chưa có compaction/retention và phép đo khi số object tăng lớn. |
| 20 | Incremental pipeline | Kafka offset; worker chỉ chọn object chưa checkpoint; checkpoint sau ETL thành công | Đã áp dụng và được đối soát qua ca tải/restart. Mốc commit nguồn và checkpoint ETL là hai trạng thái khác nhau. |
| 21 | DAG Airflow production | Worker điều phối consumer/ETL; tác vụ ML và BI tách riêng | Không dùng Airflow; có điều phối cơ bản. Lịch phụ thuộc toàn nhánh, khoảng dữ liệu thực thi, chống chạy chồng và phục hồi cả vòng chưa nghiệm thu đầy đủ. |
| 22 | Backfill an toàn | CLI mặc định dry-run, giới hạn 31 ngày, chọn object, khóa PostgreSQL chung với worker; kiểm tra version tại sink | Đã chạy dry/apply/replay trên đúng 2 object thật của lô CMP; nội dung nghiệp vụ trước–sau không đổi. Có kiểm thử PostgreSQL riêng về chặn ghi lùi và chạy chồng. Không reset checkpoint. |
| 23 | dbt | SQL/Git, validation và test riêng; tài liệu schema/luồng | Không dùng dbt. Một số chức năng tương đương đã có; chưa có DAG SQL và lineage tự sinh theo cơ chế dbt. Công cụ này không phải yêu cầu bắt buộc của luồng hiện tại. |
| 24 | Data quality dimensions | Hợp đồng nguồn, kiểm tra null/quantity/giá/trạng thái, accounting, khóa duy nhất, đối soát các tầng, backlog/latency/volume metrics | Đã kiểm thử các kiểm soát kỹ thuật và ngưỡng local. Tính đầy đủ thu thập từng ngày từ nguồn CSV kinh doanh chưa được chứng nhận; không đồng nhất với tính đầy đủ của lô Kafka đã đối soát. |
| 25 | Observability và lineage | Metadata topic/partition/offset/ingestion; object/run accounting; dataset hash/model version; /pipeline/status; 6 panel mới; 9 rule tổng cộng | Đã kiểm tra ngưỡng bằng promtool và giao nhận cảnh báo thật tới receiver nội bộ. Có trace object → ETL run → dữ liệu đích và nhánh ML riêng; chưa có catalog lineage tự động cho mọi tài sản hoặc gửi thông báo tới người khác. |
| 26 | Điều tra Spark job chậm | Ca tải đo tài nguyên/backlog/HTTP; không có Spark UI | Ngoài tình huống công nghệ. Có thể vận dụng tư duy kiểm tra I/O, số file, tài nguyên và thay đổi mã; không nhận đã kiểm thử Spark stage/shuffle. |
| 27 | Dashboard bị trùng giao dịch | Đã sửa grain, count distinct/AOV; replay/upsert; đối soát query summary với fact | Có bằng chứng xử lý ở tầng dữ liệu và nguồn BI. Power BI Desktop refresh/render và đối soát visual do sinh viên tự hoàn thiện theo xác nhận ngày 08/11. |
| 28 | Clickstream real-time | Có mẫu kiến trúc nguồn → Kafka → xử lý → lưu → tiêu thụ | Có nền tảng kiến trúc để giải thích, chưa triển khai SDK/session window/clickstream sink. Ngoài bài toán đơn hàng hiện tại. |
| 29 | RAG ingestion | Không có chunking, embedding/vector store hoặc đồng bộ xóa tài liệu | Ngoài phạm vi. Không phải điều kiện để đồ án pipeline đơn hàng đạt mục tiêu. |

## Các phần được chốt và ranh giới khi bảo vệ mục tiêu pipeline

1. **Định nghĩa chính xác điểm đầu và điểm cuối.** Nêu nhánh nào tự chạy liên tục, nhánh nào chạy khi kích hoạt, nhánh nào mới được kiểm chứng cô lập. Nếu khẳng định sự kiện mới tự đi tới feature/model/visual mới, phải có một ca cùng nguồn và cùng mã đợt chứng minh đúng điều đó.
2. **Hợp đồng dữ liệu và schema evolution đã bổ sung.** Xem orders-v1.json và dữ liệu của ca additive/breaking schema; không nhận thêm công nghệ chỉ để có tên Schema Registry.
3. **Backfill có giới hạn đã kiểm thử.** Có đối soát trước–sau của dry-run/apply/replay trên lô riêng, kiểm tra chặn ghi lùi và khóa chống ghi chồng. Không reset checkpoint toàn cục.
4. **Giám sát và SLA local đã bổ sung.** Tách số đo ACK/quan sát hoàn tất lô/P95 consumer đến hoàn tất mẻ/HTTP; theo dõi backlog, độ trễ và volume sau warm-up. Alert đã giao thực tới receiver nội bộ; không đồng nghĩa có thông báo ra ngoài hoặc SLA doanh nghiệp.
5. **Hoàn tất đầu ra đã cam kết.** Nếu báo cáo tuyên bố có dashboard hoạt động, cần Power BI Desktop Apply/Refresh và ảnh/đối soát visual. Nhánh ML chỉ cần chứng minh đúng dữ liệu, đặc trưng, phiên bản, lưu/nạp, đầu ra và xử lý lỗi theo tiêu chí kỹ thuật đã công bố; chất lượng dự báo được ghi là giới hạn ứng dụng.

Không cần thêm Spark, CDC, SCD2, Lakehouse, Airflow, dbt, clickstream hoặc RAG chỉ để đánh dấu đủ 29 câu. Chỉ bổ sung khi một yêu cầu chức năng hoặc quy mô dữ liệu cụ thể đòi hỏi.

## Cách trình bày kết quả mô hình trong đồ án

Tiêu chí pipeline/MLOps gồm: đầu vào đúng nguồn và thời điểm; đặc trưng đúng hợp đồng; huấn luyện tái lập; metadata/phiên bản có thể truy vết; artifact nạp được trong môi trường phục vụ; API trả đầu ra đúng cấu trúc và ghi rõ fallback; lỗi không bị báo thành công giả. Các lỗi căn chỉnh baseline, metric hoặc metadata vẫn cần sửa vì ảnh hưởng tính đúng của pipeline thử nghiệm.

WAPE là một chỉ số được tính và lưu để minh họa đánh giá mô hình. Không dùng việc WAPE chưa thấp làm lý do kết luận toàn pipeline không đạt. Đồng thời, không dùng pipeline thông suốt để khẳng định dự báo đã đủ cho nhập hàng thật hoặc đã tối ưu tồn kho.

Chưa gán điểm hoặc tỷ lệ đạt 29/29 vì bộ câu hỏi không cung cấp trọng số chấm và gồm nhiều mục ngoài phạm vi. Chưa xác nhận toàn bộ yêu cầu cẩm nang chỉ bằng bảng này; đây là đối chiếu kỹ thuật theo mục tiêu pipeline sinh viên vừa làm rõ.

## Nguồn kiểm tra

- [Bộ 29 câu do sinh viên gửi](<C:/Users/MINH/.codex/attachments/d8461348-6f32-4af9-a072-1aca4110c408/Pasted text.txt>).
- [Báo cáo kiểm thử E2E 04/11](../../output/e2e-2026-11-04/Bao-cao-kiem-thu-end-to-end.md), đặc biệt các mục 1, 3, 5–8.
- [Luồng chính và vận hành](../05-how-to-run/core-pipeline.md).
- [Consumer Parquet](../../ingestion/consumer_to_minio.py), [consumer tồn kho](../../ingestion/consumer_inventory_to_postgres.py), [worker/checkpoint](../../scripts/run_pipeline_worker.py).
- [ETL pipeline](../../warehouse/etl/pipeline.py), [extract](../../warehouse/etl/extract.py), [load/upsert](../../warehouse/etl/load.py), [schema BI](../../warehouse/reporting/bi_schema.sql).
- [Manifest nguồn mô hình](../../data/model_manifest.json), [rule giám sát](../../monitoring/prometheus/alert_rules.yml).
- [Kết quả mô hình 08/11 và giới hạn ứng dụng](cai-thien-du-bao-2026-11-08.md).
