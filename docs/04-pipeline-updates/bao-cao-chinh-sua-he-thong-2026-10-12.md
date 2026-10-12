# BÁO CÁO CHỈNH SỬA VÀ KIỂM TRA HỆ THỐNG STREAMING MLOPS E-COMMERCE

**Ngày lập báo cáo:** 12/10/2026

**Dự án:** Streaming MLOps E-commerce — Dự báo nhu cầu hàng hóa

**Phạm vi:** Thu nạp dữ liệu, ETL, đặc trưng, tái huấn luyện, phục vụ dự báo và báo cáo Power BI

**Trạng thái:** Hoàn tất chỉnh sửa mã nguồn và kiểm thử; chờ áp dụng migration và nghiệm thu runtime Docker.

## 1. Mục đích chỉnh sửa

Đợt chỉnh sửa nhằm khắc phục các lỗi phát hiện trong quá trình rà soát luồng end-to-end của dự án. Các lỗi tập trung ở điểm nối giữa các thành phần: ngày phân vùng dữ liệu, nguồn lịch sử dùng cho mô hình, nguồn actual dùng để đánh giá forecast và tính nhất quán của các chỉ tiêu trên dashboard.

Mục tiêu cụ thể là bảo đảm đơn hàng được xử lý theo ngày nghiệp vụ Việt Nam; duy trì khả năng truy vết dữ liệu qua các bước huấn luyện và phục vụ; tránh đánh giá accuracy khi dữ liệu actual chưa đầy đủ; thống nhất công thức phân loại và tồn kho; đồng thời giúp hệ thống nhận biết lỗi xử lý thay vì báo thành công khi dữ liệu bị thiếu.

Báo cáo này phân biệt kết quả kiểm thử mã nguồn và SQL cô lập với trạng thái vận hành của các dịch vụ thật. Các bản sửa đã vượt qua kiểm thử nhưng chưa được nghiệm thu trên toàn tuyến Docker.

## 2. Hiện trạng trước chỉnh sửa

Kiến trúc của dự án gồm hai nguồn nhu cầu chính:

- **Dữ liệu streaming:** Simulator → Kafka → MinIO Parquet → ETL → `Fact_Orders`.
- **Lịch sử CSV:** CSV Shopee/TikTok đã khử định danh → importer → `Fact_Historical_Order_Lines`.

Từ warehouse, hệ thống tạo Feature Store, huấn luyện và đăng ký model với MLflow, sau đó phục vụ qua FastAPI. Power BI đọc các fact và view để phân tích nhu cầu, tồn kho và sai số dự báo. Drift detector và retraining trigger đảm nhiệm phần giám sát, tái huấn luyện.

Khi kiểm tra trước chỉnh sửa, 75/76 pytest đạt. Test xuất Power BI thất bại do PostgreSQL chưa chạy. Việc rà soát mã nguồn và các phép thử riêng phát hiện năm lỗi sau:

| STT | Vấn đề | Mức ưu tiên | Ảnh hưởng |
|---|---|---|---|
| 1 | Batch trộn Shopee/TikTok phân vùng đơn TikTok sai ngày | P1 | ETL chọn theo ngày có thể bỏ sót dữ liệu lịch sử hoặc đơn đến muộn |
| 2 | Retraining mặc định đọc nguồn khác model đang phục vụ | P1 | Huấn luyện trên dữ liệu không phù hợp hoặc bị chặn dù nguồn đúng có đủ lịch sử |
| 3 | Power BI ghép forecast CSV với actual từ streaming | P1 | Sai nguồn đối chiếu, có thể coi dữ liệu chưa có là actual bằng 0 |
| 4 | Công thức ABC/XYZ trong SQL khác Python | P2 | Phân nhóm SKU, safety stock và reorder point không nhất quán |
| 5 | Lỗi đọc object MinIO bị chuyển thành DataFrame rỗng | P2 | ETL có thể báo thành công khi chỉ đọc được một phần dữ liệu |

P1 là các lỗi cần ưu tiên xử lý trước nghiệm thu đầu cuối. P2 là các lỗi ảnh hưởng tính đúng đắn và khả năng phát hiện sự cố trong vận hành.

## 3. Nội dung chỉnh sửa

### 3.1. Phân vùng dữ liệu theo giờ Việt Nam

**Nguyên nhân:** Shopee sử dụng trường `create_time`, TikTok sử dụng `created_time`. Consumer trước đây chọn một cột timestamp cho cả batch. Khi batch có cả hai nền tảng, cột `create_time` tồn tại nhưng giá trị ở dòng TikTok bị thiếu. Dòng này được phân vùng theo ngày nhận thay vì ngày tạo đơn.

**Bản sửa:** Parse từng cột timestamp rồi kết hợp theo từng dòng. Timestamp được chuyển sang `Asia/Ho_Chi_Minh` (UTC+7) trước khi lấy năm, tháng và ngày. Parser hỗ trợ chuỗi ISO có hoặc không có phần mili giây và các offset khác nhau. ETL và CSV importer cũng được đồng bộ cách parse này.

Nếu cả hai timestamp đều thiếu hoặc không hợp lệ, consumer giữ cơ chế dự phòng dùng ngày nhận theo giờ Việt Nam. Timestamp không ghi offset vẫn được hiểu là UTC rồi đổi sang giờ Việt Nam, thống nhất với hợp đồng xử lý hiện tại của ETL.

**Ví dụ kiểm chứng:** Đơn TikTok tạo lúc `2026-07-02T10:00:00+07:00` trong batch có Shopee được ghi vào `year=2026/month=07/day=02`. Mốc `2026-12-31T17:00:00Z` được quy đổi thành 00:00 ngày 01/01/2027 ở Việt Nam và phân vùng sang năm 2027.

**Phạm vi áp dụng:** Bản sửa có hiệu lực với các batch được xử lý sau khi cập nhật consumer. Các object MinIO đã phân vùng sai trước đây chưa được di chuyển hoặc viết lại trong đợt này.

### 3.2. Giữ đúng nguồn dữ liệu qua drift, retraining và serving

**Nguyên nhân:** Model hiện tại sử dụng `public_anonymized_historical_csv`, nhưng `default_train_fn()` trước đây gọi loader không truyền nguồn. Nhánh mặc định đọc `Fact_Orders`, làm mất liên hệ với lịch sử CSV của deployment.

**Bản sửa:**

- Loader nhận `data_origin` và danh sách `source_datasets` để chọn đúng bảng và fingerprint.
- Drift summary mang theo nguồn warehouse, origin và fingerprints từ feature spec.
- Retraining mặc định lấy nguồn của deployment và kiểm tra nguồn drift trước khi train.
- Model đăng ký mới phải giữ origin và fingerprints phù hợp với deployment được tái huấn luyện.
- Manifest mới lưu fingerprints; catalog, lịch sử và ngày dữ liệu cuối của serving cũng được lọc theo fingerprints trong feature spec.
- CLI retraining trả exit code 1 khi thất bại hoặc bị chặn để tác vụ tự động nhận biết trạng thái.

Manifest cũ chưa có fingerprints được đọc bổ sung từ feature spec cùng origin. Drift summary cũ thiếu provenance phải được tạo lại. Khi thay bộ CSV, cần chọn và huấn luyện nguồn mới một cách rõ ràng; `--force` không cho phép vòng retraining tự đổi nguồn ngầm.

**Kết quả kiểm chứng:** Các test xác nhận retraining đọc đúng origin/fingerprints, từ chối drift khác nguồn và giữ manifest cũ khi model mới đăng ký đổi bộ dữ liệu.

### 3.3. Đối chiếu forecast với actual cùng nguồn

**Nguyên nhân:** API có thể phục vụ model dựa trên lịch sử CSV, nhưng view accuracy trước đây chỉ lấy actual từ `Fact_Orders`. Bảng forecast chưa lưu origin/fingerprints để lựa chọn actual. Giá trị actual thiếu còn bị thay bằng 0 sau khi ngày mục tiêu kết thúc.

**Bản sửa:** Bổ sung `data_origin`, `source_datasets` vào `Fact_Forecast_Predictions`. Khóa unique được mở rộng để phân biệt nguồn, bộ dữ liệu và model. View Power BI chọn actual từ đúng fact và chỉ lấy các fingerprint liên quan.

Bổ sung bảng `Warehouse_Daily_Coverage` và lệnh `scripts/confirm_daily_coverage.py` để xác nhận một ngày đã được nạp đầy đủ. Điều kiện chấm accuracy gồm ngày mục tiêu đã kết thúc theo giờ Việt Nam và coverage của nguồn đã được xác nhận. Với CSV, mỗi fingerprint trong forecast phải có coverage cho ngày đó.

| Tình trạng actual | Giá trị báo cáo |
|---|---|
| Chưa xác nhận nạp đủ ngày | `actual_complete=false`; actual và error là NULL |
| Đã xác nhận đủ ngày, SKU có bán | Actual bằng tổng số lượng từ đúng nguồn/dataset |
| Đã xác nhận đủ ngày, SKU không bán | Actual bằng 0 |
| Forecast cũ thiếu provenance | Giữ `data_origin=unknown`, chưa chấm accuracy |

Coverage cần được xác nhận sau khi đối soát dữ liệu; một batch ETL thành công hoặc sự hiện diện của đơn hàng chưa chứng minh đã nạp hết ngày.

**Kết quả kiểm chứng:** Trong fixture PostgreSQL, cùng một SKU có actual CSV bằng 7 và actual streaming bằng 99. View trả đúng số lượng theo origin và loại dataset không liên quan có số lượng 1.000. Khi mới xác nhận một trong hai fingerprint CSV, actual vẫn là NULL; khi cả hai đã được xác nhận, actual bằng 7. Ngày đã xác nhận không có sales được ghi nhận bằng 0.

### 3.4. Thống nhất ABC/XYZ và chính sách tồn kho

**Nguyên nhân:** SQL tính độ biến động trên những ngày có bán, còn Python bổ sung ngày không bán bằng 0. SQL và Python cũng khác cách dùng ngưỡng Pareto, biên CV và xử lý doanh thu bằng 0.

**Bản sửa:**

- Tạo calendar grid đầy đủ trong cửa sổ dữ liệu theo từng origin.
- Dùng tỷ trọng tích lũy trước SKU hiện tại để phân nhóm ABC.
- Chuyển sang tổng số lượng bán khi không có tổng doanh thu dương.
- Thống nhất X khi `CV ≤ 0,5`, Y khi `0,5 < CV ≤ 1`, Z khi `CV > 1`.
- Thống nhất thứ tự SKU khi có giá trị đóng góp bằng nhau.
- Dùng độ lệch chuẩn bằng 0 cho cửa sổ chỉ có một ngày.
- Tính tồn kho từ thống kê chưa làm tròn, với hệ số Z=1,65 cho service level 0,95.

Với lead time 3 ngày, các công thức mặc định là:

```text
CV = độ lệch chuẩn nhu cầu ngày / nhu cầu trung bình ngày
Safety Stock = ceil(1,65 × độ lệch chuẩn × sqrt(3))
Reorder Point = ceil(nhu cầu trung bình × 3 + Safety Stock)
```

View mới `vw_powerbi_abc_xyz_by_origin` phân loại theo từng origin. View tài chính/tồn kho cũ tiếp tục phục vụ dữ liệu streaming.

**Kết quả kiểm chứng:** SQL và Python khớp ABC/XYZ, mean, std, safety stock và reorder point trên năm fixture: sales gián đoạn, doanh thu bằng 0, đóng góp bằng nhau, một ngày dữ liệu và CV đúng bằng 0,5.

### 3.5. Dừng ETL khi đọc MinIO thất bại

**Nguyên nhân:** `read_parquet_from_minio()` bắt `S3Error` rồi trả DataFrame rỗng. Extract bỏ qua frame rỗng và tiếp tục, khiến lỗi đọc có thể bị hiểu thành không có dữ liệu hoặc xử lý thành công một phần.

**Bản sửa:** Lỗi storage được truyền lên caller để pipeline dừng trước bước load. Response được đóng và giải phóng kết nối trong `finally`, kể cả khi thao tác đọc bị gián đoạn.

**Kết quả kiểm chứng:** Tests xác nhận lỗi quyền đọc/storage không bị che, batch có một object lỗi không trả thành công một phần và response được giải phóng khi read thất bại.

### 3.6. Hoàn thiện báo cáo và khởi động

Exporter Power BI cho phép warehouse chỉ chứa lịch sử CSV. File `Historical_Demand_Daily.csv` cung cấp số lượng theo SKU, ngày, platform, origin và fingerprint. Dữ liệu tài chính vẫn đọc từ streaming; CSV thiếu thông tin tài chính không được biến thành doanh thu giả.

`Forecast_vs_Actual.csv` bổ sung origin, fingerprints và trạng thái actual đầy đủ. Script `start_full_stack.bat` kiểm tra Docker/Compose và trả lỗi khi khởi động thất bại, tránh in trạng thái OK sai. Producer, consumer, ETL và training vẫn chạy riêng theo hướng dẫn vận hành.

## 4. Kết quả kiểm thử

| Hạng mục | Kết quả | Phạm vi chứng minh |
|---|---|---|
| Toàn bộ pytest | **96 passed, 0 failed, 0 skipped**, 9,98 giây | Logic, hợp đồng dữ liệu, API/TestClient, feature parity, serialization và các nhánh lỗi |
| Kiểm tra PostgreSQL cô lập | **8 trường hợp đạt** | Migration, compatibility của view, ABC/XYZ/tồn kho, accuracy đúng nguồn và query exporter |
| `docker compose config --quiet` | Đạt | Cấu hình Compose hợp lệ |
| `git diff --check` | Đạt | Không có lỗi whitespace được Git phát hiện |
| Runtime Docker toàn tuyến | Chưa nghiệm thu | Docker Engine chưa khởi động được |

Pytest chạy bằng Python 3.12.14, pytest 9.1.1 và dependencies trong `.venv/Lib/site-packages`. JUnit lưu tại [pytest_fixes_2026-10-12.xml](../03-testing-and-benchmark/pytest_fixes_2026-10-12.xml).

Có 30 cảnh báo deprecation: một cảnh báo Starlette/httpx và 29 cảnh báo PyArrow/pandas. Các cảnh báo này không làm test thất bại.

Kiểm tra SQL sử dụng PostgreSQL WASM PGlite 0.5.8 trong bộ nhớ. Tám trường hợp gồm một case nâng schema/view cũ và chạy lại migration, năm case đối chiếu ABC/XYZ/tồn kho, một case provenance/accuracy/coverage và một case thực thi toàn bộ query exporter trên warehouse chỉ chứa CSV. Fixtures được rollback sau mỗi case.

Test xuất CSV trong pytest dùng warehouse query fixtures, không phụ thuộc PostgreSQL đang mở. Việc thực thi SQL được kiểm tra riêng trên PGlite. Kết quả này không xác nhận kết nối tới warehouse Docker, Kafka broker, MinIO, MLflow hoặc API Docker thật.

## 5. Phạm vi thay đổi

| Nhóm | Tệp tiêu biểu |
|---|---|
| Thu nạp và thời gian | `ingestion/consumer_to_minio.py`, `warehouse/etl/transform.py`, `scripts/import_public_historical_orders.py` |
| ETL | `warehouse/etl/extract.py` |
| Nguồn dữ liệu/model | `ml/features/feature_pipeline.py`, `ml/training/register_model.py`, `monitoring/evidently/drift_detector.py`, `monitoring/evidently/trigger_retraining.py` |
| Serving | `serving/app/data_access.py`, `serving/app/model_loader.py`, `serving/app/main.py` |
| Warehouse/BI | `warehouse/ddl/01_star_schema.sql`, `powerbi/views_for_powerbi.sql`, `powerbi/export_powerbi_dataset.py`, `ml/features/abc_xyz.py` |
| Vận hành | `scripts/confirm_daily_coverage.py`, `scripts/init_warehouse.py`, `start_full_stack.bat` |
| Kiểm thử | `tests/test_ingestion.py`, `tests/test_pipeline_integrity.py`, `tests/test_etl.py`, `tests/test_load_test.py`, `tests/test_serving_realdata.py`, các script kiểm tra SQL |

Bản sửa partition đã được commit và push trong commit `6f78af7`. Các thay đổi bổ sung của đợt sửa lỗi còn lại đang nằm trong working tree tại thời điểm lập báo cáo.

## 6. Hạn chế và việc còn cần thực hiện

Docker Desktop hiện lỗi khởi động ở socket `sailor-ingest.sock`; Windows trả thông báo “The file cannot be accessed by the system”. Các lần khởi động chưa tạo được Docker Engine.

Vì vậy, migration chưa được áp dụng lên warehouse Docker và API chưa được rebuild trong lượt này. Chưa thực hiện lại phép thử toàn tuyến từ phát message, ghi Parquet, ETL, train/register đến HTTP serving, scrape Prometheus và refresh Power BI thật.

Model v2 vẫn ở Staging. WAPE 126,08% là kết quả được lưu từ lượt trước, chưa được đo lại trong đợt chỉnh sửa này. Việc sửa pipeline và chấm actual đúng nguồn không chứng minh chất lượng dự báo đã cải thiện hoặc đủ điều kiện Production.

## 7. Kế hoạch áp dụng và nghiệm thu

Sau khi Docker Engine hoạt động, thực hiện theo thứ tự:

1. Khởi động PostgreSQL và chạy `python scripts/init_warehouse.py` để áp dụng migration.
2. Rebuild FastAPI bằng `docker compose up -d --build fastapi`.
3. Phát batch có ID xác định cho cả Shopee và TikTok, gồm đơn đến muộn và mốc chuyển ngày; đối soát Kafka → MinIO → warehouse.
4. Kiểm tra phục vụ model với origin/fingerprints đúng, forecast được lưu cùng provenance và các nguồn fallback được nhận biết rõ.
5. Đối soát dữ liệu ngày đã hoàn tất, xác nhận coverage, xuất lại CSV và refresh Power BI.
6. Tạo lại drift summary đúng nguồn, kiểm tra retraining và serving reload, đồng thời xác nhận monitoring runtime.

Hướng dẫn lệnh chi tiết và cách chạy lại PostgreSQL cô lập được ghi tại [Lần 5 — Hướng dẫn áp dụng bản sửa](lan-05-pipeline-integrity-2026-10-12.md).

## 8. Kết luận

Đợt chỉnh sửa đã xử lý năm lỗi được phát hiện trong audit, bổ sung truy vết nguồn dữ liệu và điều kiện đầy đủ của actual, đồng thời thống nhất tính toán giữa Python và SQL. Mã nguồn đạt 96 pytest và tám kiểm tra PostgreSQL cô lập.

Trạng thái hiện tại là **sẵn sàng áp dụng bản sửa**, chưa phải **nghiệm thu vận hành end-to-end**. Việc nghiệm thu cuối cùng cần hoàn tất sau khi Docker Desktop hoạt động và các bước migration, rebuild, đối soát dịch vụ thật được thực hiện.

*Báo cáo và các bản sửa có hỗ trợ AI (Codex); số liệu kiểm thử được lấy từ kết quả thực thi đã lưu.*
