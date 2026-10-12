# Kiểm tra runtime end-to-end — 13/10/2026

## Kết quả

**Các điểm nối kỹ thuật đã chạy trên dịch vụ thật.** Migration được áp dụng lên PostgreSQL; Kafka, MinIO, MLflow và FastAPI hoạt động. Các bước thu nạp, ETL, huấn luyện, Registry, phục vụ, Power BI export, monitoring và reload đã được kiểm tra.

**Chưa đủ điều kiện Production:** chất lượng dự báo còn thấp, tồn kho của catalog lịch sử đang dùng fallback demo và endpoint đánh giá toàn catalog còn chậm. Readiness HTTP 200 không thay thế các tiêu chí này.

| Hạng mục | Kết quả thực tế |
|---|---|
| Migration PostgreSQL | Đã áp dụng các cột provenance, khóa unique và Warehouse_Daily_Coverage; giữ dữ liệu hiện có |
| Kafka → MinIO | 64 message orders, ghi đủ 64 dòng trong 32 Parquet objects |
| MinIO → warehouse | 32 đơn duy nhất: Shopee 16 đơn/35 đơn vị, TikTok 16 đơn/27 đơn vị; 0 quarantine |
| Ngày nghiệp vụ | 8 batch trộn hai nền tảng; partition ngày 12 và 13/10 khớp giờ Việt Nam, gồm mốc chuyển ngày UTC 17:00 |
| Idempotency | Mỗi đơn gửi hai lần; chạy ETL lại giữ nguyên số đơn và tổng số lượng |
| Inventory topic | 2 snapshot lưu đúng 123 và 47 đơn vị; API trả nguồn warehouse_snapshot:2026-10-13 |
| Features từ CSV warehouse | 7.000 dòng nguồn, 42.768 feature rows, 216 SKU, 43 input features |
| Training/Registry | Training thật bằng container trainer; model kiểm thử ECommerceDemandForecastModelE2E được đăng ký Staging và tải artifact thành công |
| Closed-loop | Retrain thật → đăng ký model kiểm thử v2 → HTTP reload 200 → API kiểm thử xác nhận version mới |
| API chính | `/ready` 200; model ECommerceDemandForecastModel v2, Staging, source mlflow_registry; lịch sử warehouse kết thúc 2026-08-11 |
| Forecast audit | Ghi 3 dòng forecast cho 14–16/10, đúng origin CSV và hai fingerprints; actual tương lai còn NULL |
| Power BI export | Xuất đủ 7 CSV từ warehouse thật, gồm 3 forecast và 2.211 dòng historical demand daily |
| ABC/XYZ | SQL/Python khớp phân nhóm trên 22 SKU streaming trong warehouse |
| Prometheus | Hai targets up; promtool xác nhận 1 alert rule hợp lệ |
| Grafana | HTTP health: database=ok |
| NiFi | Đã sửa mapping/bucket tại chỗ; 4 event mới đi qua Kafka → NiFi → 4 JSON objects MinIO |
| Pytest cuối | **97 passed, 0 failed, 0 skipped**, 30 deprecation warnings, 6,04 giây |

## Phạm vi kiểm chứng

Luồng streaming dùng simulator và các topic thật `ecom.orders.raw`, `inventory.logs`. Bài test sử dụng consumer loop của dự án, consumer group riêng và SKU/ID có tiền tố `E2E20261013889fdd`. Batch này được xử lý bởi Python Kafka consumer → Parquet → ETL → Fact_Orders. Nhánh inventory ghi Fact_Inventory_Daily.

Luồng mô hình dùng nguồn thật đã nạp trong warehouse: `public_anonymized_historical_csv` ở Fact_Historical_Order_Lines. Feature engineering, training, MLflow artifact/Registry và phục vụ đều dùng nguồn này. **Không coi batch streaming chỉ có hai ngày là dữ liệu đủ để huấn luyện model CSV.** Đây là hai nhánh nguồn được đối soát riêng.

Thử closed-loop sử dụng experiment và model E2E riêng, API tạm trên localhost:8002. Training và registration là thao tác thật; register callback chọn model E2E để không thay deployment chính. Thử nghiệm gọi `force_retrain=True` trên summary drift thật và xác nhận version được reload. Không xem đó là nghiệm thu scheduler tự động chạy định kỳ.

API chính trên 8000 giữ model v2 trong suốt kiểm tra. API E2E tạm đã được dừng sau thử nghiệm; artifacts và audit của model kiểm thử được giữ để đối chiếu.

## Kiểm tra HTTP đồng thời

Smoke test gồm 40 request, 4 luồng đồng thời:

- 24 request `/predict/demand`, yêu cầu model_source=mlflow_registry và history_source=warehouse.
- 8 request `/health`.
- 8 request POST `/inventory/reorder-alert` cho hai SKU kiểm thử có snapshot warehouse.

| Chỉ số | Giá trị |
|---|---:|
| Request thành công | 40/40 |
| Request lỗi | 0 |
| Thời gian toàn bài | 1,330 giây |
| Throughput quan sát | 30,07 request/giây |
| P50 | 125,67 ms |
| P95 | 275,01 ms |

Đây là smoke test nhỏ, không phải phép đo sức tải cực đại hay độ trễ của mọi endpoint. GET inventory toàn catalog được đo riêng và mất **23,312 giây** cho 216 SKU.

## Huấn luyện và drift

Training kiểm thử sử dụng lịch sử CSV warehouse, recursive multi-step evaluation với **2 walk-forward splits**, cửa sổ serving history 240 ngày.

| Mô hình kiểm thử | CV WAPE | MAE | RMSE |
|---|---:|---:|---:|
| Moving Average | 126,47% | 0,1278 | 0,4474 |
| LightGBM | 476,43% | khoảng 0,47 | khoảng 2,15 |

Moving Average được chọn trong experiment kiểm thử. WAPE 126,47% không được dùng để khẳng định chất lượng Production. Metric 126,08% của model chính là kết quả cũ; hai giá trị có giao thức/số splits khác nhau nên không xem chênh lệch là cải thiện hoặc suy giảm đã được kiểm định.

Drift summary cuối được tạo lại từ Feature Store warehouse, giữ origin và fingerprints; drift_share=37,5%, target_drift_detected=false. Các cửa sổ nằm trong lịch sử CSV đến tháng 8, không đại diện dữ liệu thị trường mới ngày 13/10.

Warnings ở trainer: thiếu seaborn nên không xuất biểu đồ; không có Git executable nên MLflow chưa ghi Git SHA tự động; API stage của MLflow có deprecation warning. Model joblib và feature spec vẫn được lưu, đăng ký và tải thành công. Môi trường Python host thiếu MLflow, nên training/registration được thực hiện trong container trainer.

## Các sửa bổ sung trong lúc kiểm tra

1. **Metadata HTTP bỏ fingerprints:** ModelManager có dữ liệu nhưng schema/endpoint chưa trả ra. Đã thêm `source_datasets`, regression test và rebuild API; endpoint thật trả đủ hai fingerprints.
2. **NiFi lệch schema:** mapping cũ dùng `$.platform`, giá trị Shopee/TikTok và bucket ecom-raw-data. Đã đổi sang `$._platform`, shopee/tiktok, hai trường order ID tương ứng và bucket cấu hình của dự án. JSON được lưu riêng dưới `raw/nifi/orders/.../ingest_date=...`, ngày nhận theo Asia/Ho_Chi_Minh. Đây là nhánh JSON trực quan, không thay partition event-date của Parquet.
3. **Test ghi đè báo cáo runtime:** test drift dùng thư mục tạm; unit benchmark không xuất đè báo cáo tải. Sau test suite, đã tạo lại summary/HTML drift từ warehouse thật.

NiFi được cập nhật qua REST API mà không xóa processors hoặc queues. Có script `scripts/repair_nifi_routing.py` để áp dụng lại mapping. Script deploy ban đầu cũng được sửa cùng cấu hình để tránh tái tạo lỗi khi triển khai mới.

## Hạn chế còn lại

- GET inventory toàn catalog mất 23,312 giây. Điều này vượt timeout 5 giây trong load runner hiện tại; cần tối ưu trước khi đo tải diện rộng bằng script đó.
- Toàn bộ 216 SKU của catalog CSV chưa có snapshot tồn kho tương ứng: GET inventory trả stock_source=simulated_demo, demand_source=model_forecast. Hai SKU streaming kiểm thử có snapshot thật nhưng demand_source=fallback_catalog vì model CSV không có lịch sử của chúng. Cần đồng bộ SKU và nguồn tồn kho nếu muốn dùng cảnh báo nhập hàng cho nghiệp vụ thật.
- Lịch sử của model chính chỉ đến 11/08/2026; forecast tháng 10 phải đi qua nhiều bước đệ quy. Cần nạp dữ liệu mới và đánh giá lại chất lượng.
- Forecast ngày 14–16/10 chưa có actual đã hoàn tất; chưa chấm accuracy thật cho các ngày tương lai và không xác nhận coverage khi chưa đủ bằng chứng.
- Chưa phát webhook ra dịch vụ ngoài; chưa refresh giao diện Power BI Desktop, chỉ kiểm chứng SQL và CSV export.
- Chưa thử tải lâu dài, crash/recovery đang ghi batch, nhiều broker/replica hoặc toàn bộ mô hình deep learning.

## Trạng thái sau kiểm tra và bằng chứng

Chín dịch vụ chính đang chạy: ZooKeeper, Kafka, MinIO, PostgreSQL, MLflow, FastAPI, NiFi, Prometheus và Grafana. Các dịch vụ có healthcheck đều healthy. Kafka khởi động lần đầu lỗi NodeExists ở ZooKeeper; khởi động lại sau khi phiên cũ hết hạn đã thành công, không xóa dữ liệu.

Producer/consumer của bài kiểm tra có thời gian chạy hữu hạn và đã dừng. NiFi cùng các dịch vụ Docker vẫn chạy. Các đơn, snapshot, JSON/Parquet và model E2E được giữ lại để kiểm tra; báo cáo tài chính từ streaming có thể chứa các đơn được đánh dấu E2E/NIFIE2E khi ETL xử lý chúng.

- [Kết quả runtime JSON](runtime_e2e_result_2026-10-13.json).
- [Kết quả pytest JUnit](pytest_runtime_2026-10-13.xml).
- Dữ liệu chi tiết: `data/runtime_checks/` và `data/monitoring_reports/` trong workspace.
- Script streaming kiểm tra lại: `python scripts/run_e2e_runtime_check.py` với Python dependencies dự án và các dịch vụ Docker sẵn sàng. Mỗi lần chạy tạo ID/SKU kiểm thử mới và để lại facts để đối soát.

Kết luận: **luồng kỹ thuật đã chạy qua các dịch vụ thật; readiness và các điểm nối chính đạt**. Hiệu năng inventory toàn catalog, dữ liệu tồn kho đúng SKU và chất lượng dự báo vẫn cần cải thiện trước nghiệm thu Production.

Phần kiểm tra, script và sửa bổ sung có hỗ trợ AI (Codex). Thời gian trong log container là UTC; ngày báo cáo và partition nghiệp vụ dùng giờ Việt Nam UTC+7.
