# Hướng dẫn Vận hành Tuần 10: Nghiệm thu Hệ thống & Chuẩn bị Bảo vệ ĐATN

Tài liệu này cung cấp quy trình kiểm tra toàn diện (End-to-End Rehearsal) và chuẩn bị bảo vệ Đồ án Tốt nghiệp (ĐATN) trước Hội đồng thẩm định.

---

## 1. Trạng thái kiểm chứng trước nghiệm thu

Không dùng số lượng test mẫu trong tài liệu cũ làm kết quả hiện tại. Baseline ngày 2026-10-09 ghi 71 pass, 1 fail và 1 warning; lỗi Power BI `Dim_Dates.day` đã được sửa và export runtime đã chạy thành công. Nghiệm thu hiện tại xác nhận data path, serving fallback, monitoring, Power BI và HTTP load; nhánh model ML đang bị chặn vì warehouse mới phủ 10 ngày lịch, chưa đủ lag 28 để tạo mẫu train. Xem [báo cáo kiểm thử](../03-testing-and-benchmark/test_report_2026-10-09.md) và [báo cáo sửa lần 3](../04-pipeline-updates/lan-03-e2e-remediation-2026-10-09.md).

---

## 2. Làm mới Dữ liệu Power BI Desktop

Khởi động PostgreSQL, áp dụng DDL mới (bao gồm bảng lưu forecast), nạp đơn hàng và chạy Model Serving:

```bash
docker compose up -d postgres
python scripts/init_warehouse.py
# Chạy ingestion/ETL và Model Serving trước khi xuất.
```

Gọi `/predict/demand` để ghi forecast ML được tạo từ lịch sử warehouse, sau đó xuất các bảng thật:

```bash
python powerbi/export_powerbi_dataset.py
```

Lệnh xuất đọc `Fact_Orders`, `Fact_Inventory_Daily` và `Fact_Forecast_Predictions`; không sinh dữ liệu ngẫu nhiên. `Forecast_vs_Actual.csv` chỉ có accuracy sau khi ngày dự báo kết thúc và actual đã có trong kho. Khi chưa có model forecast đã lưu, bảng forecast có header nhưng chưa có dòng.

Mở **Power BI Desktop** và chọn **Refresh** để tải các CSV đã cập nhật hoặc kết nối trực tiếp vào PostgreSQL Views (`powerbi/views_for_powerbi.sql`).

---

## 3. Chạy luồng dữ liệu đầu cuối theo đúng thứ tự

Khởi động PostgreSQL trước để xác nhận DDL và bảo đảm database MLflow tồn tại (lệnh init không xóa bảng/dữ liệu; không dùng `--drop-first`):

```bash
docker compose up -d postgres
python scripts/init_warehouse.py

# Khởi động Kafka, MinIO và MLflow sau khi warehouse đã sẵn sàng
docker compose up -d --build zookeeper kafka minio mlflow
```

Mở 3 terminal tại thư mục gốc. Giữ consumer chạy trong lúc producer phát dữ liệu:

```bash
# Terminal 1: Kafka orders -> MinIO Parquet
python ingestion/consumer_to_minio.py
```

```bash
# Terminal 2: Kafka inventory snapshots -> PostgreSQL Fact_Inventory_Daily
python ingestion/consumer_inventory_to_postgres.py
```

```bash
# Terminal 3: phát orders và inventory snapshots; Ctrl+C để dừng sau khi có đủ dữ liệu
python ingestion/producer.py
```

Sau khi producer dừng và consumer đã flush batch cuối, chạy:

```bash
# MinIO Parquet -> Fact_Orders/Dimensions
python warehouse/etl/pipeline.py --all-dates

# Fact_Orders -> Feature Store và feature_spec.json
python ml/features/feature_pipeline.py

# Log model + feature_spec vào experiment có artifact phục vụ được
python ml/training/train_baseline.py --experiment-name demand-forecasting-baseline

# Chỉ tạo manifest khi Registry, run artifact và feature spec đều hợp lệ
python ml/training/register_model.py --experiment-name demand-forecasting-baseline --stage Staging

# Khởi động API sau khi có dữ liệu và model đã đăng ký
docker compose up -d --build fastapi prometheus grafana
```

Xác nhận `/ready` trả HTTP 200 trước khi gọi forecast. Nếu FastAPI chưa ready, đọc `/health`, `/model/metadata` và log container.

```bash
curl -i http://localhost:8000/ready
curl http://localhost:8000/health
curl http://localhost:8000/model/metadata
```

### Gọi API theo schema hiện tại

```bash
curl -X POST "http://localhost:8000/predict/demand" \
     -H "Content-Type: application/json" \
     -d '{"sku":"DRS-MD-001","from_date":"2026-10-10","to_date":"2026-10-16","confidence_interval":true}'

curl "http://localhost:8000/inventory/reorder-alert?lead_time_days=3&service_level=0.95"
curl -X POST "http://localhost:8000/inventory/reorder-alert" \
     -H "Content-Type: application/json" \
     -d '{"skus":["DRS-MD-001"],"current_stocks":{"DRS-MD-001":10},"lead_time_days":3,"service_level":0.95}'
```

Response forecast có `model_source` và `history_source`; chỉ `mlflow_registry` hoặc `local_joblib` là model ML. Với inventory, kiểm tra `stock_source` và `demand_source` để phân biệt snapshot/fallback.

## 4. Khởi động dashboard & giám sát

Prometheus scrape `/metrics`; Grafana hiển thị model readiness, fallback rate, request rate và P95/P99. Power BI export đọc facts đã lưu trong warehouse; API chỉ lưu forecast ML có lịch sử warehouse đủ điều kiện.

## 5. Chạy Kiểm định Drift & Kích hoạt Tái huấn luyện Closed-Loop

```bash
# 1. Đo lường Data Drift & Concept Drift
python monitoring/evidently/drift_detector.py

# 2. Mở báo cáo HTML trực quan hóa phân phối biến
# data/monitoring_reports/data_drift_report.html

# 3. Summary từ synthetic_demo không tự kích hoạt retraining.
python monitoring/evidently/trigger_retraining.py
```

Chỉ chạy retraining sau khi đã tạo Feature Store và drift summary từ warehouse. Feature spec cần có `data_source=warehouse`; nguồn synthetic/local không được đăng ký làm model phục vụ. Trạng thái `SUCCESS` yêu cầu model đăng ký thành công và `/model/reload` xác nhận `model_ready=true`; nếu reload thất bại, manifest cũ được khôi phục.

---

## 6. Thực nghiệm Tải Đồng thời (Load Testing)

Chạy bài kiểm thử hiệu năng và thông lượng hệ thống phục vụ:

```bash
python tests/load_testing/run_load_test.py
```

Kết quả đo lường phân vị độ trễ (P50, P95, P99) và thông lượng (RPS) sẽ được cập nhật tự động tại:
- `data/load_test_results.json`
- `data/load_test_summary.md`

---

## 7. Danh mục Tài liệu của Đồ án

| Tài liệu | Đường dẫn | Nội dung |
|---|---|---|
| **Bối cảnh dự án** | [`PROJECT_CONTEXT.md`](../../PROJECT_CONTEXT.md) | Kiến trúc và tiến độ |
| **Kịch bản Demo Trực tiếp** | [`live_demo_script.md`](live_demo_script.md) | Kịch bản demo có kiểm tra nguồn dữ liệu/model |
| **Cấu trúc Slide Bảo vệ** | [`presentation_slides.md`](../00-architecture-overview/presentation_slides.md) | Dàn ý slide; số liệu cũ được ghi chú cần xác nhận |
| **Báo cáo Tuần 1 - 10** | [`docs/06-weekly-progress/`](../06-weekly-progress/) | Nhật ký tiến độ theo tuần |
| **Hướng dẫn Vận hành** | [`docs/05-how-to-run/`](.) | Sổ tay kỹ thuật từng module |

