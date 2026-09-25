# Hướng dẫn Vận hành Tuần 10: Nghiệm thu Hệ thống & Chuẩn bị Bảo vệ ĐATN

Tài liệu này cung cấp quy trình kiểm tra toàn diện (End-to-End Rehearsal) và chuẩn bị bảo vệ Đồ án Tốt nghiệp (ĐATN) trước Hội đồng thẩm định.

---

## 1. Kiểm tra Bộ Kiểm thử Tự động (Automated Test Suite)

Trước khi thực hiện bất kỳ buổi nghiệm thu hoặc bảo vệ nào, hãy đảm bảo toàn bộ 47 kịch bản kiểm thử đều vượt qua (100% PASS):

```bash
# Chạy toàn bộ test suite từ thư mục gốc
python -m unittest discover tests
```

**Kết quả kỳ vọng**:
```text
Ran 47 tests in ~4.2s
OK (skipped=15)
```
*(15 tests skipped là các kịch bản phụ thuộc môi trường PyTorch/LightGBM máy chủ hoặc Docker Kafka/PostgreSQL thật khi chạy trên máy Host tối giản)*.

---

## 2. Kiểm tra Dữ liệu Phẳng Power BI Desktop

Xuất khẩu bộ dữ liệu phẳng mới nhất ra thư mục `powerbi/data/`:

```bash
python powerbi/export_powerbi_dataset.py
```

Kiểm tra 5 tệp CSV đã được sinh đầy đủ tại `powerbi/data/`:
- `Dim_Products.csv` (20 SKUs đầy đủ danh mục và phân khúc ABC/XYZ)
- `Dim_Geography.csv` (10 tỉnh thành trọng điểm)
- `Fact_Orders_Summary.csv` (Tổng hợp đơn hàng 60 ngày theo sàn)
- `Inventory_Health_Alerts.csv` (Cảnh báo tồn kho CRITICAL / WARNING / NORMAL)
- `Forecast_vs_Actual.csv` (Đối chiếu thực tế vs dự báo)

Mở **Power BI Desktop**, nạp 5 tệp CSV trên hoặc kết nối trực tiếp vào PostgreSQL Views (`powerbi/views_for_powerbi.sql`).

---

## 3. Khởi động Hạ tầng & Thực nghiệm Serving

```bash
# Bước 1: Khởi động container
docker compose up -d

# Bước 2: Kiểm tra trạng thái các container
docker compose ps

# Bước 3: Kiểm tra API Model Serving
curl http://localhost:8000/health
curl http://localhost:8000/model/metadata
```

Thử nghiệm gửi yêu cầu dự báo nhu cầu:
```bash
curl -X POST "http://localhost:8000/predict/demand" \
     -H "Content-Type: application/json" \
     -d "{\"product_id\": \"DRS-MD-001\", \"horizon_days\": 7}"
```

Kiểm tra cảnh báo bổ sung tồn kho tự động:
```bash
curl "http://localhost:8000/inventory/reorder-alert"
```

---

## 4. Chạy Kiểm định Drift & Kích hoạt Tái huấn luyện Closed-Loop

```bash
# 1. Đo lường Data Drift & Concept Drift
python monitoring/evidently/drift_detector.py

# 2. Mở báo cáo HTML trực quan hóa phân phối biến
# data/monitoring_reports/data_drift_report.html

# 3. Kích hoạt quy trình tự động cập nhật Model Version nếu có drift
python monitoring/evidently/trigger_retraining.py
```

---

## 5. Thực nghiệm Tải Đồng thời (Load Testing)

Chạy bài kiểm thử hiệu năng và thông lượng hệ thống phục vụ:

```bash
python tests/load_testing/run_load_test.py
```

Kết quả đo lường phân vị độ trễ (P50, P95, P99) và thông lượng (RPS) sẽ được cập nhật tự động tại:
- `data/load_test_results.json`
- `data/load_test_summary.md`

---

## 6. Danh mục Tài liệu Hoàn chỉnh của Đồ án

| Tài liệu | Đường dẫn | Nội dung |
|---|---|---|
| **Bối cảnh Toàn văn (SSOT)** | [`PROJECT_CONTEXT.md`](../PROJECT_CONTEXT.md) | Nguồn sự thật duy nhất cho AI và Kỹ sư |
| **Kịch bản Demo Trực tiếp** | [`docs/live_demo_script.md`](live_demo_script.md) | Hướng dẫn 5 bước demo trong 15 phút bảo vệ |
| **Cấu trúc Slide Bảo vệ** | [`docs/presentation_slides.md`](presentation_slides.md) | Dàn ý 18 slide thuyết trình chuẩn học thuật |
| **Báo cáo Tuần 1 - 10** | [`docs/weekly-progress/`](weekly-progress/) | Nhật ký tiến độ chi tiết từng tuần theo chuẩn Cẩm nang |
| **Hướng dẫn Vận hành** | `docs/how-to-run-week*.md` | Sổ tay kỹ thuật từng module độc lập |
