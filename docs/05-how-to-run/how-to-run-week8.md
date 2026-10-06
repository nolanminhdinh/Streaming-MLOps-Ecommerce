# Hướng dẫn chạy trọn vẹn luồng MLOps Tuần 8: Monitoring (Prometheus, Grafana), Data Drift & Closed-Loop Retraining

Tài liệu này hướng dẫn cách khởi động hạ tầng giám sát thời gian thực với Prometheus và Grafana, kiểm tra Dashboard vận hành phục vụ mô hình & quản trị tồn kho, chạy pipeline phát hiện trôi dạt dữ liệu (Data Drift) bằng Evidently AI và kích hoạt quy trình tự động tái huấn luyện khép kín (Closed-Loop Retraining).

---

## 1. Khởi động Hạ tầng Giám sát (Prometheus & Grafana)

Khởi động các containers giám sát cùng với API Model Serving:

```bash
# Khởi động Prometheus, Grafana và FastAPI Serving
docker compose up -d prometheus grafana fastapi

# Kiểm tra trạng thái hoạt động của các containers
docker compose ps prometheus grafana fastapi
```

> **Kỳ vọng:**
> - `prometheus` đang chạy tại: **`http://localhost:9090`**
> - `grafana` đang chạy tại: **`http://localhost:3000`**
> - `fastapi` đang phục vụ metrics tại: **`http://localhost:8000/metrics`**

---

## 2. Kiểm tra Prometheus Scraper (`http://localhost:9090`)

1. Mở trình duyệt web truy cập: **`http://localhost:9090`**
2. Vào menu **Status** ➔ **Targets**:
   - Kiểm tra endpoint `fastapi:8000/metrics` có trạng thái **UP (1/1)**.
3. Vào tab **Graph**, thử nghiệm một số câu truy vấn PromQL:
   - `ecommerce_api_requests_total`: Số lượng request theo từng endpoint.
   - `ecommerce_inventory_alerts_total`: Số lượng SKU theo từng cấp độ cảnh báo (`critical`, `warning`, `normal`).
   - `ecommerce_uptime_seconds`: Thời gian uptime của dịch vụ.
   - `ecommerce_data_drift_ratio`: Tỷ lệ data drift hiện thời.

---

## 3. Xem Dashboard trên Grafana (`http://localhost:3000`)

1. Mở trình duyệt web truy cập: **`http://localhost:3000`**
2. Đăng nhập với tài khoản mặc định:
   - **Username**: `admin`
   - **Password**: `admin`
3. Điều hướng tới menu **Dashboards** ➔ Thư mục **MLOps Production** ➔ Mở dashboard:
   ```
   E-Commerce Streaming MLOps: Serving & Inventory Dashboard
   ```
4. **Các tính năng nổi bật trên Dashboard**:
   - **Hàng 1**: Trạng thái Model Registry (`LightGBM_Tuned`), Uptime, Tổng số request, và chỉ số Data Drift.
   - **Hàng 2**: 3 chỉ số Stat thẻ màu hiển thị số SKU báo động đỏ 🔴 **CRITICAL**, vàng 🟡 **WARNING**, và xanh 🟢 **NORMAL**.
   - **Hàng 3**: Biểu đồ Time-series lưu lượng Request Rate (RPS by Endpoint) và biểu đồ tròn tỷ lệ phân bổ trạng thái kho hàng.
   - Chế độ tự động làm mới (**Auto-refresh**): 10 giây/lần.

---

## 4. Chạy Pipeline Phát hiện Data Drift & Concept Drift

Chạy kịch bản phân tích trôi dạt dữ liệu bằng kiểm định Kolmogorov-Smirnov và PSI:

```bash
# Chạy kiểm định Drift và sinh báo cáo
python monitoring/evidently/drift_detector.py
```

### Kết quả xuất bản:
1. **Tệp tóm tắt JSON (`data/monitoring_reports/drift_summary.json`)**:
   - Cung cấp tỷ lệ `drift_share`, danh sách `drifted_features`, và cờ `target_drift_detected`.
2. **Báo cáo HTML Trực quan (`data/monitoring_reports/data_drift_report.html`)**:
   - Mở trực tiếp tệp này trên trình duyệt web (Chrome, Edge, Firefox) để xem giao diện Dashboard phân tích từng biến trễ (lags), rolling stats kèm các chỉ số thống kê $p$-value và PSI.

---

## 5. Tự động Kích hoạt Tái huấn luyện Khép kín (Closed-Loop Retraining)

Khi hệ thống phát hiện trôi dạt dữ liệu hoặc trôi dạt khái niệm vượt ngưỡng, kịch bản tự động thực thi chu trình khép kín:

```bash
# Kích hoạt kiểm tra điều kiện và tái huấn luyện
python monitoring/evidently/trigger_retraining.py
```

### Các bước được thực thi tự động:
1. Đọc báo cáo drift gần nhất.
2. Nếu `drift_share >= 30%` hoặc `target_drift_detected == True`:
   - Thực thi huấn luyện lại mô hình Champion với cửa sổ dữ liệu mới.
   - Tự động nâng cấp phiên bản mô hình trong `data/model_manifest.json` (từ Version 1 ➔ Version 2).
   - Ghi lại nhật ký kiểm toán tại: `data/monitoring_reports/retraining_log.json`.
3. Khi service FastAPI tiếp nhận các request tiếp theo, mô hình phiên bản mới sẽ được phục vụ tự động mà không cần can thiệp thủ công.

---

## 6. Chạy bộ kiểm thử tự động (Unit Tests)

Chạy bộ unit test kiểm tra toàn bộ module Giám sát và Tái huấn luyện:

```bash
# Kiểm thử riêng Module Monitoring Tuần 8
python -m unittest tests/test_monitoring.py

# Hoặc kiểm thử toàn bộ dự án
python -m unittest discover tests
```

> **Kỳ vọng:** Toàn bộ 42 test cases của dự án đều đạt trạng thái **OK**.

