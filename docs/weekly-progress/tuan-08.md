# Tuần 8: Giám sát Hệ thống (Prometheus, Grafana), Phát hiện Data Drift (Evidently AI) & Tái huấn luyện Khép kín (Closed-Loop Retraining)

## Mục tiêu
Thiết lập hạ tầng giám sát vận hành toàn diện cho hệ thống Streaming MLOps; triển khai Prometheus thu thập metrics thời gian thực từ API Model Serving; xây dựng Executive & Technical Dashboard trên Grafana; tích hợp module kiểm định trôi dạt dữ liệu (Data Drift) và trôi dạt khái niệm (Concept Drift) bằng Evidently AI; và giải quyết triệt để khuyết điểm kiến trúc đã nêu trong `docs/architecture.md` bằng cách xây dựng quy trình tự động kích hoạt tái huấn luyện vòng lặp khép kín (Closed-Loop Retraining Pipeline).

---

## Công việc đã thực hiện

### 1. Hạ tầng Giám sát Prometheus (`monitoring/prometheus/prometheus.yml`)
- Cấu hình chu kỳ scrape metrics 10 giây đối với service `fastapi:8000` tại endpoint `/metrics`.
- Chuẩn hóa định dạng xuất bản dữ liệu theo chuẩn **Prometheus Text Exposition Format (v0.0.4)**:
  - `ecommerce_api_requests_total{endpoint=...}`: Đếm tổng số lượng request phân bổ theo từng API endpoint.
  - `ecommerce_uptime_seconds`: Đo lường thời gian hoạt động liên tục (Uptime) của service serving.
  - `ecommerce_inventory_alerts_total{level=...}`: Đo lường số lượng SKU rơi vào các ngưỡng cảnh báo (`critical`, `warning`, `normal`).
  - `ecommerce_model_info{name, version, stage, algorithm}`: Gauge lưu trữ nhãn phiên bản và thuật toán Champion đang phục vụ.
  - `ecommerce_data_drift_ratio`: Tỷ lệ trôi dạt dữ liệu thu thập từ các lần kiểm định gần nhất.
- Kích hoạt service `prometheus` trong [`docker-compose.yml`](file:///c:/Users/MINH/Downloads/temp-extract/streaming-mlops-ecommerce/docker-compose.yml) trên cổng **`9090`**.

---

### 2. Thiết kế Dashboard Trực quan trên Grafana (`monitoring/grafana/`)
- Tự động cấu hình nguồn dữ liệu Prometheus thông qua cơ chế Datasource Provisioning (`monitoring/grafana/provisioning/datasources/prometheus.yml`).
- Tự động nạp Dashboard thông qua Provider (`monitoring/grafana/provisioning/dashboards/dashboards.yml`).
- Xây dựng Dashboard hoàn chỉnh **`E-Commerce Streaming MLOps: Serving & Inventory Dashboard`** ([`ecommerce_mlops_dashboard.json`](file:///c:/Users/MINH/Downloads/temp-extract/streaming-mlops-ecommerce/monitoring/grafana/provisioning/dashboards/ecommerce_mlops_dashboard.json)) gồm 3 phân khu chức năng:
  1. **Hàng 1 (Hệ thống phục vụ mô hình)**: Hiển thị Model Registry Status, Uptime, Tổng số lượng request, và Tỷ lệ Data Drift hiện tại.
  2. **Hàng 2 (Quản trị tồn kho & Cảnh báo)**: 3 Stat Panels hiển thị trực tiếp số lượng SKU ở mức báo động đỏ 🔴 **CRITICAL**, cảnh báo vàng 🟡 **WARNING**, và an toàn 🟢 **NORMAL**.
  3. **Hàng 3 (Lưu lượng & Hiệu năng)**: Biểu đồ Time-series Request Rate (RPS by Endpoint) và biểu đồ tròn phân bố tỷ lệ trạng thái tồn kho toàn chuỗi.
- Kích hoạt service `grafana` trong [`docker-compose.yml`](file:///c:/Users/MINH/Downloads/temp-extract/streaming-mlops-ecommerce/docker-compose.yml) trên cổng **`3000`**.

---

### 3. Phát hiện Data Drift & Concept Drift với Evidently AI (`monitoring/evidently/drift_detector.py`)
- **Kiểm định Thống kê Phân phối**:
  - Áp dụng kiểm định **Kolmogorov-Smirnov hai mẫu độc lập (KS-test)** để so sánh hàm phân phối tích lũy $F_{\text{ref}}(x)$ và $F_{\text{curr}}(x)$ trên các biến đặc trưng dạng số (Lags $1, 7$, Rolling means, Rolling std).
  - Tính toán chỉ số **Population Stability Index (PSI)** đo lường mức độ biến động cấu trúc dữ liệu theo phân vị.
  - Ngưỡng xác định trôi dạt: $p$-value $< 0.05$.
- **Phát hiện Concept Drift (Target Drift)**:
  - So sánh trực tiếp phân phối sản lượng tiêu thụ thực tế `daily_demand` giữa kỳ đối chứng lịch sử và kỳ gần nhất để phát hiện những thay đổi hành vi tiêu dùng đột ngột.
- **Xuất bản Báo cáo Trực quan & Dữ liệu máy đọc**:
  - Báo cáo HTML độc lập: `data/monitoring_reports/data_drift_report.html` với giao diện trực quan, bảng chi tiết từng đặc trưng và trạng thái `DRIFT` vs `STABLE`.
  - Tệp tóm tắt: `data/monitoring_reports/drift_summary.json` cung cấp chỉ số `drift_share` phục vụ cho API `/metrics` và bộ kích hoạt tái huấn luyện.

---

### 4. Cơ chế Tự động Kích hoạt Tái huấn luyện Khép kín (`monitoring/evidently/trigger_retraining.py`)
- Giải quyết trực tiếp khuyết điểm kiến trúc đã ghi nhận trong đề tài (`docs/architecture.md` mục 3 & 4):
  - Khi tỷ lệ đặc trưng bị trôi dạt vượt ngưỡng ($\text{drift\_share} \ge 30\%$) hoặc phát hiện Target Drift:
  - Hệ thống tự động kích hoạt quy trình tái huấn luyện mô hình Champion với cửa sổ dữ liệu được cập nhật.
  - Tự động nâng cấp phiên bản mô hình trong [`data/model_manifest.json`](file:///c:/Users/MINH/Downloads/temp-extract/streaming-mlops-ecommerce/data/model_manifest.json) (từ Version 1 lên Version 2, 3...) với nhãn `RETRAINED_DUE_TO_DRIFT`.
  - Xuất bản tệp nhật ký kiểm toán tái huấn luyện tại `data/monitoring_reports/retraining_log.json`.

---

### 5. Kiểm thử Tự động & Hướng dẫn Vận hành
- `tests/test_monitoring.py`: Bộ 8 unit tests kiểm định thuật toán KS-test, PSI, bộ phát hiện drift, quy trình Closed-Loop Retrainer, và định dạng Prometheus exposition của `/metrics` (toàn bộ 42 tests của dự án đều **OK**).
- `docs/how-to-run-week8.md`: Tài liệu hướng dẫn truy cập Prometheus Web UI (`localhost:9090`), Grafana Dashboard (`localhost:3000`), chạy kiểm định drift và xem báo cáo HTML.

---

## Bảng tổng hợp các chỉ số giám sát và ngưỡng cảnh báo

| Tên Chỉ số (Metric) | Nguồn thu thập | Ngưỡng an toàn (Healthy) | Ngưỡng báo động (Alert) | Hành động kích hoạt |
|---|---|:---:|:---:|---|
| **Data Drift Ratio** | Evidently AI / KS-Test | $< 20\%$ | $\ge 30\%$ | Kích hoạt tự động Closed-Loop Retraining |
| **Target Drift (Concept)** | Evidently AI | $p \ge 0.05$ | $p < 0.05$ | Đánh dấu suy giảm mô hình, cảnh báo retrain |
| **Critical Stock Count** | FastAPI `/metrics` | $0$ SKU | $> 0$ SKU | Báo động đỏ trên Grafana, ưu tiên tạo đơn mua |
| **Warning Stock Count** | FastAPI `/metrics` | $< 5$ SKUs | $\ge 5$ SKUs | Cảnh báo vàng, lập kế hoạch nhập hàng tuần |
| **API Request Latency** | Prometheus | P95 $< 50\text{ms}$ | P95 $> 200\text{ms}$ | Cảnh báo hiệu năng container serving |

---

## Sản phẩm bàn giao
1. `monitoring/prometheus/prometheus.yml`: File cấu hình Prometheus Scraper.
2. `monitoring/grafana/provisioning/datasources/prometheus.yml`: Cấu hình datasource Grafana.
3. `monitoring/grafana/provisioning/dashboards/dashboards.yml`: Cấu hình nạp dashboard tự động.
4. `monitoring/grafana/provisioning/dashboards/ecommerce_mlops_dashboard.json`: Dashboard Grafana hoàn chỉnh.
5. `monitoring/evidently/drift_detector.py`: Module kiểm định Data Drift & Concept Drift.
6. `monitoring/evidently/trigger_retraining.py`: Module Closed-Loop Retraining tự động.
7. `serving/app/main.py`: Cập nhật endpoint `/metrics` định dạng Prometheus Text 0.0.4.
8. `docker-compose.yml`: Kích hoạt Prometheus (`9090`) và Grafana (`3000`).
9. `tests/test_monitoring.py`: Bộ unit tests kiểm thử giám sát.
10. `docs/how-to-run-week8.md`: Hướng dẫn vận hành chi tiết.

---

## Kế hoạch tuần tiếp theo (Tuần 9)
- **Thiết kế Executive Dashboard trên Power BI (`powerbi/`)**:
  - Trực quan hóa ma trận 9 ô ABC/XYZ, bản đồ nhiệt tồn kho toàn quốc và đối chiếu doanh thu Actual vs Forecast.
- **Kiểm thử tải và độ bền hệ thống (Load Testing) với Locust / K6**:
  - Giả lập tải đồng thời từ 100 đến 1,000 người dùng gọi liên tục vào các endpoint `/predict/demand` và `/inventory/reorder-alert`.
  - Đo lường thông lượng (Throughput RPS) và độ trễ P95/P99 để khẳng định tính sẵn sàng phục vụ quy mô lớn.

---

## Minh bạch sử dụng AI
- **Phần AI hỗ trợ**: Hỗ trợ thiết kế cấu trúc JSON Dashboard của Grafana theo schema v38; lập trình công thức tiệm cận Kolmogorov-Smirnov CDF; xây dựng template báo cáo HTML responsive với Bootstrap 5.
- **Phần sinh viên tự thực hiện**: Thiết lập logic ngưỡng báo động Data Drift ($\ge 30\%$) phù hợp với đặc thù biến động ngành E-Commerce; thiết kế quy trình closed-loop nâng cấp phiên bản mô hình trong manifest; thẩm định kết quả kiểm tra độ lệch phân phối trên các biến trễ (lags).
