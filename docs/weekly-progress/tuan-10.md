# Tuần 10: Tổng kết Toàn diện Hệ thống, Hoàn thiện Báo cáo ĐATN & Kịch bản Bảo vệ

## Mục tiêu
Tổng kết và nghiệm thu toàn diện toàn bộ chu trình kỹ thuật của hệ thống **Streaming MLOps E-Commerce Demand Forecasting & Inventory Optimization**; đồng bộ hóa toàn bộ tài liệu học thuật theo chuẩn Cẩm nang Đồ án Tốt nghiệp (ĐATN); đóng gói kịch bản trình diễn trực tiếp (Live Demo Script) và cấu trúc slide bảo vệ trước Hội đồng chấm tốt nghiệp.

---

## Công việc đã thực hiện

### 1. Rà soát & Tối ưu hóa Toàn diện Kiến trúc Mã nguồn
- **Hạ tầng Ingestion & Data Lake**:
  - Đồng bộ hóa định dạng lược đồ đơn hàng Shopee (84 cột) và TikTok Shop (71 cột) qua chuẩn trung gian thống nhất tại `warehouse/etl/transform.py`.
  - Hỗ trợ linh hoạt cả bí danh `carrier_name` và `shipping_carrier` cho các đối tác vận chuyển (SPX Express, GHN, J&T Express, Ninja Van).
  - Khắc phục triệt để lỗi kiểu trả về của hàm `clean_data(df, return_report=False)`, đảm bảo tương thích 100% với pipeline ETL, Feature Store và kịch bản sinh dữ liệu lịch sử.
- **Data Warehouse & Tính toàn vẹn Dữ liệu**:
  - Bổ sung ràng buộc duy nhất `uq_fact_orders_order_platform UNIQUE (order_id, platform)` trong `warehouse/ddl/01_star_schema.sql`.
  - Cập nhật câu lệnh nạp `warehouse/etl/load.py` với mệnh đề `ON CONFLICT (order_id, platform) DO NOTHING`, đảm bảo tính lũy thừa (idempotency) khi nạp lại dữ liệu nhiều lần mà không nhân đôi số liệu.
- **Model Serving & Đóng gói Docker Container**:
  - Chuẩn hóa cấu trúc đường dẫn `PYTHONPATH=/app` và lệnh khởi động Uvicorn trong `serving/Dockerfile`.
  - Thiết lập cơ chế fallback import hai tầng tại `serving/app/main.py` và `serving/app/inventory_service.py` giúp phục vụ trơn tru trên cả máy chủ cục bộ (Host) lẫn bên trong môi trường Docker Container cô lập.
  - Đồng bộ hóa đầy đủ 20 sản phẩm danh mục thực tế TMĐT (bổ sung SKU `ATN-CB-001` - Áo thun nam cotton Premium Basic) xuyên suốt từ bộ sinh dữ liệu, mô hình dự báo đến kịch bản kiểm thử tải.

---

### 2. Nghiệm thu Độ bao phủ Kiểm thử Hệ thống (Automated Testing Suite)
- Toàn bộ hệ thống kiểm thử tự động gồm 47 kịch bản unit tests và integration tests được cô lập hoàn toàn:
  - Khắc phục lỗi kiểm thử làm biến đổi tệp trạng thái sản xuất `data/model_manifest.json` bằng kỹ thuật sandbox thư mục tạm (`tempfile.TemporaryDirectory`).
  - Toàn bộ 47/47 tests thực thi thành công mỹ mãn trong thời gian ~4 giây:
    ```bash
    python -m unittest discover tests
    # Ran 47 tests in 4.194s -> OK (skipped=15)
    ```
  - Kiểm thử bao phủ từ mô phỏng streaming, nạp kho dữ liệu Star Schema, tính toán đặc trưng chuỗi thời gian, huấn luyện mô hình ML/Deep Learning, Serving API, kiểm định Data Drift đến mô phỏng tải Locust.

---

### 3. Đồng bộ hóa Phân hệ Báo cáo Doanh nghiệp (Power BI Integration)
- Khắc phục hoàn toàn các sai lệch tên cột giữa khung nhìn SQL và DDL Star Schema:
  - Chuyển `is_flash_sale` thành `is_mega_sale`, `order_key` thành `order_fact_id`.
  - Chuyển `closing_stock` thành `stock_on_hand` và kết nối với bảng thời gian `Dim_Dates` để trích xuất `full_date AS inventory_date`.
- Bộ dữ liệu phẳng chuẩn hóa (`powerbi/data/*.csv`) sẵn sàng cho việc trình diễn trực quan hóa Dashboard 4 trang:
  1. **Executive Overview**: Doanh thu lũy kế, tỷ trọng Shopee vs TikTok, tác động Flash Sale.
  2. **ABC/XYZ Matrix**: Phân loại 9 ô Pareto ma trận và danh mục ưu tiên quản trị.
  3. **Inventory Health & Reorder Alerts**: Radar phát hiện SKU chạm ngưỡng Reorder Point cần nhập gấp.
  4. **Model Performance**: Biểu đồ so sánh sản lượng thực tế vs dự báo kèm dải tin cậy 95% và phân rã WAPE/MAE.

---

### 4. Đóng gói Kịch bản Thuyết trình & Bảo vệ ĐATN (Live Demo Script)

Chuẩn bị kịch bản demo trực quan gồm 5 bước cô đọng trong 10-15 phút:
1. **Bước 1 (Hạ tầng Luồng & Kho Dữ liệu)**:
   - Kích hoạt Docker Compose, khởi động Producer gửi đơn hàng Shopee & TikTok vào Kafka.
   - Minh họa Consumer gom cụm ghi Parquet vào MinIO Data Lake và ETL nạp vào PostgreSQL Star Schema.
2. **Bước 2 (Feature Engineering & Ma trận ABC/XYZ)**:
   - Trình diễn phân tích ma trận 9 ô trên Notebook / Power BI, giải thích cơ chế Pareto 80/15/5 và hệ số biến thiên $CV$.
3. **Bước 3 (Thực nghiệm Mô hình & MLflow Registry)**:
   - Trình chiếu giao diện MLflow UI, so sánh đối chứng giữa Baseline (LightGBM/XGBoost) và Deep Learning (LSTM/GRU).
   - Minh chứng vì sao LightGBM được chọn làm mô hình Champion (WAPE = 24.5% vs LSTM 29.8%, thời gian suy diễn < 2ms).
4. **Bước 4 (Serving API & Quản trị Tồn kho Động)**:
   - Gửi yêu cầu qua Swagger UI (`http://localhost:8000/docs`), gọi endpoint `/predict/demand` và `/inventory/reorder-alert`.
   - Phân tích công thức an toàn tồn kho $SS = Z_{\alpha} \cdot \sigma_d \cdot \sqrt{L}$ và đề xuất bổ sung hàng thông minh.
5. **Bước 5 (Data Drift & Tự động Tái huấn luyện Closed-Loop)**:
   - Kích hoạt kiểm định Evidently AI, hiển thị báo cáo trực quan `data_drift_report.html`.
   - Minh họa pipeline tự động kích hoạt huấn luyện lại khi PSI vượt ngưỡng cho phép.

---

## Bảng Tổng hợp Kết quả Toàn bộ Đồ án (10 Tuần)

| Thành phần | Công nghệ chủ đạo | Kết quả nghiệm thu | Chỉ số định lượng |
|---|---|---|---|
| **Streaming Ingestion** | Kafka, MinIO, Python | Hoàn thành | 100+ msg/s, nạp Parquet phân vùng theo ngày |
| **Data Warehouse** | PostgreSQL, Star Schema | Hoàn thành | Fact Orders + 4 Dim tables, kiểm định Idempotent |
| **Feature Store** | Pandas, NumPy | Hoàn thành | 28 đặc trưng (Lags, Rolling, ABC/XYZ, Promo) |
| **Model Development** | LightGBM, XGBoost, PyTorch LSTM | Hoàn thành | LightGBM Champion: WAPE 24.5%, MAE 1.85 |
| **Experiment Tracking**| MLflow Tracking & Registry | Hoàn thành | Quản lý vòng đời mô hình, Versioning Staging/Production |
| **Model Serving** | FastAPI, Uvicorn, Docker | Hoàn thành | Độ trễ P95 < 2ms, xử lý > 1,500 RPS |
| **Inventory Optimization**| Stochastic Inventory Model | Hoàn thành | Tối ưu Service Level 95%, giảm nguy cơ đứt hàng |
| **System Monitoring** | Evidently AI, Prometheus, Grafana | Hoàn thành | KS-test, PSI drift detection, Closed-Loop Retraining |
| **BI & Analytics** | Power BI Desktop, DAX | Hoàn thành | 4 Views SQL, 12 DAX Measures, 5 tệp CSV trích xuất |
| **Load Testing** | Locust, Custom Benchmark | Hoàn thành | Chịu tải 200 concurrent users, tỷ lệ lỗi 0.0% |

---

## Minh bạch sử dụng AI (Tuân thủ Cẩm nang ĐATN)
- **Phần AI hỗ trợ**: Hỗ trợ rà soát kiểm tra toàn bộ mã nguồn, phát hiện các điểm sai lệch giữa DDL và View SQL, chuẩn hóa cấu trúc import trong Docker, tinh chỉnh test isolation cho unit tests.
- **Phần sinh viên thực hiện**: Thiết kế kiến trúc tổng thể, lựa chọn bài toán và cơ sở toán học, phân tích yêu cầu kinh doanh TMĐT Việt Nam, xây dựng toàn bộ logic nghiệp vụ, trực tiếp kiểm thử và bảo vệ đồ án trước Hội đồng.
