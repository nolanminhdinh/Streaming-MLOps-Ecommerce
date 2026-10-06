# CẤU TRÚC BỘ SLIDE THUYẾT TRÌNH BẢO VỆ ĐATN
## HỆ THỐNG STREAMING MLOPS DỰ BÁO NHU CẦU & TỐI ƯU HÓA TỒN KHO TMĐT ĐA KÊNH

> **Khuyến nghị định dạng**: 18 – 20 slide trình chiếu PowerPoint / Canva / Marp.  
> **Thời gian thuyết trình**: 15 – 20 phút (trước khi chuyển sang 10 phút Live Demo và Q&A).

---

### SLIDE 1: TRANG TIÊU ĐỀ (TITLE SLIDE)
- **Tên đề tài**: Xây dựng kiến trúc hạ tầng dữ liệu luồng (Streaming Pipeline) và MLOps phục vụ dự báo nhu cầu hàng hóa trong Thương mại điện tử Việt Nam.
- **Học viên / Sinh viên thực hiện**: [Họ và tên sinh viên] - [MSSV]
- **Giảng viên hướng dẫn**: [Họ và tên GVHD]
- **Đơn vị đào tạo**: Khoa Công nghệ Thông tin / Chuyên ngành Kỹ thuật Dữ liệu & Trí tuệ Nhân tạo.
- **Thời gian**: Tháng 09/2026.

---

### SLIDE 2: ĐẶT VẤN ĐỀ & TÍNH CẤP THIẾT CỦA ĐỀ TÀI
- **Bối cảnh TMĐT Việt Nam**: Bùng nổ mua sắm đa kênh (Shopee, TikTok Shop), đặc biệt là hình thức Live Commerce và Flash Sale ngày đôi (9/9, 10/10, 11/11, 12/12).
- **Vấn đề cốt lõi của doanh nghiệp bán lẻ TMĐT**:
  1. *Hiệu ứng Roi da (Bullwhip Effect)*: Biến động đột biến trong Flash Sale làm tê liệt chuỗi cung ứng.
  2. *Thách thức kép*: Chi phí mất doanh thu do đứt hàng (Stockout) vs Chi phí chôn vốn tồn kho (Holding cost).
  3. *Độ trễ cung ứng (Lead Time)*: Hàng nhập/sản xuất mất 2–7 ngày, phương pháp đặt hàng tĩnh $Min-Max$ không còn hiệu quả.

---

### SLIDE 3: MỤC TIÊU VÀ ĐÓNG GÓP CỦA ĐỀ TÀI
- **Mục tiêu kỹ thuật**:
  - Xây dựng hạ tầng thu thập và xử lý luồng dữ liệu thời gian thực (Kafka -> MinIO -> Data Warehouse).
  - Tự động hóa trích xuất 28 đặc trưng chuỗi thời gian và phân loại ma trận 9 ô ABC/XYZ.
  - Xây dựng mô hình máy học dự báo nhu cầu chính xác cao (LightGBM/XGBoost vs Deep Learning LSTM/GRU).
  - Triển khai FastAPI Serving kèm logic quản trị tồn kho ngẫu nhiên (Safety Stock & Reorder Point).
  - Khép kín vòng lặp MLOps: Tự động giám sát Data Drift và tái huấn luyện mô hình.
  - Trực quan hóa Power BI Dashboard và kiểm thử khả năng chịu tải cao.

---

### SLIDE 4: TỔNG QUAN KIẾN TRÚC TOÀN HỆ THỐNG
- Sơ đồ phân tầng rõ nét:
  - **Tầng 1 (Ingestion)**: Data Simulator -> Kafka Cluster -> MinIO Bronze Data Lake.
  - **Tầng 2 (Warehouse & ETL)**: ETL Engine -> PostgreSQL Star Schema (Fact Orders & Dim Tables).
  - **Tầng 3 (Feature Store & Modeling)**: ABC/XYZ Pareto, Lags, Rolling -> MLflow Tracking & Registry.
  - **Tầng 4 (Serving & Inventory Logic)**: FastAPI REST API -> Stochastic Inventory Optimization.
  - **Tầng 5 (Monitoring & Retraining)**: Prometheus, Grafana, Evidently AI Drift Detection -> Closed-Loop Trigger.
  - **Tầng 6 (Analytics & Load Testing)**: Power BI Desktop Executive Dashboard & Locust Load Testing.

---

### SLIDE 5: HẠ TẦNG DỮ LIỆU LUỒNG (STREAMING INGESTION)
- **Thách thức**: Lược đồ đơn hàng Shopee (84 cột) và TikTok Shop (71 cột) có sự bất đồng bộ lớn về tên trường, kiểu dữ liệu, thông tin vận chuyển và đơn vị tiền tệ.
- **Giải pháp**:
  - Kafka Topic `ecom.orders.raw` và `inventory.logs` với cấu trúc JSON bán cấu trúc.
  - Kafka Consumer thực hiện gom cụm (Micro-batching) theo cửa sổ thời gian hoặc dung lượng (100 thông điệp).
  - Ghi trực tiếp vào MinIO Data Lake dưới định dạng Apache Parquet phân vùng theo ngày tháng, giảm 70% dung lượng so với JSON/CSV thuần.

---

### SLIDE 6: KHO DỮ LIỆU STAR SCHEMA & TÍNH LŨY THỪA (IDEMPOTENT ETL)
- **Mô hình Star Schema**:
  - `Fact_Orders`: Bảng dữ liệu sự thật lưu trữ từng mục hàng trong đơn.
  - 4 bảng chiều chuẩn hóa: `Dim_Products`, `Dim_Dates`, `Dim_Platforms`, `Dim_Geography`.
- **Cơ chế Idempotent ETL**:
  - Ràng buộc toàn vẹn: `uq_fact_orders_order_platform UNIQUE (order_id, platform)`.
  - Mệnh đề nạp an toàn: `ON CONFLICT DO NOTHING`, bảo đảm khi ETL chạy lại không gây nhân đôi số liệu doanh thu.

---

### SLIDE 7: FEATURE ENGINEERING & MA TRẬN 9 Ô ABC/XYZ
- **Cơ sở lý thuyết**:
  - Phân tích Pareto 80/15/5 theo doanh thu lũy kế: Nhóm A (80%), Nhóm B (15%), Nhóm C (5%).
  - Hệ số biến thiên nhu cầu: $CV = \frac{\sigma_d}{\mu_d}$ chia làm X ($CV \le 0.5$), Y ($0.5 < CV \le 1.0$), Z ($CV > 1.0$).
- **Chiến lược kết hợp 9 ô**:
  - *AX, AY*: Nhu cầu lớn, ổn định $\to$ Giữ tỷ trọng dự báo tự động, đặt hàng định kỳ.
  - *AZ, BZ*: Doanh thu lớn nhưng biến động mạnh $\to$ Thiết lập tồn kho an toàn động linh hoạt cao.
  - *CX, CY, CZ*: Doanh thu thấp $\to$ Quản lý tồn kho tinh gọn, tránh đọng vốn.
- **Feature Store**: 28 đặc trưng (Lags 1/7/14/28, Rolling mean/std 7/14/30, Cyclical Day-of-Week sin/cos, cờ Flash Sale).

---

### SLIDE 8: THỰC NGHIỆM MÔ HÌNH: BASELINE VS DEEP LEARNING
- **Phương pháp thực nghiệm**: Walk-Forward Time Series Split (5 folds), tránh rò rỉ dữ liệu tương lai (Data Leakage).
- **Bảng so sánh kết quả thực nghiệm**:
  | Thuật toán | WAPE (%) | MAE | RMSE | Thời gian huấn luyện | Độ trễ suy diễn (ms) |
  |---|---|---|---|---|---|
  | Naive / Moving Avg | 45.2% | 3.65 | 5.12 | < 1s | < 0.1ms |
  | XGBoost Default | 28.1% | 2.15 | 3.05 | 12s | 1.8ms |
  | **LightGBM Tuned (Optuna)** | **24.5%** | **1.85** | **2.60** | **8s** | **1.2ms** |
  | PyTorch LSTM (Deep Learning) | 29.8% | 2.30 | 3.25 | 145s | 8.5ms |
  | PyTorch GRU | 28.9% | 2.22 | 3.14 | 120s | 7.2ms |

---

### SLIDE 9: LUẬN GIẢI CHỌN MÔ HÌNH CHAMPION
- **Tại sao chọn LightGBM Tuned làm mô hình triển khai chính thức?**
  1. *Độ chính xác cao nhất*: Đạt WAPE 24.5% (thấp nhất trong tất cả các mô hình khảo nghiệm).
  2. *Thời gian suy diễn cực thấp*: Chỉ 1.2ms cho mỗi yêu cầu dự báo (nhanh gấp 7 lần mạng nơ-ron sâu LSTM).
  3. *Khả năng học tương tác phi tuyến*: Bắt trọn vẹn đặc trưng hiệu ứng Flash Sale và chu kỳ tuần.
  4. *Quản lý vòng đời qua MLflow*: Tự động đăng ký vào Model Registry ở trạng thái `Production`.

---

### SLIDE 10: TỐI ƯU HÓA TỒN KHO ĐỘNG (DYNAMIC INVENTORY OPTIMIZATION)
- **Mô hình Tồn kho Ngẫu nhiên (Stochastic Inventory Control)**:
  - Tồn kho an toàn động (Safety Stock):
    $$SS = Z_{\alpha} \cdot \sigma_d \cdot \sqrt{L}$$
    *(với $Z_{\alpha} = 1.645$ tương ứng Service Level 95%, $L = 3$ ngày Lead Time, $\sigma_d$ độ lệch chuẩn nhu cầu thời gian thực)*.
  - Điểm đặt hàng lại (Reorder Point):
    $$ROP = (\mu_d \cdot L) + SS$$
- **Cơ chế cảnh báo 3 cấp độ**:
  - `CRITICAL`: Tồn kho thực tế $\le ROP$ $\to$ Kích hoạt đặt hàng khẩn cấp.
  - `WARNING`: Tồn kho $\le 1.2 \cdot ROP$ $\to$ Theo dõi sát sao.
  - `NORMAL`: Tồn kho đảm bảo an toàn.

---

### SLIDE 11: MÔ HÌNH PHỤC VỤ FASTAPI & CONTAINER HÓA
- Xây dựng REST API chuẩn OpenAPI/Swagger:
  - `POST /predict/demand`: Dự báo nhu cầu 7–14 ngày cho từng SKU kèm khoảng tin cậy.
  - `GET & POST /inventory/reorder-alert`: Quét và xuất danh sách các mặt hàng có nguy cơ đứt hàng.
  - `GET /health` & `GET /model/metadata`: Giám sát sức khỏe dịch vụ và thông tin phiên bản mô hình đang phục vụ.
- **Docker hóa toàn diện**: Cấu hình `PYTHONPATH`, chạy qua máy chủ ASGI `uvicorn` hỗ trợ đa luồng.

---

### SLIDE 12: GIÁM SÁT DATA DRIFT & VÒNG LẶP RETRAINING KHÉP KÍN
- **Thách thức trôi dạt trong TMĐT**:
  - Thói quen mua sắm thay đổi đột ngột giữa mùa thường và mùa siêu khuyến mãi (Mega Sale).
- **Giải pháp Evidently AI**:
  - Kiểm định Kolmogorov-Smirnov (KS-test) cho các biến liên tục.
  - Chỉ số độ ổn định quần thể (Population Stability Index - PSI).
  - Tự động sinh báo cáo HTML trực quan hóa phân phối biến (`data_drift_report.html`).
- **Closed-Loop Retraining**:
  - Khi tỷ lệ đặc trưng trôi dạt vượt quá ngưỡng (Drift Share > 0.3), hệ thống tự động kích hoạt pipeline huấn luyện lại trên tập dữ liệu mới, cập nhật `data/model_manifest.json` và thăng cấp Version 2 không gián đoạn dịch vụ.

---

### SLIDE 13: BÁO CÁO KINH DOANH ĐIỀU HÀNH POWER BI
- Kết nối trực tiếp vào Data Warehouse thông qua 4 Views SQL chuyên biệt:
  - `vw_powerbi_executive_summary`
  - `vw_powerbi_abc_xyz_matrix`
  - `vw_powerbi_inventory_health`
  - `vw_powerbi_geographic_sales`
- Hệ thống 12 công thức DAX nghiệp vụ tính toán động WAPE %, Forecast Accuracy %, Reorder Point Dynamic.
- Xuất khẩu 5 tệp dữ liệu phẳng CSV sẵn sàng cho phân tích độc lập.

---

### SLIDE 14: THỰC NGHIỆM KIỂM THỬ TẢI & KHẢ NĂNG CHỊU LỖI
- **Kịch bản kiểm thử Locust**:
  - Kiểm thử các mức tải đồng thời: 10, 50, 100 và 200 concurrent users.
- **Kết quả đo lường**:
  - Thông lượng tối đa: **> 1,500 requests/giây (RPS)**.
  - Độ trễ phản hồi trung bình: **0.13ms - 0.45ms**.
  - Độ trễ phân vị P95: **< 2ms**.
  - Tỷ lệ lỗi (Error Rate): **0.0%** trên toàn bộ các mức tải.
  - Chứng minh hệ thống sẵn sàng đáp ứng lưu lượng truy cập cao trong các khung giờ Flash Sale cao điểm.

---

### SLIDE 15: KIỂM THỬ TỰ ĐỘNG (UNIT TESTING & QA)
- **Độ bao phủ**: 47 kịch bản kiểm thử tự động trải dài trên 8 modules:
  - `test_data_simulator.py`: Kiểm định quy luật sinh đơn Shopee/TikTok.
  - `test_etl.py`: Kiểm tra schema unification, data cleansing và nạp kho.
  - `test_features.py`: Kiểm tra tính toán lags, rolling, ma trận ABC/XYZ.
  - `test_ml_pipeline.py` & `test_deep_learning.py`: Kiểm tra huấn luyện mô hình.
  - `test_serving.py`: Kiểm tra API endpoints và logic Reorder Point.
  - `test_monitoring.py`: Kiểm tra phát hiện drift và trigger retraining (được cô lập hoàn toàn).
  - `test_load_test.py`: Kiểm tra thuật toán đo phân vị và dữ liệu Power BI.
- Kết quả: **47/47 tests PASS (100% OK)**.

---

### SLIDE 16: TỔNG KẾT ĐÁNH GIÁ MỨC ĐỘ HOÀN THÀNH
- Đã hoàn thành 100% các mục tiêu đề ra trong đề cương 10 tuần.
- Xây dựng thành công chu trình khép kín hoàn chỉnh từ Streaming Ingestion đến Executive Dashboard.
- Đáp ứng đầy đủ các yêu cầu khắt khe của Cẩm nang Đồ án Tốt nghiệp (ĐATN).

---

### SLIDE 17: HƯỚNG PHÁT TRIỂN TRONG TƯƠNG LAI
1. Mở rộng tích hợp trực tiếp qua Shopee Open API và TikTok Shop Partner Center API bằng kết nối Webhook thật.
2. Nâng cấp mô hình dự báo lên kiến trúc Transformer (Temporal Fusion Transformer - TFT) khi có dữ liệu lịch sử > 1 năm.
3. Triển khai phân cụm hạ tầng trên Kubernetes (K8s) với autoscaling pod phục vụ và Kafka multi-broker cluster.

---

### SLIDE 18: LỜI CẢM ƠN & PHẦN HỎI ĐÁP (Q&A)
- Kính gửi lời tri ân sâu sắc đến Thầy/Cô Hướng dẫn và Hội đồng Chấm Đồ án Tốt nghiệp.
- Mời Thầy/Cô theo dõi phần **Live Demo Trực tiếp Hệ thống**.
