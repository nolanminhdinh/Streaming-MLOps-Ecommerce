# KỊCH BẢN TRÌNH DIỄN TRỰC TIẾP TRƯỚC HỘI ĐỒNG BẢO VỆ ĐATN
## (LIVE DEMO SCRIPT - STREAMING MLOPS E-COMMERCE)

> **Thời lượng khuyến nghị**: 10 – 15 phút.  
> **Mục tiêu**: Chứng minh tính thực tế, độ trơn tru và tính khép kín của toàn bộ kiến trúc từ Streaming Ingestion -> Data Lakehouse -> Feature Store -> Machine Learning -> FastAPI Serving -> Dynamic Inventory -> Closed-loop Drift Retraining -> Power BI Dashboard.

---

### PHẦN CHUẨN BỊ TRƯỚC KHI BẢO VỆ (PRE-DEMO CHECKLIST)

1. Mở sẵn các cửa sổ trình duyệt:
   - **Tab 1**: FastAPI Swagger UI: `http://localhost:8000/docs`
   - **Tab 2**: MinIO Console: `http://localhost:9001` (user/pass: `minioadmin` / `minioadmin`)
   - **Tab 3**: Grafana Dashboard: `http://localhost:3000` (user/pass: `admin` / `admin123`)
   - **Tab 4**: Báo cáo Data Drift HTML: mở tệp `data/monitoring_reports/data_drift_report.html`
   - **Tab 5**: Power BI Desktop đã mở sẵn Dashboard tổng hợp từ `powerbi/data/*.csv`.
2. Mở sẵn 2 Terminal:
   - **Terminal 1**: Dành cho lệnh hệ thống và Docker (`docker compose ps`).
   - **Terminal 2**: Dành cho kịch bản chạy lệnh tương tác.

---

### BƯỚC 1: TRÌNH DIỄN HẠ TẦNG LUỒNG DỮ LIỆU THỜI GIAN THỰC (2 phút)

**Lời dẫn**:
> *"Kính thưa Thầy Cô Hội đồng, đầu tiên em xin trình diễn tầng Streaming Ingestion. Dữ liệu đơn hàng thực tế từ hai sàn TMĐT Shopee và TikTok Shop có cấu trúc lược đồ rất khác biệt (Shopee 84 cột, TikTok 71 cột) và xuất hiện theo luồng bất đồng bộ."*

**Thao tác**:
```bash
# 1. Kích hoạt sinh luồng đơn hàng giả lập Shopee & TikTok
python data_simulator/data_simulator.py --rate 10 --duration 15

# 2. Quan sát Kafka Producer phát thông điệp và Consumer gom cụm ghi vào MinIO Bronze Lake
python ingestion/consumer_to_minio.py
```

**Điểm nhấn cần chỉ ra cho Hội đồng**:
- Chỉ vào MinIO Console (`http://localhost:9001`): Cho thấy bucket `raw-orders` đã xuất hiện các tệp Parquet được phân vùng theo thời gian thực `year=YYYY/month=MM/day=DD/`.
- Giải thích: Kiến trúc tách biệt rõ ràng tầng lưu trữ Data Lake Bronze giúp bảo toàn nguyên vẹn dữ liệu gốc trước khi qua ETL.

---

### BƯỚC 2: TIỀN XỬ LÝ DỮ LIỆU & DATA WAREHOUSE STAR SCHEMA (2 phút)

**Lời dẫn**:
> *"Từ Data Lake, dữ liệu thô được đưa qua pipeline ETL để làm sạch, chuẩn hóa các trường thông tin không đồng nhất về đơn vị tiền tệ, trạng thái đơn và ánh xạ vào mô hình Star Schema trên PostgreSQL."*

**Thao tác**:
```bash
# Thực thi pipeline ETL nạp dữ liệu vào Data Warehouse
python warehouse/etl/pipeline.py
```

**Điểm nhấn cần chỉ ra cho Hội đồng**:
- Bảng sự thật `Fact_Orders` liên kết với 4 bảng chiều `Dim_Products`, `Dim_Dates`, `Dim_Platforms`, `Dim_Geography`.
- Cơ chế bảo vệ toàn vẹn: Ràng buộc duy nhất `uq_fact_orders_order_platform` cùng cơ chế `ON CONFLICT DO NOTHING` đảm bảo tính lũy thừa (Idempotent), nạp lại nhiều lần không sinh rác hoặc lệch số liệu tài chính.

---

### BƯỚC 3: FEATURE STORE & PHÂN HẠNG MA TRẬN 9 Ô ABC/XYZ (2 phút)

**Lời dẫn**:
> *"Để phục vụ cho mô hình máy học, hệ thống trích xuất 28 đặc trưng chuỗi thời gian bao gồm Lags, Rolling Statistics, hiệu ứng Flash Sale và phân khúc ma trận 9 ô ABC/XYZ theo nguyên lý Pareto và độ biến thiên nhu cầu."*

**Thao tác**:
- Mở slide hoặc Notebook [notebooks/abc_xyz_classification.ipynb](notebooks/abc_xyz_classification.ipynb) / Power BI trang ABC/XYZ.
- Chỉ vào 9 ô:
  - **Nhóm AX/AY**: Doanh thu cao, ổn định $\to$ Giữ trọng số lớn, áp dụng mô hình chuỗi thời gian có độ chính xác cao.
  - **Nhóm AZ/BZ**: Doanh thu biến động mạnh theo Flash Sale $\to$ Cần tính toán tồn kho an toàn động linh hoạt.
  - **Nhóm C**: Doanh thu thấp $\to$ Áp dụng quy tắc kiểm soát tồn kho tối thiểu để tối ưu vốn lưu động.

---

### BƯỚC 4: MODEL SERVING FASTAPI & QUẢN TRỊ TỒN KHO THỜI GIAN THỰC (3 phút)

**Lời dẫn**:
> *"Mô hình LightGBM Champion đã qua tinh chỉnh siêu tham số và đạt WAPE 24.5% được đóng gói phục vụ qua FastAPI với độ trễ phản hồi cực thấp (< 2ms). Hệ thống không chỉ trả về con số dự báo nhu cầu đơn thuần mà kết hợp trực tiếp với bài toán tối ưu tồn kho ngẫu nhiên (Stochastic Inventory Control)."*

**Thao tác**:
- Mở Swagger UI (`http://localhost:8000/docs`).
- Thực hiện `POST /predict/demand` với SKU `DRS-MD-001` (Đầm Midi Nữ), horizon 7 ngày. Cho Hội đồng xem JSON kết quả dự báo và dải tin cậy 95%.
- Thực hiện `GET /inventory/reorder-alert`:
  - Cho Hội đồng thấy danh sách SKU được gắn nhãn `CRITICAL` khi tồn kho hiện tại thấp hơn Reorder Point ($ROP$).
  - Giải thích công thức toán học được tính toán động:
    $$SS = Z_{\alpha} \cdot \sigma_d \cdot \sqrt{L}$$
    $$ROP = (\mu_d \cdot L) + SS$$
  - Hệ thống tự động đề xuất số lượng hàng cần đặt bổ sung ngay lập tức (`suggested_reorder_qty`).

---

### BƯỚC 5: GIÁM SÁT DATA DRIFT & VÒNG LẶP RETRAINING CLOSED-LOOP (3 phút)

**Lời dẫn**:
> *"Một thách thức lớn trong MLOps TMĐT là hành vi người dùng thay đổi đột ngột giữa các đợt Mega Sale gây ra Data Drift và Concept Drift. Hệ thống của em tích hợp Evidently AI để tự động phát hiện trôi dạt dữ liệu và kích hoạt tái huấn luyện không cần can thiệp thủ công."*

**Thao tác**:
```bash
# 1. Kích hoạt kiểm định phát hiện trôi dạt dữ liệu
python monitoring/evidently/drift_detector.py

# 2. Chạy cơ chế kích hoạt tái huấn luyện khép kín
python monitoring/evidently/trigger_retraining.py
```

**Điểm nhấn cần chỉ ra cho Hội đồng**:
- Mở tệp `data/monitoring_reports/data_drift_report.html`: Trình chiếu biểu đồ phân phối biến trước và sau trôi dạt bằng kiểm định hai mẫu Kolmogorov-Smirnov và chỉ số Population Stability Index (PSI).
- Xem `data/model_manifest.json`: Cho thấy hệ thống tự động thăng cấp Version mới sau khi tái huấn luyện mà không làm gián đoạn API Serving (Zero-Downtime Hot-Reload).

---

### BƯỚC 6: BÁO CÁO ĐIỀU HÀNH POWER BI & KIỂM THỬ TẢI HỆ THỐNG (2 phút)

**Lời dẫn**:
> *"Cuối cùng, toàn bộ kết quả phân tích và dự báo được kết xuất lên Power BI Executive Dashboard phục vụ ban giám đốc, và hệ thống đã được kiểm thử tải chịu được 200 người dùng đồng thời."*

**Thao tác**:
- Trình chiếu 4 trang Dashboard trên Power BI Desktop:
  1. Executive Summary (Doanh thu & tỷ trọng Shopee/TikTok).
  2. ABC/XYZ Matrix (Ma trận 9 ô phân bổ tồn kho).
  3. Inventory Health (Bản đồ nhiệt cảnh báo đứt hàng).
  4. Model Performance (Độ chính xác và sai số WAPE).
- Trình chiếu báo cáo kiểm thử tải tại `data/load_test_summary.md`:
  - 200 concurrent users: Thông lượng > 1,500 requests/giây.
  - Tỷ lệ lỗi (Error Rate): **0.0%**.
  - P95 Latency: < 2ms.

---

### KẾT LUẬN & CHUẨN BỊ TRẢ LỜI CÂU HỎI HỘI ĐỒNG (1 phút)
> *"Em xin chân thành cảm ơn Quý Thầy Cô trong Hội đồng đã lắng nghe phần trình diễn. Em rất mong nhận được những góp ý quý báu của Thầy Cô để tiếp tục hoàn thiện đề tài!"*

