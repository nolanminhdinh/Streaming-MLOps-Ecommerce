# KỊCH BẢN TRÌNH DIỄN TRỰC TIẾP TRƯỚC HỘI ĐỒNG BẢO VỆ ĐATN
## (LIVE DEMO SCRIPT - STREAMING MLOPS E-COMMERCE)

> **Thời lượng khuyến nghị**: 10 – 15 phút.  
> **Mục tiêu**: Trình diễn các mắt xích đã được khởi tạo và nêu rõ nguồn dữ liệu/model ở từng bước. Chỉ gọi là demo end-to-end khi `/ready` xanh và response provenance xác nhận warehouse + ML model.

---

### PHẦN CHUẨN BỊ TRƯỚC KHI BẢO VỆ (PRE-DEMO CHECKLIST)

1. Mở sẵn các cửa sổ trình duyệt:
   - **Tab 1**: FastAPI Swagger UI: `http://localhost:8000/docs`
   - **Tab 2**: MinIO Console: `http://localhost:9001` (user/pass: `minioadmin` / `minioadmin`)
   - **Tab 3**: Grafana Dashboard: `http://localhost:3000` (user/pass: `admin` / `admin123`)
   - **Tab 4**: Báo cáo Data Drift HTML: mở tệp `data/monitoring_reports/data_drift_report.html`
   - **Tab 5**: Power BI Desktop đã mở sẵn Dashboard tổng hợp từ `powerbi/data/*.csv`.
2. Mở 4 Terminal: một để xem Docker; ba terminal để chạy order consumer, inventory consumer và producer.

---

### BƯỚC 1: TRÌNH DIỄN HẠ TẦNG LUỒNG DỮ LIỆU THỜI GIAN THỰC (2 phút)

**Lời dẫn**:
> *"Kính thưa Thầy Cô Hội đồng, đầu tiên em xin trình diễn tầng Streaming Ingestion. Dữ liệu đơn hàng thực tế từ hai sàn TMĐT Shopee và TikTok Shop có cấu trúc lược đồ rất khác biệt (Shopee 84 cột, TikTok 71 cột) và xuất hiện theo luồng bất đồng bộ."*

**Thao tác** (khởi động consumer trước producer; giữ 2 consumer chạy):
```bash
# Terminal 1: Kafka orders -> MinIO Parquet
python ingestion/consumer_to_minio.py

# Terminal 2: inventory snapshots -> PostgreSQL Fact_Inventory_Daily
python ingestion/consumer_inventory_to_postgres.py

# Terminal 3: simulator -> Kafka orders + inventory; Ctrl+C khi đủ dữ liệu
python ingestion/producer.py
```

Sau khi dừng producer, đợi cả consumer flush batch cuối. `data_simulator.py` chạy riêng chỉ in demo cục bộ, không đẩy sự kiện vào Kafka.

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
- Mở slide hoặc Notebook [abc_xyz_classification.ipynb](../../notebooks/abc_xyz_classification.ipynb) / Power BI trang ABC/XYZ.
- Chỉ vào 9 ô:
  - **Nhóm AX/AY**: Doanh thu cao, ổn định $\to$ Giữ trọng số lớn, áp dụng mô hình chuỗi thời gian có độ chính xác cao.
  - **Nhóm AZ/BZ**: Doanh thu biến động mạnh theo Flash Sale $\to$ Cần tính toán tồn kho an toàn động linh hoạt.
  - **Nhóm C**: Doanh thu thấp $\to$ Áp dụng quy tắc kiểm soát tồn kho tối thiểu để tối ưu vốn lưu động.

---

### BƯỚC 4: MODEL SERVING FASTAPI & QUẢN TRỊ TỒN KHO THỜI GIAN THỰC (3 phút)

**Lời dẫn**:
> *"Tiếp theo em kiểm tra trạng thái model, nguồn dữ liệu lịch sử và kết quả tính tồn kho. Em chỉ báo cáo metric và latency đã đo trong lần chạy này."*

**Thao tác**:
- Mở Swagger UI (`http://localhost:8000/docs`).
- Kiểm tra `/health` và `/ready`; nếu `/ready` chưa trả 200 thì chưa trình bày API là sẵn sàng.
- Thực hiện `POST /predict/demand` với `sku`, `from_date`, `to_date`, `confidence_interval`. Cho Hội đồng xem `model_source` và `history_source`; heuristic fallback không phải model ML.
- Thực hiện `GET /inventory/reorder-alert`:
  - Cho Hội đồng thấy danh sách SKU được gắn nhãn `CRITICAL` khi tồn kho hiện tại thấp hơn Reorder Point ($ROP$).
  - Giải thích công thức toán học được tính toán động:
    $$SS = Z_{\alpha} \cdot \sigma_d \cdot \sqrt{L}$$
    $$ROP = (\mu_d \cdot L) + SS$$
- Hệ thống đề xuất số lượng bổ sung (`recommended_reorder_qty`) và ghi `stock_source`, `demand_source`.

---

### BƯỚC 5: GIÁM SÁT DATA DRIFT & VÒNG LẶP RETRAINING CLOSED-LOOP (3 phút)

**Lời dẫn**:
> *"Detector so sánh feature store với cửa sổ gần nhất. Chỉ summary drift từ dữ liệu thật mới đủ điều kiện kích hoạt retraining; dữ liệu drift tổng hợp dùng để minh họa không tự cập nhật model."*

**Thao tác**:
```bash
# 1. Kích hoạt kiểm định phát hiện trôi dạt dữ liệu
python monitoring/evidently/drift_detector.py

# 2. Chạy coordinator; chỉ có hiệu lực nếu summary dùng dữ liệu thật và vượt ngưỡng
python monitoring/evidently/trigger_retraining.py
```

**Điểm nhấn cần chỉ ra cho Hội đồng**:
- Mở `data/monitoring_reports/data_drift_report.html` và `retraining_log.json`; nếu không có drift thật, coordinator không train.
- Chỉ khi audit ghi `SUCCESS`, Registry có version mới và `/ready` vẫn xanh thì trình bày hot reload thành công. Version không cố định.

---

### BƯỚC 6: BÁO CÁO ĐIỀU HÀNH POWER BI & KIỂM THỬ TẢI HỆ THỐNG (2 phút)

**Lời dẫn**:
> *"Cuối cùng, Power BI đọc các fact và forecast đã được lưu trong warehouse. Báo cáo tải hiển thị đúng số đo của lần chạy, phân biệt HTTP và in-process."*

**Thao tác**:
- Trình chiếu 4 trang Dashboard trên Power BI Desktop:
  1. Executive Summary (Doanh thu & tỷ trọng Shopee/TikTok).
  2. ABC/XYZ Matrix (Ma trận 9 ô phân bổ tồn kho).
  3. Inventory Health (Bản đồ nhiệt cảnh báo đứt hàng).
  4. Model Performance (Độ chính xác và sai số WAPE).
- Trình chiếu báo cáo tải mới nhất tại `data/load_test_summary.md`; đọc mode, lỗi, P95/P99 và RPS trực tiếp từ bảng. Báo cáo HTTP ngày 2026-10-09 ghi nhận P95 719 ms ở 200 users, không phải dưới 150 ms.

---

### KẾT LUẬN & CHUẨN BỊ TRẢ LỜI CÂU HỎI HỘI ĐỒNG (1 phút)
> *"Em xin chân thành cảm ơn Quý Thầy Cô trong Hội đồng đã lắng nghe phần trình diễn. Em rất mong nhận được những góp ý quý báu của Thầy Cô để tiếp tục hoàn thiện đề tài!"*

