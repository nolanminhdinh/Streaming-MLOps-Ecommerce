# Hướng dẫn chạy trọn vẹn luồng MLOps Tuần 9: Power BI Executive Dashboard & Kiểm thử Tải (Load Testing)

Tài liệu này hướng dẫn chi tiết cách xuất khẩu dữ liệu và kết nối vào **Power BI Desktop** để tạo Dashboard điều hành, cũng như cách thực thi kiểm thử tải (Load Testing) hệ thống Model Serving bằng cả công cụ dòng lệnh độc lập lẫn **Locust Web UI**.

---

## 1. Xuất khẩu Bộ Dữ liệu cho Power BI Desktop

Chạy script tự động trích xuất các bảng dữ liệu chuẩn hóa sang thư mục `powerbi/data/`:

```bash
# Xuất khẩu toàn bộ dữ liệu CSV cho Power BI
python powerbi/export_powerbi_dataset.py
```

> **Kỳ vọng:** Các tệp sau được tạo thành công trong `powerbi/data/`:
> - `Dim_Products.csv`: 19 SKU kèm phân loại ngành hàng và phân khúc ma trận.
> - `Dim_Geography.csv`: Dữ liệu phân bổ địa lý 10 tỉnh thành lớn.
> - `Fact_Orders_Summary.csv`: Lịch sử doanh thu và đơn hàng 60 ngày đa kênh.
> - `Inventory_Health_Alerts.csv`: Tình trạng tồn kho, mức tồn kho an toàn và cảnh báo ROP.
> - `Forecast_vs_Actual.csv`: Dữ liệu 60 ngày đối chiếu sản lượng thực tế vs dự báo.

---

## 2. Thiết lập Dashboard trên Power BI Desktop

1. **Khởi động ứng dụng Power BI Desktop**.
2. **Nạp dữ liệu**:
   - Chọn **Get Data** ➔ **Folder** (hoặc **Text/CSV**) ➔ Chọn đường dẫn tới thư mục `powerbi/data/`.
   - Chọn **Load** để đưa toàn bộ các bảng vào mô hình dữ liệu.
3. **Thiết lập Quan hệ (Model View)**:
   - Kéo liên kết: `Inventory_Health_Alerts[sku]` ➔ `Dim_Products[sku]` (1-to-1).
   - Kéo liên kết: `Forecast_vs_Actual[sku]` ➔ `Dim_Products[sku]` (Many-to-1).
4. **Tạo DAX Measures**:
   - Tham khảo đầy đủ danh sách công thức đã soạn sẵn tại: [`powerbi/dax_measures.md`](file:///c:/Users/MINH/Downloads/temp-extract/streaming-mlops-ecommerce/powerbi/dax_measures.md).
   - Tạo các measures chính: `Total Revenue`, `Total Orders`, `AOV`, `WAPE %`, `Safety Stock Dynamic`, `Reorder Point Dynamic`.
5. **Xây dựng 3 trang Báo cáo trực quan**:
   - **Trang 1: Executive Overview**: KPI Cards, Donut Chart doanh thu Shopee/TikTok, Area Chart xu hướng doanh thu, Bar Chart Top sản phẩm.
   - **Trang 2: Demand Forecasting**: Line Chart đối chiếu Actual vs Forecast theo ngày, Gauge Chart đo độ chính xác dự báo (Accuracy %).
   - **Trang 3: Inventory Risk Matrix**: Scatter Plot ma trận 9 ô ABC/XYZ, Thẻ đếm số lượng SKU báo động đỏ 🔴 **CRITICAL**, Bảng chi tiết đề xuất đặt hàng lại.

---

## 3. Thực thi Kiểm thử Tải & Hiệu năng Serving (Load Testing)

### Cách 1: Chạy Trình Benchmark Đa luồng Tự động (Khuyến nghị - Không cần cài thêm thư viện)
Kịch bản tự động giả lập đồng thời 10, 50, 100 và 200 người dùng gọi liên tục vào các endpoint `/predict/demand` và `/inventory/reorder-alert`:

```bash
# Chạy benchmark kiểm thử tải
python tests/load_testing/run_load_test.py
```

- Kịch bản sẽ tự động đo lường:
  - Throughput (RPS - Requests per second).
  - Độ trễ phân vị: Min, Average, P50, P90, P95, P99, Max.
  - Tỷ lệ lỗi (% Error).
- **Xem kết quả báo cáo thực nghiệm:**
  - Báo cáo tổng hợp: [`data/load_test_summary.md`](file:///c:/Users/MINH/Downloads/temp-extract/streaming-mlops-ecommerce/data/load_test_summary.md) (bảng Markdown sẵn sàng đưa vào đồ án tốt nghiệp).
  - Dữ liệu chi tiết: `data/load_test_results.json`.

---

### Cách 2: Chạy Kiểm thử Tải bằng Giao diện Trực quan của Locust
Nếu môi trường ảo của bạn đã cài đặt thư viện `locust`:

1. Đảm bảo container FastAPI đang hoạt động:
   ```bash
   docker compose up -d fastapi
   ```
2. Khởi động Locust:
   ```bash
   locust -f tests/load_testing/locustfile.py --host http://localhost:8000
   ```
3. Mở trình duyệt web truy cập: **`http://localhost:8089`**
4. Nhập các tham số kiểm thử:
   - **Number of users**: `100`
   - **Spawn rate**: `10` (users/sec)
   - **Host**: `http://localhost:8000`
5. Nhấn **Start swarming** để quan sát biểu đồ thời gian thực về RPS, Failure Rate và Response Times.

---

## 4. Chạy kiểm thử tự động (Unit Tests)

Chạy bộ unit test kiểm tra tính toán phân vị độ trễ, luồng kiểm thử tải và tính toàn vẹn của dữ liệu Power BI:

```bash
# Kiểm thử riêng Module Tuần 9
python -m unittest tests/test_load_test.py

# Hoặc kiểm thử toàn bộ test suite dự án
python -m unittest discover tests
```

> **Kỳ vọng:** Toàn bộ 47 test cases của dự án đều đạt trạng thái **OK**.

