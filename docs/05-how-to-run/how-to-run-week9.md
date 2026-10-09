# Hướng dẫn chạy trọn vẹn luồng MLOps Tuần 9: Power BI Executive Dashboard & Kiểm thử Tải (Load Testing)

Tài liệu này hướng dẫn chi tiết cách xuất khẩu dữ liệu và kết nối vào **Power BI Desktop** để tạo Dashboard điều hành, cũng như cách thực thi kiểm thử tải (Load Testing) hệ thống Model Serving bằng cả công cụ dòng lệnh độc lập lẫn **Locust Web UI**.

---

## 1. Xuất khẩu Bộ Dữ liệu cho Power BI Desktop

Chạy script tự động trích xuất các bảng dữ liệu chuẩn hóa sang thư mục `powerbi/data/`:

```bash
# Xuất khẩu toàn bộ dữ liệu CSV cho Power BI
python powerbi/export_powerbi_dataset.py
```

> Các file được xuất từ facts hiện có trong PostgreSQL; số SKU/ngày phụ thuộc dữ liệu đã nạp. `Forecast_vs_Actual.csv` có thể chỉ có header nếu chưa có ML forecast được lưu và actual khớp.

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
   - Tham khảo danh sách công thức tại: [`powerbi/dax_measures.md`](../../powerbi/dax_measures.md).
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
  - Báo cáo tổng hợp: [`data/load_test_summary.md`](../../data/load_test_summary.md) (số đo của lần chạy gần nhất, không phải SLO đã được nghiệm thu).
  - Dữ liệu chi tiết: [`data/load_test_results.json`](../../data/load_test_results.json).
  - Kiểm tra `test_mode`: khi server offline, script đo in-process thay vì HTTP.

---

### Cách 2: Chạy Kiểm thử Tải bằng Giao diện Trực quan của Locust
Nếu môi trường ảo của bạn đã cài đặt thư viện `locust`:

1. Đảm bảo container FastAPI đang hoạt động và `/ready` trả HTTP 200:
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

> Không dùng con số test cũ trong hướng dẫn làm kết quả hiện hành. Xem [báo cáo kiểm thử mới nhất](../03-testing-and-benchmark/test_report_2026-10-09.md); mã đã sửa sau baseline nhưng chưa có lượt chạy lại.

