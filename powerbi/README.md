# Module Power BI Executive Dashboard (Tuần 9)

Thư mục này chứa toàn bộ tài nguyên phục vụ việc thiết kế và xây dựng **Executive Dashboard** trên **Power BI Desktop**, hỗ trợ ban giám khảo và các nhà quản lý chuỗi cung ứng theo dõi toàn diện hoạt động kinh doanh đa kênh, dự báo nhu cầu bán hàng và quản trị tồn kho thông minh.

---

## Cấu trúc thư mục

```
powerbi/
├── data/                         # Thư mục chứa các tệp CSV xuất bản sẵn sàng import
│   ├── Dim_Products.csv          # Danh mục sản phẩm, phân khúc ma trận
│   ├── Dim_Geography.csv         # Phân bổ địa lý 3 miền Bắc - Trung - Nam
│   ├── Fact_Orders_Summary.csv   # Doanh thu & đơn hàng tổng hợp theo ngày và sàn
│   ├── Inventory_Health_Alerts.csv # Tồn kho thực tế, Safety Stock, ROP, cấp độ cảnh báo
│   └── Forecast_vs_Actual.csv    # Đối chiếu 60 ngày sản lượng thực tế vs dự báo
├── views_for_powerbi.sql         # Các SQL Views tối ưu hóa cho kết nối trực tiếp PostgreSQL
├── dax_measures.md               # Tập hợp đầy đủ các công thức tính toán DAX chuẩn
├── export_powerbi_dataset.py     # Script xuất khẩu tự động các tệp dữ liệu phẳng CSV
└── README.md                     # Tài liệu hướng dẫn này
```

---

## 2 Phương thức Kết nối Dữ liệu vào Power BI Desktop

### Cách 1: Nạp trực tiếp từ các tệp CSV (Khuyến nghị cho kiểm thử & chấm đồ án)
1. Chạy script để cập nhật dữ liệu mới nhất:
   ```bash
   python powerbi/export_powerbi_dataset.py
   ```
2. Khởi động **Power BI Desktop**.
3. Chọn **Get Data** ➔ **Text/CSV** (hoặc **Folder**) ➔ Trỏ tới thư mục `powerbi/data/`.
4. Nhấn **Load** để nạp toàn bộ các bảng vào Data Model.

### Cách 2: Kết nối trực tiếp vào PostgreSQL Star Schema
1. Đảm bảo container PostgreSQL đang chạy (`docker compose up -d postgres`).
2. Thực thi các views trong `powerbi/views_for_powerbi.sql` vào database `ecom_warehouse`.
3. Trong Power BI Desktop: Chọn **Get Data** ➔ **PostgreSQL Database**:
   - **Server**: `localhost:5432`
   - **Database**: `ecom_warehouse`
   - **User**: `ecom` | **Password**: `ecom_password`
4. Chọn các Views có tiền tố `vw_powerbi_*` để tải dữ liệu.

---

## Thiết kế Bố cục Dashboard (3 Trang Báo cáo)

### Trang 1: Tổng quan Kinh doanh Đa kênh (Executive Overview)
- **KPI Cards**: Tổng doanh thu (Net Revenue), Sản lượng bán (Units Sold), Tổng đơn hàng (Orders), Giá trị đơn trung bình (AOV).
- **Donut Chart**: Tỷ trọng đóng góp doanh thu giữa **Shopee** (55%) và **TikTok Shop** (45%).
- **Area Chart**: Xu hướng tăng trưởng doanh thu theo ngày, làm nổi bật các đợt Flash Sale và Mega-sale ngày đôi.
- **Bar Chart**: Top 10 sản phẩm đóng góp doanh thu lớn nhất chuỗi.

### Trang 2: Độ chính xác Dự báo Nhu cầu (Demand Forecasting & Accuracy)
- **Line & Clustered Column Chart**: So sánh trực tiếp sản lượng thực tế (**Actual Demand**) và sản lượng mô hình dự báo (**Forecast Demand**), kèm vùng bóng mờ biểu thị khoảng tin cậy 95%.
- **Gauge Chart**: Chỉ số độ chính xác dự báo (**Forecast Accuracy %** = $1 - \text{WAPE}$).
- **Table / Matrix**: Phân rã sai số WAPE và Bias theo từng phân khúc sản phẩm (AX/AY sai số thấp $\approx 19.8\%$, AZ/BZ $\approx 28.2\%$).

### Trang 3: Quản trị Tồn kho & Cảnh báo Đặt hàng lại (Inventory Risk Matrix)
- **Scatter Plot / Matrix Grid**: Trực quan hóa ma trận 9 ô ABC/XYZ (Trục X: Hệ số biến thiên CV, Trục Y: Doanh thu Pareto).
- **Stat Cards**: Đếm nhanh số lượng SKU ở mức báo động đỏ 🔴 **CRITICAL**, cảnh báo vàng 🟡 **WARNING**, và an toàn 🟢 **NORMAL**.
- **Actionable Table**: Danh sách chi tiết các sản phẩm cần nhập hàng gấp, hiển thị Tồn kho hiện tại, Điểm đặt hàng lại (ROP), Tồn kho an toàn (SS), Số ngày còn lại trước khi đứt hàng (`days_until_stockout`) và Số lượng đề xuất đặt thêm (`recommended_reorder_qty`).
