# Module Power BI Executive Dashboard (Tuần 9)
## Thiết kế Theo Phương pháp luận Ralph Kimball (*The Data Warehouse Toolkit*)

Thư mục này chứa toàn bộ tài nguyên phục vụ việc thiết kế và xây dựng **Executive Dashboard** trên **Power BI Desktop**, hỗ trợ ban giám khảo và các nhà quản lý chuỗi cung ứng theo dõi toàn diện hoạt động kinh doanh đa kênh, dự báo nhu cầu bán hàng và quản trị tồn kho thông minh.

> 📖 **Báo cáo Kỹ thuật Đầy đủ**: Xem chi tiết tại [`docs/02-warehouse-etl-feature-store/bao_cao_xay_dung_power_bi_dashboard.md`](../docs/02-warehouse-etl-feature-store/bao_cao_xay_dung_power_bi_dashboard.md)

---

## Cấu trúc thư mục

```
powerbi/
├── data/                         # Thư mục chứa các tệp CSV xuất bản sẵn sàng import (Chuẩn Star Schema)
│   ├── Dim_Dates.csv             # Chiều lịch thời gian nhất quán (Conformed Date Dimension)
│   ├── Dim_Products.csv          # Danh mục sản phẩm, Surrogate Key, phân khúc ma trận ABC/XYZ
│   ├── Dim_Geography.csv         # Phân bổ địa lý 3 miền Bắc - Trung - Nam
│   ├── Fact_Orders_Summary.csv   # Doanh thu & đơn hàng tổng hợp theo ngày và sàn
│   ├── Inventory_Health_Alerts.csv # Tồn kho thực tế, Safety Stock, ROP, cấp độ cảnh báo
│   └── Forecast_vs_Actual.csv    # Forecast ML đã lưu, ghép actual sau ngày mục tiêu
├── Report_PowerBI.pbix           # Tệp Báo cáo Power BI hoàn thiện
├── theme_pastel.json             # Theme màu Pastel chuẩn Microsoft JSON Schema
├── views_for_powerbi.sql         # 5 SQL Views cho kết nối trực tiếp PostgreSQL
├── dax_measures.md               # Tập hợp đầy đủ các công thức tính toán DAX chuẩn
├── export_powerbi_dataset.py     # Script xuất khẩu tự động các tệp dữ liệu phẳng CSV
└── README.md                     # Tài liệu hướng dẫn này
```

---

## 2 Phương thức Kết nối Dữ liệu vào Power BI Desktop

### Cách 1: Nạp CSV từ Data Warehouse
1. Khởi động PostgreSQL, tạo/cập nhật schema và nạp dữ liệu ETL:
   ```bash
   docker compose up -d postgres
   python scripts/init_warehouse.py
   # Chạy ingestion/ETL và Model Serving như hướng dẫn vận hành.
   ```
2. Chạy API `/predict/demand` để các dự báo ML có lịch sử warehouse được ghi vào `Fact_Forecast_Predictions`.
3. Xuất lại các bảng CSV:
   ```bash
   python powerbi/export_powerbi_dataset.py
   ```
4. Trong Power BI Desktop, chọn **Refresh** để tải dữ liệu mới.

Exporter đọc trực tiếp `Fact_Orders`, `Fact_Inventory_Daily` và forecast đã lưu trong PostgreSQL. Nếu kho không truy cập được hoặc chưa có đơn hợp lệ, lệnh dừng với thông báo lỗi; nó không tạo số liệu ngẫu nhiên. `Forecast_vs_Actual` chỉ có forecast ML từ `mlflow_registry`/`local_joblib`, dựa trên lịch sử warehouse. `actual_demand` chỉ được gán sau khi ngày mục tiêu kết thúc; các forecast chưa có actual sẽ không ảnh hưởng WAPE/Bias. Khi chưa có dự báo ML đã lưu, file chỉ có header và dashboard chưa hiển thị metric accuracy.

“Actual” ở đây là giao dịch đã được ghi nhận trong warehouse. Nếu demo đang dùng `data_simulator`, accuracy là accuracy trên luồng mô phỏng đó; exporter không tự tạo actual bằng cách cộng nhiễu vào dự báo.

Các CSV đã xuất từ phiên bản cũ không tự đổi khi cập nhật mã nguồn. Chỉ bấm **Refresh** sau khi lệnh export chạy thành công để tránh tiếp tục xem snapshot demo cũ.

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
- **Donut Chart**: Tỷ trọng doanh thu Shopee/TikTok tính từ các đơn hợp lệ trong warehouse.
- **Area Chart**: Xu hướng tăng trưởng doanh thu theo ngày, làm nổi bật các đợt Flash Sale và Mega-sale ngày đôi.
- **Bar Chart**: Top 10 sản phẩm đóng góp doanh thu lớn nhất chuỗi.

### Trang 2: Độ chính xác Dự báo Nhu cầu (Demand Forecasting & Accuracy)
- **Line & Clustered Column Chart**: So sánh nhu cầu thực tế đã phát sinh với forecast ML đã lưu; forecast tương lai hiển thị riêng cho đến khi có actual.
- **Gauge Chart**: Chỉ số độ chính xác dự báo (**Forecast Accuracy %** = $1 - \text{WAPE}$); để trống khi chưa có actual đã hoàn tất.
- **Table / Matrix**: WAPE và Bias tính từ cặp forecast/actual đã hoàn tất, có thể phân rã theo phân khúc sản phẩm. Khoảng tin cậy chưa được hiệu chuẩn nên không dùng làm dải 95%.

### Trang 3: Quản trị Tồn kho & Cảnh báo Đặt hàng lại (Inventory Risk Matrix)
- **Scatter Plot / Matrix Grid**: Trực quan hóa ma trận 9 ô ABC/XYZ (Trục X: Hệ số biến thiên CV, Trục Y: Doanh thu Pareto).
- **Stat Cards**: Đếm nhanh số lượng SKU ở mức báo động đỏ 🔴 **CRITICAL**, cảnh báo vàng 🟡 **WARNING**, và an toàn 🟢 **NORMAL**.
- **Actionable Table**: Danh sách chi tiết các sản phẩm cần nhập hàng gấp, hiển thị Tồn kho hiện tại, Điểm đặt hàng lại (ROP), Tồn kho an toàn (SS), Số ngày còn lại trước khi đứt hàng (`days_until_stockout`) và Số lượng đề xuất đặt thêm (`recommended_reorder_qty`).
