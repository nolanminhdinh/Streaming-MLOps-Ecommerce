# TÀI LIỆU QUẢN LÝ DỰ ÁN BÁO CÁO & ĐẶC TẢ DỮ LIỆU BI
## PROJECT MANAGEMENT SPECIFICATION, DATA DICTIONARY & BI DATA MODEL

> **Tên Dự án**: Streaming MLOps E-Commerce Demand Forecasting & Dynamic Inventory Optimization  
> **Chuyên ngành**: Khoa học Dữ liệu / Kỹ thuật Dữ liệu & AI — Đồ án Tốt nghiệp  
> **Tác giả / PM**: Đinh Công Minh  
> **Cấu trúc tài liệu**: Biên soạn chuẩn xác theo biểu mẫu Quản trị Dự án Báo cáo Doanh nghiệp (Task Log, Data Dictionary, Mindmap, Data Model, Reports, Measures, Timeline)  
> **Phiên bản**: 2.0 (Official Release)

---

## MỤC LỤC TÀI LIỆU

1. [Bảng 1: Nhật ký Công việc & Tinh chỉnh Logic / Truy vấn SQL (Task & SQL Change Log)](#1-bảng-1-nhật-ký-công-việc--tinh-chỉnh-logic--truy-vấn-sql-task--sql-change-log)
2. [Bảng 2: Từ điển Dữ liệu - Thông tin Chi tiết (Data Dictionary)](#2-bảng-2-từ-điển-dữ-liệu---thông-tin-chi-tiết-data-dictionary)
3. [Phần 3: Mind Map - Phân tích Nghiệp vụ Báo cáo (Business Mindmap)](#3-phần-3-mind-map---phân-tích-nghiệp-vụ-báo-cáo-business-mindmap)
4. [Phần 4: Data Model - Cấu trúc Mô hình Dữ liệu (Star Schema Data Model)](#4-phần-4-data-model---cấu-trúc-mô-hình-dữ-liệu-star-schema-data-model)
5. [Bảng 5: Danh mục Báo cáo Triển khai (Reports Specification)](#5-bảng-5-danh-mục-báo-cáo-triển-khai-reports-specification)
6. [Bảng 6: Danh sách Chỉ tiêu & Thư viện Công thức DAX (Measures & DAX Formulas)](#6-bảng-6-danh-sách-chỉ-tiêu--thư-viện-công-thức-dax-measures--dax-formulas)
7. [Bảng 7: Quy trình & Tiến độ Triển khai Dự án (Project Implementation Timeline)](#7-bảng-7-quy-trình--tiến-độ-triển-khai-dự-án-project-implementation-timeline)

---

## 1. BẢNG 1: NHẬT KÝ CÔNG VIỆC & TINH CHỈNH LOGIC / TRUY VẤN SQL (TASK & SQL CHANGE LOG)

| Task | PIC | End Date | Completed | SQL Mới (Highlight Changes) | SQL Sửa Lần 2 | Logic Cũ Ban Đầu | Giải Thích Thay Đổi | Note & Chi Tiết Kỹ Thuật |
|---|:---:|:---:|:---:|---|---|---|---|---|
| **Chuẩn hóa Doanh thu Thực nhận sau Voucher** | Minh | 2026-08-25 | True | `buyer_total_amount = f.subtotal - f.seller_discount - f.platform_discount - f.voucher_total` | `net_revenue = f.buyer_total_amount - f.commission_fee - f.service_fee` | `revenue = f.quantity * f.original_price` | Giá niêm yết không phản ánh dòng tiền thực tế thu về do các đợt Flash Sale và trợ giá từ sàn Shopee / TikTok. Chuyển sang tính theo tiền người mua thực trả. | Tránh phóng đại doanh thu thật lên 15–25%; đồng bộ giữa Fact_Orders và báo cáo tài chính. |
| **Xử lý Lưới Ngày Liên tục (Cartesian Grid)** | Minh | 2026-08-28 | True | `WITH grid AS (SELECT d.date_key, p.product_key FROM Dim_Dates d CROSS JOIN Dim_Products p) SELECT g.date_key, g.product_key, COALESCE(SUM(f.quantity), 0) AS units_sold FROM grid g LEFT JOIN Fact_Orders f ON g.date_key = f.date_key AND g.product_key = f.product_key GROUP BY g.date_key, g.product_key` | | `SELECT date_key, product_key, SUM(quantity) FROM Fact_Orders GROUP BY date_key, product_key` | Nếu chỉ gom nhóm theo Fact_Orders, những ngày SKU không có lượt mua sẽ bị khuyết dòng. Mô hình chuỗi thời gian đòi hỏi lưới ngày liên tục không đứt đoạn. | CROSS JOIN giữa Dim_Dates và Dim_Products đảm bảo đủ 60 ngày liên tục cho toàn bộ 19 SKU. |
| **Chuẩn hóa Chỉ số Sai số WAPE Dự báo** | Minh | 2026-09-02 | True | `WAPE = DIVIDE(SUM(Forecast_vs_Actual[absolute_error]), SUM(Forecast_vs_Actual[actual_demand]), 0)`<br>`Accuracy = MAX(0, 1 - [WAPE %])` | | `MAPE = AVG(ABS(actual - pred) / NULLIF(actual, 0))` | Trong bán lẻ TMĐT, nhiều ngày có sản lượng bán bằng 0 (Zero-demand), khiến MAPE bị chia cho 0 gây lỗi vô cùng. WAPE chia tổng lỗi cho tổng lượng thực tế. | Chuẩn công nghiệp bán lẻ quốc tế (Retail Demand Forecasting Metric). |
| **Động hóa Định mức Tồn An toàn (SS) & ROP** | Minh | 2026-09-10 | True | `SS = CEILING(1.645 * p.sigma * SQRT(3))`<br>`ROP = CEILING(p.base_demand * 3 + SS)` | Phân bậc $Z$: Nhóm A ($Z=1.645$), Nhóm B ($Z=1.282$), Nhóm C ($Z=0.842$) | Cố định Safety Stock = 50 chiếc và ROP = 100 chiếc cho mọi SKU. | Cố định tĩnh gây đứt hàng với SKU bán chạy và đọng vốn với SKU bán chậm. Động hóa theo độ lệch chuẩn $\sigma$ và Lead Time 3 ngày. | Giảm tỷ lệ đứt hàng (Stockout Rate) thực nghiệm xuống dưới 1.5%. |
| **Chuẩn hóa Star Schema theo Ralph Kimball** | Minh | 2026-09-20 | True | Nối `Fact_Orders_Summary[order_date] -> Dim_Dates[Date]` và `Forecast_vs_Actual[sku] -> Dim_Products[sku]`. Loại bỏ đường nối nét đứt trùng lặp. | Bổ sung `Dim_Dates` dạng DAX Table bao quát 60 ngày và sự kiện Mega-sale. | Nối các bảng thông qua chuỗi `sku` tự nhiên, thiếu bảng chiều lịch `Dim_Dates`, bảng Fact bị cô lập. | Khắc phục triệt để Mistake 5 & 10 trong sách Ralph Kimball (*The Data Warehouse Toolkit*), hỗ trợ cơ chế Drill-Across đa bảng Fact. | Tối ưu hóa bộ nhớ VertiPaq và tốc độ truy vấn trên Power BI Desktop. |
| **Tối ưu Phân tầng Đệm Hứng Dữ liệu (3-Tier Buffering)** | Minh | 2026-08-22 | True | Consumer ghi micro-batch định kỳ 30 giây / 500 bản ghi sang MinIO Parquet phân vùng theo ngày. | Bổ sung cơ chế Dead Letter Queue (DLQ) cho bản ghi lỗi định dạng sàn. | Ingest trực tiếp từng bản ghi streaming đơn lẻ vào PostgreSQL Warehouse. | Ingest trực tiếp gây nghẽn kết nối và khóa bảng (Lock contention) khi có đợt Flash Sale lưu lượng đột biến. | Tách biệt tệp Raw Parquet trên Data Lake và tầng Star Schema Warehouse; chịu tải 200 RPS với P99 < 15ms. |
| **Pipeline Kỹ nghệ Đặc trưng ML chống Leakage** | Minh | 2026-09-15 | True | Lags (t-1 .. t-14) và Rolling Stats (mean, std 7d, 14d, 30d) áp dụng nghiêm ngặt `shift(1)` trước khi tính toán. | Thêm các đặc trưng ngoại sinh: Cờ ngày đôi Mega-sale, Cờ cuối tuần, Tỷ lệ giảm giá trung bình. | Tính rolling mean trực tiếp trên cửa sổ `[t, t-6]` bao gồm cả ngày dự báo $t$. | Bao gồm ngày $t$ gây rò rỉ thông tin tương lai vào mô hình huấn luyện, làm mô hình đạt điểm giả tạo nhưng sai lệch lớn khi chạy thực tế. | 43 đặc trưng chuỗi thời gian được kiểm chuẩn 100% bằng unit tests; Walk-Forward Validation Expanding Window. |

---

## 2. BẢNG 2: TỪ ĐIỂN DỮ LIỆU - THÔNG TIN CHI TIẾT (DATA DICTIONARY)

| STT | Nguồn Dữ Liệu | Tên Bảng | Mô Tả Bảng | Tên Cột | Mô Tả Cột | Loại Dữ Liệu | Ghi Chú |
|:---:|---|---|---|---|---|:---:|---|
| **1** | PostgreSQL Warehouse / DAX | `Dim_Dates` | Bảng chiều lịch thời gian chuẩn tắc (Conformed Date Dimension) | `date_key` | Khóa ngày thông minh dạng số nguyên | Integer | Smart Surrogate Key (PK) `YYYYMMDD` |
| | | | | `Date` | Ngày lịch thực tế đầy đủ | Date | Chuẩn ISO-8601 (`YYYY-MM-DD`) |
| | | | | `day_of_week` | Thứ trong tuần theo số | Smallint | `0` (Thứ Hai) $\rightarrow$ `6` (Chủ Nhật) |
| | | | | `day_name` | Tên thứ trong tuần | Text | `Thứ Hai` $\rightarrow$ `Chủ Nhật` |
| | | | | `week_of_year` | Tuần trong năm | Smallint | `1` $\rightarrow$ `53` |
| | | | | `month` | Số thứ tự tháng | Smallint | `1` $\rightarrow$ `12` |
| | | | | `month_name` | Tên tháng | Text | `Tháng 1` $\rightarrow$ `Tháng 12` |
| | | | | `month_year` | Tháng Năm | Text | `2026-08`, `2026-09` |
| | | | | `quarter` | Quý kinh doanh | Text | `Q1`, `Q2`, `Q3`, `Q4` |
| | | | | `year` | Năm lịch | Smallint | `2025` $\rightarrow$ `2027` |
| | | | | `is_weekend` | Cờ nhận diện cuối tuần | Boolean | `TRUE` nếu Thứ Bảy / Chủ Nhật |
| | | | | `is_mega_sale` | Cờ ngày hội mua sắm Mega-sale | Boolean | `TRUE` nếu ngày đôi 8/8, 9/9... |
| **2** | PostgreSQL Data Warehouse | `Dim_Products` | Bảng danh mục sản phẩm chuẩn hóa (Product Dimension) | `product_key` | Khóa nhân tạo sản phẩm | Integer | Surrogate Key (PK) |
| | | | | `sku` | Mã quản lý hàng hóa | Text | Natural Key (NK) ví dụ `EL-001` |
| | | | | `product_name` | Tên đầy đủ của sản phẩm | Text | Nhãn hiển thị trên báo cáo |
| | | | | `category` | Ngành hàng phân loại | Text | `Electronics`, `Fashion`, `Home`... |
| | | | | `base_demand` | Nhu cầu tiêu thụ cơ sở | Numeric | Sản lượng bán trung bình ngày |
| | | | | `sigma` | Độ lệch chuẩn nhu cầu | Numeric | Đo lường mức độ biến động sức mua |
| | | | | `matrix_segment` | Phân khúc ma trận 9 ô | Text | `AX, AY, AZ, BX, BY, BZ, CX, CY, CZ` |
| **3** | PostgreSQL Data Warehouse | `Dim_Geography` | Bảng chiều địa lý giao hàng theo vùng miền | `geo_key` | Khóa nhân tạo địa lý | Integer | Surrogate Key (PK) |
| | | | | `state` | Tỉnh / Thành phố | Text | `TP.HCM`, `Hà Nội`, `Đà Nẵng`... |
| | | | | `region` | Khu vực địa lý | Text | `Bắc`, `Trung`, `Nam` |
| | | | | `historical_gmp_vnd` | Dung lượng thị trường lịch sử | BigInt | Quy mô giao dịch địa phương (VND) |
| **4** | PostgreSQL Data Warehouse | `Dim_Shops` | Bảng chiều gian hàng & sàn phân phối | `shop_key` | Khóa nhân tạo gian hàng | Integer | Surrogate Key (PK) |
| | | | | `shop_id` | Mã định danh shop trên sàn | Text | Natural Key từ sàn |
| | | | | `shop_name` | Tên gian hàng chính hãng | Text | Tên hiển thị gian hàng |
| | | | | `platform` | Nền tảng sàn TMĐT | Text | `shopee`, `tiktok` |
| **5** | Data Warehouse / CSV Mart | `Fact_Orders_Summary` | Bảng số đo bán hàng tổng hợp (Sales Aggregate Fact) | `date_key` | Khóa ngoại ngày đơn hàng | Integer | Nối `Dim_Dates[date_key]` |
| | | | | `order_date` | Ngày phát sinh đơn hàng | Date | Nối `Dim_Dates[Date]` |
| | | | | `platform` | Sàn phân phối | Text | `shopee`, `tiktok` |
| | | | | `total_orders` | Tổng số đơn hàng hợp lệ | BigInt | Số đo cộng gộp (Orders Count) |
| | | | | `units_sold` | Tổng sản lượng bán | BigInt | Số đo cộng gộp (chiếc) |
| | | | | `gross_revenue` | Doanh thu niêm yết gộp | BigInt | Số đo cộng gộp (VND) |
| | | | | `discounts` | Tổng giá trị chiết khấu / voucher | BigInt | Số đo cộng gộp (VND) |
| | | | | `net_revenue` | Doanh thu thực nhận | BigInt | Số đo cộng gộp (VND) |
| | | | | `aov` | Giá trị đơn bình quân | BigInt | `net_revenue / total_orders` |
| **6** | Model Serving / MLflow | `Forecast_vs_Actual` | Bảng đối chứng dự báo MLOps (Consolidated Fact) | `date_key` | Khóa ngoại ngày đối chứng | Integer | Nối `Dim_Dates[date_key]` |
| | | | | `date` | Ngày đối chứng | Date | Nối `Dim_Dates[Date]` |
| | | | | `product_key` | Khóa ngoại sản phẩm | Integer | Nối `Dim_Products[product_key]` |
| | | | | `sku` | Mã SKU sản phẩm | Text | Nối `Dim_Products[sku]` |
| | | | | `actual_demand` | Sản lượng bán thực tế | Numeric | Số đo thực tế đối chứng |
| | | | | `forecast_demand` | Sản lượng mô hình dự báo | Numeric | Dự báo từ Champion Model |
| | | | | `lower_bound` | Cận dưới khoảng tin cậy 95% | Numeric | Giới hạn dưới suy luận thống kê |
| | | | | `upper_bound` | Cận trên khoảng tin cậy 95% | Numeric | Giới hạn trên suy luận thống kê |
| | | | | `absolute_error` | Sai số tuyệt đối | Numeric | `abs(actual_demand - forecast_demand)` |
| | | | | `squared_error` | Bình phương sai số | Numeric | `(absolute_error)^2` |
| **7** | Inventory Service / Kafka | `Inventory_Health_Alerts` | Bảng ảnh chụp tồn kho & cảnh báo (Snapshot Fact) | `date_key` | Khóa ngoại ngày ảnh chụp | Integer | Nối `Dim_Dates[date_key]` |
| | | | | `product_key` | Khóa ngoại sản phẩm | Integer | Nối `Dim_Products[product_key]` |
| | | | | `sku` | Mã SKU sản phẩm | Text | Nối `Dim_Products[sku]` |
| | | | | `current_stock` | Tồn kho thực tế hiện tại | BigInt | Số lượng tồn vật lý trong kho (chiếc) |
| | | | | `avg_daily_demand` | Nhu cầu trung bình ngày | Numeric | Tốc độ tiêu thụ bình quân ngày |
| | | | | `safety_stock` | Tồn kho an toàn định mức | BigInt | Ngưỡng đệm an toàn động ($SS$) |
| | | | | `reorder_point` | Điểm đặt hàng lại | BigInt | Ngưỡng kích hoạt lệnh nhập ($ROP$) |
| | | | | `needs_reorder` | Cờ yêu cầu đặt hàng lại | Boolean | `TRUE` nếu `current_stock <= ROP` |
| | | | | `alert_level` | Cấp độ cảnh báo rủi ro | Text | `CRITICAL`, `WARNING`, `NORMAL` |
| | | | | `days_until_stockout` | Số ngày trước khi cạn kho | Numeric | `current_stock / avg_daily_demand` |
| | | | | `recommended_reorder_qty`| Lượng đề xuất đặt thêm | BigInt | Số lượng hàng cần nhập bổ sung |

---

## 3. PHẦN 3: MIND MAP - PHÂN TÍCH NGHIỆP VỤ BÁO CÁO (BUSINESS MINDMAP)

```
HỆ THỐNG EXECUTIVE DASHBOARD & PHÂN TÍCH MLOPS
│
├── 1. TRỤ CỘT ĐIỀU HÀNH KINH DOANH ĐA KÊNH (MULTI-CHANNEL PERFORMANCE)
│   ├── Doanh thu Thực nhận (Net Revenue) & Khối lượng Đơn hàng (Orders Volume)
│   ├── Giá trị Đơn hàng Trung bình (AOV - Average Order Value)
│   ├── Cơ cấu Thị phần Kênh: Shopee (55%) vs TikTok Shop (45%)
│   ├── Tác động Mùa vụ: Tăng trưởng Cuối tuần (+30%) & Bùng nổ Mega-sale (+80%)
│   └── Phân bổ Không gian Địa lý: 10 Tỉnh thành trọng điểm & Cơ cấu 3 Miền
│
├── 2. TRỤ CỘT ĐÁNH GIÁ MÔ HÌNH HỌC MÁY (MLOPS MODEL MONITORING)
│   ├── Đối chứng Thực tế vs Dự báo (Actual vs Forecast Daily Demand)
│   ├── Dải Khoảng tin cậy Suy luận 95% (95% Confidence Interval Bounds)
│   ├── Thước đo Hiệu năng Chuẩn: Sai số WAPE % & Độ chính xác Forecast Accuracy %
│   ├── Giám sát Độ lệch Hệ thống (Forecast Bias %: Over-forecasting vs Under-forecasting)
│   └── Ma trận Đánh giá Hiệu năng Phân rã theo Nhóm hàng ABC/XYZ
│
└── 3. TRỤ CỘT QUẢN TRỊ TỒN KHO & CHUỖI CUNG ỨNG (INVENTORY HEALTH & SUPPLY CHAIN)
    ├── Giám sát Tồn kho Tức thời & Định mức An toàn Tự thích ứng (Dynamic Safety Stock)
    ├── Điểm Tái Đặt Hàng Động (Dynamic Reorder Point - ROP) theo Lead Time
    ├── Phân loại Trạng thái Khẩn cấp: 🔴 CRITICAL | 🟡 WARNING | 🟢 NORMAL
    ├── Dự báo Số ngày Cạn kho Còn lại (Days Until Stockout)
    └── Bảng Hành động Đặt hàng Tự động (Recommended Reorder Quantity)
```

---

## 4. PHẦN 4: DATA MODEL - CẤU TRÚC MÔ HÌNH DỮ LIỆU (STAR SCHEMA DATA MODEL)

### Sơ đồ Liên kết Topo (Star Schema Topology):
- **Bảng Chiều Trung tâm (Conformed Dimensions):**
  - `Dim_Dates`: Chiều lịch ngày kết nối đồng thời với các bảng Fact giao dịch và đối chứng dự báo.
  - `Dim_Products`: Chiều danh mục sản phẩm kết nối với bảng dự báo và bảng snapshot tồn kho.
- **Bảng Số đo (Fact Tables):**
  - `Fact_Orders_Summary` nối với `Dim_Dates` qua khóa `order_date` $\rightarrow$ `Date` (Quan hệ $N : 1$).
  - `Forecast_vs_Actual` nối với `Dim_Dates` qua `date` $\rightarrow$ `Date` ($N : 1$) và nối với `Dim_Products` qua `sku` ($N : 1$).
  - `Inventory_Health_Alerts` nối với `Dim_Products` qua `sku` ($1 : 1$).
- **Bảng Điều phối Đo lường (`_Measures`):**
  - Chứa toàn bộ 19 measures quản trị được chia vào 3 Display Folders. Toàn bộ các cột cơ sở được ẩn (`Hidden = True`), Power BI tự động gán icon máy tính 🧮 và ghim lên đầu Data Pane.

---

## 5. BẢNG 5: DANH MỤC BÁO CÁO TRIỂN KHAI (REPORTS SPECIFICATION)

| STT | Tên Báo Cáo | Trang Báo Cáo | Mục Tiêu Phân Tích | Nội Dung Hiển Thị & Thành Phần Visuals | Ghi Chú & Tương Tác Kỹ Thuật |
|:---:|---|---|---|---|---|
| **1** | **Tổng quan Kinh doanh Đa kênh** | `Executive Overview` | Cung cấp góc nhìn điều hành 360 độ về quy mô doanh thu, tốc độ tăng trưởng đơn hàng và cơ cấu thị phần sàn TMĐT. | **1/ KPI Cards (Hàng đầu)**: Total Revenue, Total Orders, Total Units Sold, Average Order Value (AOV).<br>**2/ Donut Chart**: Tỷ trọng doanh thu Shopee (55%) vs TikTok Shop (45%).<br>**3/ Area Chart**: Xu hướng tăng trưởng doanh thu 60 ngày theo trục ngày `Dim_Dates`, làm nổi bật ngày Mega-sale và cuối tuần.<br>**4/ Bar Chart (Cột ngang)**: Top 10 SKU mang lại doanh thu lớn nhất chuỗi.<br>**5/ Map / Column Chart**: Cơ cấu doanh số 3 miền Bắc - Trung - Nam từ `Dim_Geography`. | - Slicers: Chọn nhanh theo Tháng, Quý, và Cờ Mega-sale.<br>- Tooltips: Hover vào điểm ngày hiển thị chi tiết số đơn hủy và tỷ lệ giảm giá.<br>- Cross-filtering: Chọn sàn Shopee trên Donut Chart tự động lọc doanh số và Top SKU của riêng Shopee. |
| **2** | **Độ chính xác Dự báo Nhu cầu** | `Demand Forecasting & MLOps Accuracy` | Giám sát chất lượng dự đoán của mô hình Machine Learning Champion, phát hiện sớm suy giảm hiệu năng và Data Drift. | **1/ Line & Clustered Column Chart**: So sánh trực tiếp sản lượng bán thực tế (`Actual Demand`) và dự báo mô hình (`Forecast Demand`), kèm dải bóng mờ khoảng tin cậy 95% (`lower_bound` – `upper_bound`).<br>**2/ Gauge Chart (Đồng hồ đo)**: Chỉ số độ chính xác toàn chuỗi `Forecast Accuracy %` (Mục tiêu: $\ge 80\%$, tương ứng $WAPE \le 20\%$).<br>**3/ Bar Chart**: Độ lệch hệ thống `Forecast Bias %` nhận diện xu hướng dự báo thừa hay thiếu.<br>**4/ Matrix Table**: Phân rã sai số chi tiết $WAPE\%$ theo 9 nhóm ma trận ABC/XYZ. | - Slicers: Lọc theo Ngành hàng (Category) và Phân khúc ma trận (AX, AY, AZ...).<br>- Drill-through: Nhấp chuột phải vào một SKU để xem chi tiết chuỗi thời gian của riêng sản phẩm đó.<br>- Conditional Formatting: Bôi đỏ cảnh báo khi WAPE của nhóm hàng vượt ngưỡng 30%. |
| **3** | **Quản trị Tồn kho & Cảnh báo Nhập hàng** | `Inventory Risk Matrix & Replenishment` | Kiểm soát rủi ro đứt hàng, giải phóng hàng tồn đọng và tự động hóa đề xuất đơn nhập hàng bổ sung cho nhà cung cấp. | **1/ Scatter Plot (Ma trận 9 ô)**: Phân bố 19 SKU trên hệ trục tọa độ (Trục X: Độ biến động $CV$, Trục Y: Doanh thu Pareto).<br>**2/ Stat Status Cards**: 3 thẻ đếm nhanh số lượng mặt hàng theo mã màu cảnh báo (🔴 `Critical SKU Count`, 🟡 `Warning SKU Count`, 🟢 `Normal SKU Count`).<br>**3/ Actionable Table (Bảng hành động)**: Danh sách sản phẩm cần nhập hàng gấp, hiển thị Tồn hiện tại, $SS$, $ROP$, Số ngày trước khi cạn kho (`days_until_stockout`) và Lượng đề xuất đặt thêm (`recommended_reorder_qty`).<br>**4/ KPI Card**: Tổng số lượng hàng đề xuất đặt thêm (`Total Reorder Qty`). | - Slicers: Lọc theo Cấp độ cảnh báo (`alert_level`: CRITICAL, WARNING, NORMAL).<br>- Bookmarks: Chuyển đổi nhanh giữa chế độ xem "Cảnh báo Khẩn cấp" và "Toàn bộ Danh mục".<br>- Alert Highlighting: Bôi đỏ các SKU có `days_until_stockout < 2` ngày để ưu tiên xử lý trong ca làm việc. |

---

## 6. BẢNG 6: DANH SÁCH CHỈ TIÊU & THƯ VIỆN CÔNG THỨC DAX (MEASURES & DAX FORMULAS)

| STT | Tên Chỉ Tiêu Nghiệp Vụ | Tên Measure DAX | Nhóm / Folder Phân Loại | Định Dạng (Format) | Định Nghĩa & Công Thức Toán Học | Biểu Thức Hàm DAX Chuẩn Hóa |
|:---:|---|---|:---:|:---:|---|---|
| **1** | **Tổng Doanh thu Thực nhận** | `Total Revenue` | `01 - Tai chinh va Kenh ban` | `#,##0 ₫` | Dòng tiền thực tế thu về sau khi trừ chiết khấu: $\sum \text{net\_revenue}$ | `Total Revenue = SUM(Fact_Orders_Summary[net_revenue])` |
| **2** | **Tổng Số Đơn hàng** | `Total Orders` | `01 - Tai chinh va Kenh ban` | `#,##0` | Tổng số lượng đơn hàng hợp lệ đã thanh toán: $\sum \text{total\_orders}$ | `Total Orders = SUM(Fact_Orders_Summary[total_orders])` |
| **3** | **Tổng Sản lượng Bán** | `Total Units Sold` | `01 - Tai chinh va Kenh ban` | `#,##0` | Tổng số lượng sản phẩm vật lý tiêu thụ: $\sum \text{units\_sold}$ | `Total Units Sold = SUM(Fact_Orders_Summary[units_sold])` |
| **4** | **Giá trị Đơn hàng Trung bình** | `Average Order Value (AOV)` | `01 - Tai chinh va Kenh ban` | `#,##0 ₫` | Doanh thu bình quân trên một đơn hàng: $\frac{\text{Total Revenue}}{\text{Total Orders}}$ | `Average Order Value (AOV) = DIVIDE([Total Revenue], [Total Orders], 0)` |
| **5** | **Doanh thu Sàn Shopee** | `Shopee Revenue` | `01 - Tai chinh va Kenh ban` | `#,##0 ₫` | Doanh thu thực nhận phát sinh riêng từ sàn Shopee. | `Shopee Revenue = CALCULATE([Total Revenue], Fact_Orders_Summary[platform] = "shopee")` |
| **6** | **Doanh thu Sàn TikTok Shop** | `TikTok Revenue` | `01 - Tai chinh va Kenh ban` | `#,##0 ₫` | Doanh thu thực nhận phát sinh riêng từ sàn TikTok Shop. | `TikTok Revenue = CALCULATE([Total Revenue], Fact_Orders_Summary[platform] = "tiktok")` |
| **7** | **Tỷ trọng Doanh thu Shopee** | `Shopee Share %` | `01 - Tai chinh va Kenh ban` | `0.0%` | Tỷ lệ phần trăm doanh thu Shopee trên tổng doanh thu chuỗi. | `Shopee Share % = DIVIDE([Shopee Revenue], [Total Revenue], 0)` |
| **8** | **Tỷ trọng Doanh thu TikTok Shop**| `TikTok Share %` | `01 - Tai chinh va Kenh ban` | `0.0%` | Tỷ lệ phần trăm doanh thu TikTok Shop trên tổng doanh thu chuỗi. | `TikTok Share % = DIVIDE([TikTok Revenue], [Total Revenue], 0)` |
| **9** | **Sản lượng Bán Thực tế** | `Actual Demand` | `02 - Do chinh xac Du bao (MLOps)` | `#,##0` | Tổng sản lượng bán thực tế đối chứng: $\sum \text{actual\_demand}$ | `Actual Demand = SUM(Forecast_vs_Actual[actual_demand])` |
| **10** | **Sản lượng Mô hình Dự báo** | `Forecast Demand` | `02 - Do chinh xac Du bao (MLOps)` | `#,##0` | Tổng sản lượng dự báo từ mô hình Champion: $\sum \text{forecast\_demand}$ | `Forecast Demand = SUM(Forecast_vs_Actual[forecast_demand])` |
| **11** | **Tổng Sai số Tuyệt đối** | `Forecast Absolute Error` | `02 - Do chinh xac Du bao (MLOps)` | `#,##0` | Tổng chênh lệch độ lệch tuyệt đối: $\sum |\text{Actual}_i - \text{Forecast}_i|$ | `Forecast Absolute Error = SUM(Forecast_vs_Actual[absolute_error])` |
| **12** | **Sai số Tuyệt đối Trọng số (WAPE)** | `WAPE %` | `02 - Do chinh xac Du bao (MLOps)` | `0.0%` | Thước đo chuẩn công nghiệp tránh chia cho 0: $\frac{\sum |\text{Actual} - \text{Forecast}|}{\sum \text{Actual}}$ | `WAPE % = DIVIDE([Forecast Absolute Error], [Actual Demand], 0)` |
| **13** | **Độ chính xác Dự báo** | `Forecast Accuracy %` | `02 - Do chinh xac Du bao (MLOps)` | `0.0%` | Tỷ lệ dự báo chuẩn xác của hệ thống AI: $\max(0, 1 - \text{WAPE})$ | `Forecast Accuracy % = MAX(0, 1 - [WAPE %])` |
| **14** | **Độ lệch Hệ thống (Forecast Bias)**| `Forecast Bias %` | `02 - Do chinh xac Du bao (MLOps)` | `0.0%` | Nhận biết xu hướng dự báo thừa ($>0$) hay thiếu ($<0$): $\frac{\sum (\text{Forecast} - \text{Actual})}{\sum \text{Actual}}$ | `Forecast Bias % = DIVIDE([Forecast Demand] - [Actual Demand], [Actual Demand], 0)` |
| **15** | **Tổng Tồn kho Hiện tại** | `Total Current Stock` | `03 - Quan tri Ton kho va Canh bao` | `#,##0` | Tổng số lượng sản phẩm vật lý còn lại trong kho: $\sum \text{current\_stock}$ | `Total Current Stock = SUM(Inventory_Health_Alerts[current_stock])` |
| **16** | **Số lượng SKU Báo động Đỏ** | `Critical SKU Count` | `03 - Quan tri Ton kho va Canh bao` | `#,##0` | Đếm số mặt hàng có nguy cơ đứt hàng khẩn cấp (tồn kho $\le SS$). | `Critical SKU Count = CALCULATE(COUNTROWS(Inventory_Health_Alerts), Inventory_Health_Alerts[alert_level] = "CRITICAL")` |
| **17** | **Số lượng SKU Cảnh báo Vàng** | `Warning SKU Count` | `03 - Quan tri Ton kho va Canh bao` | `#,##0` | Đếm số mặt hàng đã chạm điểm đặt hàng lại (tồn kho $\le ROP$). | `Warning SKU Count = CALCULATE(COUNTROWS(Inventory_Health_Alerts), Inventory_Health_Alerts[alert_level] = "WARNING")` |
| **18** | **Số lượng SKU Mức An toàn** | `Normal SKU Count` | `03 - Quan tri Ton kho va Canh bao` | `#,##0` | Đếm số mặt hàng có lượng tồn kho nằm trong ngưỡng an toàn. | `Normal SKU Count = CALCULATE(COUNTROWS(Inventory_Health_Alerts), Inventory_Health_Alerts[alert_level] = "NORMAL")` |
| **19** | **Tổng Số lượng Đề xuất Nhập thêm**| `Total Reorder Qty` | `03 - Quan tri Ton kho va Canh bao` | `#,##0` | Tổng sản lượng hàng hóa hệ thống khuyến nghị làm PO đặt thêm: $\sum \text{reorder\_qty}$ | `Total Reorder Qty = SUM(Inventory_Health_Alerts[recommended_reorder_qty])` |

---

## 7. BẢNG 7: QUY TRÌNH & TIẾN ĐỘ TRIỂN KHAI DỰ ÁN (PROJECT IMPLEMENTATION TIMELINE)

| Giai Đoạn | Đầu Mục Công Việc | Ngày Bắt Đầu | Ngày Kết Thúc | Kế Hoạch | Triển Khai | Kiểm Thử (Test) | Rà Soát & Nghiệm Thu | Trạng Thái |
|:---:|---|:---:|:---:|:---:|:---:|:---:|:---:|:---:|
| **I. KHỞI TẠO & KIỂM TRA NGHIỆP VỤ** | 1. Khảo sát nghiệp vụ bán hàng Shopee & TikTok Shop | 2026-08-01 | 2026-08-07 | ✔ | ✔ | | | **Hoàn thành** |
| | 2. Thiết kế kiến trúc tổng thể Streaming MLOps 6 phân tầng | 2026-08-08 | 2026-08-14 | ✔ | ✔ | | | **Hoàn thành** |
| | 3. Xây dựng Data Simulator đa sàn (Shopee 84 cột, TikTok 71 cột) | 2026-08-15 | 2026-08-21 | | ✔ | ✔ | | **Hoàn thành** |
| **II. KẾT NỐI DỮ LIỆU & ETL WAREHOUSE** | 4. Thiết lập cụm 3 tầng đệm (Producer Buffer $\rightarrow$ Kafka $\rightarrow$ MinIO) | 2026-08-22 | 2026-08-28 | | ✔ | ✔ | | **Hoàn thành** |
| | 5. Thiết kế DDL PostgreSQL Star Schema (`01_star_schema.sql`) | 2026-08-29 | 2026-09-04 | ✔ | ✔ | ✔ | | **Hoàn thành** |
| | 6. Pipeline ETL nạp Fact_Orders & Fact_Inventory_Daily | 2026-09-05 | 2026-09-11 | | ✔ | ✔ | ✔ | **Hoàn thành** |
| **III. FEATURE STORE & MACHINE LEARNING**| 7. Kỹ nghệ đặc trưng chuỗi thời gian & Walk-Forward Validation | 2026-09-12 | 2026-09-18 | ✔ | ✔ | ✔ | | **Hoàn thành** |
| | 8. Huấn luyện Baseline & Deep Learning GRU, tracking MLflow | 2026-09-19 | 2026-09-25 | | ✔ | ✔ | ✔ | **Hoàn thành** |
| | 9. Đóng gói Model Serving FastAPI & Tự động hóa Tồn kho ROP | 2026-09-26 | 2026-10-02 | | ✔ | ✔ | ✔ | **Hoàn thành** |
| **IV. XÂY DỰNG DATA MODEL & MEASURES** | 10. Xuất khẩu bộ dữ liệu CSV chuẩn hóa (`export_powerbi_dataset.py`)| 2026-10-03 | 2026-10-04 | | ✔ | ✔ | | **Hoàn thành** |
| | 11. Xây dựng Conformed Dimension `Dim_Dates` & Chuẩn hóa Star Schema | 2026-10-05 | 2026-10-06 | ✔ | ✔ | ✔ | | **Hoàn thành** |
| | 12. Triển khai bảng `_Measures` và 19 công thức DAX trong 3 Folders | 2026-10-06 | 2026-10-07 | | ✔ | ✔ | | **Hoàn thành** |
| **V. TRIỂN KHAI BÁO CÁO POWER BI** | 13. Xây dựng Trang 1: Tổng quan Kinh doanh Đa kênh (`Executive Overview`)| 2026-10-07 | 2026-10-07 | | ✔ | ✔ | | **Hoàn thành** |
| | 14. Xây dựng Trang 2: Độ chính xác Dự báo Nhu cầu (`Demand Forecasting`)| 2026-10-07 | 2026-10-08 | | ✔ | ✔ | | **Hoàn thành** |
| | 15. Xây dựng Trang 3: Quản trị Tồn kho & Cảnh báo (`Inventory Risk Matrix`)| 2026-10-08 | 2026-10-08 | | ✔ | ✔ | | **Hoàn thành** |
| | 16. Thiết kế bộ Theme màu Pastel chuẩn Microsoft JSON Schema | 2026-10-08 | 2026-10-08 | ✔ | ✔ | | | **Hoàn thành** |
| **VI. KIỂM THỬ TẢI & NGHIỆM THU CUỐI KỲ**| 17. Thực nghiệm kiểm thử tải Locust (10-200 RPS) & Soak Test 1h | 2026-10-09 | 2026-10-09 | | | ✔ | ✔ | **Hoàn thành** |
| | 18. Nghiệm thu toàn diện hệ thống, biên soạn Slide & Kịch bản Bảo vệ | 2026-10-09 | 2026-10-09 | | | | ✔ | **Hoàn thành** |
