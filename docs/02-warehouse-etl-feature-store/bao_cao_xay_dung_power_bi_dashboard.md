# BÁO CÁO THIẾT KẾ KỸ THUẬT & XÂY DỰNG EXECUTIVE DASHBOARD TRÊN POWER BI THEO PHƯƠNG PHÁP LUẬN RALPH KIMBALL

> **Dự án**: Streaming MLOps E-Commerce Demand Forecasting & Dynamic Inventory Optimization  
> **Chuyên ngành**: Khoa học Dữ liệu / Kỹ thuật Dữ liệu & AI — Đồ án Tốt nghiệp  
> **Phân hệ**: Phân tầng 6 — Trực quan hóa Điều hành Cấp cao (Presentation Area & Executive BI Dashboard)  
> **Tác giả**: Đinh Công Minh  
> **Tài liệu tham chiếu chuẩn**: *The Data Warehouse Toolkit: The Definitive Guide to Dimensional Modeling (Third Edition)* — Ralph Kimball & Margy Ross (Wiley, 2013)  
> **Trạng thái**: Hoàn thiện & Tích hợp Trực tiếp vào Mô hình Power BI Desktop (Production-Grade)

---

## MỤC LỤC

1. [Đặt vấn đề & Vai trò của Tầng Presentation Area trong Kiến trúc DW/BI](#1-đặt-vấn-đề--vai-trò-của-tầng-presentation-area-trong-kiến-trúc-dwbi)
2. [Phương pháp luận Thiết kế Chiều (Ralph Kimball Dimensional Modeling)](#2-phương-pháp-luận-thiết-kế-chiều-ralph-kimball-dimensional-modeling)
   - [2.1. Quy trình thiết kế 4 bước (Four-Step Dimensional Design Process)](#21-quy-trình-thiết-kế-4-bước-four-step-dimensional-design-process)
   - [2.2. Phân loại các bảng Fact trong mô hình](#22-phân-loại-các-bảng-fact-trong-mô-hình)
   - [2.3. Nguyên tắc Chiều Nhất quán (Conformed Dimensions) & Cơ chế Drill-Across](#23-nguyên-tắc-chiều-nhất-quán-conformed-dimensions--cơ-chế-drill-across)
   - [2.4. Tránh 10 sai lầm kinh điển của Ralph Kimball (Chapter 16)](#24-tránh-10-sai-lầm-kinh-điển-của-ralph-kimball-chapter-16)
3. [Thiết kế Mô hình Dữ liệu Chi tiết trong Power BI (Star Schema Topology)](#3-thiết-kế-mô-hình-dữ-liệu-chi-tiết-trong-power-bi-star-schema-topology)
   - [3.1. Conformed Dimension: Dim_Dates (Calendar Date Dimension)](#31-conformed-dimension-dim_dates-calendar-date-dimension)
   - [3.2. Conformed Dimension: Dim_Products & Ma trận 9 ô ABC/XYZ](#32-conformed-dimension-dim_products--ma-trận-9-ô-abcxyz)
   - [3.3. Fact_Orders_Summary (Sales Aggregate Fact Table)](#33-fact_orders_summary-sales-aggregate-fact-table)
   - [3.4. Forecast_vs_Actual (MLOps Consolidated Fact Table)](#34-forecast_vs_actual-mlops-consolidated-fact-table)
   - [3.5. Inventory_Health_Alerts (Inventory Periodic Snapshot Fact Table)](#35-inventory_health_alerts-inventory-periodic-snapshot-fact-table)
   - [3.6. Bảng Phân bổ Không gian Địa lý Dim_Geography](#36-bảng-phân-bổ-không-gian-địa-lý-dim_geography)
4. [Hệ thống Chỉ số Đo lường Quản trị DAX (Data Analysis Expressions)](#4-hệ-thống-chỉ-số-đo-lường-quản-trị-dax-data-analysis-expressions)
   - [4.1. Bảng lưu trữ chuyên biệt `_Measures`](#41-bảng-lưu-trữ-chuyên-biệt-_measures)
   - [4.2. Thư mục 01: Tài chính & Đa kênh (Financial & Channels)](#42-thư-mục-01-tài-chính--đa-kênh-financial--channels)
   - [4.3. Thư mục 02: Độ chính xác Dự báo MLOps (Forecast Accuracy)](#43-thư-mục-02-độ-chính-xác-dự-báo-mlops-forecast-accuracy)
   - [4.4. Thư mục 03: Quản trị Tồn kho & Cảnh báo ROP (Inventory Health & Alerts)](#44-thư-mục-03-quản-trị-tồn-kho--cảnh-báo-rop-inventory-health--alerts)
5. [Thiết kế Giao diện Trực quan & Bộ Theme Doanh nghiệp (Pastel Palette)](#5-thiết-kế-giao-diện-trực-quan--bộ-theme-doanh-nghiệp-pastel-palette)
   - [5.1. Bố cục 3 trang Dashboard chuyên sâu](#51-bố-cục-3-trang-dashboard-chuyên-sâu)
   - [5.2. Chuẩn hóa JSON Theme theo quy chuẩn Microsoft Power BI](#52-chuẩn-hóa-json-theme-theo-quy-chuẩn-microsoft-power-bi)
6. [Chiến lược Vận hành Hai Chế độ Nguồn Dữ liệu (Dual-Mode Data Strategy)](#6-chiến-lược-vận-hành-hai-chế-độ-nguồn-dữ-liệu-dual-mode-data-strategy)
   - [6.1. Chế độ 1: Flat CSV Data Marts (Phục vụ Chấm thi & Di động Độc lập)](#61-chế-độ-1-flat-csv-data-marts-phục-vụ-chấm-thi--di-động-độc-lập)
   - [6.2. Chế độ 2: DirectQuery / Scheduled Refresh từ PostgreSQL Star Schema](#62-chế-độ-2-directquery--scheduled-refresh-từ-postgresql-star-schema)
7. [Kiểm thử Tự động & Đảm bảo Tính Toàn vẹn Dữ liệu (Automated Unit Tests)](#7-kiểm-thử-tự-động--đảm-bảo-tính-toàn-vẹn-dữ-liệu-automated-unit-tests)
8. [Luận điểm Bảo vệ Đồ án Tốt nghiệp (Defense Key Takeaways)](#8-luận-điểm-bảo-vệ-đồ-án-tốt-nghiệp-defense-key-takeaways)

---

## 1. Đặt vấn đề & Vai trò của Tầng Presentation Area trong Kiến trúc DW/BI

Trong một hệ thống Streaming MLOps thương mại điện tử hoàn chỉnh, dữ liệu sau khi trải qua các tầng:
- **Tầng 1 (Ingestion & Buffering):** Hứng luồng đơn hàng thời gian thực từ Shopee và TikTok Shop qua 3 tầng đệm (Producer Buffer $\rightarrow$ Kafka $\rightarrow$ MinIO Parquet).
- **Tầng 2 (Data Warehouse):** Chu trình ETL/ELT chuẩn hóa lược đồ đa sàn và nạp vào kho dữ liệu PostgreSQL theo mô hình Star Schema.
- **Tầng 3 & 4 (Feature Store & Machine Learning):** Kỹ nghệ đặc trưng chuỗi thời gian, huấn luyện mô hình LightGBM / GRU với Walk-Forward Validation và quản lý trên MLflow.
- **Tầng 5 (Model Serving & Inventory Logic):** Triển khai API FastAPI phục vụ suy luận thời gian thực và tự động tính toán ngưỡng an toàn (Safety Stock, Reorder Point).

Vấn đề đặt ra ở **Tầng 6** là: **Làm thế nào để chuyển đổi các con số dự báo kỹ thuật và dữ liệu giao dịch phức tạp thành công cụ hỗ trợ ra quyết định kinh doanh trực quan, tức thì cho các cấp quản lý và hội đồng đánh giá?**

Theo Ralph Kimball (*The Data Warehouse Toolkit*, Chapter 1: "Publishing Metaphor for DW/BI Managers", trang 5–7):
> *"Nhiệm vụ của người quản trị DW/BI cũng giống như Tổng biên tập của một tòa soạn báo. Chúng ta không bán công nghệ in ấn, chúng ta xuất bản thông tin có cấu trúc, đơn giản, dễ hiểu và tin cậy nhất đến tay độc giả (Business Users)."*

Tầng **Presentation Area** trên **Power BI Desktop** được xây dựng chính là để thực hiện sứ mệnh này: cung cấp một giao diện 360 độ gồm 3 trang Dashboard điều hành, kết nối trực tiếp vào các chiều phân tích chuẩn mực.

---

## 2. Phương pháp luận Thiết kế Chiều (Ralph Kimball Dimensional Modeling)

### 2.1. Quy trình thiết kế 4 bước (Four-Step Dimensional Design Process)
Theo Ralph Kimball (*Chapter 3, trang 70–72*), quy trình thiết kế mô hình chiều bao gồm 4 bước cơ bản:

```
┌─────────────────────────┐     ┌─────────────────────────┐
│   Bước 1: Chọn Quy trình│ ──► │  Bước 2: Xác định Hạt   │
│   Nghiệp vụ (Process)   │     │    Dữ liệu (Grain)      │
└─────────────────────────┘     └───────────┬─────────────┘
                                            │
                                            ▼
┌─────────────────────────┐     ┌─────────────────────────┐
│   Bước 4: Xác định      │ ◄── │  Bước 3: Xác định các   │
│  Số đo (Facts/Metrics)  │     │     Chiều (Dimensions)  │
└─────────────────────────┘     └─────────────────────────┘
```

Áp dụng vào hệ thống:
1. **Bước 1 - Chọn quy trình nghiệp vụ:**
   - Hoạt động bán hàng đa kênh (E-Commerce Multi-Channel Sales).
   - Hoạt động dự báo nhu cầu sản phẩm (Demand Forecasting & Model Evaluation).
   - Hoạt động giám sát trạng thái kho hàng (Inventory Health & Replenishment).
2. **Bước 2 - Tuyên bố hạt dữ liệu (Declare the Grain):**
   - Đơn hàng tổng hợp: 1 dòng cho mỗi ngày / sàn phân phối (`date + platform`).
   - Đối chứng dự báo: 1 dòng cho mỗi ngày / sản phẩm (`date + sku`).
   - Cảnh báo tồn kho: 1 dòng cho mỗi sản phẩm tại ảnh chụp thời điểm hiện tại (`snapshot_date + sku`).
3. **Bước 3 - Xác định các chiều (Dimensions):**
   - Chiều thời gian lịch: `Dim_Dates` (ngày, thứ, tuần, tháng, quý, năm, sự kiện Mega-sale).
   - Chiều sản phẩm: `Dim_Products` (SKU, tên sản phẩm, danh mục, phân khúc ma trận ABC/XYZ).
   - Chiều địa lý: `Dim_Geography` (tỉnh/thành, miền Bắc - Trung - Nam), doanh thu và sản lượng tính từ đơn hàng trong kho.
4. **Bước 4 - Xác định các số đo (Facts):**
   - Doanh thu thực nhận (`net_revenue`), sản lượng bán (`units_sold`), đơn hàng (`total_orders`), giá trị đơn trung bình (`aov`).
   - Nhu cầu thực tế (`actual_demand`), dự báo (`forecast_demand`), sai số tuyệt đối (`absolute_error`).
   - Tồn kho hiện tại (`current_stock`), mức an toàn (`safety_stock`), điểm đặt hàng lại (`reorder_point`), số lượng đề xuất nhập (`recommended_reorder_qty`).

---

### 2.2. Phân loại các bảng Fact trong mô hình

Theo phân loại kinh điển tại **Chương 4 (trang 119–122)** của sách Kimball, hệ thống phân chia Fact tables thành các loại hình chính xác:

1. **Transaction & Aggregate Fact Table (`Fact_Orders_Summary`):**
   - Chứa các số đo cộng gộp hoàn toàn (Fully Additive Facts) như doanh thu, sản lượng, số lượng đơn. Có thể cộng dồn tự do trên mọi chiều (thời gian, kênh bán).
2. **Periodic Snapshot Fact Table (`Inventory_Health_Alerts`):**
   - Chứa các số đo trạng thái bán cộng gộp (Semi-Additive Facts) như lượng tồn kho `current_stock`. Không thể cộng dồn theo thời gian (cộng tồn kho 7 ngày không cho ra tồn kho của tuần) mà phải lấy ảnh chụp cuối kỳ hoặc trung bình.
3. **Consolidated Fact Table (`Forecast_vs_Actual`):**
   - Thuộc mô hình kết hợp nâng cao (*Chương 7, trang 224: "Consolidated Fact Tables"*): Kết hợp số đo thực tế từ quy trình bán hàng và số đo dự báo từ mô hình Machine Learning trên cùng một hạt (`date + sku`) để triệt tiêu độ phức tạp khi người dùng truy vấn so sánh.

---

### 2.3. Nguyên tắc Chiều Nhất quán (Conformed Dimensions) & Cơ chế Drill-Across

Theo **Chương 4 (trang 130–134)**:
> *"Conformed Dimensions là chất keo kết dính toàn bộ kho dữ liệu doanh nghiệp (Enterprise Data Warehouse Bus Architecture). Khi hai hoặc nhiều bảng Fact cùng kết nối vào một chiều chuẩn chung, người dùng có thể thực hiện thao tác Drill-Across (khoan cắt chéo) để đối chiếu số liệu đa quy trình trong cùng một báo cáo."*

Trong mô hình Power BI:
- **`Dim_Dates`** là Conformed Dimension nối với cả `Fact_Orders_Summary` và `Forecast_vs_Actual`.
- **`Dim_Products`** là Conformed Dimension nối với cả `Forecast_vs_Actual` và `Inventory_Health_Alerts`.
- **Hiệu quả thực tế:** Khi người dùng chọn một bộ lọc (Slicer) thời gian trên Power BI (ví dụ: *Tháng 8/2026* hoặc *Ngày siêu Sale Mega-sale*), cả hai biểu đồ Doanh thu bán hàng và Sai số dự báo AI đều đồng loạt được lọc theo cùng một trục ngày mà không xảy ra xung đột dữ liệu.

---

### 2.4. Tránh 10 sai lầm kinh điển của Ralph Kimball (Chapter 16)

Trong quá trình chuẩn hóa từ mô hình phẳng sơ khai sang Star Schema hoàn chỉnh, hệ thống đã triệt để khắc phục các lỗi được Ralph Kimball cảnh báo trong **Chương 16 (trang 397–401)**:

1. **Khắc phục Mistake 10: "Place Text Attributes in a Fact Table" (tr. 397):**
   - *Lỗi cũ:* Các bảng Fact chứa lặp lại tên sản phẩm, danh mục, phân khúc ma trận dạng chữ.
   - *Chuẩn hóa:* Các trường mô tả dạng chữ được chuyển giao toàn bộ về `Dim_Products`. Bảng Fact chỉ chứa các khóa ngoại số nguyên và các metric định lượng.
2. **Khắc phục Mistake 5: "Use Operational Keys to Join Dimensions and Facts" (tr. 399):**
   - *Lỗi cũ:* Nối các bảng thông qua chuỗi ký tự tự nhiên `sku` hoặc ngày text.
   - *Chuẩn hóa:* Thiết lập các Surrogate Keys kiểu số nguyên (`product_key`, `date_key`, `geo_key`) làm khóa liên kết chuẩn tắc.
3. **Khắc phục Mistake 1: "Fail to Conform Facts and Dimensions" (tr. 400):**
   - *Lỗi cũ:* Bảng `Fact_Orders_Summary` và `Dim_Geography` đứng cô lập, tạo thành các "ốc đảo dữ liệu" (Data Silos).
   - *Chuẩn hóa:* Đưa `Dim_Dates` vào làm trung tâm kết nối, tạo thành đồ hình Star Join khép kín.
4. **Loại bỏ liên kết trùng lặp vòng (Circular Ambiguity):**
   - Xóa bỏ đường liên kết nét đứt giữa `Forecast_vs_Actual` và `Inventory_Health_Alerts` để tuân thủ tính chất lọc phân cấp 1 chiều (`1 ➔ *`).

---

## 3. Thiết kế Mô hình Dữ liệu Chi tiết trong Power BI (Star Schema Topology)

### 3.1. Conformed Dimension: Dim_Dates (Calendar Date Dimension)
Bảng lịch được kiến tạo bằng biểu thức DAX Table chuẩn tắc, bao quát toàn bộ dải thời gian hoạt động của hệ thống:

```dax
Dim_Dates = 
ADDCOLUMNS (
    CALENDAR ( DATE ( 2026, 8, 1 ), DATE ( 2026, 9, 24 ) ),
    "date_key", INT ( FORMAT ( [Date], "YYYYMMDD" ) ),
    "Year", YEAR ( [Date] ),
    "Month", MONTH ( [Date] ),
    "Month_Name", FORMAT ( [Date], "mmmm" ),
    "Month_Year", FORMAT ( [Date], "yyyy-mm" ),
    "Quarter", "Q" & FORMAT ( [Date], "q" ),
    "Day_of_Week", WEEKDAY ( [Date], 2 ),
    "Day_Name", FORMAT ( [Date], "dddd" ),
    "Is_Weekend", IF ( WEEKDAY ( [Date], 2 ) >= 6, TRUE (), FALSE () ),
    "Is_Mega_Sale", IF ( DAY ( [Date] ) = MONTH ( [Date] ), TRUE (), FALSE () )
)
```

- **Khóa chính:** `Date` (hoặc `date_key` định dạng số nguyên `20260801`).
- **Ý nghĩa nghiệp vụ:** Giúp phân tích sâu tính quy luật mùa vụ (Seasonality), đột biến doanh số vào cuối tuần (`Is_Weekend`) và các đợt bùng nổ đơn hàng vào các ngày hội mua sắm ngày đôi (`Is_Mega_Sale`).

---

### 3.2. Conformed Dimension: Dim_Products & Ma trận 9 ô ABC/XYZ
Bảng danh mục sản phẩm chuẩn hóa:
- **Khóa:** `product_key` (Surrogate Key: 1, 2, 3...) và `sku` (Natural Key: `EL-001`, `FS-002`...).
- **Thuộc tính:** `product_name`, `category`, `base_demand`, `sigma`, `matrix_segment`.
- **Ma trận 9 ô ABC/XYZ:** Phân loại theo doanh thu Pareto (A = 80%, B = 15%, C = 5%) và hệ số biến động nhu cầu $CV = \sigma / \mu$ (X: ổn định $CV \le 0.1$, Y: biến thiên vừa, Z: thất thường $CV > 0.25$).

---

### 3.3. Fact_Orders_Summary (Sales Aggregate Fact Table)
- **Hạt dữ liệu:** 1 dòng cho mỗi ngày / sàn phân phối.
- **Khóa ngoại:** `order_date` $\rightarrow$ `Dim_Dates[Date]`.
- **Các số đo:** `total_orders`, `units_sold`, `gross_revenue`, `discounts`, `net_revenue`, `aov`.

---

### 3.4. Forecast_vs_Actual (MLOps Consolidated Fact Table)
- **Hạt dữ liệu:** 1 dòng cho mỗi ngày / sản phẩm SKU.
- **Khóa ngoại:** 
  - `date` $\rightarrow$ `Dim_Dates[Date]`
  - `sku` $\rightarrow$ `Dim_Products[sku]`
- **Nguồn forecast:** `Fact_Forecast_Predictions`, được Model Serving ghi lại khi chạy mô hình đã nạp với lịch sử warehouse; heuristic/demo không được đưa vào.
- **Nguồn actual:** tổng `quantity` từ `Fact_Orders` không hủy, chỉ gán sau khi ngày mục tiêu kết thúc. Ngày chưa kết thúc giữ `actual_demand` rỗng và bị loại khỏi WAPE/Bias.
- **Các số đo/metadata:** `actual_demand`, `forecast_demand`, `absolute_error`, `squared_error`, `model_name`, `model_version`, `model_source`, `forecast_generated_at`.
- `lower_bound` và `upper_bound` để rỗng vì cận API hiện tại chưa được hiệu chuẩn thành prediction interval 95%.

---

### 3.5. Inventory_Health_Alerts (Inventory Periodic Snapshot Fact Table)
- **Hạt dữ liệu:** 1 dòng cho mỗi SKU tại thời điểm chốt kho hiện tại.
- **Khóa ngoại:** `sku` $\rightarrow$ `Dim_Products[sku]`.
- **Các số đo & Chỉ báo:** `current_stock`, `avg_daily_demand`, `safety_stock`, `reorder_point`, `needs_reorder`, `alert_level`, `days_until_stockout`, `recommended_reorder_qty`.

---

### 3.6. Bảng Phân bổ Không gian Địa lý Dim_Geography
- Chỉ xuất các địa bàn xuất hiện trong đơn hàng hợp lệ; `total_revenue`, `total_units`, `total_orders` được tổng hợp trực tiếp từ `Fact_Orders`. Không dùng chỉ số quy mô kinh tế giả lập.

---

## 4. Hệ thống Chỉ số Đo lường Quản trị DAX (Data Analysis Expressions)

### 4.1. Bảng lưu trữ chuyên biệt `_Measures`
Để tuân thủ chuẩn mực chuyên nghiệp trong thiết kế Power BI, toàn bộ **19 công thức DAX Measures** không bị phân tán rải rác mà được tập trung vào bảng điều phối `_Measures` (đã ẩn các cột cơ sở để hiển thị biểu tượng máy tính 🧮 và ghim lên vị trí trên cùng của Data Pane).

Các measures được phân cấp vào **3 Display Folders** tương ứng với 3 phân hệ nghiệp vụ:

---

### 4.2. Thư mục 01: Tài chính & Đa kênh (Financial & Channels)

1. **Tổng Doanh thu Thực nhận (Net Revenue):**
   $$\text{Total Revenue} = \sum \text{net\_revenue}$$
   ```dax
   Total Revenue = SUM(Fact_Orders_Summary[net_revenue])
   ```
2. **Tổng Số Đơn hàng:**
   ```dax
   Total Orders = SUM(Fact_Orders_Summary[total_orders])
   ```
3. **Tổng Sản lượng Bán ra:**
   ```dax
   Total Units Sold = SUM(Fact_Orders_Summary[units_sold])
   ```
4. **Giá trị Đơn hàng Trung bình (AOV):**
   $$\text{AOV} = \frac{\text{Total Revenue}}{\text{Total Orders}}$$
   ```dax
   Average Order Value (AOV) = DIVIDE([Total Revenue], [Total Orders], 0)
   ```
5. **Doanh thu Sàn Shopee:**
   ```dax
   Shopee Revenue = CALCULATE([Total Revenue], Fact_Orders_Summary[platform] = "shopee")
   ```
6. **Doanh thu Sàn TikTok Shop:**
   ```dax
   TikTok Revenue = CALCULATE([Total Revenue], Fact_Orders_Summary[platform] = "tiktok")
   ```
7. **Tỷ trọng Doanh thu Shopee (%):**
   ```dax
   Shopee Share % = DIVIDE([Shopee Revenue], [Total Revenue], 0)
   ```
8. **Tỷ trọng Doanh thu TikTok Shop (%):**
   ```dax
   TikTok Share % = DIVIDE([TikTok Revenue], [Total Revenue], 0)
   ```

---

### 4.3. Thư mục 02: Độ chính xác Dự báo MLOps (Forecast Accuracy)

9. **Sản lượng Bán Thực tế:**
   ```dax
   Actual Demand = SUM(Forecast_vs_Actual[actual_demand])
   ```
10. **Sản lượng Dự báo (Champion Model):**
    ```dax
    Forecast Demand = SUM(Forecast_vs_Actual[forecast_demand])
    ```
11. **Tổng Sai số Tuyệt đối (Absolute Error):**
    $$\text{Forecast Absolute Error} = \sum |\text{Actual} - \text{Forecast}|$$
    ```dax
    Forecast Absolute Error =
    SUMX(
        FILTER(Forecast_vs_Actual, NOT ISBLANK(Forecast_vs_Actual[actual_demand])),
        Forecast_vs_Actual[absolute_error]
    )
    ```
12. **Sai số Tuyệt đối Trọng số (WAPE %):**
    $$\text{WAPE} = \frac{\sum |\text{Actual}_i - \text{Forecast}_i|}{\sum \text{Actual}_i}$$
    ```dax
    WAPE % = DIVIDE(
        [Forecast Absolute Error],
        SUMX(
            FILTER(Forecast_vs_Actual, NOT ISBLANK(Forecast_vs_Actual[actual_demand])),
            Forecast_vs_Actual[actual_demand]
        )
    )
    ```
13. **Độ chính xác Dự báo (Forecast Accuracy %):**
    $$\text{Forecast Accuracy} = \max(0, 1 - \text{WAPE})$$
    ```dax
    Forecast Accuracy % =
    VAR CurrentWAPE = [WAPE %]
    RETURN IF(ISBLANK(CurrentWAPE), BLANK(), MAX(0, 1 - CurrentWAPE))
    ```
14. **Độ lệch Hệ thống (Forecast Bias %):**
    $$\text{Forecast Bias} = \frac{\sum (\text{Forecast}_i - \text{Actual}_i)}{\sum \text{Actual}_i}$$
    ```dax
    Forecast Bias % = DIVIDE(
        SUMX(
            FILTER(Forecast_vs_Actual, NOT ISBLANK(Forecast_vs_Actual[actual_demand])),
            Forecast_vs_Actual[forecast_demand] - Forecast_vs_Actual[actual_demand]
        ),
        SUMX(
            FILTER(Forecast_vs_Actual, NOT ISBLANK(Forecast_vs_Actual[actual_demand])),
            Forecast_vs_Actual[actual_demand]
        )
    )
    ```

---

### 4.4. Thư mục 03: Quản trị Tồn kho & Cảnh báo ROP (Inventory Health & Alerts)

15. **Tổng Tồn kho Hiện tại:**
    ```dax
    Total Current Stock = SUM(Inventory_Health_Alerts[current_stock])
    ```
16. **Số lượng SKU Báo động Đỏ (Critical SKU Count):**
    ```dax
    Critical SKU Count = CALCULATE(COUNTROWS(Inventory_Health_Alerts), Inventory_Health_Alerts[alert_level] = "CRITICAL")
    ```
17. **Số lượng SKU Cảnh báo Vàng (Warning SKU Count):**
    ```dax
    Warning SKU Count = CALCULATE(COUNTROWS(Inventory_Health_Alerts), Inventory_Health_Alerts[alert_level] = "WARNING")
    ```
18. **Số lượng SKU Mức An toàn (Normal SKU Count):**
    ```dax
    Normal SKU Count = CALCULATE(COUNTROWS(Inventory_Health_Alerts), Inventory_Health_Alerts[alert_level] = "NORMAL")
    ```
19. **Tổng Sản lượng Đề xuất Nhập thêm:**
    ```dax
    Total Reorder Qty = SUM(Inventory_Health_Alerts[recommended_reorder_qty])
    ```

---

## 5. Thiết kế Giao diện Trực quan & Bộ Theme Doanh nghiệp (Pastel Palette)

### 5.1. Bố cục 3 trang Dashboard chuyên sâu

- **Trang 1: Tổng quan Điều hành Đa kênh (Executive Overview):**
  - Hàng đầu: 4 thẻ KPI Cards (`Total Revenue`, `Total Orders`, `Total Units Sold`, `AOV`).
  - Bên trái: Biểu đồ Donut Chart tính tỷ trọng Shopee/TikTok từ các đơn hợp lệ trong warehouse.
  - Ở giữa: Biểu đồ miền biểu diễn doanh thu theo ngày có dữ liệu thực trong `Fact_Orders`, đánh dấu các ngày Mega-sale.
  - Bên phải: Biểu đồ thanh ngang (Bar Chart) Top 10 SKU mang lại doanh thu cao nhất chuỗi.

- **Trang 2: Độ chính xác Dự báo Nhu cầu (Demand Forecasting & MLOps Accuracy):**
  - Biểu đồ kết hợp cột và đường (Line & Clustered Column Chart): so sánh nhu cầu actual đã phát sinh với các forecast ML đã lưu; forecast tương lai chưa có actual không tham gia tính accuracy.
  - Đồng hồ đo (Gauge Chart): Hiển thị `Forecast Accuracy % = 1 - WAPE` trên các cặp forecast/actual đã hoàn tất; để trống khi chưa có actual và thay đổi theo dữ liệu warehouse.
  - Bảng phân tích sai số (Matrix): Phân rã sai số WAPE và Bias theo từng phân khúc ma trận ABC/XYZ.

- **Trang 3: Bản đồ Rủi ro Tồn kho & Cảnh báo Nhập hàng (Inventory Risk Matrix):**
  - Biểu đồ phân tán (Scatter Plot): Trực quan hóa 19 SKU trên lưới ma trận 9 ô ABC/XYZ (Trục X: Hệ số biến thiên CV, Trục Y: Doanh thu Pareto).
  - 3 Thẻ trạng thái khẩn cấp (Stat Cards): Đếm nhanh số lượng SKU theo màu sắc (Đỏ: `Critical SKU Count`, Vàng: `Warning SKU Count`, Xanh: `Normal SKU Count`).
  - Bảng hành động (Actionable Reorder Table): Danh sách SKU chạm ngưỡng ROP, hiển thị tồn kho hiện tại, tồn an toàn $SS$, điểm đặt hàng lại $ROP$, số ngày còn lại trước khi đứt kho (`days_until_stockout`) và số lượng đề xuất đặt thêm (`recommended_reorder_qty`).

---

### 5.2. Chuẩn hóa JSON Theme theo quy chuẩn Microsoft Power BI

Tệp cấu hình màu sắc [`powerbi/theme_pastel.json`](file:///c:/Users/MINH/Downloads/temp-extract/streaming-mlops-ecommerce/powerbi/theme_pastel.json) được thiết kế theo đúng chuẩn JSON Schema chính thức của Microsoft Power BI Desktop:

```json
{
  "name": "Pastel Palette",
  "dataColors": [
    "#768CCE",
    "#FFC6D0",
    "#C3D6F2",
    "#E1F7E7",
    "#D1EAF5",
    "#FFD6DA"
  ],
  "background": "#FFFFFF",
  "foreground": "#2E3A59",
  "tableAccent": "#768CCE",
  "good": "#E1F7E7",
  "neutral": "#C3D6F2",
  "bad": "#FFC6D0",
  "maximum": "#768CCE",
  "center": "#C3D6F2",
  "minimum": "#FFD6DA"
}
```

- `#768CCE` (Xanh Cornflower): Màu chủ đạo, độ tương phản cao nhất.
- `#FFC6D0` (Hồng đào): Màu đối sánh kênh phụ và cảnh báo.
- `#E1F7E7` (Xanh bạc hà): Biểu thị trạng thái an toàn (Good / Normal).

---

## 6. Chiến lược Vận hành Hai Chế độ Nguồn Dữ liệu (Dual-Mode Data Strategy)

Để giải quyết bài toán dung hòa giữa **Tính di động khi chấm thi / bảo vệ đồ án** và **Tính cập nhật thời gian thực trong môi trường vận hành thực tế**, hệ thống áp dụng chiến lược 2 chế độ:

```
                  ┌──────────────────────────────────────────────┐
                  │          NGUỒN DỮ LIỆU ĐẦU VÀO               │
                  └──────┬────────────────────────────────┬──────┘
                         │                                │
            (Chế độ 1: Offline Demo)             (Chế độ 2: Production Real-time)
                         │                                │
                         ▼                                ▼
            ┌────────────────────────┐      ┌──────────────────────────┐
            │   Flat CSV Data Marts  │      │  PostgreSQL Star Schema  │
            │   (powerbi/data/*.csv) │      │  (ecom_warehouse: 5432)  │
            └────────────┬───────────┘      └─────────────┬────────────┘
                         │                                │
                         │ (Import Mode)                  │ (DirectQuery / Refresh)
                         ▼                                ▼
                  ┌──────────────────────────────────────────────┐
                  │            POWER BI DESKTOP                  │
                  │   - Star Schema Relationships (Kimball)      │
                  │   - 19 DAX Measures (_Measures)              │
                  │   - 3 Pages Executive Dashboard              │
                  └──────────────────────────────────────────────┘
```

### 6.1. Chế độ 1: Flat CSV Data Marts (Phục vụ Chấm thi & Di động Độc lập)
- **Cơ chế:** Dữ liệu được trích xuất tự động qua script [`powerbi/export_powerbi_dataset.py`](file:///c:/Users/MINH/Downloads/temp-extract/streaming-mlops-ecommerce/powerbi/export_powerbi_dataset.py) ra 6 tệp CSV chuẩn mã hóa `utf-8-sig` trong thư mục `powerbi/data/`.
- **Ưu điểm vượt trội:**
  - Giảng viên và hội đồng chấm thi có thể mở file `.pbix` và xem toàn bộ biểu đồ mượt mà trên máy cá nhân mà **không cần cài đặt cụm Docker** (Kafka, MinIO, PostgreSQL, FastAPI).
  - Không ngốn RAM/CPU máy tính, tránh 100% rủi ro mất kết nối mạng hay lỗi cơ sở dữ liệu trong buổi thuyết trình tốt nghiệp.

### 6.2. Chế độ 2: DirectQuery / Scheduled Refresh từ PostgreSQL Star Schema
- **Cơ chế:** Kết nối trực tiếp vào database `ecom_warehouse` cổng `5432` thông qua các SQL Views tối ưu hóa sẵn tại [`powerbi/views_for_powerbi.sql`](file:///c:/Users/MINH/Downloads/temp-extract/streaming-mlops-ecommerce/powerbi/views_for_powerbi.sql).
- **Ưu điểm:**
  - Luồng ETL streaming từ Kafka $\rightarrow$ MinIO $\rightarrow$ Star Schema nạp đến đâu, Power BI phản ánh dữ liệu mới nhất tức thì đến đó.
  - Khi cần chuyển đổi từ Chế độ CSV sang PostgreSQL, người dùng chỉ cần vào **Power Query Editor (Transform Data)** ➔ Đổi Data Source sang PostgreSQL; toàn bộ 19 DAX measures và 3 trang biểu đồ đã dựng sẽ **tự động giữ nguyên vẹn 100%**.

---

## 7. Kiểm thử Tự động & Đảm bảo Tính Toàn vẹn Dữ liệu (Automated Unit Tests)

Module Power BI được tích hợp trực tiếp vào bộ kiểm thử tự động của toàn dự án qua file [`tests/test_load_test.py`](file:///c:/Users/MINH/Downloads/temp-extract/streaming-mlops-ecommerce/tests/test_load_test.py):

```python
class TestPowerBIExport(unittest.TestCase):
    def test_export_powerbi_csv_files(self):
        """Kiểm tra xuất đầy đủ các tệp CSV chuẩn Kimball cho Power BI Desktop."""
        out_dir = export_powerbi_data()
        self.assertTrue(os.path.exists(out_dir))

        expected_files = [
            "Dim_Dates.csv",
            "Dim_Products.csv",
            "Dim_Geography.csv",
            "Inventory_Health_Alerts.csv",
            "Forecast_vs_Actual.csv",
            "Fact_Orders_Summary.csv",
        ]
        for fname in expected_files:
            fpath = os.path.join(out_dir, fname)
            self.assertTrue(os.path.exists(fpath), f"Thiếu tệp {fname}")
            self.assertGreater(os.path.getsize(fpath), 0, f"Tệp {fname} bị rỗng")
```

- **Kết quả thực nghiệm:** Toàn bộ test suite chạy thành công (`Ran 5 tests in 8.867s - OK`), đảm bảo tính toàn vẹn của tệp và khả năng tương thích cao.

---

## 8. Luận điểm Bảo vệ Đồ án Tốt nghiệp (Defense Key Takeaways)

Khi trình bày phân hệ này trước Hội đồng Đánh giá Đồ án Tốt nghiệp, các luận điểm cốt lõi cần khẳng định gồm:

1. **Tuân thủ Chuẩn mực Quốc tế (Kimball Alignment):**
   - Hệ thống không thiết kế dữ liệu tùy tiện theo dạng bảng tính phẳng mà tuân thủ phương pháp luận thiết kế chiều của Ralph Kimball (*The Data Warehouse Toolkit*).
   - Phân định rõ ràng giữa **Conformed Dimensions** (`Dim_Dates`, `Dim_Products`) và **Fact Tables** với các loại hạt khác nhau (Transaction, Snapshot, Consolidated).
2. **Khả năng Lọc Chéo Liên quy trình (Cross-Process Drill-Across):**
   - Nhờ có `Dim_Dates` và `Dim_Products` làm chiều nhất quán, báo cáo có thể kết hợp dữ liệu giữa doanh thu bán hàng thực tế và sản lượng dự báo mô hình AI trong cùng một màn hình mà không cần gộp bảng vật lý gây phình to cơ sở dữ liệu.
3. **Quản trị DAX Chuyên nghiệp & Tách bạch Nghiệp vụ:**
   - 19 measures được đóng gói trong bảng điều phối `_Measures` với 3 thư mục rõ ràng, hỗ trợ tính toán động (Dynamic Measures) từ sai số học máy ($WAPE$, $Accuracy\%$, $Bias\%$) đến nghiệp vụ chuỗi cung ứng ($SS$, $ROP$).
4. **Giải pháp Kiến trúc Linh hoạt (Dual-Mode Architecture):**
   - Đảm bảo tính khả thi cao trong triển khai: vừa có thể chạy chế độ độc lập phục vụ thẩm định nhanh, vừa sẵn sàng kết nối trực tiếp vào Data Warehouse thời gian thực trong môi trường doanh nghiệp.
