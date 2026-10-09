# Tập hợp Công thức DAX (Data Analysis Expressions) cho Power BI Dashboard (Tuần 9)

Tài liệu này cung cấp toàn bộ các công thức DAX được tối ưu hóa cho mô hình dữ liệu Chấm sao (Star Schema) của dự án, phục vụ xây dựng Executive Dashboard trên Power BI Desktop.

---

## 1. Nhóm chỉ số Tài chính & Đơn hàng (Core Business Measures)

### 1.1. Tổng Doanh thu Thực nhận (Net Revenue)
```dax
Total Revenue = 
SUM(Fact_Orders[buyer_total_amount])
```

### 1.2. Tổng Sản lượng Bán ra (Units Sold)
```dax
Total Units Sold = 
SUM(Fact_Orders[quantity])
```

### 1.3. Tổng Số Đơn hàng Hợp lệ (Total Valid Orders)
```dax
Total Orders = 
CALCULATE(
    DISTINCTCOUNT(Fact_Orders[order_sn]),
    Fact_Orders[is_cancelled] = FALSE()
)
```

### 1.4. Giá trị Đơn hàng Trung bình (Average Order Value - AOV)
```dax
AOV = 
DIVIDE([Total Revenue], [Total Orders], 0)
```

### 1.5. Tỷ trọng Doanh thu Shopee vs TikTok Shop
```dax
Shopee Revenue = 
CALCULATE(
    [Total Revenue],
    Dim_Shops[platform] = "shopee"
)

TikTok Revenue = 
CALCULATE(
    [Total Revenue],
    Dim_Shops[platform] = "tiktok"
)

Shopee Share % = 
DIVIDE([Shopee Revenue], [Total Revenue], 0)

TikTok Share % = 
DIVIDE([TikTok Revenue], [Total Revenue], 0)
```

---

## 2. Nhóm chỉ số Quản trị Tồn kho & Cảnh báo (Inventory & Supply Chain DAX)

### 2.1. Lượng Tiêu thụ Trung bình Ngày ($\mu_d$)
```dax
Avg Daily Demand = 
DIVIDE(
    [Total Units Sold],
    DISTINCTCOUNT(Dim_Dates[date_key]),
    0
)
```

### 2.2. Độ lệch chuẩn Tiêu thụ Ngày ($\sigma_d$)
```dax
Std Daily Demand = 
STDEVX.S(
    VALUES(Dim_Dates[full_date]),
    CALCULATE([Total Units Sold])
)
```

### 2.3. Hệ số Biến thiên Nhu cầu (Coefficient of Variation - CV)
```dax
CV Volatility = 
DIVIDE([Std Daily Demand], [Avg Daily Demand], 0)
```

### 2.4. Tồn kho Hiện tại (Current Stock)
```dax
Current Stock = 
CALCULATE(
    SUM(Fact_Inventory_Daily[closing_stock]),
    LASTDATE(Dim_Dates[full_date])
)
```

### 2.5. Định mức Tồn kho An toàn Động (Dynamic Safety Stock)
Sử dụng tham số Lead Time $L = 3$ ngày và Service Level $95\%$ ($Z = 1.645$):
```dax
Safety Stock Dynamic = 
VAR Z_Score = 1.645
VAR Lead_Time = 3
VAR SS = Z_Score * [Std Daily Demand] * SQRT(Lead_Time)
RETURN
    CEILING(SS, 1)
```

### 2.6. Điểm Đặt hàng lại Động (Dynamic Reorder Point - ROP)
```dax
Reorder Point Dynamic = 
VAR Lead_Time = 3
VAR Demand_In_Lead_Time = [Avg Daily Demand] * Lead_Time
RETURN
    CEILING(Demand_In_Lead_Time + [Safety Stock Dynamic], 1)
```

### 2.7. Phân cấp Cảnh báo Rủi ro Đứt hàng (Stockout Risk Alert Level)
```dax
Alert Level = 
VAR CurStock = [Current Stock]
VAR SS = [Safety Stock Dynamic]
VAR ROP = [Reorder Point Dynamic]
RETURN
    IF(
        ISBLANK(CurStock),
        "NO_DATA",
        IF(
            CurStock <= SS,
            "CRITICAL",
            IF(CurStock <= ROP, "WARNING", "NORMAL")
        )
    )
```

### 2.8. Số lượng SKU rơi vào Báo động đỏ (Critical SKU Count)
```dax
Critical SKU Count = 
CALCULATE(
    DISTINCTCOUNT(Dim_Products[sku]),
    FILTER(
        VALUES(Dim_Products[sku]),
        [Alert Level] = "CRITICAL"
    )
)
```

### 2.9. Số ngày Dự kiến Còn lại trước khi Đứt hàng (Days Until Stockout)
```dax
Days Until Stockout = 
DIVIDE([Current Stock], [Avg Daily Demand], 0)
```

---

## 3. Nhóm chỉ số Đo lường Độ chính xác Mô hình (Forecast Accuracy DAX)

### 3.1. Sản lượng Thực tế vs Dự báo (Actual vs Forecast)
```dax
Actual Demand =
SUM(Forecast_vs_Actual[actual_demand])

Forecast Demand = 
SUM(Forecast_vs_Actual[forecast_demand])
```

### 3.2. Sai số Tuyệt đối Trọng số (Weighted Absolute Percentage Error - WAPE)
Chỉ số tiêu chuẩn bán lẻ tránh chia cho 0:
```dax
Forecast Absolute Error = 
SUMX(
    FILTER(
        Forecast_vs_Actual,
        NOT ISBLANK(Forecast_vs_Actual[actual_demand])
    ),
    Forecast_vs_Actual[absolute_error]
)

WAPE % = 
DIVIDE(
    [Forecast Absolute Error],
    SUMX(
        FILTER(
            Forecast_vs_Actual,
            NOT ISBLANK(Forecast_vs_Actual[actual_demand])
        ),
        Forecast_vs_Actual[actual_demand]
    )
)
```

### 3.3. Độ chính xác Dự báo (Forecast Accuracy %)
```dax
Forecast Accuracy % = 
VAR CurrentWAPE = [WAPE %]
RETURN
    IF(ISBLANK(CurrentWAPE), BLANK(), MAX(0, 1 - CurrentWAPE))
```

### 3.4. Độ lệch Hệ thống (Forecast Bias %)
```dax
Forecast Bias % = 
DIVIDE(
    SUMX(
        FILTER(
            Forecast_vs_Actual,
            NOT ISBLANK(Forecast_vs_Actual[actual_demand])
        ),
        Forecast_vs_Actual[forecast_demand] - Forecast_vs_Actual[actual_demand]
    ),
    SUMX(
        FILTER(
            Forecast_vs_Actual,
            NOT ISBLANK(Forecast_vs_Actual[actual_demand])
        ),
        Forecast_vs_Actual[actual_demand]
    )
)
```
- $\text{Bias} > 0$: Mô hình có xu hướng dự báo thừa (Over-forecasting) $\rightarrow$ Nguy cơ ứ đọng vốn.
- $\text{Bias} < 0$: Mô hình dự báo thiếu (Under-forecasting) $\rightarrow$ Nguy cơ thiếu hụt hàng.

> Các measure WAPE/Bias chỉ tính những dòng có `actual_demand`; dự báo tương lai chưa có nhãn thực tế sẽ không làm sai metric. `Forecast_vs_Actual` chỉ chứa dự báo ML được API ghi lại từ lịch sử warehouse; không dùng dòng heuristic/demo.
