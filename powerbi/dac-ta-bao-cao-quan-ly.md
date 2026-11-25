# Đặc tả báo cáo Power BI cho người quản lý — mô hình star schema `mart` (16/11/2026)

Báo cáo đọc **chỉ schema `mart`** trong PostgreSQL: bảng vật lý dạng star schema, được làm mới bởi `mart.refresh()` sau mỗi chu kỳ ETL (khoảng 30 giây) và sau mỗi lượt dự báo theo lô. Định nghĩa bảng: `warehouse/ddl/05_mart_star_schema.sql`; logic làm mới: `warehouse/reporting/mart_refresh.sql`. Bài mô phỏng Refresh `scripts/check_powerbi_refresh_readiness.py` kiểm tra toàn bộ mart và đạt `PASS` ngày 16/11/2026.

**Báo cáo đã được dựng sẵn dạng PBIP:** `powerbi/BaoCaoQuanLy/BaoCaoQuanLy.pbip`, sinh bởi `scripts/build_powerbi_project.py`. Gồm 15 bảng (14 bảng mart + `_Measures`), 23 quan hệ, 48 measure, 6 trang, 54 visual. Mở tệp `.pbip` bằng Power BI Desktop, nhập tài khoản PostgreSQL, rồi bấm Refresh. Sửa báo cáo bằng cách chỉnh script rồi sinh lại, hoặc sửa trực tiếp trong Desktop và lưu (lưu ý: chạy lại script sẽ ghi đè các chỉnh sửa tay).

## 1. Mục tiêu và nguyên tắc

Báo cáo giúp người quản lý quyết định **đặt hàng gì, bao nhiêu, khi nào**, dựa trên dữ liệu bán thực tế và dự báo AI. Quyết định cuối cùng thuộc về người quản lý.

1. **Đi theo luồng quyết định:** rủi ro tồn kho → nhu cầu sắp tới → việc cần làm.
2. **Một nguồn sự thật cho mỗi con số:** mọi bảng lấy từ `mart`; đề xuất đặt hàng chỉ lấy từ `fact_replenishment_daily`.
3. **Cho thấy AI tin cậy đến đâu và phiên bản nào đã dự báo:** mọi dự báo của mọi phiên bản được giữ lại để so sánh trên cùng SKU, cùng ngày mục tiêu, cùng tầm dự báo.
4. **Cho thấy dữ liệu mới đến đâu:** thẻ "Cập nhật lúc" trên mọi trang và trang Độ tin cậy dữ liệu.

## 2. Kết nối

- **Get Data → PostgreSQL database**, server `localhost:5432`, database `ecom_warehouse`, chế độ **Import**. Dùng tài khoản chỉ đọc (`powerbi/import/README.md`).
- Trong Navigator, chọn các bảng của schema `mart` ở mục 3; không cần viết SQL.
- **Tắt Auto date/time:** File → Options → Current file → Data load.
- Không nạp `mart.refresh_state` và `mart.refresh_log`; đó là bảng kỹ thuật.

## 3. Mô hình dữ liệu

### 3.1. Bảng chiều

| Bảng | Khóa | Thuộc tính chính | Ghi chú |
|---|---|---|---|
| `dim_date` | `date_key` (yyyymmdd) | `full_date`, `day_name_vi`, `iso_day_of_week`, `month_label`, `year_month`, `year_month_label`, `quarter`, `year`, `is_weekend`, `is_mega_sale`, `is_data_confirmed` | **Mark as date table** theo `full_date`; liên tục, không có dòng trống |
| `dim_product` | `product_key` | `sku`, `product_name`, `category`, `abc_class`, `xyz_class`, `matrix_segment`, `service_level`, `reference_selling_price`, `current_lead_time_days` | Dòng −1 "Không xác định" |
| `dim_platform` | `platform_key` | `platform_name` (Shopee, TikTok Shop) | |
| `dim_shop` | `shop_key` | `shop_name`, `shop_id` | |
| `dim_geography` | `geo_key` | `region`, `state`, `city`, `district` | |
| `dim_payment` | `payment_key` | `payment_method`, `is_cod` | |
| `dim_carrier` | `carrier_key` | `carrier_name` | |
| `dim_order_status` | `status_key` | `order_status`, `status_group`, `funnel_order`, `is_final` | Nhóm: Chờ thanh toán, Đang xử lý, Đang giao, Đã giao, Hoàn tất, Hủy, Hoàn trả |
| `dim_model` | `model_key` | `version_label` (ví dụ "v4 · naive"), `model_name`, `model_version`, `training_data_origin`, `model_stage`, `cv_wape`, `cv_bias`, `is_serving` | Mỗi phiên bản từng phát dự báo |

Mọi bảng chiều trừ `dim_date` có dòng khóa **−1 "Không xác định"**: nguồn **thiếu** dữ liệu. Hai bảng chiều có thêm dòng **−2 "Không áp dụng"**: thuộc tính không có nghĩa với dòng fact đó.

| Dimension | −1 "Không xác định" | −2 "Không áp dụng" |
|---|---|---|
| `dim_carrier` | Đơn đã giao/hoàn tất (theo trạng thái hoặc mốc thời gian) mà thiếu đơn vị vận chuyển; 41 đơn ngày 16/11 | **"Chưa giao vận chuyển"**: đơn chưa từng giao (chờ thanh toán, đang xử lý, hủy); 42.750 đơn |
| `dim_model` | Đề xuất dùng dự báo AI nhưng không tìm thấy phiên bản | **"Không dùng dự báo AI"**: đề xuất dựa trên trung bình lịch sử hoặc không có dữ liệu nhu cầu; 18/88 SKU |

Nhờ vậy fact không có khóa trống, và biểu đồ phân biệt được "thiếu dữ liệu" (vấn đề chất lượng cần xử lý) với "chưa áp dụng" (trạng thái bình thường). Khi đếm dữ liệu thiếu, chỉ tính khóa −1.

### 3.2. Bảng fact

| Bảng | Loại | Độ hạt | Cột đo chính |
|---|---|---|---|
| `fact_sales_line` | Giao dịch | 1 dòng hàng | `quantity`, `net_quantity` (0 nếu hủy), `gross_amount`, `net_amount`, `discount_amount`, `platform_fees`, `is_cancelled` |
| `fact_sales_order` | Accumulating snapshot | 1 đơn | `net_units`, `net_amount`, `line_count`, `hours_to_pay`, `hours_to_ship`, `hours_to_complete`, `is_cancelled`, `order_hour`, `has_header_conflict` |
| `fact_inventory_daily` | Periodic snapshot | SKU × ngày | `on_hand`, `lead_time_days` |
| `fact_demand_forecast` | Periodic snapshot | SKU × ngày mục tiêu × phiên bản × ngày phát hành | `predicted_quantity`, `lower_bound`, `upper_bound`, `horizon_days`, `is_latest_for_target`, `actual_quantity`, `actual_complete`, `absolute_error` |
| `fact_replenishment_daily` | Periodic snapshot | SKU × ngày đánh giá | `on_hand`, `lead_time_days`, `demand_lead_time`, `safety_stock`, `reorder_point`, `order_up_to`, `days_of_cover`, `recommended_order_qty`, `order_value_at_selling_price`, `action_status`, `urgency`, `order_by_date`, `reason` |

`fact_replenishment_daily` giữ **lịch sử đề xuất theo ngày**: dòng hôm nay được cập nhật liên tục, từ ngày hôm sau thì chốt lại. Mỗi dòng có `model_key` của phiên bản đã tạo dự báo dùng cho đề xuất đó.

`fact_demand_forecast` giữ **mọi** dự báo của mọi phiên bản. Khi ngày mục tiêu đã qua và đã xác nhận đủ dữ liệu, `actual_quantity`, `absolute_error` được điền và `actual_complete = TRUE`.

### 3.3. Quan hệ

Tất cả là **1 → nhiều, lọc một chiều** từ dimension xuống fact. Quan hệ đánh dấu *(không hoạt động)* chỉ dùng qua `USERELATIONSHIP` trong measure.

| Dimension | Fact | Cột | Trạng thái |
|---|---|---|---|
| `dim_date` | `fact_sales_line` | `order_date_key` | Hoạt động |
| `dim_date` | `fact_sales_order` | `order_date_key` | Hoạt động |
| `dim_date` | `fact_sales_order` | `completed_date_key` | Không hoạt động |
| `dim_date` | `fact_inventory_daily` | `snapshot_date_key` | Hoạt động |
| `dim_date` | `fact_demand_forecast` | `target_date_key` | Hoạt động |
| `dim_date` | `fact_demand_forecast` | `issued_date_key` | Không hoạt động |
| `dim_date` | `fact_replenishment_daily` | `evaluation_date_key` | Hoạt động |
| `dim_product` | `fact_sales_line`, `fact_inventory_daily`, `fact_demand_forecast`, `fact_replenishment_daily` | `product_key` | Hoạt động |
| `dim_platform` | `fact_sales_line`, `fact_sales_order` | `platform_key` | Hoạt động |
| `dim_shop` | `fact_sales_line`, `fact_sales_order` | `shop_key` | Hoạt động |
| `dim_geography` | `fact_sales_line`, `fact_sales_order` | `geo_key` | Hoạt động |
| `dim_payment`, `dim_carrier` | `fact_sales_order` | `payment_key`, `carrier_key` | Hoạt động |
| `dim_order_status` | `fact_sales_line`, `fact_sales_order` | `status_key` | Hoạt động |
| `dim_model` | `fact_demand_forecast`, `fact_replenishment_daily` | `model_key` | Hoạt động |

Không tạo:
- `dim_shop[platform_key]` → `dim_platform`: tạo hai đường lọc tới fact, gây mơ hồ.
- `fact_replenishment_daily[inventory_date_key]` → `dim_date`.
- Quan hệ trực tiếp giữa hai fact. So sánh thực tế với dự báo bằng **drill-across** qua `dim_date` và `dim_product`, hoặc dùng cột `actual_quantity` đã chấm sẵn.

### 3.4. Sắp xếp, ẩn cột, định dạng

- **Sắp xếp:** `dim_date[day_name_vi]` theo `iso_day_of_week`; `month_label` theo `month`; `year_month_label` theo `year_month`; `dim_order_status[status_group]` theo `funnel_order`.
- **Thêm cột `status_order` trong Power Query** (bước `Table.AddColumn`) của `fact_replenishment_daily`: CRITICAL = 1, REORDER = 2, NO_DEMAND_DATA = 3, OK = 4; dùng để sắp xếp `action_status`. **Không dùng cột tính DAX** cho việc này: cột sắp xếp tính từ chính cột được sắp xếp gây lỗi "circular dependency".
- **Ẩn khỏi Report view:** mọi cột `*_key`, `source_loaded_at`, `refreshed_at`.
- **Định dạng:** tiền `#,0 "₫"`; tỷ lệ là phần trăm 1 chữ số thập phân.

### 3.5. Bảng màu trạng thái

| Trạng thái | Ý nghĩa | Màu |
|---|---|---|
| `CRITICAL` | Tồn hiện có ≤ tồn an toàn | Đỏ `#D64545` |
| `REORDER` | Tồn ≤ điểm đặt hàng | Cam `#E8912D` |
| `OK` | Chưa cần đặt | Xanh lá `#3A9D5D` |
| `NO_DEMAND_DATA` | Không có lịch sử bán hay dự báo | Xám `#9AA0A6` |

## 4. Measure DAX

Tạo bảng rỗng `_Measures` và đặt mọi measure vào đó.

### 4.1. Đơn hàng từ các sàn

```dax
Orders = CALCULATE ( COUNTROWS ( fact_sales_order ), fact_sales_order[is_cancelled] = FALSE () )
Orders All Status = COUNTROWS ( fact_sales_order )
Cancelled Orders = CALCULATE ( COUNTROWS ( fact_sales_order ), fact_sales_order[is_cancelled] = TRUE () )
Cancel Rate = DIVIDE ( [Cancelled Orders], [Orders All Status] )
Net Revenue = SUM ( fact_sales_order[net_amount] )
Units Sold = SUM ( fact_sales_line[net_quantity] )
AOV = DIVIDE ( [Net Revenue], [Orders] )
Platform Fees = SUM ( fact_sales_order[platform_fees] )
Avg Hours To Complete = CALCULATE ( AVERAGE ( fact_sales_order[hours_to_complete] ), fact_sales_order[is_cancelled] = FALSE () )
Orders Completed On Date =
    CALCULATE ( COUNTROWS ( fact_sales_order ), USERELATIONSHIP ( fact_sales_order[completed_date_key], dim_date[date_key] ) )
-- 7 ngày gần nhất tính đến hôm qua so với 7 ngày trước đó (DATEADD trên toàn khoảng thời gian luôn ra xấp xỉ 0).
Revenue Last 7D = VAR LastDay = MIN ( MAX ( dim_date[full_date] ), TODAY () - 1 )
    RETURN CALCULATE ( [Net Revenue], DATESINPERIOD ( dim_date[full_date], LastDay, -7, DAY ) )
Revenue Prev 7D = VAR LastDay = MIN ( MAX ( dim_date[full_date] ), TODAY () - 1 )
    RETURN CALCULATE ( [Net Revenue], DATESINPERIOD ( dim_date[full_date], LastDay - 7, -7, DAY ) )
Revenue 7D Change % = DIVIDE ( [Revenue Last 7D] - [Revenue Prev 7D], [Revenue Prev 7D] )
Revenue Share = DIVIDE ( [Net Revenue], CALCULATE ( [Net Revenue], ALL ( dim_platform ) ) )
```

`Units Sold` lấy từ `fact_sales_line` nên lọc được theo sản phẩm; `fact_sales_order` không có `product_key`. `net_amount` giữ nghĩa số tiền người mua trả của nguồn, không phải lợi nhuận.

### 4.2. Tồn kho (ảnh chụp hôm nay)

```dax
-- Các thẻ "hôm nay" luôn dùng ngày đánh giá mới nhất, bất kể slicer ngày.
Latest Evaluation Date Key = CALCULATE ( MAX ( fact_replenishment_daily[evaluation_date_key] ), ALL ( dim_date ) )
SKUs Tracked Today = CALCULATE ( COUNTROWS ( fact_replenishment_daily ),
    fact_replenishment_daily[evaluation_date_key] = [Latest Evaluation Date Key], REMOVEFILTERS ( dim_date ) )
SKUs Critical = CALCULATE ( [SKUs Tracked Today], fact_replenishment_daily[action_status] = "CRITICAL" )
SKUs To Reorder = CALCULATE ( [SKUs Tracked Today], fact_replenishment_daily[action_status] IN { "CRITICAL", "REORDER" } )
SKUs OK = CALCULATE ( [SKUs Tracked Today], fact_replenishment_daily[action_status] = "OK" )
SKUs No Demand Data = CALCULATE ( [SKUs Tracked Today], fact_replenishment_daily[action_status] = "NO_DEMAND_DATA" )
-- Loại hẳn giá trị trống (lọc thẳng trên cột từng cho 65 thay vì 19).
SKUs Stockout 7d = CALCULATE ( COUNTROWS ( FILTER ( fact_replenishment_daily,
    NOT ISBLANK ( fact_replenishment_daily[days_of_cover] ) && fact_replenishment_daily[days_of_cover] <= 7 ) ),
    fact_replenishment_daily[Is Latest Evaluation] = TRUE (), REMOVEFILTERS ( dim_date ) )
Stock On Hand Today = CALCULATE ( SUM ( fact_replenishment_daily[on_hand] ),
    fact_replenishment_daily[evaluation_date_key] = [Latest Evaluation Date Key], REMOVEFILTERS ( dim_date ) )
Median Days Of Cover = CALCULATE ( MEDIAN ( fact_replenishment_daily[days_of_cover] ),
    fact_replenishment_daily[evaluation_date_key] = [Latest Evaluation Date Key], REMOVEFILTERS ( dim_date ) )

-- Tồn kho theo ngày: chỉ cộng các SKU trong một ngày, không cộng dồn qua nhiều ngày.
-- Bán cộng dồn: mỗi SKU lấy tồn của lần chụp gần nhất tính đến ngày đang xét rồi mới cộng.
Stock Snapshot =
VAR d = MAX ( dim_date[full_date] )
RETURN SUMX ( VALUES ( dim_product[product_key] ),
    VAR LastKey = CALCULATE ( MAX ( fact_inventory_daily[snapshot_date_key] ), dim_date[full_date] <= d, REMOVEFILTERS ( dim_date ) )
    RETURN IF ( NOT ISBLANK ( LastKey ), CALCULATE ( SUM ( fact_inventory_daily[on_hand] ),
        fact_inventory_daily[snapshot_date_key] = LastKey, REMOVEFILTERS ( dim_date ) ) ) )
```

### 4.3. Dự báo AI và so sánh phiên bản

```dax
-- Chỉ dự báo của model chạy trên kho streaming (Forecast Upper/Lower dùng cùng điều kiện).
Forecast Units = CALCULATE ( SUM ( fact_demand_forecast[predicted_quantity] ), fact_demand_forecast[is_latest_for_target] = TRUE (),
    dim_model[training_data_origin] = "warehouse_live" )
Forecast Upper = IF ( HASONEVALUE ( dim_product[sku] ),
    CALCULATE ( SUM ( fact_demand_forecast[upper_bound] ), fact_demand_forecast[is_latest_for_target] = TRUE () ) )
Forecast Lower = IF ( HASONEVALUE ( dim_product[sku] ),
    CALCULATE ( SUM ( fact_demand_forecast[lower_bound] ), fact_demand_forecast[is_latest_for_target] = TRUE () ) )
Actual Units = SUM ( fact_sales_line[net_quantity] )

-- Độ chính xác thực tế: chỉ các dòng đã chấm (ngày mục tiêu đã xác nhận đủ dữ liệu).
Scored Forecast Error = CALCULATE ( SUM ( fact_demand_forecast[absolute_error] ), fact_demand_forecast[actual_complete] = TRUE () )
Scored Actual = CALCULATE ( SUM ( fact_demand_forecast[actual_quantity] ), fact_demand_forecast[actual_complete] = TRUE () )
Scored Predicted = CALCULATE ( SUM ( fact_demand_forecast[predicted_quantity] ), fact_demand_forecast[actual_complete] = TRUE () )
Live WAPE = DIVIDE ( [Scored Forecast Error], [Scored Actual] )
Live Bias = DIVIDE ( [Scored Predicted] - [Scored Actual], [Scored Actual] )
Scored Days = CALCULATE ( DISTINCTCOUNT ( fact_demand_forecast[target_date_key] ), fact_demand_forecast[actual_complete] = TRUE () )
Scored Forecasts = CALCULATE ( COUNTROWS ( fact_demand_forecast ), fact_demand_forecast[actual_complete] = TRUE () )

Backtest WAPE = DIVIDE ( MAX ( dim_model[cv_wape] ), 100 )
Backtest Bias = DIVIDE ( MAX ( dim_model[cv_bias] ), 100 )
Serving Model = CALCULATE ( MAX ( dim_model[version_label] ), dim_model[is_serving] = TRUE () )
```

**Bảng so sánh phiên bản cho công bằng.** Matrix: hàng `dim_model[version_label]`, cột `fact_demand_forecast[horizon_days]`, giá trị `Live WAPE`, `Live Bias`, `Scored Days`. Thêm slicer `horizon_days` và lọc `actual_complete = TRUE`. Để chỉ so trên **cùng SKU × ngày mà mọi phiên bản được chọn đều có dự báo**, dùng measure:

```dax
-- WAPE chỉ trên các bộ (SKU, ngày mục tiêu, tầm dự báo) mà MỌI phiên bản đang chọn đều đã được chấm.
Common WAPE =
VAR NModels = COUNTROWS ( ALLSELECTED ( dim_model[model_key] ) )
VAR CommonKeys =
    FILTER (
        CALCULATETABLE (
            SUMMARIZE ( fact_demand_forecast, fact_demand_forecast[product_key],
                        fact_demand_forecast[target_date_key], fact_demand_forecast[horizon_days] ),
            fact_demand_forecast[actual_complete] = TRUE (),
            ALLSELECTED ( dim_model )
        ),
        CALCULATE ( DISTINCTCOUNT ( fact_demand_forecast[model_key] ),
                    fact_demand_forecast[actual_complete] = TRUE (), ALLSELECTED ( dim_model ) ) = NModels
    )
RETURN
    DIVIDE (
        CALCULATE ( SUM ( fact_demand_forecast[absolute_error] ), fact_demand_forecast[actual_complete] = TRUE (), KEEPFILTERS ( CommonKeys ) ),
        CALCULATE ( SUM ( fact_demand_forecast[actual_quantity] ), fact_demand_forecast[actual_complete] = TRUE (), KEEPFILTERS ( CommonKeys ) )
    )
```

Trong matrix so sánh, đặt `Common WAPE` cạnh `Live WAPE`: `Live WAPE` dùng mọi ngày đã chấm của từng phiên bản; `Common WAPE` chỉ dùng phần giao nhau, nên công bằng khi các phiên bản phục vụ ở các giai đoạn khác nhau.

`Backtest Bias` âm nghĩa là AI dự báo **thiếu** (−23% với v4 ngày 16/11/2026), tức có rủi ro đứt hàng.

### 4.4. Đặt hàng nhà cung cấp

```dax
Recommended Qty Today = CALCULATE ( SUM ( fact_replenishment_daily[recommended_order_qty] ),
    fact_replenishment_daily[evaluation_date_key] = [Latest Evaluation Date Key], REMOVEFILTERS ( dim_date ) )
Order Value Today = CALCULATE ( SUM ( fact_replenishment_daily[order_value_at_selling_price] ),
    fact_replenishment_daily[evaluation_date_key] = [Latest Evaluation Date Key], REMOVEFILTERS ( dim_date ) )
SKUs Order Now = CALCULATE ( [SKUs Tracked Today], fact_replenishment_daily[urgency] = "Đặt ngay" )
SKUs Order This Week = CALCULATE ( [SKUs Tracked Today], fact_replenishment_daily[urgency] = "Đặt trong tuần" )
SKUs Default Lead Time = CALCULATE ( [SKUs Tracked Today], fact_replenishment_daily[lead_time_defaulted] = TRUE () )
-- Lịch sử đề xuất: dùng với trục dim_date (ngày đánh giá).
Recommended Qty = SUM ( fact_replenishment_daily[recommended_order_qty] )
```

`order_value_at_selling_price` tính theo **giá bán tham chiếu**, vì kho chưa có giá vốn. Tiêu đề visual ghi "theo giá bán".

### 4.5. Độ mới dữ liệu

```dax
Last Order Created = FORMAT ( CALCULATE ( MAX ( fact_sales_line[create_time] ), ALL ( fact_sales_line ) ), "dd/MM/yyyy HH:mm" )
Selected SKU = IF ( HASONEVALUE ( dim_product[sku] ), SELECTEDVALUE ( dim_product[product_name] ), "Chọn một SKU ở bộ lọc phía trên" )
Last Confirmed Day = CALCULATE ( MAX ( dim_date[full_date] ), dim_date[is_data_confirmed] = TRUE () )
```

## 5. Bố cục trang

Mọi trang có thanh trên cùng giống nhau: tiêu đề, thẻ `Last Order Created`, và slicer **Sàn** / **Danh mục** / **Nhóm ABC** nếu trang dùng được.

### Trang 1 — Sức khỏe tồn kho

| Vị trí | Visual | Trường / measure |
|---|---|---|
| Hàng KPI | 5 thẻ | `SKUs Critical`, `SKUs To Reorder`, `SKUs OK`, `SKUs Stockout 7d`, `Median Days Of Cover` |
| Trái giữa | Matrix ABC × XYZ | Hàng `dim_product[abc_class]`, cột `dim_product[xyz_class]`, giá trị `SKUs To Reorder` |
| Phải giữa | Thanh ngang | SKU theo `days_of_cover` tăng dần, màu theo `action_status` |
| Dưới | Bảng | `sku`, `product_name`, `abc_class`, `on_hand`, `avg_daily_demand`, `days_of_cover`, `safety_stock`, `reorder_point`, `action_status`; lọc ngày đánh giá mới nhất |
| Dưới phải | Đường | `Stock Snapshot` theo `dim_date[full_date]` (30 ngày gần nhất) |

### Trang 2 — Dự báo nhu cầu AI

| Vị trí | Visual | Trường / measure |
|---|---|---|
| Hàng KPI | Thẻ | `Serving Model`, `Backtest WAPE`, `Backtest Bias`, `Live WAPE`, `Scored Days` |
| Giữa | Đường | Trục `dim_date[full_date]` (60 ngày qua + 14 ngày tới): `Actual Units`, `Forecast Units`; `Forecast Lower`/`Forecast Upper` khi chọn 1 SKU |
| Dưới trái | Matrix so sánh phiên bản | Hàng `dim_model[version_label]`, cột `horizon_days`, giá trị `Live WAPE`, `Live Bias`, `Scored Days` |
| Dưới phải | Thẻ chữ | Model hiện là Naive; trên dữ liệu này model học máy chưa vượt baseline; bias âm nghĩa là dự báo thiếu |

### Trang 3 — Đặt hàng nhà cung cấp

| Vị trí | Visual | Trường / measure |
|---|---|---|
| Hàng KPI | Thẻ | `SKUs Order Now`, `SKUs Order This Week`, `Recommended Qty Today`, `Order Value Today` |
| Giữa | Bảng chính (ngày đánh giá mới nhất; `action_status` ∈ CRITICAL, REORDER) | `urgency`, `sku`, `product_name`, `abc_class`, `on_hand`, `lead_time_days`, `demand_lead_time`, `safety_stock`, `reorder_point`, `recommended_order_qty`, `order_by_date`, `version_label`, `reason` |
| Phải | Cột chồng | `Recommended Qty Today` theo `category`, màu theo `urgency` |
| Dưới | Đường lịch sử | `Recommended Qty` theo `dim_date[full_date]` (ngày đánh giá) |

Bật **Export data** cho bảng chính.

### Trang 4 — Đơn hàng từ các sàn

| Vị trí | Visual | Trường / measure |
|---|---|---|
| Hàng KPI | Thẻ | `Orders`, `Net Revenue`, `AOV`, `Units Sold`, `Cancel Rate`, `Revenue 7D Change %` |
| Giữa trái | Cột cụm theo ngày | `Net Revenue` theo `dim_date[full_date]`, chia `dim_platform[platform_name]` |
| Giữa phải | Donut | `Revenue Share` theo sàn |
| Dưới trái | Thanh | `Orders` theo `dim_geography[state]` |
| Dưới giữa | Cột | `Orders` theo `fact_sales_order[order_hour]` |
| Dưới phải | Thanh | `Orders All Status` theo `dim_order_status[status_group]` (phễu) |

### Trang phụ A — Chi tiết SKU

- Slicer `dim_product[sku]` (chọn một).
- Thẻ đề xuất hôm nay và `reason`.
- Đường thực tế so với dự báo kèm khoảng tin cậy.
- Đường tồn kho 60 ngày.
- Bảng lịch sử đề xuất theo ngày, kèm phiên bản model.

Trong Power BI Desktop có thể chuyển trang này thành drill-through: kéo `dim_product[sku]` vào ô Drill through.

### Trang phụ B — Độ tin cậy dữ liệu

- Thẻ: `Last Order Created`, `Last Confirmed Day`, `Serving Model`, số SKU dùng ngày giao hàng mặc định.
- Bảng `dim_model`: phiên bản, giai đoạn, WAPE/bias backtest, lần dự báo đầu/cuối.
- Biểu đồ: đơn có `has_header_conflict`.

## 6. Checklist production

1. Trước mỗi lần phát hành, chạy `python scripts/check_powerbi_refresh_readiness.py --label <nhãn>`; kết quả phải là `PASS`.
2. **Sau migration sửa dữ liệu ở `public`** (ví dụ gom shop), chạy `python -m warehouse.reporting.mart --full`. Lệnh này nạp lại fact, giữ lịch sử đề xuất.
3. **Refresh:**
   - Desktop dùng Refresh thủ công.
   - Power BI Service cần gateway và lịch refresh, đặt sau giờ job dự báo chạy.
4. Khi `fact_sales_line` lớn, bật **Incremental refresh** theo `order_date_key`.
5. **Quyền truy cập:** dùng tài khoản PostgreSQL chỉ đọc, chỉ cấp quyền trên schema `mart`. RLS theo `dim_platform` nếu cần chia quyền.
6. **Lưu trữ:** lưu báo cáo dạng **PBIP** trong git.

## 7. Giá trị đối chiếu (16/11/2026, khoảng 23:15)

| Kiểm tra | Giá trị |
|---|---|
| Số dòng `fact_sales_line` / tổng `quantity` / tổng `net_amount` | 157.901 / 377.710 / 79.244.673.830 ₫ (khớp `public`) |
| Số đơn `fact_sales_order` | 157.694 |
| `fact_demand_forecast` | 1.843 dòng, 5 phiên bản trong `dim_model` |
| `fact_replenishment_daily` hôm nay | 88 SKU |
| `dim_date` | 1.095 ngày liên tục, 319 ngày đã xác nhận đủ dữ liệu |

## 8. Giới hạn cần ghi trên báo cáo

- **Dữ liệu là mô phỏng**, kể cả các lô kiểm thử có nhãn. 01–15/11/2026 là dữ liệu mô phỏng bổ sung (`SIM26NOV-`).
- **Hàng đang về chưa được tính** vào vị thế tồn kho (hướng mở rộng).
- **Lead time** thiếu thì dùng 3 ngày (`lead_time_defaulted`).
- **Model v4 Naive (Staging):** backtest WAPE 58,22%, bias −23,07%. SKU mới chưa có lịch sử dùng heuristic và không được lưu dự báo.
- **`Live WAPE` và bảng so sánh phiên bản** có số từ khi ngày 16/11 được xác nhận (ngày 17/11), và chỉ có ý nghĩa sau khi tích lũy đủ ngày.
- **Lịch sử đề xuất** bắt đầu từ 16/11/2026.
- **Giá trị đề xuất** tính theo giá bán vì chưa có giá vốn và nhà cung cấp.
- **Tồn kho 01–14/11** đi ngang theo số ngày 31/10, vì không có bản tin tồn kho trong khoảng này (đợt bổ sung chỉ có đơn hàng). Ngày 15/11 tăng thêm khoảng 4,2K do 15 SKU kiểm thử `RPT…` mới. Từ 16/11, producer gửi lại số tồn thật.
- **Model Naive** lấy nhu cầu ngày gần nhất, nên tổng đề xuất và WAPE thực tế dao động mạnh khi khối lượng hai ngày liền nhau chênh lệch nhiều. Ví dụ 15/11 có 3.071 sản phẩm, 16/11 có 1.103 sản phẩm, nên tổng đề xuất giảm từ khoảng 26K (16/11) xuống 7,6K (17/11).

## 9. Điều chỉnh sau lần Refresh ngày 17/11/2026

| Trang | Điều chỉnh |
|---|---|
| Mọi trang | Thẻ "Đơn mới nhất" rộng hơn, không còn bị cắt chữ |
| Sức khỏe tồn kho | Hàng giữa cao hơn để ma trận ABC × XYZ hiện đủ dòng C |
| Dự báo AI | Bảng so sánh phiên bản đưa tầm dự báo xuống hàng. `Common WAPE` chỉ đếm các phiên bản có dự báo đã chấm (bản trước đếm cả phiên bản chưa chấm nên luôn trống) |
| Đặt hàng NCC | Lượng đề xuất theo danh mục chuyển sang thanh ngang (bản trước mất nhãn danh mục). Lịch sử đề xuất chuyển sang dạng cột |
| Đơn hàng | Thẻ đổi tên thành "Tăng trưởng DT 7 ngày". Biểu đồ donut thành biểu đồ tròn (tâm donut bị cắt số) |
| Chi tiết SKU | Thẻ, biểu đồ và bảng lịch sử chỉ hiện số khi chọn đúng 1 SKU (các measure `SKU …`). Bản trước cộng gộp mọi SKU khi để All |
| Dự báo AI (sau đối chiếu số) | Thẻ "WAPE thực tế (t+1)" dùng `Serving Live WAPE`: chỉ model đang phục vụ, tầm 1 ngày, cùng giao thức với WAPE backtest |
| Chi tiết SKU (sau đối chiếu số) | Dòng tổng của bảng lịch sử đề xuất để trống (trước đó cộng tồn kho của các ngày) |
| Độ tin cậy | Bỏ thẻ "Đơn mới nhất" trùng với thẻ đầu trang và dời nội dung lên. Hiển thị "Có/Không" và "Đã xác nhận/Chưa xác nhận" thay cho True/False (cột thêm trong Power Query) |
