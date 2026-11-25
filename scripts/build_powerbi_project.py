"""Sinh báo cáo Power BI dạng PBIP (Power BI Project) trên star schema `mart`.

    python scripts/build_powerbi_project.py                 # ghi vào powerbi/BaoCaoQuanLy/
    python scripts/build_powerbi_project.py --out <thư mục>

Đọc kiểu cột thật từ PostgreSQL (information_schema) để khai báo semantic model (model.bim, TMSL) và
sinh report định dạng PBIR (cùng phiên bản schema với Power BI Desktop 2.159 đang dùng). Mở
`BaoCaoQuanLy.pbip` bằng Power BI Desktop, nhập tài khoản PostgreSQL khi được hỏi, rồi Refresh.
Không đụng tới các tệp PBIX cũ. Chạy lại script sẽ ghi đè toàn bộ thư mục đầu ra (trừ .pbi/).
"""
import argparse
import hashlib
import json
from pathlib import Path
import shutil
import sys
import uuid

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
NAME = "BaoCaoQuanLy"
SCHEMA = "https://developer.microsoft.com/json-schemas/fabric/item/report/definition"
VISUAL_SCHEMA = f"{SCHEMA}/visualContainer/2.13.0/schema.json"
VERSION_AT_IMPORT = {"visual": "2.13.0", "report": "3.4.0", "page": "2.3.1"}
STATUS_COLORS = {"CRITICAL": "#D64545", "REORDER": "#E8912D", "OK": "#3A9D5D", "NO_DEMAND_DATA": "#9AA0A6"}

TABLES = ["dim_date", "dim_product", "dim_platform", "dim_shop", "dim_geography", "dim_payment", "dim_carrier",
          "dim_order_status", "dim_model", "fact_sales_line", "fact_sales_order", "fact_inventory_daily",
          "fact_demand_forecast", "fact_replenishment_daily"]
EXCLUDED_COLUMNS = {"source_loaded_at", "refreshed_at", "updated_at"}
ADDITIVE = {"fact_sales_line": {"quantity", "net_quantity", "gross_amount", "net_amount", "discount_amount", "platform_fees"},
            "fact_sales_order": {"gross_units", "net_units", "gross_amount", "net_amount", "discount_amount", "platform_fees",
                                 "shipping_fee", "line_count"},
            "fact_demand_forecast": {"predicted_quantity", "actual_quantity", "absolute_error"}}
WHOLE_QUANTITIES = {"safety_stock", "reorder_point", "order_up_to", "recommended_order_qty", "demand_lead_time",
                    "forecast_next_7d", "predicted_quantity", "actual_quantity", "absolute_error", "lower_bound", "upper_bound"}
MONEY = {"gross_amount", "net_amount", "discount_amount", "platform_fees", "shipping_fee", "reference_selling_price",
         "unit_price", "order_value_at_selling_price"}
SORT_BY = {("dim_date", "day_name_vi"): "iso_day_of_week", ("dim_date", "month_label"): "month",
           ("dim_date", "year_month_label"): "year_month", ("dim_order_status", "status_group"): "funnel_order",
           ("fact_replenishment_daily", "action_status"): "status_order"}
# Cột thêm trong Power Query (không dùng cột tính DAX: cột sắp xếp tính từ chính cột được sắp xếp
# gây lỗi vòng phụ thuộc trong Power BI).
M_COLUMNS = {
    "fact_replenishment_daily": [
        ("status_order", "int64",
         'each if [action_status] = "CRITICAL" then 1 else if [action_status] = "REORDER" then 2 '
         'else if [action_status] = "NO_DEMAND_DATA" then 3 else if [action_status] = "OK" then 4 else 9, Int64.Type'),
    ],
    "dim_model": [("serving_label", "string", 'each if [is_serving] = true then "Có" else "Không", type text')],
    "dim_date": [("confirmed_label", "string",
                  'each if [is_data_confirmed] = true then "Đã xác nhận" else "Chưa xác nhận", type text')],
}

# (bảng nhiều, cột, bảng một, cột, hoạt động)
RELATIONSHIPS = [
    ("fact_sales_line", "order_date_key", "dim_date", "date_key", True),
    ("fact_sales_order", "order_date_key", "dim_date", "date_key", True),
    ("fact_sales_order", "completed_date_key", "dim_date", "date_key", False),
    ("fact_inventory_daily", "snapshot_date_key", "dim_date", "date_key", True),
    ("fact_demand_forecast", "target_date_key", "dim_date", "date_key", True),
    ("fact_demand_forecast", "issued_date_key", "dim_date", "date_key", False),
    ("fact_replenishment_daily", "evaluation_date_key", "dim_date", "date_key", True),
    ("fact_sales_line", "product_key", "dim_product", "product_key", True),
    ("fact_inventory_daily", "product_key", "dim_product", "product_key", True),
    ("fact_demand_forecast", "product_key", "dim_product", "product_key", True),
    ("fact_replenishment_daily", "product_key", "dim_product", "product_key", True),
    ("fact_sales_line", "platform_key", "dim_platform", "platform_key", True),
    ("fact_sales_order", "platform_key", "dim_platform", "platform_key", True),
    ("fact_sales_line", "shop_key", "dim_shop", "shop_key", True),
    ("fact_sales_order", "shop_key", "dim_shop", "shop_key", True),
    ("fact_sales_line", "geo_key", "dim_geography", "geo_key", True),
    ("fact_sales_order", "geo_key", "dim_geography", "geo_key", True),
    ("fact_sales_order", "payment_key", "dim_payment", "payment_key", True),
    ("fact_sales_order", "carrier_key", "dim_carrier", "carrier_key", True),
    ("fact_sales_line", "status_key", "dim_order_status", "status_key", True),
    ("fact_sales_order", "status_key", "dim_order_status", "status_key", True),
    ("fact_demand_forecast", "model_key", "dim_model", "model_key", True),
    ("fact_replenishment_daily", "model_key", "dim_model", "model_key", True),
]

CALCULATED_COLUMNS = {
    "dim_date": [
        ("Last 30 Days", "boolean", "dim_date[full_date] >= TODAY () - 30 && dim_date[full_date] <= TODAY ()"),
        ("Last 60 Days", "boolean", "dim_date[full_date] >= TODAY () - 60 && dim_date[full_date] <= TODAY ()"),
        ("Forecast Window", "boolean", "dim_date[full_date] >= TODAY () - 60 && dim_date[full_date] <= TODAY () + 14"),
    ],
    "fact_replenishment_daily": [
        ("Is Latest Evaluation", "boolean",
         "fact_replenishment_daily[evaluation_date_key] = MAX ( fact_replenishment_daily[evaluation_date_key] )"),
    ],
}

LATEST = "fact_replenishment_daily[Is Latest Evaluation] = TRUE (), REMOVEFILTERS ( dim_date )"
PCT, INT, NUM, VND = "0.0%;-0.0%;0.0%", "#,0", "#,0.0", '#,0 "₫"'
MEASURES = [
    # (thư mục, tên, biểu thức, định dạng)
    ("1. Đơn hàng sàn", "Orders", "CALCULATE ( COUNTROWS ( fact_sales_order ), fact_sales_order[is_cancelled] = FALSE () )", INT),
    ("1. Đơn hàng sàn", "Orders All Status", "COUNTROWS ( fact_sales_order )", INT),
    ("1. Đơn hàng sàn", "Cancelled Orders", "CALCULATE ( COUNTROWS ( fact_sales_order ), fact_sales_order[is_cancelled] = TRUE () )", INT),
    ("1. Đơn hàng sàn", "Cancel Rate", "DIVIDE ( [Cancelled Orders], [Orders All Status] )", PCT),
    ("1. Đơn hàng sàn", "Net Revenue", "SUM ( fact_sales_order[net_amount] )", VND),
    ("1. Đơn hàng sàn", "Units Sold", "SUM ( fact_sales_line[net_quantity] )", INT),
    ("1. Đơn hàng sàn", "AOV", "DIVIDE ( [Net Revenue], [Orders] )", VND),
    ("1. Đơn hàng sàn", "Platform Fees", "SUM ( fact_sales_order[platform_fees] )", VND),
    ("1. Đơn hàng sàn", "Avg Hours To Complete",
     "CALCULATE ( AVERAGE ( fact_sales_order[hours_to_complete] ), fact_sales_order[is_cancelled] = FALSE () )", NUM),
    # 7 ngày gần nhất tính đến hôm qua (ngày trọn vẹn) so với 7 ngày trước đó. Bản đầu dùng DATEADD trên toàn
    # khoảng thời gian nên luôn ra xấp xỉ 0 khi không chọn ngày.
    ("1. Đơn hàng sàn", "Revenue Last 7D",
     "VAR LastDay = MIN ( MAX ( dim_date[full_date] ), TODAY () - 1 )\n"
     "RETURN CALCULATE ( [Net Revenue], DATESINPERIOD ( dim_date[full_date], LastDay, -7, DAY ) )", VND),
    ("1. Đơn hàng sàn", "Revenue Prev 7D",
     "VAR LastDay = MIN ( MAX ( dim_date[full_date] ), TODAY () - 1 )\n"
     "RETURN CALCULATE ( [Net Revenue], DATESINPERIOD ( dim_date[full_date], LastDay - 7, -7, DAY ) )", VND),
    ("1. Đơn hàng sàn", "Revenue 7D Change %", "DIVIDE ( [Revenue Last 7D] - [Revenue Prev 7D], [Revenue Prev 7D] )", PCT),
    ("1. Đơn hàng sàn", "Revenue Share", "DIVIDE ( [Net Revenue], CALCULATE ( [Net Revenue], ALL ( dim_platform ) ) )", PCT),
    ("2. Tồn kho", "SKUs Tracked Today", f"CALCULATE ( COUNTROWS ( fact_replenishment_daily ), {LATEST} )", INT),
    ("2. Tồn kho", "SKUs Critical", 'CALCULATE ( [SKUs Tracked Today], fact_replenishment_daily[action_status] = "CRITICAL" )', INT),
    ("2. Tồn kho", "SKUs To Reorder",
     'CALCULATE ( [SKUs Tracked Today], fact_replenishment_daily[action_status] IN { "CRITICAL", "REORDER" } )', INT),
    ("2. Tồn kho", "SKUs OK", 'CALCULATE ( [SKUs Tracked Today], fact_replenishment_daily[action_status] = "OK" )', INT),
    ("2. Tồn kho", "SKUs No Demand Data",
     'CALCULATE ( [SKUs Tracked Today], fact_replenishment_daily[action_status] = "NO_DEMAND_DATA" )', INT),
    # Loại hẳn giá trị trống: bản đầu lọc thẳng trên cột và ra 65 thay vì 19 (đối chiếu SQL 17/11/2026).
    ("2. Tồn kho", "SKUs Stockout 7d",
     "CALCULATE ( COUNTROWS ( FILTER ( fact_replenishment_daily, NOT ISBLANK ( fact_replenishment_daily[days_of_cover] ) "
     f"&& fact_replenishment_daily[days_of_cover] <= 7 ) ), {LATEST} )", INT),
    ("2. Tồn kho", "Stock On Hand Today", f"CALCULATE ( SUM ( fact_replenishment_daily[on_hand] ), {LATEST} )", INT),
    ("2. Tồn kho", "Median Days Of Cover", f"CALCULATE ( MEDIAN ( fact_replenishment_daily[days_of_cover] ), {LATEST} )", NUM),
    ("2. Tồn kho", "Days Of Cover Today", f"CALCULATE ( MIN ( fact_replenishment_daily[days_of_cover] ), {LATEST} )", NUM),
    # Snapshot là bán cộng dồn: mỗi SKU lấy tồn kho của lần chụp gần nhất tính đến ngày đang xét rồi mới cộng.
    # Bản đầu chỉ cộng các SKU có snapshot đúng ngày đó nên tụt gần 0 vào ngày ít snapshot.
    ("2. Tồn kho", "Stock Snapshot",
     "VAR d = MAX ( dim_date[full_date] )\n"
     "RETURN SUMX ( VALUES ( dim_product[product_key] ),\n"
     "    VAR LastKey = CALCULATE ( MAX ( fact_inventory_daily[snapshot_date_key] ), dim_date[full_date] <= d, REMOVEFILTERS ( dim_date ) )\n"
     "    RETURN IF ( NOT ISBLANK ( LastKey ), CALCULATE ( SUM ( fact_inventory_daily[on_hand] ),\n"
     "        fact_inventory_daily[snapshot_date_key] = LastKey, REMOVEFILTERS ( dim_date ) ) ) )", INT),
    ("2. Tồn kho", "Status Color",
     f"VAR s = CALCULATE ( SELECTEDVALUE ( fact_replenishment_daily[action_status] ), {LATEST} )\n"
     "RETURN SWITCH ( s, " + ", ".join(f'"{k}", "{v}"' for k, v in STATUS_COLORS.items()) + ', "#C8C8C8" )', None),
    ("2. Tồn kho", "Reason Today", f"CALCULATE ( SELECTEDVALUE ( fact_replenishment_daily[reason] ), {LATEST} )", None),
    # Chỉ dự báo của model chạy trên kho streaming (bản đầu trộn dự báo v1/v2 cho SKU của bộ CSV công khai).
    ("3. Dự báo AI", "Forecast Units",
     "CALCULATE ( SUM ( fact_demand_forecast[predicted_quantity] ), fact_demand_forecast[is_latest_for_target] = TRUE (), "
     'dim_model[training_data_origin] = "warehouse_live" )', NUM),
    ("3. Dự báo AI", "Forecast Upper",
     "IF ( HASONEVALUE ( dim_product[sku] ), CALCULATE ( SUM ( fact_demand_forecast[upper_bound] ), "
     'fact_demand_forecast[is_latest_for_target] = TRUE (), dim_model[training_data_origin] = "warehouse_live" ) )', NUM),
    ("3. Dự báo AI", "Forecast Lower",
     "IF ( HASONEVALUE ( dim_product[sku] ), CALCULATE ( SUM ( fact_demand_forecast[lower_bound] ), "
     'fact_demand_forecast[is_latest_for_target] = TRUE (), dim_model[training_data_origin] = "warehouse_live" ) )', NUM),
    ("3. Dự báo AI", "Actual Units", "SUM ( fact_sales_line[net_quantity] )", INT),
    ("3. Dự báo AI", "Scored Forecast Error",
     "CALCULATE ( SUM ( fact_demand_forecast[absolute_error] ), fact_demand_forecast[actual_complete] = TRUE () )", NUM),
    ("3. Dự báo AI", "Scored Actual",
     "CALCULATE ( SUM ( fact_demand_forecast[actual_quantity] ), fact_demand_forecast[actual_complete] = TRUE () )", NUM),
    ("3. Dự báo AI", "Scored Predicted",
     "CALCULATE ( SUM ( fact_demand_forecast[predicted_quantity] ), fact_demand_forecast[actual_complete] = TRUE () )", NUM),
    ("3. Dự báo AI", "Live WAPE", "DIVIDE ( [Scored Forecast Error], [Scored Actual] )", PCT),
    ("3. Dự báo AI", "Live Bias", "DIVIDE ( [Scored Predicted] - [Scored Actual], [Scored Actual] )", PCT),
    ("3. Dự báo AI", "Scored Days",
     "CALCULATE ( DISTINCTCOUNT ( fact_demand_forecast[target_date_key] ), fact_demand_forecast[actual_complete] = TRUE () )", INT),
    # Thẻ KPI: chỉ model đang phục vụ, tầm t+1 — cùng giao thức với WAPE backtest (origin_t_plus_1_pooled_wape_v2);
    # [Live WAPE] trên toàn bộ sẽ trộn nhiều phiên bản và tầm dự báo khi dữ liệu tích lũy.
    ("3. Dự báo AI", "Serving Live WAPE",
     "CALCULATE ( [Live WAPE], dim_model[is_serving] = TRUE (), fact_demand_forecast[horizon_days] = 1 )", PCT),
    ("3. Dự báo AI", "Serving Live Bias",
     "CALCULATE ( [Live Bias], dim_model[is_serving] = TRUE (), fact_demand_forecast[horizon_days] = 1 )", PCT),
    ("3. Dự báo AI", "Serving Scored Days",
     "CALCULATE ( [Scored Days], dim_model[is_serving] = TRUE (), fact_demand_forecast[horizon_days] = 1 )", INT),
    ("3. Dự báo AI", "Scored Forecasts",
     "CALCULATE ( COUNTROWS ( fact_demand_forecast ), fact_demand_forecast[actual_complete] = TRUE () )", INT),
    ("3. Dự báo AI", "Backtest WAPE",
     "DIVIDE ( CALCULATE ( MAX ( dim_model[cv_wape] ), dim_model[is_serving] = TRUE () ), 100 )", PCT),
    ("3. Dự báo AI", "Backtest Bias",
     "DIVIDE ( CALCULATE ( MAX ( dim_model[cv_bias] ), dim_model[is_serving] = TRUE () ), 100 )", PCT),
    ("3. Dự báo AI", "Serving Model", "CALCULATE ( MAX ( dim_model[version_label] ), dim_model[is_serving] = TRUE () )", None),
    ("3. Dự báo AI", "Common WAPE",
     # Chỉ đếm phiên bản có dự báo đã chấm; bản đầu đếm cả phiên bản chưa chấm nên tập chung rỗng (17/11/2026).
    "VAR NModels = CALCULATE ( DISTINCTCOUNT ( fact_demand_forecast[model_key] ), fact_demand_forecast[actual_complete] = TRUE (), ALLSELECTED ( dim_model ) )\n"
     "VAR CommonKeys =\n    FILTER (\n        CALCULATETABLE (\n"
     "            SUMMARIZE ( fact_demand_forecast, fact_demand_forecast[product_key], fact_demand_forecast[target_date_key], fact_demand_forecast[horizon_days] ),\n"
     "            fact_demand_forecast[actual_complete] = TRUE (), ALLSELECTED ( dim_model ) ),\n"
     "        CALCULATE ( DISTINCTCOUNT ( fact_demand_forecast[model_key] ), fact_demand_forecast[actual_complete] = TRUE (), ALLSELECTED ( dim_model ) ) = NModels )\n"
     "RETURN DIVIDE (\n"
     "    CALCULATE ( SUM ( fact_demand_forecast[absolute_error] ), fact_demand_forecast[actual_complete] = TRUE (), KEEPFILTERS ( CommonKeys ) ),\n"
     "    CALCULATE ( SUM ( fact_demand_forecast[actual_quantity] ), fact_demand_forecast[actual_complete] = TRUE (), KEEPFILTERS ( CommonKeys ) ) )", PCT),
    ("4. Đặt hàng NCC", "Recommended Qty Today", f"CALCULATE ( SUM ( fact_replenishment_daily[recommended_order_qty] ), {LATEST} )", INT),
    ("4. Đặt hàng NCC", "Order Value Today",
     f"CALCULATE ( SUM ( fact_replenishment_daily[order_value_at_selling_price] ), {LATEST} )", VND),
    ("4. Đặt hàng NCC", "SKUs Order Now", 'CALCULATE ( [SKUs Tracked Today], fact_replenishment_daily[urgency] = "Đặt ngay" )', INT),
    ("4. Đặt hàng NCC", "SKUs Order This Week",
     'CALCULATE ( [SKUs Tracked Today], fact_replenishment_daily[urgency] = "Đặt trong tuần" )', INT),
    ("4. Đặt hàng NCC", "SKUs Default Lead Time",
     "CALCULATE ( [SKUs Tracked Today], fact_replenishment_daily[lead_time_defaulted] = TRUE () )", INT),
    ("4. Đặt hàng NCC", "Recommended Qty", "SUM ( fact_replenishment_daily[recommended_order_qty] )", INT),
    ("5. Độ tin cậy", "Last Order Created",
     'FORMAT ( CALCULATE ( MAX ( fact_sales_line[create_time] ), ALL ( fact_sales_line ) ), "dd/MM/yyyy HH:mm" )', None),
    ("6. Chi tiết SKU", "Selected SKU",
     'IF ( HASONEVALUE ( dim_product[sku] ), SELECTEDVALUE ( dim_product[product_name] ), "Hãy chọn 1 SKU" )', None),
    # Trang SKU chỉ hiện số khi chọn đúng 1 SKU; để All thì bản đầu cộng gộp mọi SKU (17/11/2026).
    ("6. Chi tiết SKU", "SKU Stock On Hand", "IF ( HASONEVALUE ( dim_product[sku] ), [Stock On Hand Today] )", INT),
    ("6. Chi tiết SKU", "SKU Days Of Cover", "IF ( HASONEVALUE ( dim_product[sku] ), [Days Of Cover Today] )", NUM),
    ("6. Chi tiết SKU", "SKU Recommended Qty", "IF ( HASONEVALUE ( dim_product[sku] ), [Recommended Qty Today] )", INT),
    ("6. Chi tiết SKU", "SKU Reason",
     'IF ( HASONEVALUE ( dim_product[sku] ), [Reason Today], "Lý do hiển thị khi chọn 1 SKU" )', None),
    ("6. Chi tiết SKU", "SKU Actual Units", "IF ( HASONEVALUE ( dim_product[sku] ), [Actual Units] )", INT),
    ("6. Chi tiết SKU", "SKU Forecast Units", "IF ( HASONEVALUE ( dim_product[sku] ), [Forecast Units] )", NUM),
    ("6. Chi tiết SKU", "SKU Stock Snapshot", "IF ( HASONEVALUE ( dim_product[sku] ), [Stock Snapshot] )", INT),
    ("6. Chi tiết SKU", "SKU Day On Hand",
     "IF ( HASONEVALUE ( dim_product[sku] ) && HASONEVALUE ( dim_date[full_date] ), SUM ( fact_replenishment_daily[on_hand] ) )", INT),
    ("6. Chi tiết SKU", "SKU Day Reorder Point",
     "IF ( HASONEVALUE ( dim_product[sku] ) && HASONEVALUE ( dim_date[full_date] ), SUM ( fact_replenishment_daily[reorder_point] ) )", INT),
    ("6. Chi tiết SKU", "SKU Day Recommended",
     "IF ( HASONEVALUE ( dim_product[sku] ) && HASONEVALUE ( dim_date[full_date] ), SUM ( fact_replenishment_daily[recommended_order_qty] ) )", INT),
    ("5. Độ tin cậy", "Last Confirmed Day",
     'FORMAT ( CALCULATE ( MAX ( dim_date[full_date] ), dim_date[is_data_confirmed] = TRUE (), ALL ( dim_date ) ), "dd/MM/yyyy" )', None),
    ("5. Độ tin cậy", "Header Conflict Orders",
     "CALCULATE ( COUNTROWS ( fact_sales_order ), fact_sales_order[has_header_conflict] = TRUE () )", INT),
]


def stable_id(*parts, length=20):
    return hashlib.sha1("|".join(parts).encode("utf-8")).hexdigest()[:length]


# ═══════════ Semantic model ═══════════

def column_types(engine):
    from sqlalchemy import text
    with engine.connect() as conn:
        rows = conn.execute(text("""SELECT table_name, column_name, data_type FROM information_schema.columns
                                    WHERE table_schema = 'mart' ORDER BY table_name, ordinal_position""")).all()
    result = {}
    for table, column, pg_type in rows:
        if table in TABLES and column not in EXCLUDED_COLUMNS:
            result.setdefault(table, []).append((column, pg_type))
    missing = set(TABLES) - set(result)
    if missing:
        raise SystemExit(f"Thiếu bảng mart: {sorted(missing)} — chạy python -m warehouse.reporting.mart trước")
    tz = [f"{t}.{c}" for t, cols in result.items() for c, pg_type in cols if pg_type == "timestamp with time zone"]
    if tz:  # connector PostgreSQL của Power BI làm lỗi bước đổi múi giờ (lỗi gặp 16/11/2026)
        raise SystemExit(f"Cột TIMESTAMPTZ được Power BI nạp: {tz} — đổi sang TIMESTAMP giờ Việt Nam trong mart")
    return result


def tabular_type(pg_type):
    if pg_type in ("integer", "bigint", "smallint"):
        return "int64", "Int64.Type"
    if pg_type in ("numeric", "double precision", "real"):
        return "double", "type number"
    if pg_type == "boolean":
        return "boolean", "type logical"
    if pg_type == "date":
        return "dateTime", "type date"
    if pg_type == "timestamp without time zone":
        return "dateTime", "type datetime"
    if pg_type == "timestamp with time zone":
        return "dateTime", "type datetimezone"
    return "string", "type text"


def m_expression(table, columns):
    names = ", ".join(f'"{c}"' for c, _ in columns)
    types = ", ".join(f'{{"{c}", {tabular_type(t)[1]}}}' for c, t in columns)
    lines = ["let", "    Source = PostgreSQL.Database(pServer, pDatabase),",
             f'    Data = Source{{[Schema="mart",Item="{table}"]}}[Data],',
             f"    Selected = Table.SelectColumns(Data, {{{names}}}),",
             f"    Typed = Table.TransformColumnTypes(Selected, {{{types}}})"]
    last = "Typed"
    for i, (name, _, expression) in enumerate(M_COLUMNS.get(table, [])):
        step = f"Added{i}"
        lines[-1] += ","
        lines.append(f'    {step} = Table.AddColumn({last}, "{name}", {expression})')
        last = step
    return lines + ["in", f"    {last}"]


def build_model(types):
    tables = []
    for table in TABLES:
        cols = []
        m_columns = [(name, {"int64": "integer", "string": "text"}[data_type]) for name, data_type, _ in M_COLUMNS.get(table, [])]
        for column, pg_type in types[table] + m_columns:
            data_type, _ = tabular_type(pg_type)
            col = {"name": column, "dataType": data_type, "sourceColumn": column, "lineageTag": stable_id(table, column, length=32),
                   "summarizeBy": "sum" if column in ADDITIVE.get(table, set()) else "none"}
            if column.endswith("_key") or column in ("snapshot_time_known",) or column in dict(m_columns):
                col["isHidden"] = True
            if data_type == "dateTime":
                col["formatString"] = "dd/MM/yyyy" if pg_type == "date" else "dd/MM/yyyy HH:mm"
            elif column in MONEY:
                col["formatString"] = VND
            elif column in WHOLE_QUANTITIES:   # lượng hàng: không hiện ".0"
                col["formatString"] = INT
            elif data_type == "int64":
                col["formatString"] = "0" if column.endswith("_key") or column in ("year", "year_month", "order_hour") else INT
            elif data_type == "double":
                col["formatString"] = NUM
            if (table, column) in SORT_BY:
                col["sortByColumn"] = SORT_BY[(table, column)]
            if table == "dim_date" and column == "full_date":
                col["isKey"] = True
            cols.append(col)
        for name, data_type, expression in CALCULATED_COLUMNS.get(table, []):
            cols.append({"type": "calculated", "name": name, "dataType": data_type, "isDataTypeInferred": False,
                         "expression": expression, "summarizeBy": "none", "lineageTag": stable_id(table, name, length=32)})
        entry = {"name": table, "lineageTag": stable_id(table, length=32), "columns": cols,
                 "partitions": [{"name": table, "mode": "import",
                                 "source": {"type": "m", "expression": m_expression(table, types[table])}}]}
        if table == "dim_date":
            entry["dataCategory"] = "Time"
        tables.append(entry)
    measures = []
    for folder, name, expression, fmt in MEASURES:
        m = {"name": name, "expression": expression.split("\n") if "\n" in expression else expression,
             "displayFolder": folder, "lineageTag": stable_id("measure", name, length=32)}
        if fmt:
            m["formatString"] = fmt
        measures.append(m)
    tables.append({"name": "_Measures", "lineageTag": stable_id("_Measures", length=32),
                   "columns": [{"type": "calculatedTableColumn", "name": "Value", "dataType": "int64", "isNameInferred": True,
                                "isDataTypeInferred": True, "sourceColumn": "[Value]", "isHidden": True, "summarizeBy": "none",
                                "lineageTag": stable_id("_Measures", "Value", length=32)}],
                   "partitions": [{"name": "_Measures", "mode": "import",
                                   "source": {"type": "calculated", "expression": "ROW ( \"Value\", 0 )"}}],
                   "measures": measures})
    relationships = [{"name": str(uuid.uuid5(uuid.NAMESPACE_URL, f"{a}.{b}->{c}.{d}")), "fromTable": a, "fromColumn": b,
                      "toTable": c, "toColumn": d, **({} if active else {"isActive": False})}
                     for a, b, c, d, active in RELATIONSHIPS]
    expressions = [
        {"name": "pServer", "kind": "m", "lineageTag": stable_id("pServer", length=32),
         "expression": '"localhost:5432" meta [IsParameterQuery=true, Type="Text", IsParameterQueryRequired=true]',
         "annotations": [{"name": "PBI_ResultType", "value": "Text"}]},
        {"name": "pDatabase", "kind": "m", "lineageTag": stable_id("pDatabase", length=32),
         "expression": '"ecom_warehouse" meta [IsParameterQuery=true, Type="Text", IsParameterQueryRequired=true]',
         "annotations": [{"name": "PBI_ResultType", "value": "Text"}]},
    ]
    return {"compatibilityLevel": 1567, "model": {
        "culture": "vi-VN", "defaultPowerBIDataSourceVersion": "powerBI_V3", "sourceQueryCulture": "vi-VN",
        "dataAccessOptions": {"legacyRedirects": True, "returnErrorValuesAsNull": True},
        "tables": tables, "relationships": relationships, "expressions": expressions,
        "annotations": [{"name": "__PBI_TimeIntelligenceEnabled", "value": "0"},
                        {"name": "PBI_QueryOrder", "value": json.dumps(["pServer", "pDatabase", *TABLES])}]}}


# ═══════════ Report (PBIR) ═══════════

def lit(value):
    if isinstance(value, bool):
        return {"expr": {"Literal": {"Value": "true" if value else "false"}}}
    if isinstance(value, (int, float)):
        return {"expr": {"Literal": {"Value": f"{value}D"}}}
    return {"expr": {"Literal": {"Value": "'" + str(value).replace("'", "''") + "'"}}}


def col(table, column):
    return {"Column": {"Expression": {"SourceRef": {"Entity": table}}, "Property": column}}


def measure(name):
    return {"Measure": {"Expression": {"SourceRef": {"Entity": "_Measures"}}, "Property": name}}


def proj(field, display=None):
    inner = field.get("Column") or field.get("Measure")
    ref = f'{inner["Expression"]["SourceRef"]["Entity"]}.{inner["Property"]}'
    p = {"field": field, "queryRef": ref, "nativeQueryRef": inner["Property"]}
    if display:
        p["displayName"] = display
    return p


def m(name, display=None):
    return proj(measure(name), display)


def c(table, column, display=None):
    return proj(col(table, column), display)


def categorical_filter(table, column, values, exclude=False):
    condition = {"In": {"Expressions": [{"Column": {"Expression": {"SourceRef": {"Source": "t"}}, "Property": column}}],
                        "Values": [[lit(v)["expr"]] for v in values]}}
    if exclude:
        condition = {"Not": {"Expression": condition}}
    return {"name": stable_id("filter", table, column, str(exclude), *map(str, values)), "field": col(table, column),
            "type": "Categorical",
            "filter": {"Version": 2, "From": [{"Name": "t", "Entity": table, "Type": 0}], "Where": [{"Condition": condition}]}}


class Page:
    def __init__(self, key, title):
        self.id, self.title, self.visuals = stable_id("page", key), title, []

    def add(self, kind, x, y, w, h, roles=None, title=None, objects=None, filters=None, sort=None, extra=None):
        name = stable_id(self.id, str(len(self.visuals)), kind)
        visual = {"visualType": kind, "drillFilterOtherVisuals": True}
        if roles:
            visual["query"] = {"queryState": {role: {"projections": items} for role, items in roles.items()}}
            if sort:
                field, direction = sort
                visual["query"]["sortDefinition"] = {"sort": [{"field": field, "direction": direction}], "isDefaultSort": True}
        if objects:
            visual["objects"] = objects
        if title:
            visual["visualContainerObjects"] = {"title": [{"properties": {"show": lit(True), "text": lit(title)}}]}
        if extra:
            visual.update(extra)
        container = {"$schema": VISUAL_SCHEMA, "name": name,
                     "position": {"x": x, "y": y, "z": len(self.visuals), "width": w, "height": h, "tabOrder": len(self.visuals)},
                     "visual": visual}
        if filters:
            container["filterConfig"] = {"filters": filters}
        self.visuals.append(container)
        return container

    def text(self, x, y, w, h, value, size="11pt", bold=False):
        style = {"fontSize": size}
        if bold:
            style["fontWeight"] = "bold"
        paragraphs = [{"textRuns": [{"value": line, "textStyle": style}]} for line in value.split("\n")]
        return self.add("textbox", x, y, w, h, objects={"general": [{"properties": {"paragraphs": paragraphs}}]})

    def header(self, subtitle, slicers=(), single_select=()):
        self.text(16, 8, 860, 44, self.title, "20pt", True)
        self.text(16, 48, 860, 26, subtitle, "10pt")
        self.add("cardVisual", 900, 10, 364, 60, {"Data": [m("Last Order Created", "Đơn mới nhất")]})
        for i, (table, column, label) in enumerate(slicers):
            objects = {"data": [{"properties": {"mode": lit("Dropdown")}}]}
            if (table, column) in single_select:
                objects["selection"] = [{"properties": {"singleSelect": lit(True)}}]
            self.add("slicer", 16 + i * 212, 80, 204, 56, {"Values": [c(table, column, label)]}, objects=objects)

    def kpis(self, y, items):
        self.add("cardVisual", 16, y, 1248, 96, {"Data": [m(n, d) for n, d in items]})


def color_by_status():
    return {"values": [{"properties": {"backColor": {"solid": {"color": {"expr": measure("Status Color")}}}},
                        "selector": {"metadata": "fact_replenishment_daily.action_status"}}]}


def bar_fill_by_status():
    return {"dataPoint": [{"properties": {"fill": {"solid": {"color": {"expr": measure("Status Color")}}}},
                           "selector": {"data": [{"dataViewWildcard": {"matchingOption": 1}}]}}]}


def build_pages():
    latest = categorical_filter("fact_replenishment_daily", "Is Latest Evaluation", [True])
    common_slicers = [("dim_product", "category", "Danh mục"), ("dim_product", "abc_class", "Nhóm ABC")]
    pages = []

    p = Page("inventory", "Sức khỏe tồn kho")
    p.header("SKU nào sắp hết, SKU nào tồn đọng — ảnh chụp ngày đánh giá mới nhất", common_slicers)
    p.kpis(144, [("SKUs Critical", "SKU nguy cấp"), ("SKUs To Reorder", "SKU cần đặt"), ("SKUs OK", "SKU ổn định"),
                 ("SKUs Stockout 7d", "Hết hàng trong 7 ngày"), ("Median Days Of Cover", "Số ngày tồn (trung vị)")])
    p.add("pivotTable", 16, 250, 400, 236, {"Rows": [c("dim_product", "abc_class", "ABC")],
                                            "Columns": [c("dim_product", "xyz_class", "XYZ")],
                                            "Values": [m("SKUs To Reorder", "SKU cần đặt")]}, title="Ma trận ABC × XYZ: số SKU cần đặt")
    to_order = categorical_filter("fact_replenishment_daily", "action_status", ["CRITICAL", "REORDER"])
    p.add("clusteredBarChart", 426, 250, 838, 236, {"Category": [c("dim_product", "sku", "SKU")],
                                                    "Y": [m("Days Of Cover Today", "Số ngày tồn")]},
          title="SKU cần đặt: số ngày tồn còn lại (đỏ = nguy cấp, cam = cần đặt)", objects=bar_fill_by_status(),
          sort=(measure("Days Of Cover Today"), "Ascending"), filters=[latest, to_order])
    p.add("tableEx", 16, 496, 838, 212, {"Values": [
        c("dim_product", "sku", "SKU"), c("dim_product", "product_name", "Sản phẩm"), c("dim_product", "abc_class", "ABC"),
        c("fact_replenishment_daily", "on_hand", "Tồn"), c("fact_replenishment_daily", "avg_daily_demand", "Nhu cầu/ngày"),
        c("fact_replenishment_daily", "days_of_cover", "Ngày tồn"), c("fact_replenishment_daily", "safety_stock", "Tồn an toàn"),
        c("fact_replenishment_daily", "reorder_point", "Điểm đặt hàng"), c("fact_replenishment_daily", "action_status", "Trạng thái")]},
        title="Chi tiết tồn kho theo SKU (nguy cấp trước)", objects=color_by_status(), filters=[latest],
        sort=(col("fact_replenishment_daily", "action_status"), "Ascending"))   # sắp theo status_order
    p.add("lineChart", 864, 496, 400, 212, {"Category": [c("dim_date", "full_date", "Ngày")],
                                            "Y": [m("Stock Snapshot", "Tồn kho")]},
          title="Tổng tồn kho 30 ngày (số gần nhất của mỗi SKU)",
          filters=[categorical_filter("dim_date", "Last 30 Days", [True])])
    pages.append(p)

    p = Page("forecast", "Dự báo nhu cầu AI")
    p.header("14 ngày tới bán bao nhiêu và AI tin cậy đến đâu", common_slicers + [("dim_product", "sku", "SKU")])
    p.kpis(144, [("Serving Model", "Model đang dùng"), ("Backtest WAPE", "WAPE backtest"), ("Backtest Bias", "Bias backtest"),
                 ("Serving Live WAPE", "WAPE thực tế (t+1)"), ("Serving Scored Days", "Số ngày đã chấm")])
    p.add("lineChart", 16, 250, 1248, 240, {"Category": [c("dim_date", "full_date", "Ngày")],
                                            "Y": [m("Actual Units", "Thực tế"), m("Forecast Units", "Dự báo"),
                                                  m("Forecast Lower", "Cận dưới (1 SKU)"), m("Forecast Upper", "Cận trên (1 SKU)")]},
          title="Thực tế và dự báo (60 ngày qua + 14 ngày tới)",
          filters=[categorical_filter("dim_date", "Forecast Window", [True])])
    p.add("pivotTable", 16, 500, 700, 208, {"Rows": [c("dim_model", "version_label", "Phiên bản"),
                                                     c("fact_demand_forecast", "horizon_days", "Tầm dự báo (ngày)")],
                                            "Values": [m("Live WAPE", "WAPE"), m("Common WAPE", "WAPE chung"),
                                                       m("Live Bias", "Bias"), m("Scored Days", "Ngày chấm")]},
          title="So sánh phiên bản trên dữ liệu đã chấm")
    p.text(726, 500, 538, 208,
           "Cách đọc\n• Model hiện tại là Naive (lấy nhu cầu ngày gần nhất). Trên dữ liệu mô phỏng này, model học máy "
           "chưa vượt baseline.\n• Bias âm = AI dự báo thiếu → rủi ro đứt hàng; cân nhắc tăng tồn an toàn.\n"
           "• WAPE chung chỉ so trên cùng SKU × ngày × tầm dự báo mà mọi phiên bản được chọn đều có, nên công bằng giữa các phiên bản.\n"
           "• Số thực tế chỉ được chấm khi ngày đã được xác nhận đủ dữ liệu.", "10pt")
    pages.append(p)

    p = Page("replenishment", "Đặt hàng nhà cung cấp")
    p.header("Đặt SKU nào, bao nhiêu, trước ngày nào và vì sao — mức phục vụ A 98% / B 95% / C 90%", common_slicers)
    p.kpis(144, [("SKUs Order Now", "SKU đặt ngay"), ("SKUs Order This Week", "SKU đặt trong tuần"),
                 ("Recommended Qty Today", "Tổng lượng đề xuất"), ("Order Value Today", "Giá trị (theo giá bán)")])
    p.add("tableEx", 16, 250, 1248, 280, {"Values": [
        c("fact_replenishment_daily", "urgency", "Mức khẩn"), c("dim_product", "sku", "SKU"),
        c("dim_product", "product_name", "Sản phẩm"), c("dim_product", "abc_class", "ABC"),
        c("fact_replenishment_daily", "on_hand", "Tồn"), c("fact_replenishment_daily", "lead_time_days", "Lead time"),
        c("fact_replenishment_daily", "demand_lead_time", "Nhu cầu trong lead time"),
        c("fact_replenishment_daily", "safety_stock", "Tồn an toàn"), c("fact_replenishment_daily", "reorder_point", "Điểm đặt hàng"),
        c("fact_replenishment_daily", "recommended_order_qty", "Đề xuất đặt"), c("fact_replenishment_daily", "order_by_date", "Đặt trước ngày"),
        c("dim_model", "version_label", "Phiên bản dự báo"), c("fact_replenishment_daily", "reason", "Lý do")]},
        title="Danh sách đề xuất đặt hàng (xuất dữ liệu: ⋯ → Export data)",
        filters=[latest, categorical_filter("fact_replenishment_daily", "action_status", ["CRITICAL", "REORDER"])],
        sort=(col("fact_replenishment_daily", "order_by_date"), "Ascending"))
    p.add("clusteredBarChart", 16, 540, 620, 168, {"Category": [c("dim_product", "category", "Danh mục")],
                                                   "Y": [m("Recommended Qty Today", "Lượng đề xuất")]},
          title="Lượng đề xuất theo danh mục", sort=(measure("Recommended Qty Today"), "Descending"))
    p.add("clusteredColumnChart", 646, 540, 618, 168, {"Category": [c("dim_date", "full_date", "Ngày đánh giá")],
                                                       "Y": [m("Recommended Qty", "Lượng đề xuất")]},
          title="Lịch sử tổng lượng đề xuất theo ngày")
    pages.append(p)

    p = Page("sales", "Đơn hàng từ các sàn")
    p.header("Bán được bao nhiêu, ở đâu, qua sàn nào, hủy bao nhiêu",
             [("dim_platform", "platform_name", "Sàn"), ("dim_date", "year_month_label", "Tháng")])
    p.kpis(144, [("Orders", "Đơn hợp lệ"), ("Net Revenue", "Doanh thu"), ("AOV", "Giá trị đơn TB"), ("Units Sold", "Sản phẩm bán"),
                 ("Cancel Rate", "Tỷ lệ hủy"), ("Revenue 7D Change %", "Tăng trưởng DT 7 ngày")])
    p.add("clusteredColumnChart", 16, 250, 830, 220, {"Category": [c("dim_date", "full_date", "Ngày")],
                                                      "Y": [m("Net Revenue", "Doanh thu")],
                                                      "Series": [c("dim_platform", "platform_name", "Sàn")]},
          title="Doanh thu theo ngày (60 ngày gần nhất)", filters=[categorical_filter("dim_date", "Last 60 Days", [True])])
    p.add("pieChart", 856, 250, 408, 220, {"Category": [c("dim_platform", "platform_name", "Sàn")],
                                             "Y": [m("Net Revenue", "Doanh thu")]}, title="Tỷ trọng doanh thu theo sàn")
    p.add("clusteredBarChart", 16, 480, 410, 228, {"Category": [c("dim_geography", "state", "Tỉnh/thành")],
                                                   "Y": [m("Orders", "Đơn")]},
          title="Đơn theo tỉnh/thành", sort=(measure("Orders"), "Descending"))
    p.add("clusteredColumnChart", 436, 480, 410, 228, {"Category": [c("fact_sales_order", "order_hour", "Giờ")],
                                                       "Y": [m("Orders", "Đơn")]}, title="Đơn theo giờ trong ngày",
          sort=(col("fact_sales_order", "order_hour"), "Ascending"))
    p.add("clusteredBarChart", 856, 480, 408, 228, {"Category": [c("dim_order_status", "status_group", "Trạng thái")],
                                                    "Y": [m("Orders All Status", "Đơn")]}, title="Phễu trạng thái đơn",
          sort=(col("dim_order_status", "status_group"), "Ascending"))
    pages.append(p)

    p = Page("sku", "Chi tiết SKU")
    p.header("Chọn một SKU để xem đề xuất, dự báo và lịch sử", [("dim_product", "sku", "SKU")],
             single_select=[("dim_product", "sku")])
    p.kpis(144, [("Selected SKU", "Sản phẩm"), ("SKU Stock On Hand", "Tồn hiện có"), ("SKU Days Of Cover", "Số ngày tồn"),
                 ("SKU Recommended Qty", "Đề xuất đặt")])
    p.add("cardVisual", 16, 248, 1248, 64, {"Data": [m("SKU Reason", "Lý do đề xuất")]})
    p.add("lineChart", 16, 320, 620, 200, {"Category": [c("dim_date", "full_date", "Ngày")],
                                           "Y": [m("SKU Actual Units", "Thực tế"), m("SKU Forecast Units", "Dự báo"),
                                                 m("Forecast Lower", "Cận dưới"), m("Forecast Upper", "Cận trên")]},
          title="Thực tế và dự báo", filters=[categorical_filter("dim_date", "Forecast Window", [True])])
    p.add("lineChart", 646, 320, 618, 200, {"Category": [c("dim_date", "full_date", "Ngày")], "Y": [m("SKU Stock Snapshot", "Tồn kho")]},
          title="Tồn kho 60 ngày", filters=[categorical_filter("dim_date", "Last 60 Days", [True])])
    p.add("tableEx", 16, 530, 1248, 178, {"Values": [
        c("dim_date", "full_date", "Ngày đánh giá"), m("SKU Day On Hand", "Tồn"),
        m("SKU Day Reorder Point", "Điểm đặt hàng"), m("SKU Day Recommended", "Đề xuất"),
        c("fact_replenishment_daily", "action_status", "Trạng thái"), c("dim_model", "version_label", "Phiên bản dự báo")]},
        title="Lịch sử đề xuất theo ngày", sort=(col("dim_date", "full_date"), "Descending"))
    pages.append(p)

    p = Page("trust", "Độ tin cậy dữ liệu")
    p.header("Dữ liệu mới đến đâu, model nào đang phục vụ, chất lượng dữ liệu")
    p.kpis(88, [("Last Confirmed Day", "Ngày xác nhận gần nhất"),
                 ("Serving Model", "Model đang dùng"), ("SKUs Default Lead Time", "SKU dùng lead time mặc định"),
                 ("Header Conflict Orders", "Đơn xung đột header")])
    p.add("tableEx", 16, 194, 1248, 236, {"Values": [
        c("dim_model", "version_label", "Phiên bản"), c("dim_model", "model_stage", "Giai đoạn"),
        c("dim_model", "training_data_origin", "Nguồn huấn luyện"), c("dim_model", "cv_wape", "WAPE backtest (đơn vị %)"),
        c("dim_model", "cv_bias", "Bias backtest (đơn vị %)"), c("dim_model", "first_forecast_at", "Dự báo đầu"),
        c("dim_model", "last_forecast_at", "Dự báo cuối"), c("dim_model", "serving_label", "Đang phục vụ")]},
        title="Các phiên bản model đã phát dự báo (WAPE/bias chỉ có từ khi có snapshot phục vụ)",
        filters=[categorical_filter("dim_model", "training_data_origin", ["unknown", "not_applicable"], exclude=True)])
    p.add("clusteredColumnChart", 16, 440, 1248, 268, {"Category": [c("dim_date", "full_date", "Ngày")],
                                                       "Y": [m("Orders All Status", "Đơn")],
                                                       "Series": [c("dim_date", "confirmed_label", "Xác nhận dữ liệu")]},
          title="Số đơn theo ngày và trạng thái xác nhận dữ liệu (30 ngày)",
          filters=[categorical_filter("dim_date", "Last 30 Days", [True])])
    pages.append(p)
    return pages


THEME = {"name": "QuanLyKho", "dataColors": ["#3A6EA5", "#E8912D", "#3A9D5D", "#D64545", "#7A5BA6", "#9AA0A6", "#2E8B9A", "#C9A227"],
         "background": "#FFFFFF", "foreground": "#252423", "tableAccent": "#3A6EA5",
         "textClasses": {"title": {"fontFace": "Segoe UI Semibold", "fontSize": 12},
                         "callout": {"fontFace": "Segoe UI Semibold", "fontSize": 24}}}


def write(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def build(out, template_theme):
    from warehouse.etl.load import get_engine
    engine = get_engine()
    try:
        types = column_types(engine)
    finally:
        engine.dispose()
    for sub in (f"{NAME}.SemanticModel", f"{NAME}.Report"):
        target = out / sub
        if target.exists():
            for child in target.iterdir():
                if child.name != ".pbi":
                    shutil.rmtree(child) if child.is_dir() else child.unlink()
    out.mkdir(parents=True, exist_ok=True)
    (out / ".gitignore").write_text("**/.pbi/localSettings.json\n**/.pbi/cache.abf\n", encoding="utf-8")
    write(out / f"{NAME}.pbip", {"version": "1.0", "artifacts": [{"report": {"path": f"{NAME}.Report"}}],
                                 "settings": {"enableAutoRecovery": True}})
    sm = out / f"{NAME}.SemanticModel"
    write(sm / "definition.pbism", {"$schema": "https://developer.microsoft.com/json-schemas/fabric/item/semanticModel/definitionProperties/1.0.0/schema.json",
                                    "version": "1.0", "settings": {}})
    model = build_model(types)
    write(sm / "model.bim", model)
    rp = out / f"{NAME}.Report"
    write(rp / "definition.pbir", {"$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definitionProperties/2.0.0/schema.json",
                                   "version": "4.0", "datasetReference": {"byPath": {"path": f"../{NAME}.SemanticModel"}}})
    d = rp / "definition"
    write(d / "version.json", {"$schema": f"{SCHEMA}/versionMetadata/1.0.0/schema.json", "version": "2.0.0"})
    base_theme = template_theme.name.removesuffix(".json")
    write(d / "report.json", {
        "$schema": f"{SCHEMA}/report/3.3.0/schema.json",
        "themeCollection": {"baseTheme": {"name": base_theme, "reportVersionAtImport": VERSION_AT_IMPORT, "type": "SharedResources"},
                            "customTheme": {"name": "QuanLyKho.json", "reportVersionAtImport": VERSION_AT_IMPORT, "type": "RegisteredResources"}},
        "resourcePackages": [{"name": "SharedResources", "type": "SharedResources",
                              "items": [{"name": base_theme, "path": f"BaseThemes/{template_theme.name}", "type": "BaseTheme"}]},
                             {"name": "RegisteredResources", "type": "RegisteredResources",
                              "items": [{"name": "QuanLyKho.json", "path": "QuanLyKho.json", "type": "CustomTheme"}]}],
        "settings": {"useStylableVisualContainerHeader": True, "exportDataMode": "AllowSummarized",
                     "defaultDrillFilterOtherVisuals": True, "allowChangeFilterTypes": True, "useEnhancedTooltips": True,
                     "useDefaultAggregateDisplayName": True}})
    themes = rp / "StaticResources/SharedResources/BaseThemes"
    themes.mkdir(parents=True, exist_ok=True)
    shutil.copy(template_theme, themes / template_theme.name)
    write(rp / "StaticResources/RegisteredResources/QuanLyKho.json", THEME)
    pages = build_pages()
    write(d / "pages/pages.json", {"$schema": f"{SCHEMA}/pagesMetadata/1.1.0/schema.json",
                                   "pageOrder": [p.id for p in pages], "activePageName": pages[0].id})
    for p in pages:
        write(d / "pages" / p.id / "page.json", {"$schema": f"{SCHEMA}/page/2.1.0/schema.json", "name": p.id,
                                                 "displayName": p.title, "displayOption": "FitToPage", "height": 720, "width": 1280})
        for v in p.visuals:
            write(d / "pages" / p.id / "visuals" / v["name"] / "visual.json", v)
    return model, pages


def check_model(model, pages):
    """Kiểm tra nội bộ: quan hệ, sortByColumn và mọi trường visual tham chiếu đều tồn tại trong model."""
    tables = {t["name"]: t for t in model["model"]["tables"]}
    columns = {(t, c["name"]) for t, tb in tables.items() for c in tb["columns"]}
    measures = [ms["name"] for ms in tables["_Measures"]["measures"]]
    problems = []
    if len(measures) != len(set(measures)):
        problems.append("Trùng tên measure")
    for r in model["model"]["relationships"]:
        for t, cname in ((r["fromTable"], r["fromColumn"]), (r["toTable"], r["toColumn"])):
            if (t, cname) not in columns:
                problems.append(f"Quan hệ thiếu cột {t}.{cname}")
    for t, tb in tables.items():
        by_name = {cdef["name"]: cdef for cdef in tb["columns"]}
        for cdef in tb["columns"]:
            target = cdef.get("sortByColumn")
            if target and (t, target) not in columns:
                problems.append(f"sortByColumn thiếu {t}.{target}")
            elif target and by_name[target].get("type") == "calculated" and f"[{cdef['name']}]" in by_name[target]["expression"]:
                # Power BI báo "circular dependency" khi cột sắp xếp tính từ chính cột được sắp xếp (lỗi gặp 16/11/2026).
                problems.append(f"Vòng phụ thuộc sắp xếp: {t}.{cdef['name']} ↔ {target}")
    text = json.dumps([v for p in pages for v in p.visuals], ensure_ascii=False)
    for match in set(__import__("re").findall(r'"Entity": "([^"]+)"\}\}, "Property": "([^"]+)"', text)):
        entity, prop = match
        if entity == "_Measures":
            if prop not in measures:
                problems.append(f"Visual dùng measure không tồn tại: {prop}")
        elif (entity, prop) not in columns:
            problems.append(f"Visual dùng cột không tồn tại: {entity}.{prop}")
    for _, _, expression, _ in MEASURES:
        for ref in __import__("re").findall(r"\[([^\]]+)\]", expression):
            if ref not in measures and not any(cname == ref for _, cname in columns):
                problems.append(f"Measure tham chiếu không tồn tại: [{ref}]")
    return problems


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--out", default=str(ROOT / "powerbi" / NAME))
    parser.add_argument("--base-theme", default=None,
                        help="Tệp base theme (mặc định lấy từ BaseThemes của PBIP hiện có, chép ra trước khi ghi đè)")
    args = parser.parse_args()
    theme = Path(args.base_theme) if args.base_theme else None
    if theme is None:
        import shutil
        import tempfile
        # Các PBIX cũ đã xóa ngày 22/11/2026; base theme lấy từ chính PBIP đang có.
        found = sorted((Path(args.out) / f"{NAME}.Report/StaticResources/SharedResources/BaseThemes").glob("*.json"))
        if not found:
            sys.exit("Không tìm thấy base theme trong PBIP hiện có; truyền --base-theme <tệp .json>")
        theme = Path(tempfile.mkdtemp()) / found[0].name
        shutil.copy(found[0], theme)
    model, pages = build(Path(args.out), theme)
    problems = check_model(model, pages)
    visuals = sum(len(p.visuals) for p in pages)
    print(json.dumps({"out": args.out, "tables": len(model["model"]["tables"]), "relationships": len(model["model"]["relationships"]),
                      "measures": len(MEASURES), "pages": len(pages), "visuals": visuals, "problems": problems}, ensure_ascii=False))
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
