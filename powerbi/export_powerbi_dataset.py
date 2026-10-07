"""
export_powerbi_dataset.py
-------------------------
Xuất bộ dữ liệu chuẩn hóa Star Schema theo phương pháp luận Ralph Kimball
(The Data Warehouse Toolkit) phục vụ Power BI Desktop:
  1. Dim_Dates.csv              (Conformed Date Dimension)
  2. Dim_Products.csv           (Product Dimension với Surrogate Key)
  3. Dim_Geography.csv          (Geography Dimension)
  4. Fact_Orders_Summary.csv    (Sales Aggregate Fact Table)
  5. Forecast_vs_Actual.csv     (MLOps Consolidated Fact Table)
  6. Inventory_Health_Alerts.csv (Inventory Periodic Snapshot Fact)
"""

from __future__ import annotations

import csv
import logging
import math
import os
import random
import sys
from datetime import date, datetime, timedelta

logger = logging.getLogger("mlops.powerbi.export")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")

# Cho phép import serving
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from serving.app.model_loader import CATALOG_METADATA, get_model_manager
from serving.app.inventory_service import get_inventory_service


def export_powerbi_data(output_dir: str | None = None) -> str:
    out_dir = output_dir or os.path.join(os.path.dirname(__file__), "data")
    out_dir = os.path.abspath(out_dir)
    os.makedirs(out_dir, exist_ok=True)

    logger.info(f"Đang xuất khẩu bộ dữ liệu Kimball Star Schema vào thư mục: {out_dir}")

    # Mapping SKU sang integer Surrogate Key (Kimball Best Practice)
    sku_to_key: dict[str, int] = {}
    for idx, sku in enumerate(CATALOG_METADATA.keys(), start=1):
        sku_to_key[sku] = idx

    start_dt = date(2026, 8, 1)
    end_dt = date(2026, 9, 24)

    # 1. Dim_Dates.csv (Conformed Date Dimension - Chapter 3)
    dates_file = os.path.join(out_dir, "Dim_Dates.csv")
    with open(dates_file, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        writer.writerow([
            "date_key", "full_date", "day_of_week", "day_name",
            "month", "month_name", "quarter", "year",
            "is_weekend", "is_mega_sale"
        ])
        curr = date(2026, 7, 1)
        max_dt = date(2026, 10, 31)
        while curr <= max_dt:
            date_key = curr.year * 10000 + curr.month * 100 + curr.day
            dow = curr.weekday()
            day_name = ["Thứ Hai", "Thứ Ba", "Thứ Tư", "Thứ Năm", "Thứ Sáu", "Thứ Bảy", "Chủ Nhật"][dow]
            month_name = f"Tháng {curr.month}"
            quarter = f"Q{(curr.month - 1) // 3 + 1}"
            is_weekend = dow in (5, 6)
            is_mega_sale = (curr.day == curr.month)
            writer.writerow([
                date_key, curr.strftime("%Y-%m-%d"), dow, day_name,
                curr.month, month_name, quarter, curr.year,
                is_weekend, is_mega_sale
            ])
            curr += timedelta(days=1)
    logger.info(f"✓ Đã xuất {dates_file}")

    # 2. Dim_Products.csv (Product Dimension - Chapter 3)
    products_file = os.path.join(out_dir, "Dim_Products.csv")
    with open(products_file, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        writer.writerow(["product_key", "sku", "product_name", "category", "base_demand", "sigma", "matrix_segment"])
        for sku, meta in CATALOG_METADATA.items():
            writer.writerow([
                sku_to_key[sku], sku, meta["name"], meta["category"],
                meta["base_demand"], meta["sigma"], meta["segment"]
            ])
    logger.info(f"✓ Đã xuất {products_file}")

    # 3. Dim_Geography.csv (Geography Dimension - Chapter 3)
    geo_file = os.path.join(out_dir, "Dim_Geography.csv")
    provinces = [
        (1, "TP.HCM", "Nam", 450000000),
        (2, "Hà Nội", "Bắc", 380000000),
        (3, "Đà Nẵng", "Trung", 120000000),
        (4, "Bình Dương", "Nam", 95000000),
        (5, "Đồng Nai", "Nam", 85000000),
        (6, "Hải Phòng", "Bắc", 75000000),
        (7, "Cần Thơ", "Nam", 65000000),
        (8, "Khánh Hoà", "Trung", 45000000),
        (9, "Nghệ An", "Bắc", 40000000),
        (10, "Lâm Đồng", "Trung", 35000000),
    ]
    with open(geo_file, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        writer.writerow(["geo_key", "state", "region", "historical_gmp_vnd"])
        for p in provinces:
            writer.writerow(p)
    logger.info(f"✓ Đã xuất {geo_file}")

    # 4. Inventory_Health_Alerts.csv (Periodic Snapshot Fact - Chapter 4)
    inv_service = get_inventory_service()
    alerts_res = inv_service.evaluate_all()
    snapshot_date_key = end_dt.year * 10000 + end_dt.month * 100 + end_dt.day

    inv_file = os.path.join(out_dir, "Inventory_Health_Alerts.csv")
    with open(inv_file, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        writer.writerow([
            "date_key", "product_key", "sku", "product_name", "category", "matrix_segment",
            "current_stock", "avg_daily_demand", "safety_stock", "reorder_point",
            "needs_reorder", "alert_level", "days_until_stockout", "recommended_reorder_qty"
        ])
        for a in alerts_res.alerts:
            p_key = sku_to_key.get(a.sku, 0)
            writer.writerow([
                snapshot_date_key, p_key, a.sku, a.product_name, a.category, a.matrix_segment,
                a.current_stock, a.avg_daily_demand, a.safety_stock, a.reorder_point,
                a.needs_reorder, a.alert_level, a.days_until_stockout, a.recommended_reorder_qty
            ])
    logger.info(f"✓ Đã xuất {inv_file}")

    # 5. Forecast_vs_Actual.csv (Consolidated Fact Table - Chapter 7)
    manager = get_model_manager()
    fva_file = os.path.join(out_dir, "Forecast_vs_Actual.csv")
    random.seed(42)
    with open(fva_file, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        writer.writerow([
            "date_key", "product_key", "date", "sku", "product_name", "category", "matrix_segment",
            "actual_demand", "forecast_demand", "lower_bound", "upper_bound",
            "absolute_error", "squared_error"
        ])
        curr = start_dt
        while curr <= end_dt:
            date_key = curr.year * 10000 + curr.month * 100 + curr.day
            for sku, meta in CATALOG_METADATA.items():
                p_key = sku_to_key[sku]
                pred, lb, ub = manager.predict_daily(sku, curr, confidence_interval=True)
                noise = random.gauss(0, meta["sigma"] * 0.8)
                actual = max(0.0, round(pred + noise, 1))
                abs_err = round(abs(actual - pred), 2)
                sq_err = round(abs_err ** 2, 2)

                writer.writerow([
                    date_key, p_key, curr.strftime("%Y-%m-%d"), sku, meta["name"], meta["category"], meta["segment"],
                    actual, pred, lb, ub, abs_err, sq_err
                ])
            curr += timedelta(days=1)
    logger.info(f"✓ Đã xuất {fva_file}")

    # 6. Fact_Orders_Summary.csv (Sales Aggregate Fact - Chapter 3 & 15)
    orders_file = os.path.join(out_dir, "Fact_Orders_Summary.csv")
    with open(orders_file, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        writer.writerow([
            "date_key", "order_date", "platform", "total_orders", "units_sold",
            "gross_revenue", "discounts", "net_revenue", "aov"
        ])
        curr = start_dt
        while curr <= end_dt:
            date_key = curr.year * 10000 + curr.month * 100 + curr.day
            for platform, ratio, price_mult in [("shopee", 0.55, 1.0), ("tiktok", 0.45, 0.95)]:
                dow = curr.weekday()
                dow_mult = 1.3 if dow in (5, 6) else 1.0
                orders = int(random.gauss(120, 15) * dow_mult * ratio)
                units = int(orders * random.uniform(1.4, 1.8))
                gross = round(units * 185_000 * price_mult)
                disc = round(gross * random.uniform(0.08, 0.14))
                net = gross - disc
                aov = round(net / max(orders, 1))

                writer.writerow([
                    date_key, curr.strftime("%Y-%m-%d"), platform, orders, units,
                    gross, disc, net, aov
                ])
            curr += timedelta(days=1)
    logger.info(f"✓ Đã xuất {orders_file}")

    logger.info("═" * 60)
    logger.info("  HOÀN THÀNH XUẤT KHẨU TẬP DỮ LIỆU KIMBALL STAR SCHEMA")
    logger.info(f"  Vị trí tệp: {out_dir}")
    logger.info("═" * 60)
    return out_dir


if __name__ == "__main__":
    export_powerbi_data()
