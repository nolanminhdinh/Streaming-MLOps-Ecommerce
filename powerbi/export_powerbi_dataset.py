"""
Export Power BI data marts from the PostgreSQL warehouse.

Every sales, inventory, and forecast value in these CSVs is read from persisted
warehouse facts. This exporter deliberately has no synthetic-data fallback:
without warehouse facts the dashboard should be empty/error, not plausible but
misleading.
"""

from __future__ import annotations

import csv
import logging
import os
from typing import Any, Iterable, Sequence

logger = logging.getLogger("mlops.powerbi.export")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")

DATASETS: dict[str, Sequence[str]] = {
    "Dim_Dates.csv": (
        "date_key", "full_date", "day_of_week", "day_name", "month", "month_name",
        "quarter", "year", "is_weekend", "is_mega_sale",
    ),
    "Dim_Products.csv": (
        "product_key", "sku", "product_name", "category", "base_demand", "sigma", "matrix_segment",
    ),
    "Dim_Geography.csv": (
        "geo_key", "state", "region", "total_revenue", "total_units", "total_orders",
    ),
    "Fact_Orders_Summary.csv": (
        "date_key", "order_date", "platform", "total_orders", "units_sold",
        "gross_revenue", "discounts", "net_revenue", "aov",
    ),
    "Forecast_vs_Actual.csv": (
        "date_key", "product_key", "date", "sku", "product_name", "category", "matrix_segment",
        "actual_demand", "forecast_demand", "lower_bound", "upper_bound", "absolute_error",
        "squared_error", "model_name", "model_version", "model_source", "forecast_generated_at",
    ),
    "Inventory_Health_Alerts.csv": (
        "date_key", "product_key", "sku", "product_name", "category", "matrix_segment",
        "current_stock", "avg_daily_demand", "safety_stock", "reorder_point", "needs_reorder",
        "alert_level", "days_until_stockout", "recommended_reorder_qty",
    ),
}


def _database_url() -> str:
    return (
        f"postgresql://{os.getenv('POSTGRES_USER', 'ecom')}:{os.getenv('POSTGRES_PASSWORD', 'ecom_password')}"
        f"@{os.getenv('POSTGRES_HOST', 'localhost')}:{os.getenv('POSTGRES_PORT', '5432')}"
        f"/{os.getenv('POSTGRES_DB', 'ecom_warehouse')}"
    )


def _install_views(conn: Any) -> None:
    sql_path = os.path.join(os.path.dirname(__file__), "views_for_powerbi.sql")
    with open(sql_path, "r", encoding="utf-8") as sql_file:
        script = sql_file.read()
    # The view file contains only standalone CREATE OR REPLACE VIEW statements.
    # Execute each separately so this also works with SQLAlchemy 2.x drivers.
    for statement in script.split(";"):
        if statement.strip():
            conn.exec_driver_sql(statement)


def _query(conn: Any, sql: str) -> list[dict[str, Any]]:
    from sqlalchemy import text

    return [dict(row) for row in conn.execute(text(sql)).mappings().all()]


def _write_csv(path: str, columns: Sequence[str], rows: Iterable[dict[str, Any]]) -> int:
    count = 0
    with open(path, "w", newline="", encoding="utf-8-sig") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
            count += 1
    return count


def export_powerbi_data(output_dir: str | None = None) -> str:
    """Refresh the Power BI CSV data marts from PostgreSQL warehouse facts."""
    try:
        from dotenv import load_dotenv
        from sqlalchemy import create_engine
    except ImportError as exc:
        raise RuntimeError("Cần cài dependencies của warehouse/serving để xuất dữ liệu Power BI.") from exc

    load_dotenv()
    out_dir = os.path.abspath(output_dir or os.path.join(os.path.dirname(__file__), "data"))
    os.makedirs(out_dir, exist_ok=True)
    engine = create_engine(_database_url(), pool_pre_ping=True, connect_args={"connect_timeout": 3})

    queries = {
        "Dim_Dates.csv": """
            SELECT date_key, full_date, day_of_week, day_name, month, month_name,
                   quarter, year, is_weekend, is_mega_sale
            FROM Dim_Dates
            ORDER BY full_date
        """,
        "Dim_Products.csv": """
            SELECT p.product_key, p.sku, p.product_name, p.category,
                   COALESCE(m.avg_daily_demand, 0) AS base_demand,
                   COALESCE(m.std_daily_demand, 0) AS sigma,
                   COALESCE(m.matrix_segment, 'NA') AS matrix_segment
            FROM Dim_Products p
            LEFT JOIN vw_powerbi_abc_xyz_matrix m ON m.sku = p.sku
            ORDER BY p.product_key
        """,
        "Dim_Geography.csv": """
            SELECT g.geo_key, g.state, g.region,
                   COALESCE(SUM(f.buyer_total_amount), 0) AS total_revenue,
                   COALESCE(SUM(f.quantity), 0) AS total_units,
                   COUNT(f.order_fact_id) AS total_orders
            FROM Dim_Geography g
            LEFT JOIN Fact_Orders f
                   ON f.geo_key = g.geo_key AND f.is_cancelled = FALSE
            GROUP BY g.geo_key, g.state, g.region
            HAVING COUNT(f.order_fact_id) > 0
            ORDER BY total_revenue DESC
        """,
        "Fact_Orders_Summary.csv": """
            SELECT d.date_key, d.full_date AS order_date, s.platform,
                   COUNT(f.order_fact_id) AS total_orders,
                   SUM(f.quantity) AS units_sold,
                   SUM(COALESCE(f.subtotal, f.quantity * f.original_price)) AS gross_revenue,
                   SUM(COALESCE(f.seller_discount, 0) + COALESCE(f.platform_discount, 0)) AS discounts,
                   SUM(f.buyer_total_amount) AS net_revenue,
                   AVG(f.buyer_total_amount) AS aov
            FROM Fact_Orders f
            JOIN Dim_Dates d ON d.date_key = f.date_key
            JOIN Dim_Shops s ON s.shop_key = f.shop_key
            WHERE f.is_cancelled = FALSE
            GROUP BY d.date_key, d.full_date, s.platform
            ORDER BY d.full_date, s.platform
        """,
        "Forecast_vs_Actual.csv": """
            SELECT date_key, product_key, date, sku, product_name, category, matrix_segment,
                   actual_demand, forecast_demand, lower_bound, upper_bound,
                   absolute_error, squared_error, model_name, model_version,
                   model_source, forecast_generated_at
            FROM vw_powerbi_forecast_vs_actual
            ORDER BY date, sku
        """,
        "Inventory_Health_Alerts.csv": """
            SELECT date_key, product_key, sku, product_name, category, matrix_segment,
                   current_stock, avg_daily_demand, safety_stock, reorder_point,
                   needs_reorder, alert_level, days_until_stockout, recommended_reorder_qty
            FROM vw_powerbi_inventory_health
            ORDER BY CASE alert_level WHEN 'CRITICAL' THEN 1 WHEN 'WARNING' THEN 2 ELSE 3 END,
                     days_until_stockout
        """,
    }

    try:
        with engine.begin() as conn:
            _install_views(conn)
            datasets = {name: _query(conn, sql) for name, sql in queries.items()}
    except Exception as exc:
        raise RuntimeError(
            "Không thể xuất Power BI từ Data Warehouse. Hãy khởi động PostgreSQL, chạy "
            "`python scripts/init_warehouse.py`, nạp dữ liệu ETL/đơn hàng và kiểm tra "
            "Fact_Forecast_Predictions đã được tạo. Không có dữ liệu mô phỏng thay thế."
        ) from exc
    finally:
        engine.dispose()

    if not datasets["Fact_Orders_Summary.csv"]:
        raise RuntimeError(
            "Data Warehouse chưa có đơn hàng hợp lệ trong Fact_Orders; chưa thể tạo dashboard thực."
        )

    logger.info("Đang ghi dữ liệu Power BI lấy từ PostgreSQL vào %s", out_dir)
    for filename, rows in datasets.items():
        count = _write_csv(os.path.join(out_dir, filename), DATASETS[filename], rows)
        logger.info("Đã xuất %s (%d dòng)", filename, count)

    if not datasets["Forecast_vs_Actual.csv"]:
        logger.warning(
            "Chưa có forecast ML được lưu. Hãy gọi /predict/demand với model thật và lịch sử warehouse; "
            "WAPE/accuracy sẽ có dữ liệu sau khi các ngày mục tiêu kết thúc."
        )
    return out_dir


if __name__ == "__main__":
    export_powerbi_data()
