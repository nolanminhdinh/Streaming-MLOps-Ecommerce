"""Opt-in PostgreSQL tests using session-local tables/views and rollback only.

RUN_POWERBI_SQL_TESTS=1 runs against the configured warehouse. No fixture is
written into public facts, dimensions, views, or their identity sequences.
"""
import os
from decimal import Decimal
from pathlib import Path
from unittest import mock

import pytest
from sqlalchemy import create_engine, text
from powerbi import export_powerbi_dataset as exporter

pytestmark = pytest.mark.skipif(os.getenv("RUN_POWERBI_SQL_TESTS") != "1",
                                reason="Set RUN_POWERBI_SQL_TESTS=1 for isolated PostgreSQL checks")


@pytest.fixture
def sql_cohort(tmp_path):
    captured = {}

    def capture(conn, sql):
        captured[list(exporter.DATASETS)[len(captured)]] = sql
        return [{"units_sold": 1}]

    with mock.patch("sqlalchemy.create_engine"), mock.patch.object(exporter, "_install_views"), \
         mock.patch.object(exporter, "_query", side_effect=capture), \
         mock.patch.object(exporter, "_write_csv", return_value=1):
        exporter.export_powerbi_data(str(tmp_path))
    engine = create_engine(exporter._database_url(), connect_args={"connect_timeout": 3})
    with engine.connect() as conn:
        transaction = conn.begin()
        try:
            for table in ["Fact_Orders", "Dim_Dates", "Dim_Shops", "Dim_Geography"]:
                conn.exec_driver_sql(f"CREATE TEMP TABLE {table} ON COMMIT DROP AS SELECT * FROM public.{table} WITH NO DATA")
            conn.exec_driver_sql("""INSERT INTO Dim_Dates (date_key, full_date, year, month, day_name, is_weekend, is_mega_sale)
                VALUES (1, '2026-10-20', 2026, 10, 'Tuesday', FALSE, FALSE),
                       (2, '2026-10-21', 2026, 10, 'Wednesday', FALSE, FALSE)""")
            conn.exec_driver_sql("INSERT INTO Dim_Shops (shop_key, shop_id, shop_name, platform) VALUES (1,'shop','shop','shopee')")
            conn.exec_driver_sql("INSERT INTO Dim_Geography (geo_key,state,region) VALUES (1,'TEST','TEST'),(2,'EMPTY','TEST')")
            # Same order ID across channels; repeated lines in one order; missing
            # shop; a cancelled order; and a day with a different order size.
            values = [
                (1, "shared", "shopee", "a", 1, 1, 1, 100, 90, False),
                (2, "shared", "shopee", "b", 1, 1, 2, 200, 180, False),
                (3, "shared", "tiktok", "a", 1, None, 1, 50, 45, False),
                (4, "no-shop", "shopee", "a", 1, None, 1, 100, 85, False),
                (5, "cancelled", "shopee", "a", 1, 1, 1, 900, 900, True),
                (6, "day-two", "shopee", "a", 2, 1, 1, 20, 20, False),
                (7, "day-two", "shopee", "b", 2, 1, 1, 30, 30, False),
            ]
            conn.execute(text("""INSERT INTO Fact_Orders
                (order_fact_id,order_id,platform,source_line_id,date_key,shop_key,geo_key,
                 quantity,subtotal,buyer_total_amount,is_cancelled,original_price,seller_discount,platform_discount)
                VALUES (:id,:order_id,:platform,:line,:day,:shop,1,:qty,:subtotal,:amount,:cancelled,1,0,0)"""),
                [dict(zip(["id","order_id","platform","line","day","shop","qty","subtotal","amount","cancelled"], row)) for row in values])
            view_script = (Path(exporter.__file__).parent / "views_for_powerbi.sql").read_text(encoding="utf-8")
            for statement in view_script.split(";"):
                if "CREATE OR REPLACE VIEW vw_powerbi_executive_summary AS" in statement or "CREATE OR REPLACE VIEW vw_powerbi_geographic_sales AS" in statement:
                    conn.exec_driver_sql(statement.replace("CREATE OR REPLACE VIEW", "CREATE TEMP VIEW"))
            yield conn, captured
        finally:
            transaction.rollback()
    engine.dispose()


def test_summary_counts_orders_across_lines_channels_and_missing_shop(sql_cohort):
    conn, queries = sql_cohort
    rows = conn.execute(text(queries["Fact_Orders_Summary.csv"])).mappings().all()
    assert sum(row["total_orders"] for row in rows) == 4
    assert sum(row["units_sold"] for row in rows) == 7
    assert sum(row["net_revenue"] for row in rows) == Decimal("450")


def test_aov_is_order_ratio_and_global_ratio_is_weighted(sql_cohort):
    conn, queries = sql_cohort
    rows = conn.execute(text(queries["Fact_Orders_Summary.csv"])).mappings().all()
    assert {(row["date_key"], row["platform"]): row["aov"] for row in rows} == {
        (1, "shopee"): Decimal("177.50"), (1, "tiktok"): Decimal("45.00"), (2, "shopee"): Decimal("50.00")}
    total_aov = sum(row["net_revenue"] for row in rows) / sum(row["total_orders"] for row in rows)
    assert total_aov == Decimal("112.50")
    assert total_aov != sum(row["aov"] for row in rows) / len(rows)


def test_geography_counts_business_orders_and_excludes_empty_locations(sql_cohort):
    conn, queries = sql_cohort
    rows = conn.execute(text(queries["Dim_Geography.csv"])).mappings().all()
    assert len(rows) == 1 and rows[0]["total_orders"] == 4
    assert rows[0]["total_units"] == 7
    geographic_orders = conn.execute(text("SELECT SUM(order_count) FROM vw_powerbi_geographic_sales")).scalar_one()
    assert geographic_orders == 4


def test_export_and_executive_view_have_identical_order_metrics(sql_cohort):
    conn, queries = sql_cohort
    exported = conn.execute(text(queries["Fact_Orders_Summary.csv"])).mappings().all()
    expected = conn.execute(text("SELECT order_date, platform, total_orders, total_revenue, average_order_value FROM vw_powerbi_executive_summary")).mappings().all()
    by_key = {(row["order_date"], row["platform"]): row for row in expected}
    for row in exported:
        original = by_key[(row["order_date"], row["platform"])]
        assert (row["total_orders"], row["net_revenue"], row["aov"]) == (
            original["total_orders"], original["total_revenue"], original["average_order_value"])
