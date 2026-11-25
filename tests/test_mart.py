"""Data mart star schema (schema `mart`, 16/11/2026).

Test tĩnh luôn chạy. Test PostgreSQL (RUN_MART_TESTS=1) chạy trên kho thật trong một transaction và
rollback: không để lại dữ liệu, không đổi mốc làm mới.
"""
from datetime import date, datetime, timedelta, timezone
import os
from pathlib import Path
import re

import pytest

ROOT = Path(__file__).resolve().parents[1]
DDL = (ROOT / "warehouse/ddl/05_mart_star_schema.sql").read_text(encoding="utf-8")
FUNCTION = (ROOT / "warehouse/reporting/mart_refresh.sql").read_text(encoding="utf-8")


def test_sql_files_have_no_percent_sign():
    """psycopg2 coi ký tự phần trăm là tham số khi chạy qua exec_driver_sql (kể cả trong comment)."""
    assert "%" not in DDL and "%" not in FUNCTION


def test_every_dimension_except_date_has_unknown_member():
    dims = set(re.findall(r"CREATE TABLE IF NOT EXISTS mart\.(dim_\w+)", DDL))
    assert dims == {"dim_date", "dim_platform", "dim_product", "dim_shop", "dim_geography", "dim_payment",
                    "dim_carrier", "dim_order_status", "dim_model"}
    for dim in dims - {"dim_date"}:
        assert re.search(rf"INSERT INTO mart\.{dim}\b[^;]*\(-1,", DDL), dim


def test_not_applicable_members_exist_where_meaningful():
    """-1 = nguồn thiếu dữ liệu; -2 = thuộc tính không áp dụng (quyết định của sinh viên 16/11/2026)."""
    assert re.search(r"INSERT INTO mart\.dim_carrier VALUES[^;]*\(-2, 'Chưa giao vận chuyển'\)", DDL)
    assert re.search(r"INSERT INTO mart\.dim_model[^;]*\(-2, 'not_applicable'[^;]*'Không dùng dự báo AI'\)", DDL)
    assert "'UNKNOWN', 'Không xác định', 'Không xác định', 'Chưa phân nhóm'" not in DDL   # nhãn sku thống nhất


def test_columns_loaded_by_power_bi_are_local_timestamps():
    """Hồi quy 16/11/2026: TIMESTAMPTZ làm Power Query lỗi trên mọi dòng có giá trị khi đổi múi giờ."""
    for column in ("first_forecast_at", "last_forecast_at", "snapshot_at", "forecast_generated_at"):
        declaration = re.search(rf"^\s+{column}\s+(\w+)", DDL, re.M).group(1)
        assert declaration == "TIMESTAMP", column
    for column in ("snapshot_at", "forecast_generated_at"):
        assert f"{column} AT TIME ZONE 'Asia/Ho_Chi_Minh'" in FUNCTION, column


def test_replenishment_history_is_never_truncated():
    truncate = re.search(r"TRUNCATE ([^;]+);", FUNCTION).group(1)
    assert "fact_replenishment_daily" not in truncate


pg = pytest.mark.skipif(os.getenv("RUN_MART_TESTS") != "1", reason="Opt-in: PostgreSQL thật, rollback")


@pytest.fixture
def mart_conn():
    from sqlalchemy import text
    from warehouse.etl.load import get_engine
    from warehouse.reporting.mart import install_mart, refresh_mart
    engine = get_engine()
    with engine.connect() as conn:
        tx = conn.begin()
        try:
            install_mart(conn)
            refresh_mart(conn)
            yield conn, text, refresh_mart
        finally:
            tx.rollback()
    engine.dispose()


def scalar(conn, text, sql, **params):
    return conn.execute(text(sql), params).scalar_one()


@pg
def test_mart_reconciles_with_integration_layer(mart_conn):
    conn, text, _ = mart_conn
    assert conn.execute(text("""SELECT COUNT(*), SUM(quantity), SUM(buyer_total_amount) FILTER (WHERE NOT is_cancelled)
                                FROM public.Fact_Orders""")).one() == \
        conn.execute(text("SELECT COUNT(*), SUM(quantity), SUM(net_amount) FROM mart.fact_sales_line")).one()
    assert scalar(conn, text, "SELECT COUNT(DISTINCT (platform, order_id)) FROM public.Fact_Orders") == \
        scalar(conn, text, "SELECT COUNT(*) FROM mart.fact_sales_order")
    assert scalar(conn, text, "SELECT SUM(net_units) FROM mart.fact_sales_order") == \
        scalar(conn, text, "SELECT SUM(net_quantity) FROM mart.fact_sales_line")
    assert scalar(conn, text, "SELECT COUNT(*) FROM public.Fact_Inventory_Daily") == \
        scalar(conn, text, "SELECT COUNT(*) FROM mart.fact_inventory_daily")
    assert scalar(conn, text, "SELECT COUNT(*) FROM public.Fact_Forecast_Predictions") == \
        scalar(conn, text, "SELECT COUNT(*) FROM mart.fact_demand_forecast")


@pg
def test_date_dimension_is_contiguous_with_yyyymmdd_keys(mart_conn):
    conn, text, _ = mart_conn
    assert scalar(conn, text, "SELECT MAX(full_date) - MIN(full_date) + 1 = COUNT(*) FROM mart.dim_date")
    assert scalar(conn, text, "SELECT COUNT(*) FROM mart.dim_date WHERE date_key <> TO_CHAR(full_date, 'YYYYMMDD')::INT") == 0


@pg
def test_forecasts_keep_every_version_and_score_actuals(mart_conn):
    """Hai phiên bản dự báo cùng SKU × ngày: cả hai được giữ, chỉ bản mới nhất có cờ; thực tế được chấm đúng."""
    conn, text, refresh = mart_conn
    day = scalar(conn, text, """SELECT MAX(full_date) FROM public.Warehouse_Daily_Coverage
                                WHERE data_origin = 'warehouse_live' AND source_dataset = ''""")
    product_key, actual = conn.execute(text("""SELECT product_key, SUM(quantity) FROM public.Fact_Orders
        WHERE create_time::date = :d AND NOT is_cancelled GROUP BY 1 ORDER BY 2 DESC LIMIT 1"""), {"d": day}).one()
    # Thời điểm phát sau mọi dự báo thật: ngày đã xác nhận có thể đã có dự báo của batch-forecaster.
    later = datetime.now(timezone.utc) + timedelta(days=1)
    for version, predicted, generated in (("98", 10, later), ("99", 25, later + timedelta(hours=1))):
        conn.execute(text("""INSERT INTO public.Fact_Forecast_Predictions (product_key, target_date, predicted_quantity,
            model_name, model_version, model_source, history_source, history_end_date, forecast_generated_at, data_origin)
            VALUES (:p, :d, :q, 'naive', :v, 'mlflow_registry', 'warehouse', :h, :g, 'warehouse_live')"""),
            {"p": product_key, "d": day, "q": predicted, "v": version, "h": day - timedelta(days=1), "g": generated})
    refresh(conn)
    rows = conn.execute(text("""SELECT m.model_version, f.horizon_days, f.is_latest_for_target, f.actual_complete,
            f.actual_quantity, f.absolute_error
        FROM mart.fact_demand_forecast f JOIN mart.dim_model m USING (model_key)
        WHERE f.product_key = :p AND f.target_date_key = mart.date_key(:d) AND m.model_version IN ('98', '99')
        ORDER BY 1"""), {"p": product_key, "d": day}).all()
    assert [r[0] for r in rows] == ["98", "99"]
    assert [r[2] for r in rows] == [False, True]
    assert all(r[1] == 1 and r[3] and r[4] == actual for r in rows)
    assert [float(r[5]) for r in rows] == [abs(10 - actual), abs(25 - actual)]


@pg
def test_previous_days_recommendations_are_not_rewritten(mart_conn):
    conn, text, refresh = mart_conn
    today_key = scalar(conn, text, "SELECT mart.date_key((NOW() AT TIME ZONE 'Asia/Ho_Chi_Minh')::DATE)")
    assert scalar(conn, text, "SELECT COUNT(*) FROM mart.fact_replenishment_daily WHERE evaluation_date_key = :k", k=today_key) == \
        scalar(conn, text, "SELECT COUNT(*) FROM bi.replenishment_recommendation")
    yesterday_key = scalar(conn, text, "SELECT mart.date_key((NOW() AT TIME ZONE 'Asia/Ho_Chi_Minh')::DATE - 1)")
    # Hôm qua có thể đã có đề xuất thật (mart chạy liên tục); thay bằng bản đánh dấu, giao dịch test sẽ rollback.
    conn.execute(text("DELETE FROM mart.fact_replenishment_daily WHERE evaluation_date_key = :y"), {"y": yesterday_key})
    conn.execute(text("""INSERT INTO mart.fact_replenishment_daily
        SELECT product_key, :y, model_key, inventory_date_key, inventory_age_days, on_hand, lead_time_days, lead_time_defaulted,
               service_level, sigma_daily, avg_daily_demand, demand_lead_time, forecast_next_7d, demand_source, safety_stock,
               reorder_point, order_up_to, days_of_cover, action_status, urgency, recommended_order_qty + 1000,
               projected_stockout_date, order_by_date, order_value_at_selling_price, 'LỊCH SỬ', refreshed_at
        FROM mart.fact_replenishment_daily WHERE evaluation_date_key = :t
        ON CONFLICT (product_key, evaluation_date_key) DO NOTHING"""), {"y": yesterday_key, "t": today_key})
    before = conn.execute(text("""SELECT COUNT(*), SUM(recommended_order_qty) FROM mart.fact_replenishment_daily
                                  WHERE evaluation_date_key = :y AND reason = 'LỊCH SỬ'"""), {"y": yesterday_key}).one()
    refresh(conn)
    refresh(conn, True)   # kể cả nạp lại toàn bộ
    after = conn.execute(text("""SELECT COUNT(*), SUM(recommended_order_qty) FROM mart.fact_replenishment_daily
                                 WHERE evaluation_date_key = :y AND reason = 'LỊCH SỬ'"""), {"y": yesterday_key}).one()
    assert before == after and before[0] > 0


@pg
def test_unknown_and_not_applicable_members_follow_rules(mart_conn):
    conn, text, _ = mart_conn
    # Vận chuyển: -2 chỉ cho đơn chưa từng giao; -1 chỉ cho đơn đã giao/hoàn tất mà thiếu đơn vị vận chuyển.
    # Hồi quy: 41 đơn COMPLETED không có mốc thời gian từng bị xếp nhầm vào "Chưa giao vận chuyển".
    shipped_like = "s.funnel_order IN (3, 4, 5, 7)"
    assert scalar(conn, text, f"""SELECT COUNT(*) FROM mart.fact_sales_order f JOIN mart.dim_order_status s USING (status_key)
        WHERE f.carrier_key = -2 AND (f.shipped_time IS NOT NULL OR f.completed_time IS NOT NULL OR {shipped_like})""") == 0
    assert scalar(conn, text, f"""SELECT COUNT(*) FROM mart.fact_sales_order f JOIN mart.dim_order_status s USING (status_key)
        WHERE f.carrier_key = -1 AND f.shipped_time IS NULL AND f.completed_time IS NULL AND NOT {shipped_like}""") == 0
    # Đề xuất: -2 khi và chỉ khi không dùng dự báo AI.
    assert scalar(conn, text, """SELECT COUNT(*) FROM mart.fact_replenishment_daily
        WHERE (model_key = -2) <> (demand_source <> 'ai_forecast')""") == 0
    assert scalar(conn, text, "SELECT COUNT(*) FROM mart.dim_product WHERE product_key = -1 AND sku = 'Không xác định'") == 1


@pg
def test_facts_have_no_missing_dimension_keys(mart_conn):
    conn, text, _ = mart_conn
    for table, columns in {"fact_sales_line": "order_date_key, product_key, platform_key, shop_key, geo_key, status_key",
                           "fact_sales_order": "platform_key, shop_key, geo_key, payment_key, carrier_key, status_key, order_date_key",
                           "fact_demand_forecast": "product_key, target_date_key, issued_date_key, model_key"}.items():
        nulls = " OR ".join(f"{c.strip()} IS NULL" for c in columns.split(","))
        assert scalar(conn, text, f"SELECT COUNT(*) FROM mart.{table} WHERE {nulls}") == 0, table
