"""Mô phỏng một lần Refresh Power BI Import trên PostgreSQL, chỉ đọc, và kiểm tra dữ liệu sạch.

Chạy đủ mọi nguồn Power BI có thể dùng: 7 query trong powerbi/import/*.pq (DATASET_QUERIES)
và toàn bộ view schema `bi`. Với mỗi nguồn: đọc hết dòng như Import, đếm NaN/Infinity (Power BI
báo lỗi kiểu số), đo thời gian. Sau đó kiểm tra quan hệ: khóa phía "một" duy nhất, không NULL;
khóa phía "nhiều" đều có trong dimension. Thêm: shop không trùng tên, nhãn ngày không đệm
khoảng trắng, đối soát KPI, độ mới dữ liệu. Không mở/sửa PBIX; không ghi vào warehouse.

Giới hạn: Power BI Desktop chạy từng query trong phiên riêng, không cùng một snapshot như script
này. Nếu ETL ghi đúng lúc refresh, một SKU/ngày mới có thể xuất hiện ở fact mà chưa có ở dimension
đã nạp trước; Power BI hiển thị thành (Blank), không làm refresh thất bại. Refresh lại là hết.

    python scripts/check_powerbi_refresh_readiness.py [--label truoc-demo]
"""
import argparse
from datetime import datetime
from decimal import Decimal
import json
import math
from pathlib import Path
import sys
import time
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

from powerbi.export_powerbi_dataset import DATASET_QUERIES, _database_url

# (bảng dimension, khóa) ← [(bảng fact, khóa)] — theo hướng dẫn quan hệ trong warehouse/README.md
# và powerbi/import/README.md. Tên "pq:" là query Power BI cũ, "bi." là view schema mới.
RELATIONSHIPS = [
    (("pq:Dim_Dates", "date_key"), [("pq:Fact_Orders_Summary", "date_key"), ("pq:Forecast_vs_Actual", "date_key"),
                                    ("pq:Inventory_Health_Alerts", "date_key"), ("pq:Historical_Demand_Daily", "date_key")]),
    (("pq:Dim_Products", "product_key"), [("pq:Forecast_vs_Actual", "product_key"), ("pq:Inventory_Health_Alerts", "product_key")]),
    (("pq:Dim_Geography", "geo_key"), []),
    (("bi.dim_date", "date_key"), [("bi.fact_order_line", "date_key"), ("bi.fact_inventory_daily", "date_key"),
                                   ("bi.inventory_health", "date_key"), ("bi.forecast_evaluation", "date_key"),
                                   ("bi.forecast_daily", "date_key"), ("bi.fact_sales_order", "date_key"),
                                   ("bi.replenishment_recommendation", "inventory_date_key")]),
    (("bi.dim_product", "product_key"), [("bi.fact_order_line", "product_key"), ("bi.fact_inventory_daily", "product_key"),
                                         ("bi.inventory_health", "product_key"), ("bi.forecast_evaluation", "product_key"),
                                         ("bi.fact_forecast_issue", "product_key"), ("bi.forecast_daily", "product_key"),
                                         ("bi.replenishment_recommendation", "product_key")]),
    (("bi.dim_shop", "shop_key"), [("bi.fact_order_line", "shop_key"), ("bi.fact_sales_order", "shop_key")]),
    (("bi.dim_geography", "geo_key"), [("bi.fact_order_line", "geo_key"), ("bi.fact_sales_order", "geo_key")]),
    (("bi.dim_platform", "platform"), [("bi.fact_order_line", "platform"), ("bi.dim_shop", "platform"),
                                       ("bi.fact_sales_order", "platform")]),
    # Bảng có khóa duy nhất theo hạt của nó (dùng làm phía "một" hoặc 1:1 với dim_product).
    (("bi.fact_sales_order", "order_business_key"), []),
    (("bi.replenishment_recommendation", "product_key"), []),
    (("bi.dim_historical_product", "historical_product_key"), [("bi.fact_historical_order_line", "historical_product_key")]),
    # Star schema mart — nguồn của báo cáo Power BI từ 16/11/2026.
    (("mart.dim_date", "date_key"), [("mart.fact_sales_line", "order_date_key"), ("mart.fact_sales_order", "order_date_key"),
                                     ("mart.fact_sales_order", "completed_date_key"), ("mart.fact_inventory_daily", "snapshot_date_key"),
                                     ("mart.fact_demand_forecast", "target_date_key"), ("mart.fact_demand_forecast", "issued_date_key"),
                                     ("mart.fact_replenishment_daily", "evaluation_date_key"),
                                     ("mart.fact_replenishment_daily", "inventory_date_key")]),
    (("mart.dim_product", "product_key"), [("mart.fact_sales_line", "product_key"), ("mart.fact_inventory_daily", "product_key"),
                                           ("mart.fact_demand_forecast", "product_key"), ("mart.fact_replenishment_daily", "product_key")]),
    (("mart.dim_platform", "platform_key"), [("mart.fact_sales_line", "platform_key"), ("mart.fact_sales_order", "platform_key"),
                                             ("mart.dim_shop", "platform_key")]),
    (("mart.dim_shop", "shop_key"), [("mart.fact_sales_line", "shop_key"), ("mart.fact_sales_order", "shop_key")]),
    (("mart.dim_geography", "geo_key"), [("mart.fact_sales_line", "geo_key"), ("mart.fact_sales_order", "geo_key")]),
    (("mart.dim_payment", "payment_key"), [("mart.fact_sales_order", "payment_key")]),
    (("mart.dim_carrier", "carrier_key"), [("mart.fact_sales_order", "carrier_key")]),
    (("mart.dim_order_status", "status_key"), [("mart.fact_sales_line", "status_key"), ("mart.fact_sales_order", "status_key")]),
    (("mart.dim_model", "model_key"), [("mart.fact_demand_forecast", "model_key"), ("mart.fact_replenishment_daily", "model_key")]),
]
MART_TABLES = ("dim_date", "dim_product", "dim_platform", "dim_shop", "dim_geography", "dim_payment", "dim_carrier",
               "dim_order_status", "dim_model", "fact_sales_line", "fact_sales_order", "fact_inventory_daily",
               "fact_demand_forecast", "fact_replenishment_daily")


def bad_number(value):
    if isinstance(value, Decimal):
        return value.is_nan() or value.is_infinite()
    return isinstance(value, float) and not math.isfinite(value)


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--label", default="", help="Nhãn lượt kiểm tra, ví dụ truoc-demo / trong-streaming")
    args = parser.parse_args()
    load_dotenv(ROOT / ".env")
    now = datetime.now(ZoneInfo("Asia/Ho_Chi_Minh"))
    engine = create_engine(_database_url(), pool_pre_ping=True, connect_args={"connect_timeout": 5})
    result = {"checked_at": now.isoformat(), "label": args.label, "status": "FAIL", "transaction_read_only": True,
              "powerbi_desktop_refresh_verified": False, "sources": {}, "relationships": [], "checks": {}}
    try:
        with engine.connect().execution_options(isolation_level="REPEATABLE READ") as conn, conn.begin():
            conn.exec_driver_sql("SET TRANSACTION READ ONLY")
            sources = {"pq:" + name.removesuffix(".csv"): sql for name, sql in DATASET_QUERIES.items()}
            for (view,) in conn.execute(text("""SELECT table_name FROM information_schema.views
                    WHERE table_schema = 'bi' ORDER BY table_name""")):
                sources["bi." + view] = f"SELECT * FROM bi.{view}"
            for table in MART_TABLES:
                sources["mart." + table] = f"SELECT * FROM mart.{table}"
            for name, sql in sources.items():
                started = time.perf_counter()
                cursor = conn.execute(text(f"SELECT * FROM ({sql}) source"))
                rows, nan_cells = 0, 0
                for row in cursor:
                    rows += 1
                    nan_cells += sum(bad_number(v) for v in row)
                result["sources"][name] = {"rows": rows, "nan_or_infinity_cells": nan_cells,
                                           "seconds": round(time.perf_counter() - started, 3)}
            for (dim, key), facts in RELATIONSHIPS:
                dim_sql = f"({sources[dim]})"
                stats = conn.execute(text(f"""SELECT COUNT(*) - COUNT(DISTINCT {key}) AS duplicate_keys,
                    COUNT(*) FILTER (WHERE {key} IS NULL) AS null_keys FROM {dim_sql} d""")).mappings().one()
                entry = {"dimension": dim, "key": key, **stats, "facts": {}}
                for fact, fkey in facts:
                    entry["facts"][fact] = conn.execute(text(f"""SELECT COUNT(*) FROM ({sources[fact]}) f
                        WHERE f.{fkey} IS NOT NULL AND NOT EXISTS (SELECT 1 FROM {dim_sql} d WHERE d.{key} = f.{fkey})""")).scalar_one()
                result["relationships"].append(entry)
            c = result["checks"]
            c["all_sources_loaded"] = len(result["sources"]) == len(sources)
            c["no_nan_or_infinity"] = all(s["nan_or_infinity_cells"] == 0 for s in result["sources"].values())
            c["dimension_keys_unique_not_null"] = all(r["duplicate_keys"] == 0 and r["null_keys"] == 0 for r in result["relationships"])
            c["no_orphan_fact_keys"] = all(v == 0 for r in result["relationships"] for v in r["facts"].values())
            c["shop_one_key_per_name"] = conn.execute(text("SELECT COUNT(*) = 0 FROM bi.quality_shop_identity")).scalar_one()
            c["date_labels_trimmed"] = conn.execute(text("""SELECT NOT EXISTS (SELECT 1 FROM bi.dim_date
                WHERE day_name <> btrim(day_name) OR month_name <> btrim(month_name))""")).scalar_one()
            c["model_status_present"] = conn.execute(text("SELECT COUNT(*) = 1 FROM bi.model_status")).scalar_one()
            c["data_freshness_one_row"] = conn.execute(text("SELECT COUNT(*) = 1 FROM bi.data_freshness")).scalar_one()
            c["sales_orders_reconcile_lines"] = conn.execute(text("""SELECT
                (SELECT COALESCE(SUM(gross_units), 0) FROM bi.fact_sales_order) = (SELECT COALESCE(SUM(quantity), 0) FROM bi.fact_order_line)
                AND (SELECT COALESCE(SUM(net_amount), 0) FROM bi.fact_sales_order)
                    = (SELECT COALESCE(SUM(buyer_amount), 0) FROM bi.fact_order_line WHERE NOT is_cancelled)""")).scalar_one()
            c["recommendations_consistent"] = conn.execute(text("""SELECT NOT EXISTS (SELECT 1 FROM bi.replenishment_recommendation
                WHERE recommended_order_qty < 0
                   OR (action_status IN ('OK', 'NO_DEMAND_DATA') AND recommended_order_qty > 0)
                   OR (action_status <> 'NO_DEMAND_DATA' AND reorder_point < safety_stock))""")).scalar_one()
            c["mart_reconciles_public"] = conn.execute(text("""SELECT
                (SELECT (COUNT(*), SUM(quantity), SUM(buyer_total_amount) FILTER (WHERE NOT is_cancelled)) FROM public.Fact_Orders)
                = (SELECT (COUNT(*), SUM(quantity), SUM(net_amount)) FROM mart.fact_sales_line)
                AND (SELECT COUNT(DISTINCT (platform, order_id)) FROM public.Fact_Orders) = (SELECT COUNT(*) FROM mart.fact_sales_order)
                AND (SELECT COUNT(*) FROM public.Fact_Forecast_Predictions) = (SELECT COUNT(*) FROM mart.fact_demand_forecast)""")).scalar_one()
            c["mart_refreshed_recently"] = conn.execute(text("""SELECT COALESCE(MAX(finished_at) > NOW() - INTERVAL '5 minutes', FALSE)
                FROM mart.refresh_log""")).scalar_one()
            c["mart_has_today_recommendations"] = conn.execute(text("""SELECT COUNT(*) > 0 FROM mart.fact_replenishment_daily
                WHERE evaluation_date_key = mart.date_key((NOW() AT TIME ZONE 'Asia/Ho_Chi_Minh')::DATE)""")).scalar_one()
            c["mart_date_contiguous"] = conn.execute(text("""SELECT MAX(full_date) - MIN(full_date) + 1 = COUNT(*)
                FROM mart.dim_date""")).scalar_one()
            result["decision_summary"] = [dict(r) for r in conn.execute(text("""SELECT action_status, COUNT(*) AS skus,
                SUM(recommended_order_qty) AS qty FROM bi.replenishment_recommendation GROUP BY 1 ORDER BY 1""")).mappings()]
            kpi = conn.execute(text("""SELECT
                (SELECT COALESCE(SUM(quantity),0) FROM bi.fact_order_line WHERE NOT is_cancelled) AS bi_units,
                (SELECT COALESCE(SUM(units_sold),0) FROM (""" + DATASET_QUERIES["Fact_Orders_Summary.csv"] + """) s) AS summary_units,
                (SELECT COUNT(*) FROM bi.fact_order_line) AS bi_lines,
                (SELECT COUNT(*) FROM public.Fact_Orders) AS fact_lines""")).mappings().one()
            result["kpi"] = {k: str(v) for k, v in kpi.items()}
            c["bi_fact_matches_public_fact"] = kpi["bi_lines"] == kpi["fact_lines"]
            c["summary_units_match_line_fact"] = kpi["bi_units"] == kpi["summary_units"]
            fresh = conn.execute(text("""SELECT MAX(loaded_at) AS last_loaded, MAX(create_time) AS last_order,
                EXTRACT(EPOCH FROM (LOCALTIMESTAMP - MAX(loaded_at))) / 60 AS minutes_since_load
                FROM public.Fact_Orders""")).mappings().one()
            result["freshness"] = {k: str(v) for k, v in fresh.items()}
            result["header_conflicts_flagged"] = conn.execute(text("SELECT COUNT(*) FROM bi.quality_order_header_conflicts")).scalar_one()
        result["status"] = "PASS" if all(result["checks"].values()) else "FAIL"
    except Exception as exc:
        result["error"] = f"{type(exc).__name__}: {exc}"
    finally:
        engine.dispose()
        out = ROOT / f"output/powerbi-refresh-check-{now:%Y-%m-%d}"
        out.mkdir(parents=True, exist_ok=True)
        path = out / f"{now:%H%M%S}{'-' + args.label if args.label else ''}.json"
        path.write_text(json.dumps(result, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")
        total = sum(s["rows"] for s in result["sources"].values())
        print(json.dumps({"status": result["status"], "sources": len(result["sources"]), "rows_loaded": total,
                          "checks": result["checks"], "freshness": result.get("freshness"),
                          "error": result.get("error"), "evidence": str(path.relative_to(ROOT))}, ensure_ascii=False, default=str))
    return 0 if result["status"] == "PASS" else 1


if __name__ == "__main__":
    sys.exit(main())
