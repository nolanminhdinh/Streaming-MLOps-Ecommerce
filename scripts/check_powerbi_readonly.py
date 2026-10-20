"""Audit actual Power BI export queries with existing views in a read-only transaction.

Writes fresh CSVs only into a separate local audit folder. Does not install views,
change warehouse facts or coverage, or replace powerbi/data and the PBIX report.
"""
import argparse
from collections import Counter
import csv
from datetime import date
from decimal import Decimal, ROUND_HALF_UP
import json
import logging
from pathlib import Path
import sys
from unittest.mock import patch
import zipfile

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from powerbi import export_powerbi_dataset as exporter

ROOT = Path(__file__).resolve().parents[1]


def pbix_structure():
    pages, visuals = [], []
    with zipfile.ZipFile(ROOT / "powerbi/Report_PowerBI.pbix") as archive:
        for name in archive.namelist():
            if name.endswith("/page.json"):
                item = json.loads(archive.read(name))
                pages.append({"name": item.get("name"), "title": item.get("displayName")})
            if name.endswith("/visual.json"):
                item = json.loads(archive.read(name))
                visual = item.get("visual", {})
                visuals.append({"type": visual.get("visualType"),
                                "has_query_binding": bool(visual.get("query"))})
    return {"pages": pages, "visuals": visuals, "visual_type_counts": dict(Counter(v["type"] for v in visuals)),
            "layout_only_no_render_or_refresh_verification": True}


def read_csv(path):
    with path.open(encoding="utf-8-sig", newline="") as source:
        return list(csv.DictReader(source))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--audit-date", required=True, type=date.fromisoformat)
    args = parser.parse_args()
    load_dotenv()
    logging.disable(logging.CRITICAL)
    snapshots = {}
    original_query = exporter._query

    def capture(conn, sql):
        rows = original_query(conn, sql)
        snapshots[list(exporter.DATASETS)[len(snapshots)]] = rows
        return rows

    output = ROOT / "docs/07-bao-cao-do-an/evidence" / f"powerbi_export_{args.audit_date}"
    with patch.object(exporter, "_install_views", side_effect=lambda conn: conn.exec_driver_sql("SET TRANSACTION READ ONLY")), \
         patch.object(exporter, "_query", side_effect=capture):
        exporter.export_powerbi_data(str(output))
    engine = create_engine(exporter._database_url(), connect_args={"connect_timeout": 3})
    try:
        with engine.connect() as conn:
            conn.execute(text("SET TRANSACTION READ ONLY"))
            source = conn.execute(text("""SELECT json_build_object(
                'noncancelled_lines', count(*), 'noncancelled_orders', count(DISTINCT (platform, order_id)),
                'units', sum(quantity), 'buyer_total_amount', sum(buyer_total_amount),
                'missing_shop_lines', count(*) FILTER(WHERE shop_key IS NULL),
                'missing_shop_units', sum(quantity) FILTER(WHERE shop_key IS NULL),
                'missing_shop_buyer_total', sum(buyer_total_amount) FILTER(WHERE shop_key IS NULL))
                FROM Fact_Orders WHERE is_cancelled=FALSE""")).scalar_one()
            joined = conn.execute(text("""SELECT json_build_object(
                'lines', count(*), 'orders', count(DISTINCT (f.platform, f.order_id)),
                'units', sum(f.quantity), 'buyer_total_amount', sum(f.buyer_total_amount))
                FROM Fact_Orders f JOIN Dim_Dates d USING(date_key) JOIN Dim_Shops s USING(shop_key)
                WHERE f.is_cancelled=FALSE""")).scalar_one()
            forecasts = conn.execute(text("""SELECT json_build_object('stored',count(*),
                'eligible_issue_date', count(*) FILTER(WHERE target_date >
                    (forecast_generated_at AT TIME ZONE 'Asia/Ho_Chi_Minh')::DATE),
                'retrospective_or_same_day', count(*) FILTER(WHERE target_date <=
                    (forecast_generated_at AT TIME ZONE 'Asia/Ho_Chi_Minh')::DATE))
                FROM Fact_Forecast_Predictions""")).scalar_one()
            coverage = conn.execute(text("SELECT count(*) FROM Warehouse_Daily_Coverage")).scalar_one()
            definitions = dict(conn.execute(text("""SELECT viewname, definition FROM pg_views
                WHERE schemaname='public' AND viewname IN
                ('vw_powerbi_forecast_vs_actual','vw_powerbi_executive_summary')""")).all())
            executive = conn.execute(text("""SELECT order_date, platform, total_orders,
                total_units_sold, total_revenue, average_order_value
                FROM vw_powerbi_executive_summary ORDER BY order_date, platform""")).mappings().all()
        summary = snapshots["Fact_Orders_Summary.csv"]
        products = snapshots["Dim_Products.csv"]
        inventory = snapshots["Inventory_Health_Alerts.csv"]
        forecast_rows = snapshots["Forecast_vs_Actual.csv"]
        histories = snapshots["Historical_Demand_Daily.csv"]
        old = ROOT / "powerbi/data"
        old_differences = {}
        for name in exporter.DATASETS:
            old_path = old / name
            if old_path.exists():
                old_rows = read_csv(old_path)
                new_rows = read_csv(output / name)
                old_differences[name] = {"old_rows": len(old_rows), "new_rows": len(new_rows),
                                         "content_equal": old_rows == new_rows}
        view_by_key = {(row["order_date"], row["platform"]): row for row in executive}
        aov_mismatches = sum(
            Decimal(str(row["aov"])) != (
                Decimal(str(row["net_revenue"])) / Decimal(row["total_orders"])
            ).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
            for row in summary
        )
        historical_skus = {r["sku"] for r in histories}
        product_skus = {r["sku"] for r in products}
        historical_data = [r for r in forecast_rows if r["actual_demand"] is not None]
        inventory_without_dates = sum(row["date_key"] is None for row in inventory)
        sum_dec = lambda rows, col: str(sum((Decimal(str(row[col] or 0)) for row in rows), Decimal(0)))
        result = {
            "audit_date": str(args.audit_date), "mode": "existing_views_readonly_local_export",
            "export_rows": {name: len(rows) for name, rows in snapshots.items()},
            "pbix": pbix_structure(), "source_noncancelled": source, "legacy_shop_join_scope": joined,
            "summary_csv_totals": {"orders": int(sum(row["total_orders"] for row in summary)),
                                   "units": int(sum(row["units_sold"] for row in summary)),
                                   "net_revenue": sum_dec(summary, "net_revenue")},
            "executive_view_orders": int(sum(row["total_orders"] for row in executive)),
            "summary_vs_executive_order_mismatch_groups": sum(
                row["total_orders"] != view_by_key[(row["order_date"], row["platform"])]["total_orders"] for row in summary),
            "summary_aov_mismatch_groups": aov_mismatches,
            "summary_orders_match_source": sum(row["total_orders"] for row in summary) == source["noncancelled_orders"],
            "summary_units_match_source": sum(row["units_sold"] for row in summary) == source["units"],
            "summary_revenue_matches_source": Decimal(sum_dec(summary, "net_revenue")) == Decimal(str(source["buyer_total_amount"])),
            "global_aov_from_totals": str((Decimal(sum_dec(summary, "net_revenue")) / sum(row["total_orders"] for row in summary)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)) if summary else None,
            "forecast": {**forecasts, "exported": len(forecast_rows), "complete_actual_rows": len(historical_data),
                         "actual_complete_true": sum(bool(row["actual_complete"]) for row in forecast_rows),
                         "coverage_rows": int(coverage),
                         "live_view_has_coverage_gate": "warehouse_daily_coverage" in definitions.get("vw_powerbi_forecast_vs_actual", "").lower(),
                         "live_view_has_issue_date_gate": "forecast_generated_at" in definitions.get("vw_powerbi_forecast_vs_actual", "").lower()},
            "inventory": {"rows": len(inventory), "alert_levels": dict(Counter(row["alert_level"] for row in inventory)),
                          "normal_with_positive_reorder_qty": sum(row["alert_level"] == "NORMAL" and row["recommended_reorder_qty"] > 0 for row in inventory),
                          "missing_date_key": inventory_without_dates},
            "historical_star_mapping": {"historical_skus": len(historical_skus), "product_dimension_skus": len(product_skus),
                                        "historical_skus_absent_from_product_dimension": len(historical_skus-product_skus)},
            "existing_csv_comparison": old_differences,
        }
        destination = output.parent / f"powerbi_{args.audit_date}.json"
        destination.write_text(json.dumps(result, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")
        print(json.dumps(result, ensure_ascii=True, indent=2, default=str))
    finally:
        engine.dispose()


if __name__ == "__main__":
    main()
