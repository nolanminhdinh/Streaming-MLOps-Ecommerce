"""Tính lại các con số của báo cáo Power BI bằng SQL độc lập trên schema mart (chỉ đọc).

Dùng để đối chiếu số hiển thị trên báo cáo PBIP (powerbi/BaoCaoQuanLy) sau khi Refresh: mỗi chỉ số
được tính lại đúng theo định nghĩa measure DAX trong scripts/build_powerbi_project.py nhưng bằng SQL,
nên sai lệch giữa hai bên là lỗi của measure hoặc của bước nạp.

    python scripts/check_powerbi_kpi_values.py --cutoff "2026-11-17 00:50:59" --label sau-refresh

--cutoff giới hạn chỉ số đơn hàng tại thời điểm đơn mới nhất trên báo cáo (thẻ "Đơn mới nhất"), vì
producer vẫn chạy sau lúc Refresh. Đề xuất đặt hàng của ngày hiện tại được làm mới liên tục (mart cập nhật
khoảng 2 giây một lần), nên chỉ số tồn kho/đặt hàng phản ánh thời điểm chạy lệnh và có thể lệch nhẹ.
"""
import argparse
from datetime import datetime, timedelta
import json
import logging
from pathlib import Path
import sys
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

from powerbi.export_powerbi_dataset import _database_url

EVIDENCE_DIR = ROOT / "docs" / "07-bao-cao-do-an" / "evidence"


def number(value):
    return float(value) if value is not None and not isinstance(value, (int, str)) else value


def compute(conn, cutoff, today):
    one = lambda sql, **p: conn.execute(text(sql), p).mappings().one()
    rows = lambda sql, **p: [dict(r) for r in conn.execute(text(sql), p).mappings()]
    result = {}

    s = one("""SELECT COUNT(*) FILTER (WHERE NOT is_cancelled) orders, COUNT(*) all_orders,
                      COUNT(*) FILTER (WHERE is_cancelled) cancelled, SUM(net_amount) revenue, MAX(create_time) last_create
               FROM mart.fact_sales_order WHERE create_time <= :cut""", cut=cutoff)
    units = one("SELECT SUM(net_quantity) u FROM mart.fact_sales_line WHERE create_time <= :cut", cut=cutoff)["u"]
    yesterday = today - timedelta(days=1)
    key = lambda d: int(d.strftime("%Y%m%d"))
    week = one("""SELECT SUM(net_amount) FILTER (WHERE order_date_key BETWEEN :a AND :b) last7,
                         SUM(net_amount) FILTER (WHERE order_date_key BETWEEN :c AND :d) prev7
                  FROM mart.fact_sales_order WHERE create_time <= :cut""",
               a=key(yesterday - timedelta(days=6)), b=key(yesterday),
               c=key(yesterday - timedelta(days=13)), d=key(yesterday - timedelta(days=7)), cut=cutoff)
    revenue = float(s["revenue"])
    platform = rows("""SELECT p.platform_name, SUM(o.net_amount) revenue FROM mart.fact_sales_order o
                       JOIN mart.dim_platform p USING (platform_key) WHERE o.create_time <= :cut GROUP BY 1 ORDER BY 1""", cut=cutoff)
    result["don_hang_tu_cac_san"] = {
        "Đơn hợp lệ": s["orders"], "Doanh thu": revenue, "Giá trị đơn TB": revenue / s["orders"],
        "Sản phẩm bán": units, "Tỷ lệ hủy": s["cancelled"] / s["all_orders"],
        "Tăng trưởng DT 7 ngày": float(week["last7"] - week["prev7"]) / float(week["prev7"]),
        "Tỷ trọng doanh thu theo sàn": {r["platform_name"]: {"doanh_thu": float(r["revenue"]), "ty_trong": float(r["revenue"]) / revenue}
                                        for r in platform},
        "Đơn mới nhất": str(s["last_create"]),
    }

    latest = one("SELECT MAX(evaluation_date_key) k FROM mart.fact_replenishment_daily")["k"]
    r = one("""SELECT COUNT(*) tracked,
          COUNT(*) FILTER (WHERE action_status = 'CRITICAL') critical,
          COUNT(*) FILTER (WHERE action_status IN ('CRITICAL', 'REORDER')) to_reorder,
          COUNT(*) FILTER (WHERE action_status = 'OK') ok,
          COUNT(*) FILTER (WHERE days_of_cover IS NOT NULL AND days_of_cover <= 7) stockout7,
          PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY days_of_cover) median_cover,
          COUNT(*) FILTER (WHERE urgency = 'Đặt ngay') order_now,
          COUNT(*) FILTER (WHERE urgency = 'Đặt trong tuần') order_week,
          SUM(recommended_order_qty) qty, SUM(order_value_at_selling_price) order_value,
          COUNT(*) FILTER (WHERE lead_time_defaulted) default_lead
        FROM mart.fact_replenishment_daily WHERE evaluation_date_key = :k""", k=latest)
    result["suc_khoe_ton_kho"] = {
        "Ngày đánh giá": latest, "SKU nguy cấp": r["critical"], "SKU cần đặt": r["to_reorder"], "SKU ổn định": r["ok"],
        "Hết hàng trong 7 ngày": r["stockout7"], "Số ngày tồn (trung vị)": number(r["median_cover"]),
        "Ma trận ABC × XYZ": rows("""SELECT p.abc_class, p.xyz_class, COUNT(*) n FROM mart.fact_replenishment_daily f
            JOIN mart.dim_product p USING (product_key) WHERE f.evaluation_date_key = :k
            AND f.action_status IN ('CRITICAL', 'REORDER') GROUP BY 1, 2 ORDER BY 1, 2""", k=latest),
    }
    result["dat_hang_nha_cung_cap"] = {
        "SKU đặt ngay": r["order_now"], "SKU đặt trong tuần": r["order_week"],
        "Tổng lượng đề xuất": number(r["qty"]), "Giá trị (theo giá bán)": number(r["order_value"]),
        "Lịch sử tổng lượng đề xuất": {str(x["evaluation_date_key"]): float(x["qty"]) for x in rows(
            "SELECT evaluation_date_key, SUM(recommended_order_qty) qty FROM mart.fact_replenishment_daily GROUP BY 1 ORDER BY 1")},
    }

    serving = one("SELECT model_key, version_label, cv_wape, cv_bias FROM mart.dim_model WHERE is_serving")
    scored = rows("""SELECT m.version_label, f.horizon_days, COUNT(*) forecasts, COUNT(DISTINCT f.target_date_key) days,
                            SUM(f.absolute_error) err, SUM(f.actual_quantity) actual, SUM(f.predicted_quantity) predicted
                     FROM mart.fact_demand_forecast f JOIN mart.dim_model m USING (model_key)
                     WHERE f.actual_complete GROUP BY 1, 2 ORDER BY 1, 2""")
    t1 = [x for x in scored if x["version_label"] == serving["version_label"] and x["horizon_days"] == 1]
    result["du_bao_nhu_cau_ai"] = {
        "Model đang dùng": serving["version_label"], "WAPE backtest": float(serving["cv_wape"]) / 100,
        "Bias backtest": float(serving["cv_bias"]) / 100,
        "WAPE thực tế (t+1)": float(t1[0]["err"] / t1[0]["actual"]) if t1 and t1[0]["actual"] else None,
        "Số ngày đã chấm": t1[0]["days"] if t1 else 0,
        "So sánh phiên bản": [{"Phiên bản": x["version_label"], "Tầm dự báo": x["horizon_days"], "Số dự báo": x["forecasts"],
                               "Ngày chấm": x["days"], "WAPE": float(x["err"] / x["actual"]) if x["actual"] else None,
                               "Bias": float((x["predicted"] - x["actual"]) / x["actual"]) if x["actual"] else None}
                              for x in scored],
    }
    result["do_tin_cay_du_lieu"] = {
        "Ngày xác nhận gần nhất": str(one("SELECT MAX(full_date) d FROM mart.dim_date WHERE is_data_confirmed")["d"]),
        "SKU dùng lead time mặc định": r["default_lead"],
        "Đơn xung đột header": one("SELECT COUNT(*) n FROM mart.fact_sales_order WHERE has_header_conflict")["n"],
        "Đơn hôm nay (chưa xác nhận)": one("""SELECT COUNT(*) n FROM mart.fact_sales_order
            WHERE order_date_key = :k AND create_time <= :cut""", k=key(today), cut=cutoff)["n"],
    }
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--cutoff", help="giờ Việt Nam 'YYYY-MM-DD HH:MM:SS'; mặc định là hiện tại")
    parser.add_argument("--label", default="manual")
    args = parser.parse_args()
    logging.disable(logging.CRITICAL)
    load_dotenv(ROOT / ".env")
    now = datetime.now(ZoneInfo("Asia/Ho_Chi_Minh"))
    cutoff = args.cutoff or now.strftime("%Y-%m-%d %H:%M:%S")
    engine = create_engine(_database_url(), connect_args={"connect_timeout": 5})
    with engine.connect() as conn:
        values = compute(conn, cutoff, datetime.strptime(cutoff[:10], "%Y-%m-%d").date())
    result = {"checked_at": now.isoformat(), "cutoff_vn": cutoff, "label": args.label, "values": values}
    EVIDENCE_DIR.mkdir(parents=True, exist_ok=True)
    out = EVIDENCE_DIR / f"powerbi-kpi-{now:%Y%m%d-%H%M%S}-{args.label}.json"
    out.write_text(json.dumps(result, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    print("evidence:", out.relative_to(ROOT))


if __name__ == "__main__":
    main()
