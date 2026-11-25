"""Gom các shop_id trùng tên về một shop chuẩn theo (platform, shop_name) — mặc định diễn tập rồi rollback.

Bối cảnh: simulator cũ (commit 3768ac0, 25/09/2026) sinh shop_id ngẫu nhiên cho mỗi đơn Shopee,
nên Dim_Shops có hàng chục nghìn dòng cho 10 tên shop. Migration này:
  1. chọn shop_key nhỏ nhất của mỗi (platform, shop_name) làm shop chuẩn;
  2. trỏ Fact_Orders.shop_key của các dòng trùng tên về shop chuẩn;
  3. xóa các dòng Dim_Shops không còn được tham chiếu sau bước 2.
Không đổi số dòng, số lượng hay số tiền của fact. Giữ khóa ETL chung với worker để không chạy chồng.
Bằng chứng: docs/07-bao-cao-do-an/evidence/shop_consolidation_<mode>_<ngày>.json và bảng ánh xạ
output/shop-consolidation-<ngày>/shop_key_mapping_<mode>.csv.

    python scripts/migrate_consolidate_shops.py            # diễn tập, rollback
    python scripts/migrate_consolidate_shops.py --apply    # commit khi mọi kiểm tra bảo toàn đạt
"""
import argparse
import csv
from datetime import datetime
import json
from pathlib import Path
import sys
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

from powerbi.export_powerbi_dataset import _database_url
from warehouse.etl.coordination import pipeline_lock
from warehouse.reporting.install import install_bi_schema


# Shop của demo web và script kiểm tra nay gửi shop_id cố định; shop chuẩn phải mang đúng id đó,
# nếu không lượt demo kế tiếp sẽ tạo lại dòng thứ hai cho cùng tên.
STABLE_SHOP_IDS = {
    ("shopee", "DEMO bảo vệ"): "DEMO-SHOPEE",
    ("shopee", "Continuous test"): "CONTINUOUS-TEST",
    ("shopee", "Fault test"): "FAULT-TEST",
}


def totals(conn):
    """Tổng fact và tổng theo tên shop — phải giống hệt trước/sau."""
    overall = dict(conn.execute(text("""SELECT COUNT(*) AS order_lines,
        COALESCE(SUM(quantity),0) AS units, COALESCE(SUM(buyer_total_amount),0) AS buyer_amount,
        COUNT(*) FILTER (WHERE shop_key IS NULL) AS null_shop_lines FROM public.Fact_Orders""")).mappings().one())
    by_name = {f"{r.platform}|{r.shop_name}": [int(r.lines), int(r.units), str(r.amount)] for r in conn.execute(text("""
        SELECT s.platform, s.shop_name, COUNT(*) lines, SUM(f.quantity) units, SUM(f.buyer_total_amount) amount
        FROM public.Fact_Orders f JOIN public.Dim_Shops s USING(shop_key) GROUP BY 1, 2"""))}
    shops = dict(conn.execute(text("""SELECT COUNT(*) AS shop_rows,
        COUNT(DISTINCT (platform, shop_name)) AS distinct_shops FROM public.Dim_Shops""")).mappings().one())
    return {**{k: str(v) for k, v in overall.items()}, **shops}, by_name


def consolidate(conn):
    conn.exec_driver_sql("""CREATE TEMP TABLE shop_map ON COMMIT DROP AS
        SELECT s.shop_key AS old_key, s.platform, s.shop_name, s.shop_id,
               MIN(s.shop_key) OVER (PARTITION BY s.platform, s.shop_name) AS canonical_key
        FROM public.Dim_Shops s""")
    mapping = [dict(r) for r in conn.execute(text("""
        SELECT m.old_key, m.platform, m.shop_name, m.shop_id, m.canonical_key, c.shop_id AS canonical_shop_id,
               (SELECT COUNT(*) FROM public.Fact_Orders f WHERE f.shop_key = m.old_key) AS facts_moved
        FROM shop_map m JOIN public.Dim_Shops c ON c.shop_key = m.canonical_key
        WHERE m.old_key <> m.canonical_key ORDER BY m.platform, m.shop_name, m.old_key""")).mappings()]
    moved = conn.execute(text("""UPDATE public.Fact_Orders f SET shop_key = m.canonical_key
        FROM shop_map m WHERE f.shop_key = m.old_key AND m.old_key <> m.canonical_key""")).rowcount
    # Fact_Inventory_Daily.shop_key luôn NULL (ràng buộc ck_inventory_central_scope), không cần trỏ lại.
    deleted = conn.execute(text("""DELETE FROM public.Dim_Shops d USING shop_map m
        WHERE d.shop_key = m.old_key AND m.old_key <> m.canonical_key""")).rowcount
    renamed = []
    for (platform, name), shop_id in STABLE_SHOP_IDS.items():
        row = conn.execute(text("""UPDATE public.Dim_Shops SET shop_id = :sid
            WHERE platform = :p AND shop_name = :n AND shop_id <> :sid RETURNING shop_key"""),
            {"sid": shop_id, "p": platform, "n": name}).first()
        if row:
            renamed.append({"platform": platform, "shop_name": name, "shop_key": row[0], "shop_id": shop_id})
    return mapping, moved, deleted, renamed


def validate(conn, before, before_by_name):
    after, after_by_name = totals(conn)
    checks = {
        "fact_totals_unchanged": all(before[k] == after[k] for k in ("order_lines", "units", "buyer_amount", "null_shop_lines")),
        "totals_per_shop_name_unchanged": before_by_name == after_by_name,
        "one_key_per_shop_name": after["shop_rows"] == after["distinct_shops"],
        "no_orphan_shop_keys": conn.execute(text("""SELECT NOT EXISTS (SELECT 1 FROM public.Fact_Orders f
            LEFT JOIN public.Dim_Shops s USING(shop_key) WHERE f.shop_key IS NOT NULL AND s.shop_key IS NULL)""")).scalar_one(),
        "bi_view_matches_fact": conn.execute(text("""SELECT (SELECT COUNT(*) FROM bi.fact_order_line)
            = (SELECT COUNT(*) FROM public.Fact_Orders)""")).scalar_one(),
        "quality_view_empty": conn.execute(text("SELECT COUNT(*) = 0 FROM bi.quality_shop_identity")).scalar_one(),
    }
    return after, checks


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--apply", action="store_true", help="Commit sau khi mọi kiểm tra bảo toàn đạt")
    args = parser.parse_args()
    load_dotenv(ROOT / ".env")
    now = datetime.now(ZoneInfo("Asia/Ho_Chi_Minh"))
    mode = "apply" if args.apply else "rehearsal"
    engine = create_engine(_database_url(), pool_pre_ping=True, connect_args={"connect_timeout": 5})
    result = {"mode": mode, "status": "FAIL", "checked_at": now.isoformat()}
    try:
        with pipeline_lock(engine), engine.connect() as conn:
            transaction = conn.begin()
            try:
                conn.exec_driver_sql("SET LOCAL lock_timeout = '10s'")
                before, before_by_name = totals(conn)
                mapping, moved, deleted, renamed = consolidate(conn)
                install_bi_schema(conn)  # cài view chất lượng bi.quality_shop_identity
                after, checks = validate(conn, before, before_by_name)
                result.update(before=before, after=after, checks=checks, facts_repointed=moved,
                              shop_rows_deleted=deleted, mapped_shop_keys=len(mapping),
                              stable_shop_ids_assigned=renamed)
                if not all(checks.values()):
                    raise RuntimeError("Kiểm tra bảo toàn không đạt: " + ", ".join(k for k, v in checks.items() if not v))
                out = ROOT / f"output/shop-consolidation-{now:%Y-%m-%d}"
                out.mkdir(parents=True, exist_ok=True)
                with (out / f"shop_key_mapping_{mode}.csv").open("w", encoding="utf-8-sig", newline="") as handle:
                    writer = csv.DictWriter(handle, fieldnames=list(mapping[0]) if mapping else ["old_key"])
                    writer.writeheader(); writer.writerows(mapping)
                result["mapping_csv"] = str((out / f"shop_key_mapping_{mode}.csv").relative_to(ROOT))
                if args.apply:
                    transaction.commit()
                    result["status"] = "APPLIED_COMMITTED"
                else:
                    transaction.rollback()
                    result["status"] = "REHEARSAL_PASSED_ROLLED_BACK"
            except Exception:
                transaction.rollback()
                raise
    except Exception as exc:
        result["error"] = f"{type(exc).__name__}: {exc}"
    finally:
        engine.dispose()
        path = ROOT / f"docs/07-bao-cao-do-an/evidence/shop_consolidation_{mode}_{now:%Y-%m-%d}.json"
        path.write_text(json.dumps(result, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")
        print(json.dumps({k: result.get(k) for k in ("mode", "status", "facts_repointed", "shop_rows_deleted", "checks", "error")},
                         ensure_ascii=False, default=str))
    return 0 if result["status"] != "FAIL" else 1


if __name__ == "__main__":
    sys.exit(main())
