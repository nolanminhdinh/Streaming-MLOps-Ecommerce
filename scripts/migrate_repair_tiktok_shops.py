"""Trả đúng shop cho các đơn TikTok bị dồn vào shop_id rỗng — mặc định diễn tập rồi rollback.

Nguyên nhân (đã sửa trong warehouse/etl/transform.py ngày 16/11/2026): lô ETL trộn Shopee + TikTok làm
shop_id TikTok thành NaN; load ghi thành chuỗi rỗng nên mọi đơn TikTok của lô trộn dồn vào một
dòng Dim_Shops (platform='tiktok', shop_id=''), và tên dòng đó bị ghi đè theo lô gần nhất.

Mỗi đơn được quy về shop theo thứ tự căn cứ, ghi lại trong bảng bằng chứng:
  1. raw_lake        — shop_name trong bản gốc Parquet trên MinIO (raw/orders/platform=tiktok);
  2. generator_code  — đơn SIM26YTD-… do data_simulator/history.py sinh (lần 8), simulator này
                       luôn dùng shop TikTok cố định ENTERPRISE_CONFIG = 'MockStore Official Shop';
  3. unknown         — không còn bản gốc (simulator cũ chọn ngẫu nhiên 1 trong 10 tên shop):
                       gán shop 'Không xác định (TikTok, simulator cũ)', không đoán tên.
Không đổi số dòng, số lượng hay số tiền của fact.

    python scripts/migrate_repair_tiktok_shops.py            # diễn tập, rollback
    python scripts/migrate_repair_tiktok_shops.py --apply    # commit khi mọi kiểm tra đạt
"""
import argparse
from collections import Counter
import csv
from datetime import datetime
import io
import json
import logging
from pathlib import Path
import sys
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from dotenv import load_dotenv
import pyarrow.parquet as pq
from sqlalchemy import create_engine, text

from powerbi.export_powerbi_dataset import _database_url
from warehouse.etl.coordination import pipeline_lock
from warehouse.etl.extract import MINIO_BUCKET, build_minio_client

GENERATOR_PREFIX = "SIM26YTD-"
GENERATOR_SHOP = "MockStore Official Shop"
UNKNOWN_SHOP = ("UNKNOWN-TIKTOK-LEGACY", "Không xác định (TikTok, simulator cũ)")


def raw_shop_names(order_ids):
    """order_id → shop_name từ bản gốc TikTok trên MinIO (chỉ đọc)."""
    found, client = {}, build_minio_client()
    for obj in client.list_objects(MINIO_BUCKET, prefix="raw/orders/platform=tiktok/", recursive=True):
        response = client.get_object(MINIO_BUCKET, obj.object_name)
        try:
            table = pq.read_table(io.BytesIO(response.read()), columns=None)
        finally:
            response.close(); response.release_conn()
        if {"order_id", "shop_name"} <= set(table.column_names):
            for order_id, name in zip(table.column("order_id").to_pylist(), table.column("shop_name").to_pylist()):
                if str(order_id) in order_ids and name and str(order_id) not in found:
                    found[str(order_id)] = (str(name), obj.object_name)
    return found


def totals(conn):
    return {f"{r.platform}": [int(r.lines), int(r.units), str(r.amount)] for r in conn.execute(text("""
        SELECT platform, COUNT(*) lines, SUM(quantity) units, SUM(buyer_total_amount) amount
        FROM public.Fact_Orders GROUP BY platform"""))}


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    logging.disable(logging.CRITICAL)
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
                blank_keys = [r[0] for r in conn.execute(text(
                    "SELECT shop_key FROM public.Dim_Shops WHERE platform='tiktok' AND btrim(shop_id)=''"))]
                facts = [dict(r) for r in conn.execute(text("""SELECT order_fact_id, order_id FROM public.Fact_Orders
                    WHERE platform='tiktok' AND shop_key = ANY(:keys)"""), {"keys": blank_keys}).mappings()]
                result.update(blank_shop_keys=blank_keys, facts_on_blank_shop=len(facts))
                before = totals(conn)
                raw = raw_shop_names({f["order_id"] for f in facts}) if facts else {}
                rows = []
                for f in facts:
                    if f["order_id"] in raw:
                        rows.append((f["order_fact_id"], f["order_id"], "raw_lake", raw[f["order_id"]][0], raw[f["order_id"]][0], raw[f["order_id"]][1]))
                    elif f["order_id"].startswith(GENERATOR_PREFIX):
                        rows.append((f["order_fact_id"], f["order_id"], "generator_code", GENERATOR_SHOP, GENERATOR_SHOP,
                                     "data_simulator/history.py + ENTERPRISE_CONFIG"))
                    else:
                        rows.append((f["order_fact_id"], f["order_id"], "unknown", UNKNOWN_SHOP[1], UNKNOWN_SHOP[0], ""))
                # TikTok: ETL dùng shop_name làm shop_id; shop không xác định có id riêng.
                for shop_id, shop_name in {(r[4], r[3]) for r in rows}:
                    conn.execute(text("""INSERT INTO public.Dim_Shops (shop_id, shop_name, platform)
                        VALUES (:sid, :name, 'tiktok') ON CONFLICT (shop_id, platform) DO NOTHING"""), {"sid": shop_id, "name": shop_name})
                conn.exec_driver_sql("CREATE TEMP TABLE shop_fix (order_fact_id BIGINT PRIMARY KEY, shop_id TEXT) ON COMMIT DROP")
                if rows:
                    conn.execute(text("INSERT INTO shop_fix VALUES (:i, :s)"), [{"i": r[0], "s": r[4]} for r in rows])
                moved = conn.execute(text("""UPDATE public.Fact_Orders f SET shop_key = s.shop_key
                    FROM shop_fix x JOIN public.Dim_Shops s ON s.platform='tiktok' AND s.shop_id = x.shop_id
                    WHERE f.order_fact_id = x.order_fact_id""")).rowcount
                deleted = conn.execute(text("""DELETE FROM public.Dim_Shops d WHERE d.shop_key = ANY(:keys)
                    AND NOT EXISTS (SELECT 1 FROM public.Fact_Orders f WHERE f.shop_key = d.shop_key)"""), {"keys": blank_keys}).rowcount
                after = totals(conn)
                basis = Counter((r[2], r[3]) for r in rows)
                checks = {
                    "fact_totals_per_platform_unchanged": before == after,
                    "all_blank_shop_facts_repointed": moved == len(facts),
                    "no_facts_on_blank_tiktok_shop": conn.execute(text("""SELECT NOT EXISTS (SELECT 1 FROM public.Fact_Orders f
                        JOIN public.Dim_Shops s USING(shop_key) WHERE s.platform='tiktok' AND btrim(s.shop_id)='')""")).scalar_one(),
                    "one_key_per_shop_name": conn.execute(text("SELECT COUNT(*) = 0 FROM bi.quality_shop_identity")).scalar_one(),
                    "no_orphan_shop_keys": conn.execute(text("""SELECT NOT EXISTS (SELECT 1 FROM public.Fact_Orders f
                        LEFT JOIN public.Dim_Shops s USING(shop_key) WHERE f.shop_key IS NOT NULL AND s.shop_key IS NULL)""")).scalar_one(),
                }
                result.update(before=before, after=after, facts_repointed=moved, blank_shop_rows_deleted=deleted,
                              attribution={f"{b} → {n}": c for (b, n), c in sorted(basis.items())}, checks=checks)
                if not all(checks.values()):
                    raise RuntimeError("Kiểm tra không đạt: " + ", ".join(k for k, v in checks.items() if not v))
                out = ROOT / f"output/shop-consolidation-{now:%Y-%m-%d}"
                out.mkdir(parents=True, exist_ok=True)
                with (out / f"tiktok_shop_attribution_{mode}.csv").open("w", encoding="utf-8-sig", newline="") as handle:
                    writer = csv.writer(handle)
                    writer.writerow(["order_fact_id", "order_id", "basis", "shop_name", "shop_id", "evidence"])
                    writer.writerows(rows)
                result["attribution_csv"] = str((out / f"tiktok_shop_attribution_{mode}.csv").relative_to(ROOT))
                if args.apply:
                    transaction.commit(); result["status"] = "APPLIED_COMMITTED"
                else:
                    transaction.rollback(); result["status"] = "REHEARSAL_PASSED_ROLLED_BACK"
            except Exception:
                transaction.rollback()
                raise
    except Exception as exc:
        result["error"] = f"{type(exc).__name__}: {exc}"
    finally:
        engine.dispose()
        path = ROOT / f"docs/07-bao-cao-do-an/evidence/tiktok_shop_repair_{mode}_{now:%Y-%m-%d}.json"
        path.write_text(json.dumps(result, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")
        print(json.dumps({k: result.get(k) for k in ("mode", "status", "facts_on_blank_shop", "facts_repointed",
                                                     "attribution", "checks", "error")}, ensure_ascii=False, default=str))
    return 0 if result["status"] != "FAIL" else 1


if __name__ == "__main__":
    sys.exit(main())
