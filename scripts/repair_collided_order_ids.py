"""Khôi phục các đơn bị gộp do simulator dùng lại mã đơn — mặc định diễn tập rồi rollback.

Nguyên nhân (đã sửa ngày 17/11/2026): bộ đếm mã đơn của data_simulator quay về 000001 mỗi lần
khởi động lại producer trong cùng ngày, nên hai đơn KHÁC nhau (khác thời điểm tạo, khách, địa chỉ)
nhận cùng mã. Upsert theo (order_id, platform, source_line_id) gộp chúng thành một, làm mất đơn.
Producer nay gắn mã lượt chạy vào mã đơn, và ETL cách ly xung đột danh tính (ORDER_IDENTITY_CONFLICT).

Khôi phục từ kho thô MinIO (nguồn bất biến), không từ warehouse đã bị gộp:
  1. đọc toàn bộ raw/orders, chuẩn hóa + kiểm định bằng đúng các bước ETL;
  2. mã đơn có nhiều hơn một thời điểm tạo = nhiều đơn dùng chung mã (bản gửi lại/cập nhật trạng thái
     có cùng thời điểm tạo nên không bị tính);
  3. đơn tạo sớm nhất giữ mã gốc (cùng quy tắc với ETL), các đơn sau nhận mã "<mã gốc>-HHMMSS"
     theo giờ tạo đơn (giờ Việt Nam);
  4. trong một giao dịch: xóa các dòng Fact_Orders của mã bị gộp rồi nạp lại bằng warehouse.etl.load;
  5. kiểm tra số đơn/dòng/số lượng/tiền khớp kho thô; ghi bảng ánh xạ + bằng chứng.
Sau --apply cần chạy: python -m warehouse.reporting.mart --full

    python scripts/repair_collided_order_ids.py            # diễn tập, rollback
    python scripts/repair_collided_order_ids.py --apply    # commit khi mọi kiểm tra đạt
"""
import argparse
from contextlib import contextmanager
import csv
from datetime import datetime
import json
import logging
from pathlib import Path
import sys
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from dotenv import load_dotenv
import pandas as pd
from sqlalchemy import create_engine, text

from powerbi.export_powerbi_dataset import _database_url
from warehouse.etl.coordination import pipeline_lock
from warehouse.etl.data_validation import DataValidator
from warehouse.etl.extract import build_minio_client, extract_orders
from warehouse.etl.load import load
from warehouse.etl.transform import clean_data, deduplicate_lines, unify_schema

EVIDENCE_DIR = ROOT / "docs" / "07-bao-cao-do-an" / "evidence"


class Connection:
    """Cho load() ghi vào giao dịch đang mở (savepoint), để xóa + nạp lại là một khối nguyên tử."""

    def __init__(self, conn):
        self.conn = conn

    @contextmanager
    def begin(self):
        with self.conn.begin_nested():
            yield self.conn

    @contextmanager
    def connect(self):
        yield self.conn


def raw_orders():
    """Các dòng đơn hợp lệ trong kho thô, đã chuẩn hóa như ETL (chỉ đọc MinIO)."""
    raw = extract_orders(build_minio_client())
    normalized = clean_data(unify_schema(raw), preserve_invalid=True)
    valid, rejected, _ = DataValidator().validate(normalized)
    valid = valid.assign(_created=pd.to_datetime(valid["create_time"]).dt.floor("s"))
    return valid, len(raw), len(rejected)


def plan(valid):
    """Bảng ánh xạ (platform, mã gốc, thời điểm tạo) → mã mới, chỉ cho các mã bị dùng chung."""
    distinct = valid.groupby(["platform", "order_id"])["_created"].nunique()
    shared = distinct[distinct > 1].reset_index()[["platform", "order_id"]]
    orders = (valid.merge(shared, on=["platform", "order_id"])
              .groupby(["platform", "order_id", "_created"], as_index=False)
              .agg(raw_lines=("source_line_id", "nunique")))
    orders = orders.sort_values(["platform", "order_id", "_created"]).reset_index(drop=True)
    first = orders.groupby(["platform", "order_id"]).cumcount() == 0
    orders["new_order_id"] = orders["order_id"].where(
        first, orders["order_id"] + "-" + orders["_created"].dt.strftime("%H%M%S"))
    if orders.duplicated(["platform", "new_order_id"]).any():
        raise SystemExit("Mã mới bị trùng; cần quy tắc đặt mã khác")
    if orders["new_order_id"].str.len().max() > 50:
        raise SystemExit("Mã mới vượt 50 ký tự (Fact_Orders.order_id)")
    return shared, orders


def rebuilt_lines(valid, orders):
    rows = valid.merge(orders[["platform", "order_id", "_created", "new_order_id"]],
                       on=["platform", "order_id", "_created"])
    rows = rows.assign(order_id=rows["new_order_id"]).drop(columns=["new_order_id", "_created"])
    latest, _ = deduplicate_lines(rows)  # bản cập nhật mới nhất của mỗi dòng đơn thắng, như ETL
    return latest


def summary(conn, ids):
    row = conn.execute(text("""
        SELECT COUNT(DISTINCT (platform, order_id)) orders, COUNT(*) lines,
               COALESCE(SUM(quantity), 0) units, COALESCE(SUM(buyer_total_amount), 0) amount
        FROM Fact_Orders WHERE order_id = ANY(:ids)"""), {"ids": ids}).mappings().one()
    return {"orders": int(row["orders"]), "lines": int(row["lines"]),
            "units": int(row["units"]), "amount": str(row["amount"])}


def totals(conn):
    row = conn.execute(text("""SELECT COUNT(*) lines, SUM(quantity) units, SUM(buyer_total_amount) amount
                               FROM Fact_Orders""")).mappings().one()
    return {"lines": int(row["lines"]), "units": int(row["units"]), "amount": str(row["amount"])}


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

    valid, raw_count, raw_rejected = raw_orders()
    shared, orders = plan(valid)
    lines = rebuilt_lines(valid, orders)
    old_ids = sorted(shared["order_id"].unique())
    new_ids = sorted(orders["new_order_id"].unique())
    expected = {"orders": len(orders), "lines": len(lines), "units": int(lines["quantity"].sum()),
                "amount": str(round(float(lines["buyer_total_amount"].sum()), 2))}
    result["raw_lake"] = {"rows": raw_count, "rejected_rows": raw_rejected, "shared_order_ids": len(shared),
                          "distinct_orders_behind_shared_ids": len(orders),
                          "shared_ids_by_prefix": shared["order_id"].str[:9].value_counts().to_dict()}
    result["expected_after"] = expected

    with pipeline_lock(engine), engine.connect() as conn:
        transaction = conn.begin()
        try:
            conn.exec_driver_sql("SET LOCAL lock_timeout = '10s'")
            clash = conn.execute(text("SELECT COUNT(*) FROM Fact_Orders WHERE order_id = ANY(:ids)"),
                                 {"ids": [i for i in new_ids if i not in old_ids]}).scalar_one()
            if clash:
                raise SystemExit(f"{clash} dòng đã dùng mã mới; đã khôi phục trước đó?")
            result["before"] = {"affected": summary(conn, old_ids), "fact_orders": totals(conn)}
            deleted = conn.execute(text("DELETE FROM Fact_Orders WHERE order_id = ANY(:ids)"),
                                   {"ids": old_ids}).rowcount
            loaded = load(lines, engine=Connection(conn))
            after = summary(conn, new_ids)
            result["after"] = {"affected": after, "fact_orders": totals(conn),
                               "deleted_lines": deleted, "loaded_lines": loaded.get("rows_loaded")}
            amount_ok = abs(float(after["amount"]) - float(expected["amount"])) < 0.01
            checks = {
                "every_raw_order_restored": after["orders"] == expected["orders"],
                "lines_match_raw": after["lines"] == expected["lines"],
                "units_match_raw": after["units"] == expected["units"],
                "amount_matches_raw": amount_ok,
                "no_other_fact_changed": (result["after"]["fact_orders"]["lines"]
                                          == result["before"]["fact_orders"]["lines"] - deleted + after["lines"]),
            }
            result["checks"] = checks
            if not all(checks.values()):
                raise RuntimeError("Kiểm tra sau khôi phục không đạt: " + json.dumps(checks))
            if args.apply:
                transaction.commit()
            else:
                transaction.rollback()
            result["status"] = "PASS"
        except BaseException:
            if transaction.is_active:
                transaction.rollback()
            raise
        finally:
            folder = ROOT / "output" / f"order-id-repair-{now:%Y%m%d}"
            folder.mkdir(parents=True, exist_ok=True)
            with open(folder / f"order_id_mapping_{mode}.csv", "w", newline="", encoding="utf-8-sig") as handle:
                writer = csv.writer(handle)
                writer.writerow(["platform", "original_order_id", "create_time_vn", "new_order_id", "raw_lines"])
                for platform, original, created, new, raw_lines in zip(
                        orders["platform"], orders["order_id"], orders["_created"],
                        orders["new_order_id"], orders["raw_lines"]):
                    writer.writerow([platform, original, created.isoformat(sep=" "), new, raw_lines])
            EVIDENCE_DIR.mkdir(parents=True, exist_ok=True)
            out = EVIDENCE_DIR / f"order-id-repair-{mode}-{now:%Y%m%d-%H%M%S}.json"
            out.write_text(json.dumps(result, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
            print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
            print("evidence:", out.relative_to(ROOT))


if __name__ == "__main__":
    main()
