"""Import anonymized Shopee/TikTok order-line history for demand forecasting.

The CSV exports are read through a strict column allowlist. Customer identifiers,
contact details, addresses, tracking numbers, and raw payloads are never loaded.
Rows are stored at source-line grain in Fact_Historical_Order_Lines because the
anonymized order IDs are not unique enough for the operational Fact_Orders key.

Preview without database writes:
  python scripts/import_public_historical_orders.py --shopee-csv <path> --tiktok-csv <path>

Load after applying warehouse DDL:
  python scripts/import_public_historical_orders.py --shopee-csv <path> --tiktok-csv <path> --load-db
"""

from __future__ import annotations

import argparse
import hashlib
import logging
import os
import sys
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

BUSINESS_TZ = os.getenv("BUSINESS_TZ", "Asia/Ho_Chi_Minh")

logger = logging.getLogger("import-public-history")
DATA_ORIGIN = "public_anonymized_historical_csv"
BATCH_SIZE = 1000

SHOPEE_FIELDS = {
    "order_sn", "item_sku", "item_name", "create_time", "quantity", "order_status",
}
TIKTOK_FIELDS = {
    "order_id", "seller_sku", "product_name", "created_time", "quantity",
    "order_status", "item_status",
}

INSERT_SQL = """
    INSERT INTO Fact_Historical_Order_Lines (
        source_dataset, source_record_id, data_origin, source_order_id, platform,
        sku, product_name, order_status, is_cancelled, quantity, create_time
    ) VALUES (
        :source_dataset, :source_record_id, :data_origin, :source_order_id, :platform,
        :sku, :product_name, :order_status, :is_cancelled, :quantity, :create_time
    )
    ON CONFLICT (source_dataset, platform, source_record_id) DO UPDATE SET
        data_origin = EXCLUDED.data_origin,
        source_order_id = EXCLUDED.source_order_id,
        sku = EXCLUDED.sku,
        product_name = EXCLUDED.product_name,
        order_status = EXCLUDED.order_status,
        is_cancelled = EXCLUDED.is_cancelled,
        quantity = EXCLUDED.quantity,
        create_time = EXCLUDED.create_time,
        loaded_at = NOW()
"""


def _file_fingerprint(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return f"sha256:{digest.hexdigest()}"


def _read_allowlisted(path: Path, allowed: set[str]) -> tuple[pd.DataFrame, set[str]]:
    header = pd.read_csv(path, nrows=0, encoding="utf-8-sig")
    present = set(header.columns)
    selected = present.intersection(allowed)
    df = pd.read_csv(
        path,
        usecols=lambda name: name in allowed,
        dtype="string",
        encoding="utf-8-sig",
        low_memory=False,
        keep_default_na=False,
    )
    return df, selected


def _normalize_file(path: Path, platform: str) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    if not path.is_file():
        raise FileNotFoundError(f"Không tìm thấy CSV {platform}: {path}")

    allowed = SHOPEE_FIELDS if platform == "shopee" else TIKTOK_FIELDS
    df, columns = _read_allowlisted(path, allowed)
    if platform == "shopee":
        required = {"order_sn", "item_sku", "create_time", "quantity", "order_status"}
        order_col, sku_col, name_col, time_col = "order_sn", "item_sku", "item_name", "create_time"
        status_col = "order_status"
    else:
        required = {"order_id", "seller_sku", "created_time", "quantity", "order_status"}
        order_col, sku_col, name_col, time_col = "order_id", "seller_sku", "product_name", "created_time"
        status_col = "item_status" if "item_status" in columns else "order_status"
    missing = sorted(required - columns)
    if missing:
        raise ValueError(f"CSV {platform} thiếu các cột bắt buộc: {missing}")

    timestamps = pd.to_datetime(df[time_col].str.strip(), errors="coerce", utc=True)
    local_times = timestamps.dt.tz_convert(ZoneInfo(BUSINESS_TZ)).dt.tz_localize(None)
    quantities = pd.to_numeric(df["quantity"].str.strip(), errors="coerce")
    skus = df[sku_col].str.strip()
    order_ids = df[order_col].str.strip()
    statuses = df[status_col].str.strip().str.upper()

    valid = (
        timestamps.notna()
        & skus.notna()
        & skus.ne("")
        & quantities.notna()
        & quantities.gt(0)
        & quantities.le(100)
        & quantities.mod(1).eq(0)
    )
    source_dataset = f"{platform}:{_file_fingerprint(path)}"
    normalized: list[dict[str, Any]] = []
    for row_index in df.index[valid]:
        status = statuses.loc[row_index] or "UNKNOWN"
        order_id = order_ids.loc[row_index] or None
        sku = str(skus.loc[row_index])
        product_name = ""
        if name_col in df.columns:
            product_name = str(df.at[row_index, name_col]).strip()
        if not product_name:
            product_name = sku
        normalized.append({
            "source_dataset": source_dataset,
            # Row position remains stable for an unchanged source file. It also
            # separates conflicting/reused anonymized pkId and order IDs.
            "source_record_id": f"row:{int(row_index)}",
            "data_origin": DATA_ORIGIN,
            "source_order_id": order_id,
            "platform": platform,
            "sku": sku,
            "product_name": product_name[:500],
            "order_status": status[:40],
            "is_cancelled": status in {"CANCELLED", "IN_CANCEL", "CANCEL_REQUESTED"},
            "quantity": int(quantities.loc[row_index]),
            "create_time": local_times.loc[row_index].to_pydatetime(),
        })

    active = [row for row in normalized if not row["is_cancelled"]]
    active_dates = [row["create_time"].date() for row in active]
    order_groups = pd.DataFrame({
        "order_id": [row["source_order_id"] for row in active],
        "date": active_dates,
    }).dropna(subset=["order_id"])
    reused_order_ids = 0
    if not order_groups.empty:
        reused_order_ids = int((order_groups.groupby("order_id")["date"].nunique() > 1).sum())

    summary = {
        "platform": platform,
        "source_dataset": source_dataset,
        "source_rows": len(df),
        "valid_rows": len(normalized),
        "rejected_rows": len(df) - len(normalized),
        "cancelled_rows": sum(row["is_cancelled"] for row in normalized),
        "active_skus": len({row["sku"] for row in active}),
        "active_days": len(set(active_dates)),
        "active_start": min(active_dates).isoformat() if active_dates else None,
        "active_end": max(active_dates).isoformat() if active_dates else None,
        "order_ids_reused_across_dates": reused_order_ids,
    }
    return normalized, summary


def import_orders(shopee_csv: Path, tiktok_csv: Path, load_db: bool = False) -> dict[str, Any]:
    shopee_rows, shopee_summary = _normalize_file(shopee_csv, "shopee")
    tiktok_rows, tiktok_summary = _normalize_file(tiktok_csv, "tiktok")
    rows = shopee_rows + tiktok_rows

    if load_db and rows:
        # Keep the read-only preview usable in lightweight environments where
        # database dependencies are intentionally not installed.
        from sqlalchemy import text
        from warehouse.etl.load import get_engine

        engine = get_engine()
        with engine.begin() as conn:
            for start in range(0, len(rows), BATCH_SIZE):
                conn.execute(text(INSERT_SQL), rows[start : start + BATCH_SIZE])

    result = {
        "data_origin": DATA_ORIGIN,
        "database_loaded": bool(load_db),
        "total_rows": len(rows),
        "shopee": shopee_summary,
        "tiktok": tiktok_summary,
    }
    return result


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    parser = argparse.ArgumentParser(description="Nạp lịch sử đơn hàng CSV đã khử định danh cho forecast.")
    parser.add_argument("--shopee-csv", type=Path, required=True)
    parser.add_argument("--tiktok-csv", type=Path, required=True)
    parser.add_argument(
        "--load-db", action="store_true",
        help="Upsert vào Fact_Historical_Order_Lines. Cần chạy scripts/init_warehouse.py trước.",
    )
    args = parser.parse_args()
    result = import_orders(args.shopee_csv, args.tiktok_csv, load_db=args.load_db)
    logger.info("Kết quả import (không có giá trị dòng hoặc thông tin cá nhân): %s", result)
    if not args.load_db:
        logger.info("Chế độ xem trước: chưa ghi dữ liệu vào PostgreSQL.")


if __name__ == "__main__":
    main()
