"""
transform.py
-------------
Tầng Transform: làm sạch dữ liệu thô từ MinIO, chuẩn hóa khóa dimension,
và chuẩn bị DataFrame sẵn sàng để load vào Star Schema.

Các bước xử lý:
  1. Chuẩn hóa cột chung (unify Shopee + TikTok vào 1 schema).
  2. Xử lý missing values và outliers.
  3. Chuẩn hóa khóa dimension (SKU, shop, địa lý, thanh toán, ĐVVC).
  4. Tính toán measures chuẩn (subtotal, buyer_total_amount, ...).
  5. Kiểm tra data quality.

Tuần 3: Tầng Transform của pipeline ETL.
"""

import logging
import os
from numbers import Real
from datetime import datetime

import numpy as np
import pandas as pd

logger = logging.getLogger("etl.transform")

# Múi giờ nghiệp vụ: ngày bán hàng, ngày Mega-sale, Dim_Dates đều tính theo giờ Việt Nam.
# Timestamp gốc (ISO có offset, hoặc naive được coi là UTC) được quy đổi về múi giờ này
# rồi bỏ tz trước khi nạp vào cột TIMESTAMP của PostgreSQL.
BUSINESS_TZ = os.getenv("BUSINESS_TZ", "Asia/Ho_Chi_Minh")


def source_line_key(row):
    """Prefer an upstream order-line ID; legacy feeds have one line per SKU."""
    for field in ("order_line_id", "line_id", "source_line_id"):
        value = row.get(field)
        if value is not None and pd.notna(value) and str(value).strip():
            if isinstance(value, Real) and float(value).is_integer():
                value = str(int(value))
            return str(value) if field == "source_line_id" else "id:" + str(value)
    sku = row.get("sku", row.get("item_sku") if row.get("_platform") == "shopee" else row.get("seller_sku"))
    return "sku:" + str(sku) if sku is not None and pd.notna(sku) and str(sku).strip() else None


def deduplicate_lines(df):
    if "source_updated_at" in df:
        df = df.sort_values("source_updated_at", kind="stable", na_position="first")
    keys = ["order_id", "source_line_id"]
    if "platform" in df:
        keys.append("platform")
    # Invalid identities must reach validation individually, not disappear as duplicates.
    identified = df[keys].notna().all(axis=1)
    duplicate = identified & df.duplicated(subset=keys, keep="last")
    return df.loc[~duplicate].copy(), int(duplicate.sum())


# ─────────────────────────────────────────────────────────────
# 1. UNIFY SCHEMA — Shopee + TikTok → Common Schema
# ─────────────────────────────────────────────────────────────

def unify_schema(df: pd.DataFrame) -> pd.DataFrame:
    """
    Chuẩn hóa cột từ Shopee và TikTok thành schema chung cho Fact_Orders.

    Mapping:
      - order_id: order_sn (Shopee) | order_id (TikTok)
      - sku: item_sku (Shopee) | seller_sku (TikTok)
      - product_name: item_name (Shopee) | product_name (TikTok)
      - create_time: create_time (Shopee) | created_time (TikTok)
      - ...
    """
    if df.empty:
        return df

    # Xác định platform
    if "_platform" not in df.columns:
        # Heuristic: nếu có order_sn → Shopee, có order_id → TikTok
        if "order_sn" in df.columns:
            df["_platform"] = "shopee"
        elif "order_id" in df.columns and "order_sn" not in df.columns:
            df["_platform"] = "tiktok"
        else:
            df["_platform"] = "unknown"

    unified_rows = []

    # Dùng to_dict('records') thay vì df.iterrows() để tăng tốc gấp hàng trăm lần
    records = df.to_dict(orient="records")
    for row in records:
        platform = row.get("_platform", "unknown")

        if platform == "shopee":
            unified = _unify_shopee_row(row)
        elif platform == "tiktok":
            unified = _unify_tiktok_row(row)
        else:
            unified = {"platform": platform, "order_id": row.get("order_id") or row.get("order_sn"),
                       "sku": row.get("sku"), "create_time": row.get("create_time"),
                       "quantity": row.get("quantity"), "order_status": row.get("order_status")}
        unified.update(source_line_id=source_line_key(row),
                       source_updated_at=next((row.get(key) for key in ("update_time", "updated_time", "updatedAt")
                                               if pd.notna(row.get(key)) and str(row.get(key)).strip()), None),
                       source_object=row.get("_source_object"), source_row=row.get("_source_row"),
                       raw_payload=row)
        unified_rows.append(unified)

    if not unified_rows:
        return pd.DataFrame()

    return pd.DataFrame(unified_rows)


def _unify_shopee_row(row: dict | pd.Series) -> dict:
    """Chuyển 1 row Shopee → schema chung."""
    return {
        "order_id": row.get("order_sn"),
        "platform": "shopee",
        "sku": row.get("item_sku"),
        "product_name": row.get("item_name"),
        "category": row.get("model_name"),  # model_name lưu category trong simulator
        "shop_id": str(row.get("shop_id", "")),
        "shop_name": row.get("shop_name"),
        "order_status": row.get("order_status"),
        "is_cancelled": row.get("order_status") == "CANCELLED",
        "cancel_reason": row.get("cancel_reason"),
        "quantity": row.get("quantity", 0),
        "original_price": row.get("original_price", 0),
        "discounted_price": row.get("model_discounted_price"),
        "subtotal": row.get("total_order_value", 0),
        "buyer_total_amount": row.get("buyer_total_amount", 0),
        "seller_discount": row.get("seller_discount", 0),
        "platform_discount": row.get("shopee_discount", 0),
        "voucher_total": (
            _optional_amount(row.get("voucher_from_seller")) +
            _optional_amount(row.get("voucher_from_shopee"))
        ),
        "commission_fee": row.get("commission_fee", 0),
        "service_fee": row.get("service_fee", 0),
        "transaction_fee": row.get("transaction_fee", 0),
        "shipping_fee": row.get("actual_shipping_fee", 0),
        "original_shipping": row.get("estimated_shipping_fee", 0),
        "tax_amount": 0,  # Shopee không tách thuế riêng
        "payment_method": row.get("payment_method"),
        "carrier_name": row.get("shipping_carrier"),
        "shipping_carrier": row.get("shipping_carrier"),
        "state": row.get("state"),
        "city": row.get("city"),
        "district": row.get("district"),
        "country": row.get("country", "VN"),
        "weight_kg": row.get("item_weight"),
        "create_time": row.get("create_time"),
        "pay_time": row.get("pay_time"),
        "shipped_time": row.get("shipped_time"),
        "completed_time": row.get("completed_time"),
        "cancel_time": row.get("cancel_time"),
    }


def _optional_amount(value):
    """Missing fees default to zero; malformed supplied fees reach quarantine."""
    return 0 if value is None or pd.isna(value) else pd.to_numeric(value, errors="coerce")


def _unify_tiktok_row(row: dict | pd.Series) -> dict:
    """Chuyển 1 row TikTok → schema chung."""
    return {
        "order_id": row.get("order_id"),
        "platform": "tiktok",
        "sku": row.get("seller_sku"),
        "product_name": row.get("product_name"),
        "category": None,  # TikTok không có category trong simulator hiện tại
        # Simulator/source TikTok hiện cung cấp shop_name thay vì shop_id.
        # Dùng tên shop làm business key ổn định để khớp seed_dim_tables và load.py.
        "shop_id": row.get("shop_id") or row.get("shop_name") or "",
        "shop_name": row.get("shop_name"),
        "order_status": row.get("order_status"),
        "is_cancelled": row.get("order_status") == "CANCELLED",
        "cancel_reason": row.get("item_cancel_reason"),
        "quantity": row.get("quantity", 0),
        "original_price": row.get("item_original_price", 0),
        "discounted_price": row.get("item_sale_price"),
        "subtotal": row.get("sub_total", 0),
        "buyer_total_amount": row.get("total_amount", 0),
        "seller_discount": row.get("seller_discount", 0),
        "platform_discount": row.get("platform_discount", 0),
        "voucher_total": 0,  # TikTok tách discount khác, không dùng voucher
        "commission_fee": 0,
        "service_fee": 0,
        "transaction_fee": 0,
        "shipping_fee": row.get("shipping_fee", 0),
        "original_shipping": row.get("original_shipping_fee", 0),
        "tax_amount": row.get("tax_amount", 0),
        "payment_method": row.get("payment_method"),
        "carrier_name": row.get("shipping_provider"),
        "shipping_carrier": row.get("shipping_provider"),
        "state": row.get("region_state"),
        "city": row.get("city_town"),
        "district": row.get("district"),
        "country": "VN",
        "weight_kg": None,  # TikTok không track weight trong simulator
        "create_time": row.get("created_time"),
        "pay_time": row.get("paid_time"),
        "shipped_time": row.get("shipped_time"),
        "completed_time": row.get("completed_time"),
        "cancel_time": row.get("cancelled_time"),
    }


# ─────────────────────────────────────────────────────────────
# 2. DATA CLEANING
# ─────────────────────────────────────────────────────────────

def clean_data(df: pd.DataFrame, return_report: bool = False, preserve_invalid: bool = False):
    """
    Làm sạch dữ liệu: xử lý missing values, outliers, duplicates.

    Args:
        df: DataFrame đầu vào.
        return_report: Nếu True trả về (cleaned_df, report). Mặc định False trả về cleaned_df.
        preserve_invalid: Normalize only; production ETL validates every row before deduplication.

    Returns:
        pd.DataFrame (hoặc tuple[pd.DataFrame, dict] nếu return_report=True).
    """
    if df.empty:
        return (df, {}) if return_report else df

    original_count = len(df)
    df = df.copy()
    if "source_line_id" not in df:
        df["source_line_id"] = [source_line_key(row) for row in df.to_dict("records")]
    report = {"original_rows": original_count, "duplicates_removed": 0,
              "missing_key_removed": 0, "quantity_outliers_removed": 0, "price_outliers_removed": 0}

    # Line identity belongs to an order and platform. Invalid rows must be validated first in ETL.
    if not preserve_invalid:
        df, report["duplicates_removed"] = deduplicate_lines(df)

    # 2b. Loại bỏ rows thiếu order_id hoặc sku
    if not preserve_invalid:
        df = df.dropna(subset=["order_id", "sku"])
        report["missing_key_removed"] = original_count - report["duplicates_removed"] - len(df)

    # 2c. Xử lý missing values
    numeric_cols = [
        "quantity", "original_price", "subtotal", "buyer_total_amount",
        "seller_discount", "platform_discount", "voucher_total",
        "commission_fee", "service_fee", "transaction_fee",
        "shipping_fee", "original_shipping", "tax_amount",
    ]
    for col in numeric_cols:
        if col in df.columns:
            converted = pd.to_numeric(df[col], errors="coerce")
            # An absent optional fee defaults to zero; a supplied malformed value stays invalid.
            optional = col not in ("quantity", "original_price", "subtotal", "buyer_total_amount")
            df[col] = converted.mask(df[col].isna(), 0) if optional else converted
    if "discounted_price" in df:
        converted = pd.to_numeric(df["discounted_price"], errors="coerce")
        df["_invalid_discounted_price"] = df["discounted_price"].notna() & converted.isna()
        df["discounted_price"] = converted
    # A malformed component must not turn into an absent optional total (zero).
    if "raw_payload" in df:
        df["_invalid_voucher_total"] = df["raw_payload"].map(
            lambda row: any(pd.isna(_optional_amount(row.get(key)))
                            for key in ("voucher_from_seller", "voucher_from_shopee")))

    # 2d. Xử lý outliers (quantity và price)
    if "quantity" in df.columns and not preserve_invalid:
        # Giới hạn quantity hợp lý: 1-100
        before_outlier = len(df)
        df = df[(df["quantity"] >= 1) & (df["quantity"] <= 100)]
        report["quantity_outliers_removed"] = before_outlier - len(df)

    if "original_price" in df.columns and not preserve_invalid:
        # Giá phải >= 0
        before_price = len(df)
        df = df[df["original_price"] >= 0]
        report["price_outliers_removed"] = before_price - len(df)

    # 2e. Parse timestamps → quy đổi về giờ nghiệp vụ (naive local time)
    timestamp_cols = ["create_time", "pay_time", "shipped_time", "completed_time", "cancel_time", "source_updated_at"]
    for col in timestamp_cols:
        if col in df.columns:
            ts = pd.to_datetime(df[col], format="mixed", errors="coerce", utc=True)
            df["_invalid_" + col] = df[col].notna() & ts.isna()
            df[col] = ts.dt.tz_convert(BUSINESS_TZ).dt.tz_localize(None)
    lifecycle = [col for col in timestamp_cols[:-1] if col in df]
    if lifecycle:
        inferred = df[lifecycle].max(axis=1)
        if "source_updated_at" not in df:
            df["source_updated_at"] = inferred
        else:
            df["source_updated_at"] = df["source_updated_at"].fillna(inferred)

    # Ngày đặt hàng theo giờ nghiệp vụ → dùng để lookup Dim_Dates.date_key khi load.
    # (Trước đây chỉ transform() thêm cột này, còn pipeline.py gọi thẳng clean_data()
    #  nên Fact_Orders.date_key luôn NULL.)
    if "create_time" in df.columns:
        df["order_date"] = df["create_time"].dt.date

    # 2f. Chuẩn hóa text
    if "order_status" in df.columns:
        df["order_status"] = df["order_status"].astype("string").str.upper().str.strip()
        df["is_cancelled"] = df["order_status"].isin(["CANCELLED", "IN_CANCEL", "CANCEL_REQUESTED"])

    if "payment_method" in df.columns:
        df["payment_method"] = df["payment_method"].astype("string").str.strip()

    if "carrier_name" in df.columns:
        df["carrier_name"] = df["carrier_name"].astype("string").str.strip()

    report["clean_rows"] = len(df)
    report["clean_pct"] = round(len(df) / original_count * 100, 1) if original_count > 0 else 0

    logger.info(
        "Cleaning: %d → %d rows (%.1f%% retained, %d duplicates, %d missing key)",
        original_count, len(df), report["clean_pct"],
        report["duplicates_removed"], report.get("missing_key_removed", 0),
    )

    if return_report:
        df.attrs["quality_report"] = report.copy()
        return df, report
    df.attrs["quality_report"] = report.copy()
    return df


def generate_quality_report(raw_df: pd.DataFrame, clean_df: pd.DataFrame) -> dict:
    """Tạo báo cáo chất lượng đối chiếu trước vs sau clean theo chuẩn ĐATN."""
    orig_len = len(raw_df) if raw_df is not None else 0
    clean_len = len(clean_df) if clean_df is not None else 0
    dup_removed = clean_df.attrs.get("quality_report", {}).get("duplicates_removed", 0) if clean_df is not None else 0
    clean_pct = round(clean_len / orig_len * 100, 1) if orig_len > 0 else 0.0
    return {
        "original_rows": orig_len,
        "clean_rows": clean_len,
        "duplicates_removed": dup_removed,
        "removed_total": max(0, orig_len - clean_len),
        "clean_pct": clean_pct,
    }


# ─────────────────────────────────────────────────────────────
# 3. MAIN TRANSFORM
# ─────────────────────────────────────────────────────────────

def transform(raw_df: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    """
    Pipeline transform chính: unify schema → clean data.

    Args:
        raw_df: DataFrame thô từ extract.

    Returns:
        (transformed_df, quality_report)
    """
    logger.info("Bắt đầu Transform: %d rows", len(raw_df))

    # Step 1: Unify schema
    unified_df = unify_schema(raw_df)
    logger.info("Unify schema: %d rows", len(unified_df))

    # Step 2: Clean data
    clean_df, report = clean_data(unified_df, return_report=True)

    logger.info("Transform hoàn tất: %d rows sẵn sàng để load", len(clean_df))
    return clean_df, report


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    # Test với dữ liệu mẫu
    from extract import extract
    from datetime import timezone

    raw = extract(target_date=datetime.now(timezone.utc))
    if not raw.empty:
        clean, report = transform(raw)
        print("Quality report:", report)
        print(clean.head())
    else:
        print("Không có dữ liệu để transform")
