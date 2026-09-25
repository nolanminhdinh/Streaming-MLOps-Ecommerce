"""
load.py
-------
Tầng Load: nạp dữ liệu đã transform vào Star Schema trên PostgreSQL.

Quy trình:
  1. Upsert Dim_Products, Dim_Shops, Dim_Geography, Dim_Payment, Dim_Carriers.
  2. Lookup khóa dimension (product_key, shop_key, ...).
  3. Insert vào Fact_Orders.

Tuần 3: Tầng Load của pipeline ETL.
"""

import logging
import os
from datetime import datetime, timezone

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

# ─── Cấu hình ───
PG_USER = os.getenv("POSTGRES_USER", "ecom")
PG_PASS = os.getenv("POSTGRES_PASSWORD", "ecom_password")
PG_HOST = os.getenv("POSTGRES_HOST", "localhost")
PG_PORT = os.getenv("POSTGRES_PORT", "5432")
PG_DB = os.getenv("POSTGRES_DB", "ecom_warehouse")

DATABASE_URL = f"postgresql://{PG_USER}:{PG_PASS}@{PG_HOST}:{PG_PORT}/{PG_DB}"

logger = logging.getLogger("etl.load")


def get_engine():
    return create_engine(DATABASE_URL)


# ─────────────────────────────────────────────────────────────
# 1. UPSERT DIMENSIONS
# ─────────────────────────────────────────────────────────────

def upsert_dim_products(engine, df: pd.DataFrame) -> dict:
    """Upsert Dim_Products, trả về mapping sku → product_key."""
    skus = df[["sku", "product_name", "category", "original_price", "weight_kg"]].drop_duplicates(subset=["sku"])

    with engine.begin() as conn:
        for _, row in skus.iterrows():
            conn.execute(text("""
                INSERT INTO Dim_Products (sku, product_name, category, unit_cost, weight_kg)
                VALUES (:sku, :name, :cat, :cost, :weight)
                ON CONFLICT (sku) DO UPDATE SET
                    product_name = EXCLUDED.product_name,
                    unit_cost = EXCLUDED.unit_cost,
                    weight_kg = EXCLUDED.weight_kg,
                    updated_at = NOW()
            """), {
                "sku": row["sku"],
                "name": row.get("product_name"),
                "cat": row.get("category"),
                "cost": row.get("original_price"),
                "weight": row.get("weight_kg"),
            })

        # Fetch mapping
        result = conn.execute(text("SELECT sku, product_key FROM Dim_Products"))
        mapping = {r[0]: r[1] for r in result}

    logger.info("Dim_Products: %d SKUs upserted", len(skus))
    return mapping


def upsert_dim_shops(engine, df: pd.DataFrame) -> dict:
    """Upsert Dim_Shops, trả về mapping (shop_id, platform) → shop_key."""
    shops = df[["shop_id", "shop_name", "platform"]].drop_duplicates(subset=["shop_id", "platform"])

    with engine.begin() as conn:
        for _, row in shops.iterrows():
            conn.execute(text("""
                INSERT INTO Dim_Shops (shop_id, shop_name, platform)
                VALUES (:sid, :name, :platform)
                ON CONFLICT (shop_id, platform) DO UPDATE SET
                    shop_name = EXCLUDED.shop_name
            """), {
                "sid": row["shop_id"] or "",
                "name": row["shop_name"],
                "platform": row["platform"],
            })

        result = conn.execute(text("SELECT shop_id, platform, shop_key FROM Dim_Shops"))
        mapping = {(r[0], r[1]): r[2] for r in result}

    logger.info("Dim_Shops: %d shops upserted", len(shops))
    return mapping


def upsert_dim_geography(engine, df: pd.DataFrame) -> dict:
    """Upsert Dim_Geography, trả về mapping (state, city, district) → geo_key."""
    geos = df[["state", "city", "district", "country"]].drop_duplicates(
        subset=["state", "city", "district"]
    )

    with engine.begin() as conn:
        for _, row in geos.iterrows():
            # Phân vùng theo state → region (Bắc/Trung/Nam)
            region = _classify_region(row.get("state", ""))
            conn.execute(text("""
                INSERT INTO Dim_Geography (state, city, district, country, region)
                VALUES (:state, :city, :district, :country, :region)
                ON CONFLICT (state, city, district) DO NOTHING
            """), {
                "state": row.get("state"),
                "city": row.get("city"),
                "district": row.get("district"),
                "country": row.get("country", "VN"),
                "region": region,
            })

        result = conn.execute(text("SELECT state, city, district, geo_key FROM Dim_Geography"))
        mapping = {(r[0], r[1], r[2]): r[3] for r in result}

    logger.info("Dim_Geography: %d locations upserted", len(geos))
    return mapping


def _classify_region(state: str) -> str:
    """Phân vùng tỉnh/TP thành Bắc/Trung/Nam."""
    south = {"TP.HCM", "Bình Dương", "Đồng Nai", "Long An", "Cần Thơ",
             "Bà Rịa - Vũng Tàu", "Tây Ninh", "Bến Tre", "Tiền Giang",
             "An Giang", "Kiên Giang", "Vĩnh Long", "Trà Vinh",
             "Sóc Trăng", "Bạc Liêu", "Cà Mau", "Hậu Giang", "Đồng Tháp"}
    central = {"Đà Nẵng", "Khánh Hoà", "Thừa Thiên Huế", "Quảng Nam",
               "Quảng Ngãi", "Bình Định", "Phú Yên", "Ninh Thuận",
               "Bình Thuận", "Gia Lai", "Đắk Lắk", "Đắk Nông",
               "Kon Tum", "Lâm Đồng", "Nghệ An", "Hà Tĩnh",
               "Quảng Bình", "Quảng Trị", "Thanh Hoá"}
    if state in south:
        return "Nam"
    elif state in central:
        return "Trung"
    else:
        return "Bắc"


def lookup_dim_dates(engine, dates: list) -> dict:
    """Lookup Dim_Dates, trả về mapping date → date_key."""
    with engine.begin() as conn:
        result = conn.execute(text("SELECT full_date, date_key FROM Dim_Dates"))
        mapping = {r[0]: r[1] for r in result}
    return mapping


def lookup_dim_payment(engine) -> dict:
    """Lookup Dim_Payment, trả về mapping payment_method → payment_key."""
    with engine.begin() as conn:
        result = conn.execute(text("SELECT payment_method, payment_key FROM Dim_Payment"))
        mapping = {r[0]: r[1] for r in result}
    return mapping


def upsert_dim_carriers(engine, df: pd.DataFrame) -> dict:
    """Upsert Dim_Carriers, trả về mapping carrier_name → carrier_key."""
    carriers = df["carrier_name"].dropna().unique()

    with engine.begin() as conn:
        for name in carriers:
            conn.execute(text("""
                INSERT INTO Dim_Carriers (carrier_name)
                VALUES (:name)
                ON CONFLICT (carrier_name) DO NOTHING
            """), {"name": name})

        result = conn.execute(text("SELECT carrier_name, carrier_key FROM Dim_Carriers"))
        mapping = {r[0]: r[1] for r in result}

    logger.info("Dim_Carriers: %d carriers upserted", len(carriers))
    return mapping


# ─────────────────────────────────────────────────────────────
# 2. LOAD FACT TABLE
# ─────────────────────────────────────────────────────────────

def load_fact_orders(
    engine,
    df: pd.DataFrame,
    product_map: dict,
    shop_map: dict,
    geo_map: dict,
    date_map: dict,
    payment_map: dict,
    carrier_map: dict,
) -> int:
    """
    Load dữ liệu vào Fact_Orders.

    Resolve foreign keys từ dimension mappings, insert batch.
    Returns: số rows đã insert.
    """
    rows_to_insert = []

    for _, row in df.iterrows():
        # Resolve dimension keys
        product_key = product_map.get(row.get("sku"))
        shop_key = shop_map.get((row.get("shop_id", ""), row.get("platform")))
        geo_key = geo_map.get((row.get("state"), row.get("city"), row.get("district")))

        # Date key
        order_date = row.get("order_date")
        date_key = date_map.get(order_date) if order_date else None

        payment_key = payment_map.get(row.get("payment_method"))
        carrier_key = carrier_map.get(row.get("carrier_name"))

        rows_to_insert.append({
            "product_key": product_key,
            "shop_key": shop_key,
            "geo_key": geo_key,
            "date_key": date_key,
            "payment_key": payment_key,
            "carrier_key": carrier_key,
            "order_id": row.get("order_id"),
            "platform": row.get("platform"),
            "order_status": row.get("order_status"),
            "is_cancelled": row.get("is_cancelled", False),
            "cancel_reason": row.get("cancel_reason"),
            "quantity": row.get("quantity", 0),
            "original_price": row.get("original_price", 0),
            "discounted_price": row.get("discounted_price"),
            "subtotal": row.get("subtotal", 0),
            "buyer_total_amount": row.get("buyer_total_amount", 0),
            "seller_discount": row.get("seller_discount", 0),
            "platform_discount": row.get("platform_discount", 0),
            "voucher_total": row.get("voucher_total", 0),
            "commission_fee": row.get("commission_fee", 0),
            "service_fee": row.get("service_fee", 0),
            "transaction_fee": row.get("transaction_fee", 0),
            "shipping_fee": row.get("shipping_fee", 0),
            "original_shipping": row.get("original_shipping", 0),
            "tax_amount": row.get("tax_amount", 0),
            "create_time": row.get("create_time"),
            "pay_time": row.get("pay_time"),
            "shipped_time": row.get("shipped_time"),
            "completed_time": row.get("completed_time"),
            "cancel_time": row.get("cancel_time"),
        })

    if not rows_to_insert:
        return 0

    with engine.begin() as conn:
        conn.execute(
            text("""
                INSERT INTO Fact_Orders (
                    product_key, shop_key, geo_key, date_key, payment_key, carrier_key,
                    order_id, platform, order_status, is_cancelled, cancel_reason,
                    quantity, original_price, discounted_price, subtotal, buyer_total_amount,
                    seller_discount, platform_discount, voucher_total,
                    commission_fee, service_fee, transaction_fee,
                    shipping_fee, original_shipping, tax_amount,
                    create_time, pay_time, shipped_time, completed_time, cancel_time
                ) VALUES (
                    :product_key, :shop_key, :geo_key, :date_key, :payment_key, :carrier_key,
                    :order_id, :platform, :order_status, :is_cancelled, :cancel_reason,
                    :quantity, :original_price, :discounted_price, :subtotal, :buyer_total_amount,
                    :seller_discount, :platform_discount, :voucher_total,
                    :commission_fee, :service_fee, :transaction_fee,
                    :shipping_fee, :original_shipping, :tax_amount,
                    :create_time, :pay_time, :shipped_time, :completed_time, :cancel_time
                )
                ON CONFLICT (order_id, platform) DO NOTHING
            """),
            rows_to_insert,
        )

    logger.info("Fact_Orders: %d rows inserted", len(rows_to_insert))
    return len(rows_to_insert)


# ─────────────────────────────────────────────────────────────
# 3. MAIN LOAD
# ─────────────────────────────────────────────────────────────

def load(df: pd.DataFrame) -> dict:
    """
    Pipeline load chính: upsert dimensions → load fact table.

    Args:
        df: DataFrame đã transform (output từ transform.py).

    Returns:
        dict chứa thống kê load.
    """
    if df.empty:
        logger.warning("DataFrame rỗng, bỏ qua load")
        return {"status": "skipped", "reason": "empty_dataframe"}

    engine = get_engine()
    logger.info("Kết nối PostgreSQL: %s", DATABASE_URL.replace(PG_PASS, "***"))

    # Upsert dimensions
    product_map = upsert_dim_products(engine, df)
    shop_map = upsert_dim_shops(engine, df)
    geo_map = upsert_dim_geography(engine, df)
    date_map = lookup_dim_dates(engine, df.get("order_date", pd.Series()).tolist())
    payment_map = lookup_dim_payment(engine)
    carrier_map = upsert_dim_carriers(engine, df)

    # Load fact
    n_loaded = load_fact_orders(
        engine, df,
        product_map, shop_map, geo_map,
        date_map, payment_map, carrier_map,
    )

    result = {
        "status": "success",
        "rows_loaded": n_loaded,
        "dim_products": len(product_map),
        "dim_shops": len(shop_map),
        "dim_geography": len(geo_map),
        "dim_carriers": len(carrier_map),
    }
    logger.info("Load hoàn tất: %s", result)
    return result


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    from extract import extract
    from transform import transform

    raw = extract(target_date=datetime.now(timezone.utc))
    if not raw.empty:
        clean, report = transform(raw)
        load_result = load(clean)
        print("Load result:", load_result)
    else:
        print("Không có dữ liệu")
