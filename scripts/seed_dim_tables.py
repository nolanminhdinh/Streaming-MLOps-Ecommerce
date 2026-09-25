"""
seed_dim_tables.py
------------------
Seed dữ liệu ban đầu cho các bảng Dimension trong Star Schema:
  - Dim_Products: nạp toàn bộ danh mục sản phẩm từ E-commerce catalog.
  - Dim_Shops: các shop/gian hàng bán chạy trên Shopee và TikTok Shop.
  - Dim_Carriers: các đơn vị vận chuyển phổ biến tại Việt Nam.
  - Dim_Geography: danh mục các tỉnh/thành phố và quận/huyện trọng điểm (3 miền).

Sử dụng:
  python scripts/seed_dim_tables.py
"""

import logging
import os
import sys

from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# Import catalog từ data_simulator
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from data_simulator.data_simulator import (
    PRODUCT_CATALOG,
    SHOPEE_SHOPS,
    TIKTOK_SHOPS,
    SHOPEE_CARRIERS,
    TIKTOK_CARRIERS,
    VN_ADDRESSES,
)

load_dotenv()

PG_USER = os.getenv("POSTGRES_USER", "ecom")
PG_PASS = os.getenv("POSTGRES_PASSWORD", "ecom_password")
PG_HOST = os.getenv("POSTGRES_HOST", "localhost")
PG_PORT = os.getenv("POSTGRES_PORT", "5432")
PG_DB = os.getenv("POSTGRES_DB", "ecom_warehouse")

DATABASE_URL = f"postgresql://{PG_USER}:{PG_PASS}@{PG_HOST}:{PG_PORT}/{PG_DB}"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("seed-dimensions")


def seed_products(conn):
    logger.info("Seeding Dim_Products (%d SKUs)...", len(PRODUCT_CATALOG))
    for p in PRODUCT_CATALOG:
        conn.execute(text("""
            INSERT INTO Dim_Products (sku, product_name, category, unit_cost, weight_kg)
            VALUES (:sku, :name, :cat, :cost, :weight)
            ON CONFLICT (sku) DO UPDATE SET
                product_name = EXCLUDED.product_name,
                category = EXCLUDED.category,
                unit_cost = EXCLUDED.unit_cost,
                weight_kg = EXCLUDED.weight_kg
        """), {
            "sku": p["sku"],
            "name": p["name"],
            "cat": p.get("category", "Chung"),
            "cost": p["price"],
            "weight": p.get("weight", 0.1),
        })
    logger.info("✓ Dim_Products seeded thành công.")


def seed_shops(conn):
    all_shops = []
    for s in SHOPEE_SHOPS:
        all_shops.append({
            "shop_id": s["shop_id"],
            "shop_name": s["shop_name"],
            "platform": "shopee",
            "connection_id": s["connection_id"],
        })
    for s in TIKTOK_SHOPS:
        all_shops.append({
            "shop_id": s["shop_name"],  # TikTok định danh qua shop_name
            "shop_name": s["shop_name"],
            "platform": "tiktok",
            "connection_id": None,
        })

    logger.info("Seeding Dim_Shops (%d shops)...", len(all_shops))
    for s in all_shops:
        conn.execute(text("""
            INSERT INTO Dim_Shops (shop_id, shop_name, platform, connection_id)
            VALUES (:sid, :name, :platform, :conn)
            ON CONFLICT (shop_id, platform) DO UPDATE SET
                shop_name = EXCLUDED.shop_name,
                connection_id = EXCLUDED.connection_id
        """), {
            "sid": s["shop_id"],
            "name": s["shop_name"],
            "platform": s["platform"],
            "conn": s.get("connection_id"),
        })
    logger.info("✓ Dim_Shops seeded thành công.")


def seed_carriers(conn):
    carriers = set(SHOPEE_CARRIERS + TIKTOK_CARRIERS)
    logger.info("Seeding Dim_Carriers (%d đơn vị)...", len(carriers))
    for c in carriers:
        conn.execute(text("""
            INSERT INTO Dim_Carriers (carrier_name)
            VALUES (:name)
            ON CONFLICT (carrier_name) DO NOTHING
        """), {"name": c})
    logger.info("✓ Dim_Carriers seeded thành công.")


def seed_geography(conn):
    # Phân loại vùng miền Việt Nam
    bac = {"Hà Nội", "Hải Phòng", "Quảng Ninh", "Bắc Ninh", "Hải Dương", "Thái Nguyên"}
    trung = {"Đà Nẵng", "Thừa Thiên Huế", "Khánh Hòa", "Quảng Nam", "Nghệ An", "Lâm Đồng"}
    nam = {"Hồ Chí Minh", "Bình Dương", "Đồng Nai", "Cần Thơ", "Long An", "Tiền Giang", "Bà Rịa - Vũng Tàu"}

    logger.info("Seeding Dim_Geography (%d địa điểm)...", len(VN_ADDRESSES))
    for addr in VN_ADDRESSES:
        state = addr["state"]
        if state in bac:
            region = "Miền Bắc"
        elif state in trung:
            region = "Miền Trung"
        elif state in nam:
            region = "Miền Nam"
        else:
            region = "Khác"

        conn.execute(text("""
            INSERT INTO Dim_Geography (state, city, district, country, region)
            VALUES (:state, :city, :district, 'VN', :region)
            ON CONFLICT (state, city, district) DO UPDATE SET
                region = EXCLUDED.region
        """), {
            "state": state,
            "city": addr["city"],
            "district": addr["district"],
            "region": region,
        })
    logger.info("✓ Dim_Geography seeded thành công.")


def main():
    engine = create_engine(DATABASE_URL)
    with engine.begin() as conn:
        seed_products(conn)
        seed_shops(conn)
        seed_carriers(conn)
        seed_geography(conn)
    logger.info("Tất cả dữ liệu Dimension mẫu đã được nạp thành công!")


if __name__ == "__main__":
    main()
