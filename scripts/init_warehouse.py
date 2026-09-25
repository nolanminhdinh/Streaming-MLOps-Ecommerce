"""
init_warehouse.py
-----------------
Khởi tạo cấu trúc kho dữ liệu Star Schema trên PostgreSQL:
  1. Kiểm tra kết nối tới PostgreSQL (với cơ chế retry).
  2. Đọc và thực thi file DDL `warehouse/ddl/01_star_schema.sql`.
  3. Kiểm tra sự tồn tại của các bảng Fact và Dimension.
  4. Xác nhận dữ liệu seed ban đầu (Dim_Dates, Dim_Payment).

Sử dụng:
  python scripts/init_warehouse.py
  python scripts/init_warehouse.py --drop-first   # Xóa sạch và tạo lại (CẨN THẬN)
"""

import argparse
import logging
import os
import sys
import time

from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError

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
logger = logging.getLogger("init-warehouse")

TABLES_TO_VERIFY = [
    "dim_products",
    "dim_shops",
    "dim_geography",
    "dim_dates",
    "dim_payment",
    "dim_carriers",
    "fact_orders",
    "fact_inventory_daily",
]


def test_connection(engine, max_retries: int = 5, delay: float = 3.0):
    """Kiểm tra kết nối PostgreSQL với retry."""
    for attempt in range(1, max_retries + 1):
        try:
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            logger.info("Kết nối PostgreSQL thành công: %s:%s/%s", PG_HOST, PG_PORT, PG_DB)
            return True
        except OperationalError as e:
            logger.warning(
                "Chưa kết nối được PostgreSQL (lần %d/%d): %s — thử lại sau %.0fs...",
                attempt, max_retries, e, delay,
            )
            time.sleep(delay)
    raise ConnectionError(f"Không thể kết nối PostgreSQL sau {max_retries} lần thử.")


def init_warehouse(drop_first: bool = False):
    ddl_path = os.path.join(
        os.path.dirname(__file__), "..", "warehouse", "ddl", "01_star_schema.sql"
    )
    if not os.path.exists(ddl_path):
        raise FileNotFoundError(f"Không tìm thấy file DDL tại: {ddl_path}")

    logger.info("Đọc file DDL: %s", ddl_path)
    with open(ddl_path, "r", encoding="utf-8") as f:
        ddl_sql = f.read()

    engine = create_engine(DATABASE_URL)
    test_connection(engine)

    with engine.begin() as conn:
        if drop_first:
            logger.warning("CẢNH BÁO: Đang xóa các bảng hiện có (--drop-first)...")
            conn.execute(text("""
                DROP TABLE IF EXISTS Fact_Orders CASCADE;
                DROP TABLE IF EXISTS Fact_Inventory_Daily CASCADE;
                DROP TABLE IF EXISTS Dim_Products CASCADE;
                DROP TABLE IF EXISTS Dim_Shops CASCADE;
                DROP TABLE IF EXISTS Dim_Geography CASCADE;
                DROP TABLE IF EXISTS Dim_Dates CASCADE;
                DROP TABLE IF EXISTS Dim_Payment CASCADE;
                DROP TABLE IF EXISTS Dim_Carriers CASCADE;
            """))
            logger.info("Đã xóa các bảng cũ thành công.")

        logger.info("Thực thi script DDL 01_star_schema.sql...")
        # PostgreSQL thực thi toàn bộ script trong 1 khối
        conn.execute(text(ddl_sql))

    # Kiểm tra lại các bảng
    logger.info("Kiểm tra sự tồn tại và số lượng bản ghi của các bảng:")
    with engine.connect() as conn:
        for tbl in TABLES_TO_VERIFY:
            try:
                res = conn.execute(text(f"SELECT COUNT(*) FROM {tbl}"))
                cnt = res.scalar()
                logger.info("  ✓ Bảng %-22s: %d bản ghi", tbl, cnt)
            except Exception as e:
                logger.error("  ✗ Lỗi kiểm tra bảng %s: %s", tbl, e)

    logger.info("Khởi tạo kho dữ liệu hoàn tất thành công!")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Khởi tạo kho dữ liệu Star Schema")
    parser.add_argument(
        "--drop-first",
        action="store_true",
        help="Xóa sạch các bảng cũ trước khi tạo lại (chỉ dùng khi dev/test)",
    )
    args = parser.parse_args()
    init_warehouse(drop_first=args.drop_first)
