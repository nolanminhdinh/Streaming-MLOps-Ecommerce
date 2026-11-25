"""Cài đặt và làm mới data mart star schema (schema `mart`) cho báo cáo quản lý.

    python -m warehouse.reporting.mart            # cài đặt (không xóa dữ liệu) + làm mới gia tăng
    python -m warehouse.reporting.mart --full     # cài đặt + nạp lại toàn bộ fact (giữ lịch sử đề xuất)

Worker gọi refresh sau mỗi chu kỳ ETL; batch-forecaster gọi sau mỗi lượt dự báo. Hàm SQL mart.refresh()
tự khóa advisory nên hai nơi gọi không chạy chồng.
"""
import argparse
import json
from pathlib import Path
import sys

from sqlalchemy import text

ROOT = Path(__file__).resolve().parents[2]
DDL = ROOT / "warehouse/ddl/05_mart_star_schema.sql"
FUNCTION = Path(__file__).with_name("mart_refresh.sql")
REFRESH_LOCK = 2026111601   # cùng khóa mà mart.refresh() lấy


def install_mart(conn):
    """Tạo schema, bảng, dòng 'Không xác định' và hàm refresh; chạy lại an toàn.

    Lấy khóa làm mới TRƯỚC khi chạy DDL: DDL (kể cả CREATE INDEX IF NOT EXISTS) khóa bảng, nếu một
    lượt refresh khác đang giữ khóa advisory và chờ ghi bảng đó thì hai bên deadlock (gặp 16/11/2026).
    """
    conn.execute(text("SELECT pg_advisory_xact_lock(:key)"), {"key": REFRESH_LOCK})
    conn.exec_driver_sql(DDL.read_text(encoding="utf-8"))
    conn.exec_driver_sql(FUNCTION.read_text(encoding="utf-8"))


def refresh_mart(conn, full: bool = False) -> dict:
    stats = conn.execute(text("SELECT mart.refresh(:full)"), {"full": full}).scalar_one()
    return stats if isinstance(stats, dict) else json.loads(stats)


def main():
    sys.path.insert(0, str(ROOT))
    from warehouse.etl.load import get_engine
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--full", action="store_true", help="Nạp lại toàn bộ fact (giữ fact_replenishment_daily)")
    args = parser.parse_args()
    engine = get_engine()
    try:
        with engine.begin() as conn:
            install_mart(conn)
        with engine.begin() as conn:
            print(json.dumps(refresh_mart(conn, args.full), ensure_ascii=False))
    finally:
        engine.dispose()


if __name__ == "__main__":
    main()
