"""
simulate_historical_data.py
---------------------------
Tạo tập dữ liệu lịch sử đơn hàng mô phỏng (Historical Dataset, ví dụ 60-90 ngày)
để phục vụ phân tích EDA, phân loại ma trận ABC/XYZ và huấn luyện mô hình Machine Learning.

Tích hợp đầy đủ các đặc tính thực tế:
  - Nhu cầu Gamma–Poisson theo SKU, mùa vụ, xu hướng và cú sốc nhiều ngày.
  - Shopee/TikTok có tỷ trọng theo nhóm hàng; sale có hiệu ứng trước và sau.
  - Tỷ lệ hủy đơn, voucher, phí sàn và vòng đời đơn hàng.
  - Lưu file ra data/historical_orders.parquet và hỗ trợ nạp thẳng vào PostgreSQL nếu cần.

Sử dụng:
  python scripts/simulate_historical_data.py --days 60 --orders-per-day 150
  python scripts/simulate_historical_data.py --days 30 --load-db
"""

import argparse
import logging
import os
import sys
from datetime import date, datetime, time, timedelta, timezone

import pandas as pd

# Cho phép import từ thư mục gốc
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from data_simulator.history import iter_historical_days, local_time
from warehouse.etl.transform import unify_schema, clean_data
from warehouse.etl.load import load

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("simulate-history")


def generate_historical_orders(
    days: int = 60,
    base_orders_per_day: int = 200,
    end_date: datetime = None,
    start_date: datetime = None,
    seed: int = 42,
    order_prefix: str = "",
) -> pd.DataFrame:
    """Sample SKU demand, then generate events in timestamp order (business time)."""
    if days < 0:
        raise ValueError("days must be nonnegative")
    end = local_time(end_date or datetime.now(timezone.utc))
    start = local_time(start_date) if start_date is not None else (end - timedelta(days=days)).replace(hour=0, minute=0, second=0, microsecond=0)
    events = []
    for day, rows, _, _ in iter_historical_days(start, end, base_orders_per_day, seed, order_prefix):
        events.extend(rows)
        logger.info("%s: generated %d orders", day, len(rows))
    return pd.DataFrame(events)


def save_and_load(raw_df: pd.DataFrame, load_db: bool = False):
    """Lưu file Parquet và tùy chọn nạp vào PostgreSQL."""
    data_dir = os.path.join(os.path.dirname(__file__), "..", "data")
    os.makedirs(data_dir, exist_ok=True)

    parquet_path = os.path.join(data_dir, "historical_orders_raw.parquet")
    raw_df.to_parquet(parquet_path, index=False)
    logger.info("✓ Đã lưu dữ liệu thô vào: %s (%d dòng)", parquet_path, len(raw_df))

    # Chuẩn hóa sang Canonical Schema
    unified_df = unify_schema(raw_df)
    clean_df = clean_data(unified_df)

    clean_parquet_path = os.path.join(data_dir, "historical_orders_clean.parquet")
    clean_df.to_parquet(clean_parquet_path, index=False)
    logger.info("✓ Đã lưu dữ liệu sạch vào: %s (%d dòng)", clean_parquet_path, len(clean_df))

    if load_db:
        logger.info("Đang nạp dữ liệu sạch vào PostgreSQL Fact_Orders...")
        res = load(clean_df)
        count = res.get("rows_loaded", 0)
        logger.info("✓ Nạp thành công %d Fact_Orders vào PostgreSQL!", count)


def main():
    parser = argparse.ArgumentParser(description="Sinh dữ liệu đơn hàng lịch sử")
    parser.add_argument("--days", type=int, default=60, help="Số ngày lịch sử (mặc định 60)")
    parser.add_argument("--orders-per-day", type=int, default=150, help="Số đơn cơ bản/ngày (mặc định 150)")
    parser.add_argument("--load-db", action="store_true", help="Nạp trực tiếp vào PostgreSQL Star Schema")
    parser.add_argument('--start-date', type=date.fromisoformat, help='Inclusive start, YYYY-MM-DD in business time')
    parser.add_argument('--end-date', type=date.fromisoformat, help='Inclusive final day, YYYY-MM-DD in business time')
    parser.add_argument('--seed', type=int, default=42)
    parser.add_argument('--order-prefix', default='')
    args = parser.parse_args()

    raw_df = generate_historical_orders(
        days=args.days,
        base_orders_per_day=args.orders_per_day,
        start_date=datetime.combine(args.start_date, time.min) if args.start_date else None,
        end_date=datetime.combine(args.end_date, time.max) if args.end_date else None,
        seed=args.seed,
        order_prefix=args.order_prefix,
    )
    save_and_load(raw_df, load_db=args.load_db)


if __name__ == "__main__":
    main()
