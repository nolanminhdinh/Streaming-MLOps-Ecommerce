"""
simulate_historical_data.py
---------------------------
Tạo tập dữ liệu lịch sử đơn hàng mô phỏng (Historical Dataset, ví dụ 60-90 ngày)
để phục vụ phân tích EDA, phân loại ma trận ABC/XYZ và huấn luyện mô hình Machine Learning.

Tích hợp đầy đủ các đặc tính thực tế:
  - Phân bổ theo 2 sàn TMĐT Shopee (55%) và TikTok Shop (45%).
  - Mùa vụ theo giờ (Flash Sale 12h, 20h, 21h), theo thứ trong tuần, và các ngày đôi Mega-sale (8/8, 9/9).
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
from dataclasses import asdict
from datetime import datetime, timedelta, timezone

import pandas as pd

# Cho phép import từ thư mục gốc
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from data_simulator.data_simulator import ECommerceSimulator
from warehouse.etl.transform import unify_schema, clean_data
from warehouse.etl.load import get_engine, load

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
) -> pd.DataFrame:
    """Sinh tập dữ liệu đơn hàng lịch sử qua nhiều ngày."""
    if end_date is None:
        end_date = datetime.now(timezone.utc)

    start_date = end_date - timedelta(days=days)
    simulator = ECommerceSimulator(
        shopee_ratio=0.55,
        cancel_rate=0.12,
        start_date=start_date,
    )

    logger.info("═" * 60)
    logger.info("  BẮT ĐẦU SINH DỮ LIỆU LỊCH SỬ %d NGÀY", days)
    logger.info("  Khoảng thời gian: %s ➔ %s", start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d"))
    logger.info("  Số đơn cơ bản/ngày: %d", base_orders_per_day)
    logger.info("═" * 60)

    raw_events = []
    current_dt = start_date

    while current_dt <= end_date:
        # Số đơn thực tế của ngày (áp dụng hệ số mùa vụ ngày trong tuần và ngày đôi)
        dow_factor = {5: 1.3, 6: 1.2}.get(current_dt.weekday(), 1.0)
        is_mega = (current_dt.day == current_dt.month)
        mega_factor = 3.5 if is_mega else 1.0

        daily_orders = int(round(base_orders_per_day * dow_factor * mega_factor))

        for _ in range(daily_orders):
            # Phân bố giờ trong ngày theo thói quen mua sắm (đỉnh lúc 12h và 20h)
            hour_weights = [
                0.01, 0.01, 0.005, 0.005, 0.005, 0.01,  # 0h - 5h
                0.02, 0.04, 0.06, 0.07, 0.08, 0.09,     # 6h - 11h
                0.12, 0.06, 0.05, 0.05, 0.05, 0.05,     # 12h - 17h (12h Flash Sale)
                0.06, 0.07, 0.12, 0.11, 0.06, 0.03,     # 18h - 23h (20h-21h Flash Sale)
            ]
            import random
            selected_hour = random.choices(range(24), weights=hour_weights)[0]
            selected_minute = random.randint(0, 59)
            selected_second = random.randint(0, 59)

            event_time = current_dt.replace(
                hour=selected_hour,
                minute=selected_minute,
                second=selected_second,
            )

            # Chọn sàn
            if random.random() < simulator.shopee_ratio:
                ev = simulator._generate_shopee_event(event_time)
                d = asdict(ev)
                d["_platform"] = "shopee"
            else:
                ev = simulator._generate_tiktok_event(event_time)
                d = asdict(ev)
                d["_platform"] = "tiktok"

            raw_events.append(d)

        current_dt += timedelta(days=1)

    raw_df = pd.DataFrame(raw_events)
    logger.info("Đã sinh thành công %d đơn hàng lịch sử.", len(raw_df))
    return raw_df


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
        try:
            res = load(clean_df)
            count = res.get("rows_loaded", 0)
            logger.info("✓ Nạp thành công %d Fact_Orders vào PostgreSQL!", count)
        except Exception as e:
            logger.error("Không thể nạp vào PostgreSQL: %s", e)


def main():
    parser = argparse.ArgumentParser(description="Sinh dữ liệu đơn hàng lịch sử")
    parser.add_argument("--days", type=int, default=60, help="Số ngày lịch sử (mặc định 60)")
    parser.add_argument("--orders-per-day", type=int, default=150, help="Số đơn cơ bản/ngày (mặc định 150)")
    parser.add_argument("--load-db", action="store_true", help="Nạp trực tiếp vào PostgreSQL Star Schema")
    args = parser.parse_args()

    raw_df = generate_historical_orders(
        days=args.days,
        base_orders_per_day=args.orders_per_day,
    )
    save_and_load(raw_df, load_db=args.load_db)


if __name__ == "__main__":
    main()
