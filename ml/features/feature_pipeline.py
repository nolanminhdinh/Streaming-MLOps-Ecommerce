"""
feature_pipeline.py
-------------------
Pipeline tự động tạo và lưu trữ đặc trưng (Feature Store Pipeline) cho bài toán dự báo nhu cầu:
  1. Đọc dữ liệu từ PostgreSQL Star Schema (hoặc Parquet fallback).
  2. Tích hợp nhãn ma trận ABC/XYZ để gán đặc tính phân khúc sản phẩm.
  3. Trích xuất nhóm đặc trưng:
     - Lịch & Mùa vụ (DayOfWeek, IsWeekend, DayOfMonth, Month, IsMegaSale).
     - Biến trễ nhu cầu (Lag 1, 2, 3, 7, 14, 21, 28).
     - Thống kê trượt (Rolling Mean, Std, Max, Min trên 7, 14, 28 ngày) với shift(1) chống Data Leakage.
     - Trung bình trượt số mũ (EMA 7, 14 ngày).
     - Tỷ lệ tăng trưởng so với tuần trước (Growth WoW).
     - Mã hóa biến phân loại (Category, ABC/XYZ class).
  4. Tạo biến mục tiêu: `target_demand_t1` (Nhu cầu ngày t+1).
  5. Xuất tập Feature Store ra `data/features_daily.parquet`.

Tuần 5: Xây dựng Feature Store phục vụ huấn luyện mô hình.
"""

import argparse
import logging
import os
import sys
from typing import Optional, Tuple

import numpy as np
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

# Cho phép import từ thư mục gốc
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from ml.features.abc_xyz import ABCXYZClassifier
from ml.features.time_series_features import TimeSeriesFeatureExtractor
from warehouse.etl.load import get_engine

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("ml.feature_pipeline")


def load_orders_data() -> pd.DataFrame:
    """Nạp dữ liệu đơn hàng sạch từ PostgreSQL hoặc file Parquet dự phòng."""
    # 1. Thử nạp từ PostgreSQL
    try:
        engine = get_engine()
        query = """
            SELECT
                f.order_id,
                f.platform,
                p.sku,
                p.product_name,
                p.category,
                f.create_time,
                f.quantity,
                f.original_price,
                f.buyer_total_amount,
                f.seller_discount,
                f.is_cancelled,
                f.order_status
            FROM Fact_Orders f
            JOIN Dim_Products p ON f.product_key = p.product_key
            WHERE f.is_cancelled = FALSE
            ORDER BY f.create_time ASC
        """
        df = pd.read_sql(query, engine)
        if not df.empty:
            logger.info("✓ Nạp thành công %d đơn hàng từ PostgreSQL Fact_Orders.", len(df))
            return df
    except Exception as e:
        logger.warning("Không thể nạp từ PostgreSQL (%s). Đang kiểm tra file Parquet...", e)

    # 2. Thử nạp từ Parquet local
    data_dir = os.path.join(os.path.dirname(__file__), "..", "..", "data")
    clean_p = os.path.join(data_dir, "historical_orders_clean.parquet")
    raw_p = os.path.join(data_dir, "historical_orders_raw.parquet")

    if os.path.exists(clean_p):
        df = pd.read_parquet(clean_p)
        logger.info("✓ Nạp thành công %d đơn hàng từ file: %s", len(df), clean_p)
        return df
    elif os.path.exists(raw_p):
        from warehouse.etl.transform import unify_schema, clean_data
        raw_df = pd.read_parquet(raw_p)
        df = clean_data(unify_schema(raw_df))
        logger.info("✓ Nạp và làm sạch %d đơn hàng từ file: %s", len(df), raw_p)
        return df

    # 3. Tự động sinh dữ liệu lịch sử nếu chưa có nguồn nào
    logger.info("! Chưa có dữ liệu lịch sử. Đang tự động sinh 60 ngày dữ liệu mô phỏng...")
    from scripts.simulate_historical_data import generate_historical_orders
    from warehouse.etl.transform import unify_schema, clean_data
    raw_df = generate_historical_orders(days=60, base_orders_per_day=150)
    df = clean_data(unify_schema(raw_df))
    return df


def build_feature_store(
    orders_df: pd.DataFrame,
    lags: list[int] = [1, 2, 3, 7, 14, 21, 28],
    rolling_windows: list[int] = [7, 14, 28],
    save_path: Optional[str] = None,
) -> pd.DataFrame:
    """
    Thực thi toàn bộ chu trình tạo đặc trưng và lưu vào Feature Store.
    """
    logger.info("═" * 60)
    logger.info("  BẮT ĐẦU FEATURE ENGINEERING PIPELINE")
    logger.info("  Tổng số đơn hàng đầu vào: %d", len(orders_df))
    logger.info("═" * 60)

    # 1. Phân loại ABC/XYZ để gắn nhãn phân khúc
    classifier = ABCXYZClassifier()
    abc_xyz_df = classifier.fit_transform(orders_df)
    sku_segments = abc_xyz_df[["sku", "abc_class", "xyz_class", "matrix_class", "cv", "safety_stock"]]

    # 2. Tổng hợp theo hạt ngày (Daily Aggregation)
    extractor = TimeSeriesFeatureExtractor(
        lags=lags,
        rolling_windows=rolling_windows,
        ema_spans=[7, 14],
    )
    daily_df = extractor.aggregate_daily(orders_df)
    logger.info("✓ Tổng hợp theo ngày: %d dòng (SKU × Ngày)", len(daily_df))

    # 3. Trích xuất đặc trưng chuỗi thời gian
    feature_df = extractor.extract_features(daily_df)

    # 4. Ghép nối với thông tin phân khúc ABC/XYZ
    merged_features = pd.merge(feature_df, sku_segments, on="sku", how="left")

    # 5. Mã hóa các biến phân loại (Categorical Encoding)
    for cat_col in ["category", "abc_class", "xyz_class", "matrix_class"]:
        if cat_col in merged_features.columns:
            merged_features[f"{cat_col}_code"] = merged_features[cat_col].astype("category").cat.codes

    # 6. Loại bỏ các dòng khởi động (burn-in period) do thiếu giá trị lag lớn nhất
    max_lag = max(lags)
    valid_features = merged_features.dropna(subset=[f"lag_{max_lag}", "target_t_plus_1"]).copy()

    logger.info(
        "✓ Hoàn tất trích xuất đặc trưng. Dữ liệu hợp lệ sẵn sàng huấn luyện: %d dòng, %d cột.",
        len(valid_features), len(valid_features.columns),
    )

    # 7. Lưu file vào Feature Store (Parquet)
    if save_path is None:
        data_dir = os.path.join(os.path.dirname(__file__), "..", "..", "data")
        os.makedirs(data_dir, exist_ok=True)
        save_path = os.path.join(data_dir, "features_daily.parquet")

    valid_features.to_parquet(save_path, index=False)
    logger.info("✓ Đã lưu Feature Store tại: %s", save_path)

    return valid_features


def main():
    parser = argparse.ArgumentParser(description="Tạo Feature Store cho chuỗi thời gian")
    parser.add_argument("--output", type=str, default=None, help="Đường dẫn file parquet đầu ra")
    args = parser.parse_args()

    orders_df = load_orders_data()
    build_feature_store(orders_df, save_path=args.output)


if __name__ == "__main__":
    main()
