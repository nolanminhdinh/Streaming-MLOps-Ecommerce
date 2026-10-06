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
     - Mã hóa biến phân loại (Category, ABC/XYZ class) bằng từ điển cố định.
     - ABC/XYZ chỉ fit trên dữ liệu trước vùng holdout walk-forward (chống leakage).
  4. Tạo biến mục tiêu: `target_demand_t1` (Nhu cầu ngày t+1).
  5. Xuất tập Feature Store ra `data/features_daily.parquet` và hợp đồng đặc trưng
     `data/feature_spec.json` cho tầng serving.

Tuần 5: Xây dựng Feature Store phục vụ huấn luyện mô hình.
"""

import argparse
import json
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


# Các cột định danh / metadata / nhãn không được đưa vào X khi huấn luyện.
# Dùng chung cho train_baseline.select_feature_columns và feature_spec.json (serving).
TARGET_COL = "target_t_plus_1"
EXCLUDE_COLS = {
    "sku", "date", "product_name", "category", "abc_class", "xyz_class",
    "matrix_class", TARGET_COL,
}

# Từ điển mã hóa cố định cho phân khúc → mã không phụ thuộc thứ tự xuất hiện trong dữ liệu
FIXED_CATEGORY_VOCAB = {
    "abc_class": ["A", "B", "C"],
    "xyz_class": ["X", "Y", "Z"],
    "matrix_class": [a + x for a in "ABC" for x in "XYZ"],
}

# Độ dài vùng holdout của walk-forward mặc định (3 fold × 14 ngày test + 7 ngày val).
DEFAULT_HOLDOUT_DAYS = 3 * 14 + 7
MIN_SEGMENT_FIT_DAYS = 14


def get_feature_columns(df: pd.DataFrame) -> list[str]:
    """Danh sách cột đặc trưng X theo đúng thứ tự cột trong Feature Store."""
    return [c for c in df.columns if c not in EXCLUDE_COLS]


def _naive_timestamps(values: pd.Series) -> pd.Series:
    ts = pd.to_datetime(values)
    if getattr(ts.dt, "tz", None) is not None:
        ts = ts.dt.tz_localize(None)
    return ts


def _resolve_segment_cutoff(orders_df: pd.DataFrame, holdout_days: int) -> pd.Timestamp:
    """
    Mốc thời gian dùng để fit ABC/XYZ: chỉ dùng dữ liệu TRƯỚC vùng holdout của walk-forward.

    Trước đây ABC/XYZ (và cv, safety_stock) được tính trên toàn bộ dữ liệu, kể cả các ngày
    test → thông tin tương lai (độ biến động, doanh thu) rò rỉ vào đặc trưng huấn luyện.
    """
    dates = _naive_timestamps(orders_df["create_time"]).dt.normalize()
    min_d, max_d = dates.min(), dates.max()
    cutoff = max_d - pd.Timedelta(days=holdout_days)
    if (cutoff - min_d).days < MIN_SEGMENT_FIT_DAYS:
        # Dữ liệu quá ngắn: lùi về nửa đầu chuỗi để vẫn có đủ ngày tính CV
        cutoff = min_d + (max_d - min_d) / 2
        logger.warning(
            "Dữ liệu chỉ có %d ngày, không đủ tách holdout %d ngày → fit ABC/XYZ trên nửa đầu (đến %s).",
            (max_d - min_d).days + 1, holdout_days, cutoff.date(),
        )
    return cutoff.normalize()


def _encode_categories(df: pd.DataFrame) -> dict:
    """Mã hóa biến phân loại bằng từ điển xác định (được lưu lại cho serving)."""
    mappings: dict = {}
    for cat_col in ["category", "abc_class", "xyz_class", "matrix_class"]:
        if cat_col not in df.columns:
            continue
        vocab = FIXED_CATEGORY_VOCAB.get(cat_col) or sorted(df[cat_col].dropna().astype(str).unique())
        mapping = {v: i for i, v in enumerate(vocab)}
        df[f"{cat_col}_code"] = df[cat_col].map(mapping).fillna(-1).astype(int)
        mappings[cat_col] = mapping
    return mappings


def build_feature_store(
    orders_df: pd.DataFrame,
    lags: list[int] = [1, 2, 3, 7, 14, 21, 28],
    rolling_windows: list[int] = [7, 14, 28],
    save_path: Optional[str] = None,
    holdout_days: int = DEFAULT_HOLDOUT_DAYS,
    spec_path: Optional[str] = None,
) -> pd.DataFrame:
    """
    Thực thi toàn bộ chu trình tạo đặc trưng và lưu vào Feature Store.

    Ngoài file Parquet, xuất kèm `feature_spec.json` (danh sách cột theo thứ tự, từ điển
    mã hóa, hồ sơ phân khúc từng SKU) để tầng serving dựng đặc trưng y hệt lúc huấn luyện.
    """
    logger.info("═" * 60)
    logger.info("  BẮT ĐẦU FEATURE ENGINEERING PIPELINE")
    logger.info("  Tổng số đơn hàng đầu vào: %d", len(orders_df))
    logger.info("═" * 60)

    # 1. Phân loại ABC/XYZ CHỈ trên dữ liệu trước vùng holdout (chống data leakage)
    cutoff = _resolve_segment_cutoff(orders_df, holdout_days)
    fit_orders = orders_df[_naive_timestamps(orders_df["create_time"]) < cutoff]
    logger.info("✓ Fit ABC/XYZ trên %d đơn hàng trước mốc %s", len(fit_orders), cutoff.date())

    classifier = ABCXYZClassifier()
    abc_xyz_df = classifier.fit_transform(fit_orders)
    segment_cols = ["sku", "abc_class", "xyz_class", "matrix_class", "cv", "safety_stock"]
    sku_segments = abc_xyz_df[segment_cols]

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

    # 4. Ghép nối với thông tin phân khúc ABC/XYZ (SKU mới xuất hiện sau mốc → nhóm C/Z)
    merged_features = pd.merge(feature_df, sku_segments, on="sku", how="left")
    merged_features["abc_class"] = merged_features["abc_class"].fillna("C")
    merged_features["xyz_class"] = merged_features["xyz_class"].fillna("Z")
    merged_features["matrix_class"] = merged_features["matrix_class"].fillna("CZ")
    merged_features["cv"] = merged_features["cv"].fillna(0.0)
    merged_features["safety_stock"] = merged_features["safety_stock"].fillna(0)

    # 5. Mã hóa các biến phân loại (Categorical Encoding) bằng từ điển cố định
    category_mappings = _encode_categories(merged_features)

    # 6. Loại bỏ các dòng khởi động (burn-in period) do thiếu giá trị lag lớn nhất
    max_lag = max(lags)
    valid_features = merged_features.dropna(subset=[f"lag_{max_lag}", TARGET_COL]).copy()

    logger.info(
        "✓ Hoàn tất trích xuất đặc trưng. Dữ liệu hợp lệ sẵn sàng huấn luyện: %d dòng, %d cột.",
        len(valid_features), len(valid_features.columns),
    )

    # 7. Lưu file vào Feature Store (Parquet) + Feature Spec (JSON)
    if save_path is None:
        data_dir = os.path.join(os.path.dirname(__file__), "..", "..", "data")
        os.makedirs(data_dir, exist_ok=True)
        save_path = os.path.join(data_dir, "features_daily.parquet")

    valid_features.to_parquet(save_path, index=False)
    logger.info("✓ Đã lưu Feature Store tại: %s", save_path)

    spec_path = spec_path or os.path.join(os.path.dirname(os.path.abspath(save_path)), "feature_spec.json")
    save_feature_spec(
        spec_path,
        feature_columns=get_feature_columns(valid_features),
        lags=lags,
        rolling_windows=rolling_windows,
        ema_spans=extractor.ema_spans,
        category_mappings=category_mappings,
        merged_features=merged_features,
        segment_cutoff=cutoff,
    )

    return valid_features


def save_feature_spec(
    spec_path: str,
    feature_columns: list[str],
    lags: list[int],
    rolling_windows: list[int],
    ema_spans: list[int],
    category_mappings: dict,
    merged_features: pd.DataFrame,
    segment_cutoff: pd.Timestamp,
) -> dict:
    """Ghi hợp đồng đặc trưng (feature contract) dùng chung giữa huấn luyện và serving."""
    profile_cols = [c for c in ["sku", "product_name", "category", "abc_class", "xyz_class",
                                "matrix_class", "cv", "safety_stock"] if c in merged_features.columns]
    profiles_df = merged_features.sort_values("date").drop_duplicates("sku", keep="last")[profile_cols]
    sku_profiles = {}
    for rec in profiles_df.to_dict(orient="records"):
        sku = str(rec.pop("sku"))
        sku_profiles[sku] = {
            k: (None if pd.isna(v) else (float(v) if isinstance(v, (int, float, np.integer, np.floating)) else str(v)))
            for k, v in rec.items()
        }

    spec = {
        "target_column": TARGET_COL,
        "feature_columns": feature_columns,
        "lags": list(lags),
        "rolling_windows": list(rolling_windows),
        "ema_spans": list(ema_spans),
        "category_mappings": category_mappings,
        "sku_profiles": sku_profiles,
        "segment_cutoff_date": str(segment_cutoff.date()),
        "generated_at": pd.Timestamp.now(tz="UTC").isoformat(),
    }
    with open(spec_path, "w", encoding="utf-8") as f:
        json.dump(spec, f, indent=2, ensure_ascii=False)
    logger.info("✓ Đã lưu Feature Spec (%d đặc trưng, %d SKU) tại: %s",
                len(feature_columns), len(sku_profiles), spec_path)
    return spec


def main():
    parser = argparse.ArgumentParser(description="Tạo Feature Store cho chuỗi thời gian")
    parser.add_argument("--output", type=str, default=None, help="Đường dẫn file parquet đầu ra")
    args = parser.parse_args()

    orders_df = load_orders_data()
    build_feature_store(orders_df, save_path=args.output)


if __name__ == "__main__":
    main()
