"""
time_series_features.py
-----------------------
Module trích xuất đặc trưng chuỗi thời gian (Time Series Feature Engineering) phục vụ
huấn luyện các mô hình Machine Learning dự báo nhu cầu (LightGBM, XGBoost, LSTM).

Nguyên tắc thiết kế:
  1. Tránh rò rỉ dữ liệu (No Data Leakage): Tất cả rolling statistics và lag features
     bắt buộc phải được tính trên dữ liệu quá khứ (shift >= 1).
  2. Tổng hợp theo hạt ngày (Daily Aggregation) cho từng SKU.
  3. Walk-Forward Validation Split: Chia tập Train / Val / Test theo trục thời gian,
     không dùng random split.

Tuần 4: Xây dựng bộ đặc trưng chuỗi thời gian.
"""

import logging
from datetime import datetime
from typing import Generator, List, Optional, Tuple

import numpy as np
import pandas as pd

logger = logging.getLogger("ml.features")


class TimeSeriesFeatureExtractor:
    """Lớp tạo đặc trưng chuỗi thời gian cho bài toán dự báo nhu cầu hàng hóa."""

    def __init__(
        self,
        lags: List[int] = [1, 2, 3, 7, 14, 28],
        rolling_windows: List[int] = [7, 14, 28],
        ema_spans: List[int] = [7, 14],
    ):
        self.lags = lags
        self.rolling_windows = rolling_windows
        self.ema_spans = ema_spans

    def aggregate_daily(
        self,
        orders_df: pd.DataFrame,
        sku_col: str = "sku",
        date_col: str = "create_time",
        quantity_col: str = "quantity",
        price_col: str = "original_price",
        discount_col: str = "seller_discount",
        revenue_col: str = "buyer_total_amount",
    ) -> pd.DataFrame:
        """
        Gom nhóm đơn hàng theo ngày và SKU, đảm bảo chuỗi thời gian liên tục.
        """
        df = orders_df.copy()

        # Loại bỏ đơn hủy khỏi chuỗi nhu cầu thực tế
        if "is_cancelled" in df.columns:
            df = df[~df["is_cancelled"]].copy()
        elif "order_status" in df.columns:
            df = df[df["order_status"] != "CANCELLED"].copy()

        df["date"] = pd.to_datetime(df[date_col]).dt.date

        # Tổng hợp các metrics theo ngày
        daily = (
            df.groupby([sku_col, "date"])
            .agg(
                daily_demand=(quantity_col, "sum"),
                daily_revenue=(revenue_col, "sum"),
                avg_unit_price=(price_col, "mean"),
                total_discount=(discount_col, "sum") if discount_col in df.columns else (quantity_col, "count"),
                order_count=(quantity_col, "count"),
            )
            .reset_index()
        )

        # Lấp đầy các ngày không phát sinh đơn bằng grid đầy đủ
        min_date = daily["date"].min()
        max_date = daily["date"].max()
        all_dates = pd.date_range(min_date, max_date, freq="D").date
        unique_skus = daily[sku_col].unique()

        grid_idx = pd.MultiIndex.from_product([unique_skus, all_dates], names=[sku_col, "date"])
        grid_df = pd.DataFrame(index=grid_idx).reset_index()

        merged = pd.merge(grid_df, daily, on=[sku_col, "date"], how="left")
        merged["daily_demand"] = merged["daily_demand"].fillna(0)
        merged["daily_revenue"] = merged["daily_revenue"].fillna(0)
        merged["order_count"] = merged["order_count"].fillna(0)
        merged["total_discount"] = merged["total_discount"].fillna(0)

        # Forward fill giá đơn vị cho các ngày không bán
        merged["avg_unit_price"] = merged.groupby(sku_col)["avg_unit_price"].ffill().bfill()

        # Thêm metadata nếu có
        meta_cols = [c for c in ["product_name", "category"] if c in df.columns]
        if meta_cols:
            sku_meta = df[[sku_col] + meta_cols].drop_duplicates(subset=[sku_col])
            merged = pd.merge(merged, sku_meta, on=sku_col, how="left")

        return merged.sort_values(by=[sku_col, "date"]).reset_index(drop=True)

    def extract_features(
        self,
        daily_df: pd.DataFrame,
        sku_col: str = "sku",
        target_col: str = "daily_demand",
    ) -> pd.DataFrame:
        """
        Trích xuất đầy đủ các nhóm đặc trưng cho từng SKU.

        Các nhóm đặc trưng:
          1. Lịch & Sự kiện: DayOfWeek, Month, IsWeekend, IsMegaSale (của ngày t)
             và target_* (của ngày mục tiêu t+1).
          2. Lag Features: nhu cầu các ngày quá khứ (t-1, t-7, t-14, ...).
          3. Rolling Statistics: Mean, Std, Max, Min trên các cửa sổ trượt (tránh data leakage bằng shift(1)).
          4. Exponential Moving Average: EMA 7, 14 ngày.
          5. Tỷ lệ tăng trưởng (Momentum): Tốc độ tăng trưởng so với tuần trước.
          6. Target: nhu cầu của ngày tiếp theo (t+1).
        """
        df = daily_df.copy()
        df["dt"] = pd.to_datetime(df["date"])

        # ── 1. Calendar & Event Features ──
        df["day_of_week"] = df["dt"].dt.dayofweek
        df["is_weekend"] = df["day_of_week"].isin([5, 6]).astype(int)
        df["day_of_month"] = df["dt"].dt.day
        df["month"] = df["dt"].dt.month
        df["quarter"] = df["dt"].dt.quarter
        # Mega-sale ngày đôi: ngày = tháng (1/1, 2/2, ..., 12/12)
        df["is_mega_sale"] = (df["day_of_month"] == df["month"]).astype(int)

        # Lịch của NGÀY MỤC TIÊU (t+1). Các cột lịch ở trên mô tả ngày t (ngày của dòng),
        # nhưng target là nhu cầu ngày t+1 → mô hình cần biết ngày được dự báo có phải
        # Mega-sale / cuối tuần hay không, nếu không hiệu ứng 10/10, 11/11 bị lệch 1 ngày.
        target_dt = df["dt"] + pd.Timedelta(days=1)
        df["target_day_of_week"] = target_dt.dt.dayofweek
        df["target_is_weekend"] = df["target_day_of_week"].isin([5, 6]).astype(int)
        df["target_day_of_month"] = target_dt.dt.day
        df["target_is_mega_sale"] = (target_dt.dt.day == target_dt.dt.month).astype(int)

        feature_dfs = []

        # Xử lý theo từng SKU để không bị trôi dữ liệu giữa các sản phẩm
        for sku, group in df.groupby(sku_col):
            grp = group.sort_values("dt").copy()

            # ── 2. Lag Features ──
            for lag in self.lags:
                grp[f"lag_{lag}"] = grp[target_col].shift(lag)

            # ── 3. Rolling Statistics (Shift 1 để tránh leakage) ──
            shifted_target = grp[target_col].shift(1)
            for window in self.rolling_windows:
                grp[f"rolling_mean_{window}"] = shifted_target.rolling(window=window, min_periods=1).mean()
                grp[f"rolling_std_{window}"] = shifted_target.rolling(window=window, min_periods=1).std().fillna(0)
                grp[f"rolling_max_{window}"] = shifted_target.rolling(window=window, min_periods=1).max()
                grp[f"rolling_min_{window}"] = shifted_target.rolling(window=window, min_periods=1).min()

            # ── 4. Exponential Moving Average (EMA) ──
            for span in self.ema_spans:
                grp[f"ema_{span}"] = shifted_target.ewm(span=span, adjust=False).mean()

            # ── 5. Momentum / Growth ──
            if 1 in self.lags and 7 in self.lags:
                grp["growth_wow"] = (grp["lag_1"] - grp["lag_7"]) / (grp["lag_7"] + 1.0)

            # ── 6. Target (Dự báo nhu cầu ngày tiếp theo t+1) ──
            grp["target_t_plus_1"] = grp[target_col].shift(-1)

            feature_dfs.append(grp)

        result_df = pd.concat(feature_dfs, ignore_index=True)
        result_df = result_df.drop(columns=["dt"])

        logger.info(
            "Trích xuất đặc trưng hoàn tất: %d dòng, %d cột đặc trưng.",
            len(result_df), len(result_df.columns),
        )
        return result_df

    @staticmethod
    def walk_forward_split(
        df: pd.DataFrame,
        date_col: str = "date",
        n_splits: int = 3,
        test_days: int = 14,
        val_days: int = 7,
    ) -> Generator[Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame], None, None]:
        """
        Chia dữ liệu theo phương pháp Walk-Forward Validation (Expanding Window) cho chuỗi thời gian.

        Với bài toán dự báo t+1, hãy truyền ``date_col="target_date"`` để mỗi nhãn
        được xếp vào fold theo ngày nhu cầu mà nhãn đại diện, thay vì ngày tạo đặc trưng.

        Yields:
            (train_df, val_df, test_df) cho từng fold.
        """
        unique_dates = sorted(df[date_col].unique())
        total_days = len(unique_dates)

        min_required_days = test_days * n_splits + val_days + 14
        if total_days < min_required_days:
            logger.warning(
                "Dữ liệu có %d ngày, ít hơn khuyến nghị (%d ngày) cho %d splits.",
                total_days, min_required_days, n_splits,
            )

        for i in range(n_splits):
            test_end_idx = total_days - i * test_days
            test_start_idx = test_end_idx - test_days
            val_start_idx = test_start_idx - val_days
            train_end_idx = val_start_idx

            if train_end_idx <= 0:
                break

            train_dates = unique_dates[:train_end_idx]
            val_dates = unique_dates[val_start_idx:test_start_idx]
            test_dates = unique_dates[test_start_idx:test_end_idx]

            train_df = df[df[date_col].isin(train_dates)].copy()
            val_df = df[df[date_col].isin(val_dates)].copy()
            test_df = df[df[date_col].isin(test_dates)].copy()

            logger.info(
                "Fold %d: Train [%s -> %s] (%d ngày) | Val [%s -> %s] (%d ngày) | Test [%s -> %s] (%d ngày)",
                i + 1,
                train_dates[0], train_dates[-1], len(train_dates),
                val_dates[0], val_dates[-1], len(val_dates),
                test_dates[0], test_dates[-1], len(test_dates),
            )

            yield train_df, val_df, test_df
