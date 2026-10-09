"""Đánh giá dự báo nhiều ngày theo đúng luồng suy luận đệ quy của serving."""

from __future__ import annotations

import logging
import os
from datetime import date, timedelta
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

import numpy as np
import pandas as pd

from ml.features.feature_pipeline import FIXED_CATEGORY_VOCAB
from serving.app.features import build_feature_row, to_model_input

logger = logging.getLogger("ml.training.recursive_evaluation")

STATS_WINDOW_DAYS = 28
DEFAULT_HISTORY_DAYS = 120
PredictOne = Callable[[Any, List[float], Sequence[str]], Any]


def _number(value: Any, default: float = 0.0) -> float:
    if value is None or pd.isna(value):
        return default
    return float(value)


def _feature_profile(group: pd.DataFrame) -> Dict[str, Any]:
    profile: Dict[str, Any] = {}
    for column in ("category", "abc_class", "xyz_class", "matrix_class", "cv", "safety_stock"):
        if column in group.columns:
            values = group[column].dropna()
            if len(values):
                profile[column] = values.iloc[0]
    return profile


def _category_mappings(df: pd.DataFrame) -> Dict[str, Dict[str, int]]:
    mappings: Dict[str, Dict[str, int]] = {}
    for column in ("category", "abc_class", "xyz_class", "matrix_class"):
        if column not in df.columns:
            continue
        vocabulary = FIXED_CATEGORY_VOCAB.get(column)
        if vocabulary is None:
            vocabulary = sorted(df[column].dropna().astype(str).unique())
        mappings[column] = {value: index for index, value in enumerate(vocabulary)}
    return mappings


def _complete_history(records: Dict[date, Dict[str, Any]], end_date: date) -> List[Dict[str, Any]]:
    eligible = [d for d in records if d <= end_date]
    if not eligible:
        return []

    start_date = min(eligible)
    previous_price = _number(records[start_date].get("avg_unit_price"))
    completed = []
    current_date = start_date
    while current_date <= end_date:
        row = dict(records.get(current_date, {}))
        demand = _number(row.get("daily_demand"))
        previous_price = _number(row.get("avg_unit_price"), previous_price)
        row.update({
            "date": current_date,
            "daily_demand": demand,
            "daily_revenue": _number(row.get("daily_revenue"), demand * previous_price),
            "avg_unit_price": previous_price,
            "total_discount": _number(row.get("total_discount")),
            "order_count": _number(row.get("order_count"), demand),
        })
        completed.append(row)
        current_date += timedelta(days=1)
    return completed


def _history_from_daily_data(
    sku: str,
    origin: date,
    daily_history: pd.DataFrame,
) -> List[Dict[str, Any]]:
    sku_rows = daily_history[daily_history["sku"].astype(str) == sku].copy()
    if sku_rows.empty:
        return []
    sku_rows["_date"] = pd.to_datetime(sku_rows["date"]).dt.date
    sku_rows = sku_rows[sku_rows["_date"] <= origin].sort_values("_date")
    records = {
        row["_date"]: {
            "date": row["_date"],
            "daily_demand": row.get("daily_demand"),
            "daily_revenue": row.get("daily_revenue"),
            "avg_unit_price": row.get("avg_unit_price"),
            "total_discount": row.get("total_discount"),
            "order_count": row.get("order_count"),
        }
        for row in sku_rows.to_dict(orient="records")
    }
    return _complete_history(records, origin)


def _history_from_features(
    sku: str,
    origin: date,
    feature_df: pd.DataFrame,
    max_lag: int,
) -> List[Dict[str, Any]]:
    """Khôi phục burn-in history từ lag lớn nhất nếu chưa có daily_history.parquet."""
    sku_rows = feature_df[feature_df["sku"].astype(str) == sku].copy()
    if sku_rows.empty:
        return []
    sku_rows["_date"] = pd.to_datetime(sku_rows["date"]).dt.date
    sku_rows = sku_rows[sku_rows["_date"] <= origin].sort_values("_date")
    if sku_rows.empty:
        return []

    records: Dict[date, Dict[str, Any]] = {}
    lag_col = f"lag_{max_lag}"
    for row in sku_rows.to_dict(orient="records"):
        row_date = row["_date"]
        prior_date = row_date - timedelta(days=max_lag)
        if prior_date not in records and lag_col in row:
            demand = _number(row.get(lag_col))
            price = _number(row.get("avg_unit_price"))
            records[prior_date] = {
                "date": prior_date,
                "daily_demand": demand,
                "daily_revenue": demand * price,
                "avg_unit_price": price,
                "total_discount": 0.0,
                # Ước lượng 1 đơn vị / đơn khi dữ liệu nguồn lịch sử không còn.
                "order_count": demand,
            }
        records[row_date] = {
            "date": row_date,
            "daily_demand": row.get("daily_demand"),
            "daily_revenue": row.get("daily_revenue"),
            "avg_unit_price": row.get("avg_unit_price"),
            "total_discount": row.get("total_discount"),
            "order_count": row.get("order_count"),
        }
    return _complete_history(records, origin)


def _predict_one(model: Any, values: List[float], feature_cols: Sequence[str]) -> float:
    x = pd.DataFrame([values], columns=list(feature_cols))
    raw = model.predict(x)
    return float(np.asarray(raw).reshape(-1)[0])


def forecast_fold_recursively(
    model: Any,
    feature_df: pd.DataFrame,
    test_df: pd.DataFrame,
    feature_cols: Sequence[str],
    target_col: str,
    daily_history: Optional[pd.DataFrame] = None,
    predict_one: Optional[PredictOne] = None,
) -> Tuple[pd.DataFrame, np.ndarray, np.ndarray]:
    """Dự báo lần lượt toàn bộ kỳ test; mỗi dự báo làm đầu vào cho ngày kế tiếp.

    Trả về các dòng test có lịch sử tại mốc dự báo, nhãn thực và dự báo cùng thứ tự.
    SKU chưa tồn tại ở mốc dự báo bị loại khỏi metric vì serving chưa có lịch sử để gọi model.
    """
    if test_df.empty:
        return test_df.copy(), np.array([], dtype=float), np.array([], dtype=float)

    scored = test_df.copy().reset_index(drop=True)
    if "target_date" in scored.columns:
        scored["_target_date"] = pd.to_datetime(scored["target_date"]).dt.date
    else:
        scored["_target_date"] = pd.to_datetime(scored["date"]).dt.date + pd.Timedelta(days=1)

    target_dates = sorted(scored["_target_date"].unique())
    forecast_start, forecast_end = target_dates[0], target_dates[-1]
    origin = forecast_start - timedelta(days=1)
    max_lag = max(
        (int(column[4:]) for column in feature_cols if column.startswith("lag_") and column[4:].isdigit()),
        default=1,
    )
    history_days = max(1, int(os.getenv("SERVING_HISTORY_DAYS", str(DEFAULT_HISTORY_DAYS))))
    mappings = _category_mappings(feature_df)
    predictions: Dict[Tuple[str, date], float] = {}
    skipped_skus = []

    for sku_value in scored["sku"].drop_duplicates().tolist():
        sku = str(sku_value)
        if daily_history is not None and not daily_history.empty:
            history = _history_from_daily_data(sku, origin, daily_history)
        else:
            history = _history_from_features(sku, origin, feature_df, max_lag)
        if not history or history[-1]["date"] != origin:
            skipped_skus.append(sku)
            continue

        history = history[-history_days:]
        if not any(_number(row.get("daily_demand")) > 0 for row in history):
            # Warehouse serving trả None cho SKU chưa có nhu cầu trong cửa sổ lịch sử.
            skipped_skus.append(sku)
            continue
        recent = history[-STATS_WINDOW_DAYS:]
        total_orders = sum(_number(row.get("order_count")) for row in recent)
        qty_per_order = (
            sum(_number(row.get("daily_demand")) for row in recent) / total_orders
            if total_orders else 1.0
        )
        group = feature_df[feature_df["sku"].astype(str) == sku]
        profile = _feature_profile(group)

        current_date = origin
        while current_date < forecast_end:
            row = build_feature_row(
                history,
                current_date,
                profile=profile,
                category_mappings=mappings,
            )
            values = to_model_input(row, feature_cols)
            raw_prediction = (predict_one or _predict_one)(model, values, feature_cols)
            prediction = max(0.0, float(np.asarray(raw_prediction).reshape(-1)[0]))
            current_date += timedelta(days=1)
            previous_price = _number(history[-1].get("avg_unit_price"))
            history.append({
                "date": current_date,
                "daily_demand": prediction,
                "daily_revenue": prediction * previous_price,
                "avg_unit_price": previous_price,
                "total_discount": 0.0,
                "order_count": prediction / qty_per_order if qty_per_order else 0.0,
            })
            if len(history) > history_days:
                history = history[-history_days:]
            predictions[(sku, current_date)] = prediction

    scored["_prediction"] = [
        predictions.get((str(sku), target_date), np.nan)
        for sku, target_date in zip(scored["sku"], scored["_target_date"])
    ]
    scored = scored.dropna(subset=["_prediction"]).copy()
    if skipped_skus:
        logger.warning(
            "Bỏ %d SKU khỏi metric vì không có lịch sử tại mốc dự báo: %s",
            len(skipped_skus), ", ".join(skipped_skus[:10]),
        )

    return (
        scored,
        scored[target_col].to_numpy(dtype=float),
        scored["_prediction"].to_numpy(dtype=float),
    )
