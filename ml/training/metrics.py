"""
metrics.py
----------
Thư viện tính toán các độ đo đánh giá mô hình dự báo nhu cầu bán lẻ (Retail Demand Metrics):
  - MAE (Mean Absolute Error)
  - RMSE (Root Mean Squared Error)
  - MAPE (Mean Absolute Percentage Error có smoothing epsilon tránh chia 0)
  - WAPE (Weighted Absolute Percentage Error - tiêu chuẩn công nghiệp ngành bán lẻ)
  - R2 Score (Hệ số xác định)
  - Forecast Bias (Đo lường độ thiên kiến dự báo: thừa hàng hay thiếu hàng)

Tương thích cả numpy array và standard Python list (chạy độc lập không phụ thuộc thư viện ngoài).

Tuần 5: Đánh giá mô hình chuỗi thời gian.
"""

import math
from typing import Dict, Optional, Sequence, Union

EVALUATION_PROTOCOL = "origin_t_plus_1_pooled_wape_v2"

try:
    import numpy as np
except ImportError:
    np = None


def _to_float_list(arr: Union[Sequence[float], "np.ndarray"]) -> list[float]:
    """Chuyển đổi input thành danh sách số thực chuẩn."""
    if np is not None and isinstance(arr, np.ndarray):
        return [float(x) for x in arr.flatten()]
    return [float(x) for x in arr]


def _validated_pairs(y_true, y_pred):
    yt, yp = _to_float_list(y_true), _to_float_list(y_pred)
    if not yt or len(yt) != len(yp):
        raise ValueError("Metrics require nonempty actual/prediction arrays of equal length.")
    if not all(math.isfinite(x) for x in yt + yp):
        raise ValueError("Metrics require finite actuals and predictions.")
    if any(x < 0 for x in yt):
        raise ValueError("Retail demand actuals must be nonnegative.")
    return yt, yp


def mean_absolute_error(y_true: Sequence[float], y_pred: Sequence[float]) -> float:
    """Tính MAE."""
    yt, yp = _validated_pairs(y_true, y_pred)
    return sum(abs(a - p) for a, p in zip(yt, yp)) / len(yt)


def root_mean_squared_error(y_true: Sequence[float], y_pred: Sequence[float]) -> float:
    """Tính RMSE."""
    yt, yp = _validated_pairs(y_true, y_pred)
    mse = sum((a - p) ** 2 for a, p in zip(yt, yp)) / len(yt)
    return math.sqrt(mse)


def mean_absolute_percentage_error(
    y_true: Sequence[float],
    y_pred: Sequence[float],
    epsilon: float = 1.0,
) -> float:
    """Tính MAPE với hằng số làm mượt epsilon để tránh lỗi chia cho 0 khi ngày bán = 0."""
    yt, yp = _validated_pairs(y_true, y_pred)
    if not math.isfinite(epsilon) or epsilon <= 0:
        raise ValueError("MAPE smoothing epsilon must be finite and positive.")
    return (sum(abs(a - p) / (a + epsilon) for a, p in zip(yt, yp)) / len(yt)) * 100.0


def weighted_absolute_percentage_error(y_true: Sequence[float], y_pred: Sequence[float]) -> Optional[float]:
    """
    Tính WAPE (Weighted Absolute Percentage Error):
      WAPE = sum(|y_true - y_pred|) / sum(y_true) * 100%

    Trả None khi tổng actual bằng 0 (tỷ số không xác định), kể cả khi dự báo
    cũng toàn 0. Khi xuất JSON, None trở thành null; dùng MAE/RMSE để diễn giải.
    """
    yt, yp = _validated_pairs(y_true, y_pred)
    total_actual = sum(yt)
    if total_actual == 0:
        return None
    return (sum(abs(a - p) for a, p in zip(yt, yp)) / total_actual) * 100.0


def forecast_bias(y_true: Sequence[float], y_pred: Sequence[float]) -> Optional[float]:
    """
    Tính độ thiên lệch dự báo (Forecast Bias):
      Bias = sum(y_pred - y_true) / sum(y_true) * 100%
      > 0: Mô hình có xu hướng dự báo thừa (Over-forecasting -> rủi ro tồn kho cao)
      < 0: Mô hình có xu hướng dự báo thiếu (Under-forecasting -> rủi ro đứt hàng)
    """
    yt, yp = _validated_pairs(y_true, y_pred)
    total_actual = sum(yt)
    if total_actual == 0:
        return None
    return (sum(p - a for a, p in zip(yt, yp)) / total_actual) * 100.0


def calculate_all_metrics(y_true: Sequence[float], y_pred: Sequence[float]) -> Dict[str, Optional[float]]:
    """Tính toán toàn bộ các metric đánh giá dự báo nhu cầu."""
    yt, raw_predictions = _validated_pairs(y_true, y_pred)
    # Cắt các giá trị dự báo âm về 0 (nhu cầu hàng hóa không thể âm)
    yp = [max(0.0, p) for p in raw_predictions]

    mae = mean_absolute_error(yt, yp)
    rmse = root_mean_squared_error(yt, yp)
    mape = mean_absolute_percentage_error(yt, yp)
    wape = weighted_absolute_percentage_error(yt, yp)
    bias = forecast_bias(yt, yp)

    # R2 Score
    n = len(yt)
    if n > 0:
        mean_y = sum(yt) / n
        ss_tot = sum((y - mean_y) ** 2 for y in yt)
        ss_res = sum((y - p) ** 2 for y, p in zip(yt, yp))
        r2 = float(1.0 - (ss_res / ss_tot)) if ss_tot > 0 else 0.0
    else:
        r2 = 0.0

    return {
        "mae": round(mae, 4),
        "rmse": round(rmse, 4),
        "mape": round(mape, 2),
        "wape": round(wape, 2) if wape is not None else None,
        "r2": round(r2, 4),
        "bias": round(bias, 2) if bias is not None else None,
    }


def aggregate_fold_metrics(fold_metrics, actuals, predictions):
    """Pool every scored pair; never turn undefined fold percentages into zeros.

    The principal WAPE uses the total absolute error / total actual across
    all folds, including absolute errors in folds whose actuals are all zero.
    A mean of *defined* fold WAPEs is retained only as a named diagnostic.
    """
    if not fold_metrics:
        raise ValueError("No scored folds are available for evaluation.")
    metrics = calculate_all_metrics(actuals, predictions)
    defined = [m["wape"] for m in fold_metrics if m["wape"] is not None]
    metrics.update({
        "mean_fold_wape": round(sum(defined) / len(defined), 2) if defined else None,
        "defined_wape_folds": len(defined),
        "undefined_wape_folds": len(fold_metrics) - len(defined),
        "scored_rows": len(actuals),
    })
    return metrics
