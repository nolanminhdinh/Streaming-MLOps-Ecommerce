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
from typing import Dict, Sequence, Union

try:
    import numpy as np
except ImportError:
    np = None


def _to_float_list(arr: Union[Sequence[float], "np.ndarray"]) -> list[float]:
    """Chuyển đổi input thành danh sách số thực chuẩn."""
    if np is not None and isinstance(arr, np.ndarray):
        return [float(x) for x in arr.flatten()]
    return [float(x) for x in arr]


def mean_absolute_error(y_true: Sequence[float], y_pred: Sequence[float]) -> float:
    """Tính MAE."""
    yt = _to_float_list(y_true)
    yp = _to_float_list(y_pred)
    if not yt or len(yt) != len(yp):
        return 0.0
    return sum(abs(a - p) for a, p in zip(yt, yp)) / len(yt)


def root_mean_squared_error(y_true: Sequence[float], y_pred: Sequence[float]) -> float:
    """Tính RMSE."""
    yt = _to_float_list(y_true)
    yp = _to_float_list(y_pred)
    if not yt or len(yt) != len(yp):
        return 0.0
    mse = sum((a - p) ** 2 for a, p in zip(yt, yp)) / len(yt)
    return math.sqrt(mse)


def mean_absolute_percentage_error(
    y_true: Sequence[float],
    y_pred: Sequence[float],
    epsilon: float = 1.0,
) -> float:
    """Tính MAPE với hằng số làm mượt epsilon để tránh lỗi chia cho 0 khi ngày bán = 0."""
    yt = _to_float_list(y_true)
    yp = _to_float_list(y_pred)
    if not yt or len(yt) != len(yp):
        return 0.0
    return (sum(abs(a - p) / (a + epsilon) for a, p in zip(yt, yp)) / len(yt)) * 100.0


def weighted_absolute_percentage_error(y_true: Sequence[float], y_pred: Sequence[float]) -> float:
    """
    Tính WAPE (Weighted Absolute Percentage Error):
      WAPE = sum(|y_true - y_pred|) / sum(y_true) * 100%

    Đây là tiêu chuẩn đánh giá hàng đầu trong chuỗi cung ứng vì không bị nổ giá trị
    khi có nhiều ngày không phát sinh đơn hàng (zero demand).
    """
    yt = _to_float_list(y_true)
    yp = _to_float_list(y_pred)
    total_actual = sum(yt)
    if total_actual == 0:
        return 0.0
    return (sum(abs(a - p) for a, p in zip(yt, yp)) / total_actual) * 100.0


def forecast_bias(y_true: Sequence[float], y_pred: Sequence[float]) -> float:
    """
    Tính độ thiên lệch dự báo (Forecast Bias):
      Bias = sum(y_pred - y_true) / sum(y_true) * 100%
      > 0: Mô hình có xu hướng dự báo thừa (Over-forecasting -> rủi ro tồn kho cao)
      < 0: Mô hình có xu hướng dự báo thiếu (Under-forecasting -> rủi ro đứt hàng)
    """
    yt = _to_float_list(y_true)
    yp = _to_float_list(y_pred)
    total_actual = sum(yt)
    if total_actual == 0:
        return 0.0
    return (sum(p - a for a, p in zip(yt, yp)) / total_actual) * 100.0


def calculate_all_metrics(y_true: Sequence[float], y_pred: Sequence[float]) -> Dict[str, float]:
    """Tính toán toàn bộ các metric đánh giá dự báo nhu cầu."""
    yt = _to_float_list(y_true)
    # Cắt các giá trị dự báo âm về 0 (nhu cầu hàng hóa không thể âm)
    yp = [max(0.0, float(p)) for p in _to_float_list(y_pred)]

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
        "wape": round(wape, 2),
        "r2": round(r2, 4),
        "bias": round(bias, 2),
    }
