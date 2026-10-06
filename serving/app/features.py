"""
features.py — Dựng đặc trưng online cho suy luận (Online Feature Builder)
-----------------------------------------------------------------------
Tái hiện CHÍNH XÁC các đặc trưng mà `ml/features/time_series_features.py` tạo ra lúc
huấn luyện, nhưng chỉ cho 1 dòng (ngày t) thay vì cả chuỗi — đủ nhanh để gọi theo request.

Quy ước giống hệt lúc huấn luyện:
  - Dòng ngày t dự báo nhu cầu ngày t+1 (target_t_plus_1).
  - lag_k            = y[t-k]
  - rolling_*_w      = thống kê trên y[t-w .. t-1] (shift 1, min_periods=1, std ddof=1)
  - ema_s            = EWM(adjust=False) trên y[0 .. t-1]
  - growth_wow       = (lag_1 - lag_7) / (lag_7 + 1)
  - Lịch (day_of_week, ...) mô tả ngày t; target_* mô tả ngày t+1.
  - Giá trị thiếu → 0 (train dùng `.fillna(0)` trước khi fit).

Tính tương đương với pipeline huấn luyện được kiểm chứng trong tests/test_serving.py
(TestOnlineFeatureParity).

Module này cố ý không import gì từ `ml/` để image serving không phải chứa mã huấn luyện.
"""

from __future__ import annotations

import math
from datetime import date, timedelta
from typing import Any, Dict, List, Optional, Sequence

DEFAULT_LAGS = [1, 2, 3, 7, 14, 21, 28]
DEFAULT_ROLLING_WINDOWS = [7, 14, 28]
DEFAULT_EMA_SPANS = [7, 14]


def _std(values: Sequence[float]) -> float:
    n = len(values)
    if n < 2:
        return 0.0  # pandas trả NaN với 1 phần tử, pipeline huấn luyện fillna(0)
    m = sum(values) / n
    return math.sqrt(sum((v - m) ** 2 for v in values) / (n - 1))


def _ema(values: Sequence[float], span: int) -> Optional[float]:
    if not values:
        return None
    alpha = 2.0 / (span + 1.0)
    e = values[0]
    for v in values[1:]:
        e = alpha * v + (1.0 - alpha) * e
    return e


def _calendar(d: date, prefix: str = "") -> Dict[str, int]:
    out = {
        f"{prefix}day_of_week": d.weekday(),
        f"{prefix}is_weekend": int(d.weekday() in (5, 6)),
        f"{prefix}day_of_month": d.day,
        f"{prefix}is_mega_sale": int(d.day == d.month),
    }
    if not prefix:
        out["month"] = d.month
        out["quarter"] = (d.month - 1) // 3 + 1
    return out


def build_feature_row(
    history: List[Dict[str, float]],
    row_date: date,
    profile: Optional[Dict[str, Any]] = None,
    category_mappings: Optional[Dict[str, Dict[str, int]]] = None,
    lags: Sequence[int] = DEFAULT_LAGS,
    rolling_windows: Sequence[int] = DEFAULT_ROLLING_WINDOWS,
    ema_spans: Sequence[int] = DEFAULT_EMA_SPANS,
) -> Dict[str, Any]:
    """
    Dựng vector đặc trưng của ngày t = `row_date` (dùng để dự báo ngày t+1).

    Args:
        history: chuỗi ngày LIÊN TỤC, kết thúc đúng tại ngày t, mỗi phần tử gồm
                 daily_demand, daily_revenue, avg_unit_price, total_discount, order_count.
        row_date: ngày t (phần tử cuối của history).
        profile: hồ sơ SKU từ feature_spec.json (category, abc/xyz/matrix_class, cv, safety_stock).
        category_mappings: từ điển mã hóa từ feature_spec.json.
    """
    if not history:
        raise ValueError("history rỗng — cần ít nhất ngày t để dựng đặc trưng")

    y = [float(h.get("daily_demand", 0.0) or 0.0) for h in history]
    t = len(y) - 1
    current = history[-1]
    past = y[:t]  # y[0 .. t-1] = chuỗi đã shift(1)

    row: Dict[str, Any] = {}
    row.update(_calendar(row_date))
    row.update(_calendar(row_date + timedelta(days=1), prefix="target_"))

    row["daily_demand"] = y[t]
    row["daily_revenue"] = float(current.get("daily_revenue", 0.0) or 0.0)
    row["avg_unit_price"] = float(current.get("avg_unit_price", 0.0) or 0.0)
    row["total_discount"] = float(current.get("total_discount", 0.0) or 0.0)
    row["order_count"] = float(current.get("order_count", 0.0) or 0.0)

    for lag in lags:
        row[f"lag_{lag}"] = y[t - lag] if t - lag >= 0 else None

    for w in rolling_windows:
        window = past[-w:]
        if window:
            row[f"rolling_mean_{w}"] = sum(window) / len(window)
            row[f"rolling_std_{w}"] = _std(window)
            row[f"rolling_max_{w}"] = max(window)
            row[f"rolling_min_{w}"] = min(window)
        else:
            for stat in ("mean", "max", "min"):
                row[f"rolling_{stat}_{w}"] = None
            row[f"rolling_std_{w}"] = 0.0

    for span in ema_spans:
        row[f"ema_{span}"] = _ema(past, span)

    if 1 in lags and 7 in lags:
        l1, l7 = row.get("lag_1"), row.get("lag_7")
        row["growth_wow"] = (l1 - l7) / (l7 + 1.0) if (l1 is not None and l7 is not None) else None

    profile = profile or {}
    row["cv"] = float(profile.get("cv") or 0.0)
    row["safety_stock"] = float(profile.get("safety_stock") or 0.0)
    for cat_col, mapping in (category_mappings or {}).items():
        row[f"{cat_col}_code"] = mapping.get(str(profile.get(cat_col)), -1)

    return row


def to_model_input(row: Dict[str, Any], feature_columns: Sequence[str]) -> List[float]:
    """Sắp cột theo đúng thứ tự huấn luyện, giá trị thiếu → 0 (khớp `.fillna(0)`)."""
    out = []
    for c in feature_columns:
        v = row.get(c)
        out.append(0.0 if v is None or (isinstance(v, float) and math.isnan(v)) else float(v))
    return out
