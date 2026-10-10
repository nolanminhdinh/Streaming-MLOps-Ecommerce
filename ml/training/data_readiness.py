"""Training-data readiness gate and supervisor notification helpers."""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Optional

import pandas as pd

logger = logging.getLogger("ml.training.data_readiness")

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_TEST_DAYS = 14
DEFAULT_VALIDATION_DAYS = 7
DEFAULT_MIN_TRAIN_DAYS = 14
DEFAULT_MAX_LAG_DAYS = 28


class InsufficientTrainingDataError(RuntimeError):
    """Training is deliberately stopped because history cannot support CV."""

    def __init__(self, report: dict[str, Any]):
        self.report = report
        missing = int(report.get("missing_history_days", 0))
        super().__init__(
            "Insufficient historical data for model training: "
            f"{report.get('available_history_days', 0)} of "
            f"{report.get('required_history_days', 0)} calendar days available; "
            f"load at least {missing} more days."
        )


def required_history_days(
    n_splits: int = 3,
    test_days: int = DEFAULT_TEST_DAYS,
    validation_days: int = DEFAULT_VALIDATION_DAYS,
    min_train_days: int = DEFAULT_MIN_TRAIN_DAYS,
    max_lag_days: int = DEFAULT_MAX_LAG_DAYS,
) -> tuple[int, int]:
    """Return (minimum evaluation dates, minimum source calendar coverage)."""
    if min(n_splits, test_days, validation_days, min_train_days) < 1 or max_lag_days < 0:
        raise ValueError("Training history window values must be positive.")
    evaluation_days = n_splits * test_days + validation_days + min_train_days
    # Feature engineering drops max_lag rows for lag warm-up and one final row
    # because the target is t+1.
    source_days = evaluation_days + max_lag_days + 1
    return evaluation_days, source_days


def _date_series(values: Iterable[Any]) -> pd.Series:
    dates = pd.to_datetime(pd.Series(values), errors="coerce", utc=True)
    return dates.dt.tz_convert(None).dt.normalize().dropna()


def _base_report(
    *,
    available_history_days: int,
    required_days: int,
    evaluation_days_available: int,
    evaluation_days_required: int,
    n_splits: int,
    test_days: int,
    validation_days: int,
    min_train_days: int,
    max_lag_days: int,
    data_origin: str,
    source_type: str,
    active_source_days: int,
    row_count: int,
    sku_count: int,
    history_start: Optional[str],
    history_end: Optional[str],
) -> dict[str, Any]:
    missing_days = max(0, required_days - available_history_days)
    sufficient = missing_days == 0 and evaluation_days_available >= evaluation_days_required
    if sufficient:
        action = "Có thể bắt đầu huấn luyện với cửa sổ dữ liệu hiện tại."
        status = "READY"
    else:
        action = (
            f"Nạp thêm tối thiểu {missing_days} ngày lịch sử hợp lệ vào warehouse, "
            "sau đó chạy lại pipeline huấn luyện. Model đang phục vụ sẽ được giữ nguyên."
        )
        status = "INSUFFICIENT_DATA"
    return {
        "event": "MODEL_TRAINING_DATA_READINESS",
        "status": status,
        "severity": "warning" if not sufficient else "info",
        "data_sufficient": sufficient,
        "data_origin": data_origin,
        "source_type": source_type,
        "available_history_days": int(available_history_days),
        "required_history_days": int(required_days),
        "missing_history_days": int(missing_days),
        "active_source_days": int(active_source_days),
        "evaluation_days_available": int(evaluation_days_available),
        "evaluation_days_required": int(evaluation_days_required),
        "minimum_train_days": int(min_train_days),
        "validation_days": int(validation_days),
        "test_days_per_fold": int(test_days),
        "walk_forward_splits": int(n_splits),
        "max_lag_days": int(max_lag_days),
        "row_count": int(row_count),
        "sku_count": int(sku_count),
        "history_start": history_start,
        "history_end": history_end,
        "action": action,
        "checked_at": datetime.now(timezone.utc).isoformat(),
    }


def assess_order_history(
    orders_df: pd.DataFrame,
    *,
    n_splits: int = 3,
    test_days: int = DEFAULT_TEST_DAYS,
    validation_days: int = DEFAULT_VALIDATION_DAYS,
    min_train_days: int = DEFAULT_MIN_TRAIN_DAYS,
    max_lag_days: int = DEFAULT_MAX_LAG_DAYS,
) -> dict[str, Any]:
    """Assess source-order coverage before feature engineering starts."""
    evaluation_days, source_days = required_history_days(
        n_splits, test_days, validation_days, min_train_days, max_lag_days
    )
    data = orders_df
    if "is_cancelled" in data.columns:
        cancelled = data["is_cancelled"].astype(str).str.strip().str.lower().isin(
            {"true", "1", "yes", "y"}
        )
        data = data.loc[~cancelled]
    elif "order_status" in data.columns:
        cancelled = data["order_status"].astype(str).str.strip().str.upper().eq("CANCELLED")
        data = data.loc[~cancelled]

    date_column = "create_time" if "create_time" in data.columns else "date"
    dates = _date_series(data[date_column]) if date_column in data.columns else pd.Series(dtype="datetime64[ns]")
    distinct = dates.drop_duplicates().sort_values()
    if distinct.empty:
        available_days = 0
        start = end = None
    else:
        available_days = int((distinct.iloc[-1] - distinct.iloc[0]).days + 1)
        start, end = distinct.iloc[0].date().isoformat(), distinct.iloc[-1].date().isoformat()
    evaluation_available = max(0, available_days - max_lag_days - 1)
    return _base_report(
        available_history_days=available_days,
        required_days=source_days,
        evaluation_days_available=evaluation_available,
        evaluation_days_required=evaluation_days,
        n_splits=n_splits,
        test_days=test_days,
        validation_days=validation_days,
        min_train_days=min_train_days,
        max_lag_days=max_lag_days,
        data_origin=str(orders_df.attrs.get("data_origin", "unknown")),
        source_type=str(orders_df.attrs.get("data_source", "unknown")),
        active_source_days=int(len(distinct)),
        row_count=len(data),
        sku_count=int(data["sku"].nunique()) if "sku" in data.columns else 0,
        history_start=start,
        history_end=end,
    )


def assess_feature_history(
    feature_df: pd.DataFrame,
    *,
    data_origin: str,
    source_type: str,
    n_splits: int = 3,
    test_days: int = DEFAULT_TEST_DAYS,
    validation_days: int = DEFAULT_VALIDATION_DAYS,
    min_train_days: int = DEFAULT_MIN_TRAIN_DAYS,
    max_lag_days: int = DEFAULT_MAX_LAG_DAYS,
) -> dict[str, Any]:
    """Assess usable target dates when only an existing Feature Store is available."""
    evaluation_days, source_days = required_history_days(
        n_splits, test_days, validation_days, min_train_days, max_lag_days
    )
    date_column = "target_date" if "target_date" in feature_df.columns else "date"
    dates = _date_series(feature_df[date_column]) if date_column in feature_df.columns else pd.Series(dtype="datetime64[ns]")
    distinct = dates.drop_duplicates().sort_values()
    evaluation_available = len(distinct)
    available_source_days = evaluation_available + max_lag_days + 1
    start = distinct.iloc[0].date().isoformat() if not distinct.empty else None
    end = distinct.iloc[-1].date().isoformat() if not distinct.empty else None
    return _base_report(
        available_history_days=available_source_days,
        required_days=source_days,
        evaluation_days_available=evaluation_available,
        evaluation_days_required=evaluation_days,
        n_splits=n_splits,
        test_days=test_days,
        validation_days=validation_days,
        min_train_days=min_train_days,
        max_lag_days=max_lag_days,
        data_origin=data_origin,
        source_type=source_type,
        active_source_days=evaluation_available,
        row_count=len(feature_df),
        sku_count=int(feature_df["sku"].nunique()) if "sku" in feature_df.columns else 0,
        history_start=start,
        history_end=end,
    )


def _status_path() -> Path:
    configured = os.getenv("TRAINING_STATUS_PATH")
    return Path(configured).expanduser().resolve() if configured else PROJECT_ROOT / "data" / "training_status.json"


def _read_previous_status(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
        return value if isinstance(value, dict) else {}
    except (OSError, ValueError):
        return {}


def _write_status(path: Path, report: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(temporary, path)


def publish_training_status(report: dict[str, Any]) -> dict[str, Any]:
    """Persist status and send a transition/change notification when configured."""
    path = _status_path()
    previous = _read_previous_status(path)
    current = dict(report)
    webhook_url = os.getenv("TRAINING_ALERT_WEBHOOK_URL", "").strip()
    previous_status = previous.get("status")
    previous_notice = previous.get("notification", {}) or {}
    is_insufficient = current.get("status") == "INSUFFICIENT_DATA"
    recovered = previous_status == "INSUFFICIENT_DATA" and bool(current.get("data_sufficient"))
    changed = (
        previous_status != current.get("status")
        or previous.get("missing_history_days") != current.get("missing_history_days")
        or previous.get("available_history_days") != current.get("available_history_days")
    )
    should_notify = bool(webhook_url) and (recovered or (is_insufficient and (changed or not previous_notice.get("delivered"))))
    current["notification"] = {
        "configured": bool(webhook_url),
        "attempted": False,
        "delivered": False,
    }
    if not should_notify:
        current["notification"]["reason"] = (
            "webhook_not_configured" if not webhook_url else "no_status_change"
        )
    _write_status(path, current)

    if should_notify:
        event = "training_data_recovered" if recovered else "training_blocked_insufficient_data"
        missing = int(current.get("missing_history_days", 0))
        message = (
            "Đã nạp đủ lịch sử; pipeline bắt đầu huấn luyện lại."
            if recovered
            else (
                "Pipeline huấn luyện đang bị chặn do thiếu dữ liệu lịch sử: "
                f"hiện có {current.get('available_history_days', 0)}/"
                f"{current.get('required_history_days', 0)} ngày, cần nạp thêm ít nhất {missing} ngày. "
                "Model hiện tại tiếp tục được giữ nguyên."
            )
        )
        payload = {
            "event": event,
            "severity": "info" if recovered else "warning",
            "title": "Dữ liệu lịch sử huấn luyện đã đủ" if recovered else "Thiếu dữ liệu lịch sử để huấn luyện model",
            "text": message,
            "message": message,
            "data_origin": current.get("data_origin"),
            "source_type": current.get("source_type"),
            "available_history_days": current.get("available_history_days"),
            "required_history_days": current.get("required_history_days"),
            "missing_history_days": current.get("missing_history_days"),
            "evaluation_days_available": current.get("evaluation_days_available"),
            "evaluation_days_required": current.get("evaluation_days_required"),
            "history_start": current.get("history_start"),
            "history_end": current.get("history_end"),
            "action": current.get("action"),
            "checked_at": current.get("checked_at"),
        }
        current["notification"]["attempted"] = True
        try:
            import urllib.request

            request = urllib.request.Request(
                webhook_url,
                data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            token = os.getenv("TRAINING_ALERT_WEBHOOK_TOKEN", "").strip()
            if token:
                request.add_header("Authorization", f"Bearer {token}")
            with urllib.request.urlopen(request, timeout=5) as response:
                current["notification"]["delivered"] = 200 <= response.status < 300
                current["notification"]["http_status"] = int(response.status)
            if current["notification"]["delivered"]:
                logger.warning("Training readiness notification delivered to configured webhook.")
            else:
                logger.error("Training readiness webhook returned HTTP %s.", current["notification"]["http_status"])
        except Exception as exc:  # Notification failure must not hide the persisted alert.
            current["notification"]["error_type"] = exc.__class__.__name__
            logger.error("Could not deliver training readiness webhook (%s).", exc.__class__.__name__)
        _write_status(path, current)

    log = logger.warning if current.get("status") == "INSUFFICIENT_DATA" else logger.info
    log(
        "Training readiness status=%s; source coverage=%d/%d days; missing=%d; report=%s",
        current.get("status"), current.get("available_history_days", 0),
        current.get("required_history_days", 0), current.get("missing_history_days", 0), path,
    )
    return current


def require_sufficient_order_history(orders_df: pd.DataFrame, **kwargs: Any) -> dict[str, Any]:
    report = assess_order_history(orders_df, **kwargs)
    if not report["data_sufficient"]:
        report = publish_training_status(report)
        raise InsufficientTrainingDataError(report)
    return report


def require_sufficient_feature_history(feature_df: pd.DataFrame, **kwargs: Any) -> dict[str, Any]:
    report = assess_feature_history(feature_df, **kwargs)
    if not report["data_sufficient"]:
        report = publish_training_status(report)
        raise InsufficientTrainingDataError(report)
    return report
