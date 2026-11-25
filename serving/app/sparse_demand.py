"""Origin-only features and direct forecasts for sparse daily demand.

No synthetic forecast is treated as an observed sale. All horizon features use
the same observed prefix and known future calendar. This module is importable in
the serving image so saved candidates do not depend on a training-script path.
"""
from __future__ import annotations

from datetime import timedelta
import numpy as np
import pandas as pd

FEATURE_NAMES = [
    "last", "lag6", "lag7", "mean7", "mean14", "mean28", "mean56",
    "median28", "std28", "positive_rate7", "positive_rate28", "positive_rate56",
    "positive_size_mean28", "max28", "days_since_positive", "observed_age",
    "positive_count56", "lifetime_positive_rate", "lifetime_mean",
    "target_dow_sin", "target_dow_cos", "target_weekend", "target_day", "horizon",
]


def validate_panel(panel):
    y = np.asarray(panel, dtype=float)
    if y.ndim != 2 or not all(y.shape) or not np.isfinite(y).all() or (y < 0).any():
        raise ValueError("Demand panel must be a nonempty finite nonnegative two-dimensional array.")
    return y


def origin_features(panel, origin_date, horizons):
    """SKU-major, horizon-minor rows, made solely from observed history."""
    y = validate_panel(panel)
    horizons = np.asarray(horizons, dtype=int)
    if len(horizons) == 0 or (horizons < 1).any():
        raise ValueError("Forecast horizons must be positive.")
    n, t = y.shape
    positive = y > 0
    has_sales = positive.any(axis=1)
    first = np.where(has_sales, positive.argmax(axis=1), t)
    last = np.where(has_sales, t - 1 - positive[:, ::-1].argmax(axis=1), -1)
    age = np.maximum(t - first, 1)
    values = [y[:, -1], y[:, -7] if t >= 7 else np.zeros(n),
              y[:, -8] if t >= 8 else np.zeros(n)]
    for w in [7, 14, 28, 56]: values.append(y[:, -w:].mean(axis=1))
    recent = y[:, -28:]
    values.extend([np.median(recent, axis=1), recent.std(axis=1)])
    for w in [7, 28, 56]: values.append(positive[:, -w:].mean(axis=1))
    count = (recent > 0).sum(axis=1)
    values.extend([recent.sum(axis=1) / np.maximum(count, 1), recent.max(axis=1),
                   np.where(has_sales, t - 1 - last, t), np.where(has_sales, age, 0),
                   positive[:, -56:].sum(axis=1), positive.sum(axis=1) / age,
                   y.sum(axis=1) / age])
    base = np.column_stack(values)
    h = len(horizons)
    repeated = np.repeat(base, h, axis=0)
    target = [pd.Timestamp(origin_date) + pd.Timedelta(days=int(k)) for k in horizons]
    dow = np.asarray([x.dayofweek for x in target])
    calendar = np.column_stack([np.sin(2 * np.pi * dow / 7), np.cos(2 * np.pi * dow / 7),
                                dow >= 5, [x.day for x in target], horizons])
    return np.column_stack([repeated, np.tile(calendar, (n, 1))])


def statistical_forecast(panel, horizon, method="moving_average7", alpha=0.1, beta=0.1):
    y = validate_panel(panel)
    if horizon < 1: raise ValueError("horizon must be positive")
    n, t = y.shape
    if method in ["moving_average7", "naive", "seasonal_naive"]:
        history = y.copy(); out = []
        for _ in range(horizon):
            if method == "moving_average7": pred = history[:, -7:].mean(axis=1)
            elif method == "naive": pred = history[:, -1]
            else: pred = history[:, -7] if history.shape[1] >= 7 else np.zeros(n)
            out.append(pred); history = np.column_stack([history, pred])
        return np.column_stack(out)
    if method == "zero": return np.zeros((n, horizon))
    if method == "median28": return np.repeat(np.median(y[:, -28:], axis=1)[:, None], horizon, axis=1)
    if method == "mean28": return np.repeat(y[:, -28:].mean(axis=1)[:, None], horizon, axis=1)
    if method not in ["sba", "tsb"] or not (0 < alpha <= 1 and 0 < beta <= 1):
        raise ValueError("Unknown method or invalid smoothing parameters.")
    level = np.zeros(n); interval = np.ones(n); probability = np.zeros(n)
    initialized = np.zeros(n, dtype=bool); gap = np.zeros(n)
    for k in range(t):
        demand = y[:, k]; positive = demand > 0; gap += 1
        first = positive & ~initialized
        level[first] = demand[first]; interval[first] = gap[first]
        probability[first] = 1 / gap[first]
        existing = initialized.copy()
        if method == "tsb":
            probability[existing] += beta * (positive[existing] - probability[existing])
        update = positive & existing
        level[update] += alpha * (demand[update] - level[update])
        interval[update] += alpha * (gap[update] - interval[update])
        gap[positive] = 0; initialized |= positive
    pred = probability * level if method == "tsb" else (1 - alpha / 2) * level / interval
    # Without new observations the multi-step conditional mean stays at the
    # estimated rate; do not feed fractional forecasts back as observed events.
    return np.repeat(pred[:, None], horizon, axis=1)


class SparseDemandRegressor:
    """Pooled direct 1..14-day histogram boosting with explicit point estimates."""
    def __init__(self, objective="absolute_error", n_estimators=100, learning_rate=0.05, training_window_days=120):
        if objective not in ["absolute_error", "poisson", "hurdle"]:
            raise ValueError("Unsupported sparse-demand objective.")
        self.objective = objective
        self.n_estimators = n_estimators
        self.learning_rate = learning_rate
        self.training_window_days = training_window_days
        self.feature_names = list(FEATURE_NAMES)
        self.max_horizon = 14

    def fit_history(self, panel, dates):
        from sklearn.ensemble import HistGradientBoostingClassifier, HistGradientBoostingRegressor
        y = validate_panel(panel)
        if len(dates) != y.shape[1] or y.shape[1] < 44:
            raise ValueError("Training needs aligned dates and at least 44 observed days.")
        dates = [pd.Timestamp(d).date() for d in dates]
        if any(b - a != timedelta(days=1) for a,b in zip(dates,dates[1:])):
            raise ValueError("Training dates must be contiguous daily observations.")
        X, targets = [], []
        # A label is admitted only if its target day lies in the provided prefix.
        for origin in range(max(28, y.shape[1] - self.training_window_days), y.shape[1] - 1):
            horizons = np.arange(1, min(14, y.shape[1] - 1 - origin) + 1)
            known = (y[:, :origin + 1] > 0).any(axis=1)
            rows = origin_features(y[:, :origin + 1], dates[origin], horizons)
            mask = np.repeat(known, len(horizons))
            X.append(rows[mask]); targets.append(y[:, origin + horizons].reshape(-1)[mask])
        # Own contiguous arrays for the native histogram implementation.
        X = np.ascontiguousarray(np.vstack(X), dtype=np.float32)
        target = np.ascontiguousarray(np.concatenate(targets), dtype=np.float32)
        if not len(target): raise ValueError("No observed training targets are available.")
        params = dict(max_iter=self.n_estimators, learning_rate=self.learning_rate,
                      max_leaf_nodes=15, max_depth=5, min_samples_leaf=40,
                      l2_regularization=5, random_state=42, early_stopping=False)
        if self.objective == "hurdle":
            occurrence = target > 0
            self.constant_probability = float(occurrence.mean()) if len(np.unique(occurrence)) < 2 else None
            self.occurrence_model = None
            if self.constant_probability is None:
                self.occurrence_model = HistGradientBoostingClassifier(**params).fit(X, occurrence)
            self.size_model = None
            if occurrence.any():
                self.size_model = HistGradientBoostingRegressor(loss="absolute_error", **params).fit(X[occurrence], target[occurrence])
        else:
            self.model = HistGradientBoostingRegressor(loss=self.objective, **params).fit(X, target)
        self.training_end = str(pd.Timestamp(dates[-1]).date())
        self.training_rows = len(target)
        return self

    def predict(self, X):
        X = np.asarray(X, dtype=float)
        if X.ndim != 2 or X.shape[1] != len(FEATURE_NAMES) or not np.isfinite(X).all():
            raise ValueError("Unexpected direct sparse-demand feature contract.")
        if self.objective == "hurdle":
            p = (np.full(len(X), self.constant_probability) if self.occurrence_model is None
                 else self.occurrence_model.predict_proba(X)[:, 1])
            size = np.zeros(len(X)) if self.size_model is None else self.size_model.predict(X)
            pred = np.where(p >= .5, size, 0)
        else:
            pred = self.model.predict(X)
        return np.maximum(0, pred)

    def forecast_panel(self, panel, origin_date, horizon=14):
        if not 1 <= horizon <= self.max_horizon: raise ValueError("Direct model supports horizons 1..14.")
        panel = validate_panel(panel)
        X = origin_features(panel, origin_date, np.arange(1, horizon + 1))
        return self.predict(X).reshape(panel.shape[0], horizon)

    def predict_horizon_from_history(self, history, horizon):
        if not history: raise ValueError("Observed daily history is required.")
        if any("date" not in row for row in history): raise ValueError("History dates are required.")
        dates = [pd.Timestamp(row["date"]).date() for row in history]
        if any(b - a != timedelta(days=1) for a, b in zip(dates, dates[1:])):
            raise ValueError("History must contain contiguous daily observations.")
        y = np.asarray([[row["daily_demand"] for row in history]], dtype=float)
        return self.forecast_panel(y, dates[-1], horizon)[0]


class VolumeCalibratedSparseModel:
    """Scale a conditional-mean model using prior out-of-time total-volume errors."""
    def __init__(self, model, factor):
        if not np.isfinite(factor) or factor <= 0:
            raise ValueError("Calibration factor must be positive and finite.")
        self.model = model
        self.factor = float(factor)
        self.max_horizon = model.max_horizon
        self.feature_names = model.feature_names
        self.training_end = model.training_end
        self.point_forecast_type = "conditional_mean_with_prior_volume_calibration"

    def predict(self, X):
        return self.model.predict(X) * self.factor

    def forecast_panel(self, panel, origin_date, horizon=14):
        return self.model.forecast_panel(panel, origin_date, horizon) * self.factor

    def predict_horizon_from_history(self, history, horizon):
        return self.model.predict_horizon_from_history(history, horizon) * self.factor
