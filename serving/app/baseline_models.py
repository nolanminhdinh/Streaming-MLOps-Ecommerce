"""Baseline estimators shared by training and model serving.

Keeping these classes in an importable module gives joblib artifacts a stable
qualified class path instead of serializing them as ``__main__``.
"""

from __future__ import annotations

import numpy as np
import pandas as pd


def _required(X, columns):
    missing = [column for column in columns if column not in X.columns]
    if missing:
        raise ValueError(f"Baseline origin-aligned requires features: {', '.join(missing)}")


class NaiveModel:
    """Forecast tomorrow with the latest observed daily demand."""

    def __init__(self, lag_col: str = "daily_demand", alignment_version: int = 2):
        self.lag_col = lag_col
        self.alignment_version = alignment_version

    def fit(self, X, y):
        return self

    def predict(self, X: pd.DataFrame) -> np.ndarray:
        # Pickled artifacts from before this fix have no alignment_version.
        # Keep their predictions reproducible; only newly constructed v2 models
        # use the corrected forecast-origin convention.
        if getattr(self, "alignment_version", 1) >= 2:
            _required(X, [self.lag_col])
            return X[self.lag_col].fillna(0).values
        if "lag_1" in X.columns:
            return X["lag_1"].fillna(0).values
        if self.lag_col in X.columns:
            return X[self.lag_col].fillna(0).values
        return np.zeros(len(X))


class SeasonalNaiveModel:
    """Forecast with demand from the same weekday in the previous week."""

    def __init__(self, alignment_version: int = 2):
        self.alignment_version = alignment_version

    def fit(self, X, y):
        return self

    def predict(self, X: pd.DataFrame) -> np.ndarray:
        if getattr(self, "alignment_version", 1) >= 2:
            # The feature row is t and the target is t+1. The previous same
            # weekday is (t+1)-7 = t-6, rather than t-7.
            _required(X, ["lag_6"])
            return X["lag_6"].fillna(0).values
        if "lag_7" in X.columns:
            return X["lag_7"].fillna(0).values
        return np.zeros(len(X))


class MovingAverageModel:
    """Forecast with the mean demand over the latest seven days."""

    def __init__(self, alignment_version: int = 2):
        self.alignment_version = alignment_version

    def fit(self, X, y):
        return self

    def predict(self, X: pd.DataFrame) -> np.ndarray:
        if getattr(self, "alignment_version", 1) >= 2:
            _required(X, ["daily_demand", "rolling_mean_7", "lag_7"])
            # Preserve the existing shifted rolling feature contract. Remove
            # y[t-7] and include the latest observed y[t] in the seven-day mean.
            # Training has a >=28-day burn-in, so this is always a full window.
            return (X["rolling_mean_7"] + (X["daily_demand"] - X["lag_7"]) / 7).values
        if "rolling_mean_7" in X.columns:
            return X["rolling_mean_7"].fillna(0).values
        return np.zeros(len(X))

    def predict_from_history(self, history, feature_columns):
        """Match predict() without constructing a one-row DataFrame."""
        if getattr(self, "alignment_version", 1) >= 2:
            missing = set(["daily_demand", "rolling_mean_7", "lag_7"]) - set(feature_columns)
            if missing:
                raise ValueError(f"Missing baseline features: {sorted(missing)}")
        if "rolling_mean_7" not in feature_columns:
            return 0.0
        window = (history[-7:] if getattr(self, "alignment_version", 1) >= 2
                  else history[:-1][-7:])
        if getattr(self, "alignment_version", 1) >= 2 and len(history) < 8:
            raise ValueError("MovingAverageModel requires eight daily observations for feature parity.")
        if not window:
            return 0.0
        value = sum(float(row.get("daily_demand", 0.0) or 0.0) for row in window) / len(window)
        return 0.0 if np.isnan(value) else value
