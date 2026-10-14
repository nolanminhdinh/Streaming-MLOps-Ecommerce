"""Baseline estimators shared by training and model serving.

Keeping these classes in an importable module gives joblib artifacts a stable
qualified class path instead of serializing them as ``__main__``.
"""

from __future__ import annotations

import numpy as np
import pandas as pd


class NaiveModel:
    """Forecast tomorrow with the latest observed daily demand."""

    def __init__(self, lag_col: str = "daily_demand"):
        self.lag_col = lag_col

    def fit(self, X, y):
        return self

    def predict(self, X: pd.DataFrame) -> np.ndarray:
        if "lag_1" in X.columns:
            return X["lag_1"].fillna(0).values
        if self.lag_col in X.columns:
            return X[self.lag_col].fillna(0).values
        return np.zeros(len(X))


class SeasonalNaiveModel:
    """Forecast with demand from the same weekday in the previous week."""

    def fit(self, X, y):
        return self

    def predict(self, X: pd.DataFrame) -> np.ndarray:
        if "lag_7" in X.columns:
            return X["lag_7"].fillna(0).values
        return np.zeros(len(X))


class MovingAverageModel:
    """Forecast with the mean demand over the latest seven days."""

    def fit(self, X, y):
        return self

    def predict(self, X: pd.DataFrame) -> np.ndarray:
        if "rolling_mean_7" in X.columns:
            return X["rolling_mean_7"].fillna(0).values
        return np.zeros(len(X))

    def predict_from_history(self, history, feature_columns):
        """Same shifted rolling_mean_7 as training, without a one-row DataFrame."""
        if "rolling_mean_7" not in feature_columns:
            return 0.0
        window = history[:-1][-7:]
        if not window:
            return 0.0
        value = sum(float(row.get("daily_demand", 0.0) or 0.0) for row in window) / len(window)
        return 0.0 if np.isnan(value) else value
