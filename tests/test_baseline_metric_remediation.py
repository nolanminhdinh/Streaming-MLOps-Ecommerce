"""Forecast-origin regressions and undefined percentage-metric contracts."""
from datetime import date
import json
import sys
import types
from unittest import mock

import joblib
import numpy as np
import pandas as pd
import pytest

from ml.features.time_series_features import TimeSeriesFeatureExtractor
from ml.training.metrics import (
    EVALUATION_PROTOCOL, aggregate_fold_metrics, calculate_all_metrics,
    forecast_bias, weighted_absolute_percentage_error,
)
from ml.training.recursive_evaluation import forecast_fold_recursively
from serving.app.baseline_models import MovingAverageModel, NaiveModel, SeasonalNaiveModel
from serving.app.features import build_feature_row


def daily_series():
    return pd.DataFrame({"sku": "A", "date": pd.date_range("2026-01-01", periods=60).date,
                         "daily_demand": np.arange(1, 61, dtype=float),
                         "daily_revenue": 0., "avg_unit_price": 0.,
                         "total_discount": 0., "order_count": 1.})


@pytest.mark.parametrize("cls,expected", [(NaiveModel, 41), (SeasonalNaiveModel, 35),
                                         (MovingAverageModel, 38)])
def test_forecast_next_day_uses_correct_observed_dates(cls, expected):
    daily = daily_series()
    features = TimeSeriesFeatureExtractor().extract_features(daily)
    row = features.iloc[[40]]  # origin Feb 10; target Feb 11
    assert cls().predict(row)[0] == pytest.approx(expected)
    history = daily.iloc[:41].to_dict(orient="records")
    online = pd.DataFrame([build_feature_row(history, date(2026, 2, 10))])
    assert cls().predict(online)[0] == pytest.approx(expected)


@pytest.mark.parametrize("cls,expected", [(NaiveModel, 40), (SeasonalNaiveModel, 34),
                                         (MovingAverageModel, 37)])
def test_unversioned_pickles_keep_historical_predictions(cls, expected, tmp_path):
    legacy = cls()
    del legacy.alignment_version
    path = tmp_path / "legacy.joblib"
    joblib.dump(legacy, path)
    loaded = joblib.load(path)
    row = TimeSeriesFeatureExtractor().extract_features(daily_series()).iloc[[40]]
    assert loaded.predict(row)[0] == pytest.approx(expected)
    if cls is MovingAverageModel:
        history = daily_series().iloc[:41].to_dict(orient="records")
        assert loaded.predict_from_history(history, list(row.columns)) == pytest.approx(expected)


def test_new_pickle_keeps_correct_alignment_and_fast_path(tmp_path):
    model = MovingAverageModel()
    path = tmp_path / "corrected.joblib"
    joblib.dump(model, path)
    loaded = joblib.load(path)
    history = daily_series().iloc[:41].to_dict(orient="records")
    row = TimeSeriesFeatureExtractor().extract_features(daily_series()).iloc[[40]]
    assert loaded.alignment_version == 2
    assert loaded.predict_from_history(history, list(row.columns)) == pytest.approx(38)
    assert loaded.predict(row)[0] == pytest.approx(38)


def test_corrected_seasonal_rejects_stale_feature_contract():
    with pytest.raises(ValueError, match="lag_6"):
        SeasonalNaiveModel().predict(pd.DataFrame({"lag_7": [3]}))


def test_recursive_predictions_never_use_future_actuals():
    daily = daily_series()
    features = TimeSeriesFeatureExtractor().extract_features(daily)
    features["target_date"] = pd.to_datetime(features["date"]) + pd.Timedelta(days=1)
    columns = ["daily_demand", "lag_6", "lag_7", "rolling_mean_7"]
    test = features.iloc[40:47].copy()
    _, _, before = forecast_fold_recursively(NaiveModel(), features, test, columns,
                                              "target_t_plus_1", daily_history=daily)
    poisoned = daily.copy()
    poisoned.loc[41:, "daily_demand"] = 1_000_000
    _, _, after = forecast_fold_recursively(NaiveModel(), features, test, columns,
                                             "target_t_plus_1", daily_history=poisoned)
    np.testing.assert_array_equal(before, [41.] * 7)
    np.testing.assert_array_equal(before, after)


@pytest.mark.parametrize("prediction", [[0, 0], [4, 4]])
def test_zero_actual_percentage_metrics_are_json_null(prediction):
    metrics = calculate_all_metrics([0, 0], prediction)
    assert metrics["wape"] is None and metrics["bias"] is None
    assert metrics["mae"] == prediction[0]
    assert json.loads(json.dumps(metrics, allow_nan=False))["wape"] is None


def test_pooled_wape_includes_errors_from_zero_actual_fold():
    folds = [calculate_all_metrics([0], [4]), calculate_all_metrics([10], [10])]
    combined = aggregate_fold_metrics(folds, [0, 10], [4, 10])
    assert combined["wape"] == 40
    assert combined["mean_fold_wape"] == 0  # explicitly diagnostic, not score for selection
    assert combined["defined_wape_folds"] == 1
    assert combined["undefined_wape_folds"] == 1


def test_all_zero_folds_remain_undefined_for_model_selection():
    metrics = aggregate_fold_metrics([calculate_all_metrics([0], [4])], [0], [4])
    assert metrics["wape"] is None and metrics["mean_fold_wape"] is None
    assert metrics["undefined_wape_folds"] == 1 and metrics["mae"] == 4


@pytest.mark.parametrize("actual,prediction", [([], []), ([1], [1, 2]),
    ([float("nan")], [0]), ([1], [float("inf")]), ([-1], [0])])
def test_invalid_metric_inputs_raise_instead_of_looking_perfect(actual, prediction):
    with pytest.raises(ValueError):
        calculate_all_metrics(actual, prediction)


def test_zero_prediction_diagnostic_and_standard_metrics():
    assert weighted_absolute_percentage_error([0, 2, 8], [0, 0, 0]) == 100
    assert forecast_bias([0, 2, 8], [0, 0, 0]) == -100
    assert calculate_all_metrics([10, 20], [12, 18])["wape"] == pytest.approx(13.33)


def test_registry_excludes_legacy_protocol_before_downloading_artifacts():
    from ml.training.register_model import find_best_run
    legacy = pd.DataFrame({"params.evaluation_strategy": ["recursive_multi_step"],
                           "metrics.cv_wape": [1.], "run_id": ["legacy"]})
    mlflow = types.ModuleType("mlflow")
    mlflow.get_experiment_by_name = lambda name: mock.Mock(experiment_id="1")
    mlflow.search_runs = lambda **kwargs: legacy
    tracking = types.ModuleType("mlflow.tracking")
    tracking.MlflowClient = mock.Mock()
    with mock.patch.dict(sys.modules, {"mlflow": mlflow, "mlflow.tracking": tracking}):
        assert find_best_run("test") is None
        tracking.MlflowClient.assert_not_called()
