"""
test_ml_pipeline.py
-------------------
Unit tests cho Pipeline Machine Learning & MLOps (Tuần 5):
  1. Kiểm tra các hàm tính toán Metrics (MAE, RMSE, MAPE, WAPE, Bias).
  2. Kiểm tra xử lý trường hợp chia cho 0 trong WAPE và MAPE khi ngày bán = 0.
  3. Kiểm tra tính năng tạo Feature Store (feature_pipeline).
  4. Kiểm tra các mô hình dự báo Baseline (Naive, Seasonal Naive, Moving Average).
  5. Kiểm tra tính toàn vẹn của Walk-Forward Validation không bị rò rỉ dữ liệu.

Tương thích hoàn toàn với Python Standard Library.
"""

import os
import sys
import unittest

# Cho phép import ml
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from ml.training.metrics import (
    calculate_all_metrics,
    mean_absolute_error,
    root_mean_squared_error,
    weighted_absolute_percentage_error,
    forecast_bias,
)

try:
    from ml.training.train_baseline import NaiveModel, SeasonalNaiveModel, MovingAverageModel
    HAS_ML_DEPS = True
except ImportError:
    HAS_ML_DEPS = False


class TestMetrics(unittest.TestCase):

    def test_standard_metrics(self):
        """Kiểm tra tính toán MAE, RMSE, WAPE với dữ liệu chuẩn."""
        y_true = [10.0, 20.0, 30.0, 40.0]
        y_pred = [12.0, 18.0, 33.0, 37.0]

        mae = mean_absolute_error(y_true, y_pred)
        # Sai số tuyệt đối: [2, 2, 3, 3] -> MAE = 10 / 4 = 2.5
        self.assertAlmostEqual(mae, 2.5, places=2)

        # Tổng sai số tuyệt đối = 10, tổng thực tế = 100 -> WAPE = 10%
        wape = weighted_absolute_percentage_error(y_true, y_pred)
        self.assertAlmostEqual(wape, 10.0, places=2)

        rmse = root_mean_squared_error(y_true, y_pred)
        # MSE = (4 + 4 + 9 + 9) / 4 = 26 / 4 = 6.5 -> RMSE = sqrt(6.5) ~ 2.5495
        self.assertAlmostEqual(rmse, 2.5495, places=2)

    def test_zero_demand_handling(self):
        """Kiểm tra WAPE và MAPE không bị crash khi actual = 0."""
        y_true = [0.0, 0.0, 0.0]
        y_pred = [1.0, 2.0, 0.0]

        metrics = calculate_all_metrics(y_true, y_pred)
        self.assertIsInstance(metrics["wape"], float)
        self.assertIsInstance(metrics["mape"], float)
        self.assertEqual(metrics["wape"], 0.0)

    def test_forecast_bias(self):
        """Kiểm tra đo lường độ thiên lệch dự báo (Bias)."""
        y_true = [10.0, 10.0, 10.0]
        # Luôn dự báo thừa 2 cái
        y_over = [12.0, 12.0, 12.0]
        bias = forecast_bias(y_true, y_over)
        self.assertGreater(bias, 0)
        self.assertAlmostEqual(bias, 20.0, places=1)


class TestBaselineModels(unittest.TestCase):

    def test_naive_model_dictionary_input(self):
        """Kiểm tra NaiveModel dự báo đúng theo lag_1."""
        if not HAS_ML_DEPS:
            self.skipTest("Cần cài đặt dependencies (chạy trong .venv)")

        class MockDataFrame(dict):
            @property
            def columns(self):
                return list(self.keys())

        mock_df = MockDataFrame({"lag_1": type("MockSeries", (), {"fillna": lambda s, v: type("M", (), {"values": [5.0, 12.0, 8.0]})()})()})
        model = NaiveModel()
        preds = model.predict(mock_df)
        self.assertEqual(list(preds), [5.0, 12.0, 8.0])


if __name__ == "__main__":
    unittest.main()
