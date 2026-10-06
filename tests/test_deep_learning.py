"""
test_deep_learning.py
---------------------
Unit tests cho các module Deep Learning & Model Registry (Tuần 6):
  1. Kiểm tra TimeSeriesSequenceDataset tạo đúng cấu trúc tensor 3D cửa sổ trượt (Sliding Windows).
  2. Kiểm tra DeepLearningTrainer đảm bảo dự báo không âm (Demand >= 0).
  3. Kiểm tra tính năng tạo Model Manifest và đăng ký Model Registry.

Chạy tương thích hoàn toàn trên Python Standard Library.
"""

import os
import sys
import unittest
from datetime import datetime

# Cho phép import ml
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
try:
    import numpy as np
    HAS_NUMPY = True
except ImportError:
    HAS_NUMPY = False

from ml.training.deep_learning_models import TimeSeriesSequenceDataset, DeepLearningTrainer
from ml.training.register_model import register_champion_model, REGISTERED_MODEL_NAME


class TestDeepLearningData(unittest.TestCase):

    def test_sliding_window_dimensions(self):
        """Kiểm tra kích thước các tensor 3D được sinh ra bởi TimeSeriesSequenceDataset."""
        if not HAS_NUMPY:
            self.skipTest("Cần numpy để chạy test tensor sliding windows")

        # Giả lập chuỗi thời gian 30 ngày với 5 đặc trưng
        n_samples = 30
        n_features = 5
        seq_len = 7

        features = np.random.randn(n_samples, n_features)
        targets = np.random.randint(1, 20, size=n_samples)

        dataset = TimeSeriesSequenceDataset(features, targets, seq_len=seq_len)

        # Số lượng cửa sổ kỳ vọng = 30 - 7 = 23 cửa sổ
        expected_windows = n_samples - seq_len
        self.assertEqual(len(dataset), expected_windows)

        # Kiểm tra shape của cửa sổ đầu tiên: (seq_len, n_features)
        X_sample, y_sample = dataset[0]
        if hasattr(X_sample, "shape"):
            self.assertEqual(X_sample.shape, (seq_len, n_features))
            self.assertEqual(y_sample.shape, (1,))


class TestDeepLearningTrainer(unittest.TestCase):

    def test_trainer_initialization(self):
        """Kiểm tra khởi tạo trainer với các tham số mạng."""
        trainer = DeepLearningTrainer(
            model_type="lstm",
            seq_len=14,
            hidden_dim=32,
            num_layers=1,
            max_epochs=10,
        )
        self.assertEqual(trainer.model_type, "lstm")
        self.assertEqual(trainer.seq_len, 14)
        self.assertEqual(trainer.hidden_dim, 32)

    def test_predict_non_negative_constraint(self):
        """Kiểm tra dự báo của trainer luôn đảm bảo >= 0 (không âm)."""
        if not HAS_NUMPY:
            self.skipTest("Cần numpy để chạy test predict")

        trainer = DeepLearningTrainer(model_type="lstm", seq_len=5)
        # Giả lập dữ liệu có cả giá trị âm
        X_test = np.array([[-5.0, -10.0], [2.0, 3.0], [-1.0, 0.0]])
        preds = trainer.predict(X_test)

        self.assertEqual(len(preds), len(X_test))
        # Mọi giá trị dự báo phải >= 0
        self.assertTrue(all(p >= 0.0 for p in preds))


class TestModelRegistry(unittest.TestCase):

    def test_register_model_manifest(self):
        """Kiểm tra đăng ký mô hình và tạo tệp manifest chuẩn cho FastAPI Serving."""
        import json
        import tempfile
        from unittest import mock

        fake_run = {
            "run_id": "local_champion_run", "model_name": "LIGHTGBM",
            "wape": 21.3, "mae": 1.7, "rmse": 2.4,
            "artifact_uri": "data/champion_model.joblib", "model_file": "lightgbm_model.joblib",
        }
        with tempfile.TemporaryDirectory() as tmp_dir, \
                mock.patch("ml.training.register_model.find_best_run", return_value=fake_run):
            manifest = register_champion_model(stage="Staging", manifest_dir=tmp_dir)
            with open(os.path.join(tmp_dir, "model_manifest.json"), encoding="utf-8") as f:
                self.assertEqual(json.load(f)["model_file"], "lightgbm_model.joblib")

        self.assertIsInstance(manifest, dict)
        self.assertEqual(manifest["model_registry_name"], REGISTERED_MODEL_NAME)
        self.assertEqual(manifest["stage"], "Staging")
        self.assertIn("champion_algorithm", manifest)
        self.assertEqual(manifest["status"], "READY_FOR_SERVING")
        self.assertIn("metrics", manifest)
        self.assertEqual(manifest["metrics"]["cv_wape"], 21.3)

    def test_register_without_runs_writes_nothing(self):
        """Không có run nào → không sinh manifest với metrics bịa."""
        import tempfile
        from unittest import mock

        with tempfile.TemporaryDirectory() as tmp_dir, \
                mock.patch("ml.training.register_model.find_best_run", return_value=None):
            manifest = register_champion_model(stage="Staging", manifest_dir=tmp_dir)
            self.assertEqual(manifest, {})
            self.assertFalse(os.path.exists(os.path.join(tmp_dir, "model_manifest.json")))


if __name__ == "__main__":
    unittest.main()
