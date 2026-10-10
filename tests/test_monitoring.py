"""
test_monitoring.py
------------------
Unit tests cho Module Giám sát Hệ thống & Phát hiện Trôi dạt (Tuần 8):
  1. Kiểm tra thuật toán thống kê KS-Test (calculate_ks_2samp) và PSI (calculate_psi).
  2. Kiểm tra bộ phát hiện Data Drift & Concept Drift (DriftDetector).
  3. Kiểm tra cơ chế tự động kích hoạt tái huấn luyện (ClosedLoopRetrainer).
  4. Kiểm tra định dạng đầu ra Prometheus Text Exposition của endpoint /metrics.

Chạy tương thích hoàn toàn trên Python Standard Library.
"""

from __future__ import annotations

import json
import os
import sys
import unittest

# Cho phép import monitoring và serving
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from monitoring.evidently.drift_detector import (
    DriftDetector,
    calculate_ks_2samp,
    calculate_psi,
)
from monitoring.evidently.trigger_retraining import ClosedLoopRetrainer
from serving.app.main import get_metrics


class TestStatisticalDriftFunctions(unittest.TestCase):

    def test_ks_statistic_identical_distributions(self):
        """Hai phân phối giống hệt nhau phải có KS-stat = 0 và p-value = 1."""
        data = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0]
        ks_stat, p_val = calculate_ks_2samp(data, data)
        self.assertEqual(ks_stat, 0.0)
        self.assertEqual(p_val, 1.0)

    def test_ks_statistic_shifted_distributions(self):
        """Hai phân phối lệch xa nhau phải có KS-stat lớn và p-value cực nhỏ."""
        ref = [10.0, 11.0, 12.0, 10.5, 11.5, 12.5, 10.8]
        curr = [50.0, 52.0, 55.0, 51.0, 53.0, 54.0, 52.5]
        ks_stat, p_val = calculate_ks_2samp(ref, curr)
        self.assertGreater(ks_stat, 0.8)
        self.assertLess(p_val, 0.05)

    def test_psi_calculation(self):
        """PSI trên phân phối lệch lớn phải vượt ngưỡng cảnh báo 0.25."""
        ref = [1.0, 2.0, 3.0, 4.0, 5.0] * 10
        curr = [10.0, 20.0, 30.0, 40.0, 50.0] * 10
        psi = calculate_psi(ref, curr)
        self.assertGreater(psi, 0.25)


class TestDriftDetector(unittest.TestCase):

    def setUp(self):
        self.detector = DriftDetector(drift_share_threshold=0.30)

    def test_detect_injected_drift(self):
        """Kiểm tra phát hiện trôi dạt khi có sự cố dịch chuyển phân phối."""
        ref, curr = self.detector.generate_synthetic_drift_data(n_samples=50, inject_drift=True)
        summary = self.detector.detect_drift(ref, curr)

        self.assertTrue(summary["drift_detected"])
        self.assertGreaterEqual(summary["drift_share"], 0.30)
        self.assertGreater(summary["number_of_drifted_features"], 0)

        # Kiểm tra sự tồn tại của các tệp báo cáo
        summary_file = os.path.join(self.detector.reports_dir, "drift_summary.json")
        html_file = os.path.join(self.detector.reports_dir, "data_drift_report.html")
        self.assertTrue(os.path.exists(summary_file))
        self.assertTrue(os.path.exists(html_file))

    def test_detect_no_drift_when_healthy(self):
        """Kiểm tra không phát hiện drift khi dữ liệu nằm trong phân phối ổn định."""
        ref, curr = self.detector.generate_synthetic_drift_data(n_samples=50, inject_drift=False)
        summary = self.detector.detect_drift(ref, curr)

        self.assertFalse(summary["dataset_drift_detected"])
        self.assertLess(summary["drift_share"], 0.30)


class TestClosedLoopRetraining(unittest.TestCase):

    def test_evaluate_and_trigger_flow(self):
        """Kiểm tra quy trình tự động cập nhật Manifest và ghi nhật ký kiểm toán trên tệp tạm."""
        import tempfile
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_manifest = os.path.join(tmp_dir, "test_manifest.json")
            tmp_log = os.path.join(tmp_dir, "test_retraining_log.json")
            with open(tmp_manifest, "w", encoding="utf-8") as f:
                json.dump({"model_registry_name": "TestModel", "version": "1"}, f)

            def fake_register(manifest_dir):
                return {"model_registry_name": "TestModel", "version": "2", "stage": "Staging",
                        "run_id": "registered-run-2", "metrics": {"cv_wape": 20.0}}

            reload_calls = []
            retrainer = ClosedLoopRetrainer(
                summary_path=os.path.join(tmp_dir, "no_summary.json"),
                manifest_path=tmp_manifest,
                retraining_log_path=tmp_log,
                train_fn=lambda: {"model_name": "LIGHTGBM", "cv_wape": 20.0},
                register_fn=fake_register,
                reload_fn=lambda: reload_calls.append(1) or True,
            )
            result = retrainer.evaluate_and_trigger(force_retrain=True)

            self.assertEqual(result["status"], "RETRAINED")
            self.assertEqual(result["new_version"], "2")
            self.assertTrue(os.path.exists(tmp_log))
            self.assertEqual(reload_calls, [1])
            with open(tmp_manifest, encoding="utf-8") as f:
                manifest = json.load(f)
            self.assertEqual(manifest["version"], "2")
            self.assertEqual(manifest["metrics"], {"cv_wape": 20.0})

    def test_failed_training_keeps_current_model(self):
        """Huấn luyện lỗi → FAILED, manifest giữ nguyên, không có metrics bịa."""
        import tempfile
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_manifest = os.path.join(tmp_dir, "m.json")
            original = {"model_registry_name": "TestModel", "version": "3", "metrics": {"cv_wape": 25.0}}
            with open(tmp_manifest, "w", encoding="utf-8") as f:
                json.dump(original, f)

            def broken_train():
                raise RuntimeError("không kết nối được PostgreSQL")

            retrainer = ClosedLoopRetrainer(
                summary_path=os.path.join(tmp_dir, "none.json"),
                manifest_path=tmp_manifest,
                retraining_log_path=os.path.join(tmp_dir, "log.json"),
                train_fn=broken_train,
                register_fn=lambda d: self.fail("không được đăng ký khi train lỗi"),
                reload_fn=lambda: self.fail("không được reload khi train lỗi"),
            )
            result = retrainer.evaluate_and_trigger(force_retrain=True)

            self.assertEqual(result["status"], "FAILED")
            with open(tmp_manifest, encoding="utf-8") as f:
                self.assertEqual(json.load(f), original)

    def test_synthetic_drift_does_not_auto_retrain(self):
        """Drift summary sinh từ dữ liệu giả lập không được tự động kích hoạt retrain."""
        import tempfile
        with tempfile.TemporaryDirectory() as tmp_dir:
            summary = os.path.join(tmp_dir, "drift_summary.json")
            with open(summary, "w", encoding="utf-8") as f:
                json.dump({"drift_detected": True, "drift_share": 0.8, "data_source": "synthetic_demo"}, f)
            retrainer = ClosedLoopRetrainer(
                summary_path=summary,
                manifest_path=os.path.join(tmp_dir, "m.json"),
                retraining_log_path=os.path.join(tmp_dir, "log.json"),
                train_fn=lambda: self.fail("không được train"),
            )
            self.assertEqual(retrainer.evaluate_and_trigger()["status"], "SKIPPED")


class TestFeatureStoreDriftWindows(unittest.TestCase):

    def test_windows_split_by_date(self):
        """Drift thật: Reference = 28 ngày trước, Current = 7 ngày cuối của Feature Store."""
        import tempfile
        try:
            import pandas as pd
        except ImportError:
            self.skipTest("Cần pandas")
        from monitoring.evidently.drift_detector import load_feature_store_windows, run_drift_pipeline

        dates = pd.date_range("2026-07-01", periods=40, freq="D")
        df = pd.DataFrame({
            "sku": ["A"] * 40, "date": dates.date,
            "lag_1": range(40), "daily_demand": [10.0] * 33 + [40.0] * 7,
        })
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = os.path.join(tmp_dir, "features_daily.parquet")
            df.to_parquet(path, index=False)
            ref, curr, meta = load_feature_store_windows(path)
            self.assertEqual(len(curr["daily_demand"]), 7)
            self.assertEqual(len(ref["daily_demand"]), 28)
            self.assertEqual(meta["current_window"], ["2026-08-03", "2026-08-09"])
            self.assertEqual(meta["data_source"], "feature_store")

            summary = run_drift_pipeline(features_path=path, reports_dir=tmp_dir)
            self.assertEqual(summary["data_source"], "feature_store")
            self.assertTrue(summary["target_drift_detected"])


class TestPrometheusMetricsExposition(unittest.TestCase):

    def test_metrics_prometheus_text_format(self):
        """Kiểm tra endpoint /metrics xuất đúng định dạng Prometheus exposition text."""
        res = get_metrics(format=None)
        # Nếu là Response object (FastAPI), lấy body text
        content = res.body.decode("utf-8") if hasattr(res, "body") else str(res)

        self.assertIn("ecommerce_uptime_seconds", content)
        self.assertIn("ecommerce_api_requests_total", content)
        self.assertIn("ecommerce_inventory_alerts_total", content)
        self.assertIn("ecommerce_model_info", content)
        self.assertIn("ecommerce_data_drift_ratio", content)

    def test_metrics_json_format(self):
        """Kiểm tra endpoint /metrics xuất đúng định dạng JSON khi yêu cầu format=json."""
        res = get_metrics(format="json")
        self.assertIsInstance(res, dict)
        self.assertIn("uptime_seconds", res)
        self.assertIn("request_counts", res)
        self.assertIn("inventory_status", res)
        self.assertIn("model_status", res)
        self.assertIn("data_drift", res)


if __name__ == "__main__":
    unittest.main()
