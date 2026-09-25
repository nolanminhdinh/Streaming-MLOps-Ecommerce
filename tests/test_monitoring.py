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

            retrainer = ClosedLoopRetrainer(
                manifest_path=tmp_manifest,
                retraining_log_path=tmp_log,
            )
            result = retrainer.evaluate_and_trigger(force_retrain=True)

            self.assertEqual(result["status"], "RETRAINED")
            self.assertEqual(result["new_version"], "2")
            self.assertTrue(os.path.exists(tmp_log))


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
