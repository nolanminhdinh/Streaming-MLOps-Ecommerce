"""
test_load_test.py
-----------------
Unit tests cho Module Kiểm thử Tải & Xuất bản Dữ liệu Power BI (Tuần 9):
  1. Kiểm tra tính toán phân vị độ trễ (percentile P50, P95, P99).
  2. Kiểm tra thực thi tác vụ kiểm thử tải đơn lẻ (_execute_single_request).
  3. Kiểm tra kịch bản kiểm thử tải đa luồng mini-benchmark.
  4. Kiểm tra quy trình xuất khẩu dữ liệu cho Power BI Desktop.

Chạy tương thích hoàn toàn trên Python Standard Library.
"""

from __future__ import annotations

import os
import sys
import unittest

# Cho phép import tests và powerbi
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from tests.load_testing.run_load_test import percentile, LoadTestRunner
from powerbi.export_powerbi_dataset import export_powerbi_data


class TestPercentileCalculations(unittest.TestCase):

    def test_percentile_edge_cases(self):
        """Kiểm tra tính toán phân vị trên các mảng mẫu."""
        self.assertEqual(percentile([], 0.5), 0.0)
        self.assertEqual(percentile([10.0], 0.5), 10.0)

    def test_percentile_known_distribution(self):
        """Kiểm tra P50 (median) và P95 trên dãy số từ 1 đến 100."""
        data = list(range(1, 101))  # 1..100
        p50 = percentile(data, 0.50)
        p95 = percentile(data, 0.95)

        self.assertAlmostEqual(p50, 50.5, delta=1.0)
        self.assertAlmostEqual(p95, 95.0, delta=1.0)


class TestLoadTestRunner(unittest.TestCase):

    def setUp(self):
        self.runner = LoadTestRunner()
        # Unit benchmark must not change behavior when a local API happens to be running.
        self.runner.is_live_server = False

    def test_single_request_execution(self):
        """Kiểm tra thực thi 1 request trả về đúng tuple (success, elapsed_ms, task_type)."""
        ok, elapsed_ms, task_type = self.runner._execute_single_request()
        self.assertTrue(ok)
        self.assertGreater(elapsed_ms, 0.0)
        self.assertIn(task_type, ["predict", "reorder_get", "reorder_post", "health"])

    def test_mini_benchmark_run(self):
        """Kiểm tra chạy mini-benchmark 5 users và 25 requests."""
        res = self.runner.run_benchmark(concurrency_levels=[5], requests_per_level=25)
        self.assertIn("concurrency_results", res)
        self.assertIn("5", res["concurrency_results"])
        stats = res["concurrency_results"]["5"]
        self.assertEqual(stats["total_requests"], 25)
        self.assertEqual(stats["successful_requests"], 25)
        self.assertEqual(stats["error_rate_pct"], 0.0)
        self.assertGreater(stats["throughput_rps"], 0.0)


class TestPowerBIExport(unittest.TestCase):

    def test_export_powerbi_csv_files(self):
        """CSV serialization with warehouse query fixtures, independent of live services."""
        import csv
        import tempfile
        from unittest import mock
        from powerbi.export_powerbi_dataset import DATASETS

        fixture = {filename: [] for filename in DATASETS}
        # CSV-only warehouse is valid: financial tables stay empty.
        fixture["Historical_Demand_Daily.csv"] = [{
            "date_key": 20260702, "order_date": "2026-07-02", "sku": "A", "product_name": "A",
            "platform": "shopee", "units_sold": 7, "data_origin": "public_anonymized_historical_csv", "source_dataset": "file-a",
        }]
        fixture["Forecast_vs_Actual.csv"] = [{
            "sku": "A", "forecast_demand": 5, "actual_demand": None,
            "data_origin": "public_anonymized_historical_csv", "source_datasets": '["file-a"]', "actual_complete": False,
        }]
        with tempfile.TemporaryDirectory() as tmp, \
             mock.patch("sqlalchemy.create_engine"), \
             mock.patch("powerbi.export_powerbi_dataset._install_views"), \
             mock.patch("powerbi.export_powerbi_dataset._query", side_effect=list(fixture.values())):
            out_dir = export_powerbi_data(output_dir=tmp)
            for filename in DATASETS:
                self.assertTrue(os.path.exists(os.path.join(out_dir, filename)))
            with open(os.path.join(out_dir, "Forecast_vs_Actual.csv"), encoding="utf-8-sig", newline="") as file:
                forecast = next(csv.DictReader(file))
            self.assertEqual(forecast["actual_demand"], "")
            self.assertEqual(forecast["data_origin"], "public_anonymized_historical_csv")
            with open(os.path.join(out_dir, "Fact_Orders_Summary.csv"), encoding="utf-8-sig", newline="") as file:
                self.assertEqual(list(csv.DictReader(file)), [])


if __name__ == "__main__":
    unittest.main()
