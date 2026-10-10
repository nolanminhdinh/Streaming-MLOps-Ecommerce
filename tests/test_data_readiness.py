"""Kiểm thử cổng readiness và thông tin hướng dẫn nạp lịch sử huấn luyện."""

from __future__ import annotations

import json
import os
import tempfile
import unittest
from datetime import datetime, timedelta
from unittest import mock

import pandas as pd

from ml.training.data_readiness import (
    InsufficientTrainingDataError,
    assess_order_history,
    require_sufficient_order_history,
    required_history_days,
)


def make_orders(days: int, *, cancelled_future_date: bool = False) -> pd.DataFrame:
    start = datetime(2026, 1, 1)
    rows = [
        {"sku": "SKU-1", "create_time": start + timedelta(days=offset), "is_cancelled": False}
        for offset in range(days)
    ]
    if cancelled_future_date:
        rows.append({
            "sku": "SKU-1",
            "create_time": start + timedelta(days=400),
            "is_cancelled": True,
        })
    data = pd.DataFrame(rows)
    data.attrs.update({"data_source": "warehouse", "data_origin": "warehouse_live"})
    return data


class TestTrainingDataReadiness(unittest.TestCase):

    def test_default_and_closed_loop_history_requirements(self):
        self.assertEqual(required_history_days(), (63, 92))
        self.assertEqual(required_history_days(n_splits=2), (49, 78))

    def test_assessment_counts_distinct_calendar_days_and_ignores_cancelled_orders(self):
        report = assess_order_history(make_orders(10, cancelled_future_date=True))

        self.assertEqual(report["status"], "INSUFFICIENT_DATA")
        self.assertFalse(report["data_sufficient"])
        self.assertEqual(report["available_history_days"], 10)
        self.assertEqual(report["required_history_days"], 92)
        self.assertEqual(report["missing_history_days"], 82)
        self.assertIn("Nạp thêm tối thiểu 82 ngày", report["action"])

    def test_insufficient_history_persists_supervisor_status_before_raising(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            status_path = os.path.join(tmp_dir, "training_status.json")
            with mock.patch.dict(os.environ, {
                "TRAINING_STATUS_PATH": status_path,
                "TRAINING_ALERT_WEBHOOK_URL": "",
            }):
                with self.assertRaises(InsufficientTrainingDataError) as raised:
                    require_sufficient_order_history(make_orders(10))

            with open(status_path, encoding="utf-8") as status_file:
                persisted = json.load(status_file)
            self.assertEqual(raised.exception.report["status"], "INSUFFICIENT_DATA")
            self.assertEqual(persisted["missing_history_days"], 82)
            self.assertEqual(persisted["notification"]["reason"], "webhook_not_configured")

    def test_exact_minimum_coverage_is_ready(self):
        report = assess_order_history(make_orders(92))

        self.assertEqual(report["status"], "READY")
        self.assertTrue(report["data_sufficient"])
        self.assertEqual(report["evaluation_days_available"], 63)
        self.assertEqual(report["missing_history_days"], 0)


if __name__ == "__main__":
    unittest.main()
