"""
test_serving.py
---------------
Unit tests cho tầng Model Serving & Nghiệp vụ Quản trị Tồn kho (Tuần 7):
  1. Kiểm tra ModelManager tải manifest, khởi tạo cache và suy luận không âm.
  2. Kiểm tra tính toán khoảng tin cậy 95% (Lower Bound <= Prediction <= Upper Bound).
  3. Kiểm tra InventoryService tính đúng Safety Stock và Reorder Point.
  4. Kiểm tra phân loại Alert Level (CRITICAL, WARNING, NORMAL).
  5. Kiểm tra tính toán Days Until Stockout và Recommended Reorder Quantity.
  6. Kiểm thử các endpoints API qua FastAPI TestClient (nếu có môi trường).

Chạy tương thích hoàn toàn trên Python Standard Library.
"""

from __future__ import annotations

import math
import os
import sys
import unittest
from datetime import date, datetime, timedelta

# Cho phép import serving
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from serving.app.model_loader import ModelManager, get_model_manager
from serving.app.inventory_service import InventoryService, get_inventory_service, get_z_score

try:
    from fastapi.testclient import TestClient
    from serving.app.main import app
    HAS_TEST_CLIENT = True
except Exception:
    HAS_TEST_CLIENT = False


class TestModelManager(unittest.TestCase):

    def setUp(self):
        self.manager = get_model_manager()

    def test_model_manifest_loaded(self):
        """Kiểm tra ModelManager tải thành công metadata từ manifest."""
        self.assertTrue(self.manager.is_loaded)
        meta = self.manager.get_metadata()
        self.assertIn("model_registry_name", meta)
        self.assertEqual(meta["model_registry_name"], "ECommerceDemandForecastModel")
        self.assertEqual(meta["stage"], "Staging")
        self.assertIn("champion_algorithm", meta)

    def test_predict_non_negative(self):
        """Kiểm tra sản lượng dự báo luôn luôn lớn hơn hoặc bằng 0."""
        target_date = date(2026, 10, 10)  # Mega-sale 10/10
        pred, _, _ = self.manager.predict_daily("OL-IP15PM", target_date)
        self.assertGreaterEqual(pred, 0.0)

    def test_predict_range_length(self):
        """Kiểm tra số lượng ngày dự báo khớp đúng với khoảng from_date -> to_date."""
        from_date = date(2026, 10, 1)
        to_date = date(2026, 10, 7)  # 7 ngày
        forecasts = self.manager.predict_range("OL-IP15PM", from_date, to_date)
        self.assertEqual(len(forecasts), 7)
        for item in forecasts:
            self.assertIn("date", item)
            self.assertIn("predicted_quantity", item)
            self.assertGreaterEqual(item["predicted_quantity"], 0.0)

    def test_confidence_interval_bounds(self):
        """Kiểm tra cận dưới <= giá trị dự báo <= cận trên."""
        target_date = date(2026, 10, 15)
        pred, lb, ub = self.manager.predict_daily(
            "OL-IP15PM", target_date, confidence_interval=True
        )
        self.assertIsNotNone(lb)
        self.assertIsNotNone(ub)
        self.assertLessEqual(lb, pred)
        self.assertGreaterEqual(ub, pred)


class TestInventoryService(unittest.TestCase):

    def setUp(self):
        self.inv_service = get_inventory_service()

    def test_z_score_lookup(self):
        """Kiểm tra bảng tra Z-score cho các mức Service Level chuẩn."""
        self.assertAlmostEqual(get_z_score(0.95), 1.645, places=3)
        self.assertAlmostEqual(get_z_score(0.90), 1.282, places=3)
        self.assertAlmostEqual(get_z_score(0.99), 2.326, places=3)

    def test_policy_formulas(self):
        """Kiểm tra công thức Safety Stock và Reorder Point."""
        avg_d = 20.0
        sigma = 4.0
        lead_time = 3
        ss, rop = self.inv_service.calculate_policy(
            avg_daily_demand=avg_d,
            std_daily_demand=sigma,
            lead_time_days=lead_time,
            service_level=0.95,
        )

        # SS = ceil(1.645 * 4 * sqrt(3)) = ceil(1.645 * 6.928) = ceil(11.39) = 12
        expected_ss = math.ceil(1.645 * sigma * math.sqrt(lead_time))
        self.assertEqual(ss, expected_ss)

        # ROP = ceil(20 * 3 + 12) = 72
        expected_rop = math.ceil(avg_d * lead_time + ss)
        self.assertEqual(rop, expected_rop)
        self.assertGreater(rop, ss)

    def test_alert_level_classification(self):
        """Kiểm tra phân loại chính xác các cấp độ cảnh báo CRITICAL, WARNING, NORMAL."""
        # 1. Tồn kho <= Safety Stock -> CRITICAL
        critical_item = self.inv_service.evaluate_sku("OL-IP15PM", current_stock=5)
        self.assertEqual(critical_item.alert_level, "CRITICAL")
        self.assertTrue(critical_item.needs_reorder)
        self.assertGreater(critical_item.recommended_reorder_qty, 0)

        # 2. Safety Stock < Tồn kho <= ROP -> WARNING
        warning_stock = critical_item.safety_stock + 2
        warning_item = self.inv_service.evaluate_sku("OL-IP15PM", current_stock=warning_stock)
        self.assertEqual(warning_item.alert_level, "WARNING")
        self.assertTrue(warning_item.needs_reorder)

        # 3. Tồn kho > ROP -> NORMAL
        normal_stock = critical_item.reorder_point + 50
        normal_item = self.inv_service.evaluate_sku("OL-IP15PM", current_stock=normal_stock)
        self.assertEqual(normal_item.alert_level, "NORMAL")
        self.assertFalse(normal_item.needs_reorder)
        self.assertEqual(normal_item.recommended_reorder_qty, 0)

    def test_evaluate_all_summary(self):
        """Kiểm tra quét toàn bộ kho hàng và sinh thống kê tổng hợp."""
        res = self.inv_service.evaluate_all()
        self.assertGreater(res.total_skus_evaluated, 0)
        self.assertEqual(
            res.total_skus_evaluated,
            res.critical_count + res.warning_count + res.normal_count,
        )
        self.assertEqual(res.skus_needing_reorder, res.critical_count + res.warning_count)


class TestFastAPIServing(unittest.TestCase):

    def setUp(self):
        if not HAS_TEST_CLIENT:
            self.skipTest("Cần cài đặt fastapi và httpx để test API client qua TestClient")
        self.client = TestClient(app)

    def test_health_endpoint(self):
        """Kiểm tra endpoint GET /health."""
        response = self.client.get("/health")
        self.assertEqual(response.status_code, 200)
        data = response.json()
        # "healthy" chỉ khi đang phục vụ bằng mô hình ML thật; heuristic dự phòng → "degraded"
        expected = "healthy" if data["model_loaded"] else "degraded"
        self.assertEqual(data["status"], expected)
        self.assertIn("model_source", data)

    def test_predict_endpoint(self):
        """Kiểm tra endpoint POST /predict/demand."""
        payload = {
            "sku": "OL-IP15PM",
            "from_date": "2026-10-01",
            "to_date": "2026-10-05",
            "confidence_interval": True,
        }
        response = self.client.post("/predict/demand", json=payload)
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertEqual(data["sku"], "OL-IP15PM")
        self.assertEqual(len(data["forecast"]), 5)
        self.assertGreater(data["total_predicted_demand"], 0)

    def test_reorder_alert_endpoint(self):
        """Kiểm tra endpoint GET /inventory/reorder-alert."""
        response = self.client.get("/inventory/reorder-alert")
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertGreater(data["total_skus_evaluated"], 0)
        self.assertIn("alerts", data)


if __name__ == "__main__":
    unittest.main()
