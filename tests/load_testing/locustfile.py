"""
locustfile.py
-------------
Kịch bản kiểm thử tải (Load Testing) cho hệ thống FastAPI Model Serving:
  - Giả lập hành vi người dùng thực tế gọi các API dự báo và cảnh báo tồn kho.
  - Phân bổ trọng số (Task Weights) theo thực tế vận hành:
      + 50% lưu lượng: POST /predict/demand (Dự báo nhu cầu cho 1 SKU)
      + 30% lưu lượng: GET /inventory/reorder-alert (Quét cảnh báo tồn kho toàn chuỗi)
      + 15% lưu lượng: POST /inventory/reorder-alert (Thẩm định tồn kho tùy biến)
      + 5% lưu lượng:  GET /health & GET /model/metadata (Giám sát hệ thống)

Chạy với lệnh:
  locust -f tests/load_testing/locustfile.py --host http://localhost:8000
"""

from __future__ import annotations

import random
from datetime import date, timedelta

try:
    from locust import HttpUser, between, task
    HAS_LOCUST = True
except ImportError:
    HAS_LOCUST = False
    # Mock lớp HttpUser cho môi trường không cài locust
    class HttpUser:  # type: ignore
        pass
    def task(weight: int = 1):  # type: ignore
        def decorator(f): return f
        return decorator
    def between(a, b): return None  # type: ignore

SKU_LIST = [
    "OL-IP15PM", "ATN-CB-001", "SN-65W-GAN", "OMO-6KG", "TN-TWS-PRO", "KCN-ANESSA",
    "BP-GM-104", "AF-6L-001", "SRM-CERAVE", "GTT-JR-001", "BL-15-CS",
    "NHH-KLA180", "CG-G304", "DG-CT-1M8", "DEN-LED-10", "SR-VC-TO30",
    "KT-KF94-10", "QM-USB-01", "TX-PU-001", "CL-SS-S24"
]


class ECommerceUser(HttpUser):
    wait_time = between(0.05, 0.25) if HAS_LOCUST else None

    @task(50)
    def test_predict_demand(self):
        """Giả lập gọi API dự báo nhu cầu cho 1 SKU ngẫu nhiên trong 7-14 ngày tới."""
        sku = random.choice(SKU_LIST)
        today = date(2026, 10, 1)
        horizon = random.randint(7, 14)
        to_date = today + timedelta(days=horizon)

        payload = {
            "sku": sku,
            "from_date": today.strftime("%Y-%m-%d"),
            "to_date": to_date.strftime("%Y-%m-%d"),
            "confidence_interval": True,
        }
        if hasattr(self, "client"):
            self.client.post("/predict/demand", json=payload, name="/predict/demand")

    @task(30)
    def test_get_inventory_alerts(self):
        """Giả lập nhân viên kho tải danh sách cảnh báo tồn kho toàn hệ thống."""
        if hasattr(self, "client"):
            self.client.get(
                "/inventory/reorder-alert?lead_time_days=3&service_level=0.95",
                name="/inventory/reorder-alert [GET]"
            )

    @task(15)
    def test_custom_inventory_alert(self):
        """Giả lập người dùng kiểm tra tồn kho tùy biến với số lượng thực tế."""
        sampled_skus = random.sample(SKU_LIST, k=random.randint(3, 8))
        custom_stocks = {sku: random.randint(5, 120) for sku in sampled_skus}

        payload = {
            "skus": sampled_skus,
            "current_stocks": custom_stocks,
            "lead_time_days": 3,
            "service_level": 0.95,
        }
        if hasattr(self, "client"):
            self.client.post("/inventory/reorder-alert", json=payload, name="/inventory/reorder-alert [POST]")

    @task(5)
    def test_health_check(self):
        """Giả lập hệ thống giám sát kiểm tra sức khỏe và metadata mô hình."""
        if hasattr(self, "client"):
            self.client.get("/health", name="/health")
            self.client.get("/model/metadata", name="/model/metadata")
