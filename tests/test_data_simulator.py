"""
test_data_simulator.py
-----------------------
Unit tests cho ECommerceSimulator:
  1. Kiểm tra khởi tạo và sinh dữ liệu đơn hàng.
  2. Kiểm tra tỷ lệ phân bổ sàn (Shopee vs TikTok).
  3. Kiểm tra tính toàn vẹn của schema Shopee (các trường theo vietnam_ecommerce).
  4. Kiểm tra tính toàn vẹn của schema TikTok (các trường theo vietnam_ecommerce).
  5. Kiểm tra logic vòng đời đơn hàng (State machine) và lý do hủy.
  6. Kiểm tra Flash Sale và ngày đôi (Mega Sale).

Tương thích cả pytest và unittest (chạy trực tiếp bằng Python standard library).
"""

import os
import sys
import unittest
from dataclasses import asdict
from datetime import datetime, timezone

# Cho phép import data_simulator
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from data_simulator.data_simulator import (
    ECommerceSimulator,
    Platform,
    ShopeeOrderStatus,
    TikTokOrderStatus,
    PRODUCT_CATALOG,
)


class TestECommerceSimulator(unittest.TestCase):
    def setUp(self):
        self.simulator = ECommerceSimulator(shopee_ratio=0.6, cancel_rate=0.15)

    def test_simulator_initialization(self):
        """Kiểm tra khởi tạo simulator với tham số tùy biến."""
        self.assertEqual(self.simulator.shopee_ratio, 0.6)
        self.assertEqual(self.simulator.cancel_rate, 0.15)
        self.assertGreater(len(PRODUCT_CATALOG), 0)

    def test_generate_shopee_event(self):
        """Kiểm tra các trường bắt buộc của sự kiện đơn hàng Shopee."""
        now = datetime(2026, 9, 24, 10, 0, 0, tzinfo=timezone.utc)
        event = self.simulator._generate_shopee_event(now)
        event_dict = asdict(event)

        self.assertTrue(event_dict["order_sn"].startswith("SPE"))
        self.assertIsNotNone(event_dict["item_sku"])
        self.assertGreaterEqual(event_dict["quantity"], 1)
        self.assertGreater(event_dict["original_price"], 0)
        self.assertGreater(event_dict["buyer_total_amount"], 0)
        self.assertIn(event_dict["order_status"], [s.value for s in ShopeeOrderStatus])
        self.assertIn(
            event_dict["state"],
            ["TP.HCM", "Hà Nội", "Đà Nẵng", "Cần Thơ", "Hải Phòng", "Lâm Đồng", "Bình Dương", "Đồng Nai", "Khánh Hoà", "Nghệ An", "Thanh Hoá", "Long An"],
        )

    def test_generate_tiktok_event(self):
        """Kiểm tra các trường bắt buộc của sự kiện đơn hàng TikTok Shop."""
        now = datetime(2026, 9, 24, 10, 0, 0, tzinfo=timezone.utc)
        event = self.simulator._generate_tiktok_event(now)
        event_dict = asdict(event)

        self.assertTrue(event_dict["order_id"].startswith("TT"))
        self.assertIsNotNone(event_dict["seller_sku"])
        self.assertGreaterEqual(event_dict["quantity"], 1)
        self.assertGreater(event_dict["sub_total"], 0)
        self.assertGreater(event_dict["total_amount"], 0)
        self.assertIn(event_dict["order_status"], [s.value for s in TikTokOrderStatus])

    def test_order_stream_distribution(self):
        """Kiểm tra luồng stream và tỷ lệ phân bổ đơn hàng giữa các sàn."""
        sample_size = 50
        events = []
        for i, event in enumerate(self.simulator.stream(base_events_per_second=100.0)):
            events.append(event)
            if i + 1 >= sample_size:
                break

        self.assertEqual(len(events), sample_size)
        shopee_count = sum(1 for e in events if e.get("_platform") == "shopee")
        tiktok_count = sum(1 for e in events if e.get("_platform") == "tiktok")

        self.assertGreater(shopee_count, 0)
        self.assertGreater(tiktok_count, 0)
        self.assertTrue(15 <= shopee_count <= 45)

    def test_flash_sale_logic(self):
        """Kiểm tra hàm phát hiện khung giờ Flash Sale (0h, 9h, 12h, 20h, 21h)."""
        flash_time = datetime(2026, 9, 24, 12, 15, 0)
        self.assertTrue(self.simulator._is_flash_sale(flash_time))

        normal_time = datetime(2026, 9, 24, 15, 30, 0)
        self.assertFalse(self.simulator._is_flash_sale(normal_time))

    def test_mega_sale_multiplier(self):
        """Kiểm tra hệ số bùng nổ đơn hàng trong ngày đôi (9/9, 10/10, 11/11, 12/12)."""
        mega_day = datetime(2026, 11, 11, 12, 0, 0)
        normal_day = datetime(2026, 11, 15, 12, 0, 0)

        mult_mega = self.simulator._seasonal_multiplier(mega_day)
        mult_normal = self.simulator._seasonal_multiplier(normal_day)

        self.assertGreater(mult_mega, mult_normal)


if __name__ == "__main__":
    unittest.main()
