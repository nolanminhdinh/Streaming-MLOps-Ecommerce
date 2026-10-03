"""
test_data_simulator.py
-----------------------
Unit tests cho ECommerceSimulator (Mô hình 1 Doanh nghiệp & Quản trị Tồn kho song hành):
  1. Kiểm tra khởi tạo simulator với danh mục SKU và tồn kho ban đầu của Doanh nghiệp (Mock Retail Enterprise).
  2. Kiểm tra định danh Doanh nghiệp và các gian hàng đa kênh (MockStore Official Mall & MockStore Official Shop).
  3. Kiểm tra tính toàn vẹn của schema Shopee (các trường theo vietnam_ecommerce).
  4. Kiểm tra tính toàn vẹn của schema TikTok (các trường theo vietnam_ecommerce).
  5. Kiểm tra cơ chế trừ tồn kho khi phát sinh đơn hàng (Xuất bán / Outbound Sale).
  6. Kiểm tra xử lý hết hàng (Out of Stock) dẫn đến từ chối/hủy đơn hàng.
  7. Kiểm tra cơ chế tự động kích hoạt đề xuất nhập hàng (Reorder Point) và nhập kho (Restock) sau Lead Time.
  8. Kiểm tra phân cấp cảnh báo tồn kho 3 mức (CRITICAL, WARNING, NORMAL) cho báo cáo quản trị.
  9. Kiểm tra chu kỳ mua sắm thực tế: Flash sale, Mega-sale, Ngày lĩnh lương (Payday), và giờ Livestream TikTok.
  10. Kiểm tra phân phối chọn sản phẩm theo luật Pareto 80/20 (Zipf popularity weights).
  11. Kiểm tra luồng hoàn hàng và nhập lại kho (Reverse Logistics & Return Restock).
  12. Kiểm tra hoàn kho tức thì khi đơn huỷ trước khi giao hàng (Cancellation Release).

Tương thích cả pytest và unittest.
"""

import os
import sys
import unittest
from dataclasses import asdict
from datetime import datetime, timedelta, timezone

# Cho phép import data_simulator
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from data_simulator.data_simulator import (
    ECommerceSimulator,
    Platform,
    ShopeeOrderStatus,
    TikTokOrderStatus,
    PRODUCT_CATALOG,
    ENTERPRISE_CONFIG,
)


class TestECommerceSimulator(unittest.TestCase):
    def setUp(self):
        self.simulator = ECommerceSimulator(shopee_ratio=0.6, cancel_rate=0.15)

    def test_simulator_initialization(self):
        """Kiểm tra khởi tạo simulator với tham số Doanh nghiệp và kho ban đầu."""
        self.assertEqual(self.simulator.shopee_ratio, 0.6)
        self.assertEqual(self.simulator.cancel_rate, 0.15)
        self.assertEqual(self.simulator.enterprise["enterprise_id"], "MOCK-CORP-VN")
        self.assertEqual(len(self.simulator.inventory), len(PRODUCT_CATALOG))
        for p in PRODUCT_CATALOG:
            sku = p["sku"]
            self.assertIn(sku, self.simulator.inventory)
            self.assertGreater(self.simulator.inventory[sku].stock_on_hand, 0)
            self.assertGreater(self.simulator.inventory[sku].safety_stock, 0)
            self.assertGreater(self.simulator.inventory[sku].reorder_point, self.simulator.inventory[sku].safety_stock)

    def test_enterprise_storefront_identity(self):
        """Kiểm tra thông tin gian hàng chính hãng Doanh nghiệp trên Shopee và TikTok."""
        now = datetime(2026, 9, 24, 10, 0, 0, tzinfo=timezone.utc)

        # Shopee
        sp_event = self.simulator._generate_shopee_event(now)
        self.assertEqual(sp_event.shop_name, "MockStore Official Mall")
        self.assertEqual(sp_event.shop_id, "10000001")
        self.assertEqual(sp_event.fulfillment_flag, "fulfilled_by_seller")

        # TikTok
        tt_event = self.simulator._generate_tiktok_event(now)
        self.assertEqual(tt_event.shop_name, "MockStore Official Shop")
        self.assertEqual(tt_event.fulfillment_type, "Seller Fulfillment")
        if tt_event.warehouse_id:
            self.assertEqual(tt_event.warehouse_id, "WH-MOCK-CENTRAL")

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

    def test_inventory_decrement_on_order(self):
        """Kiểm tra tồn kho bị khấu trừ chính xác khi phát sinh đơn xuất bán."""
        test_sku = "OL-IP15PM"
        initial_stock = self.simulator.inventory[test_sku].stock_on_hand
        sold_qty = 5
        now = datetime(2026, 9, 24, 10, 0, 0, tzinfo=timezone.utc)

        ok, stock_left = self.simulator._deduct_inventory(test_sku, sold_qty, "TEST-ORD-01", now)
        self.assertTrue(ok)
        self.assertEqual(stock_left, initial_stock - sold_qty)
        self.assertEqual(self.simulator.inventory[test_sku].stock_on_hand, initial_stock - sold_qty)
        self.assertEqual(self.simulator.inventory[test_sku].total_sold, sold_qty)

    def test_inventory_out_of_stock_triggers_cancellation(self):
        """Kiểm tra khi hết hàng trong kho, đơn đặt hàng bị từ chối/hủy với lý do Hết hàng."""
        test_sku = "BP-GM-104"
        # Đưa tồn kho về 0
        self.simulator.inventory[test_sku].stock_on_hand = 0
        now = datetime(2026, 9, 24, 10, 0, 0, tzinfo=timezone.utc)

        # Thử trừ kho khi stock = 0
        ok, stock_left = self.simulator._deduct_inventory(test_sku, 1, "TEST-ORD-OOS", now)
        self.assertFalse(ok)
        self.assertEqual(stock_left, 0)
        self.assertGreater(self.simulator.inventory[test_sku].out_of_stock_count, 0)

    def test_inventory_reorder_trigger_and_restock(self):
        """Kiểm tra kích hoạt đề xuất nhập hàng (ROP) và nhập hàng bổ sung sau Lead Time."""
        test_sku = "SN-65W-GAN"
        inv = self.simulator.inventory[test_sku]
        rop = inv.reorder_point
        reorder_qty = inv.reorder_qty
        lead_time = inv.lead_time_days

        # Hạ tồn kho xuống đúng ngưỡng Reorder Point
        inv.stock_on_hand = rop + 2
        now = datetime(2026, 9, 24, 10, 0, 0, tzinfo=timezone.utc)

        # Bán 3 sản phẩm -> tồn kho rơi xuống dưới ROP
        ok, stock_left = self.simulator._deduct_inventory(test_sku, 3, "TEST-ORD-ROP", now)
        self.assertTrue(ok)
        self.assertLessEqual(inv.stock_on_hand, rop)
        self.assertEqual(inv.incoming_stock, reorder_qty)
        self.assertIsNotNone(inv.restock_eta)

        # Thời gian trôi qua chưa đủ Lead Time -> Hàng chưa về
        too_early = now + timedelta(days=lead_time - 1)
        self.simulator._process_incoming_restocks(too_early)
        self.assertEqual(inv.incoming_stock, reorder_qty)

        # Thời gian đến đúng hạn giao hàng (sau Lead Time) -> Hàng được nhập kho thành công
        arrival_time = now + timedelta(days=lead_time + 1)
        self.simulator._process_incoming_restocks(arrival_time)
        self.assertEqual(inv.incoming_stock, 0)
        self.assertIsNone(inv.restock_eta)
        self.assertEqual(inv.stock_on_hand, stock_left + reorder_qty)
        self.assertEqual(inv.total_restocked, reorder_qty)

    def test_inventory_alert_levels(self):
        """Kiểm tra phân loại mức cảnh báo tồn kho: CRITICAL, WARNING, NORMAL."""
        test_sku = "OMO-6KG"
        inv = self.simulator.inventory[test_sku]
        ss = inv.safety_stock
        rop = inv.reorder_point

        # 1. Trạng thái CRITICAL: tồn kho <= Safety Stock
        inv.stock_on_hand = ss - 5
        snapshot = {row["sku"]: row for row in self.simulator.get_inventory_snapshot()}
        self.assertEqual(snapshot[test_sku]["alert_level"], "CRITICAL")
        self.assertTrue(snapshot[test_sku]["needs_reorder"])
        self.assertGreater(snapshot[test_sku]["recommended_reorder_qty"], 0)

        # 2. Trạng thái WARNING: Safety Stock < tồn kho <= Reorder Point
        inv.stock_on_hand = ss + 5
        snapshot = {row["sku"]: row for row in self.simulator.get_inventory_snapshot()}
        self.assertEqual(snapshot[test_sku]["alert_level"], "WARNING")
        self.assertTrue(snapshot[test_sku]["needs_reorder"])

        # 3. Trạng thái NORMAL: tồn kho > Reorder Point
        inv.stock_on_hand = rop + 50
        snapshot = {row["sku"]: row for row in self.simulator.get_inventory_snapshot()}
        self.assertEqual(snapshot[test_sku]["alert_level"], "NORMAL")
        self.assertFalse(snapshot[test_sku]["needs_reorder"])

        # 4. Kiểm tra danh sách lọc get_inventory_alerts()
        inv.stock_on_hand = ss - 1  # Đặt lại thành CRITICAL
        alerts = self.simulator.get_inventory_alerts()
        alert_skus = [a["sku"] for a in alerts]
        self.assertIn(test_sku, alert_skus)

    def test_manual_restock(self):
        """Kiểm tra hàm nhập hàng thủ công (chủ động restock theo lệnh)."""
        test_sku = "KCN-ANESSA"
        initial_stock = self.simulator.inventory[test_sku].stock_on_hand
        self.simulator.restock_sku(test_sku, quantity=100, immediate=True)
        self.assertEqual(self.simulator.inventory[test_sku].stock_on_hand, initial_stock + 100)

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
        normal_day = datetime(2026, 11, 10, 12, 0, 0)

        mult_mega = self.simulator._seasonal_multiplier(mega_day)
        mult_normal = self.simulator._seasonal_multiplier(normal_day)

        self.assertGreater(mult_mega, mult_normal)

    def test_payday_seasonality(self):
        """Kiểm tra cơ chế tăng trưởng ngày lĩnh lương (ngày 15 và 25-28 hàng tháng)."""
        payday_15 = datetime(2026, 10, 15, 10, 0, 0)
        payday_25 = datetime(2026, 10, 25, 10, 0, 0)
        regular_day = datetime(2026, 10, 14, 10, 0, 0)  # Thứ 4, không phải ngày đôi 10/10

        self.assertTrue(self.simulator._is_payday(payday_15))
        self.assertTrue(self.simulator._is_payday(payday_25))
        self.assertFalse(self.simulator._is_payday(regular_day))

        self.assertEqual(self.simulator._payday_multiplier(payday_15), 1.8)
        self.assertEqual(self.simulator._payday_multiplier(payday_25), 1.6)
        self.assertEqual(self.simulator._payday_multiplier(regular_day), 1.0)

        # Seasonal multiplier vào ngày lĩnh lương cao hơn ngày thường (cùng khung giờ, cùng ngày trong tuần)
        mult_payday = self.simulator._seasonal_multiplier(payday_15)
        mult_regular = self.simulator._seasonal_multiplier(regular_day)
        self.assertGreater(mult_payday, mult_regular)

    def test_pareto_product_distribution(self):
        """Kiểm tra hành vi khách hàng chọn sản phẩm theo trọng số phổ biến (Pareto 80/20)."""
        samples = [self.simulator._pick_product()["sku"] for _ in range(500)]
        top_hero_sku = "KT-KF94-10"   # Trọng số 15.0 (khẩu trang bán cực chạy)
        niche_sku = "DG-CT-1M8"       # Trọng số 1.5 (bộ drap giường cồng kềnh)

        count_hero = samples.count(top_hero_sku)
        count_niche = samples.count(niche_sku)

        # SKU bán chạy phải có tần suất xuất hiện cao hơn đáng kể so với SKU kén khách
        self.assertGreater(count_hero, count_niche)

    def test_reverse_logistics_return_restock(self):
        """Kiểm tra quy trình hoàn hàng và tái nhập kho (Reverse Logistics Restocking)."""
        test_sku = "OL-IP15PM"
        inv = self.simulator.inventory[test_sku]
        stock_start = inv.stock_on_hand
        returned_qty = 4
        cancel_time = datetime(2026, 9, 24, 10, 0, 0, tzinfo=timezone.utc)

        # Đưa đơn hàng hoàn vào hàng đợi
        self.simulator._queue_return(test_sku, returned_qty, "TEST-RET-01", cancel_time)
        self.assertEqual(len(self.simulator.pending_returns), 1)
        self.assertEqual(inv.pending_return_stock, returned_qty)

        # 1 ngày sau: hàng hoàn vẫn đang trên đường chuyển phát ngược -> chưa vào kho
        too_early = cancel_time + timedelta(days=1)
        self.simulator._process_pending_returns(too_early)
        self.assertEqual(inv.stock_on_hand, stock_start)
        self.assertEqual(len(self.simulator.pending_returns), 1)

        # 6 ngày sau: hàng hoàn đã tới kho trung tâm -> nhập lại kho thành công
        arrived_time = cancel_time + timedelta(days=6)
        self.simulator._process_pending_returns(arrived_time)
        self.assertEqual(inv.stock_on_hand, stock_start + returned_qty)
        self.assertEqual(inv.total_returned, returned_qty)
        self.assertEqual(inv.pending_return_stock, 0)
        self.assertEqual(len(self.simulator.pending_returns), 0)

        # Kiểm tra sự kiện biến động kho RETURN_RESTOCK
        ret_events = [m for m in self.simulator.movement_history if m.movement_type == "RETURN_RESTOCK"]
        self.assertGreaterEqual(len(ret_events), 1)
        self.assertEqual(ret_events[-1].quantity, returned_qty)

    def test_cancellation_release_stock_before_shipping(self):
        """Kiểm tra hoàn trả tồn kho tức thì khi đơn huỷ trước khi giao hàng (CANCEL_RELEASE)."""
        test_sku = "SN-65W-GAN"
        inv = self.simulator.inventory[test_sku]
        stock_start = inv.stock_on_hand
        cancel_qty = 2
        now = datetime(2026, 9, 24, 10, 0, 0, tzinfo=timezone.utc)

        self.simulator._release_cancelled_stock(test_sku, cancel_qty, "TEST-CAN-01", now)
        self.assertEqual(inv.stock_on_hand, stock_start + cancel_qty)

        can_events = [m for m in self.simulator.movement_history if m.movement_type == "CANCEL_RELEASE"]
        self.assertGreaterEqual(len(can_events), 1)
        self.assertEqual(can_events[-1].quantity, cancel_qty)

    def test_tiktok_livestream_order_type(self):
        """Kiểm tra loại đơn TikTok Shop: phát sinh đơn livestream vào khung giờ tối."""
        evening_live_time = datetime(2026, 9, 24, 20, 30, 0, tzinfo=timezone.utc)
        order_types = set()
        for _ in range(30):
            event = self.simulator._generate_tiktok_event(evening_live_time)
            order_types.add(event.order_type)

        # Trong 30 đơn giờ livestream tối, phải có xuất hiện đơn loại livestream
        self.assertIn("livestream", order_types)


if __name__ == "__main__":
    unittest.main()
