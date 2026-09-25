"""
test_etl.py
-----------
Unit tests cho tầng ETL (Transform & Data Validation):
  1. Kiểm tra chuẩn hóa schema Shopee → Common Schema.
  2. Kiểm tra chuẩn hóa schema TikTok → Common Schema.
  3. Kiểm tra làm sạch dữ liệu (loại bỏ trùng lặp, missing values).
  4. Kiểm tra DataValidator chấp thuận các bản ghi hợp lệ.
  5. Kiểm tra DataValidator cách ly các bản ghi lỗi (missing key, negative price, invalid timeline).
"""

import os
import sys
import unittest
try:
    import pandas as pd
    HAS_PANDAS = True
except ImportError:
    HAS_PANDAS = False

# Cho phép import warehouse/etl
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
if HAS_PANDAS:
    from warehouse.etl.transform import unify_schema, clean_data
    from warehouse.etl.data_validation import DataValidator


@unittest.skipIf(not HAS_PANDAS, "Cần pandas để kiểm thử ETL Pipeline")
class TestETLPipeline(unittest.TestCase):

    def setUp(self):
        self.validator = DataValidator()

    def test_unify_shopee_schema(self):
        """Kiểm tra mapping cột từ Shopee sang Canonical Schema."""
        shopee_raw = pd.DataFrame([{
            "_platform": "shopee",
            "order_sn": "SPE260924000001",
            "item_sku": "OL-IP15PM",
            "item_name": "Ốp lưng iPhone 15 Pro Max",
            "model_name": "Phụ kiện",
            "shop_id": "123456",
            "shop_name": "TechStore",
            "order_status": "COMPLETED",
            "quantity": 2,
            "original_price": 59000,
            "model_discounted_price": 50000,
            "total_order_value": 118000,
            "buyer_total_amount": 100000,
            "seller_discount": 18000,
            "shopee_discount": 0,
            "voucher_from_seller": 0,
            "voucher_from_shopee": 0,
            "actual_shipping_fee": 15000,
            "estimated_shipping_fee": 15000,
            "payment_method": "VNPay",
            "cod": "False",
            "state": "Hà Nội",
            "city": "Hà Nội",
            "district": "Cầu Giấy",
            "shipping_carrier": "SPX Express",
            "create_time": "2026-09-24T10:00:00",
            "pay_time": "2026-09-24T10:05:00",
            "shipped_time": "2026-09-25T08:00:00",
            "completed_time": "2026-09-27T14:00:00",
            "cancel_time": None,
            "cancel_reason": None,
        }])

        unified = unify_schema(shopee_raw)
        self.assertEqual(len(unified), 1)
        row = unified.iloc[0]
        self.assertEqual(row["order_id"], "SPE260924000001")
        self.assertEqual(row["platform"], "shopee")
        self.assertEqual(row["sku"], "OL-IP15PM")
        self.assertEqual(row["quantity"], 2)
        self.assertEqual(row["buyer_total_amount"], 100000)
        self.assertEqual(row["carrier_name"], "SPX Express")
        self.assertEqual(row["shipping_carrier"], "SPX Express")

    def test_unify_tiktok_schema(self):
        """Kiểm tra mapping cột từ TikTok Shop sang Canonical Schema."""
        tiktok_raw = pd.DataFrame([{
            "_platform": "tiktok",
            "order_id": "TT260924000001",
            "seller_sku": "DEN-LED-10",
            "product_name": "Đèn LED dây trang trí",
            "shop_name": "DecorHome",
            "order_status": "COMPLETED",
            "quantity": 1,
            "item_original_price": 89000,
            "item_sale_price": 75000,
            "sub_total": 89000,
            "total_amount": 95000,
            "seller_discount": 14000,
            "platform_discount": 0,
            "tax_amount": 8900,
            "shipping_fee": 15000,
            "original_shipping_fee": 20000,
            "payment_method": "COD",
            "is_cod": "True",
            "region_state": "Hồ Chí Minh",
            "city_town": "Hồ Chí Minh",
            "district": "Quận 1",
            "shipping_provider": "J&T Express",
            "created_time": "2026-09-24T11:00:00",
            "paid_time": "2026-09-24T11:05:00",
            "shipped_time": "2026-09-25T09:00:00",
            "completed_time": "2026-09-27T15:00:00",
            "cancelled_time": None,
        }])

        unified = unify_schema(tiktok_raw)
        self.assertEqual(len(unified), 1)
        row = unified.iloc[0]
        self.assertEqual(row["order_id"], "TT260924000001")
        self.assertEqual(row["platform"], "tiktok")
        self.assertEqual(row["sku"], "DEN-LED-10")
        self.assertEqual(row["buyer_total_amount"], 95000)
        self.assertEqual(row["carrier_name"], "J&T Express")
        self.assertEqual(row["shipping_carrier"], "J&T Express")

    def test_clean_data_deduplication(self):
        """Kiểm tra loại bỏ trùng lặp (order_id, sku)."""
        df = pd.DataFrame([
            {"order_id": "SPE001", "sku": "SKU-A", "quantity": 1, "original_price": 50000, "create_time": "2026-09-24T10:00:00"},
            {"order_id": "SPE001", "sku": "SKU-A", "quantity": 1, "original_price": 50000, "create_time": "2026-09-24T10:00:00"},  # Trùng lặp
            {"order_id": "SPE002", "sku": "SKU-B", "quantity": 2, "original_price": 80000, "create_time": "2026-09-24T10:05:00"},
        ])
        cleaned = clean_data(df)
        self.assertEqual(len(cleaned), 2)

    def test_data_validator_clean(self):
        """Kiểm tra DataValidator duyệt qua bản ghi sạch."""
        df = pd.DataFrame([{
            "order_id": "SPE001",
            "platform": "shopee",
            "sku": "SKU-A",
            "create_time": "2026-09-24T10:00:00",
            "pay_time": "2026-09-24T10:05:00",
            "shipped_time": "2026-09-25T10:00:00",
            "completed_time": "2026-09-27T10:00:00",
            "order_status": "COMPLETED",
            "quantity": 2,
            "original_price": 100000.0,
            "subtotal": 200000.0,
            "buyer_total_amount": 190000.0,
        }])
        clean_df, quarantine_df, report = self.validator.validate(df)
        self.assertEqual(len(clean_df), 1)
        self.assertEqual(len(quarantine_df), 0)
        self.assertEqual(report.pass_rate, 1.0)

    def test_data_validator_quarantine_corrupted(self):
        """Kiểm tra DataValidator phát hiện và cách ly các bản ghi sai sót."""
        corrupted = pd.DataFrame([
            # Thiếu SKU
            {"order_id": "ERR01", "platform": "shopee", "sku": None, "create_time": "2026-09-24T10:00:00", "order_status": "COMPLETED", "quantity": 1, "original_price": 50000},
            # Quantity âm
            {"order_id": "ERR02", "platform": "tiktok", "sku": "SKU-B", "create_time": "2026-09-24T10:00:00", "order_status": "COMPLETED", "quantity": -5, "original_price": 50000},
            # Giá âm
            {"order_id": "ERR03", "platform": "shopee", "sku": "SKU-C", "create_time": "2026-09-24T10:00:00", "order_status": "COMPLETED", "quantity": 1, "original_price": -99000},
            # Thời gian đảo ngược (pay_time trước create_time)
            {"order_id": "ERR04", "platform": "shopee", "sku": "SKU-D", "create_time": "2026-09-24T10:00:00", "pay_time": "2026-09-24T08:00:00", "order_status": "COMPLETED", "quantity": 1, "original_price": 50000},
        ])

        clean_df, quarantine_df, report = self.validator.validate(corrupted)
        self.assertEqual(len(clean_df), 0)
        self.assertEqual(len(quarantine_df), 4)
        self.assertEqual(report.pass_rate, 0.0)
        self.assertIn("MISSING_SKU", report.error_counts)
        self.assertIn("NON_POSITIVE_QUANTITY", report.error_counts)
        self.assertIn("NEGATIVE_ORIGINAL_PRICE", report.error_counts)
        self.assertIn("PAY_TIME_BEFORE_CREATE_TIME", report.error_counts)


if __name__ == "__main__":
    unittest.main()
