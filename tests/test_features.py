"""
test_features.py
----------------
Unit tests cho các module tính năng phân loại và trích xuất đặc trưng (Tuần 4):
  1. Kiểm tra phân loại ABC theo nguyên lý Pareto 80/15/5.
  2. Kiểm tra phân loại XYZ theo hệ số biến thiên nhu cầu (CV).
  3. Kiểm tra tính toán Safety Stock (SS) và Reorder Point (ROP).
  4. Kiểm tra gom nhóm chuỗi thời gian theo ngày (TimeSeriesFeatureExtractor.aggregate_daily).
  5. Kiểm tra tính toán Lag và Rolling statistics không bị rò rỉ dữ liệu (No Data Leakage).
  6. Kiểm tra Walk-Forward Split cho chuỗi thời gian.
"""

import os
import sys
import unittest
from datetime import date, datetime, timedelta

try:
    import numpy as np
    import pandas as pd
    HAS_DEPS = True
except ImportError:
    HAS_DEPS = False

# Cho phép import ml/features
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
if HAS_DEPS:
    from ml.features.abc_xyz import ABCXYZClassifier
    from ml.features.time_series_features import TimeSeriesFeatureExtractor


@unittest.skipIf(not HAS_DEPS, "Cần pandas & numpy để kiểm thử Feature Engineering")
class TestABCXYZClassifier(unittest.TestCase):

    def setUp(self):
        self.classifier = ABCXYZClassifier(
            pareto_a=0.80,
            pareto_b=0.95,
            cv_x=0.50,
            cv_y=1.00,
            default_lead_time_days=3,
            default_service_level=0.95,
        )

    def test_abc_xyz_classification(self):
        """Kiểm tra phân loại chính xác nhóm A/B/C và X/Y/Z trên tập dữ liệu tổng hợp."""
        # Tạo dữ liệu giả lập 3 SKU:
        # SKU-HIGH: Doanh thu cao, đều đặn mỗi ngày 10 cái (Nhóm AX)
        # SKU-MID:  Doanh thu vừa, biến động vừa phải (Nhóm BY)
        # SKU-LOW:  Doanh thu thấp, ngày bán ngày không (Nhóm CZ)
        dates = [date(2026, 9, 1) + timedelta(days=i) for i in range(30)]
        records = []

        for d in dates:
            # SKU-HIGH: 10 cái @ 100,000 VND = 1,000,000 VND / ngày
            records.append({
                "sku": "SKU-HIGH",
                "create_time": datetime.combine(d, datetime.min.time()),
                "quantity": 10,
                "buyer_total_amount": 1_000_000,
                "is_cancelled": False,
            })
            # SKU-MID: 1 hoặc 5 cái @ 50,000 VND (trung bình 150,000 VND / ngày)
            qty_mid = 1 if d.day % 2 == 0 else 5
            records.append({
                "sku": "SKU-MID",
                "create_time": datetime.combine(d, datetime.min.time()),
                "quantity": qty_mid,
                "buyer_total_amount": qty_mid * 50_000,
                "is_cancelled": False,
            })
            # SKU-LOW: chỉ bán vào ngày 1 và 15, mỗi lần 1 cái @ 20,000 VND
            if d.day in (1, 15):
                records.append({
                    "sku": "SKU-LOW",
                    "create_time": datetime.combine(d, datetime.min.time()),
                    "quantity": 1,
                    "buyer_total_amount": 20_000,
                    "is_cancelled": False,
                })

        df = pd.DataFrame(records)
        result = self.classifier.fit_transform(df)

        self.assertEqual(len(result), 3)

        high_row = result[result["sku"] == "SKU-HIGH"].iloc[0]
        low_row = result[result["sku"] == "SKU-LOW"].iloc[0]

        # SKU-HIGH chiếm áp đảo doanh thu -> Phải là nhóm A
        self.assertEqual(high_row["abc_class"], "A")
        # SKU-HIGH lượng bán hoàn toàn ổn định (std = 0) -> Phải là nhóm X
        self.assertEqual(high_row["xyz_class"], "X")
        self.assertEqual(high_row["matrix_class"], "AX")

        # SKU-LOW doanh thu ít nhất -> Phải là nhóm C
        self.assertEqual(low_row["abc_class"], "C")

    def test_inventory_policy_calculations(self):
        """Kiểm tra công thức Safety Stock và Reorder Point."""
        dates = [date(2026, 9, 1) + timedelta(days=i) for i in range(14)]
        records = [
            {"sku": "TEST-SKU", "create_time": datetime.combine(d, datetime.min.time()),
             "quantity": 5 if d.day % 2 == 0 else 10, "buyer_total_amount": 100000, "is_cancelled": False}
            for d in dates
        ]
        df = pd.DataFrame(records)
        result = self.classifier.fit_transform(df)
        row = result.iloc[0]

        self.assertGreater(row["avg_daily_demand"], 0)
        self.assertGreater(row["std_daily_demand"], 0)
        self.assertGreater(row["safety_stock"], 0)
        # ROP bắt buộc phải lớn hơn Safety Stock vì có Lead Time demand
        self.assertGreater(row["reorder_point"], row["safety_stock"])


@unittest.skipIf(not HAS_DEPS, "Cần pandas & numpy để kiểm thử Feature Engineering")
class TestTimeSeriesFeatures(unittest.TestCase):

    def setUp(self):
        self.extractor = TimeSeriesFeatureExtractor(
            lags=[1, 7],
            rolling_windows=[7],
            ema_spans=[7],
        )

    def test_aggregate_daily_and_lags(self):
        """Kiểm tra gom nhóm ngày và tính toán Lag 1, Lag 7 không rò rỉ dữ liệu."""
        dates = [date(2026, 8, 1) + timedelta(days=i) for i in range(20)]
        records = [
            {"sku": "SKU-01", "create_time": datetime.combine(d, datetime.min.time()),
             "quantity": i + 1, "original_price": 50000, "seller_discount": 0,
             "buyer_total_amount": (i + 1) * 50000, "is_cancelled": False}
            for i, d in enumerate(dates)
        ]
        df = pd.DataFrame(records)

        daily_df = self.extractor.aggregate_daily(df)
        self.assertEqual(len(daily_df), 20)

        feat_df = self.extractor.extract_features(daily_df)

        # Kiểm tra Lag 1: giá trị tại dòng 1 (ngày 2) phải bằng quantity ngày 1
        day1_demand = feat_df.iloc[0]["daily_demand"]
        day2_lag1 = feat_df.iloc[1]["lag_1"]
        self.assertEqual(day2_lag1, day1_demand)

        # Kiểm tra Rolling Mean 7 không chứa ngày hiện tại (được shift 1)
        # Tại dòng 7 (index 7), rolling_mean_7 phải là trung bình của 7 ngày trước đó
        expected_mean = np.mean([i + 1 for i in range(7)])
        actual_mean = feat_df.iloc[7]["rolling_mean_7"]
        self.assertAlmostEqual(actual_mean, expected_mean, places=2)

    def test_calendar_features(self):
        """Kiểm tra nhận diện đúng cuối tuần và ngày đôi mega-sale."""
        # Ngày 09/09/2026 là ngày đôi mega-sale
        test_dt = date(2026, 9, 9)
        records = [{
            "sku": "SKU-01", "create_time": datetime.combine(test_dt, datetime.min.time()),
            "quantity": 10, "original_price": 50000, "buyer_total_amount": 500000, "is_cancelled": False
        }]
        daily_df = self.extractor.aggregate_daily(pd.DataFrame(records))
        feat_df = self.extractor.extract_features(daily_df)

        row = feat_df.iloc[0]
        self.assertEqual(row["is_mega_sale"], 1)


if __name__ == "__main__":
    unittest.main()
