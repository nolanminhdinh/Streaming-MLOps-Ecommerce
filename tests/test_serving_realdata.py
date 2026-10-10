"""
test_serving_realdata.py
------------------------
Kiểm thử các bản sửa ở tầng serving (xem docs/04-pipeline-updates/lan-02-sua-loi-va-toi-uu-he-thong.md):
  1. Parity: đặc trưng dựng online (serving/app/features.py) khớp từng cột với
     TimeSeriesFeatureExtractor dùng lúc huấn luyện → không còn training–serving skew.
  2. Dự báo dùng mô hình thật + lịch sử từ Data Warehouse (repository giả), dự báo bước 1
     trùng khớp với model.predict trên dòng Feature Store tương ứng.
  3. Cảnh báo tồn kho lấy tồn kho từ snapshot Fact_Inventory_Daily và ghi rõ nguồn.
  4. /metrics xuất histogram độ trễ theo route template.
"""

from __future__ import annotations

import os
import sys
import tempfile
import unittest
from datetime import date, datetime, timedelta
from unittest import mock

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from ml.features.feature_pipeline import build_feature_store
from ml.features.time_series_features import TimeSeriesFeatureExtractor
from serving.app import data_access, model_loader
from serving.app.features import build_feature_row, to_model_input
from serving.app.inventory_service import InventoryService

try:
    import joblib
    import lightgbm as lgb
    HAS_LGB = True
except ImportError:
    HAS_LGB = False


def make_orders(days: int = 90, seed: int = 7) -> pd.DataFrame:
    """Đơn hàng giả lập 3 SKU với mức nhu cầu khác nhau + chu kỳ tuần."""
    rng = np.random.default_rng(seed)
    start = datetime(2026, 6, 1)
    base = {"SKU-HIGH": 30, "SKU-MID": 12, "SKU-LOW": 3}
    rows = []
    oid = 0
    for i in range(days):
        d = start + timedelta(days=i)
        weekly = 1.4 if d.weekday() >= 5 else 1.0
        for sku, b in base.items():
            n_orders = int(rng.poisson(b * weekly / 2))
            for _ in range(n_orders):
                oid += 1
                q = int(rng.integers(1, 4))
                rows.append({
                    "order_id": f"O{oid}", "sku": sku, "product_name": sku, "category": "Test",
                    "create_time": d + timedelta(hours=int(rng.integers(0, 23))),
                    "quantity": q, "original_price": 50000.0, "seller_discount": 1000.0,
                    "buyer_total_amount": q * 49000.0, "is_cancelled": False,
                })
    return pd.DataFrame(rows)


class FakeRepository:
    """Repository giả thay PostgreSQL: trả lịch sử từ bảng daily đã tổng hợp."""

    def __init__(self, daily: pd.DataFrame, stocks: dict | None = None):
        self.daily = daily
        self.stocks = stocks or {}

    def available(self) -> bool:
        return True

    def get_catalog(self, data_origin=None):
        return {s: {"name": f"Sản phẩm {s}", "category": "Test"} for s in self.daily["sku"].unique()}

    def get_last_order_date(self, data_origin=None):
        return max(self.daily["date"])

    def get_daily_history(self, sku, end_date, days=120, data_origin=None):
        sub = self.daily[(self.daily["sku"] == sku) & (self.daily["date"] <= end_date)].tail(days)
        if sub.empty:
            return None
        cols = ["date", "daily_demand", "daily_revenue", "avg_unit_price", "total_discount", "order_count"]
        return sub[cols].to_dict(orient="records")

    def get_latest_stock(self):
        return self.stocks


class TestOnlineFeatureParity(unittest.TestCase):

    def test_online_row_matches_training_pipeline(self):
        """Mọi cột chuỗi thời gian/lịch dựng online phải bằng đúng cột tương ứng lúc huấn luyện."""
        extractor = TimeSeriesFeatureExtractor(lags=[1, 2, 3, 7, 14, 21, 28], rolling_windows=[7, 14, 28],
                                               ema_spans=[7, 14])
        daily = extractor.aggregate_daily(make_orders(days=70))
        feats = extractor.extract_features(daily)
        cols = [c for c in feats.columns
                if c not in {"sku", "date", "product_name", "category", "target_t_plus_1"}]

        for sku in ["SKU-HIGH", "SKU-LOW"]:
            hist = daily[daily["sku"] == sku].to_dict(orient="records")
            f_sku = feats[feats["sku"] == sku].reset_index(drop=True)
            for t in [0, 1, 6, 7, 20, 27, 28, 45, len(hist) - 1]:
                online = build_feature_row(hist[: t + 1], hist[t]["date"])
                expected = f_sku.loc[t, cols].astype(float).fillna(0).tolist()
                actual = to_model_input(online, cols)
                np.testing.assert_allclose(actual, expected, rtol=1e-9, atol=1e-9,
                                           err_msg=f"Lệch đặc trưng tại {sku} t={t}")


@unittest.skipUnless(HAS_LGB, "Cần lightgbm + joblib")
class TestServingWithWarehouseHistory(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.TemporaryDirectory()
        data_dir = cls.tmp.name
        os.makedirs(os.path.join(data_dir, "mlflow_artifacts"))

        orders = make_orders(days=90)
        cls.features = build_feature_store(orders, save_path=os.path.join(data_dir, "features_daily.parquet"))
        cls.feature_cols = [c for c in cls.features.columns if c not in {
            "sku", "date", "product_name", "category", "abc_class", "xyz_class", "matrix_class", "target_t_plus_1"}]

        cls.model = lgb.LGBMRegressor(n_estimators=60, random_state=42, verbosity=-1)
        cls.model.fit(cls.features[cls.feature_cols].fillna(0), cls.features["target_t_plus_1"])
        joblib.dump(cls.model, os.path.join(data_dir, "mlflow_artifacts", "lightgbm_model.joblib"))

        cls.daily = TimeSeriesFeatureExtractor().aggregate_daily(orders)
        cls.repo = FakeRepository(cls.daily, stocks={"SKU-HIGH": {"stock_on_hand": 5, "as_of": "2026-08-29"}})

        cls.patches = [
            mock.patch.object(model_loader, "DATA_DIR", data_dir),
            mock.patch.dict(os.environ, {
                "FEATURE_SPEC_PATH": os.path.join(data_dir, "feature_spec.json"),
                # The test explicitly exercises the local artifact fallback, independent of .env.
                "MLFLOW_TRACKING_URI": "",
            }),
        ]
        for p in cls.patches:
            p.start()
        data_access.set_repository(cls.repo)

        cls.manager = model_loader.ModelManager(manifest_path=os.path.join(data_dir, "missing_manifest.json"))
        cls.manager.load_model()

    @classmethod
    def tearDownClass(cls):
        for p in cls.patches:
            p.stop()
        data_access.set_repository(None)
        cls.tmp.cleanup()

    def test_model_loaded_from_artifact(self):
        self.assertTrue(self.manager.has_model)
        self.assertEqual(self.manager.model_source, model_loader.SOURCE_JOBLIB)
        self.assertEqual(self.manager.feature_spec["feature_columns"], self.feature_cols)

    def test_first_step_matches_feature_store_prediction(self):
        """Dự báo ngày d+1 phải bằng model.predict trên dòng Feature Store của ngày d."""
        row = self.features[self.features["sku"] == "SKU-MID"].iloc[-10]
        d = row["date"]
        expected = max(0.0, float(self.model.predict(
            pd.DataFrame([row[self.feature_cols].astype(float).fillna(0).values], columns=self.feature_cols))[0]))

        result = self.manager.forecast("SKU-MID", d + timedelta(days=1), d + timedelta(days=1))
        self.assertEqual(result["model_source"], model_loader.SOURCE_JOBLIB)
        self.assertEqual(result["history_source"], "warehouse")
        self.assertAlmostEqual(result["items"][0]["predicted_quantity"], round(expected, 1), places=6)

    def test_recursive_forecast_is_data_driven(self):
        """Dự báo nhiều ngày sau mốc dữ liệu cuối: không âm, SKU bán chạy > SKU bán chậm."""
        last = max(self.daily["date"])
        start, end = last + timedelta(days=1), last + timedelta(days=7)
        high = self.manager.forecast("SKU-HIGH", start, end, confidence_interval=True)
        low = self.manager.forecast("SKU-LOW", start, end)

        self.assertEqual(len(high["items"]), 7)
        for it in high["items"]:
            self.assertGreaterEqual(it["predicted_quantity"], 0.0)
            self.assertLessEqual(it["lower_bound"], it["predicted_quantity"])
            self.assertGreaterEqual(it["upper_bound"], it["predicted_quantity"])
        self.assertGreater(sum(i["predicted_quantity"] for i in high["items"]),
                           sum(i["predicted_quantity"] for i in low["items"]))

    def test_forecast_too_far_raises(self):
        last = max(self.daily["date"])
        with self.assertRaises(ValueError):
            self.manager.forecast("SKU-MID", last + timedelta(days=200), last + timedelta(days=201))

    def test_inventory_uses_warehouse_stock_and_model_demand(self):
        svc = InventoryService.__new__(InventoryService)
        svc.model_manager = self.manager
        item = svc.evaluate_sku("SKU-HIGH", lead_time_days=3)
        self.assertEqual(item.current_stock, 5)
        self.assertTrue(item.stock_source.startswith("warehouse_snapshot"))
        self.assertEqual(item.demand_source, "model_forecast")
        self.assertEqual(item.alert_level, "CRITICAL")

        no_snapshot = svc.evaluate_sku("SKU-LOW", lead_time_days=3)
        self.assertEqual(no_snapshot.stock_source, "simulated_demo")


class TestFallbackIsExplicit(unittest.TestCase):

    def test_heuristic_fallback_is_labelled(self):
        """Không có mô hình & không có kho → vẫn trả lời nhưng ghi rõ nguồn heuristic."""
        with tempfile.TemporaryDirectory() as tmp, \
                mock.patch.object(model_loader, "DATA_DIR", tmp), \
                mock.patch.dict(os.environ, {"FEATURE_SPEC_PATH": os.path.join(tmp, "none.json")}):
            data_access.set_repository(data_access.WarehouseRepository(enabled=False))
            try:
                mgr = model_loader.ModelManager(manifest_path=os.path.join(tmp, "none.json"))
                mgr.load_model()
                res = mgr.forecast("OL-IP15PM", date(2026, 10, 1), date(2026, 10, 3))
            finally:
                data_access.set_repository(None)
        self.assertFalse(mgr.has_model)
        self.assertEqual(res["model_source"], model_loader.SOURCE_HEURISTIC_CATALOG)
        self.assertEqual(res["history_source"], "none")


class TestMetricsInstrumentation(unittest.TestCase):

    def test_latency_histogram_by_route(self):
        try:
            from fastapi.testclient import TestClient
            from serving.app.main import app
        except Exception:
            self.skipTest("Cần fastapi + httpx")
        client = TestClient(app)
        r = client.post("/predict/demand", json={"sku": "OL-IP15PM", "from_date": "2026-10-01",
                                                  "to_date": "2026-10-02"})
        self.assertEqual(r.status_code, 200)
        self.assertIn("model_source", r.json())

        body = client.get("/metrics").text
        self.assertIn('ecommerce_api_request_duration_seconds_bucket{endpoint="/predict/demand"', body)
        self.assertRegex(body, r'ecommerce_api_requests_total\{endpoint="/predict/demand"\} [1-9]')
        self.assertIn("ecommerce_model_ready", body)


if __name__ == "__main__":
    unittest.main()
