"""Behavior checks for stochastic SKU history; no database required."""
import random
import unittest
from datetime import date, datetime, timedelta

import numpy as np
import pandas as pd

from data_simulator.data_simulator import PRODUCT_CATALOG
from data_simulator.demand import ProductDemandModel, expected_multiplier
from data_simulator.history import BUSINESS_ZONE, iter_historical_days
from warehouse.etl.transform import unify_schema, clean_data


class HistoricalDemandTests(unittest.TestCase):
    def year(self, seed=42):
        model = ProductDemandModel(PRODUCT_CATALOG, 300, seed)
        rows = []
        for i in range(304):
            day = date(2026, 1, 1) + timedelta(days=i)
            rows.extend(dict(row, month=day.month, day=day) for row in model.sample_day(day))
        return pd.DataFrame(rows)

    def test_seed_reproducibility_and_different_seed(self):
        first = self.year(71)
        pd.testing.assert_frame_equal(first, self.year(71))
        self.assertFalse(first.sampled_orders.equals(self.year(72).sampled_orders))

    def test_product_seasons_and_decline(self):
        summer, winter = date(2026, 6, 18), date(2026, 1, 18)
        self.assertGreater(expected_multiplier('QM-USB-01', summer), 4 * expected_multiplier('QM-USB-01', winter))
        self.assertGreater(expected_multiplier('KCN-ANESSA', summer), 2 * expected_multiplier('KCN-ANESSA', winter))
        self.assertGreater(expected_multiplier('BL-15-CS', date(2026, 8, 18)), 2 * expected_multiplier('BL-15-CS', summer))
        self.assertLess(expected_multiplier('OL-IP15PM', date(2026, 7, 14)), expected_multiplier('OL-IP15PM', date(2026, 1, 14)))

    def test_counts_vary_and_sku_series_are_not_identical(self):
        frame = self.year()
        totals = frame.groupby('month').sampled_orders.sum()
        self.assertGreater(totals.std() / totals.mean(), .15)
        sku = frame.pivot(index='day', columns='sku', values='sampled_orders')
        self.assertLess(sku['QM-USB-01'].corr(sku['KT-KF94-10']), .6)
        self.assertTrue((sku['BL-15-CS'] == 0).any())
        self.assertTrue((frame.sampled_orders >= 0).all())

    def test_campaign_and_regular_day_remain_stochastic(self):
        a, b = date(2026, 6, 6), date(2026, 6, 13)
        self.assertGreater(expected_multiplier('BP-GM-104', a), 2 * expected_multiplier('BP-GM-104', b))
        samples = [ProductDemandModel(PRODUCT_CATALOG, seed=i).sample_day(a)[0]['sampled_orders'] for i in range(25)]
        self.assertGreater(np.var(samples), np.mean(samples))

    def test_events_within_range_ordered_and_sku_specific(self):
        start = datetime(2026, 6, 1, tzinfo=BUSINESS_ZONE)
        end = datetime(2026, 6, 2, 10, tzinfo=BUSINESS_ZONE)
        state = random.getstate()
        batches = list(iter_historical_days(start, end, 100, seed=12, order_prefix='TEST-'))
        self.assertEqual(random.getstate(), state)
        self.assertEqual(len(batches), 2)
        rows = [r for _, events, _, _ in batches for r in events]
        clean = clean_data(unify_schema(pd.DataFrame(rows)))
        self.assertEqual(len(clean), len(rows))
        stamps = [datetime.fromisoformat(r.get('create_time') or r['created_time']) for r in rows]
        self.assertEqual(stamps, sorted(stamps))
        self.assertTrue(clean.create_time.between(start.replace(tzinfo=None), end.replace(tzinfo=None)).all())
        for column in ('pay_time', 'shipped_time', 'completed_time', 'cancel_time'):
            self.assertTrue((clean[column].dropna() <= end.replace(tzinfo=None)).all())
        for day, events, inventory, demand in batches:
            self.assertEqual(len(inventory), len(PRODUCT_CATALOG))
            self.assertTrue(all(r['stock_on_hand'] >= 0 for r in inventory))
            if day == start.date():
                actual = pd.Series([r.get('item_sku') or r.get('seller_sku') for r in events]).value_counts().to_dict()
                self.assertEqual(actual, {r['sku']: r['sampled_orders'] for r in demand if r['sampled_orders']})

    def test_invalid_inputs(self):
        with self.assertRaises(ValueError):
            ProductDemandModel(PRODUCT_CATALOG, base_orders_per_day=0)
        with self.assertRaises(ValueError):
            list(iter_historical_days(datetime(2026, 2, 2), datetime(2026, 1, 1)))

    def test_replenishment_scales_without_eliminating_stockouts(self):
        batches = list(iter_historical_days(datetime(2026, 1, 1), datetime(2026, 2, 1), 300, seed=20261031))
        rows = [r for _, events, _, _ in batches for r in events]
        cancelled = sum(r['order_status'] == 'CANCELLED' for r in rows)
        self.assertLess(cancelled / len(rows), .3)
        self.assertTrue(any(r['reorder_point'] > p['reorder_point'] for _, _, inventory, _ in batches for r in inventory for p in PRODUCT_CATALOG if r['sku'] == p['sku']))


if __name__ == '__main__':
    unittest.main()
