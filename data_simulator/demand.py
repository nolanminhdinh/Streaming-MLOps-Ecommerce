"""Configurable synthetic demand assumptions, not estimates fitted to real sales.

Demand is sampled per SKU, not allocated from a fixed daily order total. Shared
market shocks coexist with category and SKU shocks; no monthly totals are forced.
"""
from dataclasses import dataclass
from datetime import date, timedelta
import math

@dataclass(frozen=True)
class DemandProfile:
    months: tuple = (1.0,) * 12
    annual_trend: float = 0.1
    volatility: float = 0.35
    sale_lift: float = 1.5
    weekend_lift: float = 0.12
    inactive_probability: float = 0.0


PROFILES = {
    'OL-IP15PM': DemandProfile(annual_trend=-0.38, sale_lift=1.3),
    'CL-SS-S24': DemandProfile(annual_trend=-0.28, sale_lift=1.2),
    'SN-65W-GAN': DemandProfile(annual_trend=0.38, sale_lift=2.0),
    'ATN-CB-001': DemandProfile((.9,.7,1.1,1.5,1.7,1.5,1.3,1.1,.9,.8,.7,.8), .12, .42, 1.8),
    'OMO-6KG': DemandProfile((1.1,.95,1,1,1,1,1,1,1,1,1,1.1), .06, .15, .5, .04),
    'TN-TWS-PRO': DemandProfile(annual_trend=-.12, volatility=.5, sale_lift=2.5),
    'KCN-ANESSA': DemandProfile((.65,.7,1.1,1.5,1.9,2.1,1.8,1.45,1.1,.8,.65,.6), .18, .3, 1.3),
    'BP-GM-104': DemandProfile((.9,.75,.8,.85,1,1.2,1.4,1.5,1.2,1.1,1.3,1.4), .15, .65, 2.8, .3, .08),
    'AF-6L-001': DemandProfile((1.5,1.1,.7,.75,.8,.85,.8,.9,1,1.1,1.6,1.7), -.08, .6, 3.0, .2, .12),
    'SRM-CERAVE': DemandProfile(annual_trend=.22, volatility=.22, sale_lift=1.1),
    'GTT-JR-001': DemandProfile((1.15,.85,1.1,1.2,1,.85,.8,1.3,1.5,1.2,1,1.1), .08, .48, 2.0, .25),
    'BL-15-CS': DemandProfile((.65,.5,.7,.75,.8,.8,1.3,3.0,2.6,1.1,.7,.65), .1, .5, 1.4, .08, .08),
    'NHH-KLA180': DemandProfile((.9,.8,1.1,1,1.1,1.15,1.1,1.1,1.1,1.4,1.3,1.2), -.08, .32, 1.4),
    'CG-G304': DemandProfile(annual_trend=.3, volatility=.55, sale_lift=2.3, weekend_lift=.3),
    'DG-CT-1M8': DemandProfile((1.4,1.1,.8,.75,.8,.8,.8,.9,1,1.3,1.5,1.6), .05, .55, 1.8, .15, .12),
    'DEN-LED-10': DemandProfile((2.3,1.5,.55,.5,.6,.65,.7,.8,1.1,1.4,1.9,2.8), .12, .55, 1.6),
    'SR-VC-TO30': DemandProfile(annual_trend=.45, volatility=.45, sale_lift=1.8),
    'KT-KF94-10': DemandProfile((1.6,1.4,1.1,.8,.7,.7,.75,.9,1.2,1.45,1.6,1.7), -.2, .22, .4, .03),
    'QM-USB-01': DemandProfile((.2,.3,.8,1.8,2.8,2.5,2.0,1.3,.7,.35,.2,.15), .05, .48, 1.3),
    'TX-PU-001': DemandProfile((1.2,.8,1.5,.9,.85,.8,.85,1,1.1,1.8,1.3,1.5), .2, .6, 2.2, .25, .1),
}

# A scenario of market seasonality; deliberately separate from each SKU's profile.
MARKET_MONTHS = (.95, .62, .86, .97, 1.12, 1.03, .88, 1.16, 1.32, 1.18, 1.65, 1.5)


def expected_multiplier(sku: str, day: date) -> float:
    profile = PROFILES.get(sku, DemandProfile())
    growth = math.exp(profile.annual_trend * (day.timetuple().tm_yday - 1) / 365)
    # Campaign lead-in, peak and post-sale pullback, with product-specific response.
    distance = day.day - day.month
    campaign = (1 + profile.sale_lift if distance == 0 else
                1 + .22 * profile.sale_lift if -2 <= distance < 0 else
                max(.55, 1 - .12 * profile.sale_lift) if 1 <= distance <= 2 else 1)
    payday = 1.15 if day.day in (15, 25, 26, 27, 28) else 1
    weekend = 1 + profile.weekend_lift if day.weekday() >= 5 else 1
    return MARKET_MONTHS[day.month - 1] * profile.months[day.month - 1] * growth * campaign * payday * weekend


class ProductDemandModel:
    def __init__(self, catalog, base_orders_per_day=300, seed=42):
        import numpy as np
        if not math.isfinite(base_orders_per_day) or base_orders_per_day <= 0:
            raise ValueError('base_orders_per_day must be positive and finite')
        self.catalog = catalog
        self.base = base_orders_per_day
        self.rng = np.random.default_rng(seed)
        self.total_weight = sum(p.get('popularity_weight', 1) for p in catalog)
        self.market_state = 0.0
        self.sku_state = {p['sku']: 0.0 for p in catalog}
        self.category_state = {}
        self.viral = {}
        self.month = None
        self.last_day = None

    def sample_day(self, day):
        if self.last_day is not None and day <= self.last_day:
            raise ValueError('Days must be sampled in strictly increasing order')
        self.last_day = day
        month = (day.year, day.month)
        if month != self.month:
            self.category_state = {category: self.rng.normal(0, .16) for category in sorted({p.get('category', '') for p in self.catalog})}
            self.month = month
        self.market_state = .8 * self.market_state + self.rng.normal(0, .13)
        results = []
        for product in self.catalog:
            sku = product['sku']
            profile = PROFILES.get(sku, DemandProfile())
            self.sku_state[sku] = .7 * self.sku_state[sku] + self.rng.normal(0, .12)
            if self.viral.get(sku, (day, 1))[0] <= day and self.rng.random() < .008:
                self.viral[sku] = (day + timedelta(days=int(self.rng.integers(3, 8))), self.rng.uniform(1.5, 3))
            viral_end, viral_lift = self.viral.get(sku, (day, 1))
            viral = viral_lift if day < viral_end else 1
            mean = (self.base * product.get('popularity_weight', 1) / self.total_weight *
                    expected_multiplier(sku, day) * viral *
                    math.exp(self.market_state + self.category_state[product.get('category', '')] + self.sku_state[sku]))
            # Gamma-Poisson (negative binomial): variance exceeds the mean.
            shape = 1 / profile.volatility ** 2
            intensity = self.rng.gamma(shape, mean / shape)
            count = int(self.rng.poisson(intensity))
            if self.rng.random() < profile.inactive_probability:
                count = 0
            results.append({'sku': sku, 'expected_orders': mean, 'sampled_orders': count})
        return results
