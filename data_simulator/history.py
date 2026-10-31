"""Chronological historical events using the same order/inventory engine as live data."""
from dataclasses import asdict
from datetime import datetime, timedelta
from collections import deque
import math
import random
from zoneinfo import ZoneInfo

from data_simulator.data_simulator import ECommerceSimulator, PRODUCT_CATALOG
from data_simulator.demand import ProductDemandModel, expected_multiplier

BUSINESS_ZONE = ZoneInfo('Asia/Ho_Chi_Minh')
HOUR_WEIGHTS = (.01,.01,.005,.005,.005,.01,.02,.04,.06,.07,.08,.09,.12,.06,.05,.05,.05,.05,.06,.07,.12,.11,.06,.03)


def local_time(value):
    return value.replace(tzinfo=BUSINESS_ZONE) if value.tzinfo is None else value.astimezone(BUSINESS_ZONE)


def event_as_of(row, cutoff):
    """Remove transitions after the cutoff and choose a reachable platform status."""
    shopee = row['_platform'] == 'shopee'
    fields = ('pay_time', 'shipped_time', 'completed_time', 'cancel_time', 'update_time', 'synced_at') if shopee else (
        'paid_time', 'rts_time', 'shipped_time', 'delivered_time', 'completed_time', 'cancelled_time', 'update_time', 'synced_at')
    for field in fields:
        value = row.get(field)
        if value and datetime.fromisoformat(value) > cutoff:
            row[field] = None
    cancel = 'cancel_time' if shopee else 'cancelled_time'
    pay = 'pay_time' if shopee else 'paid_time'
    if row.get(cancel):
        status = 'CANCELLED'
    elif row.get('completed_time'):
        status = 'COMPLETED'
    elif not shopee and row.get('delivered_time'):
        status = 'DELIVERED'
    elif row.get('shipped_time'):
        status = 'SHIPPED'
    elif row.get(pay):
        status = 'READY_TO_SHIP' if shopee else 'AWAITING_SHIPMENT'
    else:
        status = 'UNPAID'
    row['order_status'] = status
    if shopee:
        row['order_status_raw'] = status
    else:
        row['item_status'] = 'IN_TRANSIT' if status == 'SHIPPED' else status
    if status != 'CANCELLED':
        for key in ('cancel_reason', 'item_cancel_reason', 'cancel_by', 'cancel_user'):
            if key in row:
                row[key] = None
    return row


def iter_historical_days(start_date, end_date, base_orders_per_day=300, seed=42, order_prefix=''):
    """Yield date, raw events, closing inventory and latent SKU demand diagnostics.

    Same range and seed reproduce order counts/SKUs. UUID ingestion identifiers
    remain unique. Partial final days are sampled only up to the requested cutoff.
    """
    start = local_time(start_date)
    end = min(local_time(end_date), datetime.now(BUSINESS_ZONE))
    if start > end:
        raise ValueError('start_date must be on or before end_date and the present')
    if len(order_prefix) > 20:
        raise ValueError('order_prefix must be at most 20 characters')
    model = ProductDemandModel(PRODUCT_CATALOG, base_orders_per_day, seed)
    products = {p['sku']: p for p in PRODUCT_CATALOG}
    weight_sum = sum(p.get('popularity_weight', 1) for p in PRODUCT_CATALOG)
    def planned_units(product, day):
        # Approximate units/order, not a peek at the day's sampled orders.
        return base_orders_per_day * product.get('popularity_weight', 1) / weight_sum * expected_multiplier(product['sku'], day) * 2.5
    initial = {p['sku']: max(p['initial_stock'], math.ceil(planned_units(p, start.date()) * (p['lead_time_days'] + 4))) for p in PRODUCT_CATALOG}
    sim = ECommerceSimulator(start_date=start, initial_stock_override=initial)
    observed_units = {p['sku']: deque(maxlen=14) for p in PRODUCT_CATALOG}
    previous_state = random.getstate()
    random.seed(seed)
    try:
        day = start.replace(hour=0, minute=0, second=0, microsecond=0)
        while day.date() <= end.date():
            for product in PRODUCT_CATALOG:
                sku = product['sku']
                observed = observed_units[sku]
                planned = planned_units(product, day.date())
                rate = .7 * (sum(observed) / len(observed)) + .3 * planned if observed else planned
                inv = sim.inventory[sku]
                # Inventory capacity follows trailing demand, while sudden spikes can still stock out.
                inv.safety_stock = max(product['safety_stock'], math.ceil(rate * .8))
                inv.reorder_point = max(product['reorder_point'], math.ceil(rate * (inv.lead_time_days + 1.2)))
                inv.reorder_qty = max(product['reorder_qty'], math.ceil(rate * 7))
            demand = model.sample_day(day.date())
            scheduled = []
            for record in demand:
                product = products[record['sku']]
                for _ in range(record['sampled_orders']):
                    stamp = day.replace(hour=random.choices(range(24), weights=HOUR_WEIGHTS)[0], minute=random.randrange(60), second=random.randrange(60))
                    if start <= stamp <= end:
                        scheduled.append((stamp, product))
            scheduled.sort(key=lambda item: item[0])
            events = []
            daily_units = {sku: 0 for sku in products}
            for stamp, product in scheduled:
                # Beauty/fashion skew towards TikTok; electronics/essentials towards Shopee.
                shopee_ratio = .42 if product['category'] in ('Mỹ phẩm', 'Thời trang nữ', 'Thời trang nam') else .65
                if random.random() < shopee_ratio:
                    row = asdict(sim._generate_shopee_event(stamp, product=product))
                    row['_platform'] = 'shopee'
                    row['order_sn'] = order_prefix + row['order_sn']
                else:
                    row = asdict(sim._generate_tiktok_event(stamp, product=product))
                    row['_platform'] = 'tiktok'
                    row['order_id'] = order_prefix + row['order_id']
                events.append(event_as_of(row, end))
                daily_units[product['sku']] += row['quantity']
            for sku, units in daily_units.items():
                observed_units[sku].append(units)
            snapshot_at = min(day + timedelta(days=1) - timedelta(seconds=1), end)
            inventory = sim.get_inventory_snapshot(snapshot_at)
            for record in demand:
                record['date'] = day.date().isoformat()
            yield day.date(), events, inventory, demand
            sim.movement_history.clear()
            day += timedelta(days=1)
    finally:
        random.setstate(previous_state)
