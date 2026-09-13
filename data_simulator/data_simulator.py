"""
Data_Simulator.py
------------------
Mô phỏng luồng phát sinh đơn hàng E-Commerce theo thời gian thực, có gắn:
- Xu hướng (Trend) tăng trưởng dài hạn.
- Mùa vụ (Seasonality): chu kỳ tuần, ngày lễ.
- Sự kiện Flash Sale: tăng đột biến nhu cầu trong khung giờ ngắn.

Trạng thái: khung sườn (skeleton) — sẽ hoàn thiện logic sinh dữ liệu và
kết nối Kafka Producer thực tế ở Tuần 2.
"""

import json
import random
import time
from dataclasses import dataclass, asdict
from datetime import datetime


@dataclass
class OrderEvent:
    order_id: str
    sku: str
    warehouse_id: str
    quantity: int
    unit_price: float
    timestamp: str


class ECommerceSimulator:
    """Sinh sự kiện đơn hàng giả lập với các quy luật thực tế."""

    def __init__(self, sku_pool_size: int = 200, warehouse_count: int = 5):
        self.sku_pool = [f"SKU-{i:04d}" for i in range(sku_pool_size)]
        self.warehouses = [f"WH-{i:02d}" for i in range(warehouse_count)]
        self._order_counter = 0

    def _is_flash_sale(self, now: datetime) -> bool:
        """TODO: định nghĩa khung giờ Flash Sale (ví dụ 12h-13h, 20h-22h)."""
        return now.hour in (12, 20)

    def _seasonal_multiplier(self, now: datetime) -> float:
        """TODO: hệ số mùa vụ theo ngày trong tuần / ngày lễ."""
        weekend_boost = 1.3 if now.weekday() >= 5 else 1.0
        flash_sale_boost = 2.5 if self._is_flash_sale(now) else 1.0
        return weekend_boost * flash_sale_boost

    def generate_event(self) -> OrderEvent:
        now = datetime.utcnow()
        self._order_counter += 1
        base_qty = random.randint(1, 5)
        qty = max(1, int(base_qty * self._seasonal_multiplier(now)))

        return OrderEvent(
            order_id=f"ORD-{self._order_counter:08d}",
            sku=random.choice(self.sku_pool),
            warehouse_id=random.choice(self.warehouses),
            quantity=qty,
            unit_price=round(random.uniform(10.0, 500.0), 2),
            timestamp=now.isoformat(),
        )

    def run(self, events_per_second: int = 5, duration_seconds: int | None = None):
        """
        Vòng lặp sinh sự kiện liên tục.

        TODO (Tuần 2): thay print() bằng gửi message thực sự tới Kafka
        topic `ecom.orders.raw` thông qua `ingestion/producer.py`.
        """
        start = time.time()
        while duration_seconds is None or (time.time() - start) < duration_seconds:
            for _ in range(events_per_second):
                event = self.generate_event()
                print(json.dumps(asdict(event), ensure_ascii=False))
            time.sleep(1)


if __name__ == "__main__":
    simulator = ECommerceSimulator()
    simulator.run(events_per_second=5, duration_seconds=10)
