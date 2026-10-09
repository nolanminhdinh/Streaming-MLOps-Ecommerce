"""
inventory_service.py
--------------------
Dịch vụ Quản trị Tồn kho & Cảnh báo Đặt hàng lại (Inventory Reorder Alert Service):
  1. Tính toán Tồn kho an toàn: Safety Stock (SS) = Z * sigma_demand * sqrt(Lead_Time)
  2. Tính toán Điểm đặt hàng lại: Reorder Point (ROP) = (Demand_trung_bình * Lead_Time) + SS
  3. So sánh với tồn kho hiện tại (Current Stock): snapshot mới nhất trong Fact_Inventory_Daily
     (nạp từ topic inventory.logs). Nhu cầu d, σ lấy từ dự báo mô hình / lịch sử Fact_Orders.
     Mỗi dòng cảnh báo ghi rõ stock_source và demand_source.
  4. Phân cấp cảnh báo: CRITICAL, WARNING, NORMAL
  5. Đề xuất số lượng đặt hàng tối ưu (Recommended Order Quantity) và số ngày còn lại trước khi đứt hàng.
"""

from __future__ import annotations

import logging
import math
import os
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from typing import Any, Dict, List, Optional, Tuple

try:
    from serving.app.data_access import get_repository
    from serving.app.model_loader import get_model_manager
    from serving.app.schemas import ReorderAlertItem, ReorderAlertResponse
except ImportError:
    from app.data_access import get_repository
    from app.model_loader import get_model_manager
    from app.schemas import ReorderAlertItem, ReorderAlertResponse

logger = logging.getLogger("mlops.serving.inventory")
BUSINESS_TZ = os.getenv("BUSINESS_TZ", "Asia/Ho_Chi_Minh")


def _business_today() -> date:
    """Lấy ngày nghiệp vụ thống nhất với ETL và partition Data Lake."""
    return datetime.now(ZoneInfo(BUSINESS_TZ)).date()

# Bảng tra Z-Score theo Service Level chuẩn trong Logistics & Supply Chain
SERVICE_LEVEL_Z_TABLE = {
    0.80: 0.842,
    0.85: 1.036,
    0.90: 1.282,
    0.95: 1.645,  # Mặc định 95%
    0.98: 2.054,
    0.99: 2.326,
}


def get_z_score(service_level: float) -> float:
    """Tra cứu giá trị Z tương ứng với Service Level mong muốn."""
    if service_level in SERVICE_LEVEL_Z_TABLE:
        return SERVICE_LEVEL_Z_TABLE[service_level]
    # Nội suy hoặc lấy giá trị gần nhất
    for sl, z in sorted(SERVICE_LEVEL_Z_TABLE.items()):
        if service_level <= sl:
            return z
    return 1.645


class InventoryService:
    """Xử lý nghiệp vụ đánh giá tồn kho và phát tín hiệu cảnh báo nhập hàng."""

    def __init__(self):
        self.model_manager = get_model_manager()

    def calculate_policy(
        self,
        avg_daily_demand: float,
        std_daily_demand: float,
        lead_time_days: int = 3,
        service_level: float = 0.95,
    ) -> Tuple[int, int]:
        """
        Tính Safety Stock và Reorder Point chuẩn:
          - SS  = Z * sigma * sqrt(L)
          - ROP = (d * L) + SS
        """
        z = get_z_score(service_level)
        ss_val = z * std_daily_demand * math.sqrt(lead_time_days)
        safety_stock = max(1, math.ceil(ss_val))

        lead_time_demand = avg_daily_demand * lead_time_days
        rop_val = lead_time_demand + safety_stock
        reorder_point = max(safety_stock + 1, math.ceil(rop_val))

        return safety_stock, reorder_point

    def _demand_stats(self, sku: str, lead_time_days: int) -> Tuple[float, float, str]:
        """
        Nhu cầu bình quân / độ lệch chuẩn ngày dùng cho SS và ROP:
          - "model_forecast":    d = trung bình dự báo L ngày tới (mô hình thật), σ = lịch sử 28 ngày
          - "warehouse_history": d, σ = thống kê 28 ngày gần nhất từ Fact_Orders
          - "fallback_catalog":  bảng demo (khi không có Data Warehouse)
        """
        today = _business_today()
        fc = self.model_manager.forecast(sku, today, today + timedelta(days=max(lead_time_days, 1) - 1))
        qtys = [it["predicted_quantity"] for it in fc["items"]]
        if fc["history_source"] == "warehouse":
            history, _ = self.model_manager._load_history(sku, today)
            recent = [h["daily_demand"] for h in (history or [])[-28:]]
            n = len(recent)
            mean_hist = sum(recent) / n if n else 0.0
            sigma = math.sqrt(sum((v - mean_hist) ** 2 for v in recent) / (n - 1)) if n > 1 else 0.0
            if not fc["model_source"].startswith("heuristic"):
                return sum(qtys) / len(qtys), sigma, "model_forecast"
            return mean_hist, sigma, "warehouse_history"
        meta = self.model_manager.get_product_info(sku)
        return meta["base_demand"], meta["sigma"], "fallback_catalog"

    def evaluate_sku(
        self,
        sku: str,
        current_stock: Optional[int] = None,
        lead_time_days: int = 3,
        service_level: float = 0.95,
        stock_snapshot: Optional[Dict[str, Dict[str, Any]]] = None,
    ) -> ReorderAlertItem:
        """Đánh giá chi tiết 1 SKU và sinh báo cáo cảnh báo."""
        meta = self.model_manager.get_product_info(sku)
        segment = meta.get("segment", "BX")

        avg_demand, sigma, demand_source = self._demand_stats(sku, lead_time_days)

        # Tính SS và ROP theo công thức
        safety_stock, reorder_point = self.calculate_policy(
            avg_daily_demand=avg_demand,
            std_daily_demand=sigma,
            lead_time_days=lead_time_days,
            service_level=service_level,
        )

        # Nguồn tồn kho: số liệu gửi lên > snapshot Fact_Inventory_Daily > giả lập demo
        if current_stock is not None:
            stock_source = "request"
        else:
            if stock_snapshot is None:
                stock_snapshot = get_repository().get_latest_stock() or {}
            snap = stock_snapshot.get(sku)
            if snap is not None:
                current_stock = int(snap["stock_on_hand"])
                stock_source = f"warehouse_snapshot:{snap['as_of']}"
            else:
                current_stock = self._simulated_stock(sku, segment, safety_stock, reorder_point)
                stock_source = "simulated_demo"

        # Xác định mức độ cảnh báo (Alert Level)
        if current_stock <= safety_stock:
            alert_level = "CRITICAL"
            needs_reorder = True
        elif current_stock <= reorder_point:
            alert_level = "WARNING"
            needs_reorder = True
        else:
            alert_level = "NORMAL"
            needs_reorder = False

        # Số ngày còn lại trước khi hết sạch tồn kho (Days Until Stockout)
        days_until_stockout = round(current_stock / max(avg_demand, 0.1), 1)

        # Số lượng nhập đề xuất: Đưa tồn kho về mức ROP + 7 ngày tiêu thụ
        target_buffer = reorder_point + math.ceil(avg_demand * 7)
        recommended_reorder_qty = max(0, target_buffer - current_stock) if needs_reorder else 0

        return ReorderAlertItem(
            sku=sku,
            product_name=meta["name"],
            category=meta["category"],
            matrix_segment=segment,
            current_stock=current_stock,
            avg_daily_demand=round(avg_demand, 1),
            lead_time_days=lead_time_days,
            safety_stock=safety_stock,
            reorder_point=reorder_point,
            needs_reorder=needs_reorder,
            alert_level=alert_level,
            days_until_stockout=days_until_stockout,
            recommended_reorder_qty=recommended_reorder_qty,
            stock_source=stock_source,
            demand_source=demand_source,
        )

    @staticmethod
    def _simulated_stock(sku: str, segment: str, safety_stock: int, reorder_point: int) -> int:
        """
        Tồn kho GIẢ LẬP cho chế độ demo khi chưa có snapshot trong Fact_Inventory_Daily.
        Kết quả luôn gắn nhãn stock_source="simulated_demo" để không bị nhầm với số liệu thật.
        """
        if "AZ" in segment or sku in ("KCN-ANESSA", "DEN-LED-10"):
            return math.floor(safety_stock * 0.7)
        if "AY" in segment or sku in ("TN-TWS-PRO", "AF-6L-001"):
            return math.floor(safety_stock + (reorder_point - safety_stock) * 0.5)
        return math.ceil(reorder_point * 1.6)

    def evaluate_all(
        self,
        skus: Optional[List[str]] = None,
        custom_stocks: Optional[Dict[str, int]] = None,
        lead_time_days: int = 3,
        service_level: float = 0.95,
    ) -> ReorderAlertResponse:
        """Quét và thẩm định toàn bộ danh mục sản phẩm."""
        catalog, _ = self.model_manager.get_catalog()
        target_skus = skus if skus is not None else list(catalog.keys())
        custom_stocks = custom_stocks or {}
        # Đọc snapshot tồn kho 1 lần cho cả lượt quét thay vì 1 truy vấn / SKU
        stock_snapshot = get_repository().get_latest_stock() or {}

        items: List[ReorderAlertItem] = []
        critical_count = 0
        warning_count = 0
        normal_count = 0

        for sku in target_skus:
            stock = custom_stocks.get(sku, None)
            item = self.evaluate_sku(
                sku=sku,
                current_stock=stock,
                lead_time_days=lead_time_days,
                service_level=service_level,
                stock_snapshot=stock_snapshot,
            )
            items.append(item)

            if item.alert_level == "CRITICAL":
                critical_count += 1
            elif item.alert_level == "WARNING":
                warning_count += 1
            else:
                normal_count += 1

        # Sắp xếp ưu tiên: CRITICAL lên đầu, sau đó đến WARNING, cuối cùng NORMAL
        priority_map = {"CRITICAL": 0, "WARNING": 1, "NORMAL": 2}
        items.sort(key=lambda x: (priority_map.get(x.alert_level, 3), x.days_until_stockout))

        return ReorderAlertResponse(
            total_skus_evaluated=len(items),
            skus_needing_reorder=critical_count + warning_count,
            critical_count=critical_count,
            warning_count=warning_count,
            normal_count=normal_count,
            alerts=items,
            generated_at=datetime.now(timezone.utc).isoformat(),
        )


_inventory_service_instance: Optional[InventoryService] = None


def get_inventory_service() -> InventoryService:
    global _inventory_service_instance
    if _inventory_service_instance is None:
        _inventory_service_instance = InventoryService()
    return _inventory_service_instance
