"""
data_access.py — Truy cập Data Warehouse cho tầng serving
---------------------------------------------------------
Cung cấp dữ liệu THẬT cho suy luận và cảnh báo tồn kho, thay cho bảng hardcode trước đây:
  - Danh mục sản phẩm         ← Dim_Products
  - Lịch sử nhu cầu theo ngày ← Fact_Orders (đơn không hủy, liên tục theo ngày, ngày trống = 0)
  - Tồn kho hiện tại          ← Fact_Inventory_Daily (snapshot mới nhất của từng SKU)

Khi PostgreSQL không truy cập được, repository "ngắt mạch" trong DB_RETRY_SECONDS giây để
request sau không phải chờ timeout kết nối; tầng gọi tự chuyển sang nguồn dự phòng và
ghi rõ nguồn dữ liệu (`*_source`) trong response.
"""

from __future__ import annotations

import logging
import os
import threading
import time
from datetime import date, timedelta
from typing import Any, Callable, Dict, List, Optional, Tuple

logger = logging.getLogger("mlops.serving.data_access")

DB_RETRY_SECONDS = float(os.getenv("SERVING_DB_RETRY_SECONDS", "30"))
CACHE_TTL_SECONDS = float(os.getenv("SERVING_CACHE_TTL_SECONDS", "60"))


def _database_url() -> str:
    return (
        f"postgresql://{os.getenv('POSTGRES_USER', 'ecom')}:{os.getenv('POSTGRES_PASSWORD', 'ecom_password')}"
        f"@{os.getenv('POSTGRES_HOST', 'localhost')}:{os.getenv('POSTGRES_PORT', '5432')}"
        f"/{os.getenv('POSTGRES_DB', 'ecom_warehouse')}"
    )


class WarehouseRepository:
    """Đọc dữ liệu kho với cache TTL ngắn và circuit breaker."""

    def __init__(self, database_url: Optional[str] = None, enabled: Optional[bool] = None):
        self.database_url = database_url or _database_url()
        if enabled is None:
            enabled = os.getenv("SERVING_USE_WAREHOUSE", "true").lower() != "false"
        self.enabled = enabled
        self._engine = None
        self._down_until = 0.0
        self._cache: Dict[Any, Tuple[float, Any]] = {}
        self._lock = threading.Lock()

    # ─── hạ tầng ───

    def _get_engine(self):
        if self._engine is None:
            from sqlalchemy import create_engine
            self._engine = create_engine(
                self.database_url,
                pool_pre_ping=True,
                pool_size=5,
                max_overflow=5,
                connect_args={"connect_timeout": 3},
            )
        return self._engine

    def available(self) -> bool:
        return self.enabled and time.time() >= self._down_until

    def _query(self, cache_key: Any, fn: Callable[[Any], Any], ttl: float = CACHE_TTL_SECONDS) -> Any:
        """Chạy truy vấn có cache; lỗi kết nối → ngắt mạch và trả về None."""
        if not self.available():
            return None
        now = time.time()
        with self._lock:
            hit = self._cache.get(cache_key)
            if hit and now - hit[0] < ttl:
                return hit[1]
        try:
            with self._get_engine().connect() as conn:
                value = fn(conn)
        except Exception as e:
            self._down_until = time.time() + DB_RETRY_SECONDS
            logger.warning("Không truy vấn được Data Warehouse (%s) — dùng nguồn dự phòng trong %.0fs.",
                           e.__class__.__name__, DB_RETRY_SECONDS)
            return None
        with self._lock:
            if len(self._cache) > 2000:  # chặn cache phình vô hạn theo (sku, ngày)
                self._cache = {k: v for k, v in self._cache.items() if now - v[0] < ttl}
            self._cache[cache_key] = (now, value)
        return value

    # ─── truy vấn nghiệp vụ ───

    def get_catalog(self) -> Optional[Dict[str, Dict[str, Any]]]:
        """{sku: {name, category}} từ Dim_Products."""
        from sqlalchemy import text

        def run(conn):
            rows = conn.execute(text(
                "SELECT sku, product_name, category FROM Dim_Products ORDER BY sku"
            ))
            return {r[0]: {"name": r[1] or r[0], "category": r[2] or "Khác"} for r in rows}

        catalog = self._query("catalog", run, ttl=300)
        return catalog or None

    def get_last_order_date(self) -> Optional[date]:
        """Ngày gần nhất có đơn hàng trong kho — mốc cuối của dữ liệu 'đã biết'."""
        from sqlalchemy import text

        def run(conn):
            return conn.execute(text(
                "SELECT MAX(create_time)::date FROM Fact_Orders WHERE is_cancelled = FALSE"
            )).scalar()

        return self._query("last_order_date", run)

    def get_daily_history(self, sku: str, end_date: date, days: int = 120) -> Optional[List[Dict[str, Any]]]:
        """
        Chuỗi nhu cầu theo ngày LIÊN TỤC của 1 SKU trong (end_date - days, end_date].
        Ngày không có đơn → 0 (giống aggregate_daily lúc huấn luyện); giá đơn vị ffill.
        Trả về None nếu SKU chưa có đơn nào trong cửa sổ.
        """
        from sqlalchemy import text

        start_date = end_date - timedelta(days=days - 1)

        def run(conn):
            rows = conn.execute(text("""
                WITH days AS (
                    SELECT generate_series(CAST(:start AS DATE), CAST(:end AS DATE), INTERVAL '1 day')::date AS d
                ),
                agg AS (
                    SELECT f.create_time::date AS d,
                           SUM(f.quantity)            AS daily_demand,
                           SUM(f.buyer_total_amount)  AS daily_revenue,
                           AVG(f.original_price)      AS avg_unit_price,
                           SUM(f.seller_discount)     AS total_discount,
                           COUNT(*)                   AS order_count
                    FROM Fact_Orders f
                    JOIN Dim_Products p ON p.product_key = f.product_key
                    WHERE p.sku = :sku
                      AND f.is_cancelled = FALSE
                      AND f.create_time >= CAST(:start AS DATE)
                      AND f.create_time <  CAST(:end AS DATE) + 1
                    GROUP BY 1
                )
                SELECT days.d, COALESCE(agg.daily_demand, 0), COALESCE(agg.daily_revenue, 0),
                       agg.avg_unit_price, COALESCE(agg.total_discount, 0), COALESCE(agg.order_count, 0)
                FROM days LEFT JOIN agg ON agg.d = days.d
                ORDER BY days.d
            """), {"sku": sku, "start": start_date, "end": end_date})
            return [
                {
                    "date": r[0],
                    "daily_demand": float(r[1]),
                    "daily_revenue": float(r[2]),
                    "avg_unit_price": float(r[3]) if r[3] is not None else None,
                    "total_discount": float(r[4]),
                    "order_count": float(r[5]),
                }
                for r in rows
            ]

        history = self._query(("history", sku, end_date, days), run)
        if not history or not any(h["daily_demand"] > 0 for h in history):
            return None

        # ffill rồi bfill giá đơn vị cho ngày không bán (khớp aggregate_daily)
        last_price = None
        for h in history:
            if h["avg_unit_price"] is None:
                h["avg_unit_price"] = last_price
            else:
                last_price = h["avg_unit_price"]
        first_price = next((h["avg_unit_price"] for h in history if h["avg_unit_price"] is not None), 0.0)
        for h in history:
            if h["avg_unit_price"] is None:
                h["avg_unit_price"] = first_price
        return history

    def get_latest_stock(self) -> Optional[Dict[str, Dict[str, Any]]]:
        """{sku: {stock_on_hand, as_of}} — snapshot mới nhất trong Fact_Inventory_Daily."""
        from sqlalchemy import text

        def run(conn):
            rows = conn.execute(text("""
                SELECT DISTINCT ON (p.sku) p.sku, i.stock_on_hand, d.full_date
                FROM Fact_Inventory_Daily i
                JOIN Dim_Products p ON p.product_key = i.product_key
                JOIN Dim_Dates d ON d.date_key = i.date_key
                ORDER BY p.sku, d.full_date DESC, i.loaded_at DESC
            """))
            return {r[0]: {"stock_on_hand": int(r[1]), "as_of": str(r[2])} for r in rows}

        stocks = self._query("latest_stock", run, ttl=30)
        return stocks or None

    def record_served_forecasts(
        self,
        sku: str,
        forecasts: List[Dict[str, Any]],
        model_name: str,
        model_version: str,
        model_source: str,
        history_source: str,
        history_end: Optional[str],
    ) -> int:
        """Lưu dự báo ML đã phát ra để đối chiếu với nhu cầu thực tế sau này.

        Không ghi dự báo heuristic/demo hoặc dự báo không có lịch sử trong warehouse.
        Ngày mục tiêu phải sau ngày cuối của lịch sử đầu vào để tránh ghi nhận dự báo
        hồi cứu như một kết quả forecast thật.
        """
        if (
            not self.enabled
            or model_source not in {"mlflow_registry", "local_joblib"}
            or history_source != "warehouse"
            or not history_end
        ):
            return 0

        try:
            history_end_date = date.fromisoformat(str(history_end)[:10])
            rows = []
            for item in forecasts:
                target_date = date.fromisoformat(str(item["date"])[:10])
                if target_date <= history_end_date:
                    continue
                rows.append({
                    "sku": sku,
                    "target_date": target_date,
                    "predicted_quantity": max(0.0, float(item["predicted_quantity"])),
                    "model_name": model_name,
                    "model_version": str(model_version),
                    "model_source": model_source,
                    "history_source": history_source,
                    "history_end_date": history_end_date,
                })
        except (KeyError, TypeError, ValueError) as e:
            logger.warning("Không thể chuẩn hóa dự báo để lưu Power BI (%s).", e.__class__.__name__)
            return 0

        if not rows or not self.available():
            return 0

        from sqlalchemy import text

        try:
            with self._get_engine().begin() as conn:
                result = conn.execute(text("""
                    INSERT INTO Fact_Forecast_Predictions (
                        product_key, target_date, predicted_quantity, model_name,
                        model_version, model_source, history_source, history_end_date
                    )
                    SELECT p.product_key, :target_date, :predicted_quantity, :model_name,
                           :model_version, :model_source, :history_source, :history_end_date
                    FROM Dim_Products p
                    WHERE p.sku = :sku
                    ON CONFLICT (product_key, target_date, model_version, history_end_date)
                    DO NOTHING
                """), rows)
            return int(result.rowcount or 0)
        except Exception as e:
            # Forecast serving phải tiếp tục được ngay cả khi phần ghi audit lỗi.
            logger.warning(
                "Không lưu được forecast vào Fact_Forecast_Predictions (%s). "
                "Hãy áp dụng lại warehouse/ddl/01_star_schema.sql.",
                e.__class__.__name__,
            )
            return 0


_repo: Optional[WarehouseRepository] = None


def get_repository() -> WarehouseRepository:
    global _repo
    if _repo is None:
        _repo = WarehouseRepository()
    return _repo


def set_repository(repo: Optional[WarehouseRepository]) -> None:
    """Thay repository (dùng trong test)."""
    global _repo
    _repo = repo
