"""
schemas.py
----------
Pydantic schemas cho API Model Serving & Quản trị tồn kho (Tuần 7):
  - DemandPredictRequest / DemandPredictResponse
  - ReorderAlertItem / ReorderAlertResponse
  - ModelMetadataResponse / HealthResponse

Tương thích cả trong môi trường production (FastAPI/Pydantic v2)
lẫn môi trường thử nghiệm độc lập.
"""

from __future__ import annotations

from datetime import date
from typing import Any, Dict, List, Optional

try:
    from pydantic import BaseModel, Field
    HAS_PYDANTIC = True
except ImportError:
    HAS_PYDANTIC = False

    class BaseModel:  # type: ignore
        def __init__(self, **kwargs: Any):
            for k, v in kwargs.items():
                setattr(self, k, v)

        def dict(self, *args: Any, **kwargs: Any) -> Dict[str, Any]:
            return {
                k: v.dict() if hasattr(v, "dict") else v
                for k, v in self.__dict__.items()
                if not k.startswith("_")
            }

        def model_dump(self, *args: Any, **kwargs: Any) -> Dict[str, Any]:
            return self.dict()

        def __repr__(self) -> str:
            attrs = ", ".join(f"{k}={v!r}" for k, v in self.__dict__.items() if not k.startswith("_"))
            return f"{self.__class__.__name__}({attrs})"

    def Field(default: Any = ..., **kwargs: Any) -> Any:  # type: ignore
        return default


# ─────────────────────────────────────────────────────────────
# 1. SCHEMAS DỰ BÁO NHU CẦU (DEMAND PREDICTION)
# ─────────────────────────────────────────────────────────────

class DemandPredictRequest(BaseModel):
    """Yêu cầu dự báo nhu cầu cho 1 SKU trong khoảng thời gian cụ thể."""
    sku: str = Field(..., description="Mã định danh sản phẩm (SKU)")
    from_date: date = Field(..., description="Ngày bắt đầu dự báo (YYYY-MM-DD)")
    to_date: date = Field(..., description="Ngày kết thúc dự báo (YYYY-MM-DD)")
    store_id: Optional[str] = Field(None, description="Mã kho hoặc cửa hàng (tùy chọn)")
    confidence_interval: Optional[bool] = Field(False, description="Có trả về khoảng tin cậy 95% hay không")


class DailyForecast(BaseModel):
    """Sản lượng dự báo theo từng ngày."""
    date: str = Field(..., description="Ngày dự báo (YYYY-MM-DD)")
    predicted_quantity: float = Field(..., description="Sản lượng dự báo (đảm bảo >= 0)")
    lower_bound: Optional[float] = Field(None, description="Cận dưới khoảng tin cậy 95%")
    upper_bound: Optional[float] = Field(None, description="Cận trên khoảng tin cậy 95%")


class DemandPredictResponse(BaseModel):
    """Kết quả dự báo nhu cầu hoàn chỉnh."""
    sku: str = Field(..., description="Mã SKU")
    product_name: str = Field(..., description="Tên sản phẩm")
    category: str = Field(..., description="Danh mục ngành hàng")
    from_date: str = Field(..., description="Ngày bắt đầu")
    to_date: str = Field(..., description="Ngày kết thúc")
    total_predicted_demand: float = Field(..., description="Tổng sản lượng dự báo trong kỳ")
    forecast: List[DailyForecast] = Field(..., description="Chi tiết chuỗi dự báo theo ngày")
    model_name: str = Field(..., description="Tên mô hình phục vụ từ Model Registry")
    model_version: str = Field(..., description="Phiên bản mô hình")
    model_stage: str = Field(..., description="Trạng thái triển khai (Staging/Production)")
    model_source: Optional[str] = Field(None, description="mlflow_registry | local_joblib | heuristic_history | heuristic_catalog")
    history_source: Optional[str] = Field(None, description="warehouse (lịch sử thật từ Fact_Orders) | none")

    model_config = {"protected_namespaces": ()}


class BatchDemandPredictRequest(BaseModel):
    """Yêu cầu dự báo hàng loạt theo danh sách SKU."""
    requests: List[DemandPredictRequest] = Field(..., description="Danh sách các SKU cần dự báo")


class BatchDemandPredictResponse(BaseModel):
    """Kết quả dự báo hàng loạt."""
    total_skus: int = Field(..., description="Tổng số SKU đã xử lý")
    results: List[DemandPredictResponse] = Field(..., description="Danh sách kết quả dự báo")


# ─────────────────────────────────────────────────────────────
# 2. SCHEMAS CẢNH BÁO TỒN KHO & ĐẶT HÀNG LẠI (INVENTORY ALERT)
# ─────────────────────────────────────────────────────────────

class ReorderAlertRequest(BaseModel):
    """Yêu cầu quét cảnh báo tồn kho tuỳ chỉnh."""
    skus: Optional[List[str]] = Field(None, description="Danh sách SKU cần kiểm tra (None = toàn bộ catalog)")
    current_stocks: Optional[Dict[str, int]] = Field(
        None, description="Bản đồ mức tồn kho thực tế hiện tại {sku: current_stock}"
    )
    service_level: Optional[float] = Field(0.95, description="Mức độ phục vụ mong muốn (mặc định 95%)")
    lead_time_days: Optional[int] = Field(3, description="Thời gian giao hàng của nhà cung cấp (Lead time tính theo ngày)")


class ReorderAlertItem(BaseModel):
    """Báo cáo tình trạng tồn kho và đề xuất đặt hàng cho 1 SKU."""
    sku: str = Field(..., description="Mã SKU")
    product_name: str = Field(..., description="Tên sản phẩm")
    category: str = Field(..., description="Danh mục")
    matrix_segment: Optional[str] = Field("AX", description="Phân khúc ma trận ABC/XYZ (AX, AY, AZ, BX, ...)")
    current_stock: int = Field(..., description="Số lượng tồn kho thực tế hiện tại")
    avg_daily_demand: float = Field(..., description="Lượng tiêu thụ trung bình ngày")
    lead_time_days: int = Field(..., description="Thời gian chờ hàng (Lead time, ngày)")
    safety_stock: int = Field(..., description="Định mức tồn kho an toàn (SS = Z * sigma * sqrt(L))")
    reorder_point: int = Field(..., description="Điểm đặt hàng lại (ROP = d * L + SS)")
    needs_reorder: bool = Field(..., description="Cờ đánh dấu cần nhập hàng gấp (current_stock <= ROP)")
    alert_level: str = Field(..., description="Mức độ cảnh báo: CRITICAL, WARNING, NORMAL")
    days_until_stockout: float = Field(..., description="Số ngày dự kiến còn lại trước khi đứt hàng")
    recommended_reorder_qty: int = Field(..., description="Số lượng đề xuất nhập thêm để đạt mức tồn kho an toàn mục tiêu")
    stock_source: Optional[str] = Field(None, description="Nguồn tồn kho: request | warehouse_snapshot:<ngày> | simulated_demo")
    demand_source: Optional[str] = Field(None, description="Nguồn nhu cầu: model_forecast | warehouse_history | fallback_catalog")


class ReorderAlertResponse(BaseModel):
    """Báo cáo tổng hợp cảnh báo đặt hàng lại toàn hệ thống."""
    total_skus_evaluated: int = Field(..., description="Tổng số SKU đã được thẩm định")
    skus_needing_reorder: int = Field(..., description="Số SKU rơi vào ngưỡng cần đặt hàng")
    critical_count: int = Field(..., description="Số lượng SKU ở mức báo động đỏ (CRITICAL)")
    warning_count: int = Field(..., description="Số lượng SKU ở mức cảnh báo vàng (WARNING)")
    normal_count: int = Field(..., description="Số lượng SKU ở mức an toàn (NORMAL)")
    alerts: List[ReorderAlertItem] = Field(..., description="Danh sách chi tiết cảnh báo theo từng SKU")
    generated_at: str = Field(..., description="Thời điểm tạo báo cáo (ISO 8601)")


# ─────────────────────────────────────────────────────────────
# 3. SCHEMAS QUẢN TRỊ MÔ HÌNH & HỆ THỐNG
# ─────────────────────────────────────────────────────────────

class ModelMetadataResponse(BaseModel):
    """Thông tin chi tiết về Champion Model đang phục vụ."""
    model_config = {"protected_namespaces": ()}
    model_registry_name: str
    version: str
    stage: str
    champion_algorithm: str
    status: str
    registered_at: str
    input_feature_count: int
    metrics: Dict[str, float]
    inventory_defaults: Dict[str, Any]
    model_source: Optional[str] = None


class HealthResponse(BaseModel):
    """Trạng thái sức khỏe và độ sẵn sàng của Serving Service."""
    model_config = {"protected_namespaces": ()}
    status: str
    service: str
    version: str
    model_loaded: bool
    model_name: str
    model_version: str
    model_stage: str
    champion_algorithm: str
    uptime_seconds: float
    model_source: Optional[str] = None
    warehouse_connected: Optional[bool] = None
