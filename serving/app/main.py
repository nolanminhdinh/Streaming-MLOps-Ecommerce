"""
main.py — FastAPI Model Serving & Quản trị tồn kho
---------------------------------------------------
Cung cấp REST API dự báo nhu cầu và cảnh báo nhập hàng thông minh:
  - Tải Champion Model từ MLflow Model Registry / Manifest (In-Memory Cache).
  - Endpoint POST /predict/demand: Dự báo sản lượng theo SKU và khoảng ngày (y_hat >= 0).
  - Endpoint POST /predict/batch: Dự báo sản lượng hàng loạt cho nhiều SKU.
  - Endpoint GET /inventory/reorder-alert: Quét toàn bộ kho hàng và sinh cảnh báo ROP / Safety Stock.
  - Endpoint POST /inventory/reorder-alert: Thẩm định tồn kho tùy biến theo số lượng thực tế.
  - Endpoint GET /health & GET /model/metadata: Giám sát trạng thái sẵn sàng của dịch vụ.
  - Endpoint GET /metrics: Chỉ số phục vụ cho Prometheus / Grafana.
"""

from __future__ import annotations

import json
import logging
import os
import time
from contextlib import asynccontextmanager
from datetime import date, datetime, timezone
from typing import Any, Dict, List, Optional

try:
    from serving.app.inventory_service import get_inventory_service
    from serving.app.model_loader import get_model_manager
    from serving.app.schemas import (
        BatchDemandPredictRequest,
        BatchDemandPredictResponse,
        DailyForecast,
        DemandPredictRequest,
        DemandPredictResponse,
        HealthResponse,
        ModelMetadataResponse,
        ReorderAlertItem,
        ReorderAlertRequest,
        ReorderAlertResponse,
    )
except ImportError:
    from app.inventory_service import get_inventory_service
    from app.model_loader import get_model_manager
    from app.schemas import (
        BatchDemandPredictRequest,
        BatchDemandPredictResponse,
        DailyForecast,
        DemandPredictRequest,
        DemandPredictResponse,
        HealthResponse,
        ModelMetadataResponse,
        ReorderAlertItem,
        ReorderAlertRequest,
        ReorderAlertResponse,
    )

logger = logging.getLogger("mlops.serving.api")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")

START_TIME = time.time()
REQUEST_COUNTER = {"predict_demand": 0, "predict_batch": 0, "reorder_alert": 0}

try:
    from fastapi import FastAPI, HTTPException, Query, status
    from fastapi.middleware.cors import CORSMiddleware
    from fastapi.responses import JSONResponse
    HAS_FASTAPI = True
except ImportError:
    HAS_FASTAPI = False
    logger.warning("FastAPI chưa được cài đặt trong môi trường hiện tại. Chạy ở chế độ logic core.")

    class FastAPI:  # type: ignore
        def __init__(self, *args: Any, **kwargs: Any):
            self.title = kwargs.get("title", "")
        def get(self, *args: Any, **kwargs: Any):
            def decorator(f: Any): return f
            return decorator
        def post(self, *args: Any, **kwargs: Any):
            def decorator(f: Any): return f
            return decorator
        def add_middleware(self, *args: Any, **kwargs: Any): pass

    class HTTPException(Exception):  # type: ignore
        def __init__(self, status_code: int, detail: str):
            self.status_code = status_code
            self.detail = detail

    def Query(default: Any = None, **kwargs: Any) -> Any:  # type: ignore
        return default


# ─────────────────────────────────────────────────────────────
# LIFESPAN & APPLICATION SETUP
# ─────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Khởi tạo tài nguyên trước khi phục vụ request (Model Cache)."""
    logger.info("═" * 60)
    logger.info("  KHỞI ĐỘNG FASTAPI MODEL SERVING SERVICE")
    logger.info("═" * 60)
    manager = get_model_manager()
    manager.load_model()
    inv_service = get_inventory_service()
    logger.info(f"✓ Model Serving đã sẵn sàng. Uptime bắt đầu: {datetime.now(timezone.utc).isoformat()}")
    yield
    logger.info("Đang dừng dịch vụ Model Serving...")


app = FastAPI(
    title="E-Commerce Demand Forecasting & Inventory Alert API",
    description=(
        "Hệ thống API phục vụ mô hình dự báo nhu cầu chuỗi thời gian đa kênh (Shopee, TikTok) "
        "và cảnh báo điểm đặt hàng lại (Reorder Point) / Tồn kho an toàn (Safety Stock)."
    ),
    version="1.0.0",
    lifespan=lifespan if HAS_FASTAPI else None,
)

if HAS_FASTAPI:
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )


# ─────────────────────────────────────────────────────────────
# ENDPOINTS
# ─────────────────────────────────────────────────────────────

@app.get("/", tags=["General"])
def root() -> Dict[str, Any]:
    """Trang chủ chào mừng và tài liệu hướng dẫn."""
    manager = get_model_manager()
    meta = manager.get_metadata()
    return {
        "service": "E-Commerce Demand Forecasting API",
        "status": "online",
        "version": "1.0.0",
        "champion_model": meta.get("model_registry_name"),
        "model_version": meta.get("version"),
        "model_stage": meta.get("stage"),
        "docs_url": "/docs",
        "health_check": "/health",
    }


@app.get("/health", response_model=HealthResponse, tags=["Monitoring"])
def health_check() -> HealthResponse:
    """Kiểm tra độ sẵn sàng và trạng thái của mô hình phục vụ."""
    manager = get_model_manager()
    meta = manager.get_metadata()
    uptime = round(time.time() - START_TIME, 1)

    return HealthResponse(
        status="healthy" if manager.is_loaded else "degraded",
        service="fastapi-demand-serving",
        version="1.0.0",
        model_loaded=manager.is_loaded,
        model_name=meta.get("model_registry_name", "ECommerceDemandForecastModel"),
        model_version=str(meta.get("version", "1")),
        model_stage=meta.get("stage", "Staging"),
        champion_algorithm=meta.get("champion_algorithm", "LightGBM_Tuned"),
        uptime_seconds=uptime,
    )


@app.get("/model/metadata", response_model=ModelMetadataResponse, tags=["Model Registry"])
def get_model_metadata() -> ModelMetadataResponse:
    """Truy vấn metadata chi tiết của Champion Model được đăng ký ở MLflow Model Registry."""
    manager = get_model_manager()
    meta = manager.get_metadata()

    return ModelMetadataResponse(
        model_registry_name=meta.get("model_registry_name", "ECommerceDemandForecastModel"),
        version=str(meta.get("version", "1")),
        stage=meta.get("stage", "Staging"),
        champion_algorithm=meta.get("champion_algorithm", "LightGBM_Tuned"),
        status=meta.get("status", "READY_FOR_SERVING"),
        registered_at=meta.get("registered_at", datetime.now(timezone.utc).isoformat()),
        input_feature_count=int(meta.get("input_feature_count", 28)),
        metrics=meta.get("metrics", {"cv_wape": 24.5, "cv_mae": 1.85, "cv_rmse": 2.6}),
        inventory_defaults={
            "lead_time_days": meta.get("lead_time_days", 3),
            "service_level": meta.get("service_level", 0.95),
        },
    )


@app.post("/predict/demand", response_model=DemandPredictResponse, tags=["Inference"])
def predict_demand(request: DemandPredictRequest) -> DemandPredictResponse:
    """
    Dự báo nhu cầu cho 1 SKU trong khoảng thời gian [from_date, to_date]:
      - Áp dụng các đặc trưng chuỗi thời gian, mùa vụ theo thứ, ngày đôi Flash Sale/Mega-sale.
      - Đảm bảo toàn bộ giá trị dự báo không âm (predicted_quantity >= 0).
      - Tùy chọn trả về cận trên/dưới của khoảng tin cậy 95%.
    """
    REQUEST_COUNTER["predict_demand"] += 1

    if request.from_date > request.to_date:
        raise HTTPException(
            status_code=400,
            detail=f"Ngày bắt đầu ({request.from_date}) không được lớn hơn ngày kết thúc ({request.to_date})."
        )

    # Giới hạn khoảng dự báo tối đa 90 ngày cho 1 request
    delta_days = (request.to_date - request.from_date).days + 1
    if delta_days > 90:
        raise HTTPException(
            status_code=400,
            detail=f"Khoảng thời gian dự báo vượt quá 90 ngày (yêu cầu: {delta_days} ngày)."
        )

    manager = get_model_manager()
    meta = manager.get_metadata()
    prod_info = manager.get_product_info(request.sku)

    # Thực hiện dự báo
    forecast_items = manager.predict_range(
        sku=request.sku,
        from_date=request.from_date,
        to_date=request.to_date,
        confidence_interval=bool(request.confidence_interval),
    )

    total_qty = round(sum(item["predicted_quantity"] for item in forecast_items), 1)

    daily_models = [
        DailyForecast(
            date=item["date"],
            predicted_quantity=item["predicted_quantity"],
            lower_bound=item.get("lower_bound"),
            upper_bound=item.get("upper_bound"),
        )
        for item in forecast_items
    ]

    return DemandPredictResponse(
        sku=request.sku,
        product_name=prod_info["name"],
        category=prod_info["category"],
        from_date=str(request.from_date),
        to_date=str(request.to_date),
        total_predicted_demand=total_qty,
        forecast=daily_models,
        model_name=meta.get("model_registry_name", "ECommerceDemandForecastModel"),
        model_version=str(meta.get("version", "1")),
        model_stage=meta.get("stage", "Staging"),
    )


@app.post("/predict/batch", response_model=BatchDemandPredictResponse, tags=["Inference"])
def predict_batch(request: BatchDemandPredictRequest) -> BatchDemandPredictResponse:
    """Dự báo nhu cầu hàng loạt cho danh sách nhiều SKU."""
    REQUEST_COUNTER["predict_batch"] += 1
    results: List[DemandPredictResponse] = []

    for req in request.requests:
        res = predict_demand(req)
        results.append(res)

    return BatchDemandPredictResponse(
        total_skus=len(results),
        results=results,
    )


@app.get("/inventory/reorder-alert", response_model=ReorderAlertResponse, tags=["Inventory Management"])
def get_reorder_alerts(
    lead_time_days: int = Query(3, ge=1, le=30, description="Lead time (ngày)"),
    service_level: float = Query(0.95, ge=0.50, le=0.99, description="Mức độ phục vụ (Service level)"),
) -> ReorderAlertResponse:
    """
    Quét tự động toàn bộ danh mục sản phẩm và sinh danh sách cảnh báo tồn kho:
      - Tính Safety Stock = Z * sigma * sqrt(L).
      - Tính Reorder Point = d * L + Safety Stock.
      - Phân cấp theo độ ưu tiên: CRITICAL (báo động đỏ) -> WARNING (cảnh báo) -> NORMAL (an toàn).
    """
    REQUEST_COUNTER["reorder_alert"] += 1
    inv_service = get_inventory_service()
    return inv_service.evaluate_all(
        lead_time_days=lead_time_days,
        service_level=service_level,
    )


@app.post("/inventory/reorder-alert", response_model=ReorderAlertResponse, tags=["Inventory Management"])
def post_reorder_alerts(request: ReorderAlertRequest) -> ReorderAlertResponse:
    """Thẩm định cảnh báo tồn kho tùy biến với mức tồn kho thực tế (Current Stock) được gửi lên."""
    REQUEST_COUNTER["reorder_alert"] += 1
    inv_service = get_inventory_service()
    return inv_service.evaluate_all(
        skus=request.skus,
        custom_stocks=request.current_stocks,
        lead_time_days=request.lead_time_days or 3,
        service_level=request.service_level or 0.95,
    )


@app.get("/metrics", tags=["Monitoring"])
def get_metrics(
    format: Optional[str] = Query(None, description="Định dạng trả về: 'prometheus' hoặc 'json'")
) -> Any:
    """Cung cấp các metrics vận hành phục vụ giám sát Prometheus / Grafana."""
    uptime = round(time.time() - START_TIME, 1)
    inv_service = get_inventory_service()
    alert_summary = inv_service.evaluate_all()
    manager = get_model_manager()
    meta = manager.get_metadata()

    # Kiểm tra tỷ lệ data drift nếu tệp báo cáo tồn tại
    drift_ratio = 0.0
    drift_file = os.path.join(
        os.path.dirname(__file__), "..", "..", "data", "monitoring_reports", "drift_summary.json"
    )
    if os.path.exists(drift_file):
        try:
            with open(drift_file, "r", encoding="utf-8") as f:
                ddata = json.load(f)
                drift_ratio = float(ddata.get("drift_share", 0.0))
        except Exception:
            pass

    if format == "json":
        return {
            "uptime_seconds": uptime,
            "request_counts": REQUEST_COUNTER,
            "inventory_status": {
                "total_skus": alert_summary.total_skus_evaluated,
                "critical_alerts": alert_summary.critical_count,
                "warning_alerts": alert_summary.warning_count,
                "normal_skus": alert_summary.normal_count,
            },
            "model_status": {
                "loaded": manager.is_loaded,
                "name": meta.get("model_registry_name"),
                "version": meta.get("version"),
                "stage": meta.get("stage"),
            },
            "data_drift": {
                "drift_ratio": drift_ratio,
            },
        }

    # Định dạng Prometheus text exposition format (mặc định cho scraper)
    lines = [
        "# HELP ecommerce_uptime_seconds Service uptime in seconds",
        "# TYPE ecommerce_uptime_seconds gauge",
        f"ecommerce_uptime_seconds {uptime}",
        "",
        "# HELP ecommerce_api_requests_total Total number of API requests by endpoint",
        "# TYPE ecommerce_api_requests_total counter",
        f'ecommerce_api_requests_total{{endpoint="/predict/demand"}} {REQUEST_COUNTER["predict_demand"]}',
        f'ecommerce_api_requests_total{{endpoint="/predict/batch"}} {REQUEST_COUNTER["predict_batch"]}',
        f'ecommerce_api_requests_total{{endpoint="/inventory/reorder-alert"}} {REQUEST_COUNTER["reorder_alert"]}',
        "",
        "# HELP ecommerce_inventory_alerts_total Current inventory alert counts by severity level",
        "# TYPE ecommerce_inventory_alerts_total gauge",
        f'ecommerce_inventory_alerts_total{{level="critical"}} {alert_summary.critical_count}',
        f'ecommerce_inventory_alerts_total{{level="warning"}} {alert_summary.warning_count}',
        f'ecommerce_inventory_alerts_total{{level="normal"}} {alert_summary.normal_count}',
        "",
        "# HELP ecommerce_model_info Metadata about the loaded champion model",
        "# TYPE ecommerce_model_info gauge",
        f'ecommerce_model_info{{name="{meta.get("model_registry_name", "ECommerceDemandForecastModel")}",version="{meta.get("version", "1")}",stage="{meta.get("stage", "Staging")}",algorithm="{meta.get("champion_algorithm", "LightGBM_Tuned")}"}} 1',
        "",
        "# HELP ecommerce_data_drift_ratio Share of input features detected with statistical drift",
        "# TYPE ecommerce_data_drift_ratio gauge",
        f"ecommerce_data_drift_ratio {drift_ratio}",
    ]
    body = "\n".join(lines) + "\n"
    if HAS_FASTAPI:
        from fastapi.responses import Response
        return Response(content=body, media_type="text/plain; version=0.0.4; charset=utf-8")
    return body
