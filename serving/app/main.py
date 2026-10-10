"""
main.py — FastAPI Model Serving & Quản trị tồn kho
---------------------------------------------------
Cung cấp REST API dự báo nhu cầu và cảnh báo nhập hàng thông minh:
  - Tải Champion Model từ MLflow Model Registry / artifact cục bộ (In-Memory Cache).
  - Endpoint POST /predict/demand: Dự báo sản lượng theo SKU và khoảng ngày (y_hat >= 0),
    dựng đặc trưng từ lịch sử nhu cầu thật trong Data Warehouse.
  - Endpoint POST /predict/batch: Dự báo sản lượng hàng loạt cho nhiều SKU.
  - Endpoint GET /inventory/reorder-alert: Quét toàn bộ kho hàng và sinh cảnh báo ROP / Safety Stock.
  - Endpoint POST /inventory/reorder-alert: Thẩm định tồn kho tùy biến theo số lượng thực tế.
  - Endpoint GET /health & GET /ready: Phân biệt liveness với readiness model + warehouse.
  - Endpoint GET /metrics: Chỉ số Prometheus (prometheus_client: counter, histogram độ trễ, gauge).
"""

from __future__ import annotations

import json
import logging
import os
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

try:
    from serving.app.data_access import get_repository
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
    from app.data_access import get_repository
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

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram, generate_latest
from prometheus_client.exposition import CONTENT_TYPE_LATEST

logger = logging.getLogger("mlops.serving.api")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")

START_TIME = time.time()

try:
    from fastapi import FastAPI, HTTPException, Query, Request, status
    from fastapi.middleware.cors import CORSMiddleware
    from fastapi.responses import JSONResponse, Response
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
        def middleware(self, *args: Any, **kwargs: Any):
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
# PROMETHEUS METRICS (registry riêng → không lẫn metric mặc định của process, an toàn đa luồng)
# ─────────────────────────────────────────────────────────────

REGISTRY = CollectorRegistry()
TRACKED_ENDPOINTS = ["/predict/demand", "/predict/batch", "/inventory/reorder-alert"]

REQUESTS_TOTAL = Counter(
    "ecommerce_api_requests", "Total number of API requests by endpoint",
    ["endpoint"], registry=REGISTRY,
)
REQUEST_LATENCY = Histogram(
    "ecommerce_api_request_duration_seconds", "API request latency in seconds",
    ["endpoint", "method", "status"], registry=REGISTRY,
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0),
)
PREDICTION_SOURCE_TOTAL = Counter(
    "ecommerce_predictions", "Forecast responses by model source (phát hiện fallback heuristic)",
    ["model_source"], registry=REGISTRY,
)
UPTIME = Gauge("ecommerce_uptime_seconds", "Service uptime in seconds", registry=REGISTRY)
INVENTORY_ALERTS = Gauge(
    "ecommerce_inventory_alerts_total", "Current inventory alert counts by severity level",
    ["level"], registry=REGISTRY,
)
MODEL_INFO = Gauge(
    "ecommerce_model_info", "Metadata about the loaded champion model",
    ["name", "version", "stage", "algorithm", "source"], registry=REGISTRY,
)
MODEL_READY = Gauge(
    "ecommerce_model_ready", "1 nếu đang phục vụ bằng mô hình ML thật, 0 nếu heuristic dự phòng",
    registry=REGISTRY,
)
DATA_DRIFT_RATIO = Gauge(
    "ecommerce_data_drift_ratio", "Share of input features detected with statistical drift",
    registry=REGISTRY,
)
TRAINING_DATA_SUFFICIENT = Gauge(
    "ecommerce_training_data_sufficient",
    "Training data readiness: 1=ready, 0=insufficient, -1=not evaluated",
    registry=REGISTRY,
)
TRAINING_HISTORY_DAYS = Gauge(
    "ecommerce_training_history_days",
    "Training history coverage by kind: available, required, or missing calendar days",
    ["kind"], registry=REGISTRY,
)
TRAINING_STATUS = Gauge(
    "ecommerce_training_status", "Latest training pipeline status (one active status series)",
    ["status"], registry=REGISTRY,
)
for _ep in TRACKED_ENDPOINTS:
    REQUESTS_TOTAL.labels(endpoint=_ep)  # khởi tạo series = 0 để dashboard có dữ liệu ngay


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
    get_inventory_service()
    logger.info("✓ Model Serving sẵn sàng (model_source=%s). Uptime bắt đầu: %s",
                manager.model_source, datetime.now(timezone.utc).isoformat())
    yield
    logger.info("Đang dừng dịch vụ Model Serving...")


app = FastAPI(
    title="E-Commerce Demand Forecasting & Inventory Alert API",
    description=(
        "Hệ thống API phục vụ mô hình dự báo nhu cầu chuỗi thời gian đa kênh (Shopee, TikTok) "
        "và cảnh báo điểm đặt hàng lại (Reorder Point) / Tồn kho an toàn (Safety Stock)."
    ),
    version="1.2.0",
    lifespan=lifespan if HAS_FASTAPI else None,
)

if HAS_FASTAPI:
    # CORS_ALLOW_ORIGINS="https://a.com,https://b.com"; mặc định "*" cho demo.
    # Trình duyệt từ chối "*" kèm credentials, nên chỉ bật credentials khi liệt kê origin cụ thể.
    _origins = [o.strip() for o in os.getenv("CORS_ALLOW_ORIGINS", "*").split(",") if o.strip()]
    app.add_middleware(
        CORSMiddleware,
        allow_origins=_origins,
        allow_credentials="*" not in _origins,
        allow_methods=["GET", "POST"],
        allow_headers=["*"],
    )

    @app.middleware("http")
    async def prometheus_middleware(request: Request, call_next):
        """Đo độ trễ & đếm request theo route template (tránh bùng nổ nhãn theo URL)."""
        start = time.perf_counter()
        status_code = 500
        try:
            response = await call_next(request)
            status_code = response.status_code
            return response
        finally:
            route = request.scope.get("route")
            endpoint = getattr(route, "path", None) or "unmatched"
            if endpoint != "/metrics":
                REQUEST_LATENCY.labels(endpoint, request.method, str(status_code)).observe(
                    time.perf_counter() - start
                )
                if endpoint in TRACKED_ENDPOINTS:
                    REQUESTS_TOTAL.labels(endpoint=endpoint).inc()


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
        "version": "1.2.0",
        "champion_model": meta.get("model_registry_name"),
        "model_version": meta.get("version"),
        "model_stage": meta.get("stage"),
        "model_source": manager.model_source,
        "docs_url": "/docs",
        "health_check": "/health",
    }


@app.get("/health", response_model=HealthResponse, tags=["Monitoring"])
def health_check() -> HealthResponse:
    """
    Liveness/status: "degraded" khi rơi về heuristic dự phòng; dùng /ready để
    xác nhận có cả model ML và lịch sử kho trước khi nhận traffic dự báo.
    """
    manager = get_model_manager()
    meta = manager.get_metadata()
    uptime = round(time.time() - START_TIME, 1)

    return HealthResponse(
        status="healthy" if manager.has_model else "degraded",
        service="fastapi-demand-serving",
        version="1.2.0",
        model_loaded=manager.has_model,
        model_name=meta.get("model_registry_name", "ECommerceDemandForecastModel"),
        model_version=str(meta.get("version", "0")),
        model_stage=meta.get("stage", "Staging"),
        champion_algorithm=str(meta.get("champion_algorithm", "none")),
        uptime_seconds=uptime,
        model_source=manager.model_source,
        warehouse_connected=get_repository().available(),
    )


@app.get("/ready", tags=["Monitoring"])
def readiness_check() -> Dict[str, Any]:
    """Readiness chỉ xanh khi model ML và dữ liệu kho cần cho dự báo đều sẵn sàng."""
    manager = get_model_manager()
    repository = get_repository()
    warehouse_connected = repository.available()
    data_origin = manager.feature_spec.get("data_origin")
    last_order_date = repository.get_last_order_date(data_origin=data_origin) if warehouse_connected else None
    warehouse_connected = repository.available()
    ready = manager.has_model and warehouse_connected and last_order_date is not None
    payload = {
        "status": "ready" if ready else "not_ready",
        "model_loaded": manager.has_model,
        "warehouse_connected": warehouse_connected,
        "history_available": last_order_date is not None,
        "history_end": last_order_date.isoformat() if last_order_date else None,
        "history_data_origin": data_origin or "unverified_warehouse",
    }
    if not ready:
        return JSONResponse(status_code=503, content=payload) if HAS_FASTAPI else payload
    return payload


def _read_training_status() -> Dict[str, Any]:
    status_path = os.getenv(
        "TRAINING_STATUS_PATH",
        os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "data", "training_status.json")),
    )
    try:
        with open(status_path, "r", encoding="utf-8") as status_file:
            value = json.load(status_file)
        return value if isinstance(value, dict) else {}
    except FileNotFoundError:
        return {}
    except Exception as exc:
        logger.warning("Không đọc được training readiness status: %s", exc)
        return {}


@app.get("/training/status", tags=["Monitoring"])
def get_training_status() -> Dict[str, Any]:
    """Cho supervisor xem lần đánh giá dữ liệu huấn luyện gần nhất."""
    report = _read_training_status()
    if not report:
        return {
            "status": "NOT_EVALUATED",
            "data_sufficient": None,
            "message": "Chưa có lần chạy training readiness nào.",
        }
    # Chỉ xuất trạng thái giao nhận, không trả lỗi HTTP có thể chứa URL webhook.
    notification = report.get("notification") or {}
    safe_notification = {
        key: notification[key]
        for key in ("configured", "attempted", "delivered", "http_status", "reason")
        if key in notification
    }
    report["notification"] = safe_notification
    return report


@app.get("/model/metadata", response_model=ModelMetadataResponse, tags=["Model Registry"])
def get_model_metadata() -> ModelMetadataResponse:
    """Truy vấn metadata chi tiết của Champion Model đang phục vụ."""
    manager = get_model_manager()
    meta = manager.get_metadata()

    return ModelMetadataResponse(
        model_registry_name=meta.get("model_registry_name", "ECommerceDemandForecastModel"),
        version=str(meta.get("version", "0")),
        stage=meta.get("stage", "Staging"),
        champion_algorithm=str(meta.get("champion_algorithm", "none")),
        status=meta.get("status", "UNKNOWN"),
        registered_at=meta.get("registered_at", datetime.now(timezone.utc).isoformat()),
        input_feature_count=int(meta.get("input_feature_count", 0)),
        metrics={k: float(v) for k, v in (meta.get("metrics") or {}).items()},
        inventory_defaults={
            "lead_time_days": meta.get("lead_time_days", 3),
            "service_level": meta.get("service_level", 0.95),
        },
        model_source=manager.model_source,
        training_data_origin=meta.get("training_data_origin"),
    )


@app.post("/model/reload", tags=["Model Registry"])
def reload_model(request: Request) -> Dict[str, Any]:
    """
    Nạp lại Champion Model + manifest + feature spec (được trigger_retraining gọi sau khi retrain).
    Nếu đặt biến môi trường MODEL_RELOAD_TOKEN thì request phải gửi header X-Reload-Token khớp.
    """
    token = os.getenv("MODEL_RELOAD_TOKEN")
    if token and request.headers.get("X-Reload-Token") != token:
        raise HTTPException(status_code=403, detail="Sai hoặc thiếu X-Reload-Token")
    manager = get_model_manager()
    if not manager.load_model():
        raise HTTPException(
            status_code=503,
            detail="Không tải được model mới; model đang phục vụ được giữ nguyên nếu có.",
        )
    _ALERT_CACHE["summary"] = None  # cảnh báo tồn kho phải tính lại theo mô hình mới
    meta = manager.get_metadata()
    logger.info("✓ Đã nạp lại mô hình: v%s (%s)", meta.get("version"), manager.model_source)
    return {"status": "reloaded", "version": meta.get("version"), "model_source": manager.model_source,
            "model_ready": manager.has_model}


def _predict_one(request: DemandPredictRequest) -> DemandPredictResponse:
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

    try:
        result = manager.forecast(
            sku=request.sku,
            from_date=request.from_date,
            to_date=request.to_date,
            confidence_interval=bool(request.confidence_interval),
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    PREDICTION_SOURCE_TOTAL.labels(model_source=result["model_source"]).inc()
    forecast_items = result["items"]
    total_qty = round(sum(item["predicted_quantity"] for item in forecast_items), 1)

    # Chỉ lưu dự báo ML từ lịch sử warehouse. Heuristic/demo không được tính vào
    # chỉ số accuracy của mô hình trên Power BI.
    record_forecasts = getattr(get_repository(), "record_served_forecasts", None)
    if callable(record_forecasts):
        record_forecasts(
            sku=request.sku,
            forecasts=forecast_items,
            model_name=str(
                meta.get("champion_algorithm")
                or meta.get("model_registry_name", "ECommerceDemandForecastModel")
            ),
            model_version=str(meta.get("version", "0")),
            model_source=result["model_source"],
            history_source=result["history_source"],
            history_end=result.get("history_end"),
        )

    return DemandPredictResponse(
        sku=request.sku,
        product_name=prod_info["name"],
        category=prod_info["category"],
        from_date=str(request.from_date),
        to_date=str(request.to_date),
        total_predicted_demand=total_qty,
        forecast=[
            DailyForecast(
                date=item["date"],
                predicted_quantity=item["predicted_quantity"],
                lower_bound=item.get("lower_bound"),
                upper_bound=item.get("upper_bound"),
            )
            for item in forecast_items
        ],
        model_name=meta.get("model_registry_name", "ECommerceDemandForecastModel"),
        model_version=str(meta.get("version", "0")),
        model_stage=meta.get("stage", "Staging"),
        model_source=result["model_source"],
        history_source=result["history_source"],
    )


@app.post("/predict/demand", response_model=DemandPredictResponse, tags=["Inference"])
def predict_demand(request: DemandPredictRequest) -> DemandPredictResponse:
    """
    Dự báo nhu cầu cho 1 SKU trong khoảng thời gian [from_date, to_date]:
      - Đặc trưng lag/rolling/EMA dựng từ lịch sử warehouse theo data_origin của model, dự báo đệ quy nhiều ngày.
      - Đảm bảo toàn bộ giá trị dự báo không âm (predicted_quantity >= 0).
      - Tùy chọn trả về cận trên/dưới (±1.96σ nhu cầu lịch sử).
      - `model_source` cho biết kết quả đến từ mô hình ML hay heuristic dự phòng.
    """
    return _predict_one(request)


@app.post("/predict/batch", response_model=BatchDemandPredictResponse, tags=["Inference"])
def predict_batch(request: BatchDemandPredictRequest) -> BatchDemandPredictResponse:
    """Dự báo nhu cầu hàng loạt cho danh sách nhiều SKU (lịch sử dùng chung cache của repository)."""
    results: List[DemandPredictResponse] = [_predict_one(req) for req in request.requests]
    return BatchDemandPredictResponse(total_skus=len(results), results=results)


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
    inv_service = get_inventory_service()
    return inv_service.evaluate_all(
        lead_time_days=lead_time_days,
        service_level=service_level,
    )


@app.post("/inventory/reorder-alert", response_model=ReorderAlertResponse, tags=["Inventory Management"])
def post_reorder_alerts(request: ReorderAlertRequest) -> ReorderAlertResponse:
    """Thẩm định cảnh báo tồn kho tùy biến với mức tồn kho thực tế (Current Stock) được gửi lên."""
    inv_service = get_inventory_service()
    return inv_service.evaluate_all(
        skus=request.skus,
        custom_stocks=request.current_stocks,
        lead_time_days=request.lead_time_days or 3,
        service_level=request.service_level or 0.95,
    )


_ALERT_CACHE: Dict[str, Any] = {"summary": None, "timestamp": 0.0}
_CACHE_TTL = 30.0  # 30 giây cache tránh tính toán lại liên tục khi Prometheus scrape


def _get_cached_alert_summary(inv_service):
    now = time.time()
    if _ALERT_CACHE["summary"] is None or (now - _ALERT_CACHE["timestamp"]) > _CACHE_TTL:
        _ALERT_CACHE["summary"] = inv_service.evaluate_all()
        _ALERT_CACHE["timestamp"] = now
    return _ALERT_CACHE["summary"]


def _read_drift_ratio() -> float:
    drift_file = os.path.join(
        os.path.dirname(__file__), "..", "..", "data", "monitoring_reports", "drift_summary.json"
    )
    if os.path.exists(drift_file):
        try:
            with open(drift_file, "r", encoding="utf-8") as f:
                return float(json.load(f).get("drift_share", 0.0))
        except Exception as e:
            logger.warning("Không đọc được drift_summary.json: %s", e)
    return 0.0


def _refresh_training_gauges() -> Dict[str, Any]:
    report = _read_training_status()
    if not report:
        TRAINING_DATA_SUFFICIENT.set(-1)
        available = required = missing = 0
        state = "not_evaluated"
    else:
        sufficient = report.get("data_sufficient")
        TRAINING_DATA_SUFFICIENT.set(-1 if sufficient is None else (1 if sufficient else 0))
        available = int(report.get("available_history_days", 0) or 0)
        required = int(report.get("required_history_days", 0) or 0)
        missing = int(report.get("missing_history_days", 0) or 0)
        state = str(report.get("status", "unknown")).lower()
    TRAINING_HISTORY_DAYS.labels(kind="available").set(available)
    TRAINING_HISTORY_DAYS.labels(kind="required").set(required)
    TRAINING_HISTORY_DAYS.labels(kind="missing").set(missing)
    TRAINING_STATUS.clear()
    TRAINING_STATUS.labels(status=state).set(1)
    return {
        "status": state,
        "data_sufficient": None if state == "not_evaluated" else bool(report.get("data_sufficient")),
        "available_history_days": available,
        "required_history_days": required,
        "missing_history_days": missing,
        "report": report,
    }


def _refresh_gauges() -> Dict[str, Any]:
    """Cập nhật các gauge trạng thái ngay trước khi Prometheus scrape."""
    alert_summary = _get_cached_alert_summary(get_inventory_service())
    manager = get_model_manager()
    meta = manager.get_metadata()
    drift_ratio = _read_drift_ratio()
    training_status = _refresh_training_gauges()

    UPTIME.set(round(time.time() - START_TIME, 1))
    INVENTORY_ALERTS.labels(level="critical").set(alert_summary.critical_count)
    INVENTORY_ALERTS.labels(level="warning").set(alert_summary.warning_count)
    INVENTORY_ALERTS.labels(level="normal").set(alert_summary.normal_count)
    MODEL_INFO.clear()  # chỉ giữ 1 series cho model hiện tại (tránh series cũ sau khi đổi version)
    MODEL_INFO.labels(
        name=str(meta.get("model_registry_name", "ECommerceDemandForecastModel")),
        version=str(meta.get("version", "0")),
        stage=str(meta.get("stage", "Staging")),
        algorithm=str(meta.get("champion_algorithm", "none")),
        source=manager.model_source,
    ).set(1)
    MODEL_READY.set(1 if manager.has_model else 0)
    DATA_DRIFT_RATIO.set(drift_ratio)
    return {
        "alert_summary": alert_summary,
        "meta": meta,
        "drift_ratio": drift_ratio,
        "manager": manager,
        "training_status": training_status,
    }


@app.get("/metrics", tags=["Monitoring"])
def get_metrics(
    format: Optional[str] = Query(None, description="Định dạng trả về: 'prometheus' hoặc 'json'")
) -> Any:
    """Cung cấp các metrics vận hành phục vụ giám sát Prometheus / Grafana."""
    state = _refresh_gauges()

    if format == "json":
        alert_summary, meta, manager = state["alert_summary"], state["meta"], state["manager"]
        return {
            "uptime_seconds": round(time.time() - START_TIME, 1),
            "request_counts": {
                ep: REGISTRY.get_sample_value("ecommerce_api_requests_total", {"endpoint": ep}) or 0.0
                for ep in TRACKED_ENDPOINTS
            },
            "inventory_status": {
                "total_skus": alert_summary.total_skus_evaluated,
                "critical_alerts": alert_summary.critical_count,
                "warning_alerts": alert_summary.warning_count,
                "normal_skus": alert_summary.normal_count,
            },
            "model_status": {
                "loaded": manager.has_model,
                "source": manager.model_source,
                "name": meta.get("model_registry_name"),
                "version": meta.get("version"),
                "stage": meta.get("stage"),
            },
            "data_drift": {
                "drift_ratio": state["drift_ratio"],
            },
            "training_readiness": {
                key: state["training_status"][key]
                for key in (
                    "status",
                    "data_sufficient",
                    "available_history_days",
                    "required_history_days",
                    "missing_history_days",
                )
            },
        }

    body = generate_latest(REGISTRY)
    if HAS_FASTAPI:
        return Response(content=body, media_type=CONTENT_TYPE_LATEST)
    return body.decode("utf-8")
