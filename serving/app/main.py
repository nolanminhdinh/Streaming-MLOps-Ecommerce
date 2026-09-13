"""
main.py — FastAPI Model Serving
--------------------------------
Cung cấp API dự báo lượng cầu và cảnh báo nhập hàng, tải mô hình tốt nhất
từ MLflow Model Registry.

Trạng thái: skeleton — hoàn thiện load model + logic ROP/Safety Stock ở Tuần 7.
"""

from datetime import date

from fastapi import FastAPI
from pydantic import BaseModel

app = FastAPI(
    title="E-Commerce Demand Forecasting API",
    description="API dự báo nhu cầu và cảnh báo tồn kho cho hệ thống TMĐT",
    version="0.1.0",
)


class DemandPredictRequest(BaseModel):
    sku: str
    from_date: date
    to_date: date


class DemandPredictResponse(BaseModel):
    sku: str
    forecast: list[dict]  # [{"date": "...", "predicted_quantity": ...}, ...]
    model_version: str


class ReorderAlertItem(BaseModel):
    sku: str
    current_stock: int
    safety_stock: int
    reorder_point: int
    needs_reorder: bool


@app.get("/health")
def health_check():
    return {"status": "ok"}


@app.post("/predict/demand", response_model=DemandPredictResponse)
def predict_demand(request: DemandPredictRequest):
    """
    TODO (Tuần 7):
    1. Load model Production từ MLflow Model Registry (cache lại, không load
       mỗi request).
    2. Chuẩn bị feature (lag, rolling) cho SKU + khoảng thời gian yêu cầu.
    3. Trả về forecast thực tế thay vì dữ liệu giả lập bên dưới.
    """
    return DemandPredictResponse(
        sku=request.sku,
        forecast=[{"date": str(request.from_date), "predicted_quantity": 0}],
        model_version="TODO",
    )


@app.get("/inventory/reorder-alert", response_model=list[ReorderAlertItem])
def reorder_alert():
    """
    TODO (Tuần 7):
    1. Lấy dự báo demand gần nhất cho từng SKU.
    2. Tính Safety Stock = z * sigma_demand * sqrt(lead_time).
    3. Tính Reorder Point = demand_trung_bình * lead_time + Safety Stock.
    4. So sánh với tồn kho hiện tại (Fact_Inventory_Daily) để trả danh sách
       SKU cần nhập hàng.
    """
    return []
