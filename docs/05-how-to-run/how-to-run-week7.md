# Hướng dẫn chạy trọn vẹn luồng MLOps Tuần 7: FastAPI Model Serving & Quản trị Tồn kho

Tài liệu này hướng dẫn cách khởi chạy dịch vụ FastAPI Model Serving trong Docker, kiểm thử các API Endpoints dự báo sản lượng bán hàng (Demand Forecasting) và cảnh báo điểm đặt hàng lại (Reorder Point / Safety Stock) qua giao diện Swagger UI và dòng lệnh `curl`.

---

## 1. Khởi động Container FastAPI Serving

Khởi động service `fastapi` thông qua Docker Compose (được liên kết với mạng nội bộ `ecom-net` cùng PostgreSQL, MinIO và MLflow):

```bash
# Khởi động service fastapi ở chế độ nền
docker compose up -d fastapi

# Kiểm tra trạng thái container và healthcheck
docker compose ps fastapi
```

> `/health` là liveness của tiến trình. Docker readiness check dùng `/ready`, chỉ xanh khi có model ML đã nạp và có ngày đơn hàng gần nhất trong warehouse. Trước khi nạp dữ liệu/model, container có thể đang chạy nhưng chưa ready.

Nếu muốn chạy trực tiếp bằng Python cục bộ (trong môi trường ảo `.venv`):
```bash
uvicorn serving.app.main:app --host 0.0.0.0 --port 8000 --reload
```

---

## 2. Kiểm tra Swagger UI & Tài liệu API trực quan

Mở trình duyệt web và truy cập một trong hai địa chỉ sau để tương tác trực tiếp với API:
- **Swagger UI (Interactive Docs)**: **`http://localhost:8000/docs`**
- **ReDoc (Detailed Spec)**: **`http://localhost:8000/redoc`**

---

## 3. Kiểm thử các API Endpoints bằng `curl`

### 3.1. Kiểm tra liveness (`GET /health`) và readiness (`GET /ready`)
```bash
curl -s http://localhost:8000/health
curl -i http://localhost:8000/ready
```

`/health` có thể trả `status=degraded`, `model_loaded=false`; `/ready` trả HTTP 503 cho tới khi model và warehouse history sẵn sàng. Không coi `200` từ `/health` là bằng chứng model đã hoạt động.

---

### 3.2. Truy vấn thông số Champion Model (`GET /model/metadata`)
```bash
curl -s http://localhost:8000/model/metadata
```
Metadata phản ánh manifest/version được nạp trong môi trường hiện tại. Không dùng các metric hoặc version mẫu cũ trong báo cáo làm số đo model đang chạy; kiểm tra `model_source` và metric thật.

---

### 3.3. Dự báo sản lượng nhu cầu cho 1 SKU (`POST /predict/demand`)
Dự báo lượng tiêu thụ cho sản phẩm `OL-IP15PM` (Ốp lưng iPhone 15 Pro Max) từ ngày `2026-10-01` đến `2026-10-07`, kèm khoảng tin cậy 95%:

```bash
curl -X POST http://localhost:8000/predict/demand \
  -H "Content-Type: application/json" \
  -d '{
    "sku": "OL-IP15PM",
    "from_date": "2026-10-01",
    "to_date": "2026-10-07",
    "confidence_interval": true
  }'
```

**Dạng response minh họa** (các giá trị thay đổi theo dữ liệu và model đang chạy):
```json
{
  "sku": "OL-IP15PM",
  "product_name": "Ốp lưng iPhone 15 Pro Max",
  "category": "Phụ kiện điện thoại",
  "from_date": "2026-10-01",
  "to_date": "2026-10-07",
  "total_predicted_demand": 0.0,
  "forecast": [
    {"date": "2026-10-01", "predicted_quantity": 0.0, "lower_bound": 0.0, "upper_bound": 0.0}
  ],
  "model_name": "ECommerceDemandForecastModel",
  "model_version": "1",
  "model_stage": "Staging",
  "model_source": "mlflow_registry",
  "history_source": "warehouse"
}
```

Danh sách `forecast` có một phần tử cho mỗi ngày trong khoảng yêu cầu. Nếu `model_source` là `heuristic_history` thì API đang dùng fallback, không phải ML model.

---

### 3.4. Quét toàn bộ kho hàng để phát hiện SKU cần nhập gấp (`GET /inventory/reorder-alert`)
Hệ thống tự động tính Safety Stock và Reorder Point cho toàn bộ danh mục và sắp xếp các SKU theo mức độ nguy cấp (`CRITICAL` -> `WARNING` -> `NORMAL`):

```bash
curl -s "http://localhost:8000/inventory/reorder-alert?lead_time_days=3&service_level=0.95"
```

**Kết quả mẫu:**
```json
{
  "total_skus_evaluated": 19,
  "skus_needing_reorder": 8,
  "critical_count": 4,
  "warning_count": 4,
  "normal_count": 11,
  "alerts": [
    {
      "sku": "KCN-ANESSA",
      "product_name": "Kem chống nắng Anessa SPF50+",
      "category": "Mỹ phẩm",
      "matrix_segment": "AZ",
      "current_stock": 13,
      "avg_daily_demand": 19.5,
      "lead_time_days": 3,
      "safety_stock": 20,
      "reorder_point": 79,
      "needs_reorder": true,
      "alert_level": "CRITICAL",
      "days_until_stockout": 0.7,
      "recommended_reorder_qty": 203
    }
  ]
}
```

---

### 3.5. Kiểm tra cảnh báo tồn kho với số lượng thực tế tùy biến (`POST /inventory/reorder-alert`)
Người dùng có thể truyền vào danh sách SKU và số lượng tồn kho thực tế hiện tại để hệ thống phân tích tức thì:

```bash
curl -X POST http://localhost:8000/inventory/reorder-alert \
  -H "Content-Type: application/json" \
  -d '{
    "skus": ["OL-IP15PM", "KCN-ANESSA", "OMO-6KG"],
    "current_stocks": {
      "OL-IP15PM": 10,
      "KCN-ANESSA": 5,
      "OMO-6KG": 150
    },
    "lead_time_days": 3,
    "service_level": 0.95
  }'
```

---

### 3.6. Truy vấn Metrics vận hành hệ thống (`GET /metrics`)
```bash
curl -s http://localhost:8000/metrics
```

---

## 4. Chạy kiểm thử tự động (Unit Tests)

Chạy bộ unit test kiểm tra ModelManager, InventoryService và các công thức quản trị tồn kho:

```bash
# Kiểm thử riêng tầng Serving Tuần 7
python -m unittest tests/test_serving.py

# Hoặc kiểm thử toàn bộ dự án
python -m unittest discover tests
```

> **Kỳ vọng:** Toàn bộ test cases đều đạt trạng thái **OK**.

