# Tài liệu Thiết kế Kiến trúc Hệ thống

## 1. Bài toán

- **Input**: Luồng sự kiện đơn hàng E-Commerce theo thời gian thực (mã SKU, số lượng,
  thời gian, kho hàng, giá...), mô phỏng bởi `data_simulator/data_simulator.py`.
- **Output**:
  - Dự báo lượng cầu (demand forecast) theo SKU/thời gian.
  - Cảnh báo nhập hàng (Reorder Alert) dựa trên Safety Stock & Reorder Point.
  - Executive Dashboard: ma trận ABC/XYZ, Actual vs Forecast, danh sách SKU rủi ro.
- **Metric đánh giá mô hình**: MAE, RMSE, MAPE trên tập Test (Walk-Forward Validation).
- **Kịch bản vận hành thực tế**: Dữ liệu đổ về liên tục qua Kafka → tự động huấn luyện
  lại khi phát hiện Data Drift → mô hình mới được đăng ký và phục vụ qua FastAPI mà
  không cần restart thủ công (mục tiêu hướng tới, sẽ hoàn thiện dần).

## 2. Sơ đồ kiến trúc tổng thể

```
                     ┌──────────────────────┐
                     │   Data_Simulator.py   │
                     │ (Trend/Seasonality/   │
                     │   Flash Sale)         │
                     └──────────┬────────────┘
                                │ produce
                                v
                     ┌──────────────────────┐
                     │   Apache Kafka        │
                     │ ecom.orders.raw       │
                     │ inventory.logs        │
                     └──────────┬────────────┘
                                │ consume (raw)
                                v
                     ┌──────────────────────┐
                     │  MinIO (Data Lake)    │
                     │  Raw Parquet          │
                     └──────────┬────────────┘
                                │ ETL/ELT
                                v
                     ┌──────────────────────┐
                     │ PostgreSQL (Star      │
                     │ Schema Warehouse)     │
                     │ Fact_Orders,          │
                     │ Fact_Inventory_Daily, │
                     │ Dim_Products,         │
                     │ Dim_Warehouses,       │
                     │ Dim_Dates             │
                     └──────────┬────────────┘
                                │ feature engineering
                                v
                     ┌──────────────────────┐
                     │ Training: LightGBM /  │
                     │ XGBoost vs LSTM/GRU   │
                     │ (Walk-Forward Split)  │
                     └──────────┬────────────┘
                                │ log params/metrics
                                v
                     ┌──────────────────────┐
                     │ MLflow Tracking +     │
                     │ Model Registry        │
                     └──────────┬────────────┘
                                │ load best model
                                v
                     ┌──────────────────────┐
                     │ FastAPI Serving       │
                     │ /predict/demand       │
                     │ /inventory/reorder-   │
                     │ alert                 │
                     └──────────┬────────────┘
                                │ metrics/logs
                                v
        ┌───────────────────────┴────────────────────────┐
        v                                                 v
┌──────────────────┐                             ┌──────────────────┐
│ Prometheus +      │                             │ Evidently AI      │
│ Grafana           │                             │ (Data Drift)      │
│ (Latency, RPS,     │                             │ → Alert Trigger   │
│  CPU/RAM)          │                             │ → Retraining      │
└──────────────────┘                             └──────────────────┘
                                │
                                v
                     ┌──────────────────────┐
                     │ Power BI Executive    │
                     │ Dashboard (ABC/XYZ,   │
                     │ Actual vs Forecast)   │
                     └──────────────────────┘
```

## 3. Nguyên tắc thiết kế

1. **Tách bạch Raw / Warehouse (Bronze - Silver - Gold)**: dữ liệu thô luôn được giữ
   nguyên vẹn trong MinIO trước khi qua ETL, đảm bảo có thể tái xử lý (replay) nếu
   logic ETL thay đổi.
2. **Walk-Forward Validation**: không dùng random split cho dữ liệu chuỗi thời gian để
   tránh rò rỉ thông tin tương lai (data leakage).
3. **Model Registry làm nguồn chân lý duy nhất (single source of truth)** cho việc
   FastAPI biết mô hình nào đang ở trạng thái Production.
4. **Closed-loop retraining (đang hoàn thiện)**: Evidently AI phát hiện drift → gửi tín
   hiệu → job huấn luyện lại được kích hoạt → đăng ký version mới trên MLflow.
   *(Ghi chú: đây là điểm còn thiếu ở bản kế hoạch ban đầu, sẽ triển khai chi tiết ở
   Tuần 8, xem mục "Khuyết điểm cần cải thiện" bên dưới.)*

## 4. Khuyết điểm đã biết & hướng khắc phục

| Khuyết điểm | Hướng khắc phục dự kiến |
|---|---|
| Chưa có orchestration tool (Airflow/Prefect) | Đánh giá bổ sung ở Tuần 5 nếu còn thời gian; tối thiểu dùng cron job/script trigger |
| Ground Truth Matching chưa có bước cụ thể | Bổ sung job đối soát dự báo vs thực tế trong Tuần 7-8 |
| Data validation trước khi nạp Warehouse | Bổ sung rule kiểm tra schema cơ bản trong `warehouse/etl` |
| Business impact chưa được đánh giá | Bổ sung ở Tuần 9-10 khi tổng hợp báo cáo |

## 5. Ghi chú minh bạch AI

Phần khung kiến trúc này được phác thảo với sự hỗ trợ của Claude (Anthropic) dựa trên
đề cương do sinh viên cung cấp. Phần triển khai chi tiết code, cấu hình, và tinh chỉnh
theo dữ liệu thực tế do sinh viên thực hiện.
