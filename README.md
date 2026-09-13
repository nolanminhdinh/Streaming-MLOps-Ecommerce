# Streaming MLOps E-commerce — Dự báo nhu cầu hàng hóa

Đồ án tốt nghiệp: Xây dựng kiến trúc hạ tầng dữ liệu luồng (Streaming Pipeline) và MLOps
phục vụ dự báo nhu cầu hàng hóa trong Thương mại điện tử.

## 1. Mục tiêu dự án

- Xây dựng hạ tầng thu thập, lưu trữ và xử lý dữ liệu đơn hàng E-Commerce theo thời gian thực.
- Huấn luyện, quản lý vòng đời và triển khai mô hình dự báo nhu cầu (demand forecasting).
- Giám sát hệ thống, phát hiện Data Drift và cảnh báo tái huấn luyện mô hình.
- Trực quan hóa kết quả cho người ra quyết định (Executive Dashboard) qua Power BI.

## 2. Kiến trúc hệ thống

```
Data_Simulator.py --> Kafka (ecom.orders.raw, inventory.logs)
                          |
                          v
                Kafka Consumer --> MinIO (Data Lake, Raw Parquet)
                          |
                          v
              ETL/ELT --> PostgreSQL (Star Schema: Fact/Dim)
                          |
                          v
        Feature Engineering (Lag, Rolling, Walk-Forward Split)
                          |
                          v
        Training: LightGBM/XGBoost (Baseline) vs LSTM/GRU (Deep Learning)
                          |
                          v
                MLflow Tracking + Model Registry
                          |
                          v
        FastAPI Model Serving (/predict/demand, /inventory/reorder-alert)
                          |
                          v
   Prometheus/Grafana (giám sát hạ tầng) + Evidently AI (Data Drift)
                          |
                          v
                Power BI Executive Dashboard (ABC/XYZ, Actual vs Forecast)
```

Chi tiết kiến trúc và luận giải thiết kế: xem [docs/architecture.md](docs/architecture.md).

## 3. Cấu trúc thư mục

```
streaming-mlops-ecommerce/
├── docs/                   # Tài liệu thiết kế, báo cáo tiến độ theo tuần
├── data_simulator/         # Script mô phỏng luồng đơn hàng E-Commerce
├── ingestion/              # Kafka Producer/Consumer, ghi dữ liệu vào MinIO
├── warehouse/              # DDL Star Schema + pipeline ETL vào PostgreSQL
├── notebooks/              # EDA, phân loại ma trận ABC/XYZ
├── ml/                     # Feature engineering, huấn luyện mô hình, MLflow
├── serving/                # FastAPI Model Serving
├── monitoring/             # Prometheus, Grafana, Evidently AI
├── powerbi/                # File/báo cáo Power BI
├── tests/                  # Unit test / stress test
├── scripts/                # Script tiện ích (khởi tạo DB, seed data...)
├── docker-compose.yml
└── requirements.txt
```

## 4. Công nghệ sử dụng

| Thành phần | Công nghệ |
|---|---|
| Message Streaming | Apache Kafka + Zookeeper |
| Data Lake | MinIO (Object Storage) |
| Data Warehouse | PostgreSQL (Star Schema) |
| Experiment Tracking | MLflow |
| Model Serving | FastAPI |
| Giám sát hạ tầng | Prometheus + Grafana |
| Giám sát Data Drift | Evidently AI |
| BI/Báo cáo | Microsoft Power BI |

## 5. Lộ trình thực hiện (10 tuần)

Xem chi tiết đề cương tuần tại [docs/weekly-progress](docs/weekly-progress).

| Tuần | Nội dung chính |
|---|---|
| 1 | Thiết kế kiến trúc hệ thống, khởi tạo repo |
| 2 | Dựng hạ tầng Kafka/Zookeeper/MinIO/Postgres + Data Simulator |
| 3 | Lưu trữ phân tầng (Data Lake) + Star Schema |
| 4 | Tiền xử lý dữ liệu + phân loại ABC/XYZ |
| 5 | Báo cáo tiến độ + thiết lập MLflow + feature engineering |
| 6 | Huấn luyện & so sánh mô hình (Baseline vs Deep Learning) |
| 7 | Model Serving (FastAPI) + logic tồn kho |
| 8 | Giám sát hệ thống + phát hiện Data Drift |
| 9 | Power BI Dashboard + kiểm thử tải |
| 10 | Hoàn thiện báo cáo ĐATN + chuẩn bị bảo vệ |

## 6. Hướng dẫn chạy nhanh

```bash
# Bước 1: tạo file .env từ file mẫu (không commit .env lên Git)
cp .env.example .env

# Bước 2: khởi động hạ tầng dữ liệu luồng (Kafka, Zookeeper, MinIO, PostgreSQL)
docker compose up -d

# Kiểm tra các service đang chạy (kỳ vọng ở trạng thái "healthy")
docker compose ps
```

Hướng dẫn chi tiết từng bước (kèm troubleshooting) cho Docker Desktop:
xem [docs/how-to-run-week2.md](docs/how-to-run-week2.md).

> Ghi chú: Repo đang ở **Tuần 2** — hạ tầng dữ liệu luồng cơ bản đã chạy được.
> Các service MLflow/FastAPI/Prometheus/Grafana sẽ được kích hoạt dần ở
> Tuần 5, 7, 8 theo đề cương ở trên.

## 7. Minh bạch sử dụng AI

Theo yêu cầu của Cẩm nang ĐATN, các phần có sử dụng AI hỗ trợ (Claude) sẽ được ghi chú
rõ trong `docs/` và trong commit message tương ứng, phân biệt rõ với phần tự thực hiện.

## 8. Tác giả

- Sinh viên thực hiện: _(điền tên)_
- GVHD: _(điền tên)_
- Khóa/Lớp: _(điền thông tin)_
