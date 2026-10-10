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

Chi tiết kiến trúc và luận giải thiết kế: xem [docs/00-architecture-overview/architecture.md](docs/00-architecture-overview/architecture.md).
> 📘 **Tài liệu Bối cảnh Toàn văn cho AI / Kỹ sư**: Xem tệp [`PROJECT_CONTEXT.md`](PROJECT_CONTEXT.md) để nắm toàn bộ bài toán, công thức toán, tiến độ tuần (W1 - W9) và cách vận hành.

## 3. Cấu trúc thư mục

```
streaming-mlops-ecommerce/
├── PROJECT_CONTEXT.md      # Nguồn sự thật toàn diện cho các mô hình AI & kỹ sư
├── docs/                   # Tài liệu thiết kế, báo cáo tiến độ theo tuần
├── data_simulator/         # Script mô phỏng luồng đơn hàng E-Commerce
├── ingestion/              # Kafka Producer/Consumer, ghi dữ liệu vào MinIO
├── warehouse/              # DDL Star Schema + pipeline ETL vào PostgreSQL
├── notebooks/              # EDA, phân loại ma trận ABC/XYZ
├── ml/                     # Feature engineering, huấn luyện mô hình, MLflow
├── serving/                # FastAPI Model Serving
├── monitoring/             # Prometheus, Grafana, Evidently AI
├── powerbi/                # File/báo cáo Power BI & Dữ liệu phẳng CSV
├── tests/                  # Unit tests & kịch bản kiểm thử tải đa luồng
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
| Trực quan hóa Dataflow | Apache NiFi |
| BI/Báo cáo | Microsoft Power BI |

## 5. Lộ trình thực hiện (10 tuần)

Xem chi tiết đề cương tuần tại [docs/06-weekly-progress](docs/06-weekly-progress).

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

```powershell
# Tạo file .env từ file mẫu (không commit .env lên Git)
Copy-Item .env.example .env

# Môi trường Python cho producer/consumer, ETL, training
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt

# Khởi tạo warehouse trên PostgreSQL; lệnh này không xóa dữ liệu
docker compose up -d postgres
python scripts/init_warehouse.py

# Khởi động các hạ tầng còn lại
docker compose up -d --build zookeeper kafka minio mlflow nifi prometheus grafana
docker compose ps
```

Producer, hai Kafka consumer, ETL, huấn luyện và đăng ký model chạy riêng trên host. Xem [hướng dẫn nghiệm thu end-to-end](docs/05-how-to-run/how-to-run-week10.md) để chạy đúng thứ tự. `/health` xác nhận tiến trình API còn sống; `/ready` chỉ trả HTTP 200 khi model ML và lịch sử warehouse sẵn sàng.

Hướng dẫn chi tiết từng bước cho từng giai đoạn:
- Tuần 2 (Hạ tầng Docker & Kafka): xem [docs/05-how-to-run/how-to-run-week2.md](docs/05-how-to-run/how-to-run-week2.md).
- Tuần 3 (Data Lake MinIO, Star Schema & Pipeline ETL): xem [docs/05-how-to-run/how-to-run-week3.md](docs/05-how-to-run/how-to-run-week3.md).
- Tuần 4 (EDA & Ma trận 9 ô ABC/XYZ): xem [docs/05-how-to-run/how-to-run-week4.md](docs/05-how-to-run/how-to-run-week4.md), [notebooks/eda.ipynb](notebooks/eda.ipynb), [notebooks/abc_xyz_classification.ipynb](notebooks/abc_xyz_classification.ipynb), và báo cáo [docs/06-weekly-progress/tuan-04.md](docs/06-weekly-progress/tuan-04.md).
- Tuần 5 (MLflow Tracking, Feature Store & Baseline Models): xem [docs/05-how-to-run/how-to-run-week5.md](docs/05-how-to-run/how-to-run-week5.md) và báo cáo [docs/06-weekly-progress/tuan-05.md](docs/06-weekly-progress/tuan-05.md).
- Tuần 6 (Deep Learning LSTM/GRU, Optuna Tuning, Đối chiếu ABC/XYZ & Model Registry): xem [docs/05-how-to-run/how-to-run-week6.md](docs/05-how-to-run/how-to-run-week6.md) và báo cáo [docs/06-weekly-progress/tuan-06.md](docs/06-weekly-progress/tuan-06.md).
- Tuần 7 (Model Serving FastAPI & Nghiệp vụ Quản trị Tồn kho Safety Stock/ROP): xem [docs/05-how-to-run/how-to-run-week7.md](docs/05-how-to-run/how-to-run-week7.md) và báo cáo [docs/06-weekly-progress/tuan-07.md](docs/06-weekly-progress/tuan-07.md).
- Tuần 8 (Giám sát Prometheus/Grafana, Evidently Data Drift & Tái huấn luyện Closed-Loop): xem [docs/05-how-to-run/how-to-run-week8.md](docs/05-how-to-run/how-to-run-week8.md) và báo cáo [docs/06-weekly-progress/tuan-08.md](docs/06-weekly-progress/tuan-08.md).
- Tuần 9 (Power BI Executive Dashboard, DAX Measures & Thực nghiệm Kiểm thử tải Locust): xem [docs/05-how-to-run/how-to-run-week9.md](docs/05-how-to-run/how-to-run-week9.md) và báo cáo [docs/06-weekly-progress/tuan-09.md](docs/06-weekly-progress/tuan-09.md).
- Tuần 10 (Tổng kết Toàn diện, Nghiệm thu Hệ thống & Kịch bản Bảo vệ ĐATN): xem [docs/05-how-to-run/how-to-run-week10.md](docs/05-how-to-run/how-to-run-week10.md) và báo cáo [docs/06-weekly-progress/tuan-10.md](docs/06-weekly-progress/tuan-10.md).
- **Báo cáo Cơ sở Lý thuyết & Công nghệ Toàn diện**: xem [docs/00-architecture-overview/cong_nghe_ly_thuyet.md](docs/00-architecture-overview/cong_nghe_ly_thuyet.md).
- **Kiểm thử Hiệu năng Luồng Dữ liệu**: xem [docs/03-testing-and-benchmark/bao_cao_kiem_thu_hieu_nang_lan_1.md](docs/03-testing-and-benchmark/bao_cao_kiem_thu_hieu_nang_lan_1.md).
- **Nhật ký Khắc phục Lỗi & Tối ưu hóa Luồng (Changelog)**: xem [docs/04-pipeline-updates/lan-01-khac-phuc-loi-va-toi-uu-luong.md](docs/04-pipeline-updates/lan-01-khac-phuc-loi-va-toi-uu-luong.md).
- **Báo cáo Sửa lỗi & Tối ưu Hệ thống Lần 2 (rà soát end-to-end)**: xem [docs/04-pipeline-updates/lan-02-sua-loi-va-toi-uu-he-thong.md](docs/04-pipeline-updates/lan-02-sua-loi-va-toi-uu-he-thong.md).
- **Báo cáo Khắc phục Lần 3 (rà soát liên kết và vận hành)**: xem [docs/04-pipeline-updates/lan-03-e2e-remediation-2026-10-09.md](docs/04-pipeline-updates/lan-03-e2e-remediation-2026-10-09.md).
- **Báo cáo kiểm thử hệ thống mới nhất (76 pytest đạt)**: xem [docs/03-testing-and-benchmark/test_report_2026-10-10.md](docs/03-testing-and-benchmark/test_report_2026-10-10.md).
- **Báo cáo nâng cấp Lần 4 — data readiness và cảnh báo supervisor**: xem [docs/04-pipeline-updates/lan-04-training-data-readiness-2026-10-10.md](docs/04-pipeline-updates/lan-04-training-data-readiness-2026-10-10.md).

> **Trạng thái kiểm chứng ngày 2026-10-10:** 76 pytest đạt; Docker `/ready` trả 200 với model ML v2 từ Registry và lịch sử CSV đến 2026-08-11. Model vẫn ở `Staging` vì WAPE 126,08%; không xem readiness là tiêu chí chất lượng Production. Nhánh thiếu dữ liệu có 4 unit test, status API/Prometheus alert đã được cấu hình; webhook bên ngoài chưa được xác nhận vì chưa cấu hình URL.

## 7. Minh bạch sử dụng AI

Theo yêu cầu của Cẩm nang ĐATN, các phần có sử dụng AI hỗ trợ (Claude) sẽ được ghi chú
rõ trong `docs/` và trong commit message tương ứng, phân biệt rõ với phần tự thực hiện.

## 8. Tác giả

- Sinh viên thực hiện: _(điền tên)_
- GVHD: _(điền tên)_
- Khóa/Lớp: _(điền thông tin)_
