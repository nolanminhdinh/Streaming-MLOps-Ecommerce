# Tuần 2: Dựng hạ tầng dữ liệu luồng (Data Ingestion & Simulator)

## Mục tiêu
Triển khai cụm container hóa cơ bản và luồng sinh dữ liệu thời gian thực.

## Công việc đã thực hiện
- [x] Viết `docker-compose.yml` khởi chạy Kafka, Zookeeper, MinIO, PostgreSQL
      với healthcheck đầy đủ cho từng service.
- [x] Tự động hóa việc tạo Kafka Topics (`ecom.orders.raw`, `inventory.logs`)
      qua service `kafka-init`, không cần thao tác thủ công.
- [x] Tự động hóa việc tạo MinIO bucket (`ecom-raw-lake`) qua service `minio-init`.
- [x] Viết hướng dẫn chạy chi tiết cho Docker Desktop (`docs/how-to-run/how-to-run-week2.md`)
      kèm bảng troubleshooting các lỗi thường gặp.
- [x] Hoàn thiện logic sinh dữ liệu thật trong `data_simulator/data_simulator.py`
      (chuẩn hóa theo schema vietnam_ecommerce: Shopee + TikTok Shop,
      mô phỏng Flash Sale, ngày đôi Mega-sale, Trend tăng trưởng dài hạn,
      phí sàn thực tế, và vòng đời đơn hàng).
- [x] Nối Simulator với Kafka Producer (`ingestion/producer.py`) để bắn message
      thực tế vào topic `ecom.orders.raw` với retry và graceful shutdown.

## Sản phẩm bàn giao
- `docker-compose.yml` chạy ổn định 4 service chính + 2 service init.
- `docs/how-to-run/how-to-run-week2.md`.
- `data_simulator/data_simulator.py`: Bộ sinh dữ liệu đơn hàng đa kênh hoàn chỉnh.
- `ingestion/producer.py`: Kafka Producer kết nối trực tiếp với simulator.
- `tests/test_data_simulator.py`: Bộ unit test kiểm định logic sinh đơn.

## Việc còn lại / Kế hoạch tuần tiếp theo (Tuần 3)
- Triển khai Kafka Consumer gom batch ghi Parquet xuống MinIO Data Lake (`ingestion/consumer_to_minio.py`).
- Xây dựng mô hình Star Schema trên PostgreSQL (`warehouse/ddl/01_star_schema.sql`).
- Xây dựng pipeline ETL (Extract, Transform, Data Quality Validation, Load).

## Vấn đề phát sinh / Ghi chú
_(điền trong quá trình chạy thực tế trên máy Docker Desktop)_

