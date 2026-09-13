# Tuần 2: Dựng hạ tầng dữ liệu luồng (Data Ingestion & Simulator)

## Mục tiêu
Triển khai cụm container hóa cơ bản và luồng sinh dữ liệu thời gian thực.

## Công việc đã thực hiện
- [x] Viết `docker-compose.yml` khởi chạy Kafka, Zookeeper, MinIO, PostgreSQL
      với healthcheck đầy đủ cho từng service.
- [x] Tự động hóa việc tạo Kafka Topics (`ecom.orders.raw`, `inventory.logs`)
      qua service `kafka-init`, không cần thao tác thủ công.
- [x] Tự động hóa việc tạo MinIO bucket (`ecom-raw-lake`) qua service `minio-init`.
- [x] Viết hướng dẫn chạy chi tiết cho Docker Desktop (`docs/how-to-run-week2.md`)
      kèm bảng troubleshooting các lỗi thường gặp.
- [ ] Hoàn thiện logic sinh dữ liệu thật trong `Data_Simulator.py` (Trend/
      Seasonality/Flash Sale) — *đang thực hiện, xem mục "Việc còn lại"*.
- [ ] Nối Simulator với Kafka Producer để bắn message liên tục.

## Sản phẩm bàn giao
- `docker-compose.yml` chạy ổn định 4 service chính + 2 service init.
- `docs/how-to-run-week2.md`.

## Việc còn lại / Kế hoạch tuần tiếp theo
- Hoàn chỉnh `data_simulator/data_simulator.py`: định nghĩa cụ thể khung giờ
  Flash Sale, hệ số mùa vụ theo ngày lễ VN (nếu có thời gian, dùng thư viện
  `holidays` hoặc danh sách cứng).
- Nối `data_simulator.py` → `ingestion/producer.py` để bắn message thật vào
  Kafka liên tục (thay vì chỉ `print()`).
- Bắt đầu Tuần 3: viết Kafka Consumer ghi Parquet vào MinIO.

## Vấn đề phát sinh / Ghi chú
_(điền trong quá trình chạy thực tế trên máy Docker Desktop)_
