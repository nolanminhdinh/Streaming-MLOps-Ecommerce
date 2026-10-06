-- 00_create_mlflow_db.sql
-- Tạo database riêng cho metadata của MLflow Tracking Server.
-- Mục đích: không trộn các bảng nội bộ của MLflow (experiments, runs, metrics, ...)
-- vào database kho dữ liệu `ecom_warehouse` (Star Schema).
--
-- Lưu ý: script trong /docker-entrypoint-initdb.d chỉ chạy MỘT LẦN khi volume Postgres
-- còn trống. Với volume đã tồn tại, chạy `python scripts/init_warehouse.py` để tạo bổ sung.
CREATE DATABASE mlflow;
