# Module Machine Learning

- `features/`: Feature engineering cho dữ liệu chuỗi thời gian (Lag features,
  Rolling statistics, Walk-Forward Split). Triển khai Tuần 5.
- `training/`: Script huấn luyện Baseline (LightGBM/XGBoost) và Deep Learning
  (LSTM/GRU), log kết quả lên MLflow. Triển khai Tuần 6.
- `mlflow/`: Cấu hình MLflow Tracking Server (kết nối MinIO lưu artifact,
  PostgreSQL lưu metadata). Triển khai Tuần 5.
