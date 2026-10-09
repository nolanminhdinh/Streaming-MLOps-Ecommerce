# Module Machine Learning

- `features/`: Feature engineering cho dữ liệu chuỗi thời gian (Lag features,
  Rolling statistics, Walk-Forward Split). Triển khai Tuần 5.
- `training/`: Script huấn luyện Baseline (LightGBM/XGBoost) và Deep Learning
  (LSTM/GRU), log kết quả lên MLflow. Triển khai Tuần 6.
- `mlflow/`: Cấu hình MLflow Tracking Server (kết nối MinIO lưu artifact,
  PostgreSQL lưu metadata). Triển khai Tuần 5.

## Đánh giá forecast nhiều ngày

- Walk-forward chia fold theo ngày của nhãn (`target_date = date + 1`) để biên fold
  khớp với ngày nhu cầu thực tế.
- Test mặc định 14 ngày được dự báo đệ quy theo từng SKU. Sau mỗi bước, dự báo vừa
  tạo được đưa vào lịch sử để dựng lag, rolling và EMA cho ngày tiếp theo; nhãn thực
  trong test chỉ dùng để tính metric.
- `build_feature_store` lưu thêm `data/daily_history.parquet` để khôi phục ngữ cảnh
  lịch sử đầy đủ. Run MLflow mới được gắn `evaluation_strategy=recursive_multi_step`;
  đăng ký champion bỏ qua các run cũ chưa có nhãn này.

## Phạm vi của metric hiện tại

Dữ liệu mô phỏng hiện dùng để kiểm tra luồng xử lý dữ liệu, tạo Feature Store, chạy
quy trình huấn luyện, ghi nhận run, đăng ký model và phục vụ dự báo. WAPE/MAE trên dữ
liệu này chỉ là chỉ số kiểm tra kỹ thuật trong bộ dữ liệu mô phỏng; không chứng minh
độ chính xác trên dữ liệu hay hoạt động của một doanh nghiệp thực tế. Tên “champion”
chỉ biểu thị run đứng đầu trong lần so sánh hiện tại để hoàn thiện luồng quản lý model.
Muốn đánh giá khả năng dự báo thực tế cần nạp dữ liệu doanh nghiệp rồi chạy lại pipeline.
