# Module Machine Learning

**Thử nghiệm nhu cầu thưa 08/11/2026:** [Dữ liệu gốc, protocol và quyết định ứng viên](../docs/07-bao-cao-do-an/cai-thien-du-bao-2026-11-08.md). `serving/app/sparse_demand.py` cung cấp SBA/TSB và HistGradientBoosting trực tiếp chân trời 1–14 từ cùng lịch sử quan sát. Nhánh Hurdle dự báo điểm thiên về trung vị, không phải kỳ vọng lượng tồn kho. Backend học thực tế là scikit-learn 1.9.1; LightGBM lỗi native khi fit. Ứng viên lưu riêng trong output/models/forecast-improvement-2026-11-08, chưa Registry/reload serving. WAPE hồi cứu Hurdle 96,73% (bias −65,86%), chọn lồng nhau 102,15%; Poisson hiệu chỉnh 231,46% bị loại. Các mô hình phục vụ cũ tiếp tục dùng nhánh đệ quy cũ; hook trực tiếp không đổi mặc định khi chưa nạp ứng viên.

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
# Cập nhật baseline và metric ngày 07/11/2026

Baseline mới dự báo t+1 từ origin t: Naive=y[t], Seasonal Naive=y[t−6], Moving Average=mean(y[t−6..t]). Hợp đồng mới có lag_6. Artifact cũ thiếu alignment_version giữ công thức cũ; không ghi đè metric lịch sử hoặc coi baseline alignment_version là Registry version.

WAPE/bias không xác định khi tổng actual=0, trả None/JSON null. Chọn model theo WAPE gộp và protocol origin_t_plus_1_pooled_wape_v2; mean_fold_wape là chẩn đoán, không dùng thay điểm chính. MAPE hiện dùng mẫu số y+1. Xem [bằng chứng, holdout và điều kiện còn mở](../docs/07-bao-cao-do-an/khac-phuc-cam-nang-2026-11-07.md).
