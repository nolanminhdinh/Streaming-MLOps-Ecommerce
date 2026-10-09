# BÁO CÁO KHẮC PHỤC — ĐÁNH GIÁ DỰ BÁO NHIỀU NGÀY

> **Mã báo cáo**: `PIPE-UPDATE-RUN-03`
> **Ngày**: 09/10/2026
> **Vấn đề**: Metric walk-forward chưa mô phỏng cách serving dự báo nhiều ngày.
> **Phạm vi**: Feature Store, huấn luyện, so sánh/tuning model, đăng ký model và kiểm tra luồng Docker.

## 1. Mục tiêu

Mục tiêu của thay đổi này là làm cho quy trình đánh giá và quản lý model nhất quán với luồng serving, đồng thời kiểm tra hạ tầng có thể xử lý dữ liệu đầu-cuối. Dữ liệu hiện tại là dữ liệu mô phỏng; các metric bên dưới chỉ xác nhận pipeline kỹ thuật hoạt động, không đại diện cho độ chính xác trên dữ liệu doanh nghiệp.

## 2. Nguyên nhân

- Target `target_t_plus_1` đại diện cho nhu cầu ngày kế tiếp, nhưng fold trước đó được chia theo ngày tạo feature. Nhãn ở biên fold vì vậy có thể nằm ở một ngày khác fold mà feature row thuộc về.
- Các feature lag/rolling đã dựng sẵn cho toàn bộ tập test chứa nhu cầu thực tế của những ngày trước trong cùng horizon. Serving không có dữ liệu thực tương lai này: nó dùng dự báo của ngày vừa qua để dựng feature cho ngày tiếp theo.
- Các run MLflow cũ được chấm theo quy trình khác có thể bị chọn làm champion nếu chỉ xếp theo WAPE.

## 3. Thay đổi

- Tạo `target_date` và chia walk-forward theo ngày mà target đại diện.
- Thêm evaluator đệ quy dùng chung cho training, model comparison và hyperparameter tuning. Mỗi ngày test được dự báo từ lịch sử đến ngày trước đó; dự báo mới được đưa vào lịch sử trước khi dựng feature cho bước tiếp theo.
- Lưu `daily_history.parquet` cùng Feature Store để giữ phần lịch sử trước giai đoạn đánh giá. Với Feature Store cũ, pipeline thử khôi phục chuỗi từ nguồn đơn hàng khi dữ liệu khớp.
- Gắn `evaluation_strategy=recursive_multi_step` vào run mới. Registry chỉ chọn các run có strategy này để so sánh champion cùng giao thức.
- Ghim `setuptools` và SQLAlchemy tương thích trong Dockerfile MLflow; ghim `setuptools` tương thích trong Dockerfile serving để các image khởi động được với MLflow 2.14.
- Cập nhật tài liệu về cách đánh giá và giới hạn diễn giải metric trên dữ liệu mô phỏng.

## 4. Kiểm chứng

### Đánh giá đệ quy trong Docker

Đã chạy 3 fold, mỗi fold dự báo đệ quy 14 ngày. Mỗi phương pháp có 840 điểm được chấm.

| Phương pháp | WAPE | MAE | RMSE |
|---|---:|---:|---:|
| Naive | 50,99% | 9,0810 | 12,1833 |
| Seasonal naive | 54,90% | 9,7738 | 13,8658 |
| Moving average | 43,93% | 7,8367 | 10,5766 |
| Ridge | 40,43% | 7,1692 | 9,9261 |
| LightGBM | 40,93% | 7,2708 | 10,4188 |

Kết quả máy chạy được lưu tại `data/recursive_eval_run_20261009/recursive_metrics_docker.json` (thư mục `data` bị loại khỏi Git).

### Luồng dữ liệu và dịch vụ

- Docker Compose: Kafka, MinIO, Postgres healthy; FastAPI healthy; MLflow và NiFi khởi động.
- FastAPI kết nối được warehouse. Endpoint `/health` báo `degraded` vì chưa có model được đăng ký và service đang dùng heuristic; smoke test không tạo model giả.
- Gửi 30 đơn thử qua Kafka, ghi 2 tệp Parquet vào MinIO, rồi chạy ETL dry-run: 30/30 bản ghi qua kiểm tra, không có bản ghi quarantine. Không ghi dữ liệu thử vào warehouse.
- MLflow tracking ghi và đọc lại metric cùng artifact thành công trong experiment smoke riêng.
- Đã chạy 9 kiểm thử mục tiêu ở lượt sửa trước. Chưa chạy toàn bộ pytest trong lượt kiểm tra Docker này.

Kết quả smoke luồng dữ liệu được lưu tại `data/recursive_eval_run_20261009/dataflow_smoke_result.json` (không đưa vào Git).

## 5. Giới hạn và kết luận

Metric mới xác nhận phép đánh giá nhiều ngày không dùng actual lag trong horizon test và bám sát cách serving truyền dự báo ngày trước vào bước kế tiếp. Các con số chỉ là kiểm tra kỹ thuật trên dữ liệu mô phỏng. Sau khi thay bằng dữ liệu doanh nghiệp, cần chạy lại pipeline và đánh giá lại trước khi diễn giải WAPE/MAE như chất lượng dự báo vận hành.
