# Module Monitoring

- `prometheus/`: file cấu hình `prometheus.yml` scrape metrics từ FastAPI.
- `grafana/`: dashboard JSON export (Requests/sec, Latency P95/P99, CPU/RAM).
- `evidently/`: script kiểm định Data Drift giữa tập train và dữ liệu mới,
  cùng logic kích hoạt cảnh báo tái huấn luyện (closed-loop retraining —
  xem khuyết điểm đã ghi nhận trong docs/architecture.md).

Triển khai ở Tuần 8.

## Nhánh pipeline khi chưa đủ dữ liệu huấn luyện

`ml/training/train_baseline.py` kiểm tra độ phủ lịch sử trước khi tạo feature và kết nối MLflow.
Mặc định, cấu hình 3 fold cần tối thiểu **92 ngày lịch**: 29 ngày khởi động `lag_28`/target
và 63 ngày cho train, validation cùng các test window (`14 + 7 + 3 × 14`). Luồng closed-loop
hiện chạy 2 fold nên ngưỡng tương ứng là 78 ngày. Có thể tăng số ngày train tối thiểu bằng
`TRAINING_MIN_FIT_DAYS`; lag lớn nhất lấy từ cấu hình feature pipeline hiện tại.

Nếu thiếu dữ liệu, pipeline kết thúc ở trạng thái `INSUFFICIENT_DATA` trước khi tạo model hoặc
đăng ký version mới. Báo cáo `data/training_status.json` ghi số ngày hiện có, số ngày cần có,
số ngày còn thiếu, khoảng ngày nguồn và hành động đề xuất. Model đang phục vụ không bị thay đổi.
Closed-loop retraining ghi `BLOCKED_INSUFFICIENT_DATA` vào audit log rồi dừng, không gọi registry
hay model reload.

Supervisor có thể xem trạng thái tại `GET /training/status`, trên Grafana ở panel **TRAINING DATA
READINESS**, hoặc tại Prometheus `/alerts` sau khi rule `EcommerceTrainingDataInsufficient`
chuyển sang firing. Mặc định dashboard vẫn báo được dù chưa cấu hình hệ thống gửi tin ra ngoài.

Để gửi trực tiếp tới Slack-compatible webhook hoặc HTTP receiver, đặt `TRAINING_ALERT_WEBHOOK_URL`
trong `.env`; nếu đích cần Bearer token thì đặt thêm `TRAINING_ALERT_WEBHOOK_TOKEN`. Thông báo chỉ
được gửi khi trạng thái thiếu dữ liệu mới xuất hiện, số ngày thiếu thay đổi, hoặc dữ liệu đã đủ trở
lại; lỗi webhook được ghi trong report và không che mất cảnh báo trên dashboard.

Ví dụ với Docker Compose, một lần chạy training có thể được supervisor khởi động lại sau khi nạp
thêm lịch sử:

```powershell
docker compose run --rm trainer ml/training/train_baseline.py
```

Khi chưa đủ, lệnh trả exit code 2 và hướng dẫn số ngày cần nạp thêm; sau khi đủ, lệnh đi tiếp qua
feature engineering, walk-forward training và MLflow tracking.
