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
## Giám sát pipeline hoàn thiện ngày 08 tháng 11 năm 2026

API `/pipeline/status` phân biệt heartbeat, số raw chờ, tuổi raw chờ, tuổi lần ETL hoàn tất và P95 từ consumer nhận đến hoàn tất mẻ. Giá trị chưa đo là null; ở Prometheus là -1. Pipeline không có dữ liệu mới không tự bị xem là lỗi freshness. P95 này không bao gồm thời gian ở nguồn/producer và không phải độ trễ HTTP.

Ba rule mới xét backlog >180 giây, P95 mẻ >120 giây và volume ngoài [0,25; 4] lần trung vị sau ít nhất 6 mẻ tham chiếu. Đây là ngưỡng local; số dòng mẻ không được diễn giải thành nhu cầu thị trường. Grafana được provision thêm 6 panel tại hàng DATA PIPELINE END TO END.

Compose có Alertmanager và receiver nội bộ. `http://localhost:9094/events` cho xem 100 nhóm cảnh báo gần nhất; toàn bộ nhật ký được giữ trong `data/monitoring/alerts*.jsonl`, có luân chuyển tệp khi lớn. Không có email/Slack/người nhận bên ngoài được cấu hình trong đợt này. [Dữ liệu kiểm thử và cảnh báo thật đã nhận](../output/pipeline-completion-2026-11-08/Ket_qua_kiem_thu.html).
