# Module Monitoring

- `prometheus/`: file cấu hình `prometheus.yml` scrape metrics từ FastAPI.
- `grafana/`: dashboard JSON export (Requests/sec, Latency P95/P99, CPU/RAM).
- `evidently/`: script kiểm định Data Drift giữa tập train và dữ liệu mới,
  cùng logic kích hoạt cảnh báo tái huấn luyện (closed-loop retraining —
  xem khuyết điểm đã ghi nhận trong docs/architecture.md).

Triển khai ở Tuần 8.
