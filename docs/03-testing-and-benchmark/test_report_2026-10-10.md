# Báo cáo kiểm thử hệ thống — 2026-10-10

## 1. Tóm tắt

| Hạng mục | Kết quả | Nhận định |
|---|---:|---|
| Toàn bộ pytest | **76 passed, 0 failed, 1 warning** trong 2,25 giây | Đạt |
| FastAPI trong Docker | `/ready` HTTP 200; model và lịch sử warehouse đã nạp | Đạt readiness |
| Training status | `NOT_EVALUATED` | Chưa có lần kiểm tra readiness bị chặn trên dữ liệu runtime hiện tại |
| Prometheus | Nạp rule `EcommerceTrainingDataInsufficient`; `promtool` xác nhận 1 rule hợp lệ | Đạt cấu hình |
| Data readiness thiếu lịch sử | 4 test mới xác nhận coverage, ngưỡng, persist status và thông báo hành động | Đạt unit test; chưa phát webhook thật |

Model đang chạy là `ECommerceDemandForecastModel` v2, stage `Staging`, nguồn `mlflow_registry`, dùng nguồn `public_anonymized_historical_csv` với 43 đặc trưng. WAPE walk-forward là **126,08%**; trạng thái API ready chỉ xác nhận model được nạp và warehouse có lịch sử, không xác nhận độ chính xác đủ cho Production. Giữ model ở Staging và tiếp tục cải thiện baseline trước khi nâng stage.

## 2. Môi trường và cách chạy

- Ngày chạy: 2026-10-10; Docker Compose đang hoạt động.
- Python: 3.12.14 từ runtime cục bộ của Codex, pytest 9.1.1 và các package trong `.venv/Lib/site-packages`. Python launcher trong `.venv` tham chiếu Windows Store Python không truy cập được trong phiên này.
- `OMP_NUM_THREADS=1` để tránh LightGBM khởi tạo quá nhiều worker cho bộ test nhỏ.
- Lượt pytest cuối được chạy với quyền truy cập localhost và thư mục tạm cần thiết cho FastAPI TestClient, Power BI export và các test tạo tệp tạm.

Lệnh tương đương trong phiên kiểm thử:

```powershell
$env:PYTHONPATH = (Resolve-Path '.venv\Lib\site-packages').Path
$env:OMP_NUM_THREADS = '1'
& 'C:\Users\MINH\.cache\codex-runtimes\codex-primary-runtime\dependencies\python\python.exe' -m pytest -q --tb=short
```

## 3. Lịch sử các lượt kiểm thử và sửa lỗi

| Lượt | Kết quả | Phát hiện / hành động |
|---|---|---|
| Baseline 2026-10-09 | 71 passed, 1 failed, 1 warning | Power BI view dùng `Dim_Dates.day` không có trong schema; đã sửa ở Lần 3 và export runtime thành công. Chi tiết tại [baseline 09-10](test_report_2026-10-09.md). |
| Pytest đầu tiên 2026-10-10 | 64 passed, 8 failed, 1 warning | Phát hiện test Registry giả dùng local run không được phép tạo manifest, benchmark phụ thuộc API localhost ngẫu nhiên, mock retraining không trả version mới, fixture serving cũ chữ ký provenance và rò cấu hình Registry vào test local artifact. |
| Chạy lại nhóm mục tiêu | 21 passed, 1 warning | Registry test mô phỏng Registry thật; benchmark unit cố định chạy in-process; fake repository nhận `data_origin`; test local artifact cô lập `MLFLOW_TRACKING_URI`; mock retraining trả version mới hợp lệ. |
| Toàn bộ suite cuối 2026-10-10 | **76 passed, 0 failed, 1 warning** | Bổ sung 4 test cho readiness: ngưỡng đủ dữ liệu, dữ liệu thiếu, loại đơn hủy, lưu trạng thái supervisor trước khi dừng train. |

Các test sửa trong lượt này nằm tại `tests/test_deep_learning.py`, `tests/test_load_test.py`, `tests/test_monitoring.py`, `tests/test_serving_realdata.py` và `tests/test_data_readiness.py`. Đây là thay đổi test/fixture để phản ánh hợp đồng runtime hiện hành và không phụ thuộc dịch vụ tình cờ đang bật.

### Cảnh báo còn lại

Một `StarletteDeprecationWarning`: cặp `starlette.testclient` và `httpx` hiện dùng được nhưng Starlette khuyến nghị package `httpx2`. Không có test nào bị skip hoặc fail vì cảnh báo này.

## 4. Kiểm tra runtime Docker

Đã truy vấn từ trong container để tránh giới hạn socket localhost của sandbox:

- `GET /ready` → **200**, `status=ready`, `model_loaded=true`, `warehouse_connected=true`, `history_available=true`.
- Lịch sử model có `history_end=2026-08-11`, `history_data_origin=public_anonymized_historical_csv`.
- `GET /model/metadata` → model v2 Staging; model source `mlflow_registry`; 43 features.
- `GET /training/status` → `NOT_EVALUATED`, vì chưa có lần chạy training readiness nào cần ghi trạng thái trên runtime này.
- `promtool check rules /etc/prometheus/alert_rules.yml` → `SUCCESS: 1 rules found`; API Prometheus cũng liệt kê `EcommerceTrainingDataInsufficient` ở trạng thái inactive.

Trạng thái inactive là đúng khi chưa có chuỗi metric báo thiếu ngày. Nhánh dữ liệu thiếu được kiểm tra bằng test cô lập 10 ngày: báo cáo yêu cầu 92 ngày, còn thiếu 82 ngày, ghi JSON status rồi ném `InsufficientTrainingDataError` để pipeline dừng trước huấn luyện. Webhook chưa cấu hình nên lượt này xác nhận nội dung/trạng thái lưu, không xác nhận nhận tin ở hệ thống bên ngoài.

## 5. Phạm vi chưa nghiệm thu

- Không phát webhook đến Slack/Teams/email thật vì `TRAINING_ALERT_WEBHOOK_URL` chưa được cấu hình trong môi trường test.
- Không chạy thêm một lượt train runtime: dataset hiện có đã đủ coverage và model v2 đã đăng ký; kiểm thử readiness thiếu dữ liệu dùng fixture 10 ngày để không làm thay đổi model đang phục vụ.
- WAPE 126,08% chưa phù hợp để nâng model lên Production; cần xem phân phối nhu cầu, metric theo SKU và baseline thay thế.
- Kết quả tải 1.200 request của 2026-10-09 vẫn là phép đo lịch sử riêng; lượt 2026-10-10 này không đo lại throughput/P95.

## 6. Kết luận

Bộ test hiện tại đạt hoàn toàn về chức năng; API Docker ready và rule readiness được nạp. Supervisor có thể xem trạng thái tại `/training/status`, metric Prometheus và dashboard Grafana. Chất lượng dự báo và luồng nhận webhook bên ngoài vẫn cần được nghiệm thu riêng trước khi coi model đủ điều kiện Production.
