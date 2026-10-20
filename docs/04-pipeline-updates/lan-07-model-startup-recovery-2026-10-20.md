# Khắc phục lần 7 — Phục hồi model sau lỗi Registry lúc khởi động

Ngày thực hiện: **20/10/2026**. Phạm vi: lỗi API không nạp model sau startup timeout và `/ready` trả 503. Không huấn luyện, đăng ký version mới hoặc thay model manifest.

## 1. Nguyên nhân

Lượt audit 19/10 xác nhận MLflow timeout lúc API khởi động. Manager chuyển sang heuristic và đặt `is_loaded=true`. Các request sau chỉ kiểm tra cờ đã khởi tạo, nên không thử nạp model lại dù Registry hồi phục.

Đầu lượt sửa 20/10, FastAPI unhealthy và các dependency chưa chạy đầy đủ. Cần cả bản sửa phục hồi lẫn khởi động lại dependency để kiểm chứng model trên runtime.

## 2. Thay đổi

- Manager chưa có model và có cấu hình Registry sẽ kiểm tra thời điểm retry khi được truy cập qua `get_model_manager`, bao gồm các readiness probe định kỳ của Docker.
- Retry chạy trong thread nền, không bắt request readiness chờ tải Registry/artifact. Trong lúc chưa có model, readiness vẫn trả 503 đúng trạng thái.
- Backoff mặc định 5, 10, 20, 40 rồi tối đa 60 giây; có thể cấu hình `MODEL_LOAD_RETRY_INITIAL_SECONDS` và `MODEL_LOAD_RETRY_MAX_SECONDS` trong Compose.
- Một khóa đồng bộ giữ tối đa một lần load đang chạy; request đồng thời không tạo thêm lượt tải. Manual reload dùng cùng khóa.
- Trạng thái model/spec/manifest ứng viên chỉ được công bố sau bước load, giữ nguyên lock và trạng thái backoff của manager. Nếu đã có model tốt, recovery không tự thay version đang ghim; reload thất bại tiếp tục giữ model cũ.
- Bắt lỗi ngoài dự kiến trong quá trình tạo ứng viên để giải phóng khóa và lên lịch thử lại.
- Log startup ghi `model_ready` và nguồn thực tế thay vì luôn thông báo model đã sẵn sàng.

Retry này được kích hoạt bởi request/probe, không có scheduler tải model độc lập khi hoàn toàn không có request. Docker Compose hiện đã có probe `/ready` mỗi 10 giây. Model Registry vẫn được ghim theo version trong manifest.

Các tệp chính: `serving/app/model_loader.py`, `serving/app/main.py`, `docker-compose.yml`, `.env.example`, `tests/test_model_recovery.py`.

## 3. Kiểm thử

Bốn regression mới xác nhận:

1. Lần đầu thất bại, lần sau phục hồi; readiness trả 503 ngay khi worker bị giữ chờ, sau đó chuyển ready.
2. Các probe trước thời điểm retry không tải model; backoff tăng và dừng ở giới hạn cấu hình.
3. Model tốt không bị recovery tự thay; reload lỗi giữ model và cùng khóa đồng bộ.
4. Ngoại lệ ngoài dự kiến không khóa vĩnh viễn quá trình phục hồi.

Nhóm mới: **4 passed**. Toàn suite với seed 20261019: **110 passed, 30 warnings trong 5,51 giây**. Warnings thuộc Starlette/httpx và PyArrow/pandas, giống đợt kiểm tra trước. JUnit lưu tại `docs/07-bao-cao-do-an/evidence/pytest_recovery_2026-10-20.xml`.

## 4. Triển khai và kiểm tra dịch vụ thật

Đã build image FastAPI từ workspace và chạy `docker compose up -d postgres minio mlflow fastapi`, giữ nguyên volume dữ liệu. Kết quả:

| Kiểm tra | Sau sửa và triển khai |
|---|---|
| FastAPI Docker health | healthy |
| PostgreSQL / MinIO / MLflow | healthy / healthy / healthy |
| `/ready` | HTTP 200, model_loaded=true, warehouse_connected=true |
| `/health` | HTTP 200, healthy |
| Nguồn model đang phục vụ | mlflow_registry |
| Phiên bản | ECommerceDemandForecastModel v2, Staging, moving_average |
| Feature columns | 43 |
| Request dự báo SKU lịch sử | HTTP 200, model_source=mlflow_registry, history_source=warehouse |
| Quantity kiểm tra | Hữu hạn, không âm |
| Số dòng audit forecast trước/sau replay | 6 / 6 |

Request dùng lại SKU/ngày đã có trong audit để kiểm tra inference, không tạo thêm bản ghi forecast. Model vẫn là version 2, không train hoặc chọn lại champion.

## 5. Thử phục hồi có sử dụng artifact thật

Trong một tiến trình riêng chạy bên trong container FastAPI, lượt đầu `_try_load_mlflow` được giả lập không tải được model. Sau backoff cấu hình 1 giây cho probe này, retry nền gọi Registry và đọc artifact thật. Kết quả: hai lần thử, lần đầu không model, lần hai nạp version 2 từ Registry. Thao tác lên lịch retry mất khoảng 0,0038 giây trong lượt đo này.

Không dừng MLflow, không đổi manager của tiến trình API và không thay version đang phục vụ. Phép thử xác nhận lỗi lần đầu rồi hồi phục với artifact thật trong tiến trình cô lập; không phải thử lỗi mạng toàn hệ thống hoặc benchmark latency.

Hiện vật:

- `docs/07-bao-cao-do-an/evidence/model_recovery_runtime_2026-10-20.json`.
- `docs/07-bao-cao-do-an/evidence/model_recovery_probe_2026-10-20.json`.
- `docs/07-bao-cao-do-an/evidence/pytest_recovery_2026-10-20.xml`.

## 6. Phạm vi còn lại

Lỗi không nạp lại model sau startup failure đã được xử lý và API hiện ready. Lịch sử vẫn kết thúc 11/08/2026 và metric v2 vẫn WAPE 126,08%; bản sửa không làm dữ liệu mới hơn hoặc tăng độ chính xác model. Các vấn đề coverage, BI, validation và cảnh báo trong báo cáo audit vẫn cần xử lý riêng.

## 7. Minh bạch sử dụng AI

AI hỗ trợ phân tích nguyên nhân, sửa mã, thêm regression test, build/triển khai API và lưu bằng chứng kiểm tra. Không thay dữ liệu nguồn, kết quả audit lịch sử hoặc phiên bản model đã đăng ký.
