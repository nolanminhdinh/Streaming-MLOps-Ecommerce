# Nâng cấp sau kiểm thử hệ thống — Lần 4: Training Data Readiness

> **Ngày:** 2026-10-10
> **Phạm vi:** ngăn train/retrain khi lịch sử chưa đáp ứng walk-forward; báo trạng thái thiếu dữ liệu để supervisor biết cần nạp thêm lịch sử.
> **Kiểm thử cuối:** 76 passed, 0 failed, 1 warning; runtime FastAPI `/ready` trả 200.
> **Báo cáo test:** [test_report_2026-10-10.md](../03-testing-and-benchmark/test_report_2026-10-10.md).

> **Minh bạch hỗ trợ:** Phần rà soát, cập nhật test và biên soạn tài liệu trong lượt này có Codex hỗ trợ; kết quả được đối chiếu bằng pytest và kiểm tra runtime nêu dưới đây.

## 1. Bối cảnh và nguyên nhân

Nghiệm thu ngày 2026-10-09 đã tìm thấy warehouse chỉ có 10 ngày lịch. Feature lag 28 không tạo được hàng huấn luyện nên train/register phải dừng. Sau đó hai CSV lịch sử đã được import riêng vào warehouse; pipeline hiện đã train được model Staging và API trả ready. Tuy nhiên, khi hệ thống được nối với nguồn dữ liệu vận hành, coverage có thể lại thiếu hoặc bị gián đoạn. Trước thay đổi này, trạng thái thiếu dữ liệu chỉ hiện trong log/exit code, chưa có báo cáo máy đọc được và thông tin rõ ràng cho supervisor.

## 2. Thay đổi đã thực hiện

| Khu vực | Thay đổi | Tác động |
|---|---|---|
| Readiness gate | Thêm `ml/training/data_readiness.py`; tính số ngày cần từ số fold, độ dài validation/test, train tối thiểu và lag lớn nhất | Mặc định 3 folds cần 63 ngày evaluation và 92 ngày lịch sử nguồn (`63 + lag 28 + target 1`). Closed-loop 2 folds cần 78 ngày lịch sử. |
| Training | `train_baseline.py` đánh giá dữ liệu đơn hàng trước feature engineering; sau khi dựng feature store, kiểm tra lại số ngày/target đủ dùng | Thiếu dữ liệu thì persist báo cáo và thoát trước cấu hình MLflow/model. Không tạo artifact hay đăng ký model từ coverage không đạt. |
| Closed-loop retraining | `trigger_retraining.py` trả `BLOCKED_INSUFFICIENT_DATA`, ghi stage `data_readiness` và giữ nguyên manifest/model hiện tại | Không gọi Registry/reload khi chưa đủ lịch sử; audit ghi số ngày thiếu và hành động cần làm. |
| Supervisor status | Lưu `data/training_status.json` hoặc đường dẫn `TRAINING_STATUS_PATH`; thêm `GET /training/status` | Hiển thị `INSUFFICIENT_DATA`, số ngày có/yêu cầu/còn thiếu, nguồn và hướng khắc phục. Endpoint chỉ trả trường notification đã lọc, không lộ URL/token webhook. |
| Notification | Webhook tùy chọn qua `TRAINING_ALERT_WEBHOOK_URL`; token tùy chọn qua `TRAINING_ALERT_WEBHOOK_TOKEN` | Gửi cảnh báo khi trạng thái/coverage thay đổi và thông báo khi dữ liệu phục hồi; lỗi gửi không xóa trạng thái đã lưu. Mặc định không gọi ra ngoài khi chưa cấu hình URL. |
| Monitoring | Prometheus scrape training readiness; thêm `EcommerceTrainingDataInsufficient`; Grafana có panel coverage/training status | Supervisor có thể nhận biết trạng thái chặn qua metric, alert và dashboard thay vì chỉ đọc log container. |
| Docker training | Thêm service `trainer` profile `training`, mount workspace có quyền ghi cho training status/model artifacts | Tách tác vụ train khỏi container FastAPI vốn mount thư mục dữ liệu read-only. |
| Provenance model | Bảng lịch sử CSV riêng và nguồn `public_anonymized_historical_csv`; Registry/serving dùng đúng origin; class baseline phục vụ có module ổn định | Không trộn dòng lịch sử ẩn danh vào khóa đơn của `Fact_Orders`; artifact có nguồn dữ liệu nhất quán khi register và serve. |
| Kiểm thử | Sửa test fixtures để dùng Registry run/version hợp lệ, cô lập benchmark khỏi API tình cờ đang chạy, cập nhật repository giả theo chữ ký `data_origin`; thêm `tests/test_data_readiness.py` | Bộ test tái lập được giữa môi trường local và Docker, đồng thời kiểm tra nhánh thiếu dữ liệu. |

## 3. Hợp đồng trạng thái thiếu dữ liệu

Khi coverage không đủ, status JSON/API cung cấp `status=INSUFFICIENT_DATA`, `available_history_days`, `required_history_days`, `missing_history_days`, `history_start`, `history_end`, số ngày evaluation và `action`. Lệnh train dừng có chủ đích; model đang phục vụ không bị thay thế. Closed-loop audit dùng `BLOCKED_INSUFFICIENT_DATA` để phân biệt rõ với training lỗi hoặc registration lỗi.

Notification chỉ gửi khi webhook được cấu hình và trạng thái/coverage có thay đổi, tránh lặp cùng một cảnh báo ở mỗi lần kiểm tra. Sau khi nguồn lịch sử được nạp đủ, pipeline lưu trạng thái phục hồi và mới tiếp tục bước train. Cấu hình webhook là tùy chọn; không commit URL hoặc token vào repo.

## 4. Cách vận hành

1. Đặt `TRAINING_MIN_FIT_DAYS` nếu muốn tăng số ngày train tối thiểu; mặc định là 14. Nhu cầu coverage sẽ được tính cùng cấu hình walk-forward và max lag.
2. Nếu cần báo supervisor, đặt `TRAINING_ALERT_WEBHOOK_URL` và tùy chọn `TRAINING_ALERT_WEBHOOK_TOKEN` trong `.env`/secret store, sau đó khởi động lại service liên quan.
3. Chạy train trong container có quyền ghi status/artifact:

   ```bash
   docker compose run --rm trainer ml/training/train_baseline.py \
     --experiment-name demand-forecasting-baseline \
     --data-origin public_anonymized_historical_csv
   ```

4. Nếu lệnh báo thiếu dữ liệu, xem `data/training_status.json`, gọi `GET http://localhost:8000/training/status`, xem metric `ecommerce_training_history_days{kind="missing"}` và alert `EcommerceTrainingDataInsufficient`; nạp thêm dữ liệu lịch sử vào warehouse rồi chạy lại.
5. Chỉ đăng ký/reload model sau khi train thành công và kiểm tra `/ready` cùng `/model/metadata`.

## 5. Kết quả kiểm thử sau thay đổi

- Pytest cuối: **76 passed, 0 failed, 1 warning**. Cảnh báo duy nhất là Starlette khuyến nghị `httpx2` thay cho cách tích hợp TestClient hiện tại; test vẫn đạt.
- Bốn test readiness xác nhận 92 ngày mặc định, 78 ngày cho hai fold, loại đơn hủy khỏi coverage, thiếu 82 ngày với fixture 10 ngày, lưu trạng thái rồi mới raise, và READY đúng ở coverage 92 ngày.
- Docker `/ready`: 200; model v2 Staging từ MLflow, 43 features, history end 2026-08-11.
- Prometheus: `promtool` báo một rule hợp lệ và API xác nhận alert đã được load.
- `/training/status` runtime hiện trả `NOT_EVALUATED`, vì dữ liệu hiện có đủ và chưa có lần chạy nào bị gate. Webhook không cấu hình nên chưa có xác nhận giao nhận bên ngoài.

## 6. Hành động tiếp theo

- Cấu hình webhook trong môi trường triển khai và thực hiện một lần diễn tập với fixture/warehouse staging thiếu dữ liệu; xác nhận người trực nhận cảnh báo.
- Model v2 còn ở Staging với WAPE 126,08%; phân tích lỗi theo SKU và cải thiện baseline trước khi xem xét Production.
- Khi nạp lịch sử mới, chạy lại train → register → reload; xác nhận audit/version và dự báo dùng cùng `data_origin`.
