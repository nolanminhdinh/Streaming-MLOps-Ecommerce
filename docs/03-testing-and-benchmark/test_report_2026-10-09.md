# Báo cáo kiểm thử hệ thống — 2026-10-09

## 1. Phạm vi và môi trường

- **Mã nguồn được kiểm tra:** workspace tại commit nền `2c9493c` (`fix(ml): align evaluation with recursive serving`), bao gồm các thay đổi cục bộ đang có trong workspace vào thời điểm kiểm thử.
- **Môi trường:** Docker Compose đang chạy; bộ test Python chạy bằng `.venv`; tải HTTP gửi tới FastAPI đang chạy trên cổng 8000.
- **Các phần đã kiểm tra:** pytest, tải HTTP tới serving, và soak test một giờ cho luồng producer → Kafka → consumer → MinIO.
- **Giới hạn:** soak test dùng topic Kafka và bucket MinIO riêng, không chạy toàn bộ ETL, huấn luyện lại mô hình hoặc toàn bộ pipeline end-to-end.

## 2. Tóm tắt kết quả

| Hạng mục | Kết quả | Đánh giá |
|---|---:|---|
| Bộ pytest | 71 đạt, 1 lỗi, 1 cảnh báo | Chưa đạt hoàn toàn |
| Soak test Kafka → MinIO | 3.600,96 giây; 5.498 gửi, 5.498 nhận; 182 file Parquet; 0% mất dữ liệu | Đạt về tính toàn vẹn dữ liệu; có lỗi consumer và gián đoạn kết nối đã tự phục hồi |
| Kiểm tra tải HTTP | 1.200 request tổng cộng; lỗi HTTP 0% ở cả bốn mức tải | Đạt về tỷ lệ lỗi; độ trễ tăng đáng kể ở tải cao |
| Health của ứng dụng | Docker container còn chạy; endpoint ứng dụng trả `degraded` | Chưa sẵn sàng phục vụ bằng model ML đã nạp |

## 3. Kết quả chi tiết

### 3.1. Pytest

Lệnh:

```powershell
& '.\.venv\Scripts\python.exe' -m pytest -q
```

Kết quả: **71 passed, 1 failed, 1 warning** trong khoảng 6,12 giây.

Test lỗi:

```text
tests/test_load_test.py::TestPowerBIExport::test_export_powerbi_csv_files
psycopg2.errors.UndefinedColumn: column d.day does not exist
```

View `vw_powerbi_executive_summary` tham chiếu `d.day`, trong khi `Dim_Dates` của schema hiện tại định nghĩa `full_date` và không có cột `day`. Cần thống nhất truy vấn với schema, ví dụ tính ngày từ `full_date` hoặc bổ sung cột và migration tương ứng. Lỗi xuất hiện khi cài các Power BI views vào PostgreSQL; test không hoàn tất bước xuất CSV.

### 3.2. Soak test một giờ

- **Thời lượng thực tế:** 3.600,96 giây.
- **Producer:** 5.498 bản ghi; 0 lỗi.
- **Consumer:** 5.498 bản ghi; báo cáo ghi nhận 1 lỗi consumer.
- **MinIO:** 182 file Parquet; kiểm tra độc lập xác nhận tổng 5.498 dòng.
- **Mất dữ liệu:** 0%.
- **RAM cuối phiên:** 3,86 MB theo báo cáo của script.

Trong lúc chạy, Kafka ghi nhận timeout khi fetch, hết hạn heartbeat/session và reconnect/rejoin coordinator. Consumer phục hồi và bắt kịp dữ liệu trước khi kết thúc. Khoảng thời gian giữa hai checkpoint ở gần cuối phiên kéo dài hơn bình thường; do đó kết quả toàn vẹn đạt nhưng độ ổn định kết nối vẫn cần cải thiện/quan sát thêm.

Script bật nén LZ4 nhưng gói `lz4` không có trong `requirements.txt`. Để chạy soak test, `kafka-python-ng==2.2.3` và `lz4==4.4.5` đã được cài riêng vào `.venv`; file requirements không được sửa trong lượt kiểm thử.

### 3.3. Tải HTTP tới FastAPI

Mỗi mức tải gửi 300 request, trộn dự báo nhu cầu, cảnh báo và health check.

| Đồng thời | Request | Throughput | Trung bình | P95 | P99 | Lỗi |
|---:|---:|---:|---:|---:|---:|---:|
| 10 users | 300 | 272,2 RPS | 36,12 ms | 54,98 ms | 63,56 ms | 0% |
| 50 users | 300 | 248,6 RPS | 188,06 ms | 261,04 ms | 276,30 ms | 0% |
| 100 users | 300 | 272,2 RPS | 308,67 ms | 380,16 ms | 391,11 ms | 0% |
| 200 users | 300 | 234,1 RPS | 553,73 ms | 719,03 ms | 736,58 ms | 0% |

Độ trễ P95 ở 200 người dùng đồng thời là 719,03 ms. Phần kết luận trong `data/load_test_summary.md` nói P95 dưới 150 ms qua HTTP, không khớp với bảng số đo; không nên dùng kết luận đó cho tới khi được cập nhật theo kết quả thực nghiệm.

### 3.4. Trạng thái Model Serving

Endpoint `/health` trả `status=degraded`, `model_loaded=false`, `model_source=heuristic_catalog`, dù Docker health check báo container còn khỏe và `warehouse_connected=true`. Kiểm tra dự báo trực tiếp cho thấy serving dùng lịch sử warehouse nhưng trả nguồn dự báo heuristic (`heuristic_history`), chưa dùng artifact ML đã nạp. Vì vậy, tỷ lệ lỗi HTTP 0% không đồng nghĩa mô hình ML đã hoạt động.

## 4. Các vấn đề cần xử lý

1. **P1 — Sửa lỗi Power BI SQL/schema:** loại bỏ tham chiếu cột `d.day` không tồn tại hoặc cập nhật schema/migration nhất quán; sau đó chạy lại test export.
2. **P1 — Nạp artifact ML cho Serving:** xác nhận cấu hình registry/artifact và health chỉ báo sẵn sàng khi model đã tải đúng; kiểm tra `/predict/demand` trả nguồn `mlflow_registry` hoặc `local_joblib` theo cấu hình.
3. **P2 — Khai báo phụ thuộc LZ4:** thêm `lz4` vào bộ requirements phù hợp để soak test chạy được trong môi trường sạch.
4. **P2 — Cập nhật kết luận báo cáo tải:** đồng bộ nội dung báo cáo với P95 đo được; điều tra độ trễ 719 ms tại 200 users.
5. **P2 — Tăng độ ổn định Kafka consumer:** điều tra timeout/heartbeat/reconnect trong soak test và chạy lại soak test sau khi điều chỉnh.

## 5. Báo cáo và tác động khi kiểm thử

- Báo cáo JSON soak test được tạo tại `data/soak_test_report.json`; các kết quả tải được làm mới tại `data/load_test_results.json` và `data/load_test_summary.md`. Thư mục `data/` bị Git ignore; báo cáo Markdown này lưu kết quả cần thiết để review trong Git.
- Soak test dùng topic riêng `ecom.soak.codex.2c9493c` và bucket riêng `ecom-soak-codex-20261009-2c9493c`; chúng được giữ lại làm dữ liệu kiểm chứng.
- Container FastAPI đã được build/recreate từ workspace để kiểm tra serving hiện tại. Các gói test được cài trong `.venv`.
- Không chỉnh sửa mã nguồn như một phần của hoạt động kiểm thử.

## 6. Ghi chú sau baseline

Đây là kết quả baseline trước đợt sửa mã. Các hành động mục 4 đã được xử lý trong phạm vi mã ở [báo cáo khắc phục Lần 3](../04-pipeline-updates/lan-03-e2e-remediation-2026-10-09.md). Đã có một lượt nghiệm thu runtime sau sửa mã ngày 2026-10-09; bảng phía trên vẫn là baseline cũ. Kết quả mới được ghi riêng dưới đây để không trộn hai lần đo.

## 7. Nghiệm thu runtime sau sửa mã — 2026-10-09

- **Ingestion:** producer gửi 29 orders và 20 inventory snapshots; consumer PostgreSQL upsert 20 SKU. Consumer MinIO flush Parquet batch mới cùng backlog Kafka.
- **ETL:** extract 18.233 rows từ 133 Parquet; clean 9.132, quarantine 0; load 9.132 vào `Fact_Orders`.
- **Feature/training/Registry:** warehouse chỉ có 10 ngày lịch; `lag_28` cần tối thiểu 30 ngày để tạo một dòng train. Feature Store có 0 dòng, training dừng với exit code 1, MLflow chưa có run/model và register dừng với exit code 1. Đây là blocker dữ liệu, không phải lỗi kết nối MLflow.
- **Serving:** `/health` HTTP 200 `degraded`; `/ready` HTTP 503 vì không có model ML. Forecast fallback trả 200 và `history_source=warehouse`; inventory trả 200 với `stock_source=warehouse_snapshot`.
- **Power BI/Monitoring:** export thành công: 1.095 Dim Dates, 20 products, 20 geography, 8 order-summary rows, 20 inventory alerts, 0 forecast rows. Prometheus query `up` ghi nhận FastAPI = 1; Grafana API health `database=ok`.
- **HTTP load:** 1.200 requests, 0% lỗi. P95: 55,91 ms (10 users), 180,32 ms (50), 236,07 ms (100), 396,25 ms (200). Ở 200 users throughput 479,3 RPS. Đo ở chế độ fallback; không dùng làm số đo hiệu năng model ML. Raw output: `data/load_test_results.json`.
- **Giới hạn:** chưa chạy drift/retraining closed-loop vì chưa có Feature Store/model đủ điều kiện. FastAPI Compose healthcheck `unhealthy` là hệ quả đúng của `/ready` 503; API liveness vẫn trả 200. Toàn tuyến chỉ có thể nghiệm thu model sau khi nạp thêm lịch sử warehouse đủ dài và lặp lại feature → train → register → `/ready`.
- **Nguồn dữ liệu:** orders được tạo bởi `ECommerceSimulator` của dự án và backlog Kafka hiện có; không dùng API thật Shopee/TikTok. Kết quả này xác nhận plumbing demo trên Docker, không phải nghiệm thu production data/integration.
