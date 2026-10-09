# Báo cáo Khắc phục Luồng End-to-End — Lần 3

> **Ngày:** 2026-10-09
> **Phạm vi:** nối ingestion → lake/warehouse → feature/training/registry → serving → monitoring/retraining → Power BI
> **Trạng thái:** đã chạy nghiệm thu runtime một phần ngày 2026-10-09. Data ingestion, ETL, serving fallback, monitoring và Power BI hoạt động; nhánh ML chưa ready vì warehouse mới có 10 ngày lịch, không đủ lag 28 ngày. Chi tiết ở §5.

## 1. Baseline được rà soát

Báo cáo kiểm thử ngày 2026-10-09 ghi nhận 71 passed, 1 failed, 1 warning. Lỗi là view Power BI dùng `Dim_Dates.day`, cột không có trong schema hiện tại. Cùng lượt đo đó, HTTP có 0% lỗi nhưng P95 đạt 719,03 ms ở 200 người dùng; API trả `degraded`, không có model ML được nạp. Soak test riêng producer → Kafka → MinIO nhận đủ 5.498/5.498 bản ghi trong 3.600,96 giây, nhưng không bao gồm ETL, training hay toàn tuyến.

## 2. Sửa mã và nối lại các mắt xích

| Mắt xích | Vấn đề | Sửa đổi |
|---|---|---|
| Power BI / warehouse | View tham chiếu `d.day` không tồn tại | Tính `day` từ `Dim_Dates.full_date` trong [views_for_powerbi.sql](../../powerbi/views_for_powerbi.sql). |
| TikTok → Star Schema | Simulator gửi `shop_name`, ETL chỉ đọc `shop_id`; order ID có thể trùng giữa marketplace | Dùng `shop_name` làm shop ID dự phòng và deduplicate theo `(order_id, platform)` trong [transform.py](../../warehouse/etl/transform.py). |
| Ingestion dependencies | Producer bật LZ4 nhưng `lz4` thiếu trong requirements | Khai báo `lz4==4.4.5` trong [requirements.txt](../../requirements.txt). |
| Training → Registry | Registry mặc định tìm experiment khác experiment có model artifact; comparison runs chỉ có metrics; feature pipeline có thể tạo synthetic fallback | Chuyển registry sang `demand-forecasting-baseline`; feature spec ghi `data_source`; retraining/registration chỉ nhận Feature Store từ `warehouse` với feature columns hợp lệ. |
| Registry → Serving | Serving có thể tải version mới nhất khác manifest hoặc tráo sang file local không khớp khi Registry lỗi | Ghim version theo manifest; khi Registry được cấu hình thì không fallback sang artifact local tùy ý; reload tải candidate rồi đổi nguyên khối, giữ model cũ nếu candidate lỗi. |
| Retraining → reload | Có thể báo `SUCCESS` khi registration/reload không thực sự có model | Không tạo version/manifest giả; kiểm tra response `model_ready`, khôi phục manifest cũ nếu reload lỗi và chỉ ghi `SUCCESS` sau reload hợp lệ. |
| Serving health | Docker `/health` chỉ xác nhận API sống dù chưa có model/history | Giữ `/health` cho liveness; thêm `/ready` kiểm tra model ML và lịch sử warehouse; Docker healthcheck dùng readiness. MLflow có healthcheck để API đợi server sẵn sàng. |
| Inventory business date | Thống kê tồn kho dùng ngày hệ điều hành thay vì múi giờ nghiệp vụ | Tính ngày theo `BUSINESS_TZ` trong `inventory_service.py`. |
| Load evidence | Script luôn tuyên bố 0% lỗi và ngưỡng P95 cố định bất kể số đo | Dùng catalog động và sinh kết luận từ kết quả thực tế; phân biệt HTTP với in-process trong [run_load_test.py](../../tests/load_testing/run_load_test.py). |
| Operations dashboard | Không thể nhìn thấy readiness, fallback và latency percentiles | Bổ sung model readiness, heuristic fallback rate, P95/P99 vào dashboard Grafana. |
| Demo/scorecard | Kịch bản logic cục bộ gọi nhầm là end-to-end và báo model/metric giả định | Sửa [demo_pipeline_flow.py](../../scripts/demo_pipeline_flow.py) và [verify_pipeline_effectiveness.py](../../scripts/verify_pipeline_effectiveness.py) để ghi rõ dữ liệu tổng hợp, quarantine và phép đo in-process. |

## 3. Trạng thái dữ liệu model trong workspace

Manifest đang có trong `data/` là tệp cục bộ bị Git ignore, không có `run_id`/`model_file` và không có artifact model tương ứng trong workspace. Serving cần một lần train từ Feature Store đủ lịch sử lấy qua `Fact_Orders` và đăng ký thành công trên MLflow trước khi `/ready` xanh; Feature Store synthetic/local không được đăng ký làm model phục vụ.

Thứ tự triển khai đã được ghi lại trong [hướng dẫn nghiệm thu tuần 10](../05-how-to-run/how-to-run-week10.md): khởi động consumer trước producer; lưu orders vào MinIO và inventory snapshots vào PostgreSQL; chạy ETL; tạo Feature Store; train baseline; đăng ký cùng experiment; sau đó mới coi FastAPI ready. Dùng `/predict/demand` response provenance và `/inventory/reorder-alert` source fields để xác nhận nguồn thật.

## 4. Checklist nghiệm thu

1. Khởi động dependencies; kiểm tra Kafka, MinIO, PostgreSQL, MLflow đều healthy.
2. Chạy hai consumer rồi producer; xác nhận Parquet mới và inventory snapshot được ghi.
3. Chạy ETL `--all-dates`; xác nhận `Fact_Orders`, inventory facts và Dim tables có dữ liệu.
4. Tạo Feature Store; train/register từ cùng experiment; xác nhận artifact chứa model và feature spec.
5. Khởi động serving; yêu cầu `/ready` HTTP 200 và xác nhận `model_source=mlflow_registry`, `history_source=warehouse`.
6. Chạy export Power BI và benchmark tải; đối chiếu output mới với raw metrics.
7. Chạy drift thật; nếu đủ điều kiện retrain, kiểm tra audit `SUCCESS`, version được ghim và model vẫn ready sau reload.

## 5. Kết quả nghiệm thu runtime — 2026-10-09

### Đã chạy thành công

- Orders trong lượt này do `ECommerceSimulator` của dự án tạo và đi qua Kafka; không kết nối API thật của Shopee/TikTok. Runtime xác nhận plumbing demo, không chứng minh dữ liệu hay tích hợp production.
- PostgreSQL init không dùng `--drop-first`; các bảng và database MLflow sẵn có được giữ lại. Kafka, MinIO, PostgreSQL và MLflow đều healthy.
- Hai consumer đã chạy trước producer. Producer gửi 29 orders và snapshot 20 SKU; inventory consumer upsert 20 dòng. MinIO consumer flush batch mới và backlog vào Parquet.
- ETL đọc 18.233 dòng từ 133 file Parquet; sau chuẩn hóa còn 9.132 đơn hợp lệ, cách ly 0, upsert 9.132 dòng `Fact_Orders`.
- Power BI export chạy qua các view SQL: 1.095 ngày, 20 sản phẩm, 20 geography rows, 8 dòng tổng hợp đơn, 20 inventory alerts; forecast CSV có 0 dòng do chưa có forecast ML được lưu.
- `/health` trả 200 `degraded`; `/ready` trả 503 với `model_loaded=false`, `warehouse_connected=true`, `history_available=true`. Forecast API và reorder API trả 200 ở chế độ fallback; response ghi `history_source=warehouse` và `stock_source=warehouse_snapshot`.
- Prometheus scrape FastAPI thành công (`up=1`); Grafana `/api/health` trả `database=ok`.
- HTTP load run có 1.200 request, 0% lỗi. P95 lần lượt là 55,91 / 180,32 / 236,07 / 396,25 ms ở 10 / 50 / 100 / 200 users; mức 200 users đạt 479,3 RPS. Đây là serving fallback, không phải hiệu năng của model ML. Số liệu thô nằm ở `data/load_test_results.json`.

### Model gate còn chặn nghiệm thu toàn tuyến

Feature pipeline đọc 11.550 đơn không hủy, nhưng warehouse chỉ phủ 10 ngày lịch. Với `lag_28` và target ngày kế tiếp cần tối thiểu 30 ngày liên tục để có dù chỉ một dòng train; pipeline tạo 0 dòng. Training vì vậy dừng trước khi có run/artifact. MLflow Registry hiện không có registered model; registration trả mã lỗi khác 0 và không ghi manifest mới. FastAPI healthcheck đang `unhealthy` đúng theo readiness contract vì `/ready` chưa đạt.

Trong lượt chạy, đã bổ sung fail-fast để feature pipeline không ghi Feature Store rỗng và làm lệnh register trả exit code khác 0 khi không có champion. Không tạo hoặc đăng ký model từ dữ liệu quá ngắn. Tối thiểu 30 ngày chỉ tạo được mẫu đầu tiên; cần lịch sử dài hơn để walk-forward mặc định có giá trị đánh giá.

Drift/retraining closed-loop chưa chạy vì chưa có Feature Store/model đủ điều kiện; chạy lúc này sẽ không nghiệm thu được nhánh ML và có thể tạo audit gây hiểu nhầm.
