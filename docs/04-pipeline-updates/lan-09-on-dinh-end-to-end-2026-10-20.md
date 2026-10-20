# Lần 9 — Ổn định end-to-end, 20/10/2026

Kafka → MinIO → ETL → PostgreSQL → CSV Power BI đã có dịch vụ chạy liên tục, lưu checkpoint và được kiểm chứng bằng fixture có nhãn. API nạp model Registry v2, `/ready` trả 200. Đây là xác nhận thông suốt kỹ thuật trong môi trường Docker Desktop hiện tại; chất lượng dự báo và PBIX có các điều kiện nghiệm thu riêng.

## Thay đổi

| Phần | Hành vi sau sửa |
|---|---|
| Consumer đơn | Giữ null/platform không hỗ trợ trong partition `unknown`; ETL ngày đọc cả partition này. Giữ platform gốc để audit. |
| Payload lỗi | JSON hỏng được bọc cùng nội dung gốc. Batch trộn số và chuỗi lỗi vẫn lưu được Parquet dạng payload JSON, sau đó validate/quarantine. |
| ETL | Voucher sai kiểu không làm transform dừng. Kiểm tra giá giảm, trọng lượng, độ dài trường và timestamp trước load. Quarantine hỗ trợ số vô hạn và vẫn xuất JSON hợp lệ. |
| Đơn đến muộn | Chuẩn hóa `update_time`/`updated_time`/`updatedAt`, thiếu thì dùng mốc vòng đời mới nhất. Dedupe chọn version mới nhất; DB từ chối version cũ. |
| Tồn kho | Lưu timestamp từ nguồn, chặn snapshot cũ ghi lùi. Bản tin lỗi vào quarantine trước commit; bản tin hợp lệ trong batch tiếp tục xử lý. |
| Calendar | Tạo ngày dimension còn thiếu, tránh mất snapshot do join hoặc fact có date NULL. |
| API inventory | POST chặn lead time ngoài 1–30, service level ngoài 0,50–0,99 và tồn kho âm, phù hợp giới hạn GET. |
| BI inventory | Z=1,645 và sàn SS/ROP như API. SKU NORMAL không còn đề xuất nhập dương. Nhu cầu đầu vào hai tầng vẫn phân biệt theo nguồn. |
| ML data | Bỏ backfill giá từ tương lai/chéo SKU. Readiness đếm ngày thực có quan sát, không coi hai ngày cách xa nhau là đủ lịch sử. |
| Vận hành | Thêm `pipeline-worker` giám sát hai consumer, xử lý object mới ở mọi ngày, export bảy CSV. Checkpoint chỉ tăng sau ETL và export thành công; crash trước checkpoint được replay bằng upsert. |
| Export | Ghi từng CSV qua tệp tạm rồi thay thế. Chỉ cài view lúc worker khởi động, tránh DDL mỗi chu kỳ gây deadlock với truy vấn BI. |
| NiFi | Nối failure/unmatched vào `quarantine/nifi`; PutS3 có retry/backoff và connection giữ nội dung sau retry. Giữ nguyên processor/queue cũ; bổ sung bucket cho nhánh lỗi. |
| Monitoring | Thêm rule API down, model unavailable, fallback, heartbeat pipeline và lịch sử cũ; `/ready` có tuổi lịch sử và cờ stale. |

Migration chỉ thêm `Fact_Orders.source_updated_at` và `Fact_Inventory_Daily.snapshot_at`, không xóa fact. Bộ đếm DB dùng số dòng thực ghi, không đếm version cũ bị từ chối thành thành công.

## Kiểm chứng

- Toàn suite: **145 passed**, không skipped; 50 cảnh báo tương thích thư viện, seed `20261020`, 4,01 giây. PostgreSQL integration dùng bảng/sequence tạm và rollback.
- `DI202610209733ad`: **12/12 kiểm tra đạt**, 22 sự kiện; đối soát Kafka/Parquet, khóa dòng, đa SKU, cùng SKU khác line ID, đơn hủy, cùng order ID khác sàn, ngày Việt Nam, quarantine và replay. [Báo cáo](../03-testing-and-benchmark/data_integrity_DI202610209733ad.md).
- `AUTO202610207f50a8`: 6 bản tin đơn + 3 tồn kho, **không gọi ETL/export thủ công**. Warehouse giữ đúng hai đơn COMPLETED, quantity 3+4; tồn mới nhất 50 không bị bản cũ 10 ghi lùi. Có 3 lỗi đơn, 1 lỗi tồn trong quarantine Python và 2 payload lỗi trong NiFi. CSV khớp warehouse, API đọc tồn 50, tham số sai trả 422.
- Restart: giữ 287 object đã xử lý; tổng đơn/quantity/doanh thu/tồn kho không đổi, heartbeat healthy và pending=0.
- HTTP model thật: 20 request, concurrency 4, một SKU/một ngày/đã làm nóng; 0 lỗi, `mlflow_registry` v2; P50 12,48 ms, P95 37,80 ms. Đây là smoke benchmark, chưa phải soak/capacity SLA.
- BI: 11.679 đơn không hủy, 24.534 đơn vị, buyer total 7.615.435.480,00; AOV tổng 652.062,29. Số đơn/quantity/doanh thu khớp nguồn; 0 nhóm sai AOV/số đơn. Inventory có 0 dòng thiếu date và 0 SKU NORMAL có đề xuất nhập dương.
- `/ready`=200; lịch sử model kết thúc 11/08, tuổi 70 ngày, `history_stale=true`. Sáu rule Prometheus được nạp hợp lệ. Cảnh báo lịch sử cũ phản ánh dữ liệu cần bổ sung.

Số liệu thay đổi so với lần 8 do thêm fixture `DI...`/`AUTO...` và xử lý backlog. Không diễn giải đơn thử thành dữ liệu kinh doanh thật.

## Bằng chứng

- [JUnit](../07-bao-cao-do-an/evidence/pytest_runtime_fixes_2026-10-20.xml).
- [Worker tự chạy](../07-bao-cao-do-an/evidence/continuous_pipeline_AUTO202610207f50a8.json).
- [Restart](../07-bao-cao-do-an/evidence/worker_restart_2026-10-20.json).
- [HTTP](../07-bao-cao-do-an/evidence/live_forecast_benchmark_2026-10-20.json).
- [Đối soát BI lần 9](../07-bao-cao-do-an/evidence/powerbi_2026-10-20_runtime-fixes.json).
- [API/monitoring](../07-bao-cao-do-an/evidence/serving_monitoring_2026-10-20.json).
- [Trạng thái dịch vụ cuối lượt](../07-bao-cao-do-an/evidence/services_runtime_fixes_2026-10-20.json): worker healthy, pending=0; NiFi bảy processor RUNNING và không có FlowFile chờ tại lần kiểm tra cuối.
- [ETL](../07-bao-cao-do-an/evidence/etl_warehouse_2026-10-20.json), [ingestion](../07-bao-cao-do-an/evidence/ingestion_offline_2026-10-20.json), [NiFi](../07-bao-cao-do-an/evidence/nifi_error_routes_2026-10-20.json).

Giữ bằng chứng thử chưa đạt: `AUTO202610202b88bf` phát hiện nhánh lỗi NiFi thiếu bucket; FlowFile nằm trong queue rồi được lưu sau sửa. Lượt DI trực tiếp trên Windows lỗi selector Kafka; kiểm chứng Linux Docker đạt. Một số lượt đầu không ghi được docs do mount chỉ đọc; lần thành công gắn riêng docs để ghi. Worker chính chỉ đọc mã nguồn, ghi data và powerbi/data. Đã phát hiện deadlock khi thay view trong mỗi chu kỳ, worker tự retry; mã cuối chỉ cài view khi khởi động.

## Vận hành

```powershell
docker compose up -d --build
docker compose ps
docker compose logs -f pipeline-worker
```

Worker áp dụng migration thêm cột, dùng group consumer hiện hữu và theo dõi object mới ở mọi ngày để nhận cả đơn đến muộn. Poll mặc định 30 giây, timeout flush đơn mặc định 10 giây trong worker; chỉnh bằng `PIPELINE_POLL_SECONDS`, `PIPELINE_OBJECT_BATCH_SIZE`, `CONSUMER_BATCH_TIMEOUT`. Producer/simulator chỉ phát dữ liệu khi người vận hành chủ động chạy; worker không tự sinh đơn.

Checkpoint: `data/pipeline/checkpoint.json`; sức khỏe: `data/pipeline/status.json`. Không xóa checkpoint/offset khi restart. Giữ data, powerbi/data và volume DB/MinIO khi bàn giao; dữ liệu bị Git ignore chưa có trên Git.

NiFi hiện tại đã sửa trực tiếp. Flow mới từ template cũ cần chạy `scripts/repair_nifi_routing.py` rồi `scripts/repair_nifi_error_routes.py`. Không chạy lại script deploy có thao tác xóa canvas trên flow đang sử dụng.

## Các điều kiện chưa đóng

1. Model v2 dùng nguồn CSV riêng kết thúc 11/08. Cần lịch sử mới đúng SKU/nguồn và xác nhận coverage. Không ghép dữ liệu simulator thành lịch sử thật của SKU CSV.
2. Chưa thay/huấn luyện lại model. WAPE cao, baseline alignment, metric actual toàn 0 và nghiệm thu LSTM/GRU còn là công việc ML riêng. Sửa feature/readiness áp dụng cho lượt dựng dữ liệu/huấn luyện sau; artifact cũ giữ theo phiên bản.
3. Catalog/demo fallback vẫn có nhãn nguồn. SKU thử có tồn thật nhưng nhu cầu có thể là `fallback_catalog`; smoke test không chứng minh chất lượng quyết định nhập hàng.
4. Fallback line ID theo SKU đòi hỏi tối đa một dòng/SKU/đơn; nhiều dòng phải có line ID riêng. Thiếu timestamp version chỉ suy ra từ vòng đời; cùng timestamp chưa sắp thứ tự chắc chắn.
5. Chưa refresh/render hoặc hoàn thiện trang PBIX. CSV thay an toàn từng file, chưa có giao dịch nguyên tử cho cả bảy file phía Power BI Desktop.
6. Rule Prometheus chưa có Alertmanager/người nhận; chưa nghiệm thu scheduler drift/retrain/rollback, HA hoặc mất host/volume. NiFi đã kiểm tra phục hồi nhánh thiếu bucket, chưa fault-inject mất MinIO toàn hệ thống.

Không đánh dấu các giới hạn trên đã đóng chỉ vì luồng kỹ thuật chạy qua. Mã và báo cáo lần này chưa commit/push.
