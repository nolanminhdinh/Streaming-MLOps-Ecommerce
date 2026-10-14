# Sửa dòng hàng, quarantine và inventory — 14/10/2026

**Bài 1 đạt 12/12 tiêu chí trên Kafka/MinIO/PostgreSQL thật; 106 pytest đạt.** Đã sửa các lỗi làm mất dòng SKU và thiếu audit, chưa nghiệm thu toàn bộ tải/chịu lỗi production.

## Bản sửa

- Dedupe và UNIQUE/upsert của Fact_Orders dùng `(order_id, platform, source_line_id)`. Ưu tiên `order_line_id`/`line_id` từ nguồn; giữ riêng hai dòng cùng SKU có ID khác nhau. Numeric ID `17` và `17.0` được chuẩn hóa thành cùng khóa.
- ETL chuẩn hóa toàn bộ đầu vào, validate trước rồi mới dedupe dòng hợp lệ. Không loại thiếu SKU/quantity sai trước validator; platform lạ được kiểm định thay vì bỏ qua.
- Bản ghi bị từ chối được lưu JSONL trong MinIO `quarantine/orders/run_id=.../rejected.jsonl`, kèm payload gốc, source object/row, run ID và lý do. Không báo SUCCESS hoặc nạp facts nếu ghi audit thất bại.
- Tách valid/duplicate/rejected, kiểm tra `raw = valid sau dedupe + duplicates + rejected`; không coi toàn bộ dòng bị loại là duplicate.
- Upsert cập nhật quantity, tiền và dimension theo dòng, cùng trạng thái và timestamps vòng đời.
- Power BI views/DAX và history/features streaming đếm đơn theo `(platform, order_id)`, không đếm dòng SKU như đơn mới. AOV dùng tổng tiền / số đơn.
- Moving Average suy luận trực tiếp từ shifted rolling_mean_7, tránh dựng DataFrame/43 features cho từng bước. Có parity test 90 bước đệ quy; model khác giữ đường suy luận cũ. Không cần huấn luyện lại.
- Ngày nghiệp vụ và run ID ETL dùng `Asia/Ho_Chi_Minh`.

## Áp dụng và kết quả

Đã chạy migration không dùng `--drop-first`, giữ dữ liệu cũ; source_line_id được backfill từ SKU. Dòng cũ không có product được giữ với khóa `legacy:<order_fact_id>`, cần đối soát riêng nếu nối lại nguồn. Đã cài views, rebuild API và xác nhận `/ready` 200. Model chính vẫn tải, lịch sử CSV tới 11/08/2026.

| Kiểm tra | Kết quả |
|---|---|
| Regression suite | 106 passed, 0 failed; 30 deprecation warnings |
| Kafka → MinIO | 22/22 sự kiện, 7 Parquet objects |
| Warehouse | 20/20 dòng hợp lệ, có nhiều SKU và hai dòng cùng SKU/ID riêng |
| Tổng quantity/subtotal fixture | 48 units / 508.000 VND, gồm cả đơn hủy để đối soát measure |
| Quarantine | 2/2 dòng có lý do, payload và source object lưu bền vững |
| Replay / giờ Việt Nam | Số dòng, quantity, tiền không đổi; partition đúng ở 23:59/00:01 |
| Backfill hai lượt test cũ | Mỗi lượt từ 12 lên 16 dòng, 38 units / 402.000 VND; lưu hai dòng lỗi |
| Power BI executive summary | 11.598 đơn / 7.590.770.770 VND khớp facts sau backfill fixture |
| HTTP inventory 216 SKU | 0,194 / 0,114 / 0,109 giây trong ba lượt tuần tự; số đo cũ 23,312 giây |

Backfill chỉ chọn file của hai fixture `DI20261013f7b3a3`, `DI20261013ba6be6`, không chạy toàn lake hoặc tự xác nhận coverage. Kafka đã restart sau lỗi session ZooKeeper khi khởi động stack; đây không phải thử HA. Số đo HTTP local tuần tự không đại diện P95/P99 dưới tải hoặc benchmark đối chứng cùng thời điểm.

## Giới hạn còn lại

1. Nguồn thiếu line ID dùng `sku:<SKU>` và giả định một dòng/SKU/đơn. Hai dòng cùng SKU cần ID ổn định. Khi chuyển nguồn cũ sang line ID cần migration đối soát, tránh tạo thêm dòng cũ.
2. Schema phẳng hiện dùng measure cấp dòng; adapter thực cần phân bổ tiền/discount/fee cấp đơn hợp lý, không lặp tiền cả đơn trên từng SKU.
3. Upsert chưa có source version để chặn event cũ ghi đè trạng thái mới. Replay giống nhau đã đạt; đảo thứ tự cập nhật/late arrival cần bài 2 đầy đủ.
4. Quarantine chưa có công cụ tự sửa/resolve/replay hoặc retention policy; mỗi lượt ETL có audit riêng. Schema evolution và poison message cần bài 8 đầy đủ.
5. **216 SKU catalog CSV vẫn thiếu snapshot tồn kho thực**, được gắn `simulated_demo`. Cần nạp `inventory.logs` đúng SKU từ nguồn nghiệp vụ trước khi dùng cảnh báo nhập hàng thực tế.
6. Chưa nghiệm thu load/burst/soak/scale/backup/restore/cảnh báo tới người nhận; chưa đánh giá lại độ chính xác mô hình.

## Triển khai ở môi trường khác

Backup và thử migration trên staging trước. Dừng writer/ETL cũ khi triển khai vì ON CONFLICT cũ không còn sau migration; không rollback code cũ nếu chưa có kế hoạch schema tương thích.

```powershell
.\.venv\Scripts\python.exe scripts/init_warehouse.py
.\.venv\Scripts\python.exe -m pytest tests -q
.\.venv\Scripts\python.exe scripts/run_data_integrity_check.py
docker compose build fastapi
docker compose up -d fastapi
```

Power BI exporter cài views khi chạy; với kết nối trực tiếp, áp dụng `powerbi/views_for_powerbi.sql`.

- [Bài 1 chạy lại](../03-testing-and-benchmark/data_integrity_DI2026101400c8fb.md).
- [Bằng chứng backfill/views/HTTP](../03-testing-and-benchmark/integrity_fixes_runtime_2026-10-14.json).
- [JUnit](../03-testing-and-benchmark/pytest_integrity_fixes_2026-10-14.xml).
- [Checklist production](../03-testing-and-benchmark/checklist-truoc-van-hanh-production.md).

Kiểm thử, sửa đổi và tài liệu có hỗ trợ AI (Codex).
