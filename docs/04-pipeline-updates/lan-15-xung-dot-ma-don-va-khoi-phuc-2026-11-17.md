# Lần 15 — Xung đột mã đơn: cách ly trong ETL và khôi phục từ kho thô (17/11/2026)

## 1. Vấn đề

Trên báo cáo Power BI, một số mã đơn mang dữ liệu không nhất quán: cùng mã nhưng khác thời điểm tạo, khác địa chỉ, khác shop. Ví dụ `SPE261116000002` chứa hai đơn, một đơn tạo lúc 21:50 và một đơn lúc 22:39 ngày 16/11.

**Nguyên nhân (lỗi ở nguồn mô phỏng):**
- `data_simulator` tạo mã đơn theo dạng `SPE<yymmdd><bộ đếm>`.
- Bộ đếm quay về `000001` mỗi khi khởi động lại producer trong cùng ngày, nên hai đơn **khác nhau** nhận cùng một mã.
- ETL upsert theo khóa `(order_id, platform, source_line_id)` nên gộp hai đơn thành một:
  - khác SKU: đơn sau thành thêm dòng của đơn trước;
  - cùng SKU: đơn sau ghi đè đơn trước, làm mất hẳn một đơn.

## 2. Quyết định của sinh viên (17/11/2026)

Chọn **phương án A**. Quy tắc "ETL giữ bản ghi đầu tiên khi trùng" **không** được áp dụng chung, vì hai lý do:
- Một đơn hàng gửi nhiều sự kiện cập nhật trạng thái (UNPAID → … → COMPLETED/CANCELLED). Giữ bản đầu tiên sẽ khóa đơn ở trạng thái đầu.
- Hai đơn khác nhau dùng chung mã không phải là bản trùng. Bỏ một bản đi là âm thầm mất doanh thu.

| Tình huống | Xử lý |
|---|---|
| Cùng mã, cùng thời điểm tạo (gửi lại, cập nhật trạng thái) | Giữ như cũ: bản có `source_updated_at` mới nhất thắng |
| Cùng mã, **khác thời điểm tạo** (xung đột danh tính) | Đơn đến trước giữ mã; các dòng còn lại bị **cách ly** với lý do `ORDER_IDENTITY_CONFLICT` |
| Đơn đã bị gộp trong lịch sử | Khôi phục từ kho thô MinIO, đơn sau nhận mã `<mã gốc>-HHMMSS` theo giờ tạo đơn |

## 3. Thay đổi

**Nguồn — `ingestion/producer.py`, `data_simulator/data_simulator.py`**
- Mã đơn gắn thêm mã lượt chạy `R<HHMMSS giờ khởi động>`, ví dụ `SPE261117R083015000001`.
- Mỗi lần chạy lại producer có một dải mã riêng.
- Test: `test_restarted_producer_never_reuses_order_ids`.

**ETL — `warehouse/etl/identity.py`, gọi từ `warehouse/etl/pipeline.py` sau bước kiểm định, trước bước khử trùng**
- Mốc so sánh là `create_time` đã lưu trong `Fact_Orders` (truy vấn chỉ đọc). Nếu mã chưa có trong warehouse, mốc là `create_time` sớm nhất trong lô.
- Dòng lệch mốc đi vào `quarantine/orders/...` cùng luồng cách ly hiện có. Lý do được đếm trong `validation_errors`.
- Phương trình đối soát `raw = hợp lệ + trùng + cách ly` vẫn giữ.
- Test trong `tests/test_order_line_integrity.py`:
  - cập nhật trạng thái không bị coi là xung đột;
  - mã dùng lại giữ đơn đầu và cách ly đơn sau;
  - mã đã có trong warehouse thắng đơn mới;
  - pipeline ghi cách ly và đối soát đúng.

**Khôi phục — `scripts/repair_collided_order_ids.py`**
- Mặc định chạy diễn tập rồi rollback; `--apply` mới commit. Giữ khóa `pipeline_lock` chung với worker và backfill.
- Đọc toàn bộ `raw/orders` qua đúng các bước ETL (unify → clean → validate).
- Trong một giao dịch: xóa các dòng `Fact_Orders` của mã bị gộp, rồi nạp lại bằng `warehouse.etl.load`.
- Sau đó chạy `python -m warehouse.reporting.mart --full`.

## 4. Kết quả đã kiểm chứng (17/11/2026)

| Chỉ số | Giá trị |
|---|---|
| Kho thô | 20.272 dòng (76 dòng bị kiểm định loại) |
| Mã bị dùng chung | 159 (`SPE261116` 80, `TT2611160` 65, `SPE261006` 8, `TT2610060` 6) |
| Số đơn thật đứng sau 159 mã | 358 (có mã bị 3 đơn dùng chung) |
| Trước khôi phục | 159 mã, 340 dòng, 678 sản phẩm, 157.946.470 ₫ |
| Sau khôi phục | 358 đơn, 358 dòng, 723 sản phẩm, 165.626.560 ₫ (khớp kho thô) |
| Đơn bị ghi đè mất hẳn, nay đã nạp lại | 18 dòng, 45 sản phẩm, 7.680.090 ₫ |
| `Fact_Orders` | 157.901 → 157.919 dòng; các dòng khác không đổi |

- Kiểm tra trong script đạt 5/5:
  - đủ đơn;
  - khớp số dòng;
  - khớp số lượng;
  - khớp số tiền;
  - không đổi dòng nào ngoài phạm vi.
- Mart full refresh: `fact_sales_line` 157.919 dòng, `fact_sales_order` 157.893 đơn.
- `check_powerbi_refresh_readiness.py --label ORDERID-REPAIR`: **PASS 16/16**.
- Kiểm thử:
  - toàn bộ bộ test: 261 đạt;
  - test PostgreSQL (`RUN_PIPELINE_COMPLETION_PG=1`), mart và toàn vẹn dòng đơn: đạt;
  - 2 test mart được sửa để không phụ thuộc thời điểm chạy (sau nửa đêm, ngày vừa xác nhận đã có dự báo và đề xuất thật).
- Bằng chứng:
  - `docs/07-bao-cao-do-an/evidence/order-id-repair-rehearsal-20261117-002313.json`
  - `docs/07-bao-cao-do-an/evidence/order-id-repair-apply-20261117-002347.json`
  - bảng ánh xạ mã cũ → mã mới: `output/order-id-repair-20261117/order_id_mapping_apply.csv`

## 5. Giới hạn còn lại

- 16 mã `DI2026101…`/`DI2026102…` vẫn có xung đột header. Đây là dữ liệu do bài kiểm tra toàn vẹn ghi trực tiếp, không có trong kho thô, nên không khôi phục và được giữ nguyên.
- Nếu backfill lại các object 06/10 hoặc 16/11, các đơn sau sẽ đi vào cách ly với lý do `ORDER_IDENTITY_CONFLICT` thay vì ghi đè. Bản đã khôi phục (mã `-HHMMSS`) không bị ảnh hưởng.
- Model đang phục vụ, manifest và feature snapshot không đổi. Dữ liệu ngày 06/10 và 16/11 tăng nhẹ, sẽ có hiệu lực khi huấn luyện lại thủ công.
- Với dữ liệu thật, Shopee/TikTok bảo đảm mã đơn là duy nhất. Bước cách ly là lưới an toàn phát hiện lỗi nguồn.

## 6. Minh bạch sử dụng AI

- **AI hỗ trợ:**
  - phân tích nguyên nhân từ kho thô;
  - viết `identity.py`, script khôi phục, test và tài liệu này;
  - chạy diễn tập, áp dụng và kiểm chứng.
- **Sinh viên:**
  - phát hiện dữ liệu bất thường trên báo cáo;
  - đề xuất thêm timestamp vào mã đơn;
  - chọn phương án A (cách ly + khôi phục) thay vì giữ bản đầu tiên;
  - duyệt chạy khôi phục.
