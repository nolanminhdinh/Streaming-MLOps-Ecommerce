# Kiểm tra lại tính xác định sau khi sửa API tồn kho — 16/11/2026

Lặp lại đúng bài kiểm tra ngày 15/11 (`output/replay-determinism-2026-11-15/bao-cao.md`): cùng script `scripts/check_replay_determinism.py`, cùng seed 20261115 và cấu trúc 41 message, nhãn mới `RPT11161927`. Hai lượt gửi cùng chuỗi byte, `pipeline-worker` xử lý như vận hành thật.

## Thay đổi được kiểm chứng

- `serving/app/data_access.py`: thêm `get_stock_for_skus()` đọc trực tiếp kho (không cache) cho các SKU được yêu cầu nhưng chưa có trong cache `latest_stock` 30 giây; `_query(ttl<=0)` không lưu cache.
- `serving/app/inventory_service.py`: `evaluate_all()` và `evaluate_sku()` bổ sung snapshot bằng `_complete_snapshot()` trước khi dùng tồn kho giả lập. SKU có số do request gửi lên không bị tra thêm.
- `tests/test_serving_realdata.py`: thêm test hồi quy `test_inventory_rereads_warehouse_when_cache_misses_new_sku`.
- `scripts/demo_web_server.py`: đọc bỏ body trước khi trả 403. Trước đây, trên Windows, client thỉnh thoảng nhận `ConnectionAbortedError` thay vì 403 (test `test_cross_origin_or_missing_token_cannot_publish` lỗi khoảng 2/5 lần; sau khi sửa đạt 25/25 lần).

Bộ test: 240 đạt, 20 bỏ qua (opt-in PostgreSQL). Image `fastapi` đã build lại trước khi chạy.

## Kết quả: 20/20 đạt

| Yêu cầu | Lượt 1 | Lượt 2 |
|---|---|---|
| Kafka nhận đủ, lag 0 | 34 / 7, lag 0 | 34 / 7, lag 0 |
| Raw lake giữ đủ message đơn | 34 | 34 |
| Đối soát raw = hợp lệ + trùng + lỗi | 34 = 24 + 6 + 4 | 34 = 24 + 6 + 4 |
| Dòng fact / khóa trùng / SKU | 24 / 0 / 13 | 24 / 0 / 13 |
| SHA-256 nội dung nghiệp vụ `Fact_Orders` | `f6c0da6b…` | `f6c0da6b…` |
| Tổng số lượng / doanh thu | 40 / 9.739.030 | 40 / 9.739.030 |
| Phiên bản mới hơn thắng; snapshot cũ không ghi đè | COMPLETED; 344 | COMPLETED; 344 |
| Cách ly bản ghi lỗi | 4 đơn, 1 tồn kho | 4 đơn, 1 tồn kho |
| Lineage (run_id, object, hợp đồng) | có | có |
| `bi.fact_order_line` khớp fact | 24 / 40 / 9.739.030 | như lượt 1 |
| API tồn kho dùng số thật, giống nhau giữa hai lượt | `warehouse_snapshot` | `warehouse_snapshot` |
| Pipeline healthy, API ready | có | có |

## Quan sát còn lại (không đổi so với 15/11)

- **Ghi lại dòng khi gửi lại.** `order_fact_id` giữ nguyên, nhưng 24/24 dòng fact và 5/5 dòng tồn kho bị ghi lại: `loaded_at` đổi, `loaded_records` vẫn là 24. Nguyên nhân là upsert dùng `>=` trên phiên bản nguồn.
- **Lake tăng dung lượng.** Mỗi lần gửi lại tạo thêm object raw và object cách ly; đây là thiết kế at-least-once.
- **Phạm vi kiểm tra phiên bản.** Bản cũ và bản mới nằm cùng lô ETL, nên điều kiện chặn bản cũ ở tầng PostgreSQL chưa được thử riêng.
- **Thời gian xử lý:** 33,9 giây và 27,6 giây, theo chu kỳ poll 30 giây của worker.

## Minh bạch sử dụng AI

AI (Claude) hỗ trợ: tìm nguyên nhân, sửa code serving và demo web, viết test hồi quy, chạy lại kiểm tra và viết báo cáo này. Sinh viên cần xem lại thay đổi mã nguồn và kết quả trước khi đưa vào báo cáo đồ án.
