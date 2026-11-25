# Kiểm tra tính xác định khi chạy lại cùng một bộ dữ liệu — 15/11/2026

> **Cập nhật 16/11/2026:** lỗi API tồn kho mô tả bên dưới đã được sửa và kiểm tra lại đạt 20/20. Xem `output/replay-determinism-2026-11-16/bao-cao.md`.

## Cách làm

- Bộ dữ liệu `dataset.jsonl` (nhãn `RPT11151427`, seed 20261115, SHA-256 trong `dataset-manifest.json`) gồm 41 message:
  - 34 message đơn hàng:
    - 24 đơn hợp lệ Shopee/TikTok do simulator sinh;
    - 2 bản trùng tuyệt đối;
    - 4 bản đổi phiên bản (mới hơn +6 giờ và cũ hơn −6 giờ);
    - 4 bản lỗi: giá không phải số, số lượng 0, thiếu sàn, JSON hỏng.
  - 7 message tồn kho:
    - 5 snapshot hợp lệ;
    - 1 snapshot cũ đến sau;
    - 1 snapshot âm.
- Hai lượt gửi đúng cùng chuỗi byte vào Kafka, cách nhau sau khi lượt trước xử lý xong. `pipeline-worker` xử lý như vận hành thật; không chạy ETL/consumer thủ công.
- Sau mỗi lượt chụp trạng thái: offset và lag Kafka, object raw/quarantine trong MinIO, `Fact_Orders`, `Fact_Inventory_Daily`, `Dim_Products`, view `bi.fact_order_line`, lineage `data/pipeline/runs/*.json`, API `/inventory/reorder-alert` và `/pipeline/status`.
- Script: `scripts/check_replay_determinism.py`. Kết quả gốc: `run1.json`, `run2.json`, `comparison.json`.

## Đối chiếu với yêu cầu bắt buộc của data pipeline

| Yêu cầu | Kiểm tra | Lượt 1 | Lượt 2 | Kết quả |
|---|---|---|---|---|
| Thu nhận đầy đủ | Kafka nhận đủ message | 34 đơn / 7 tồn kho | 34 / 7 | Đạt |
| | Consumer không còn lag | 0 / 0 | 0 / 0 | Đạt |
| | Raw lake giữ mọi message đơn, kể cả lỗi | 34 dòng | 34 dòng | Đạt |
| Đối soát | raw = hợp lệ + trùng + bị loại, theo từng object | 34 = 24 + 6 + 4 | 34 = 24 + 6 + 4 | Đạt |
| Chống trùng / idempotent | Số dòng fact | 24 | 24 | Đạt |
| | Khóa nghiệp vụ trùng trong `Fact_Orders` | 0 | 0 | Đạt |
| | SKU trong `Dim_Products` | 13 | 13 | Đạt |
| Xác định | SHA-256 nội dung nghiệp vụ `Fact_Orders` (bỏ `order_fact_id`, `loaded_at`) | `f3a3b5d7…` | `f3a3b5d7…` | Đạt |
| | Tổng số lượng / doanh thu | 40 / 9.739.030 | 40 / 9.739.030 | Đạt |
| | Lý do cách ly theo mã lỗi | giống nhau | giống nhau | Đạt |
| Thứ tự / phiên bản sự kiện | Bản mới hơn thắng (2 đơn) | COMPLETED | COMPLETED | Đạt |
| | Snapshot tồn kho cũ không ghi đè | 344 | 344 | Đạt |
| Chất lượng dữ liệu | Bản ghi lỗi bị cách ly, không vào kho | 4 đơn, 1 tồn kho | 4, 1 | Đạt |
| Truy vết | run_id, object nguồn, object cách ly, hợp đồng `ecommerce.orders/1.0` | có | có | Đạt |
| Nhất quán hạ nguồn | `bi.fact_order_line` khớp fact | 24 / 40 / 9.739.030 | như lượt 1 | Đạt |
| | API tồn kho giống nhau giữa hai lượt | 63 (`simulated_demo`) | số thật trong kho | **Không đạt** |
| | API dùng tồn kho thật | không | có | **Không đạt** |
| Giám sát | Pipeline healthy, API ready | có | có | Đạt |

Tổng: 18/20 kiểm tra đạt. Toàn bộ tầng dữ liệu (Kafka → MinIO → ETL → PostgreSQL → view BI) cho kết quả nghiệp vụ giống hệt nhau giữa hai lượt.

## Lỗi phát hiện: API tồn kho dùng số giả lập khi cache chưa có SKU mới

`get_latest_stock()` trong `serving/app/data_access.py` cache toàn bộ bảng tồn kho mới nhất 30 giây. `evaluate_sku()` trong `serving/app/inventory_service.py` không tìm thấy SKU trong cache thì chuyển sang `_simulated_stock()` thay vì truy vấn lại. Ở lượt 1, kho đã có snapshot của 5 SKU (249, 344, 299, 280, 220) nhưng API trả 63 cho cả 5. Gọi lại sau khi cache hết hạn trả đúng số trong kho. Giá trị giả có nhãn `stock_source="simulated_demo"`, nhưng đề xuất nhập hàng trong khoảng thời gian đó dựa trên số giả.

Hướng sửa đề xuất: khi SKU được yêu cầu không có trong cache, bỏ qua cache và truy vấn lại một lần; nếu vẫn không có thì trả trạng thái "chưa có tồn kho" thay vì số giả lập.

## Quan sát khác (không phải lỗi nghiệp vụ)

- **Ghi lại dòng khi gửi lại.** `order_fact_id` giữ nguyên, nhưng cả 24/24 dòng `Fact_Orders` và 5/5 dòng tồn kho bị ghi lại ở lượt 2: `loaded_at` đổi và `loaded_records` vẫn là 24. Nguyên nhân là điều kiện upsert dùng `>=` trên phiên bản nguồn. Nội dung nghiệp vụ không đổi, nhưng `loaded_records` không phân biệt được dòng mới với dòng ghi lại không thay đổi.
- **Lake tăng dung lượng.** Lượt 2 tạo thêm 4 object raw và 2 object cách ly. Đây là đúng thiết kế at-least-once: lake giữ mọi lần nhận, kho mới là nơi chống trùng.
- **Phạm vi kiểm tra phiên bản.** Bản cũ và bản mới của cùng đơn nằm trong cùng lô ETL, nên được giải quyết ở bước loại trùng trong lô (giữ `source_updated_at` lớn nhất). Điều kiện chặn bản cũ ở tầng PostgreSQL chưa được kiểm tra riêng trong bài này; unit test hiện có bao phủ trường hợp đó.
- **Thời gian xử lý:** 23,3 giây và 25,5 giây, phụ thuộc chu kỳ poll 30 giây của worker.

## Sự cố trong lần chạy đầu

Lần chạy đầu (nhãn `RPT11151410`, thư mục `output/replay-determinism-2026-11-15-host-sleep-incident`) không hợp lệ để so sánh. Windows vào Modern Standby từ 14:10:15 đến 14:25:03 (sự kiện Kernel-Power 506/507), làm toàn bộ máy ảo Docker đóng băng 15 phút, nên hai lượt bị ETL gộp. Sau khi máy thức, worker tự nối lại Kafka, xử lý hết tồn đọng, lag về 0 và không tạo fact trùng (24 dòng). Đây là bằng chứng phục hồi, không phải kết quả so sánh hai lượt.

## Minh bạch sử dụng AI

AI (Claude) hỗ trợ: thiết kế bộ dữ liệu, viết script kiểm tra, chạy hai lượt trên stack local, phân tích log và viết báo cáo này. Sinh viên cần xem lại kết quả và quyết định có đưa vào báo cáo đồ án hay không.
