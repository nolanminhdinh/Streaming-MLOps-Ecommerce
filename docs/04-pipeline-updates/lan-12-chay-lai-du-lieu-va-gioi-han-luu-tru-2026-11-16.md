# Lần 12 — Kiểm tra chạy lại cùng bộ dữ liệu và giới hạn lưu trữ MinIO (15–16/11/2026)

## 1. Mục đích và phạm vi

Kiểm tra pipeline có cho cùng một kết quả khi nhận lại đúng một bộ dữ liệu hay không, đối chiếu với các yêu cầu bắt buộc của một data pipeline: thu nhận đầy đủ, đối soát, chống trùng, xác định, thứ tự phiên bản, chất lượng dữ liệu, truy vết, nhất quán hạ nguồn và giám sát.

Đợt này sửa hai lỗi phát hiện được. **Sự tăng dung lượng MinIO khi nhận lại dữ liệu được giữ nguyên có chủ đích** trong bản demo, dùng làm bằng chứng phản biện; hướng giải quyết cho luồng vận hành thực tế được ghi ở mục 5 và chưa triển khai.

## 2. Kiểm tra chạy lại hai lượt

Script `scripts/check_replay_determinism.py` dựng một bộ dữ liệu cố định (seed 20261115, có SHA-256), gồm 41 message:

- **34 message đơn hàng:**
  - 24 đơn hợp lệ Shopee/TikTok;
  - 2 bản trùng tuyệt đối;
  - 4 bản đổi phiên bản;
  - 4 bản lỗi.
- **7 message tồn kho:**
  - 5 snapshot hợp lệ;
  - 1 snapshot cũ đến sau;
  - 1 snapshot âm.

Hai lượt gửi đúng cùng chuỗi byte vào Kafka; `pipeline-worker` xử lý như vận hành thật, không chạy ETL thủ công. Sau mỗi lượt, script chụp trạng thái Kafka, MinIO, PostgreSQL, view `bi`, lineage và API.

| Lần chạy | Nhãn dữ liệu | Kết quả | Hồ sơ |
|---|---|---|---|
| 15/11, lần đầu | `RPT11151410` | Không hợp lệ để so sánh: Windows vào Modern Standby 14:10:15–14:25:03, Docker đóng băng 15 phút, hai lượt bị ETL gộp | `output/replay-determinism-2026-11-15-host-sleep-incident/` |
| 15/11, chạy lại | `RPT11151427` | 18/20 đạt; API tồn kho trả số giả lập ở lượt 1 | `output/replay-determinism-2026-11-15/bao-cao.md` |
| 16/11, sau khi sửa | `RPT11161927` | **20/20 đạt** | `output/replay-determinism-2026-11-16/bao-cao.md` |

Kết quả lượt 16/11:
- Hai lượt cho cùng 24 dòng fact, 0 khóa trùng, cùng SHA-256 nội dung nghiệp vụ, cùng tổng 40 sản phẩm / 9.739.030 đ.
- Bản ghi lỗi bị cách ly như nhau (4 đơn, 1 tồn kho) với cùng mã lỗi; bản mới hơn thắng; snapshot tồn kho cũ không ghi đè.
- `bi.fact_order_line` khớp fact; API trả tồn kho thật ở cả hai lượt.

Sự cố máy ngủ là bằng chứng phục hồi ngoài kế hoạch: khi máy thức, worker tự nối lại Kafka, xử lý hết tồn đọng, lag về 0, không mất dòng, không tạo fact trùng.

Trường hợp đơn có `_platform=None` (mục 3.2 của chương Ingestion ghi nhận có thể mất dòng) lần này được lưu vào `raw/orders/platform=unknown` và bị cách ly với mã `INVALID_PLATFORM`; 34/34 dòng đơn có mặt trong raw ở cả hai lượt.

## 3. Lỗi đã sửa

| Lỗi | Nguyên nhân | Sửa |
|---|---|---|
| API `/inventory/reorder-alert` trả tồn kho giả lập (`simulated_demo`) cho SKU vừa có snapshot trong kho | `get_latest_stock()` cache toàn bảng 30 giây; SKU không có trong cache rơi thẳng về `_simulated_stock()` | `serving/app/data_access.py`: thêm `get_stock_for_skus()` đọc thẳng kho, không cache. `serving/app/inventory_service.py`: thêm `_complete_snapshot()`, chỉ tra SKU thiếu và không có số do request gửi lên. Test hồi quy `test_inventory_rereads_warehouse_when_cache_misses_new_sku` |
| Test `test_cross_origin_or_missing_token_cannot_publish` lỗi khoảng 2/5 lần trên Windows | `scripts/demo_web_server.py` trả 403 trước khi đọc body POST; đóng socket còn dữ liệu làm Windows gửi RST, client nhận `ConnectionAbortedError` | Đọc bỏ body (≤ 8 KB) trước khi trả 403; đạt 25/25 lần liên tiếp |
| `python -m pytest -q` báo 56 lỗi | Quét cả bản sao test trong `Tai_lieu_chat_DATN_*`, `tmp/`; fixture `case_data` nằm trong plugin không được nạp | Thêm `pytest.ini` (`testpaths = tests`, tự nạp `tests.pipeline_evidence_plugin`) |

Bộ test sau khi sửa: 240 đạt, 20 bỏ qua (opt-in PostgreSQL).

## 4. Giới hạn được giữ lại: dung lượng MinIO tăng theo số lần nhận

### 4.1. Cơ chế

- Raw lake chỉ ghi thêm. Mỗi lần consumer flush (`CONSUMER_BATCH_TIMEOUT=10` giây hoặc 500 message) tạo một object Parquet mới cho mỗi phân vùng (sàn, ngày), tên có UUID. Consumer không loại trùng; chống trùng thực hiện ở warehouse. Gửi lại 34 đơn tạo thêm 4–5 object raw.
- Mỗi lượt ETL có bản ghi lỗi tạo thêm `quarantine/orders/run_id=…/rejected.jsonl`.
- `minio-init` chỉ tạo bucket; bucket không có lifecycle rule nên không có gì bị xóa.
- Mỗi chu kỳ 30 giây, `run_cycle()` trong `scripts/run_pipeline_worker.py`:
  - liệt kê **toàn bộ** `raw/orders/`;
  - so với danh sách `completed_objects`;
  - ghi lại **toàn bộ** `data/pipeline/checkpoint.json`.

  Danh sách này không bao giờ được cắt bớt.

### 4.2. Số đo ngày 16/11/2026

| Mục | Giá trị |
|---|---|
| `raw/orders` | 451 object, 30,2 MB (trung bình khoảng 67 KB/object) |
| `raw/nifi` (lịch sử NiFi, ngoài phạm vi) | 313 object, 0,6 MB |
| `mlflow/*` | 40 object, 2,4 MB |
| `quarantine/*` | 60 object, 0,5 MB |
| Volume `minio-data` | 48,8 MB |
| Liệt kê `raw/orders` | 0,04 giây |
| `checkpoint.json` | 451 mục, 48 KB (khoảng 106 byte/mục) |

### 4.3. Ảnh hưởng

- **Tính đúng của dữ liệu: không ảnh hưởng.** Object gửi lại có tên mới nên ETL vẫn xử lý, nhưng upsert theo `(order_id, platform, source_line_id)` giữ kho không đổi. Bằng chứng: hai lượt cùng SHA-256 nội dung nghiệp vụ.
- **Chi phí xử lý trùng.**
  - ETL đọc và kiểm định lại dòng đã có.
  - Upsert dùng `>=` trên phiên bản nguồn nên 24/24 dòng fact và 5/5 dòng tồn kho bị ghi lại (`loaded_at` đổi, `loaded_records` vẫn là 24).
  - Số "dòng thô" trong lineage tính cả bản trùng; số đơn báo cáo phải lấy từ warehouse.
- **Hiệu năng dài hạn (ước tính, chưa đo).**
  - Producer liên tục với flush 10 giây tạo khoảng 2–3 object mỗi 10 giây, tức khoảng 17.000–26.000 object/ngày.
  - Sau một tháng, mỗi chu kỳ phải liệt kê vài trăm nghìn object và ghi lại tệp JSON vài chục MB mỗi 30 giây; độ trễ ingest → warehouse tăng dần.
  - Nhiều tệp nhỏ cũng làm ETL và backfill chậm hơn so với ít tệp lớn.
- **Đầy đĩa.** MinIO hết chỗ thì ghi object thất bại, consumer không commit offset, luồng dừng mà không mất dữ liệu. Kafka giữ message 240 giờ, nên nếu luồng dừng quá 10 ngày thì message cũ bị xóa và mới mất dữ liệu.

### 4.4. Lý do giữ nguyên trong bản demo

- Quy mô demo (hàng trăm object, dưới 50 MB) chưa chạm ngưỡng ảnh hưởng.
- Raw bất biến là điều kiện để truy vết và backfill trên object thật; xóa hay gộp sớm sẽ làm mất bằng chứng.
- Giữ nguyên cho phép trình bày trực tiếp với hội đồng: dữ liệu trùng có trong lake nhưng không vào kho.

## 5. Hướng giải quyết cho luồng vận hành thực tế (chưa triển khai)

| Ưu tiên | Hướng | Giải quyết | Điều kiện |
|---|---|---|---|
| 1 | Thay `completed_objects` bằng watermark theo phân vùng ngày (ngày + `last_modified` + tên object), chỉ liệt kê các phân vùng chưa đóng; `list_parquet_files(target_date=…)` đã hỗ trợ lọc ngày | Chi phí mỗi chu kỳ không tăng theo tổng số object | Dữ liệu đến muộn vào phân vùng cũ cần cửa sổ mở lại có giới hạn hoặc chuyển cho backfill |
| 2 | Lifecycle rule cho `quarantine/` và `raw/`, thời gian giữ lớn hơn cửa sổ backfill 31 ngày; trước khi xóa, chuyển sang lưu trữ lạnh | Dung lượng không tăng vô hạn | Lineage trỏ tới object đã xóa phải ghi rõ "đã lưu trữ" |
| 3 | Gộp tệp nhỏ (compaction) theo ngày sau khi phân vùng đóng, giữ manifest ánh xạ object cũ → object gộp | Giảm số object, ETL/backfill đọc nhanh hơn | Chỉ gộp object đã checkpoint; giữ hash để đối soát |
| 4 | Upsert bỏ qua dòng không đổi (`WHERE` thêm so sánh `IS DISTINCT FROM` trên cột nghiệp vụ) và tách đếm "mới / cập nhật / không đổi" trong lineage | Gửi lại không ghi lại dòng; số liệu lineage phân biệt được | Giữ nguyên quy tắc phiên bản mới hơn thắng |
| 5 | Metric dung lượng bucket, số object chờ và cảnh báo khi vượt ngưỡng; cảnh báo khi luồng dừng gần hết retention Kafka | Phát hiện sớm trước khi mất dữ liệu | Bổ sung rule Prometheus/Alertmanager |
| 6 | Dọn `raw/nifi` sau khi chốt lưu làm bằng chứng lịch sử | Bỏ dữ liệu ngoài phạm vi | Sinh viên quyết định |

Không đề xuất loại trùng ngay ở raw lake: giữ mọi lần nhận là điều kiện truy vết và backfill; chống trùng đúng chỗ là warehouse.

## 6. Câu hỏi phản biện và hướng trả lời

**Gửi lại cùng dữ liệu thì kết quả có khác không?** Không khác về nghiệp vụ. Hai lượt cùng 24 dòng, cùng SHA-256 nội dung, cùng tổng số lượng và doanh thu, cùng kết quả cách ly. Chỉ cột kỹ thuật `loaded_at` thay đổi (xem `output/replay-determinism-2026-11-16/`).

**Vì sao MinIO vẫn tăng nếu đã chống trùng?** Raw lake ghi nhận mọi lần nhận theo ngữ nghĩa at-least-once để giữ bằng chứng và cho phép backfill; chống trùng nằm ở warehouse. Đây là lựa chọn thiết kế; giới hạn hiện tại là chưa có chính sách giữ dữ liệu và checkpoint chưa co giãn (mục 4–5).

**Hệ thống có chịu được chạy liên tục nhiều tháng không?** Chưa kiểm chứng. Số đo hiện tại ở quy mô demo; ước tính cho thấy chi phí liệt kê object và ghi checkpoint tăng tuyến tính. Hướng giải quyết ưu tiên là watermark theo phân vùng và lifecycle (mục 5).

**Khi máy chủ dừng đột ngột thì sao?** Sự cố Modern Standby ngày 15/11 đóng băng toàn bộ Docker 15 phút; sau khi máy thức, luồng tự phục hồi, không mất dòng và không tạo fact trùng.

## 7. Tái lập

```powershell
python -m pytest -q --tb=short
docker compose up -d --build fastapi
python scripts/check_replay_determinism.py --out-dir output/replay-determinism-<ngày>
```

Script ghi dữ liệu có nhãn `RPT…` vào kho và giữ lại, giống `check_continuous_pipeline.py`. Không chạy khi máy có thể vào chế độ ngủ: hai lượt sẽ bị ETL gộp và mục "Điều kiện thử" báo FAIL.

## 8. Minh bạch sử dụng AI

AI (Claude) hỗ trợ:
- thiết kế bộ dữ liệu và viết `scripts/check_replay_determinism.py`;
- chạy kiểm tra trên stack local, phân tích log và nhật ký nguồn điện Windows;
- sửa `serving/app/data_access.py`, `serving/app/inventory_service.py`, `scripts/demo_web_server.py`, thêm `pytest.ini` và test hồi quy;
- đo dung lượng MinIO và soạn tài liệu này.

Sinh viên quyết định giữ nguyên giới hạn lưu trữ MinIO cho bản demo và dùng làm nội dung phản biện. Sinh viên cần xem lại thay đổi mã nguồn và số liệu trước khi đưa vào báo cáo cuối.
