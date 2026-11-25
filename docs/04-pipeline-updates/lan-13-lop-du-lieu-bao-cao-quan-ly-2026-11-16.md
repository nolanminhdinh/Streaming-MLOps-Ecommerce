# Lần 13 — Lớp dữ liệu cho báo cáo quản lý, huấn luyện lại model trên kho streaming (16/11/2026)

## 1. Mục tiêu

Chuẩn bị dữ liệu cho báo cáo Power BI dùng trong vận hành: người quản lý kết hợp dự báo AI để quyết định đặt hàng. Các quyết định của sinh viên ngày 16/11/2026:
- báo cáo có 4 trang chính: Tồn kho, Dự báo AI, Đặt hàng nhà cung cấp, Đơn hàng từ các sàn;
- huấn luyện lại model trên dữ liệu kho của các SKU đang bán;
- mức phục vụ theo nhóm ABC: A 98%, B 95%, C 90%;
- worker tự xác nhận độ đủ dữ liệu theo ngày;
- bổ sung dữ liệu mô phỏng 01–15/11;
- hàng đang về là hướng mở rộng.

## 2. Thay đổi

| Hạng mục | Thay đổi | Kiểm chứng |
|---|---|---|
| Tồn kho | `Fact_Inventory_Daily` thêm `lead_time_days`, `incoming_stock`, `restock_eta`; consumer kiểm định và lưu; ràng buộc `ck_inventory_supply_valid` | Test đơn vị và PostgreSQL; lượt producer: 20/20 snapshot có lead time 2–4 ngày |
| Coverage | `warehouse/etl/coverage.py`: worker xác nhận ngày khi đã qua, hết object chờ, lag Kafka = 0 và khối lượng ≥ 20% trung vị 28 ngày | 6 test; 319 ngày được xác nhận (01/01–15/11) |
| Nguồn huấn luyện | `--data-origin warehouse_live` trong feature pipeline và huấn luyện: chỉ các ngày đã xác nhận; serving lấy ngày cuối lịch sử là ngày đã xác nhận | Model v4 phục vụ SKU đang bán qua `mlflow_registry` |
| Dữ liệu bổ sung | 11.709 dòng đơn 01–15/11 (`SIM26NOV-`, seed 20261116) bằng cùng generator lần 8 | Shop TikTok đúng; coverage tự xác nhận 14 ngày |
| Dự báo theo lô | Service `batch-forecaster`: 14 ngày × SKU đang hoạt động; lưu khoảng tin cậy; ghi `Model_Serving_Snapshot` | 143 SKU, 129 dùng model, 14 dùng heuristic, 0 lỗi |
| View `bi` | `forecast_daily`, `replenishment_recommendation`, `fact_sales_order`, `model_status`, `model_serving_history`, `data_freshness`; `dim_product` thêm ABC/XYZ | Mô phỏng Refresh `PASS` 28 nguồn; tổng đơn khớp tổng dòng; đề xuất không mâu thuẫn |
| Đặc tả Power BI | `powerbi/dac-ta-bao-cao-quan-ly.md`: mô hình, quan hệ, DAX, bố cục 6 trang, checklist production, giá trị đối chiếu | Sinh viên dựng PBIX |

Bộ test: 251 đạt; 54 test PostgreSQL opt-in đạt.

## 3. Kết quả huấn luyện lại

Huấn luyện trên dữ liệu đúng danh mục đang bán: 129.541 dòng đơn, 319 ngày đã xác nhận, 177 SKU. Đánh giá walk-forward đệ quy, 3 fold, WAPE gộp:

| Model | WAPE | Bias |
|---|---|---|
| **Naive** (champion, v4 Staging) | 58,22% | −23,07% |
| Ridge | 70,73% | −38,58% |
| Moving Average | 91,17% | 38,81% |
| LightGBM | 95,39% | 30,12% |
| Seasonal Naive | 124,32% | 44,94% |

Trên dữ liệu mô phỏng này, model học máy chưa vượt baseline Naive. Bias âm cho thấy xu hướng dự báo thiếu. Cả hai điểm đều được hiển thị trên trang AI.

Artifact của model cũ (v2, CSV công khai) đã được sao lưu kèm mã băm ở `output/model-retrain-2026-11-16/backup-before/`. Các run cũ vẫn còn trong MLflow Registry.

## 4. Sự cố phát hiện và cách xử lý

**Quy tắc coverage ban đầu xác nhận nhầm 14 ngày producer tắt.**
- *Vấn đề:* ba điều kiện đầu chỉ chứng minh pipeline không tồn đọng, không phân biệt được "nguồn ngừng gửi" với "không có đơn". Model vì thế thấy nhu cầu bằng 0.
- *Cách xử lý:*
  - thêm điều kiện khối lượng;
  - xóa 14 dòng xác nhận sai (do chính quy tắc cũ ghi trong ngày);
  - sinh viên quyết định bổ sung dữ liệu mô phỏng cho khoảng trống.
- *Bài học cho production:* coverage cần tín hiệu từ phía nguồn (số lượng kỳ vọng hoặc heartbeat của connector), không chỉ trạng thái pipeline.

**Ký tự phần trăm trong `bi_schema.sql` làm lệnh cài view lỗi.**
- *Vấn đề:* psycopg2 coi ký tự phần trăm là tham số, kể cả khi nằm trong comment.
- *Cách xử lý:* bỏ ký tự này khỏi tệp, dùng `CHR(37)` khi cần.

## 5. Giới hạn

- **Hàng đang về:** chưa tính vào vị thế tồn kho; dữ liệu đã được thu.
- **Lead time:** thiếu thì dùng mặc định 3 ngày, có cờ đánh dấu.
- **Dữ liệu kinh doanh:** chưa có giá vốn và nhà cung cấp. Giá trị đề xuất tính theo giá bán tham chiếu.
- **SKU mới chưa có lịch sử:** dùng heuristic và không được lưu vào bảng dự báo.
- **Độ chính xác thực tế:** `Live WAPE` có dữ liệu từ khi ngày 16/11 được xác nhận.
- **Đơn vị tồn kho:** tồn kho của simulator thấp so với nhu cầu của dữ liệu bổ sung, nên nhiều SKU ở trạng thái CRITICAL. Đây là đặc điểm của dữ liệu mô phỏng.

## 6. Minh bạch sử dụng AI

AI (Claude) hỗ trợ:
- thiết kế và viết mã: consumer tồn kho, coverage, nguồn `warehouse_live`, job dự báo, các view `bi`;
- chạy huấn luyện, đăng ký model, bổ sung dữ liệu mô phỏng theo quyết định của sinh viên;
- viết test, kiểm chứng và soạn đặc tả Power BI cùng tài liệu này.

Sinh viên quyết định các tham số nghiệp vụ, phạm vi và dựng PBIX.
