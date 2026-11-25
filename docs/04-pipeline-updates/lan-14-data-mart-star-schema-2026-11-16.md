# Lần 14 — Data mart star schema cho báo cáo quản lý (16/11/2026)

## 1. Mục tiêu

Thay lớp view `bi` bằng một **data mart dạng star schema gồm các bảng vật lý** (schema `mart`) làm nguồn duy nhất của báo cáo Power BI.

Các quyết định của sinh viên ngày 16/11/2026:
- dùng bảng vật lý;
- giữ lịch sử đề xuất đặt hàng theo ngày;
- ghi phiên bản model đã dự báo cho từng ngày, để so sánh chính xác giữa các phiên bản;
- khóa ngày dạng yyyymmdd.

## 2. Kiến trúc

```
Kafka → MinIO (raw) → public (kho tích hợp, ETL không đổi) → mart (star schema) → Power BI
```

**`mart.refresh()`** làm mới theo kiểu gia tăng:
- bảng chiều được upsert toàn bộ;
- fact được nạp theo mốc `loaded_at` / `forecast_id`, chồng lấn 10 phút;
- có khóa advisory để không chạy chồng.

**Nơi gọi:**
- worker gọi sau mỗi chu kỳ ETL (khoảng 2 giây mỗi lần);
- batch-forecaster gọi sau mỗi lượt dự báo;
- lệnh tay: `python -m warehouse.reporting.mart [--full]`.

Các view `bi` và bảng `public` giữ nguyên để tương thích.

## 3. Mô hình

**Bảng chiều:**
- `dim_date` (khóa yyyymmdd, liên tục, có cờ `is_data_confirmed`);
- `dim_product` (ABC/XYZ, mức phục vụ, lead time hiện hành);
- `dim_platform`, `dim_shop`, `dim_geography`, `dim_payment`, `dim_carrier`;
- `dim_order_status` (nhóm phễu);
- `dim_model` (phiên bản, thuật toán, nguồn huấn luyện, WAPE/bias backtest, đang phục vụ).

Mọi bảng chiều trừ `dim_date` có dòng khóa −1 "Không xác định" (nguồn thiếu dữ liệu). Theo quyết định của sinh viên, `dim_carrier` và `dim_model` có thêm −2 "Không áp dụng":
- `dim_carrier` −2 "Chưa giao vận chuyển": đơn chưa từng giao, xét theo cả trạng thái lẫn mốc thời gian;
- `dim_model` −2 "Không dùng dự báo AI".

Lần áp dụng đầu chỉ xét mốc thời gian nên đã xếp nhầm 41 đơn COMPLETED (không có mốc giao/hoàn tất) vào −2; quy tắc đã sửa để xét cả trạng thái, kèm test hồi quy. Kết quả ngày 16/11: 42.750 đơn −2, 41 đơn −1; 18/88 đề xuất dùng model −2.

**Bảng fact:**

| Fact | Loại | Độ hạt |
|---|---|---|
| `fact_sales_line` | Giao dịch | Dòng hàng |
| `fact_sales_order` | Accumulating snapshot | Đơn, có các mốc thanh toán / giao / hoàn tất |
| `fact_inventory_daily` | Periodic snapshot | SKU × ngày |
| `fact_demand_forecast` | Periodic snapshot | SKU × ngày mục tiêu × phiên bản × ngày phát hành; giữ mọi phiên bản; thực tế và sai số được chấm khi ngày mục tiêu đã xác nhận đủ dữ liệu |
| `fact_replenishment_daily` | Periodic snapshot | SKU × ngày đánh giá; ghi `model_key` của phiên bản đã dự báo. Dòng hôm nay được cập nhật, các ngày trước được giữ, không bị TRUNCATE kể cả khi `--full` |

## 4. Kiểm chứng

| Kiểm tra | Kết quả |
|---|---|
| Đối soát với `public` | 157.901 dòng hàng, 377.710 sản phẩm, 79.244.673.830 ₫, 157.694 đơn: khớp tuyệt đối |
| `dim_date` | 1.095 ngày liên tục; 319 ngày đã xác nhận |
| Thời gian | Nạp đầy đủ 76 giây (một lần); gia tăng khoảng 2 giây |
| Test tĩnh | 3 đạt: không có ký tự phần trăm trong SQL, mọi bảng chiều có dòng −1, lịch sử đề xuất không bị TRUNCATE |
| Test PostgreSQL (`RUN_MART_TESTS=1`, rollback) | 5 đạt: đối soát; bảng ngày liên tục, khóa yyyymmdd; hai phiên bản dự báo cùng SKU × ngày được giữ, chỉ bản mới nhất có cờ, thực tế và sai số đúng; đề xuất ngày trước không bị ghi lại kể cả khi nạp đầy đủ; fact không thiếu khóa |
| Mô phỏng Refresh Power BI | `PASS` 42 nguồn, gồm 14 bảng mart và 9 nhóm quan hệ của star schema |
| Bộ test | 254 đạt; 54 PostgreSQL opt-in đạt |

## 5. Báo cáo Power BI (PBIP)

`scripts/build_powerbi_project.py` sinh `powerbi/BaoCaoQuanLy/` từ cấu trúc thật của `mart`:
- **Semantic model:** 15 bảng, 23 quan hệ (2 không hoạt động: ngày hoàn tất và ngày phát hành dự báo), 48 measure, `dim_date` được đánh dấu là bảng ngày, tham số `pServer`/`pDatabase`.
- **Report (PBIR):** 6 trang, 54 visual.

**Đã kiểm chứng:**
- 65/65 tệp report hợp lệ theo JSON schema chính thức của Microsoft;
- kiểm tra nội bộ: quan hệ, sortByColumn, mọi trường visual tham chiếu đều tồn tại.

**Chưa kiểm chứng:** `model.bim`, DAX và truy vấn M khi mở trong Power BI Desktop. Phần này chờ sinh viên mở và báo lỗi nếu có.

## 6. Giới hạn

- **Bảng so sánh phiên bản và `Live WAPE`** có số từ khi ngày 16/11 được xác nhận. Các phiên bản v1/v2 dự báo cho SKU của bộ CSV công khai, không chấm được bằng dữ liệu kho.
- **Lịch sử đề xuất** bắt đầu từ 16/11/2026.
- **Nạp đầy đủ** mất khoảng 76 giây và giữ khóa làm mới; chỉ dùng sau migration sửa dữ liệu.
- **Hàng đang về** chưa đưa vào công thức (hướng mở rộng).

## 7. Minh bạch sử dụng AI

AI (Claude) thiết kế mô hình theo yêu cầu của sinh viên, viết DDL, hàm làm mới, tích hợp worker/job dự báo, test, kiểm chứng và tài liệu. Sinh viên quyết định dùng bảng vật lý, giữ lịch sử đề xuất, theo dõi phiên bản dự báo và dạng khóa ngày.
