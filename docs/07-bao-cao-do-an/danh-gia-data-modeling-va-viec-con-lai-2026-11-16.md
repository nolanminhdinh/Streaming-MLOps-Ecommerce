# Đánh giá data modeling và các phần còn chưa hoàn thiện — 16/11/2026

Số liệu trong tài liệu này đo trực tiếp trên PostgreSQL local ngày 16/11/2026, đối chiếu với `warehouse/ddl/01_star_schema.sql`, `warehouse/ddl/04_integrity_constraints.sql`, `warehouse/reporting/bi_schema.sql` và `warehouse/README.md`.

## 1. Kết luận

Mô hình dữ liệu **phù hợp về cấu trúc** với bài toán: star schema ở độ hạt dòng hàng phục vụ tổng hợp nhu cầu theo SKU × ngày cho dự báo và báo cáo. Có:
- snapshot tồn kho theo ngày;
- fact dự báo có provenance;
- dữ liệu lịch sử ngoài tách riêng;
- schema `bi` làm lớp ngữ nghĩa cho Power BI.

Ràng buộc toàn vẹn đầy đủ và nhất quán: 0 lệch giữa `date_key` và ngày `create_time` trên 145.660 dòng; chiều thời gian đúng quy ước `DOW` (Chủ nhật = 0).

Tuy nhiên, **dữ liệu bên trong mô hình** có năm vấn đề làm sai lệch báo cáo hoặc làm đứt mạch dự báo, xếp theo mức độ ở mục 3:
1. dữ liệu thử lẫn vào bảng thật;
2. chiều shop bị "nổ";
3. danh mục sản phẩm không thống nhất giữa luồng streaming và dữ liệu huấn luyện;
4. bảng coverage trống nên dự báo chưa từng được chấm;
5. chiều thời gian thiếu thuộc tính nghiệp vụ.

## 2. Những điểm mô hình làm đúng

| Khía cạnh | Hiện trạng | Đánh giá |
|---|---|---|
| Độ hạt fact bán hàng | `Fact_Orders`: một dòng hàng, khóa nghiệp vụ `(platform, order_id, source_line_id)`, ghi chú độ hạt trong `COMMENT ON TABLE` | Đúng cho dự báo theo SKU × ngày; chống trùng ở mức khóa |
| Loại fact tồn kho | `Fact_Inventory_Daily`: periodic snapshot, 1 dòng/SKU/ngày (`uq_fact_inventory_product_date`); `bi.inventory_latest` không cộng snapshot giữa các ngày | Đúng loại fact; tránh lỗi cộng tồn kho phổ biến |
| Fact dự báo | `Fact_Forecast_Predictions` lưu model, version, nguồn lịch sử, `data_origin`, `source_datasets`; khóa duy nhất theo lần phát | Có provenance; đối chiếu được với thực tế |
| Tách nguồn ngoài | `Fact_Historical_Order_Lines` và `bi.dim_historical_product` riêng; không ép SKU ngoài vào doanh thu | Tránh trộn domain dữ liệu |
| Ràng buộc | FK vật lý, `NOT NULL` cho product/date, `CHECK` số lượng dương, tiền không âm/NaN, tồn kho không âm, sàn hợp lệ | Chặn lỗi tại tầng lưu trữ |
| Lớp ngữ nghĩa | Schema `bi` đổi tên cột mơ hồ (`unit_cost` → `reference_selling_price`), cờ `missing_shop`, `missing_geography`, `header_conflict`, `snapshot_time_known` | Power BI dùng được mà không cần biết chi tiết bảng vật lý |
| Thời gian | `date_key` khớp ngày `create_time` theo giờ Việt Nam ở 145.660/145.660 dòng; `ensure_dim_dates` thêm ngày khi cần | Nhất quán |
| Địa lý | 20 dòng, không trùng, không NULL `city`/`district`/`region` | Sạch |

## 3. Vấn đề phát hiện

### 3.1. Dữ liệu thử nằm chung bảng với dữ liệu nghiệp vụ — giữ nguyên theo quyết định của sinh viên

> **Quyết định của sinh viên (16/11/2026): giữ nguyên, không gắn cờ, không xóa.**
>
> Lý do: toàn bộ dữ liệu trong kho đều là dữ liệu mô phỏng/kiểm thử. Phần gọi là "nghiệp vụ" ở bảng dưới cũng do simulator sinh ra. Phân biệt "dữ liệu thử" với "dữ liệu nghiệp vụ" vì vậy không có ý nghĩa trong phạm vi demo, và các lô có nhãn được giữ lại làm bằng chứng truy vết cho các bài kiểm tra.
>
> Đề xuất khắc phục bên dưới chỉ áp dụng khi hệ thống nhận dữ liệu doanh nghiệp thật. Khi đó, kiểm thử phải chạy trên database/schema riêng, không ghi vào kho vận hành.

**Hiện trạng.** Các bài kiểm tra E2E, nghiệm thu và demo web ghi dữ liệu có nhãn vào chính `public.Fact_Orders`, `Dim_Products`, `Fact_Inventory_Daily`.

| | Dòng fact | Sản phẩm | Số tiền (VND) | SKU |
|---|---|---|---|---|
| Nghiệp vụ | 145.281 | 347.068 | 88.819.624.350 | 107 |
| Dữ liệu thử (`RPT`, `E2E`, `AUTO`, `FLT`, `CMP`, `WEB…`/category `DEMO`) | 379 | 765 | 151.953.080 | 84 |

Không view nào trong `bi` hoặc `powerbi/views_for_powerbi.sql` lọc nhãn thử. Riêng demo web (`WEB…`, category `DEMO`) chiếm 86 dòng và khoảng 36,1 triệu đồng. Các nhãn `RPT`, `FLT`, `AUTO` ngày 15–16/11 do các bài kiểm tra có AI hỗ trợ tạo ra.

**Ảnh hưởng.**
- Tổng doanh thu và số lượng lệch nhỏ (khoảng 0,17%).
- Tính theo SKU thì lệch lớn: 84/191 SKU có đơn là SKU thử. Số SKU, phân loại ABC/XYZ, top sản phẩm và tồn kho theo SKU trên Power BI đều bị ảnh hưởng.

**Khắc phục (cần sinh viên quyết định, vì liên quan xóa dữ liệu).**
- **Ngắn hạn, không xóa:**
  - Thêm bảng `public.test_data_labels(prefix, source, created_at)`.
  - Thêm cờ `is_test_data` vào `bi.fact_order_line`, `bi.dim_product`, `bi.fact_inventory_daily`.
  - Power BI lọc `is_test_data = false` ở cấp model.
- **Dài hạn:** chạy kiểm thử trên database hoặc schema riêng (các test opt-in PostgreSQL đã làm theo cách rollback). Các script E2E nhận tham số đích kiểm thử, không ghi vào kho thật.

### 3.2. Chiều shop bị "nổ" do dữ liệu simulator cũ — **đã xử lý 16/11/2026**

> **Đã xử lý và kiểm chứng (16/11/2026), theo quyết định của sinh viên.**
>
> 1. **Gom shop** — `scripts/migrate_consolidate_shops.py`:
>    - diễn tập có rollback, rồi `--apply`; giữ khóa ETL chung với worker;
>    - mỗi `(platform, shop_name)` giữ `shop_key` nhỏ nhất; 5.675 dòng fact được trỏ lại, 22.023 dòng shop thừa bị xóa (22.080 → 57);
>    - 6 kiểm tra đạt: tổng fact và tổng theo từng tên shop không đổi, mỗi tên một khóa, không fact mồ côi, view `bi` khớp, view chất lượng rỗng;
>    - shop demo/kiểm thử được gán `shop_id` cố định (`DEMO-SHOPEE`, `CONTINUOUS-TEST`, `FAULT-TEST`);
>    - bảng ánh xạ: `output/shop-consolidation-2026-11-16/shop_key_mapping_apply.csv`.
> 2. **Chặn nguồn sinh lại:**
>    - `scripts/demo_web_server.py`, `check_continuous_pipeline.py`, `check_fault_recovery.py` dùng shop cố định thay vì shop mới mỗi lượt;
>    - thêm view `bi.quality_shop_identity` (rỗng là trạng thái đúng), được kiểm tra trong `scripts/check_powerbi_refresh_readiness.py`.
> 3. **Lỗi ETL phát hiện trong lúc kiểm chứng (mục 3.2b)** và sửa dữ liệu bị ảnh hưởng.
>
> Kết quả sau cùng:
> - Shopee 22 shop / 22 tên, TikTok 37 shop / 37 tên;
> - không còn `shop_id` rỗng, `bi.quality_shop_identity` rỗng;
> - 241 test và 50 test PostgreSQL opt-in đạt.
>
> Giới hạn: backfill lại raw cũ của simulator trước 03/10 sẽ tạo lại shop_id ngẫu nhiên, nhưng view chất lượng sẽ phát hiện.

**Hiện trạng.**
- `Dim_Shops` có 22.080 dòng. Riêng Shopee có 22.044 `shop_id` nhưng chỉ 22 tên shop; 10 tên của simulator cũ chiếm 22.006 id (khoảng 2.200 id mỗi tên, dải 10000002–99994059).
- Simulator hiện tại dùng một shop cố định (`MockStore Official Mall`, id `10000001`). Các id ngẫu nhiên là di sản của phiên bản simulator cũ, sinh `shop_id` mới cho mỗi đơn.
- 16.504 dòng shop không được fact nào tham chiếu.
- 6.263 dòng fact Shopee của simulator cũ gắn với 5.535 `shop_key`, gần như mỗi đơn một shop.

**Nguồn gốc (truy vết ngày 16/11/2026).**

Simulator ở commit `3768ac0` (25/09/2026) chọn shop như sau:

```python
def _pick_shop(self):
    name = random.choice(SHOP_NAMES)                    # 10 tên cố định
    shop_id = str(random.randint(10000000, 99999999))   # id ngẫu nhiên MỚI cho mỗi đơn
```

Hệ quả:
- Mỗi đơn Shopee mang một `shop_id` 8 chữ số ngẫu nhiên, dù tên shop chỉ là 1 trong 10 tên.
- `Dim_Shops` dùng khóa tự nhiên `(shop_id, platform)` (`load.py`, `ON CONFLICT (shop_id, platform)`), nên mỗi id mới thành một dòng shop mới.
- TikTok không bị, vì simulator cũ bỏ qua id TikTok (`shop_name, _, _ = self._pick_shop()`) và ETL dùng `shop_name` làm `shop_id` cho TikTok (`transform.py` dòng 179).
- Commit `fc96ec6` (03/10/2026) chuyển sang mô hình một doanh nghiệp với shop cố định `MockStore Official Mall` / `10000001`, nên lỗi dừng phát sinh từ đó.

Cấu thành 22.044 dòng Shopee và 22 tên:

| Nhóm | Số tên | Số `shop_id` | Nguồn |
|---|---|---|---|
| Shop của simulator cũ | 10 (`FashionKorea_VN`, `SneakerHub_SG`, …, `MinhShopVN`) | 22.006 (2.172–2.233 mỗi tên) | `random.randint` mỗi đơn |
| Shop kiểm thử | 11 (`DEMO bảo vệ`, `Continuous test`, `Fault test`, `E2E test shop`, `Soak audit`, `Pipeline acceptance`, 5 `Integrity test DI…`) | 37 | Script kiểm tra đặt `shop_id` bằng nhãn lượt chạy |
| Shop hiện hành | 1 (`MockStore Official Mall`) | 1 | Simulator từ 03/10 |

16.504 dòng shop không còn fact tham chiếu là vì lần 8 (31/10/2026) đã thay 106.131 đơn mô phỏng cũ bằng dữ liệu simulator mới. Fact được thay, nhưng dimension chỉ upsert, không bao giờ xóa, nên các shop của đơn cũ vẫn ở lại.

Mô hình không sai: trong dữ liệu Shopee thật, `shop_id` là định danh ổn định của một gian hàng. Lỗi nằm ở dữ liệu nguồn vi phạm giả định đó. Mô hình thiếu một kiểm tra chất lượng để phát hiện tình huống "một tên shop có quá nhiều id".

**Ảnh hưởng.**
- Phân tích theo `shop_key` (số đơn mỗi shop, top shop) cho kết quả vô nghĩa.
- Quan hệ `dim_shop` → `fact_order_line` trong Power BI có hàng chục nghìn thành viên rác.
- Ràng buộc `UNIQUE(shop_id, platform)` không phát hiện được lỗi này, vì đây là lỗi ở nguồn chứ không phải trùng khóa.

**Khắc phục.**
- Lập bảng ánh xạ `shop_key` cũ → `shop_key` chuẩn theo `(platform, shop_name)`. Cập nhật `Fact_Orders.shop_key` trong một migration có diễn tập và rollback, giống `migrate_warehouse_schema.py`.
- Đánh dấu hoặc xóa các dòng shop không còn được tham chiếu.
- Thêm kiểm tra chất lượng: cảnh báo khi số `shop_id` mới trong một lô vượt ngưỡng.

Cần sinh viên quyết định vì đây là chỉnh sửa dữ liệu lịch sử.

### 3.2b. Lỗi ETL: đơn TikTok trong lô trộn sàn dồn vào một shop rỗng — đã sửa 16/11/2026

**Phát hiện.** Sau khi gom shop, một đợt demo web làm `bi.quality_shop_identity` có dòng: TikTok `MockStore Official Shop` mang 2 khóa. Một trong hai là khóa 5520 với `shop_id` rỗng, đang giữ 61.759 đơn TikTok, và tên của nó vừa bị lệnh upsert đổi theo lô mới nhất.

**Nguyên nhân.**
- Lô ETL thường gồm object của cả hai sàn, nên DataFrame có cột `shop_id` (từ Shopee) và giá trị ở dòng TikTok là `NaN`.
- `_unify_tiktok_row` dùng `row.get("shop_id") or row.get("shop_name")`. Vì `NaN` là truthy, kết quả là `NaN`; load ghi thành chuỗi rỗng.
- Mọi đơn TikTok của lô trộn dồn vào `(shop_id='', tiktok)`. Mỗi lô, `ON CONFLICT … DO UPDATE SET shop_name` ghi đè tên dòng này theo đơn TikTok đầu tiên.
- `scripts/simulate_historical_data.py` (nạp lịch sử lần 8) cũng dùng `unify_schema` trên dữ liệu trộn nên bị cùng lỗi.
- Đã tái hiện offline: lô chỉ TikTok cho `shop_id` đúng; lô trộn cho `nan`.

Lỗi này cũng khiến migration gom shop (bước 1 mục 3.2) chuyển 24 đơn của `DEMO WEB20261104162313c84ab6` vào khóa 5520, vì lúc đó khóa 5520 tạm mang đúng tên này. Bước sửa dữ liệu dưới đây đã trả 24 đơn về shop demo theo bản gốc.

**Sửa mã.**
- `warehouse/etl/transform.py` thêm `_present_text()` coi `None`/`NaN`/chuỗi rỗng là thiếu, dùng cho `shop_id` của cả TikTok và Shopee. Trước đây, Shopee thiếu `shop_id` trong lô trộn thành chuỗi `"nan"`.
- Test hồi quy `test_mixed_platform_batch_keeps_shop_identity`.

**Sửa dữ liệu** — `scripts/migrate_repair_tiktok_shops.py`: diễn tập rồi `--apply`. Mỗi đơn được quy về shop theo căn cứ, ghi trong `output/shop-consolidation-2026-11-16/tiktok_shop_attribution_apply.csv`:

| Căn cứ | Số đơn | Shop |
|---|---|---|
| Bản gốc Parquet trên MinIO | 3.045 | `MockStore Official Shop` |
| Bản gốc Parquet trên MinIO | 24 | `DEMO WEB20261104162313c84ab6` |
| Mã nguồn: đơn `SIM26YTD-…` do `data_simulator/history.py` sinh (lần 8), simulator luôn dùng shop TikTok cố định trong `ENTERPRISE_CONFIG` | 54.181 | `MockStore Official Shop` |
| Không còn bản gốc (30/09–01/10, simulator cũ chọn ngẫu nhiên 1 trong 10 tên) | 4.509 | `Không xác định (TikTok, simulator cũ)`, không đoán tên |

Kiểm tra:
- tổng fact theo từng sàn không đổi;
- 61.759/61.759 đơn được trỏ lại;
- không còn đơn ở shop rỗng (dòng shop rỗng đã xóa);
- mỗi tên một khóa; không fact mồ côi.

**Kiểm chứng trên luồng thật.**
- Producer streaming và một đợt demo web sau khi sửa: 8/8 lượt ETL gần nhất đều trộn Shopee + TikTok.
- 189 đơn TikTok mới đều vào `MockStore Official Shop`, 6 đơn demo TikTok vào `DEMO bảo vệ`.
- Không phát sinh shop rỗng hay trùng tên.

### 3.3. Danh mục sản phẩm không thống nhất giữa streaming và dữ liệu huấn luyện — ưu tiên cao (mức đồ án)

**Hiện trạng.**
- Model đang phục vụ huấn luyện trên `public_anonymized_historical_csv`: 7.000 dòng, 228 SKU, 26/12/2025–11/08/2026.
- Luồng streaming có 191 SKU.
- **Hai tập SKU không giao nhau (0 SKU chung).**
- Cả 27 dự báo đã lưu thuộc 3 SKU lịch sử; 0 dự báo cho SKU streaming.
- Để lưu dự báo, 3 SKU lịch sử đã được chèn vào `Dim_Products` (FK `NOT NULL` của `Fact_Forecast_Predictions`). Điều này trái nguyên tắc "danh mục lịch sử tách riêng" trong `warehouse/README.md`.
- Thử ngày 16/11: dự báo cho SKU streaming `OL-IP15PM` trả `model_source = heuristic_catalog`, `history_source = none`.

**Ảnh hưởng.**
- Luồng streaming (trọng tâm đồ án) và nhánh ML không dùng chung dữ liệu: model không dự báo được SKU đang bán.
- Đề xuất nhập hàng cho SKU streaming dựa trên heuristic.
- Trên Power BI, `Dim_Products` lẫn 3 SKU lịch sử không có doanh thu.

**Khắc phục.**
- **Phương án A, phù hợp đồ án:** công bố rõ giới hạn này. Khi luồng streaming tích lũy đủ lịch sử (ít nhất `TRAINING_MIN_FIT_DAYS` cộng lag 28 ngày), huấn luyện trên `data_origin` streaming để model dùng đúng danh mục đang bán.
- **Phương án B, mô hình hóa:** tạo chiều sản phẩm thống nhất theo nguồn: `dim_product(product_key, source_system, source_sku, …)` cùng bảng ánh xạ SKU giữa các nguồn khi có căn cứ nghiệp vụ. `Fact_Forecast_Predictions` tham chiếu khóa này thay vì chèn SKU lịch sử vào catalog vận hành.

### 3.4. Bảng coverage trống nên dự báo chưa từng được chấm — ưu tiên trung bình

**Hiện trạng.**
- `Warehouse_Daily_Coverage` có 0 dòng.
- `bi.forecast_evaluation` có 6 dòng, đều `actual_complete = false`.
- Coverage chỉ được ghi thủ công qua `scripts/confirm_daily_coverage.py`.

**Ảnh hưởng.** Báo cáo "dự báo so với thực tế" trên Power BI không có dòng nào được chấm, nên vòng đánh giá MLOps sau triển khai chưa khép.

Cơ chế chặn này là đúng thiết kế: chưa xác nhận đủ dữ liệu thì không chấm. Nhưng chưa có ngày nào được xác nhận.

**Khắc phục.**
- Sinh viên xác nhận coverage cho các ngày đã có căn cứ bằng `confirm_daily_coverage.py`.
- Hoặc để worker tự ghi coverage cho ngày streaming khi ngày đã qua, không còn object chờ và lag Kafka bằng 0. Điều kiện này cần được thống nhất trước, vì coverage là một khẳng định về độ đầy đủ của dữ liệu.

### 3.5. Chiều thời gian thiếu thuộc tính nghiệp vụ và nhãn bị đệm khoảng trắng — một phần giữ nguyên theo quyết định của sinh viên

> **Quyết định của sinh viên (16/11/2026): không thêm cờ ngày lương và ngày lễ vào `Dim_Dates`.**
>
> Lý do: mỗi doanh nghiệp có ngày trả lương khác nhau, nên cờ ngày lương cố định (15/25) không phải thuộc tính chung của lịch. Đưa nó vào chiều thời gian sẽ áp một giả định của một công ty cho mọi phân tích. Với ngày lễ, sinh viên cũng quyết định không bổ sung trong phạm vi đồ án. Hiệu ứng ngày lương/sale hiện được mô phỏng trong simulator và đặc trưng ML, không thuộc lớp mô hình dữ liệu dùng chung.
>
> **Nhãn `day_name`/`month_name` bị đệm khoảng trắng: đã sửa ngày 16/11/2026.**
>
> | Vị trí | Thay đổi | Phạm vi áp dụng |
> |---|---|---|
> | `warehouse/ddl/01_star_schema.sql` | `TO_CHAR(d, 'Day'/'Month')` → `'FMDay'/'FMMonth'`; sửa luôn comment `day_of_week` cho đúng quy ước Chủ nhật = 0 | Kho mới |
> | `warehouse/etl/load.py` (`ensure_dim_dates`) | Đổi tương tự | Ngày mới ETL thêm sau 2027 |
> | `warehouse/ddl/04_integrity_constraints.sql` | `UPDATE … SET day_name = btrim(day_name), month_name = btrim(month_name)` chỉ trên dòng còn khoảng trắng; chạy lại không đổi gì | Kho đang có |
>
> Áp dụng bằng `scripts/migrate_warehouse_schema.py`:
> - diễn tập có rollback, rồi `--apply`;
> - tổng đơn, số lượng, tiền, tồn kho, dự báo và lịch sử trước và sau giống hệt;
> - bằng chứng ở `evidence/warehouse_schema_rehearsal_2026-11-16.json` và `evidence/warehouse_schema_apply_2026-11-16.json`.
>
> Kết quả:
> - 1.095/1.095 ngày không còn khoảng trắng; đủ 7 thứ và 12 tháng.
> - Không đổi `date_key`, `full_date` hay fact.
> - 240 test và 50 test PostgreSQL opt-in đạt; worker khởi động lại healthy.
> - Power BI cần Refresh để nhận nhãn mới.
> - CSV dự phòng cũ trong `powerbi/data` vẫn giữ nhãn có khoảng trắng cho tới lần xuất CSV tiếp theo.

**Hiện trạng.**
- `is_holiday` bằng `false` cho toàn bộ 1.095 ngày, chưa có Tết, 30/4, 2/9.
- Không có cờ ngày lương (15 và 25), dù `PROJECT_CONTEXT.md` mục 2.1 nêu đây là yếu tố gây biến động nhu cầu.
- `day_name` và `month_name` do `TO_CHAR(d, 'Day')` sinh ra bị đệm khoảng trắng (`'Monday   '`, `'November '`).

**Ảnh hưởng.** Power BI không phân tích được theo lễ, Tết hay ngày lương. Nhãn có khoảng trắng gây lệch hiển thị, và có thể tách thành nhiều giá trị nếu sau này có nguồn ghi không đệm.

**Khắc phục.**
- Dùng `TO_CHAR(d, 'FMDay')` và `'FMMonth'`; cập nhật các dòng cũ bằng `trim()`.
- Thêm `is_payday` và bảng ngày lễ Việt Nam (gồm Tết âm lịch theo năm) để cập nhật `is_holiday`.
- Đối chiếu với đặc trưng lễ/sale trong `ml/features` để báo cáo và model dùng cùng định nghĩa.

### 3.6. Các điểm nhỏ hơn

| Điểm | Hiện trạng | Đề xuất |
|---|---|---|
| Khóa chiều thiếu giá trị | `carrier_key` NULL 27,41%; `geo_key`, `payment_key` 0,43%; `shop_key` 0,02% | Thêm thành viên "Không xác định" (khóa −1) theo chuẩn Kimball để Power BI không hiện ô trống; giữ cờ `missing_*` |
| Lịch sử trạng thái đơn | `Fact_Orders` chỉ giữ trạng thái mới nhất (ghi đè khi chuyển trạng thái); lịch sử chỉ còn ở raw lake | Đủ cho dự báo. Nếu cần phân tích phễu hoặc thời gian xử lý đơn, thêm fact sự kiện trạng thái `fact_order_status_event` |
| Thuộc tính sản phẩm | `sub_category` NULL 100%; category `Chung` 66 SKU; 9 SKU thiếu giá/trọng lượng; `unit_cost` thực chất là giá bán | Bỏ hoặc điền `sub_category`; đổi tên cột vật lý ở migration sau; quyết định có cần lịch sử thay đổi danh mục (SCD2) hay không |
| Phiên bản sự kiện | `source_updated_at` NULL 10,75% (dòng legacy, guard so với timestamp lớn nhất) | Chấp nhận; ghi là dữ liệu legacy |
| Đơn xung đột header | 28 mã đơn, 66 dòng khác ngày/shop/địa lý/trạng thái hủy (đã biết từ 01/11) | Vẫn cần truy vết payload gốc; view `bi.quality_order_header_conflicts` đang đánh dấu |
| Tiền cấp header | 29 đơn nhiều dòng; hiện không có đơn nào lặp cùng số tiền ở mọi dòng | Chưa có quy tắc phân bổ tiền header xuống dòng cho nguồn nhiều SKU thật (đã nêu trong `warehouse/README.md`) |
| Múi giờ cột thời gian | `create_time` lưu theo giờ Việt Nam, còn `loaded_at` (`DEFAULT NOW()`) theo giờ UTC của PostgreSQL; cả hai đều `TIMESTAMP` không múi giờ (ví dụ đơn tạo 21:53 có `loaded_at` 14:53) | Khi hiển thị `loaded_at` trên Power BI, ghi rõ UTC hoặc cộng 7 giờ; về lâu dài dùng `TIMESTAMPTZ` |
| Tồn kho | 24 snapshot legacy không biết thời điểm sự kiện; SS/ROP trong fact đến từ simulator, khác SS/ROP do API tính | Phân biệt rõ hai nguồn SS/ROP trên báo cáo |

## 4. Các phần đồ án còn chưa hoàn thiện

Tổng hợp từ mã, dữ liệu và các tài liệu đánh giá gần nhất (`doi-chieu-rubric-cham-diem-2026-11-08.md`, `danh-gia-lai-pipeline-va-cam-nang-2026-11-08.md`, `kiem-tra-e2e-va-yeu-cau-pipeline-thuc-te-2026-11-16.md`).

### 4.1. Ảnh hưởng trực tiếp đến buổi bảo vệ

| # | Việc | Hiện trạng |
|---|---|---|
| 1 | Báo cáo Word chưa có kết quả 15–16/11 | Bản mới nhất `output/docx/…_2026-11-08_pipeline.docx`. Chưa có: chạy lại 20/20, thử lỗi 3/3, sửa API tồn kho, sửa giám sát, đánh giá data modeling này |
| 2 | Power BI theo schema `bi` | Sinh viên tự hoàn thiện. Có `powerbi/Report_PowerBI_Postgres.pbix`, chưa được nghiệm thu trong hồ sơ. Cần lọc dữ liệu thử (3.1) trước khi chụp số liệu |
| 3 | Thông tin tác giả trong `README.md` | Dòng 173–175 còn `_(điền tên)_`, `_(điền thông tin)_` |
| 4 | Slide, diễn tập, hỏi đáp | Chưa có bằng chứng trong hồ sơ (checklist học vụ trang 23–25) |
| 5 | Minh chứng sử dụng AI | Có khai báo; còn thiếu ảnh quá trình và phần tự kiểm tra theo cẩm nang |
| 6 | Commit | Toàn bộ thay đổi 15–16/11 (sửa serving, demo web, Compose, script kiểm tra, tài liệu) chưa được commit |

### 4.2. Nội dung kỹ thuật còn mở theo rubric

| # | Việc | Ghi chú |
|---|---|---|
| 7 | Đối chứng hiệu năng có kiểm soát | Hai cấu hình (ví dụ batch/poll hoặc số partition/worker), cùng dữ liệu và phần cứng; ghi môi trường thực nghiệm |
| 8 | Quy mô mục tiêu (5V) và thử nhiều mức tải | Ca soak 600 đơn/10 phút chưa chứng minh công suất tối đa |
| 9 | Xác thực nguồn dữ liệu lịch sử | Quyền sử dụng, múi giờ chưa xác nhận, độ phủ ngày, ánh xạ SKU giữa hai nguồn (liên quan 3.3) |
| 10 | Coverage và đánh giá dự báo sau triển khai | Mục 3.4 |
| 11 | Model phục vụ chưa dùng dữ liệu streaming | Mục 3.3; model ở `Staging`, WAPE cao là giới hạn đã công bố |
| 12 | Feature spec Registry khác local | `generated_at`, `sku_profiles` lệch từ 19/10; thứ tự cột feature khớp |

### 4.3. Hướng phát triển (ghi là giới hạn, không bắt buộc trước bảo vệ)

- Giới hạn lưu trữ MinIO/checkpoint ([Lần 12](../04-pipeline-updates/lan-12-chay-lai-du-lieu-va-gioi-han-luu-tru-2026-11-16.md)).
- Metric lag Kafka, receiver cảnh báo ra ngoài, SLO, CI, mở rộng, HA/sao lưu, bảo mật, dữ liệu cá nhân ([mục 6 báo cáo E2E](kiem-tra-e2e-va-yeu-cau-pipeline-thuc-te-2026-11-16.md)).

## 5. Thứ tự đề xuất trước bảo vệ

1. ~~Quyết định cách xử lý dữ liệu thử (3.1)~~ — đã quyết định giữ nguyên (16/11). ~~Chiều shop (3.2)~~ — đã gom và sửa lỗi ETL TikTok (3.2b), kiểm chứng Refresh Power BI `PASS` khi đang streaming (16/11).
2. ~~Thêm cờ ngày lương/ngày lễ (3.5)~~ — đã quyết định không thêm (16/11). ~~Nhãn ngày bị đệm khoảng trắng~~ — đã sửa (16/11).
3. **Xác nhận coverage cho các ngày đủ căn cứ (3.4)** nếu muốn trình bày "dự báo so với thực tế".
4. Chuẩn bị câu trả lời cho 3.3 (model và streaming dùng hai danh mục khác nhau): đây là câu hỏi phản biện dễ gặp nhất.
5. Cập nhật báo cáo Word, slide, thông tin tác giả, rồi commit.

## 6. Tái lập số liệu

Các truy vấn trong tài liệu chạy chỉ đọc trên `ecom_warehouse`:
- đếm dòng từng bảng;
- `GROUP BY` theo `platform`/`shop_name` trên `dim_shops`;
- tỷ lệ NULL khóa ngoại;
- phân loại nhãn thử theo tiền tố `order_id`/`sku`/category;
- đối chiếu `date_key` với `create_time::date`;
- giao tập SKU giữa `fact_historical_order_lines` và `fact_orders`;
- `actual_complete` trong `bi.forecast_evaluation`.

## 7. Minh bạch sử dụng AI

AI (Claude) hỗ trợ: đọc DDL, schema `bi` và tài liệu; chạy truy vấn chỉ đọc trên PostgreSQL local; tổng hợp đánh giá và đề xuất. Không sửa schema, dữ liệu hay mã trong lượt này. Một phần dữ liệu thử nêu ở 3.1 (`RPT`, `FLT`, `AUTO` ngày 15–16/11) do các bài kiểm tra có AI hỗ trợ tạo ra. Sinh viên đã quyết định giữ nguyên dữ liệu thử (3.1) và không thêm cờ ngày lương/ngày lễ (3.5). Theo yêu cầu của sinh viên, AI sửa nhãn ngày/tháng bị đệm khoảng trắng (3.5) và truy vết nguồn gốc 22.044 `shop_id` qua lịch sử git (3.2). Theo quyết định của sinh viên, AI viết và chạy migration gom shop (3.2), phát hiện và sửa lỗi ETL TikTok cùng dữ liệu bị ảnh hưởng (3.2b), cho demo web và script kiểm tra dùng shop cố định, và viết `scripts/check_powerbi_refresh_readiness.py` để mô phỏng Refresh. Việc quy 54.181 đơn về `MockStore Official Shop` dựa trên mã nguồn simulator, không dựa trên bản gốc dữ liệu; 4.509 đơn không có căn cứ được gán shop "Không xác định". Đề xuất 3.4 vẫn chờ sinh viên quyết định.
