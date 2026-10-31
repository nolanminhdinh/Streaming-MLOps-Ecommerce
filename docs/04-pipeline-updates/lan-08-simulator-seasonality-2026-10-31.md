# Lần 8 — Nhu cầu lịch sử theo SKU và nhập kho thích ứng

## 1. Tổng quan và phạm vi

**Ngày thực hiện:** 31/10/2026, múi giờ `Asia/Ho_Chi_Minh` (UTC+07).
**Commit triển khai:** [`54c39a4`](https://github.com/nolanminhdinh/Streaming-MLOps-Ecommerce/commit/54c39a4acc5afb0c85a4ad94d87a4d03a088e42d).
**Kho dữ liệu:** PostgreSQL 16, database `ecom_warehouse`.

Người dùng nhận thấy tổng đơn theo tháng gần như bằng nhau sau khi nạp lịch sử
từ đầu năm. Đợt sửa này chuyển bộ sinh lịch sử từ tổng đơn ngày cố định sang
lấy mẫu nhu cầu riêng cho từng SKU, đồng thời điều chỉnh nhập kho theo quy mô
nhu cầu. Bộ mới đã thay đúng đợt `SIM26YTD-`, giữ dữ liệu có trước đợt đó.

| Nội dung | Kết quả sau thay thế |
|---|---:|
| Khoảng ngày nghiệp vụ | 01/01–31/10/2026 |
| Số ngày có đơn mô phỏng | 304 |
| Đơn mô phỏng mới | 125.423 |
| Đơn cũ được bảo vệ | 19.428 |
| Tổng dòng `fact_orders` | 144.851 |
| Tồn kho mô phỏng mới | 6.060 dòng |
| Tồn kho cũ được bảo vệ | 38 dòng |
| Tổng dòng `fact_inventory_daily` | 6.098 |

Các số đếm đơn ở đây là dòng `fact_orders`; kiểm chứng runtime cũng xác nhận
125.423 mã đơn khác nhau trong bộ mới, do simulator này tạo một dòng/SKU/đơn.
Không áp dụng cách đếm dòng này để suy ra số đơn cho nguồn nhiều dòng hàng khác.

Lượt chạy tạo payload mô phỏng, chuẩn hóa và nạp PostgreSQL trực tiếp. Không
nghiệm thu lại tuyến Kafka → MinIO, API/BI hoặc huấn luyện lại mô hình dự báo.
Các tham số mô phỏng là giả định kịch bản, chưa được hiệu chỉnh bằng doanh số thực.

## 2. Nguyên nhân và nội dung sửa

### 2.1. Vì sao dữ liệu trước quá đều?

Lượt trước dùng mức nền 300 đơn/ngày, thứ Bảy nhân 1,3, Chủ nhật nhân 1,2 và ngày
trùng ngày/tháng nhân 3,5. Vì số ngày và số cuối tuần của các tháng gần nhau,
tổng tháng chỉ nằm trong khoảng 9.750–11.025 đơn. Các trường giờ đặt, sản phẩm
và trạng thái có ngẫu nhiên, nhưng tổng đơn ngày vẫn là công thức xác định.

Lượt trước chạy bằng trình điều phối cục bộ `scratch/backfill_ytd_20261031.py`,
kế thừa cách tính số đơn của script lịch sử cũ. Đây là hạn chế của phương pháp
sinh nhu cầu, không phải lỗi PostgreSQL hoặc pgAdmin.

### 2.2. So sánh thiết kế

| Thành phần | Trước sửa | Sau sửa |
|---|---|---|
| Tổng đơn ngày | Mức nền cố định × cuối tuần × ngày đôi | Tổng số đơn được lấy mẫu độc lập có điều kiện theo từng SKU |
| Mức phổ biến SKU | Trọng số nền cố định | Trọng số nền kết hợp mùa vụ, xu hướng và nhiễu riêng |
| Mùa vụ mặt hàng | Chưa có đường nhu cầu riêng | 20 cấu hình SKU; ví dụ quạt/kem chống nắng, balo, đồ trang trí, hàng thiết yếu |
| Chiến dịch | Cùng hệ số ngày đôi | Mức nhạy sale theo SKU; hiệu ứng trước, trong và sau sale |
| Biến động | Ngẫu nhiên thuộc tính đơn | Thêm nhiễu thị trường, nhóm hàng, SKU, đợt tăng ngắn và ngày không có đơn |
| Kênh bán | Xác suất Shopee 55% chung | Shopee 42% ở mỹ phẩm/thời trang, 65% ở các nhóm còn lại trong bộ lịch sử |
| Kho lịch sử | Ngưỡng/lượng nhập nền cố định | Điều chỉnh theo nhu cầu quan sát và mức kế hoạch mùa vụ |
| Thứ tự sự kiện | Có thể sinh giờ lộn thứ tự trong ngày | Sắp xếp timestamp trước khi cập nhật kho |
| Mốc thời gian | Trình điều phối cũ cắt các chuyển trạng thái sau cutoff | Cơ chế dùng chung cho lịch sử: cắt timestamp và chọn lại trạng thái sàn |
| Nạp dữ liệu | Các đợt nạp riêng | Loader nhận ngữ cảnh giao dịch để hỗ trợ thay thế nguyên tử |

### 2.3. Mô hình nhu cầu và kho

Kỳ vọng `mu(sku, day)` gồm quy mô nền × tỷ trọng SKU × mùa vụ thị trường ×
mùa vụ SKU × xu hướng riêng × lịch chiến dịch/cuối tuần/payday × các trạng thái
nhiễu × hệ số tăng nhu cầu ngắn. Tổng đơn ngày/tháng không được chuẩn hóa về một
chỉ tiêu cố định. `--orders-per-day 300` là quy mô nền, không phải cam kết 300 đơn/ngày.

Với độ biến động cấu hình `v`, cường độ lấy mẫu từ Gamma có `shape=1/v²`,
`scale=mu/shape`; số đơn lấy mẫu Poisson theo cường độ đó. Trước cơ chế ngày
ngừng bán của SKU thưa, phương sai có điều kiện là `mu + v²*mu²`. Không ép có
đơn cho mọi SKU/ngày. Nhiễu thị trường và SKU có tương quan qua ngày; nhiễu nhóm
hàng thay đổi theo tháng. Đợt tăng ngắn kéo dài 3–7 ngày.

Kho dùng tối đa 14 ngày đã qua để tính trung bình **số lượng yêu cầu**, kể cả
đơn bị hủy; kết hợp 70% quan sát và 30% mức kế hoạch. Mức kế hoạch dùng lịch mùa
vụ đã cấu hình và giả định 2,5 sản phẩm/đơn, không đọc trước số đơn lấy mẫu của
ngày sắp sinh. Tồn đầu kỳ cũng được đặt theo quy mô kịch bản. Với tốc độ `r`
đơn vị/ngày, các mức sàn từ danh mục được giữ và áp dụng:

```text
SS  = max(SS_catalog,  ceil(0.8 * r))
ROP = max(ROP_catalog, ceil(r * (lead_time_days + 1.2)))
Q   = max(Q_catalog,   ceil(7 * r))
```

Đây là chính sách kiểm thử, chưa phải nghiệm tối ưu chi phí tồn kho. Lead time
và đột biến vẫn có thể gây thiếu hàng. Nhu cầu tiềm ẩn, yêu cầu đặt hàng và lượng
bán không hủy là các đại lượng khác nhau.

### 2.4. Mã nguồn thay đổi

- [`demand.py`](../../data_simulator/demand.py): profile SKU và lấy mẫu Gamma–Poisson.
- [`history.py`](../../data_simulator/history.py): lịch sử theo thời gian, kênh bán,
  kho thích ứng và trạng thái tại mốc chặn.
- [`data_simulator.py`](../../data_simulator/data_simulator.py): chọn sản phẩm có
  xét thời điểm, nhận SKU được chỉ định và sửa timestamp hoàn thành của Shopee.
- [`simulate_historical_data.py`](../../scripts/simulate_historical_data.py): dùng
  bộ sinh mới; thêm ngày bắt đầu/kết thúc, seed, tiền tố; lỗi nạp DB được truyền ra.
- [`load.py`](../../warehouse/etl/load.py): nhận engine/ngữ cảnh giao dịch tùy chọn.
- [`test_historical_demand.py`](../../tests/test_historical_demand.py): 7 kiểm thử mới.

Luồng trực tiếp vẫn dùng hàm tốc độ `orders_per_second()` hiện có; lựa chọn sản
phẩm có truyền thời điểm dùng thêm hệ số SKU. Lấy mẫu số đơn bằng mô hình mới và
chính sách kho thích ứng nêu trên thuộc luồng **lịch sử**.

## 3. So sánh trước–sau từ kết quả chạy

### 3.1. Tổng đơn theo tháng

| Tháng | Trước: đơn | Sau: đơn | Trước: đơn/ngày | Sau: đơn/ngày | Sau: hủy (%) |
|---|---:|---:|---:|---:|---:|
| 2026-01 | 10.740 | 12.932 | 346,45 | 417,16 | 13,95 |
| 2026-02 | 9.750 | 7.148 | 348,21 | 255,29 | 14,35 |
| 2026-03 | 10.710 | 8.847 | 345,48 | 285,39 | 15,18 |
| 2026-04 | 10.575 | 10.269 | 352,50 | 342,30 | 13,79 |
| 2026-05 | 10.800 | 13.781 | 348,39 | 444,55 | 14,60 |
| 2026-06 | 10.575 | 13.325 | 352,50 | 444,17 | 15,78 |
| 2026-07 | 10.650 | 10.326 | 343,55 | 333,10 | 13,47 |
| 2026-08 | 11.025 | 14.947 | 355,65 | 482,16 | 15,03 |
| 2026-09 | 10.350 | 16.670 | 345,00 | 555,67 | 14,82 |
| 2026-10 | 10.956 | 17.178 | 353,42 | 554,13 | 15,93 |

CV ở đây là độ lệch chuẩn **tổng thể** chia trung bình của 10 mức đơn/ngày theo
tháng. Trước: **1,12%**; sau: **24,25%**. Chỉ số mô tả mức phân hóa
giữa các tháng của hai kịch bản; không phải tiêu chí chứng minh tính thực tế.

Tổng số đơn mới tăng 19.292 dòng, khoảng 18,18%, nhưng mục tiêu của bản sửa là
cấu trúc biến động và sự khác nhau giữa SKU, không phải tăng số lượng đơn.
Lượt trước chặn lúc **23:10:13.710874**, lượt sau lúc **23:31:20.659634** ngày
31/10/2026. Các tháng dùng cùng khoảng ngày nhưng tháng 10 có mốc cuối ngày khác
nhau; bộ sinh và chính sách kho cũng đổi, nên đây không phải thí nghiệm nhân quả
chỉ thay một tham số. Số trung bình/ngày có tính cả ngày 31/10 chưa trọn ngày.

### 3.2. Mặt hàng có đường nhu cầu khác nhau

| Tháng | Quạt mini: trước | Sau | Kem chống nắng: trước | Sau | Balo: trước | Sau |
|---|---:|---:|---:|---:|---:|---:|
| 2026-01 | 191 | 38 | 860 | 463 | 187 | 103 |
| 2026-02 | 173 | 44 | 781 | 548 | 181 | 61 |
| 2026-03 | 177 | 112 | 817 | 882 | 192 | 104 |
| 2026-04 | 188 | 307 | 830 | 1.214 | 155 | 97 |
| 2026-05 | 199 | 546 | 846 | 2.079 | 208 | 193 |
| 2026-06 | 178 | 427 | 825 | 1.852 | 169 | 209 |
| 2026-07 | 161 | 281 | 767 | 2.308 | 168 | 226 |
| 2026-08 | 181 | 283 | 881 | 2.093 | 209 | 581 |
| 2026-09 | 185 | 287 | 852 | 1.802 | 153 | 417 |
| 2026-10 | 203 | 78 | 876 | 1.477 | 173 | 192 |

Quạt mini tăng vào mùa nóng rồi giảm; balo tăng mạnh vào tháng 8–9; kem chống
nắng có mùa cao điểm riêng. Các con số trên là yêu cầu đặt hàng gồm cả hủy,
không phải doanh số hoàn tất. Đây là kết quả của các quy luật đã cấu hình và
nhiễu ngẫu nhiên, không phải phép đo nhu cầu thị trường.

Tỷ lệ hủy toàn bộ bộ trước là **28,46%**, bộ sau **14,79%**.
Tỷ lệ hủy theo tháng của bộ mới nằm trong khoảng **13,47–15,93%**.
Không quy toàn bộ thay đổi tỷ lệ hủy cho riêng chính sách nhập kho: cơ cấu SKU,
lịch nhu cầu, sinh vòng đời và chuỗi ngẫu nhiên cũng thay đổi.

## 4. Kiểm thử, thay thế và bảo toàn dữ liệu

### 4.1. Kiểm thử chức năng

```powershell
python -m unittest tests.test_historical_demand tests.test_data_simulator -v
```

Lượt kiểm chứng đính kèm đạt **24/24 kiểm thử**, gồm 17 kiểm thử hiện có và 7 kiểm
thử lịch sử. Thời gian và kết quả từng test nằm trong [nhật ký JSON](evidence/simulator-tests-2026-10-31.json).
Các kiểm thử mới kiểm tra seed, mùa vụ SKU, độ biến động, ngày không có đơn,
thứ tự/mốc thời gian, đầu vào và khả năng nhập kho theo quy mô nhu cầu.

Đã kiểm tra cú pháp và thử rollback với loader thật: đơn kiểm tra xuất hiện
trong giao dịch nhưng không còn sau khi chủ động hoàn tác; metadata sản phẩm
trở về trạng thái ban đầu. Đây là phép thử của đợt triển khai, không phải một
test tích hợp đã bổ sung vào suite công khai.

### 4.2. Thay đúng dữ liệu của đợt trước

1. Chuẩn bị các tệp raw/clean/tồn kho mới và kiểm tra số dòng, ngày và mốc thời gian.
2. Sao lưu database trước thay thế dạng PostgreSQL custom dump, dung lượng
   **7.506.033 byte**. Bản dump và payload chi tiết giữ cục bộ, không đưa lên GitHub.
3. Xác định đơn cũ bằng tiền tố `SIM26YTD-`, xác nhận 106.131 dòng. Xác định 6.060
   bản ghi tồn kho theo ID 52–6111 **kết hợp** khoảng `loaded_at` của lượt chạy cũ,
   không xóa rộng theo ngày nghiệp vụ.
4. Khóa các bảng cần thiết, xóa đúng tập cũ, nạp bộ mới và kiểm tra bảo toàn trong
   cùng một giao dịch. Các loader dùng chung kết nối; sai kiểm tra thì rollback.
5. Giữ 38 khóa tồn kho cũ để bộ mới không ghi đè; phục hồi metadata sản phẩm/shop
   có trước giao dịch do các bước upsert có thể cập nhật các trường đó.
6. Chỉ xác nhận thành công sau commit và kiểm tra lại bằng kết nối độc lập.

| Tập dữ liệu được bảo vệ | Số dòng trước/sau | Đối chiếu nội dung |
|---|---:|---|
| `fact_orders` ngoài tiền tố mô phỏng | 19.428 / 19.428 | Khớp |
| Tồn kho cũ, ID ≤ 51 trong snapshot này | 38 / 38 | Khớp |
| `dim_products` | 126 / 126 | Khớp |
| `dim_shops` | 22.047 / 22.047 | Khớp |

Đối chiếu dùng số dòng và MD5 của chuỗi JSONB toàn bộ dòng, sắp theo khóa chính.
Hash baseline từ lúc thay thế và hash kiểm chứng sau đó trùng nhau; giá trị cụ
thể có trong [bằng chứng JSON](evidence/simulator-comparison-2026-10-31.json).
Điều kiện ID/khoảng thời gian nêu trên là dấu vết riêng của snapshot này, không
phải quy tắc để xóa dữ liệu ở môi trường khác.

### 4.3. Kiểm chứng PostgreSQL sau commit

| Phép kiểm tra | Kết quả |
|---|---:|
| Tổng đơn / mô phỏng / dữ liệu được bảo vệ | 144.851 / 125.423 / 19.428 |
| Mã đơn mô phỏng khác nhau / SKU | 125.423 / 20 |
| Ngày đầu / ngày cuối | 01/01/2026 / 31/10/2026 |
| Ngày có dữ liệu / ngày thiếu | 304 / 0 |
| Tổng tồn kho / tồn kho cũ được bảo vệ | 6.098 / 38 |
| Chuyển trạng thái sau mốc chặn | 0 |
| Đơn mới `COMPLETED` thiếu `completed_time` | 0 |

Các số liệu được đọc lại trong giao dịch `READ ONLY` khi biên soạn báo cáo.
Thời điểm đọc, các kết quả tổng hợp, hash baseline và SHA-256 của bản sao lưu
được lưu trong [bằng chứng JSON](evidence/simulator-comparison-2026-10-31.json).
[Tệp SQL kiểm chứng](evidence/simulator-verification-2026-10-31.sql) không thay dữ liệu.

## 5. Tái lập, giới hạn và công việc tiếp theo

Lệnh sau tạo dữ liệu bằng bộ sinh mới; thêm `--load-db` nếu cần nạp DB:

```powershell
python scripts/simulate_historical_data.py --start-date 2026-01-01 --end-date 2026-10-31 --orders-per-day 300 --seed 20261031 --order-prefix SIM26YTD-
```

Lệnh không tự thay thế một đợt cũ hoặc tự bảo vệ dữ liệu trùng tiền tố. Không
chạy lại trên kho hiện tại với cùng tiền tố nếu chưa lập phạm vi thay thế và
giao dịch bảo toàn. Cơ chế thay thế đợt thực nghiệm dùng trình điều phối cục bộ,
không được đóng gói thành công cụ migration công khai trong commit triển khai.

Kết quả chính xác trong báo cáo dùng cutoff có giờ, seed và tệp kết quả đã lưu.
Lệnh CLI chỉ truyền ngày kết thúc sẽ lấy cuối ngày hoặc hiện tại, nên không cam
kết tái tạo đúng 125.423 dòng. UUID ingestion cũng không tái lập từng byte.
Nguồn tổng hợp cục bộ là `data/demo/ytd-20261031/summary.json` và
`data/demo/ytd-20261031-v2/summary.json`; GitHub kèm bản tổng hợp không chứa payload.

Các giới hạn còn lại:

- Các profile, nhiễu và tỷ trọng kênh là giả định, chưa fit vào doanh số thật;
  đỉnh nhu cầu xuất hiện như thiết kế không đủ chứng minh mô hình thị trường đúng.
- Các kiểm tra mốc thời gian báo cáo tập trung vào các cột vòng đời canonical
  trong `fact_orders`, không phải kiểm chứng mọi trường SLA/phí trong payload gốc.
- Chưa chạy lại full regression, benchmark toàn tuyến, retraining hoặc so sánh
  WAPE/MAE. Chưa có kết luận rằng chất lượng dự báo đã tăng.
- Khi dùng dữ liệu này cho ML, cần phân biệt yêu cầu đặt hàng với lượng bán
  không hủy, chia dữ liệu theo thời gian và đánh giá thêm trên dữ liệu thật.

AI hỗ trợ sửa mã, kiểm thử, điều phối thay dữ liệu, thu thập bằng chứng và soạn
báo cáo. Các bảng kết quả được đối chiếu từ tệp đã lưu và PostgreSQL; không thay
số liệu lịch sử để tạo kết quả so sánh.
