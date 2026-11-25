# Phần 6 — Power BI và tổng hợp thực nghiệm

> **Cập nhật 17/11/2026 (ghi đè các mô tả cũ bên dưới về nguồn và trang báo cáo):** báo cáo quản lý hiện hành là PBIP `powerbi/BaoCaoQuanLy/` dựng trên data mart star schema `mart`, Import từ PostgreSQL. Số trên báo cáo đã được đối chiếu với SQL độc lập sau lần Refresh ngày 17/11/2026 (mục 9). Hai tệp PBIX cũ giữ nguyên làm hiện vật lịch sử.

> **Nguồn chính từ 01/11/2026:** Power BI đọc PostgreSQL bằng Import. CSV là snapshot dự phòng chạy riêng; checkpoint worker chỉ phụ thuộc ETL. Bộ query và cách chuyển PBIX tại [powerbi/import](../../powerbi/import/README.md). Các số liệu audit 19/10 bên dưới giữ nguyên phạm vi lịch sử; chưa xác nhận PBIX đã Apply/Refresh nguồn mới trong lượt này.

Ngày kiểm tra: **19/10/2026**. Phạm vi: SQL, exporter, DAX tài liệu, cấu trúc PBIX, xuất CSV kiểm chứng bằng truy vấn chỉ đọc và tổng hợp bằng chứng kiểm thử. Không thay view, facts, coverage, PBIX hoặc bộ CSV chính trong `powerbi/data/`.

## 1. Vai trò và hiện vật Power BI

Lớp BI phục vụ ba nhóm quyết định theo thiết kế: theo dõi bán hàng đa kênh, so sánh forecast với actual đã hoàn tất và ưu tiên rủi ro tồn kho. Repository cung cấp SQL view, exporter, DAX, theme và PBIX.

Exporter chuẩn có bước `CREATE OR REPLACE VIEW` trước khi truy vấn. Để giữ nguyên runtime trong lượt kiểm tra, script audit thay riêng bước cài view bằng `SET TRANSACTION READ ONLY`, dùng các view đang có và thực hiện nguyên các SELECT của exporter. Bản xuất được ghi vào thư mục kiểm chứng riêng, không thay `powerbi/data/`.

| Dataset | Độ hạt / ý nghĩa | Số dòng xuất |
|---|---|---:|
| Dim_Dates | Ngày lịch 2025–2027 | 1.095 |
| Dim_Products | Sản phẩm hiện có trong dimension | 76 |
| Dim_Geography | Địa lý có dòng bán phù hợp | 20 |
| Fact_Orders_Summary | Ngày × platform, phạm vi join shop/ngày | 14 |
| Forecast_vs_Actual | Forecast được chọn theo SKU/ngày/nguồn | 6 |
| Inventory_Health_Alerts | Snapshot tồn mới nhất có tham số demand tương ứng | 24 |
| Historical_Demand_Daily | Ngày × SKU × platform × nguồn/dataset lịch sử | 2.211 |

Nội dung cả bảy CSV kiểm chứng giống bộ CSV đang lưu. Đây là bằng chứng tái lập đầu ra hiện tại, không chứng minh các định nghĩa KPI đã đúng hoặc PBIX đã refresh thành công.

## 2. Đối soát chỉ tiêu đơn hàng và tiền

`Fact_Orders` có độ hạt dòng hàng. Exporter dùng `COUNT(order_fact_id)` cho `total_orders` và `AVG(buyer_total_amount)` cho AOV trong bảng summary, trong khi view executive dùng distinct `(platform, order_id)` và tổng tiền chia số đơn. Hai đường dữ liệu không thống nhất.

| Phạm vi | Dòng hàng không hủy | Đơn duy nhất | Quantity | Tổng buyer_total_amount |
|---|---:|---:|---:|---:|
| Toàn bộ Fact_Orders không hủy | 11.660 | 11.646 | 24.392 | 7.609.968.840 |
| Sau join với Dim_Dates và Dim_Shops | 11.628 | 11.614 | 24.337 | 7.597.764.230 |
| Tổng được ghi vào CSV summary | 11.628, nhưng đặt tên orders | — | 24.337 | 7.597.764.230 |

Trong cùng phạm vi join, CSV đếm cao hơn số đơn **14**, với 4 nhóm ngày/platform có chênh lệch so với view executive. Vì mẫu số là dòng, AOV hiện là trung bình tiền mỗi dòng hàng, chưa phải trung bình tiền mỗi đơn.

Join với shop loại 32 dòng không hủy, tương ứng 55 units và 12.204.610 đơn vị tiền theo trường nguồn. Phần 3 đã xác nhận tất cả 32 dòng thiếu shop có tiền tố fixture E2E/DI. Cần quy định việc đưa fixture vào báo cáo và cách xử lý shop chưa lookup được; không để join ngầm quyết định phạm vi doanh thu.

Hướng chỉnh lý cần nghiệm thu sau kiểm tra: dùng distinct business order key; tính AOV từ tổng tiền/số đơn trên cùng phạm vi; cân nhắc lấy platform từ fact và quy tắc dimension chưa biết để không bỏ dòng âm thầm.

Các số tiền ở trên là tổng trường `buyer_total_amount` trong dữ liệu mô phỏng/kiểm thử, không phải doanh thu doanh nghiệp đã xác minh. Điều kiện không hủy vẫn có thể gồm trạng thái chưa hoàn thành/thanh toán; tên “doanh thu thực nhận” cần quy tắc nghiệp vụ riêng trước khi sử dụng.

Exporter địa lý cũng dùng COUNT dòng dưới tên `total_orders`; view geographic lại đếm distinct. Đây là chênh lệch công thức từ mã, chưa đo riêng mức chênh lệch cho từng địa bàn trong lượt này.

## 3. Actual–Forecast và điều kiện chấm điểm

View đang chạy có cả điều kiện thời điểm phát hành và coverage. Các forecast chỉ đủ điều kiện đi vào tập view khi nguồn model/lịch sử phù hợp và target date sau ngày phát hành theo giờ Việt Nam. Điều kiện này loại forecast hồi cứu và cùng ngày theo quy ước hiện tại của view.

Với mỗi SKU/ngày/nguồn/fingerprint, view chọn forecast phát hành gần nhất trong các bản đủ điều kiện. Phải chốt chính sách chọn lần phát hành/horizon khi so sánh giữa model, vì một view chọn một forecast mới nhất chưa thể thay thế bảng đánh giá mọi version hoặc từng horizon.

Actual chỉ được điền khi ngày mục tiêu đã kết thúc và coverage được xác nhận. Với lịch sử CSV, mọi fingerprint của forecast phải có coverage ngày tương ứng. Demand được ghép đúng origin/dataset; nếu chưa complete, actual và error để NULL, không tự coi thiếu nguồn là actual=0.

| Chỉ tiêu đã đọc trên runtime | Kết quả |
|---|---:|
| Forecast lưu trong fact | 6 |
| Forecast thỏa điều kiện ngày phát hành | 6 |
| Forecast được xuất | 6 |
| Actual complete | 0 |
| Dòng có actual để tính sai số | 0 |
| Coverage đã xác nhận | 0 |

Do vậy **chưa có WAPE/accuracy vận hành hợp lệ trên dữ liệu forecast đã phát hành**. WAPE walk-forward 126,08% của phần ML là phép thực nghiệm khác, không phải giá trị đo được từ sáu dòng này.

View trả lower/upper bound bằng NULL, không xuất khoảng tin cậy giả. DAX WAPE/Bias trong tài liệu lọc dòng có actual và DIVIDE trả blank khi mẫu số không hợp lệ; đây là lựa chọn thận trọng hơn tiện ích metric Python hiện trả 0 khi tổng actual bằng 0.

Measure `Forecast Accuracy = max(0, 1-WAPE)` là chỉ số suy ra có chặn dưới, không phải accuracy phân loại hoặc xác suất đúng. Khi có dữ liệu để báo cáo, ưu tiên hiển thị WAPE/Bias và định nghĩa rõ thay vì chỉ một gauge accuracy.

## 4. Tồn kho, nguồn dữ liệu và mô hình quan hệ

View inventory lấy snapshot thật mới nhất rồi join với tham số demand từ origin operational `unverified_warehouse`. Nó tính theo lịch sử đó; không gọi API hoặc model Registry để dự báo nhu cầu lead time. Vì vậy không mô tả bảng BI inventory như bản sao chính xác của kết quả API đang dùng origin CSV lịch sử/fallback.

View và API còn khác các chi tiết: SQL chưa áp sàn SS=1/ROP≥SS+1 như API; lượng nhập SQL tính từ mức đích ngay cả khi NORMAL, còn API chỉ đề xuất nếu cần nhập. Trong 24 dòng xuất hôm nay không có dòng NORMAL có reorder quantity dương; chênh lệch công thức vẫn cần thống nhất khi đổi dữ liệu.

Snapshot trong view có `last_audited_date`, nhưng exporter bỏ cột này và không xuất nhãn demand source/stock source. Có date_key để suy ra ngày snapshot qua dimension, song giao diện cần hiển thị độ mới rõ ràng thay vì chỉ số tồn.

`Dim_Products.csv` có 76 SKU, trong khi lịch sử sau loại hủy có 216 SKU; **214 SKU lịch sử chưa có trong dimension sản phẩm xuất ra**. `Historical_Demand_Daily.csv` không có product_key. Cần một dimension SKU lịch sử phù hợp hoặc bổ sung mapping trước khi tuyên bố toàn bộ dataset dùng chung một star schema đầy đủ.

Summary không có product_key và tổng hợp ở ngày/platform. Vì vậy riêng bộ CSV này chưa cung cấp grain doanh thu theo SKU/ngày để làm đầy đủ biểu đồ top sản phẩm doanh thu như thiết kế. Tải thêm fact/view phù hợp hoặc điều chỉnh biểu đồ là hai hướng cần chốt.

## 5. DAX tài liệu và PBIX thực tế

Tài liệu DAX dùng các bảng/cột của phương thức import raw facts, nhưng bộ CSV là marts khác grain:

- Các measure SUM trên `Fact_Orders` không thể dán trực tiếp vào mô hình chỉ import CSV summary, nếu không có bảng raw tương ứng.
- `Fact_Inventory_Daily[closing_stock]` không khớp DDL `stock_on_hand` hoặc CSV `current_stock`.
- Measure revenue/units trong tài liệu chưa tự lọc hủy, trong khi Total Orders có lọc; cần thống nhất mẫu số/phạm vi nếu import raw facts.
- Avg Daily Demand dùng số ngày của Dim_Dates theo filter context; không nên vô tình lấy cả 1.095 ngày calendar cho dữ liệu bán có khoảng quan sát ngắn hơn.

Chưa có bằng chứng các công thức trong tài liệu đều là measure thực đang được PBIX sử dụng. Đã đọc cấu trúc ZIP của `Report_PowerBI.pbix`:

| Hạng mục | Hiện vật xác nhận |
|---|---|
| Trang báo cáo | 1 trang: Quản trị Tồn kho và cảnh báo |
| Thành phần | 1 shape, 1 textbox, 1 cardVisual |
| Visual có query binding | 1 card |
| Measure card tham chiếu | Critical SKU Count, Normal SKU Count, Total Current Stock, Total Reorder Qty, Warning SKU Count |

Đây là dashboard đang phát triển, chưa khớp mô tả ba trang hoàn chỉnh trong README. Lượt này không mở Power BI Desktop, không render trang, không chạy DAX engine hoặc xác nhận refresh/relationships. Không dùng phân tích JSON layout để thay thế nghiệm thu hiển thị và tính toán thực.

## 6. Kiểm thử hiện hành và số đo hiệu năng lịch sử

### 6.1. Kết quả hiện hành

Nhóm export/load test cùng các regression liên quan: **27 passed trong 1,76 giây**. Các test export dùng fixture/mocks, nên phép xuất dữ liệu thật ở mục 1 là bằng chứng bổ sung cần thiết.

Sau khi hoàn thành sáu phần, đã chạy toàn bộ suite với Python runtime 3.12.14, dependencies `.venv`, OMP/OPENBLAS threads=1, seed Python random **20261019**, thư mục tạm workspace và quyền loopback cho TestClient:

**106 passed, 30 warnings trong 7,31 giây.** Warning gồm Starlette/httpx deprecation và tương thích PyArrow/pandas. JUnit XML được lưu cùng hồ sơ.

Đây là một lượt chạy với seed xác định, không phủ định trường hợp simulator buyer amount=0 đã tái hiện bằng seed 128 ở phần 1, hoặc các lỗi dữ liệu biên được audit phát hiện ngoài suite. Không cộng số test từng phần vì có test trùng nhau.

### 6.2. Giới hạn số đo hiệu năng

| Hiện vật / mốc | Số đo | Phạm vi được phép kết luận |
|---|---|---|
| Báo cáo runtime sau sửa 09/10 | 1.200 HTTP requests; 200 users P95 396,25 ms, 479,3 RPS, 0% lỗi | Được ghi trong báo cáo lịch sử; serving fallback; raw file tải đã bị lượt khác ghi đè |
| `data/load_test_results.json`, 12/10 | In-process, 5 users, 25 requests; P95 15,98 ms, 367,4 RPS | Có raw JSON hiện hành của lượt đó; không đo mạng HTTP hoặc tải 200 users |
| `data/soak_test_report.json`, 09/10 | 3.600,96 giây; produced/consumed 5.498/5.498; 182 files; 0% loss theo bộ đếm; 1 consumer error | Test ingestion một giờ theo hồ sơ; không phải soak toàn bộ ETL/train/serve; chưa tái chạy hôm nay |
| Kiểm tra 19/10 | Unit/regression và đối soát dữ liệu; API fallback, readiness 503 | Chưa đo lại load/soak hoặc latency của model live |

Không trộn P95 in-process với P95 HTTP, concurrency users với RPS, hoặc kết quả fallback với model ML. Các tuyên bố “Production-Grade Verified”, zero-loss tuyệt đối, ưu việt hơn direct-to-DB hoặc tiết kiệm chi phí cần bằng chứng độc lập; các số đo cục bộ hiện có chưa đủ cho những kết luận đó.

## 7. Đánh giá nghiệm thu tổng hợp

Đã xác nhận có luồng kỹ thuật tích hợp và nhiều hiện vật thực nghiệm, kiểm tra ETL ngày 14/10 khớp và metric baseline tái lập được. Tuy nhiên, tại ngày 19/10, model live chưa nạp; BI còn sai khác định nghĩa KPI và PBIX chưa hoàn thiện ba trang. Vì vậy cần phân biệt **đã chạy được end-to-end ở các lượt trước** với **trạng thái sẵn sàng và chất lượng hiện tại**.

Các ưu tiên trước khi chốt kết luận báo cáo: phục hồi model và kiểm chứng nguồn dự báo thật; sửa số đơn/AOV; chốt coverage và baseline; hoàn thiện PBIX/measure/relationships; nghiệm thu cảnh báo, retry và kịch bản phục hồi. Danh sách chung nằm trong tài liệu tổng hợp của bộ báo cáo.

## 8. Hiện vật và minh bạch sử dụng AI

- `scripts/check_powerbi_readonly.py`: tái lập SELECT exporter với view hiện có trong transaction chỉ đọc.
- `evidence/powerbi_2026-10-19.json`: row counts, đối soát KPI, coverage và cấu trúc PBIX.
- `evidence/powerbi_export_2026-10-19/`: bảy CSV snapshot kiểm chứng.
- `evidence/pytest_full_2026-10-19.xml`: kết quả toàn suite.

AI hỗ trợ đọc mã/hiện vật, chạy test, đối soát chỉ đọc, xuất snapshot riêng và biên soạn báo cáo. Chưa thay PBIX, SQL/DAX sản phẩm hoặc tự xác nhận coverage. Sinh viên xác nhận đóng góp tự thực hiện và hoàn thiện thông tin học thuật theo mẫu trường.

## 9. Báo cáo quản lý PBIP trên data mart (16–17/11/2026)

### 9.1. Mục tiêu và phạm vi

Báo cáo phục vụ người quản lý kho và bán hàng. Người quản lý dùng số thực tế từ pipeline kết hợp dự báo AI để tự ra quyết định đặt hàng cuối cùng; báo cáo chỉ đề xuất. Phạm vi do sinh viên chốt ngày 16/11/2026:

- **4 trang chính:** Sức khỏe tồn kho; Dự báo nhu cầu AI; Đặt hàng nhà cung cấp; Đơn hàng từ các sàn.
- **2 trang phụ:** Chi tiết SKU; Độ tin cậy dữ liệu.
- **Mức phục vụ theo nhóm ABC:** A 98%, B 95%, C 90%.
- **Ngoài phạm vi:** hàng đang về (`incoming_stock`, `restock_eta`) được thu nhưng không đưa vào công thức (hướng mở rộng).

### 9.2. Luồng dữ liệu và mô hình

```
Kafka → MinIO raw → ETL → PostgreSQL public → mart (star schema vật lý) → Power BI (Import) → PBIP BaoCaoQuanLy
```

- **Nguồn duy nhất của báo cáo là schema `mart`.** Gồm 9 bảng chiều và 5 bảng fact:
  - `fact_sales_line`, `fact_sales_order`;
  - `fact_inventory_daily`;
  - `fact_demand_forecast`: giữ mọi phiên bản dự báo, kèm số thực tế khi ngày đã xác nhận;
  - `fact_replenishment_daily`: lịch sử đề xuất theo ngày, không bao giờ bị xóa.
- **Khóa ngày** có dạng `yyyymmdd`. Dòng `-1` là "không xác định"; dòng `-2` là "không áp dụng" (đơn chưa giao vận chuyển, đề xuất không dùng dự báo AI).
- **Làm mới:** `pipeline-worker` làm mới mart gia tăng sau mỗi chu kỳ (khoảng 2 giây); `batch-forecaster` làm mới sau mỗi lượt dự báo. Người dùng chỉ cần nhấn Refresh trong Power BI Desktop.
- **Mô hình ngữ nghĩa** sinh bằng `scripts/build_powerbi_project.py`: 15 bảng, 23 quan hệ, 63 measure, 6 trang, 55 visual. 66/66 tệp PBIP đạt JSON schema của Microsoft.

### 9.3. Các quyết định định nghĩa chỉ số

| Chỉ số | Định nghĩa | Lý do |
|---|---|---|
| Đơn hợp lệ, Doanh thu | Đơn không hủy; `net_amount` cấp đơn | Tách hẳn số đơn khỏi số dòng hàng (lỗi đếm đơn/AOV từng gặp ngày 19/10) |
| Tăng trưởng DT 7 ngày | 7 ngày trọn vẹn đến hôm qua so với 7 ngày trước đó | Bản dùng `DATEADD` trên toàn khoảng luôn ra xấp xỉ 0 |
| Hết hàng trong 7 ngày | Số SKU có `days_of_cover` khác trống và ≤ 7 | Lọc thẳng trên cột tính cả giá trị trống (65 thay vì 19) |
| Tổng tồn kho theo ngày | Mỗi SKU lấy lần chụp tồn kho gần nhất tính đến ngày đó rồi cộng | Không bị tụt về gần 0 vào ngày ít bản tin |
| Dự báo trên biểu đồ | Chỉ model huấn luyện trên `warehouse_live`, bản mới nhất cho mỗi SKU × ngày | Không trộn dự báo của model huấn luyện trên CSV công khai |
| WAPE thực tế (t+1) | Model đang phục vụ, tầm dự báo 1 ngày, chỉ trên ngày đã xác nhận | Cùng giao thức với WAPE backtest (`origin_t_plus_1_pooled_wape_v2`) |
| WAPE chung | Chỉ trên cùng SKU × ngày × tầm dự báo mà mọi phiên bản được chọn đều có | So sánh công bằng giữa các phiên bản |
| Trang Chi tiết SKU | Chỉ hiện số khi chọn đúng 1 SKU; dòng tổng của bảng lịch sử để trống | Không cộng gộp tồn kho của nhiều ngày hoặc nhiều SKU |

### 9.4. Đối chiếu số trên báo cáo với SQL độc lập (17/11/2026)

Ảnh báo cáo được chụp sau lần Refresh có "Đơn mới nhất" là 17/11/2026 00:50. Các con số được tính lại bằng `scripts/check_powerbi_kpi_values.py --cutoff "2026-11-17 00:50:59"`. Script dùng SQL viết riêng trên `mart`, không dùng lại DAX. Producer và mart vẫn chạy sau lúc Refresh, nên các chỉ số đặt hàng của ngày hiện tại được tính lại vài phút sau và có thể lệch nhẹ.

| Trang | Chỉ số | Trên báo cáo | SQL | Kết luận |
|---|---|---:|---:|---|
| Đơn hàng | Đơn hợp lệ | 131K | 130.693 | Khớp |
| Đơn hàng | Doanh thu | 80bn ₫ | 79.562.008.660 ₫ | Khớp |
| Đơn hàng | Giá trị đơn TB | 609K ₫ | 608.770 ₫ | Khớp |
| Đơn hàng | Sản phẩm bán | 314K | 314.154 | Khớp |
| Đơn hàng | Tỷ lệ hủy | 17,6% | 17,63% | Khớp |
| Đơn hàng | Tăng trưởng DT 7 ngày (10–16/11 so với 03–09/11) | −5,1% | −5,08% | Khớp |
| Đơn hàng | Tỷ trọng Shopee / TikTok Shop | 53,34% / 46,66% | 53,34% / 46,66% | Khớp |
| Tồn kho | SKU cần đặt / ổn định / hết hàng trong 7 ngày | 20 / 62 / 24 | 20 / 62 / 24 | Khớp |
| Tồn kho | Ma trận ABC × XYZ (A-Y, A-Z, B-Y, B-Z, C-Z) | 3, 8, 1, 4, 4 | 3, 8, 1, 4, 4 | Khớp |
| Tồn kho | SKU nguy cấp; số ngày tồn trung vị | 14; 5,0 | 15; 4,85 | Lệch do tồn kho giảm tiếp sau lúc Refresh |
| Đặt hàng | SKU đặt ngay / trong tuần | 13 / 7 | 13 / 7 | Khớp |
| Đặt hàng | Tổng lượng đề xuất | 9K | 9.012 | Khớp |
| Đặt hàng | Giá trị theo giá bán | 3bn ₫ | 2.809.924.000 ₫ | Khớp; thẻ làm tròn đến tỷ |
| Đặt hàng | Lịch sử tổng đề xuất 16/11; 17/11 | khoảng 26K; khoảng 9K | 25.982; 9.012 | Khớp |
| Dự báo | WAPE / bias backtest v4 | 58,2% / −23,1% | 58,22% / −23,07% | Khớp |
| Dự báo | WAPE thực tế (t+1) / bias; số ngày chấm | 221,5% / 218,8%; 1 | 221,52% / 218,83%; 1 (129 dự báo) | Khớp |
| Độ tin cậy | Ngày xác nhận gần nhất; SKU dùng lead time mặc định; đơn xung đột header | 16/11; 68; 16 | 16/11; 68; 16 | Khớp |

**Kiểm tra công thức trên một SKU (BL-15-CS, nhóm B, mức phục vụ 95%):**
- Điểm đặt hàng = nhu cầu 3 ngày lead time theo dự báo AI (27) + tồn an toàn (126) = 153.
- Mức tồn đích = 153 + 7 ngày × 9/ngày = 216. Đề xuất = 216 − 113 = 103.
- Số ngày tồn = 113 / 9 = 12,6.
- Tồn 113 < tồn an toàn 126, nên trạng thái là **nguy cấp**.
- Báo cáo hiển thị đúng các giá trị 113, 12,6, 103 và lý do "Tồn hiện có 113 ≤ điểm đặt hàng 153 = nhu cầu 3 ngày 27 (dự báo AI) + tồn an toàn 126".

Sau khi đối chiếu, có hai chỉnh sửa trong bộ sinh PBIP:
- **Bảng lịch sử đề xuất của trang Chi tiết SKU** từng có dòng tổng cộng tồn kho của các ngày (113 + 130 = 243). Dòng tổng nay để trống.
- **Thẻ "WAPE thực tế"** chuyển sang tính riêng model đang phục vụ ở tầm t+1. Hiện chỉ có một phiên bản và một tầm nên số không đổi. Khi có nhiều phiên bản hoặc nhiều tầm dự báo được chấm, bản trước sẽ trộn chúng lại.

### 9.5. Giải thích các con số dễ bị hiểu nhầm

- **WAPE thực tế 221,5% với 1 ngày được chấm.** Model Naive dự báo ngày 16/11 bằng nhu cầu ngày 15/11 (3.071 sản phẩm), trong khi ngày 16/11 thực tế chỉ bán 1.103 sản phẩm. Producer mô phỏng chỉ chạy một phần ngày 16/11. Đây là giới hạn đã công bố của dữ liệu mô phỏng và model baseline, không phải lỗi pipeline. Con số chỉ có ý nghĩa khi tích lũy đủ ngày.
- **Tổng đề xuất giảm từ 25.982 (16/11) xuống khoảng 9K (17/11).** Cùng nguyên nhân: nhu cầu trong lead time do Naive dự báo đổi theo khối lượng ngày gần nhất.
- **Tổng tồn kho đi ngang 01–14/11 và có đỉnh ngày 15/11.**
  - Trong khoảng 01–14/11 không có bản tin tồn kho, vì đợt bổ sung dữ liệu chỉ có đơn hàng, nên đường đi theo số ngày 31/10.
  - Ngày 15/11 có thêm 15 SKU kiểm thử `RPT…` (khoảng 4,2K hàng).
  - Từ 16/11 producer gửi lại số tồn thật.
  - Theo quyết định ngày 16/11/2026, dữ liệu kiểm thử không bị gắn cờ hay lọc.
- **16 đơn xung đột header** là các mã `DI2026101…`/`DI2026102…` do bài kiểm tra toàn vẹn ghi trực tiếp. 159 mã đơn bị simulator dùng lại (358 đơn thật) đã được khôi phục từ kho thô ngày 17/11/2026 (`docs/04-pipeline-updates/lan-15-…`).

### 9.6. Quy trình demo và kiểm chứng

1. Giữ toàn bộ service và producer chạy. Mart tự làm mới.
2. `python scripts/check_powerbi_refresh_readiness.py --label <nhãn>`: mô phỏng Refresh chỉ đọc, gồm 16 kiểm tra (đủ nguồn, không NaN, khóa duy nhất, không khóa mồ côi, mart khớp `public`…). Kết quả ngày 17/11/2026: **PASS 16/16**.
3. Mở `powerbi/BaoCaoQuanLy/BaoCaoQuanLy.pbip` và nhấn Refresh.
4. `python scripts/check_powerbi_kpi_values.py --cutoff "<giờ trên thẻ Đơn mới nhất>"`: đọc số SQL để đối chiếu trực tiếp với báo cáo.

### 9.7. Hiện vật

- PBIP: `powerbi/BaoCaoQuanLy/` (sinh lại bằng `scripts/build_powerbi_project.py`). Đặc tả: `powerbi/dac-ta-bao-cao-quan-ly.md`.
- Data mart: `warehouse/ddl/05_mart_star_schema.sql`, `warehouse/reporting/mart_refresh.sql`. Test: `tests/test_mart.py` (11 test trên PostgreSQL).
- Bằng chứng:
  - đối chiếu số liệu: `evidence/powerbi-kpi-20261117-005656-doi-chieu-anh-refresh-0050.json`;
  - mô phỏng Refresh: `output/powerbi-refresh-check-2026-11-17/004715-PBIP-FIX-1711.json`.
- Kiểm thử toàn bộ: 261 test đạt; test PostgreSQL và mart đạt.

### 9.8. Giới hạn

- Dữ liệu là mô phỏng, kể cả các lô kiểm thử có nhãn. Ngày 01–15/11/2026 là dữ liệu mô phỏng bổ sung.
- Chưa tính hàng đang về. Lead time thiếu thì dùng 3 ngày (68 SKU, chủ yếu là SKU kiểm thử).
- Giá trị đề xuất tính theo giá bán vì chưa có giá vốn.
- Lịch sử đề xuất bắt đầu từ 16/11/2026. WAPE thực tế mới có 1 ngày được chấm.
- Giao diện (bố cục, định dạng hiển thị) sẽ được sinh viên chỉnh tiếp. Các số trong mục 9.4 không phụ thuộc vào giao diện.

### 9.9. Minh bạch sử dụng AI

- **AI hỗ trợ:**
  - thiết kế và cài đặt data mart;
  - viết bộ sinh PBIP, các measure DAX và script đối chiếu;
  - sửa các lỗi phát hiện qua ảnh chụp;
  - chạy kiểm thử và biên soạn mục này.
- **Sinh viên:**
  - xác định mục tiêu nghiệp vụ, chọn cấu trúc trang, mức phục vụ ABC và star schema vật lý;
  - Refresh báo cáo trên Power BI Desktop, chụp ảnh và phát hiện bất thường;
  - duyệt các phương án sửa;
  - sẽ chỉnh giao diện cuối cùng.
