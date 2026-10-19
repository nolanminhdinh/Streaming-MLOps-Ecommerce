# Phần 6 — Power BI và tổng hợp thực nghiệm

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
