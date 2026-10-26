# Rà soát tài liệu dự án dưới góc nhìn PM

Ngày rà soát: **26/10/2026**, UTC+7. Phiên bản mã tham chiếu: `9d224d1` ngày 24/10/2026; kiểm tra trên nội dung workspace hiện có.

## 1. Kết luận

**Tài liệu có độ bao phủ kỹ thuật tốt, nhưng chưa thể xác nhận đã viết đúng, nhất quán và đầy đủ để bàn giao chính thức.** Vấn đề chính là nhiều thế hệ tài liệu cùng tồn tại: thiết kế mục tiêu, kết quả lịch sử, báo cáo sửa lỗi và trạng thái hiện tại chưa được quản lý thành một bộ thống nhất.

- Đã có: bài toán, phạm vi, người dùng, BR-01–BR-08, tiêu chí đề xuất, kiến trúc, ETL/warehouse, ML/MLOps, API/tồn kho, monitoring, BI, hướng dẫn demo, tiến độ và nhiều bằng chứng kiểm thử.
- Cần sửa: mô hình và số liệu cũ, schema/khóa dữ liệu, định nghĩa doanh thu, trạng thái hoàn thiện Power BI, một số hướng dẫn khởi động và các tuyên bố vượt phạm vi bằng chứng.
- Cần bổ sung: ma trận nghiệm thu theo yêu cầu, người phụ trách và ngày hoàn tất thực tế, hồ sơ nguồn dữ liệu, danh mục bàn giao có thể khôi phục, tài liệu học thuật chính thức.
- Hồ sơ hiện có hỗ trợ **trình diễn kỹ thuật có giới hạn**. Chưa đủ căn cứ nghiệm thu chất lượng dự báo cho nhập hàng thực tế hoặc vận hành production.

Không nên viết lại toàn bộ từ đầu. Nên giữ báo cáo lịch sử, cập nhật tài liệu đang được dùng và lập một bảng trạng thái có ngày đối chiếu rõ ràng.

## 2. Phạm vi và cách kiểm tra

Đã kiểm kê và quét nội dung **82 tệp Markdown, 18.311 dòng**; đọc sâu các tài liệu tổng quan, đặc tả, hướng dẫn chạy, báo cáo tổng hợp và báo cáo kiểm chứng mới. Đối chiếu với mã simulator, DDL, ETL, feature pipeline, Compose, manifest model, SQL/DAX, header CSV, JUnit và bằng chứng JSON liên quan.

Kiểm tra thêm nội dung lưu trong hai workbook Excel, cấu trúc báo cáo PBIX và trạng thái lưu của hai notebook. Các workbook được đọc từ XML; PBIX được đọc từ định nghĩa trang/visual trong tệp. Không mở Power BI Desktop, không chạy DAX engine, không xác nhận giao diện Excel hoặc kết quả thực thi notebook.

| Nhóm | Số Markdown | Nhận xét |
|---|---:|---|
| Kiến trúc và cơ sở lý thuyết | 3 | Có sơ đồ và giải thích; còn nhiều mô tả cũ hoặc tuyệt đối hóa |
| Simulator và buffering | 2 | Có thiết kế; cần sửa số trường và phạm vi chứng minh |
| ETL, feature store và BI specification | 3 | Chi tiết nhưng nhiều chỗ chưa khớp DDL/CSV hiện tại |
| Kiểm thử và benchmark | 17 | Bằng chứng có giá trị khi đọc đúng ngày, môi trường và phạm vi |
| Nhật ký cập nhật | 13 | Có lịch sử sửa lỗi và bằng chứng trước/sau |
| Hướng dẫn chạy và demo | 12 | Demo mới khá rõ; hướng dẫn vận hành cũ cần hợp nhất |
| Tiến độ tuần | 10 | Hữu ích làm lịch sử; không dùng làm trạng thái nghiệm thu hiện tại |
| Bộ báo cáo đồ án | 8 | Thận trọng về giới hạn; phần tổng hợp còn trộn trạng thái trước/sau sửa |
| Data dictionary mới | 1 | Phân biệt rõ thiết kế tích hợp và khả năng đã xác nhận |
| Mục lục docs và tài liệu 5 phần | 2 | Đã có nền tảng PM/PO; cần cập nhật trạng thái và liên kết mới |
| README gốc và các module | 11 | Có hướng dẫn cơ bản; một số tên tệp/bảng và cách chạy đã lỗi thời |

**Giới hạn:** đây là rà soát tài liệu và đối chiếu tĩnh, không phải một lượt nghiệm thu runtime mới. Không chạy lại test, gửi sự kiện, train model, đổi cấu hình dịch vụ hoặc sửa tài liệu gốc. Không kiểm chứng lại URL bên ngoài hay tính hiện hành của API sàn. Các kết quả ngày 20–24/10 dưới đây được ghi nhận từ hồ sơ đã lưu.

Danh mục tệp, hash, liên kết và kết quả đối chiếu máy đọc được nằm trong [static-audit.json](../outputs/pm-documentation-review-20261026/static-audit.json). Số lượng trên không tính báo cáo rà soát này, thư viện phụ thuộc, dữ liệu phát sinh hoặc thư mục thử nghiệm cá nhân.

## 3. Những nội dung đang làm tốt

1. [Tài liệu dự án 5 phần](tai-lieu-du-an-5-phan.md) đã có người dùng, phạm vi trong/ngoài, backlog, acceptance criteria, SLO đề xuất, rủi ro và Definition of Done. Vì vậy, không thể kết luận dự án thiếu hoàn toàn tài liệu PM/PO.
2. [Báo cáo tổng hợp đồ án](07-bao-cao-do-an/bao-cao-tong-hop-do-an.md) phân biệt luồng kỹ thuật, chất lượng model và readiness production; có danh sách A-01–A-17 và nhiều giới hạn được thừa nhận.
3. [Báo cáo lần 9](04-pipeline-updates/lan-09-on-dinh-end-to-end-2026-10-20.md) mô tả checkpoint, xử lý bản tin lỗi, version đơn/tồn kho, kiểm thử restart, benchmark nhỏ và điều kiện chưa đóng.
4. [Báo cáo sáu tình huống demo](03-testing-and-benchmark/demo_web_six_scenarios_2026-10-24.md) giải thích phương pháp đối chiếu độc lập, trạng thái đang xử lý, số lần quarantine và giới hạn phép thử.
5. [Data dictionary ngày 20/10](data-dictionary/DATA_DICTIONARY_VI.md) phân biệt tên trường simulator, nguồn API ứng viên và khóa đề xuất. Đây là cách mô tả phù hợp hơn các cam kết tương thích 100% trong tài liệu cũ.

## 4. Các vấn đề cần xử lý

Ưu tiên dưới đây phục vụ chỉnh lý hồ sơ, không phải phân loại sự cố production: **P1** sửa trước khi dùng làm bản chính hoặc căn cứ nghiệm thu; **P2** hoàn thiện theo phạm vi bàn giao; **P3** chỉnh khả năng tra cứu/bảo trì. Các vai trò ghi bên dưới là **đề xuất**, chưa phải phân công đã được xác nhận.

### DOC-01 — P1: Mô hình, metric và hiệu năng chưa thống nhất

**Bằng chứng:** [architecture.md](00-architecture-overview/architecture.md), dòng 153–160, ghi LightGBM Tuned WAPE 24,5% và inference dưới 2 ms; dòng 194 ghi tải đỉnh trên 3.000 requests/giây. Manifest đang lưu là `moving_average`, version 2, `Staging`, `cv_wape=126.08`. Báo cáo ngày 24/10 cũng xác nhận model này. Workbook đặc tả BI cũ, sheet `TIMELINE DATA!J6`, còn ghi WAPE khoảng 19,8% trên tập kiểm thử thực tế.

**Ảnh hưởng:** người đọc có thể đưa nhầm model và kết quả vào báo cáo bảo vệ. Ghi chú “kiến trúc mục tiêu” ở đầu tài liệu không làm các số kết quả cũ trở thành bằng chứng hiện hành.

**Cần làm:** lập bảng model theo phiên bản, dataset, protocol, ngày đo, metric và artifact. Cập nhật sơ đồ đang sử dụng; tách các kết quả chưa tái lập khỏi phần kết luận. Phân biệt smoke HTTP 20 request/concurrency 4 ngày 20/10 với thử tải 200 người dùng và benchmark in-process.

**Điều kiện đóng:** mỗi con số dùng trong bản chính dẫn đến đúng run/artifact; tên model và metric thống nhất giữa README, sơ đồ, workbook, báo cáo và slide. Chủ trì đề xuất: MLOps + người biên tập báo cáo.

### DOC-02 — P1: Quy trình khởi động lần đầu chưa thống nhất

**Bằng chứng:** [README gốc](../README.md), dòng 121, khởi động Kafka nhưng không có `kafka-init`. [Runbook tuần 10](05-how-to-run/how-to-run-week10.md), mục 3, cũng bỏ bước này. Trong [Compose](../docker-compose.yml), Kafka tắt tự tạo topic; tạo hai topic thuộc service `kafka-init`. Trên môi trường mới chưa có topic, chuỗi lệnh đó chưa đủ. Tài liệu 5 phần đã có `kafka-init`/`minio-init`, cho thấy các hướng dẫn đang lệch nhau.

README đồng thời giới thiệu worker tự chạy nhưng vẫn nói cả hai consumer và ETL chạy riêng trên host; runbook tuần 10 chưa hướng dẫn chọn chế độ thủ công hay worker.

**Cần làm:** chọn một quy trình chuẩn từ môi trường trống, gồm tạo cấu hình, topic/bucket, schema/migration, worker, dữ liệu, model, API và kiểm tra đầu ra. Ghi rõ khi nào dùng chế độ thủ công và cách dừng worker trước khi thay đổi luồng. Không coi máy đã có volume/topic là thử nghiệm cài mới.

**Điều kiện đóng:** người tiếp quản thực hiện được quy trình trên môi trường thử nghiệm trống, có topic, nhận được fixture và đạt kiểm tra đầu ra; giữ nguyên dữ liệu máy đang dùng. Chủ trì đề xuất: DE/MLOps.

### DOC-03 — P1: Đặc tả dữ liệu không khớp schema triển khai

**Bằng chứng đối chiếu trực tiếp:**

| Nội dung | Tài liệu cũ | Mã hoặc tệp xuất hiện tại |
|---|---|---|
| Số trường sự kiện | Shopee 84, TikTok 71 | Dataclass Shopee 61, TikTok 70; không phải xác nhận số cột API sàn |
| Số dimension | 5 | DDL có 6: Products, Shops, Geography, Dates, Payment, Carriers |
| Khóa ngày | BI specification ghi `YYYYMMDD` | DDL dùng `SERIAL`; CSV có `date_key=1` cho 01/01/2025 |
| Cột ngày dimension | Nhiều hướng dẫn nối `Dim_Dates[Date]` | CSV xuất `full_date`; nếu có đổi tên trong Power BI phải mô tả bước đổi tên |
| Lag/rolling | Sơ đồ có 30 ngày | Mặc định lag `[1,2,3,7,14,28]`, rolling `[7,14,28]` |
| Version sự kiện | Tài liệu 5 phần coi chặn ghi lùi còn mở | Mã load và migration đã có `source_updated_at`, `snapshot_at`; vẫn còn giới hạn tie-break khi cùng thời gian |

Nguồn: [kiến trúc](00-architecture-overview/architecture.md), [BI specification](02-warehouse-etl-feature-store/data_dictionary_data_model_specification.md), [DDL](../warehouse/ddl/01_star_schema.sql), [feature code](../ml/features/time_series_features.py), [báo cáo lần 9](04-pipeline-updates/lan-09-on-dinh-end-to-end-2026-10-20.md).

**Cần làm:** xác định tài liệu sở hữu contract raw, warehouse và BI; ghi rõ phần đã triển khai và phần đề xuất. Đồng bộ Markdown, workbook và sơ đồ từ DDL/header thực tế. Giữ riêng số trường raw, số cột warehouse và số feature model.

**Điều kiện đóng:** tên cột, kiểu, grain, PK/FK, unique key và mapping khớp bản triển khai được chọn; relationship BI có hướng dẫn có thể thực hiện. Chủ trì đề xuất: DE.

### DOC-04 — P1: Định nghĩa doanh thu dễ gây hiểu sai

**Bằng chứng:** [BI specification](02-warehouse-etl-feature-store/data_dictionary_data_model_specification.md), dòng 28, ghi `net_revenue = buyer_total_amount - commission_fee - service_fee`. Trong khi [view](../powerbi/views_for_powerbi.sql) cộng `buyer_total_amount`, và [exporter](../powerbi/export_powerbi_dataset.py) đổi tên kết quả thành `net_revenue`. Workbook cũng gọi chỉ tiêu là dòng tiền thực nhận. [DAX mới](../powerbi/dax_measures.md), dòng 74, đã ghi rõ tiền người mua không đồng nghĩa doanh thu đã thu tiền.

**Ảnh hưởng:** cùng nhãn nhưng khác ý nghĩa kinh tế, có thể làm sai diễn giải revenue/AOV và kết luận lợi ích.

**Cần làm:** thống nhất glossary KPI: tiền người mua, doanh thu theo dự án, doanh thu sau phí, tiền đã quyết toán và lợi nhuận. Chọn một định nghĩa cho dashboard hiện tại, ghi rõ hủy/hoàn/phí/thuế và cách phân bổ tiền cấp đơn cho nhiều dòng.

**Điều kiện đóng:** mỗi KPI có công thức, grain, bộ lọc, đơn vị và nguồn; ví dụ nhiều dòng có phí/giảm giá đối soát được. Không ghi “thực nhận” nếu chưa có dữ liệu quyết toán tương ứng. Chủ trì đề xuất: PO + DE/BI.

### DOC-05 — P1: Power BI được mô tả hoàn thiện vượt hiện vật

**Bằng chứng:** [Power BI README](../powerbi/README.md), dòng 28, gọi PBIX là báo cáo hoàn thiện và mô tả ba trang; kiến trúc mô tả bốn trang. Đọc định nghĩa trong [PBIX](../powerbi/Report_PowerBI.pbix) hiện chỉ có một trang “Quản trị Tồn kho và cảnh báo”, ba visual: shape, textbox, card; chỉ card có query. Đây là kiểm tra cấu trúc lưu, chưa phải kết quả render/refresh.

**Cần làm:** chốt phạm vi dashboard ba hay bốn trang. Hoàn thiện PBIX theo phạm vi đó hoặc ghi rõ trạng thái bản nháp và phần chưa thực hiện. Không dùng CSV đã đối soát làm bằng chứng PBIX đã hoàn thành.

**Điều kiện đóng:** refresh/render trên Desktop, xác nhận KPI, slicer, relationship, DAX và các trang cam kết; lưu ảnh và biên bản kiểm tra. Chủ trì đề xuất: BI + PO.

### DOC-06 — P1: Một số cam kết chưa có bằng chứng đủ phạm vi

**Bằng chứng:** kiến trúc dòng 7, 245 và 368 cam kết tương thích thực tế 100%, chỉ thay connector và giữ nguyên toàn bộ phía sau. [Thiết kế buffering](01-ingestion-buffering/thiet_ke_buoc_dem_hung_du_lieu.md), dòng 332, gọi hệ thống Production-Ready. BI specification dòng 31 ghi stockout thực nghiệm dưới 1,5%. Trong khi data dictionary mới và báo cáo tổng hợp thừa nhận chưa tích hợp shop thật, chưa nghiệm thu nhiều điều kiện production và chưa chứng minh tiết kiệm chi phí thực tế.

**Cần làm:** đổi thành mục tiêu/giả thuyết khi chưa có chứng cứ. Với tương thích nguồn thật, phải xác nhận quyền API, schema, status mapping, SKU/shop/line ID và tiền cấp đơn. Với stockout phải có dataset, cách đo, baseline, khoảng thời gian và kết quả đối chứng.

**Điều kiện đóng:** kết luận không vượt phạm vi thử nghiệm; phân biệt đạt luồng demo, chất lượng model và nghiệm thu production. Chủ trì đề xuất: PM/PO + tác giả.

### DOC-07 — P2: Có yêu cầu và checklist, chưa có hồ sơ nghiệm thu khép kín

**Bằng chứng:** tài liệu 5 phần đã có BR-01–BR-08, SLO đề xuất, vai trò và DoD. Bản tổng hợp có A-01–A-17. Tuy nhiên chưa thấy một bảng hợp nhất nối yêu cầu → phiên bản triển khai → test/evidence → kết quả hiện tại → người thực hiện → người xác nhận → hạn đóng. Chính tài liệu 5 phần cũng nói cần lập biên bản riêng.

Workbook có PIC/end date/Completed cho một số việc cũ; các cột đó chưa thay thế trạng thái nghiệm thu hiện tại. Không nên suy ra chưa có kế hoạch, nhưng kế hoạch hiện có chưa đủ để kiểm soát công việc còn mở.

**Cần làm:** lập một ma trận nghiệm thu; chọn tiêu chí áp dụng cho demo và pilot riêng. Ghi rõ người nhận/xác nhận phù hợp dự án cá nhân, không giả định đã có PO doanh nghiệp ký. Các SLO đề xuất chỉ trở thành cam kết sau khi người có thẩm quyền xác nhận.

**Điều kiện đóng:** BR/SLO trong phạm vi được gán trạng thái, evidence, người phụ trách và ngày; có bản chấp nhận phạm vi kèm ngoại lệ còn mở. Chủ trì đề xuất: PM.

### DOC-08 — P2: Hồ sơ nguồn dữ liệu và gói bàn giao chưa hoàn chỉnh

**Bằng chứng:** [phần bài toán/dữ liệu](07-bao-cao-do-an/01-bai-toan-va-du-lieu.md), mục 4.2, chưa xác minh nguồn xuất bản và giấy phép CSV. `.gitignore` loại `data/` và phần lớn CSV. Các báo cáo đã lưu nhiều JSON/JUnit/ảnh trong Git, nhưng dữ liệu huấn luyện, manifest, raw/model artifact và một phần bằng chứng vẫn thuộc dữ liệu cục bộ.

**Cần làm:** bổ sung dataset card có nguồn/quyền sử dụng, phạm vi thời gian, fingerprint, timezone, cách khử định danh và giới hạn chia sẻ. Tạo manifest bàn giao nêu artifact bắt buộc, checksum, nơi lưu được phép truy cập, cách lấy và cách dựng lại. Không đưa dữ liệu/secrets bị hạn chế vào Git chỉ để lấp thiếu hồ sơ.

**Điều kiện đóng:** người nhận biết rõ những gì có trong repo, cần nhận thêm gì và phục hồi được demo/model theo đúng phiên bản. Chủ trì đề xuất: DE + MLOps + tác giả.

### DOC-09 — P2: Mục lục và trạng thái “mới nhất” đã cũ

**Bằng chứng:** [mục lục kiểm thử](03-testing-and-benchmark/README.md) dừng ở 14/10 dù có báo cáo 20/10 và 24/10. Runbook tuần 10 gọi kết quả 10/10 là mới nhất. `PROJECT_CONTEXT.md` tự nhận SSOT nhưng trạng thái ghi đè đầu tệp là 09/10. Bản tổng hợp đồ án có cập nhật đầu tệp ngày 20/10 nhưng phần tóm tắt/bảng tổng hợp vẫn nói model chưa nạp và KPI còn lỗi theo lượt 19/10.

**Cần làm:** tạo bảng trạng thái hiện tại có ngày bằng chứng; giữ các kết quả trước sửa trong mục lịch sử. Cập nhật mục lục và đường đọc chính đến demo 24/10, data dictionary 20/10 và các bản sửa liên quan. Mỗi tài liệu đang dùng ghi ngày cập nhật, phạm vi và tài liệu thay thế nếu có.

**Điều kiện đóng:** người đọc từ README tìm được một kết luận hiện tại thống nhất; không phải tự ghép nhiều đoạn ghi đè. Chủ trì đề xuất: PM + người biên tập.

### DOC-10 — P2: Vận hành có kế hoạch kiểm tra, chưa có đủ bằng chứng đóng

**Bằng chứng:** [checklist production](03-testing-and-benchmark/checklist-truoc-van-hanh-production.md) có 10 nhóm, mẫu RPO/RTO và điều kiện đạt. Báo cáo lần 9 vẫn để mở Alertmanager/người nhận, scheduler drift/retrain/rollback, coverage, HA và mất host/volume. Đây là các hạng mục đã được ghi nhận, không phải hoàn toàn thiếu tài liệu.

**Cần làm:** chuyển các mục liên quan thành runbook có bước thao tác, owner, lịch, đầu vào và kết quả mong đợi; có diễn tập restore, cảnh báo đến người nhận và rollback. Cập nhật checklist bằng chứng mới, giữ rõ các phần chưa chạy.

**Điều kiện đóng:** đủ bằng chứng theo phạm vi pilot/production đã chọn. Với đồ án/demo, được để thành giới hạn hoặc hướng phát triển, nhưng phải bỏ các tuyên bố production-ready tương ứng. Chủ trì đề xuất: DE/MLOps + PM.

### DOC-11 — P2: Bản học thuật chính thức còn thiếu phần hoàn thiện

**Bằng chứng:** README còn chỗ trống sinh viên/GVHD/lớp, trong khi docs đã có tên tác giả. Slide còn thông tin người học/GVHD chưa điền và thời gian tháng 09/2026. Báo cáo tổng hợp ghi rõ là bản thảo, chưa theo mẫu trường, chưa có danh mục trích dẫn hoàn chỉnh. Một số tài liệu đã có link nguồn nhưng chưa thành hồ sơ tham khảo học thuật thống nhất.

**Cần làm:** xác nhận thông tin tác giả và yêu cầu trường; hoàn thiện bìa, tóm tắt, mục lục, danh mục hình/bảng/từ viết tắt, phương pháp, thảo luận giới hạn và trích dẫn. Ghi đúng đóng góp và sử dụng AI; không tự điền thông tin chưa xác nhận.

**Điều kiện đóng:** bản nộp theo mẫu và có rà soát học thuật; các phát biểu thực nghiệm/nguồn dữ liệu dẫn được đến căn cứ. Chủ trì đề xuất: tác giả + người hướng dẫn/biên tập.

### DOC-12 — P3: Liên kết và README module giảm khả năng bàn giao

**Kết quả quét:** 277 lượt liên kết Markdown tương đối, tính cả hình; **1 lượt trỏ tới đích không tồn tại**. Ngoài ra có **68 liên kết `file:///...`** gắn đường dẫn máy cũ. Phép kiểm tra này không xác nhận anchor, URL web hoặc nội dung của đích liên kết.

- Liên kết hỏng ở [nhật ký lần 1](04-pipeline-updates/lan-01-khac-phuc-loi-va-toi-uu-luong.md), dòng 4: `../test-pipeline/bao_cao_kiem_thu_hieu_nang_lan_1.md`; báo cáo hiện nằm trong `03-testing-and-benchmark`.
- `monitoring/README.md` dẫn tên cũ `docs/architecture.md` trong văn bản.
- `scripts/README.md` liệt kê `init_kafka_topics.sh` không có trong workspace và `Dim_Warehouses` không có trong DDL hiện tại.

**Cần làm:** dùng liên kết tương đối theo cấu trúc repo; đồng bộ danh mục script/bảng và thêm kiểm tra liên kết khi cập nhật tài liệu.

**Điều kiện đóng:** liên kết nội bộ tồn tại, không còn đường dẫn cá nhân trong tài liệu chia sẻ, script/module README đúng mã. Chủ trì đề xuất: người biên tập + DE.

## 5. Ma trận nghiệm thu sơ bộ theo yêu cầu đã có

Đây là kết quả rà soát bằng chứng, **chưa phải biên bản ký nghiệm thu**. Trạng thái áp dụng đúng fixture/phạm vi đã ghi, không mở rộng thành bảo đảm toàn hệ thống.

| Yêu cầu | Mức bằng chứng hiện có | Điều kiện còn thiếu |
|---|---|---|
| BR-01: bảo toàn dòng hàng/replay | Có fixture nhiều dòng, line ID, version và replay ngày 20/10; demo replay 24/10 | Contract nguồn thật; cùng SKU nhiều dòng phải có ID; tie-break sự kiện cùng timestamp |
| BR-02: truy vết dữ liệu bị loại | Có quarantine và đối soát các cohort; demo quantity âm được kiểm tra độc lập | Schema evolution, replay sau sửa và phạm vi lỗi chưa thử |
| BR-03: dự báo SKU/ngày | Có artifact/Registry và demo forecast có nguồn/version | Coverage, baseline/metric, đánh giá chất lượng theo horizon và ngưỡng được chấp nhận |
| BR-04: cảnh báo tồn kho | Có công thức, nhãn nguồn, kiểm tra tồn giả định 0/100 không ghi DB | Tồn kho thực đủ mới và chất lượng demand; UAT quy tắc nghiệp vụ |
| BR-05: KPI không đếm dòng thành đơn | SQL/export được sửa và có đối soát 20/10 | Glossary tài chính thống nhất; PBIX refresh/render và kiểm tra filters |
| BR-06: model có provenance | Có manifest, feature spec, origin và các báo cáo kiểm chứng | Hồ sơ model theo version, hồ sơ quyền dữ liệu và quy trình duyệt chất lượng |
| BR-07: phát hiện bất thường | Có metrics/rule và nhãn fallback/stale | Giao nhận cảnh báo, lịch drift/retrain và diễn tập lỗi theo phạm vi |
| BR-08: tiếp quản và phục hồi | Có nhiều runbook và kịch bản demo | Quy trình cài mới thống nhất, manifest bàn giao, diễn tập phục hồi và người nhận xác nhận |

Không cộng 145 test ngày 20/10 với 12 test demo ngày 24/10 thành “157 test toàn suite”. Đây là những phạm vi chạy khác nhau; muốn ghi số suite hiện tại cần một lượt chạy toàn bộ với phiên bản và cấu hình được lưu.

## 6. Bảng trạng thái nên dùng làm căn cứ biên tập

| Nội dung | Trạng thái được hồ sơ hỗ trợ | Ngày/phạm vi |
|---|---|---|
| Luồng Kafka → MinIO → warehouse → CSV | Có worker/checkpoint và fixture tự chạy | Báo cáo lần 9, 20/10; demo đối chiếu 24/10 |
| Regression toàn suite đã lưu | 145 tests, 0 failures/errors/skipped trong JUnit | 20/10; không phải kết quả chạy lại 26/10 |
| Demo UI | 6/6 tình huống đối chiếu đạt | Phiên `six-scenarios-20261024-1344` |
| Model được lưu/ghi nhận trong demo | Moving Average v2, Staging, CV WAPE 126,08% | Manifest cục bộ và báo cáo 24/10 |
| Dữ liệu lịch sử model | Kết thúc 11/08; tách nguồn simulator | Báo cáo 24/10; không coi forecast tháng 8 là dữ liệu hiện thời |
| HTTP model live | P95 37,80 ms; 20 request, concurrency 4, một SKU/một ngày, đã làm nóng | 20/10; smoke benchmark |
| KPI orders/AOV | Có sửa SQL/export và đối soát theo snapshot | 20/10; số dòng thay đổi khi thêm fixture |
| PBIX | Một trang/ba visual trong định nghĩa tệp hiện có | Kiểm tra cấu trúc 26/10; chưa render/refresh |
| Production/chất lượng nhập hàng thật | Chưa đủ bằng chứng nghiệm thu | Coverage, chất lượng model, BI và vận hành còn điều kiện mở |

Khi xuất bản bảng này cần đưa về tài liệu sở hữu trạng thái và ghi ngày cập nhật. Không dùng bản rà soát tĩnh này thay trạng thái dịch vụ đang chạy.

## 7. Thứ tự hoàn thiện đề xuất

1. **Chốt bản chính:** chỉ định nơi sở hữu trạng thái, mô hình/metric và contract; cập nhật README, kiến trúc, báo cáo tổng hợp, slide và workbook liên quan. Giữ các lần kiểm thử cũ làm lịch sử.
2. **Sửa các nội dung dễ làm người nhận thực hiện sai:** quy trình cài mới, topic/bucket, chế độ worker/thủ công, khóa/cột dữ liệu và glossary doanh thu. Chốt dashboard cam kết và trạng thái thực hiện.
3. **Khép hồ sơ demo/đồ án:** ma trận BR/evidence, danh sách việc mở có người phụ trách/ngày dự kiến, dataset/model card, gói bàn giao và bản học thuật. Đưa người tiếp quản đi qua kịch bản từ môi trường riêng.
4. **Nếu triển khai pilot/production:** thống nhất SLO/chất lượng, nghiệm thu coverage/freshness, tải, cảnh báo, scheduler, restore/rollback và quyền truy cập trước khi đánh dấu đạt. Không bắt buộc phải hoàn thành production để nộp đồ án nếu đã giới hạn phạm vi rõ ràng.

Không tự gán hạn hoặc người ký thay dự án. PM cần chốt các trường đó với người thực hiện/người nhận trước khi dùng bảng như kế hoạch cam kết.

## 8. Điều kiện xác nhận bộ tài liệu đã hoàn tất

- Không còn mâu thuẫn về model, metric, schema, KPI và chức năng đã triển khai trong tài liệu đang dùng.
- Mọi kết quả quan trọng có ngày, phạm vi, phiên bản và bằng chứng; kết quả chưa thử ghi rõ chưa thử.
- Hướng dẫn khởi động được kiểm tra trên môi trường riêng; người nhận lấy đủ dữ liệu/artifact bắt buộc.
- Yêu cầu trong phạm vi có trạng thái nghiệm thu và người xác nhận; ngoại lệ có người xử lý và hạn dự kiến.
- Dashboard đúng phạm vi cam kết hoặc được ghi rõ là chưa hoàn thành; không dùng mô tả thiết kế thay kết quả.
- Hồ sơ học thuật và nguồn dữ liệu được hoàn thiện theo yêu cầu thực tế.

**Kết luận PM:** nên tiếp tục chỉnh lý và khép hồ sơ trước khi đánh dấu “hoàn thiện”. Nền tảng tài liệu đã có, ưu tiên hiện tại là thống nhất sự thật, làm rõ phần đã chứng minh và biến các danh sách tồn đọng thành kế hoạch có thể theo dõi.

## 9. Minh bạch sử dụng AI

Báo cáo do Codex hỗ trợ kiểm kê, đọc và đối chiếu tĩnh tài liệu/mã/hiện vật. Không xác nhận thêm kết quả runtime hoặc quyền dùng dữ liệu. Đây là đánh giá phục vụ quản lý dự án; người có thẩm quyền của dự án xác nhận phạm vi nghiệm thu và thông tin tác giả.
