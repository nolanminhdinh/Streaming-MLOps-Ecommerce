# Đánh giá đồ án theo cẩm nang ngành Khoa học dữ liệu

Ngày đánh giá: 07/11/2026. Góc nhìn: giảng viên phản biện nghiêm khắc.

## Kết luận đánh giá

Đồ án có khối lượng triển khai đáng kể và bằng chứng tích hợp hệ thống tương đối tốt. Điểm mạnh nằm ở kiến trúc dữ liệu, kiểm soát tính toàn vẹn, truy vết nguồn và phục hồi dịch vụ. Hồ sơ hiện tại chưa đủ để xác nhận đã đáp ứng toàn bộ yêu cầu của cẩm nang. Nếu phản biện trên bộ hồ sơ này, tôi sẽ yêu cầu sửa lớn trước khi đề nghị xác nhận đủ điều kiện bảo vệ.

Nếu buộc phải quy đổi chất lượng nội dung hiện có thành thang 10, mức tham khảo của tôi là 6,0–6,5/10. Đây là nhận định chuyên môn riêng, không phải thang điểm hay trọng số do cẩm nang quy định. Các điều kiện bắt buộc về khai báo AI, minh chứng, mẫu báo cáo và khả năng tự giải thích phải được xét riêng; điểm tham khảo không thay thế các điều kiện đó.

Không có căn cứ từ việc sử dụng AI để kết luận sinh viên thiếu trung thực. Việc sinh viên thực sự hiểu và trực tiếp thực hiện phần nào cần được kiểm tra bằng trao đổi trực tiếp, nhật ký và minh chứng.

## Căn cứ và giới hạn

Tôi đã đọc cẩm nang 25 trang, kiểm tra nội dung và các bảng trong bản Word ngày 29/10, đối chiếu tài liệu kho dữ liệu ngày 01/11, báo cáo cùng JSON kiểm thử ngày 04/11, notebook EDA, mã đặc trưng, baseline và metric. Một số tình huống biên được chạy lại trực tiếp ngày 07/11.

Đây không phải một lượt nghiệm thu lại toàn bộ Docker, Power BI Desktop hoặc huấn luyện lại model. Bằng chứng ngày 04/11 được xem là hồ sơ của lần đo đó, không phải trạng thái dịch vụ được tôi xác nhận trực tiếp ngày 07/11. Chưa phỏng vấn sinh viên hoặc kiểm tra giấy xác nhận của GVHD/GVPB. Không kết luận các thủ tục đó đã thiếu chỉ vì không có trong repository.

Công cụ dựng lại bản Word không chạy được do môi trường không có LibreOffice. Vì vậy, đánh giá này xác nhận nội dung và cấu trúc đã đọc, không chứng nhận toàn bộ dàn trang bản Word hay mức tuân thủ Phụ lục 3. Cẩm nang có nhắc mẫu nhưng nội dung đầy đủ của các tệp mẫu không nằm trong PDF này.

## Cách áp dụng cẩm nang cho đề tài

Tên đề tài trong báo cáo là nghiên cứu và triển khai kiến trúc hạ tầng dữ liệu luồng và MLOps phục vụ dự báo nhu cầu. Nhóm phù hợp nhất là “Dữ liệu lớn — Xây dựng kiến trúc Data Lake/Data Warehouse”, trang 10–11. Phần dự báo liên quan đến nhóm “Phân tích chuỗi thời gian”, trang 9–10, và “Học máy và dự đoán”, trang 4–5, tùy phạm vi đã được duyệt.

Không bắt buộc đề tài phải có thuật toán mới. Không tự động đánh trượt vì dùng Moving Average hoặc không dùng Spark/Hadoop. Cẩm nang cho phép công việc tương đương; công nghệ là phương tiện, còn mức đáp ứng phải được giải trình bằng thiết kế, thực nghiệm và kết quả. Nếu có đề cương được duyệt thu hẹp phạm vi, cần đối chiếu thêm đề cương đó.

Tuy nhiên, việc tự ghi “không đánh giá độ chính xác” trong báo cáo không tự miễn yêu cầu đánh giá kết quả của nhóm chuỗi thời gian. Nếu chọn trọng tâm hạ tầng, vẫn phải đáp ứng yêu cầu định lượng khả năng xử lý, đặc điểm dữ liệu, cấu hình hệ thống, so sánh hiệu suất và diễn giải kết quả. Trang 24 còn yêu cầu giải trình công việc tương đương khi không đạt chính xác các yêu cầu tối thiểu.

## Bảng đối chiếu

| Tiêu chí | Bằng chứng đã thấy | Đánh giá nghiêm khắc |
| --- | --- | --- |
| Xác định bài toán, đầu vào, đầu ra | SKU/ngày; phân biệt lượng đặt không hủy với nhu cầu tiềm ẩn; xác định người sử dụng | Khá. Thiếu mục tiêu định lượng rõ cho thông lượng và độ trễ đầu cuối |
| Nguồn và mô tả dữ liệu | Simulator, bảng lịch sử riêng, data dictionary, fingerprint | Đạt một phần. Nguồn xuất bản, quyền sử dụng CSV và mức đầy đủ theo ngày chưa được chứng minh |
| EDA và làm sạch | Validation, quarantine, đối soát trước/sau và notebook có mã | Đạt một phần. Notebook chưa lưu kết quả; thiếu bộ biểu đồ và diễn giải được kiểm chứng trên đúng dữ liệu dùng model |
| Thiết kế Lake/Warehouse | Kafka, MinIO, PostgreSQL; khóa dòng, event version, schema BI và migration | Khá, là phần mạnh nhất. Cần giải thích lựa chọn so với phương án đơn giản hơn và giới hạn một máy |
| Tích hợp và phục hồi | Replay, checkpoint, ngắt dịch vụ, reload lỗi giữ model cũ, rollback | Khá trong phạm vi các ca được lưu. Không suy rộng thành chịu mất volume hoặc sự cố nhiều nút |
| Đánh giá hạ tầng | Tải 10 phút, 600 đơn, 2.400 HTTP request; đo một số truy vấn | Đạt một phần. Thiếu thực nghiệm so sánh cấu hình có kiểm soát, đường cong tải và phân vị độ trễ sự kiện đầu cuối |
| Đánh giá dự báo | Có hồ sơ WAPE và walk-forward; bản Word loại sai số khỏi tiêu chí đánh giá | Yếu nếu xem dự báo là đầu ra chuyên môn chính. Có lỗi căn chỉnh baseline và xử lý metric bằng 0 |
| Giá trị nghiệp vụ | SS, ROP và ba mức cảnh báo | Chứng minh được logic hỗ trợ. Chưa chứng minh tối ưu chi phí hay giảm thiếu hàng |
| Báo cáo và minh bạch AI | Có bảng KL-2, nhật ký và nguồn tham khảo | Chưa đủ hồ sơ yêu cầu. Thiếu trường công cụ theo công đoạn, mức chỉnh sửa thực tế và ảnh quá trình AI trong phụ lục |
| Làm chủ đồ án và thủ tục | Không thể suy ra từ mã nguồn hoặc số test | Chưa đánh giá; cần kiểm tra trực tiếp và chứng từ tương ứng |

## Những phần xứng đáng được ghi nhận

1. Thiết kế không dừng ở việc dựng nhiều dịch vụ. Mã và tài liệu xử lý những vấn đề có ý nghĩa: độ hạt dòng hàng, replay, cập nhật đến muộn, quarantine và checkpoint.
2. Báo cáo kho dữ liệu ngày 01/11 ghi nhận migration bảo toàn 144.851 dòng đơn, 6.098 snapshot, 13 forecast và 7.000 dòng lịch sử. Có bộ kiểm thử schema riêng và đối soát trước/sau. Các số lượng này thuộc snapshot đó, không phải dữ liệu kinh doanh thật đã được xác minh.
3. Hồ sơ ngày 04/11 có thử dừng/khôi phục từng dịch vụ; kiểm tra model warm/cold khi MLflow lỗi; thử chuyển version, giữ model khi reload lỗi và quay lui trong môi trường cô lập.
4. Ca tải ngày 04/11 ghi nhận 600 ACK đơn tương ứng 600 dòng fact, không thiếu/thừa/trùng trong lô; 2.400 request đạt tiêu chí, P95 HTTP chung 42,201 ms. Đây là bằng chứng tốt tại mức tải đã thử.
5. Báo cáo thường phân biệt nguồn model thật với heuristic/fallback và giữ ngày đo. Tính minh bạch này là điểm cộng về tư duy phản biện; phát hiện hạn chế không làm mất giá trị của toàn bộ đồ án.

## Các vấn đề phải sửa

### 1. Phạm vi báo cáo chưa khớp cách đánh giá theo cẩm nang

Bản Word mục mục tiêu, mục 1.6 và mục 3.1 chủ động không dùng chất lượng dự báo làm tiêu chí. Đây là lựa chọn phạm vi cần giải trình, không phải một kết luận đã được cẩm nang chấp nhận.

Nếu bảo vệ theo hướng hạ tầng, cần lập ma trận: yêu cầu trang 10–11 → thành phần đã làm → phép đo → ngưỡng đạt → bằng chứng. Nếu bảo vệ cả chất lượng dự báo, phải bổ sung đánh giá ngoài mẫu và so sánh mô hình nhất quán. Không cần mở thêm LSTM/GRU để tăng số công nghệ.

### 2. EDA chưa có minh chứng đủ mạnh

Hai notebook hiện có 6 và 7 ô mã; cả hai không lưu output hoặc execution count. Điều này không chứng minh chúng chưa từng được chạy ở nơi khác, nhưng bản nộp hiện tại không cung cấp kết quả có thể xem lại.

Trong notebook EDA đã có nhận xét về thị phần 55%/45%, tỷ lệ hủy khoảng 12%, đỉnh mua sắm và mức tăng cuối tuần 20–30%. Không được dùng tỷ lệ do simulator cấu hình để suy ra thực trạng thị trường. Các nhận xét phải gắn với bảng/biểu đồ thực chạy, nguồn và cỡ mẫu.

Cần có thống kê mean, median, std, min–max; missing và duplicate theo trường; phân bố lượng bán; tỷ lệ ngày 0; ví dụ ngoại lai; bảng trước/sau làm sạch và lý do xử lý. Phải tách EDA của nguồn lịch sử model khỏi EDA của simulator.

### 3. Độ tin cậy của dữ liệu huấn luyện chưa được chốt

Hồ sơ ML ngày 19/10 mô tả 7.000 dòng nguồn, 216 SKU không hủy, 49.032 ô ngày và 95,49% demand bằng 0 sau tạo lưới. Báo cáo kho ngày 01/11 vẫn ghi nhận bảng coverage có 0 dòng.

Không thể đồng nhất ngày thiếu giao dịch với ngày đã thu thập đủ nhưng không bán. Tính đúng của phép zero-fill phải được xác nhận bằng đặc điểm nguồn và coverage. 49.032 ô tổng hợp không phải 49.032 giao dịch gốc độc lập.

CSV có fingerprint và đã khử định danh vẫn cần hồ sơ nguồn xuất bản, điều kiện sử dụng, ngày tải và múi giờ. Fingerprint chỉ xác định tệp; nó không chứng minh bản quyền hoặc tính đại diện.

Dữ liệu mô phỏng mở rộng ngày 31/10 được nạp trực tiếp PostgreSQL và chưa huấn luyện lại model. Không dùng quy mô mới này để mô tả quy mô huấn luyện của model v2.

### 4. Có lỗi căn chỉnh baseline và diễn giải metric

Kiểm tra trực tiếp ngày 07/11 dùng một chuỗi có lượng bán tăng từ 1 đến 41. Với origin 10/02/2026 và mục tiêu 11/02/2026:

| Baseline | Giá trị theo định nghĩa thông thường từ lịch sử đã có | Mã hiện tại trả |
| --- | ---: | ---: |
| Naive, dùng quan sát mới nhất | 41 | 40 |
| Seasonal naive, cùng thứ tuần trước ngày mục tiêu | 35 | 34 |
| Trung bình 7 ngày gần nhất gồm ngày origin | 38 | 37 |

Nguyên nhân là model dự báo t+1 nhưng ưu tiên lag/rolling đã dịch khỏi ngày t. Cần thống nhất lại định nghĩa, huấn luyện, đánh giá và serving; việc các tầng cùng trả một giá trị không chứng minh giá trị đó đúng với định nghĩa baseline.

Với actual [0,0] và prediction [4,4], mã hiện tại trả MAE=4 nhưng WAPE=0 và bias=0. WAPE trong trường hợp tổng actual bằng 0 phải được xử lý như không xác định hoặc có quy ước báo cáo riêng, không diễn giải là dự báo chính xác. Test hiện có còn xác nhận chính hành vi trả WAPE=0, nên chỉ số test đạt không giải quyết lỗi diễn giải này.

Manifest model v2 lưu Moving Average với CV WAPE=126,08%. Hồ sơ audit cho WAPE gộp=124,41%; hai giá trị dùng cách tổng hợp khác nhau. Với actual không âm và tổng actual dương, dự báo toàn 0 có WAPE=100% theo chính công thức đang dùng. Vì vậy, chưa có cơ sở kết luận model hiện tại tốt theo WAPE. Đây là phép đối chiếu toán học, không phải khuyến nghị vận hành bằng dự báo 0.

Kết quả kém không tự động làm đồ án không đạt. Thiết kế đánh giá đúng, phân tích nguyên nhân và giới hạn trung thực vẫn có giá trị. Cần sửa baseline, xác nhận coverage, thống nhất cách tổng hợp, báo tỷ lệ được chấm và dành holdout cuối chưa dùng để chọn model nếu muốn kết luận tổng quát hóa.

### 5. Đánh giá hạ tầng còn thiếu đối chứng

Ca 10 phút ở 1 đơn/giây và 4 HTTP request/giây chứng minh tính ổn định ở mức tải đó. Không chứng minh giới hạn chịu tải, mức đồng thời 200 người hoặc khả năng mở rộng.

P95 HTTP 42,201 ms là thời gian request API; 10,2 giây drain là thời gian từ sau dừng phát đến lúc quan sát đã xử lý hết. Cả hai không thay thế P95 producer ACK → warehouse của mọi sự kiện.

Báo cáo kho đã có ba phép EXPLAIN ANALYZE với thời gian 50,665 ms, 6,122 ms và 659,1 ms. Đây là đo đạc thực, nhưng mỗi truy vấn mới được quan sát một lần, chưa có đối chứng cấu hình hoặc tải đồng thời. Không được nói đồ án hoàn toàn chưa đo hiệu năng; điều còn thiếu là thiết kế so sánh có kiểm soát.

Nên chọn một thí nghiệm vừa sức: thay batch size hoặc polling interval, giữ cùng máy/cùng dữ liệu/cùng tải, chạy lặp lại và so sánh thông lượng, P50/P95/P99 đầu cuối, backlog, tài nguyên và số bản ghi. Làm rõ đánh đổi giữa độ trễ và hiệu suất. Không cần xây thêm cụm phân tán chỉ để đủ tên công nghệ.

### 6. MLOps và nghiệp vụ cần được mô tả đúng phạm vi

Tái huấn luyện/reload ngày 04/11 đã được thử thành công bằng trigger ép tay và Registry cô lập, gồm lần chạy lại sau bổ sung fingerprint. Không tiếp tục coi thiếu fingerprint là lỗi hiện tại vì manifest đã có hai source_datasets.

Tuy nhiên, hồ sơ chưa xác nhận vòng drift thực tế → tự kích hoạt → tái huấn luyện → kiểm tra chất lượng → triển khai theo lịch vận hành liên tục. Cần phân biệt đường chức năng có thể gọi với cơ chế điều phối tự động đã nghiệm thu.

Model lịch sử và SKU simulator là hai phạm vi dữ liệu khác nhau. Trong ca stock 0/50/100 ngày 04/11, SKU KCN-ANESSA dùng demand_source=fallback_catalog. Ba mức cảnh báo đúng không chứng minh SKU đó được dự báo bởi model v2. Những ca ngày 24/10 dùng SKU lịch sử và model_forecast vẫn được ghi nhận riêng, không đánh đồng tất cả ca là fallback.

SS và ROP cho thấy một chính sách hỗ trợ ra quyết định. Nếu muốn dùng từ “tối ưu”, cần định nghĩa hàm mục tiêu, chi phí, ràng buộc và đối chứng chính sách. Trong phạm vi đã làm, “hỗ trợ quản trị tồn kho” phù hợp hơn với bằng chứng.

### 7. Báo cáo nộp và bảng khai báo AI cần hoàn thiện

Bản Word ngày 29/10 vẫn có trường thông tin “[Bổ sung …]”, còn ghi chưa có cẩm nang chính thức và mô tả các thành phần trước thay đổi 01/11. Cần hợp nhất kết quả 31/10, 01/11 và 04/11, giữ rõ ngày từng phép đo; cập nhật vai trò NiFi, Power BI PostgreSQL Import, schema BI và kiểm thử phục hồi.

Bảng KL-2 có công đoạn AI hỗ trợ và nội dung cần rà soát nhưng chưa nêu đầy đủ công cụ cụ thể, mục đích và mức chỉnh sửa thực tế từng phần. “Cần tự rà soát” là kế hoạch, không phải mô tả sinh viên đã kiểm chứng và sửa gì. Cẩm nang không bắt buộc phải bịa tỷ lệ phần trăm; có thể khai báo định tính với ví dụ cụ thể.

Trong bản Word đã đọc, hình ảnh thuộc phần kiến trúc và demo, chưa thấy ảnh quá trình sử dụng AI trong phụ lục như yêu cầu trang 25. Cần bổ sung ảnh thật đã có, nhật ký và ví dụ đầu ra được chỉnh sửa; không tạo lại bằng chứng giả. Nếu không còn ảnh lịch sử, phải trình bày trung thực và trao đổi cách bổ sung hồ sơ phù hợp với GVHD.

Power BI đã có nguồn SQL và bộ truy vấn, nhưng hồ sơ 04/11 chưa xác nhận Apply/Refresh/render trong Desktop. Nếu là thành phần phụ trợ được công bố rõ thì mức ảnh hưởng nhỏ hơn; nếu tuyên bố dashboard đã hoàn thiện thì phải có nghiệm thu tương ứng.

## Thứ tự hoàn thiện đề nghị

| Ưu tiên | Việc cần làm | Kết quả có thể kiểm tra |
| --- | --- | --- |
| 1 | Chốt trọng tâm hạ tầng và ma trận đáp ứng cẩm nang | Mỗi yêu cầu có vị trí báo cáo, phép đo, bằng chứng hoặc giải trình tương đương |
| 2 | Hoàn thiện nguồn dữ liệu, AI và thông tin bản nộp | Thẻ dữ liệu; bảng AI đủ trường; phụ lục minh chứng; không còn placeholder |
| 3 | Hoàn thiện EDA trên đúng nguồn dữ liệu | Notebook lưu output, biểu đồ kèm số liệu và nhận xét có căn cứ |
| 4 | Sửa baseline/metric, xác nhận coverage | Ví dụ ngày mục tiêu đúng; quy ước zero-demand rõ; không tạo coverage giả |
| 5 | Chạy một thí nghiệm hạ tầng có đối chứng | Cùng môi trường và dữ liệu, ít nhất hai cấu hình, kết quả lặp và phân vị đầu cuối |
| 6 | Hợp nhất báo cáo và diễn tập bảo vệ | Một bản cuối thống nhất mã, kiến trúc, số liệu và phạm vi đã làm |

## Câu hỏi phản biện cần trả lời được

1. Một dòng fact biểu diễn gì? Một đơn có hai dòng cùng SKU được phân biệt thế nào?
2. Vì sao upload trước commit offset? Nếu upload xong mà crash trước commit thì dữ liệu được bảo vệ ở tầng nào?
3. Sự kiện cũ đến sau hoặc hai sự kiện khác payload có cùng timestamp được xử lý thế nào?
4. Tại sao một ngày trống được điền 0? Bằng chứng nào xác nhận nguồn đã đầy đủ?
5. Vì sao model dự báo t+1 lại đọc lag_1 của ngày t? Em sửa và kiểm chứng căn chỉnh ra sao?
6. WAPE 126,08% nghĩa là gì? Vì sao không được gọi đó là accuracy? Khi tổng actual bằng 0 phải báo thế nào?
7. Model v2 dùng 216 SKU lịch sử có quan hệ gì với 20 SKU simulator? Khi nào API dùng fallback?
8. Kết quả 600 đơn và P95 HTTP chứng minh điều gì, chưa chứng minh điều gì về thời gian thực và khả năng mở rộng?
9. Mắt xích nào của vòng MLOps đã tự động thật, mắt xích nào đang được gọi bằng tay hoặc chỉ thử trong môi trường cô lập?
10. AI tạo phần nào? Em đã sửa quyết định nào và có thể giải thích hoặc chỉnh trực tiếp một đoạn mã quan trọng không?

## Các nguồn chính đã đối chiếu

- Cẩm nang: `C:/Users/MINH/Downloads/Cẩm nang ĐATN - KHDL.pdf`; trọng tâm trang 3–5, 9–11 và 23–25.
- Báo cáo nộp: `output/docx/Bao_cao_DATN_Dinh_Cong_Minh.docx`; mục mục tiêu, 1.6, 2.7–2.11, 3.1–3.10, bảng KL-2 và phụ lục.
- Kho dữ liệu hiện hành: `docs/07-bao-cao-do-an/bao-cao-xay-dung-kho-du-lieu-2026-11-01.md`, `warehouse/README.md`.
- Kiểm thử dịch vụ: `output/e2e-2026-11-04/Bao-cao-kiem-thu-end-to-end.md`; đối chiếu `soak.json`, `api-monitor.json`, `retrain-manifest-fixed.json`.
- Hồ sơ ML: `docs/07-bao-cao-do-an/04-feature-engineering-va-machine-learning.md`, `docs/07-bao-cao-do-an/evidence/ml_pipeline_2026-10-19.json`, `data/model_manifest.json`.
- Mã kiểm tra chuyên môn: `ml/features/time_series_features.py`, `serving/app/baseline_models.py`, `ml/training/metrics.py`, `ml/training/compare_models.py`, `ml/training/deep_learning_models.py`.
- EDA: `notebooks/eda.ipynb`, `notebooks/abc_xyz_classification.ipynb`.

Các đường dẫn tương đối trong danh mục trên thuộc thư mục gốc dự án `C:/Users/MINH/Đếm ngày xa trường/temp-extract/streaming-mlops-ecommerce`.

## Ghi nhận kiểm tra ngày 07/11

Đã chạy đạt 20 test tập trung về core operations, data readiness, features và ML pipeline, sau khi chuyển thư mục tạm vào vùng làm việc. Lượt toàn bộ có 42 test đạt trước khi dừng tại nhóm HTTP do môi trường cấm mở socket; các lỗi subtest HTTP không được dùng làm căn cứ trừ điểm sản phẩm. Lượt mở rộng loại nhóm HTTP đã được dừng khi không tiếp tục trả kết quả, nên không công bố số đạt toàn suite mới. Các nhóm có điều kiện tích hợp PostgreSQL không được xem là đã nghiệm thu lại trong lượt này. Không thay mã nguồn hoặc thuật toán của dự án trong quá trình đánh giá. Các phép thử trực tiếp về baseline và zero-demand metric được lưu tại `tmp/handbook-review-20261107/current-probes.json`.
