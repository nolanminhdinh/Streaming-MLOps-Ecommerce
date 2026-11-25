# BÁO CÁO XÂY DỰNG KHO DỮ LIỆU

Hệ thống Streaming MLOps dự báo nhu cầu và hỗ trợ quản trị tồn kho thương mại điện tử

Sinh viên thực hiện Đinh Công Minh

Chuyên ngành Khoa học dữ liệu

Hà Nội tháng 11 năm 2026

Báo cáo chuyên đề về yêu cầu dữ liệu, thiết kế khái niệm, thiết kế logic, triển khai vật lý và kiểm chứng kho dữ liệu PostgreSQL cho đồ án tốt nghiệp.

# TÓM TẮT

Kho dữ liệu được xây dựng để tiếp nhận đơn hàng và tồn kho từ luồng mô phỏng Shopee và TikTok Shop, thống nhất dữ liệu để phân tích, tạo lịch sử nhu cầu và lưu kết quả dự báo phục vụ đối chiếu. Thiết kế sử dụng PostgreSQL 16 cho các bảng quan hệ, MinIO lưu dữ liệu thô và Kafka làm bộ đệm truyền sự kiện. Phạm vi hiện tại không bao gồm kết nối API thương mại thực tế hoặc vận hành cụm phân tán có khả năng chịu lỗi nhiều nút.

Kết quả chính là phân định độ hạt của bốn fact nghiệp vụ, củng cố ràng buộc dữ liệu và triển khai schema bi gồm 14 view báo cáo. Fact dòng hàng được giữ ở mức chi tiết, có khóa chung để phân tích theo sản phẩm, ngày, shop và địa lý. Snapshot tồn kho và dự báo được tách riêng để tránh cộng sai số dư hoặc trộn kết quả thuộc các nguồn lịch sử khác nhau.

Ngày 01/11/2026, migration đã được áp dụng trong một transaction và bảo toàn 144.851 dòng đơn hàng, 6.098 snapshot tồn kho, 13 dự báo đã lưu và 7.000 dòng lịch sử. Nhóm kiểm thử hồi quy liên quan đạt 81 trường hợp; trong đó 12 trường hợp kiểm tra schema mới trên các schema PostgreSQL cô lập và rollback. Đây là bằng chứng triển khai và tính toàn vẹn ở phạm vi kiểm tra, chưa phải xác nhận dữ liệu nguồn đã sạch hoàn toàn hoặc toàn hệ thống đã nghiệm thu production.

Các vấn đề còn mở gồm 28 mã đơn có thuộc tính giữa các dòng chưa nhất quán, 24 snapshot legacy chưa biết thời điểm sự kiện và thiếu xác nhận coverage cho việc đánh giá actual. Báo cáo trình bày rõ những giới hạn này để có thể đối chiếu giữa lý thuyết, mã nguồn và thực nghiệm.

# MỤC LỤC

1 Giới thiệu và phạm vi

2 Cơ sở lý thuyết

3 Phân tích yêu cầu dữ liệu

4 Thiết kế khái niệm và logic

5 Thiết kế vật lý và triển khai

6 Nạp dữ liệu và vận hành

7 Kiểm thử và đối soát

8 Khai thác dữ liệu qua Power BI

9 Đánh giá và hướng phát triển

Kết luận

Tài liệu tham khảo

Phụ lục A Từ điển dữ liệu vật lý

Phụ lục B Truy vấn tái lập

# 1 GIỚI THIỆU VÀ PHẠM VI

## 1.1 Bài toán và lý do xây dựng kho dữ liệu

Hệ thống bán hàng đa kênh cần trả lời đồng thời các câu hỏi về số đơn, lượng hàng bán, tiền người mua thanh toán, tồn kho hiện tại và nhu cầu dự kiến. Các câu hỏi này có độ hạt khác nhau: một đơn có thể có nhiều dòng hàng, một SKU có nhiều snapshot tồn kho và một ngày mục tiêu có thể có dự báo từ nhiều nguồn dữ liệu. Nếu đưa tất cả vào cùng một bảng hoặc nối trực tiếp các fact, tổng số và giá trị có thể bị nhân lên.

Vì vậy, mục tiêu thiết kế không chỉ là tạo đủ bảng để lưu JSON từ nguồn. Kho dữ liệu phải có quy ước nhận diện bản ghi, mô tả thời gian, ràng buộc quan hệ và ý nghĩa đo lường thống nhất. Cùng một dữ liệu cần dùng được cho ETL, phân tích BI và lịch sử nhu cầu mà không phải sửa định nghĩa KPI ở từng công cụ.

## 1.2 Mục tiêu và kết quả cần đạt

Mục tiêu thứ nhất là mô tả được dữ liệu ở ba mức khái niệm, logic và vật lý. Mục tiêu thứ hai là chứng minh vì sao chọn mức dòng hàng cho dữ liệu bán và mức SKU theo ngày cho tồn kho. Mục tiêu thứ ba là xây dựng cơ chế nạp có thể phát lại mà không nhân đôi fact. Mục tiêu thứ tư là cung cấp một hợp đồng dữ liệu cho Power BI, có thể kiểm tra trực tiếp bằng SQL.

Kết quả bàn giao gồm DDL của các bảng, migration ràng buộc, schema bi, công cụ audit, bộ kiểm thử và bằng chứng đối soát trước sau. Không lấy việc vẽ được biểu đồ làm thay thế cho việc kiểm tra schema và ngữ nghĩa số liệu.

## 1.3 Phạm vi dữ liệu và giả định nghiệp vụ

Dữ liệu luồng hiện được phát bởi simulator của dự án. Một doanh nghiệp được giả định sử dụng kho trung tâm; stock_on_hand là số dư theo SKU, không phải số dư riêng cho từng shop. SKU của catalog vận hành được coi là duy nhất trong phạm vi doanh nghiệp. Nếu mở rộng sang nhiều doanh nghiệp, natural key cần bổ sung merchant hoặc tenant; ràng buộc sku duy nhất hiện tại chưa đáp ứng trường hợp đó.

Nguồn lịch sử ngoài được lưu riêng để phục vụ demand forecasting. Mã SKU giống nhau giữa một tệp bên ngoài và catalog vận hành không đủ chứng minh chúng là cùng sản phẩm. Vì vậy, báo cáo không coi lịch sử ngoài là giao dịch có thể cộng vào doanh thu vận hành, và không tự sinh giá vốn, shop hay địa chỉ cho bản ghi thiếu thông tin.

NiFi không thuộc luồng nghiệm thu hiện tại. Training, đăng ký model, drift detection, retraining và refresh Power BI là những tác vụ có phạm vi riêng; không mô tả toàn bộ dự án như một pipeline streaming tự động hoàn toàn.

## 1.4 Phương pháp thực hiện và đối chiếu

Trình tự được kế thừa từ cách tổ chức báo cáo đồ án trước: phân tích yêu cầu, thiết kế khái niệm, thiết kế logic và chuẩn hóa, thiết kế vật lý, triển khai rồi kiểm thử. Đối với kho dữ liệu phân tích, trình tự này được kết hợp với bốn quyết định dimensional modeling của Kimball là chọn quá trình nghiệp vụ, xác định độ hạt, chọn dimensions và xác định facts [2].



Bảng 1 Ma trận lý thuyết và hiện vật triển khai



| Cơ sở | Quyết định trong đồ án | Đối chứng |

| --- | --- | --- |

| Yêu cầu và độ hạt | Bán hàng ở cấp dòng; tồn kho ở cấp SKU ngày | 01_star_schema.sql; bi_schema.sql |

| Khóa quan hệ | PK surrogate và unique natural key | Audit metadata PostgreSQL; DDL |

| Mô hình sao | Dimensions dùng chung cho các fact vận hành | bi.dim_date, dim_product, dim_shop, dim_geography |

| Toàn vẹn dữ liệu | FK, NOT NULL, CHECK và UNIQUE | 04_integrity_constraints.sql; 12 test schema |

| Nạp lặp an toàn | Upsert theo khóa và phiên bản nguồn | load.py; inventory consumer |

| Tính đúng của báo cáo | Đối soát số dòng, tổng và khả năng nối | Migration evidence; reporting_schema_checks JSON |

# 2 CƠ SỞ LÝ THUYẾT

## 2.1 Kho dữ liệu và cơ sở dữ liệu giao dịch

Tài liệu Oracle trình bày kho dữ liệu theo các đặc trưng hướng chủ đề, tích hợp, biến thiên theo thời gian và ít biến động sau khi nạp [1]. Trong đồ án, các chủ đề chính là bán hàng, tồn kho và nhu cầu dự báo; dữ liệu hai sàn được chuyển về schema thống nhất để truy vấn chung. PostgreSQL được dùng như nền tảng quan hệ cho lớp phân tích, còn dữ liệu thô được giữ ở MinIO để truy vết và xử lý lại.

Có một giới hạn cần phân biệt: Fact_Orders đang cập nhật trạng thái mới nhất của dòng hàng. Vì vậy, lớp này có tính chất lưu trạng thái hiện hành, chưa đáp ứng đầy đủ yêu cầu bảo toàn mọi phiên bản lịch sử của một kho dữ liệu bất biến. Việc giữ raw ở lake hỗ trợ truy vết, nhưng không đồng nghĩa đã có fact sự kiện hay cơ chế truy vấn trạng thái tại mọi thời điểm. Đây là lựa chọn triển khai có chủ ý trong phạm vi đồ án, không phải bằng chứng mọi đặc trưng kho dữ liệu cổ điển đã được thỏa mãn.

## 2.2 Quá trình nghiệp vụ và độ hạt

Theo Kimball, cần xác định grain trước khi quyết định dimensions và facts [2]. Grain là phát biểu về sự kiện hoặc đối tượng mà một dòng biểu diễn, không chỉ là danh sách cột khóa. Trong bài toán này, phát biểu một dòng là một đơn không phù hợp với nguồn có nhiều SKU; phát biểu một dòng là snapshot của một SKU trong một ngày lại phù hợp với cách nạp tồn kho hiện tại.

Ví dụ minh họa: đơn A có hai dòng SKU X và Y; đơn B có một dòng SKU X. Bảng chi tiết có ba dòng nhưng chỉ có hai đơn. Khi lọc SKU X có hai đơn, lọc SKU Y có một đơn; cộng hai kết quả phân nhóm thành ba không phải tổng số đơn toàn bộ. Đây là lý do phải dùng distinct order key trong đúng filter context và không cộng các distinct count đã tổng hợp.

## 2.3 Fact giao dịch và fact snapshot

Kimball phân biệt fact theo grain giao dịch, snapshot định kỳ và snapshot tích lũy [3]. Fact giao dịch phản ánh sự kiện, snapshot định kỳ phản ánh trạng thái ở một kỳ, còn snapshot tích lũy theo dõi tiến trình với các mốc được cập nhật. Kiểu fact quyết định cách cộng số liệu: lượng bán có thể cộng theo các dòng bán phù hợp, còn số dư tồn kho có thể cộng theo SKU tại cùng một lát cắt nhưng không cộng qua nhiều ngày để ra tồn hiện tại.

Fact_Inventory_Daily là snapshot ngày có cập nhật trong ngày. Fact_Orders mang định danh dòng bán và các mốc vòng đời, nhưng cách upsert khiến nó là bảng trạng thái mới nhất của dòng hàng, không phải nhật ký bất biến. Báo cáo giữ cách gọi này để tránh gán một loại fact chuẩn chỉ dựa trên tên bảng. Hai view fact_inventory_daily và inventory_latest được tách để người dùng không nhầm lịch sử stock với stock mới nhất.

## 2.4 Mô hình sao và dimensions dùng chung

Trong mô hình sao, dimension cung cấp thuộc tính để lọc hoặc phân nhóm, còn fact lưu các phép đo ở một grain xác định. Khóa của dimension phải nhận diện duy nhất thành viên để quan hệ một nhiều có ý nghĩa [4]. Đồ án dùng product_key và date_key xuyên suốt các bảng vận hành. Nguồn lịch sử ngoài chưa được đồng nhất danh mục với catalog live nên không được coi là dùng chung dimension sản phẩm chỉ vì có cột sku cùng tên.

## 2.5 Phụ thuộc hàm và chuẩn hóa

Chuẩn hóa xem xét các phụ thuộc giữa thuộc tính để giảm lặp và tránh bất thường khi cập nhật. 1NF tổ chức giá trị theo các thuộc tính xác định; 2NF loại phụ thuộc một phần vào khóa ghép; 3NF loại phụ thuộc bắc cầu không phù hợp qua thuộc tính không khóa [5]. Chẳng hạn, nếu lưu product_name trên mọi dòng đơn như thuộc tính quản trị sản phẩm, việc đổi tên phải cập nhật nhiều dòng. Tách Dim_Products làm nơi quản lý mô tả giúp tránh bất thường này.

Không thể suy ra toàn bộ schema đạt 3NF chỉ vì đã tách nhiều bảng hoặc đặt tên Dim. Thuộc tính region có thể phụ thuộc state trong dimension địa lý; platform được giữ trên fact dù một shop đã gắn với sàn; đây là các điểm lặp có chủ ý để phục vụ phân tích hoặc nhận diện bản ghi. JSONB source_datasets lưu tập fingerprint provenance, cũng không được mô tả như thuộc tính nghiệp vụ nguyên tử theo cách hiểu 1NF cổ điển. Báo cáo phân tích chuẩn hóa cho từng quan hệ thay vì khẳng định toàn bộ warehouse đạt BCNF.

## 2.6 Khóa và ràng buộc toàn vẹn

PostgreSQL hỗ trợ primary key, unique, foreign key, not null và check để thực thi các quy tắc ngay tại nơi lưu dữ liệu [6]. PK nhận diện dòng vật lý; natural key thể hiện định danh từ nghiệp vụ; FK giữ liên kết tới dimension. UNIQUE không thay thế NOT NULL: một khóa ghép có thành phần nullable cần được xem xét riêng. Trong schema hiện tại, product và ngày là bắt buộc đối với đơn hàng và tồn kho, trong khi shop hoặc địa chỉ có thể chưa được nguồn cung cấp.

CHECK kiểm tra một dòng, không chứng minh các dòng cùng đơn có cùng địa chỉ hoặc shop [6]. Bởi vậy, ràng buộc quantity dương được đặt ở bảng; xung đột header được phát hiện bằng truy vấn nhóm và view chất lượng. Không biến một kiểm tra cấp nhóm thành CHECK tham chiếu tùy ý sang dòng khác.

## 2.7 Thay đổi dimension và quản lý thời gian

SCD Type 1 ghi đè thuộc tính và không giữ phiên bản trước; Type 2 thêm phiên bản dimension để phân tích theo giá trị có hiệu lực trong lịch sử [7]. Các thuộc tính sản phẩm được loader cập nhật hiện dùng cách ghi đè. Kho chưa có valid_from, valid_to hoặc is_current cho dimension nên không được tuyên bố đã triển khai SCD Type 2. Nếu tên hoặc phân loại sản phẩm thay đổi, báo cáo lịch sử dùng dimension hiện hành có thể thay đổi cách nhóm.

Đồ án còn phân biệt thời gian nguồn và thời gian nạp. create_time phục vụ ngày nghiệp vụ; source_updated_at chống ghi lùi phiên bản đơn; snapshot_at phản ánh thời điểm snapshot; loaded_at là metadata của việc ghi kho. Snapshot legacy thiếu snapshot_at được đánh dấu chưa biết thời gian sự kiện, thay vì dùng loaded_at để tạo cảm giác đã biết chính xác.

## 2.8 View chỉ mục và nạp có thể phát lại

View thông thường là một truy vấn được đặt tên, không phải bản sao dữ liệu đã materialize [8]. Schema bi vì vậy cung cấp giao diện ổn định trên bảng gốc. Index hỗ trợ một số đường truy xuất nhưng có chi phí ghi và lưu trữ; không phải mọi truy vấn tổng hợp đều cần dùng index [9]. Hiệu năng phải được kiểm tra qua kế hoạch thực thi của workload cụ thể.

INSERT ON CONFLICT cho phép xử lý khi natural key đã tồn tại, kết hợp điều kiện cập nhật để giữ phiên bản phù hợp [10]. Trong đồ án, offset chỉ commit sau khi lưu đích thành công; tình huống tiến trình chết sau khi ghi nhưng trước commit có thể khiến bản tin được đọc lại. Upsert làm giảm trùng fact khi replay, nhưng không tạo ra transaction phân tán chung cho Kafka, MinIO và PostgreSQL. Vì vậy, toàn luồng được mô tả là at least once, chưa phải exactly once [11].

# 3 PHÂN TÍCH YÊU CẦU DỮ LIỆU

## 3.1 Câu hỏi phân tích và mức chi tiết cần lưu



Bảng 2 Yêu cầu phân tích và dữ liệu cần thiết



| Câu hỏi | Mức dữ liệu cần lưu | Điều kiện đúng |

| --- | --- | --- |

| Bao nhiêu đơn và đơn vị bán theo ngày sàn | Dòng đơn có order key, date_key, platform | Loại hủy theo định nghĩa KPI; distinct order key |

| Sản phẩm hoặc shop nào đóng góp lượng bán | Dòng đơn có product_key, shop_key | Không dùng summary ngày sàn thiếu các khóa này |

| Phân bổ giao dịch theo địa lý | Dòng đơn có geo_key | Giữ nhóm địa lý chưa xác định; không loại âm thầm |

| Tồn hiện tại và cần đặt thêm SKU nào | Snapshot mới nhất mỗi product | Không cộng số dư qua các ngày; có policy thì mới phân cấp |

| Dự báo có khớp thực tế | Forecast cùng SKU ngày và phạm vi nguồn | Actual chỉ hoàn tất khi ngày kết thúc và coverage được xác nhận |

| Nạp lại có làm tăng số dòng | Natural key và phiên bản sự kiện | Replay không nhân fact; sự kiện cũ không ghi đè mới |

## 3.2 Nguồn dữ liệu và hợp đồng đầu vào

Đơn hàng mô phỏng được gửi lên ecom.orders.raw, còn snapshot stock theo SKU được gửi lên inventory.logs. Consumer đơn hàng lưu Parquet vào raw/orders theo sàn và ngày nghiệp vụ. ETL đọc raw, chuẩn hóa tên trường khác nhau giữa hai nền tảng, kiểm định và nạp vào warehouse. Các dòng không đạt được giữ ở quarantine với nguyên nhân; việc lưu lỗi thất bại không được coi là xử lý batch thành công.

Nguồn lịch sử ngoài được phân biệt bằng data_origin, source_dataset và source_record_id. source_dataset là định danh dataset đã nhập, không phải tùy ý coi mọi tệp CSV là cùng nguồn. Forecast cần lưu model_name, model_version, model_source, history_source, history_end_date và source_datasets để biết nó được tạo từ phạm vi nào.

## 3.3 Quy tắc nghiệp vụ

Một dòng đơn được nhận diện bằng platform, order_id và source_line_id. Nếu nguồn có line ID thì dùng định danh đó; trường hợp không có ID, fallback theo SKU là thỏa hiệp và không phân biệt được mọi trường hợp hai dòng cùng SKU trong một đơn. Một SKU có thể xuất hiện ở nhiều đơn và nhiều ngày. Một đơn nhiều dòng không làm tăng số đơn tương ứng với số dòng.

Một snapshot stock được nhận diện bằng product_key và date_key ở kho trung tâm; snapshot mới hơn trong cùng ngày thay bản cũ. stock_on_hand không âm. shop_key của bảng tồn kho không được dùng để gán phân bổ tồn theo shop vì dữ liệu nguồn chưa có nghiệp vụ này. Các ngày chưa có đơn không tự trở thành ngày zero sales đã được xác nhận; coverage là điều kiện riêng.

Quy tắc tiền giữ theo dữ liệu nguồn hiện có. buyer_amount không tự đồng nghĩa lợi nhuận hay doanh thu đã thu tiền. Khi tiếp nhận API nhiều dòng hàng thật, phải xác nhận tiền được cung cấp ở cấp header hay cấp line và có quy tắc phân bổ trước khi cộng. Trường legacy unit_cost đang nhận original_price từ loader, nên được trình bày ở BI là reference_selling_price, không dùng làm giá vốn.

## 3.4 Yêu cầu phi chức năng và tiêu chí kiểm tra

Kho cần khởi tạo lại được từ DDL, có migration diễn tập được trước khi commit và bảo toàn số dòng cùng tổng facts sau thay đổi schema. Các yêu cầu bổ sung gồm truy vết dữ liệu lỗi, giữ thời gian nghiệp vụ nhất quán, không làm mất dòng vì join báo cáo và tách quyền đọc BI khỏi quyền ghi ETL. Hiệu năng được xem xét trên truy vấn cụ thể, chưa đặt một con số độ trễ chung như kết quả đã nghiệm thu.

# 4 THIẾT KẾ KHÁI NIỆM VÀ LOGIC

## 4.1 Các thực thể và mối quan hệ

Mô hình khái niệm có sản phẩm, shop, địa chỉ, ngày, phương thức thanh toán và đơn vị vận chuyển; sự kiện quan sát gồm dòng đơn hàng, snapshot tồn kho và dự báo. Dữ liệu lịch sử ngoài là một nhóm nguồn riêng. Quan hệ khái niệm sản phẩm phát sinh nhiều dòng đơn không được hiểu là một mã SKU ngoài dataset đã có cùng ý nghĩa với SKU vận hành.

FIGURE_ERD

Hình 1 Quan hệ chính của mô hình vận hành và nhánh lịch sử riêng

## 4.2 Chuyển sang mô hình quan hệ

Các dimension sử dụng surrogate key số nguyên để fact có thể tham chiếu ổn định. Natural key được giữ để upsert và đối soát: sku cho catalog hiện tại; shop_id cùng platform cho shop; state, city, district cho địa lý; full_date cho lịch. date_key là SERIAL surrogate key, không phải mã YYYYMMDD. day_of_week theo PostgreSQL DOW có Chủ nhật bằng 0, Thứ hai bằng 1 và Thứ bảy bằng 6.



Bảng 3 Grain và khóa của các fact



| Bảng vật lý | Một dòng biểu diễn | Khóa nghiệp vụ |

| --- | --- | --- |

| Fact_Orders | Trạng thái mới nhất của dòng hàng | platform + order_id + source_line_id |

| Fact_Inventory_Daily | Snapshot cuối đã biết của SKU trong ngày | product_key + date_key |

| Fact_Forecast_Predictions | Dự báo theo SKU ngày và phiên bản lịch sử | product_key + target_date + model_name + model_version + history_end_date + data_origin + source_datasets |

| Fact_Historical_Order_Lines | Dòng nguồn lịch sử đã khử định danh | source_dataset + platform + source_record_id |

| Warehouse_Daily_Coverage | Xác nhận đầy đủ một ngày nguồn | data_origin + source_dataset + full_date |

## 4.3 Chuẩn hóa và lựa chọn phi chuẩn hóa có kiểm soát

Ở dữ liệu đơn dạng phẳng, các mô tả sản phẩm, shop và địa chỉ lặp theo từng dòng. Thiết kế chuyển các mô tả được quản trị chung sang dimension, giữ measure và khóa tham chiếu ở fact. Khóa dòng nghiệp vụ được tách khỏi khóa surrogate để vừa chống trùng khi replay vừa nhận diện dòng vật lý. Các mốc thời gian của dòng đơn vẫn nằm trong fact do chúng thuộc vòng đời của chính dòng đó.

Hai quyết định phi chuẩn hóa được nêu rõ. Thứ nhất, platform được giữ trên fact để natural key không va chạm giữa các sàn và để giữ thông tin sàn khi shop chưa resolve được. Thứ hai, dimension ngày và địa lý giữ các thuộc tính phân loại thuận tiện cho báo cáo. Các lựa chọn này phục vụ mô hình phân tích; chúng không phải bằng chứng schema được chuẩn hóa hoàn toàn đến BCNF.

Khi thiết kế quan hệ cấp order header, về lý thuyết có thể tách bảng header một dòng mỗi đơn và bảng line nhiều dòng. Tuy nhiên, dữ liệu hiện có 28 mã đơn xung đột thuộc tính giữa các dòng nên chưa đủ căn cứ tự chọn header đúng. Đợt sửa hiện tại giữ grain line, tạo view kiểm tra và chưa xây bảng header vật lý từ các giá trị được chọn tùy tiện.

## 4.4 Catalog lịch sử và provenance dự báo

bi.dim_historical_product có grain data_origin, source_dataset và sku. historical_product_key được mã hóa bằng jsonb_build_array rồi chuyển text, tránh va chạm do ghép chuỗi bằng một ký tự phân cách có thể xuất hiện trong dữ liệu. bi.fact_historical_order_line dùng cùng biểu thức khóa, vì vậy không cần ép 226 SKU ngoài catalog live vào Dim_Products chỉ để tạo được relationship.

Các dự báo đã lưu được trình bày qua hai giao diện: fact_forecast_issue giữ đầy đủ các issue trong bảng vật lý; forecast_evaluation chỉ lấy những dự báo mà view đánh giá chọn và giữ các trường provenance. Việc một issue chưa xuất hiện ở evaluation không đồng nghĩa đã bị xóa. So sánh accuracy phải giới hạn nguồn và tập dataset phù hợp, tránh cộng nhiều dự báo chồng lên một tập actual.

## 4.5 Hợp đồng schema báo cáo

Schema public tiếp tục phục vụ consumer, loader và serving để tránh làm đứt luồng đang có. Schema bi cung cấp view với tên, grain và khóa được công bố. Hợp đồng này là lớp ngữ nghĩa, không phải một kho dữ liệu vật lý thứ hai. Các FK vật lý nằm ở bảng nguồn; PostgreSQL không tự tạo FK trên view để Power BI nhận biết thay cho người dựng mô hình.



Bảng 4 Danh mục view và cardinality đã kiểm tra



| View trong bi | Grain và lưu ý | Số dòng |

| --- | --- | --- |

| dim_date | Một ngày; date_key; full_date duy nhất | 1095 |

| dim_geography | Một địa chỉ phân cấp; geo_key | 20 |

| dim_historical_product | Danh mục theo nguồn dataset SKU; không mặc định conformed với live | 230 |

| dim_platform | Một sàn; platform | 2 |

| dim_product | Một SKU live; product_key; reference_selling_price không phải giá vốn | 126 |

| dim_shop | Một shop theo sàn; shop_key | 22047 |

| fact_forecast_issue | Toàn bộ issue đã lưu; giữ provenance | 13 |

| fact_historical_order_line | Dòng lịch sử; khóa nguồn và historical_product_key | 7000 |

| fact_inventory_daily | Một SKU ngày; stock có thể cộng theo SKU nhưng không qua ngày | 6098 |

| fact_order_line | Một dòng hàng; đủ khóa product shop geo date platform; có cờ chất lượng | 144851 |

| forecast_evaluation | Tập forecast được chọn cho đối chiếu actual | 6 |

| inventory_health | Giữ toàn bộ SKU có snapshot; policy thiếu được đánh dấu riêng | 38 |

| inventory_latest | Snapshot mới nhất từng SKU; thời điểm giữa SKU có thể khác nhau | 38 |

| quality_order_header_conflicts | Các đơn cần truy vết thuộc tính header giữa nhiều dòng | 28 |

# 5 THIẾT KẾ VẬT LÝ VÀ TRIỂN KHAI

## 5.1 Lựa chọn công nghệ và tổ chức lưu trữ

PostgreSQL 16 cung cấp kiểu số chính xác, khóa quan hệ, transaction và SQL view cho workload của đồ án. NUMERIC được dùng cho tiền và số lượng dự báo; INTEGER dùng cho số lượng nguyên và các khóa liên kết; DATE dùng cho ngày phân tích. TIMESTAMPTZ được dùng ở snapshot_at và thời điểm phát forecast, còn các timestamp nghiệp vụ đơn được ETL chuyển về giờ địa phương trước khi lưu dạng timestamp không múi giờ. Khi truy vấn cần giữ đúng quy ước này thay vì tự chuyển múi giờ lần thứ hai.

Dữ liệu raw và metadata MLflow không được coi là cùng một schema phân tích. MinIO lưu raw theo partition và artifact; PostgreSQL ecom_warehouse lưu facts và dimensions, trong khi database MLflow riêng giữ metadata tracking. Phần báo cáo này tập trung ecom_warehouse. Docker volume giữ dữ liệu PostgreSQL qua việc tạo lại container; đó chưa phải chiến lược backup và phục hồi đã được thử nghiệm.

## 5.2 Bảng vật lý và cardinality



Bảng 5 Bảng vật lý tại thời điểm audit



| Bảng public | Số dòng | Vai trò |

| --- | --- | --- |

| dim_products | 126 | Dimension |

| dim_shops | 22047 | Dimension |

| dim_geography | 20 | Dimension |

| dim_dates | 1095 | Dimension |

| dim_payment | 9 | Dimension |

| dim_carriers | 7 | Dimension |

| fact_orders | 144851 | Fact nghiệp vụ |

| fact_inventory_daily | 6098 | Fact nghiệp vụ |

| fact_forecast_predictions | 13 | Fact nghiệp vụ |

| fact_historical_order_lines | 7000 | Fact nghiệp vụ |

| warehouse_daily_coverage | 0 | Xác nhận đầy đủ dữ liệu theo ngày |

Số dòng trên là snapshot kiểm tra ngày 01/11/2026. Có 144.851 dòng đơn thuộc cả trạng thái hủy và không hủy; phạm vi không hủy có 118.688 dòng và 118.650 đơn distinct. Hai con số không được dùng thay cho nhau. Tương tự, 6.098 dòng inventory là lịch sử SKU ngày, còn inventory_latest chỉ có 38 SKU ở thời điểm đọc.

## 5.3 Khóa chỉ mục và ràng buộc

Ràng buộc unique dòng đơn hiện có là order_id, platform và source_line_id. Unique inventory là product_key và date_key. Forecast sử dụng uq_fact_forecasts_issue; source_datasets là một thành phần nhận diện provenance. Do history_end_date vẫn nullable ở thiết kế vật lý, unique này cần được kiểm tra cẩn thận khi có dữ liệu legacy NULL; audit hiện tại không phát hiện dòng như vậy hoặc identity bị trùng. Báo cáo không mở rộng kết luận này thành bảo đảm cho mọi dữ liệu tương lai.

Migration bổ sung NOT NULL cho product_key, date_key và is_cancelled của Fact_Orders, cùng product_key và date_key của inventory. Các CHECK chặn quantity không dương, platform ngoài miền hỗ trợ, một số trường tiền âm hoặc NaN, stock âm và việc ghi stock kho trung tâm theo shop. Ràng buộc được áp dụng vào bảng thật, không chỉ là điều kiện lọc để làm báo cáo đẹp hơn.



Bảng 6 Quy tắc toàn vẹn và mức thực thi



| Quy tắc | Nơi thực thi | Trạng thái |

| --- | --- | --- |

| Sản phẩm và ngày bắt buộc | NOT NULL ở order và inventory | Đã áp dụng |

| Dòng đơn không trùng natural key | UNIQUE hiện có | Đã có trước migration |

| Quantity dương và stock không âm | CHECK ở bảng vật lý | Đã áp dụng và thử chèn sai |

| Tham chiếu dimension tồn tại | FK ở bảng vật lý | Có trong DDL nguồn |

| Cùng đơn thống nhất header | View nhóm kiểm tra chất lượng | Phát hiện; chưa sửa nguồn |

| Ngày actual đã đầy đủ | Warehouse_Daily_Coverage và logic evaluation | Chưa có dòng coverage trong audit |

| Thời gian nguồn của legacy stock | snapshot_time_known | 24 dòng chưa biết, không backfill giả |

Các index đang phục vụ truy cập theo ngày, sản phẩm, shop, sàn, trạng thái và create_time của đơn. Chưa thêm index chỉ vì có một cột xuất hiện trên dashboard. Với truy vấn tổng hợp đọc phần lớn fact, kế hoạch có thể chọn sequential scan và parallel aggregation hợp lý; cần nhìn vào EXPLAIN thay vì mặc định index scan luôn tốt hơn.

## 5.4 Migration và bảo toàn dữ liệu

scripts/migrate_warehouse_schema.py mặc định diễn tập trong một transaction rồi rollback. Khi dùng --apply, tiến trình lấy advisory lock, giới hạn thời gian chờ lock, khóa các fact cần đối soát, ghi baseline, áp dụng ràng buộc và cài view. Chỉ khi số dòng và các tổng được chọn không thay đổi, đồng thời kiểm tra grain và khả năng nối đạt, transaction mới commit. Việc rollback rehearsal không xóa dữ liệu cũ hoặc thay số nhận diện của fact.

Thành phần được bảo toàn qua lần migration gồm count dòng fact, tổng quantity, tổng buyer_total_amount, tổng stock ở toàn bộ snapshot và số forecast/historical rows. Đây là kiểm tra conservation ở các đại lượng đã chọn, không phải checksum cho từng byte dữ liệu hay bằng chứng mọi thuộc tính nghiệp vụ đã đúng. Script không thực hiện xóa kho, nạp lại dữ liệu hoặc tự sửa 28 mã đơn xung đột.

# 6 NẠP DỮ LIỆU VÀ VẬN HÀNH

## 6.1 Luồng dữ liệu từ nguồn đến báo cáo

FIGURE_FLOW

Hình 2 Luồng đơn hàng và tồn kho cùng ranh giới của lớp báo cáo

pipeline-worker quản lý hai consumer, liệt kê các object raw chưa hoàn tất và gọi ETL theo batch. Checkpoint object được ghi sau khi ETL thành công. Power BI Import đọc schema bi khi refresh. CSV được giữ làm snapshot dự phòng bằng tác vụ riêng; lỗi xuất file không chặn checkpoint hay buộc nạp lại một batch warehouse đã hoàn tất.

## 6.2 Chuẩn hóa và kiểm định trước khi nạp

Các tên trường đặc thù hai sàn được ánh xạ về schema thống nhất, timestamp được parse và chuyển sang BUSINESS_TZ, kiểu số được chuẩn hóa và version được suy ra khi nguồn thiếu trường cập nhật riêng. Validator kiểm tra định danh, kiểu giá trị, độ dài và miền nghiệp vụ trước khi deduplicate. Điều này tránh việc một dòng trùng nhưng sai dữ liệu bị bỏ mất dấu vết chỉ vì đã chọn một dòng đại diện.

Các dòng bị từ chối được lưu vào quarantine với payload và nguyên nhân. Batch gồm cả dòng tốt và dòng lỗi vẫn phải lưu audit lỗi thành công trước khi được coi hoàn tất. Nếu raw Parquet không đọc được, ETL không được bỏ qua object rồi báo thành công. Kiểm tra accounting yêu cầu raw bằng valid cộng rejected cộng duplicate đã nhận diện ở phạm vi batch.

## 6.3 Thứ tự nạp dimension và fact

Loader upsert sản phẩm, shop, địa lý và carrier, bảo đảm ngày tồn tại, sau đó resolve surrogate key để ghi fact. Không resolve được product hoặc ngày thì phải dừng batch vì đó là liên kết bắt buộc. Shop và địa lý chưa có thể để NULL có kiểm soát; view fact giữ missing_shop và missing_geography để người dùng biết mức thiếu. Bảng thanh toán dùng danh mục được seed, không tự bịa một phương thức mới khi chưa xác định nguồn.

Các bước nạp dimension và fact hiện sử dụng nhiều transaction khi gọi loader mặc định. Nếu fact thất bại sau khi dimension đã commit, có thể còn thành viên dimension chưa được fact tham chiếu. Tình trạng này không tự gây mất fact vì checkpoint chưa tiến, nhưng cũng có nghĩa không được tuyên bố toàn bộ dimension và fact đã được nạp atomically như một transaction duy nhất. Luồng upsert cho phép thử lại để hoàn tất.

## 6.4 Trùng bản tin và sự kiện đến muộn

Với đơn hàng, ON CONFLICT cập nhật dòng khi version nguồn không cũ hơn version đã lưu. Với inventory, snapshot_at kiểm soát việc snapshot cũ không ghi đè snapshot mới trong cùng ngày. Nếu consumer crash sau khi ghi MinIO hoặc DB nhưng trước commit Kafka offset, bản tin có thể được đọc lại; object raw hoặc quarantine có thể phát sinh bản sao audit, trong khi unique và upsert bảo vệ grain của fact.

Phiên bản nguồn bằng nhau vẫn cần một quy tắc nghiệp vụ nếu hai payload khác nội dung. Mã hiện tại cho phép cập nhật khi version bằng nhau; chưa có tie breaker độc lập hoặc conflict log riêng cho trường hợp này. Đây là mục cần bổ sung nếu tích hợp nguồn thực có thể phát hai nội dung khác nhau với cùng timestamp.

## 6.5 Xử lý lỗi vận hành và phạm vi phục hồi



Bảng 7 Lỗi và hành vi mong đợi



| Tình huống | Hành vi của luồng | Kiểm tra hoặc giới hạn |

| --- | --- | --- |

| MinIO lỗi khi lưu raw hoặc quarantine | Không commit batch chưa lưu; thử lại qua restart | Đã có test bảo vệ commit; cần thêm fault injection dài hạn |

| PostgreSQL lỗi khi ghi fact | Batch không checkpoint; replay phải idempotent | Upsert và các test SQL hiện có |

| JSON hoặc dữ liệu sai | Giữ raw hoặc quarantine với lý do | Không sửa im lặng để qua validator |

| CSV bị khóa | Tác vụ backup lỗi độc lập | Worker không gọi exporter trong cycle |

| Consumer dừng giữa ETL | Không công bố healthy sau cycle nếu child đã thoát | Worker ghi trạng thái và Docker restart |

| Stock legacy không có timestamp nguồn | Giữ NULL và cờ chưa biết | Không dùng thời gian nạp để thay thế |

| Một SKU chưa đủ dữ liệu tính policy | Giữ SKU và demand_policy_available=false | Không coi như NORMAL hoặc tự đặt policy bằng 0 |

# 7 KIỂM THỬ VÀ ĐỐI SOÁT

## 7.1 Thiết kế thực nghiệm

Thực nghiệm gồm ba mức. Mức thứ nhất đọc metadata và dữ liệu thật bằng transaction chỉ đọc. Mức thứ hai diễn tập migration rồi rollback, sau đó áp dụng thật và đối chiếu conservation. Mức thứ ba tạo schema nguồn và schema báo cáo riêng cho từng test PostgreSQL, chèn fixture có chủ ý và rollback sau kiểm tra. Các fixture không tăng sequence hoặc ghi vào public facts.

Nhóm hồi quy được chạy bằng Python 3.12 có sẵn cùng dependencies của dự án, với các cờ RUN_WAREHOUSE_SCHEMA_TESTS và RUN_POWERBI_SQL_TESTS. Có cảnh báo tương thích PyArrow và Pandas nhưng không có test lỗi trong nhóm 81 trường hợp. Kết quả này không có nghĩa toàn bộ suite dự án hoặc toàn bộ môi trường Docker đã được kiểm tra ở cùng lần chạy.



Bảng 8 Kết quả kiểm thử có thể đối chiếu



| Nhóm hoặc kiểm tra | Kết quả | Bằng chứng |

| --- | --- | --- |

| Schema mới trên PostgreSQL cô lập | 12 đạt | pytest_warehouse_schema_2026-11-01.xml |

| Hồi quy schema BI ETL ingestion checkpoint | 81 đạt; không lỗi hoặc bỏ qua | pytest_warehouse_schema_regression_2026-11-01.xml |

| Diễn tập migration | Đạt và rollback | warehouse_schema_rehearsal_2026-11-01.json |

| Áp dụng migration | COMMITTED; baseline trước sau bằng nhau | warehouse_schema_apply_2026-11-01.json |

| View sau commit bằng kết nối đọc mới | 14 view; count và grain đạt | reporting_schema_checks_2026-11-01.json |

## 7.2 Tình huống kiểm thử schema

Fixture nhiều dòng gồm ba dòng thuộc hai đơn khác nhau giữa hai sàn, kiểm tra count line bằng ba, distinct order bằng hai, quantity bằng sáu và buyer amount bằng sáu mươi. Fixture khác địa lý giữa hai dòng cùng đơn kiểm tra cả hai dòng còn nguyên và đều được gắn cờ. Fixture tồn kho theo hai ngày kiểm tra chỉ lấy snapshot mới nhất từng SKU và vẫn giữ SKU chưa có policy. Fixture lịch sử có cùng mã SKU ở hai dataset kiểm tra khóa catalog không bị trộn.

Các thử nghiệm ràng buộc cố cập nhật quantity về 0 hoặc âm, giá thành NaN, buyer amount âm và khóa product/date NULL; PostgreSQL từ chối. Thử nghiệm inventory kiểm tra stock âm, shop_key có giá trị trong phạm vi kho trung tâm và product_key NULL. Cuối cùng, áp dụng cùng DDL và schema BI hai lần để xác nhận tính lặp lại của migration.

## 7.3 Conservation trước và sau migration



Bảng 9 Tổng được chọn để kiểm tra bảo toàn



| Đại lượng toàn bộ fact | Trước | Sau |

| --- | --- | --- |

| Dòng Fact_Orders | 144.851 | 144.851 |

| Tổng quantity gồm cả hủy | 346.819 | 346.819 |

| Tổng buyer_total_amount gồm cả hủy | 88.881.805.910 | 88.881.805.910 |

| Snapshot inventory | 6.098 | 6.098 |

| Tổng stock qua mọi snapshot | 1.743.739 | 1.743.739 |

| Forecast issues | 13 | 13 |

| Dòng historical | 7.000 | 7.000 |

Tổng stock 1.743.739 ở bảng trên chỉ là đại lượng kỹ thuật để xác nhận migration không đổi dữ liệu lịch sử; nó không được đưa lên KPI tồn hiện tại. Cùng nguyên tắc đó, số tiền gồm cả đơn hủy không phải tổng bán hàng hợp lệ của dashboard. Khi đối soát KPI cần áp dụng đúng điều kiện is_cancelled và đúng lát cắt thời gian.

## 7.4 Phát hiện chất lượng dữ liệu



Bảng 10 Các phát hiện còn cần xử lý



| Phát hiện | Số lượng | Cách biểu diễn hoặc hành động |

| --- | --- | --- |

| Đơn có xung đột thuộc tính giữa các dòng | 28 mã đơn và 66 dòng liên quan | quality_order_header_conflicts; header_conflict |

| Trong số đơn trên có khác shop | 6 mã đơn | Truy payload nguồn; không tự chọn shop |

| Trong số đơn trên có khác trạng thái hủy | 1 mã đơn | Xác định hủy cấp dòng hay cấp đơn trước khi chuẩn hóa |

| Dòng đơn thiếu shop | 29 dòng | missing_shop=true; giữ fact |

| Dòng đơn thiếu địa lý | 8 dòng | missing_geography=true; giữ fact |

| Dòng đơn thiếu carrier | 39.179 dòng | Thuộc tính optional; không tự gán đơn vị vận chuyển |

| Snapshot chưa biết event time | 24 dòng | snapshot_time_known=false |

| SKU historical chưa có trong catalog live | 226 SKU | Dimension lịch sử riêng; không tự đồng nhất |

| Dòng xác nhận coverage | 0 dòng | Không công bố actual hoàn tất hoặc WAPE giả |

Các phát hiện không được đồng nhất với vi phạm khóa vật lý. Ví dụ, chưa biết carrier không tự làm sai grain dòng hàng, còn cùng đơn khác địa chỉ là một quy tắc cấp nhóm cần hiểu nghiệp vụ trước khi sửa. Việc tách dimension lịch sử giải quyết khả năng biểu diễn và nối dữ liệu, không chứng minh hai catalog đã được đối chiếu sản phẩm thực tế.

## 7.5 Kiểm tra kế hoạch truy vấn



Bảng 11 Thời gian thực thi quan sát tại PostgreSQL



| Truy vấn | Execution time tính bằng ms | Phạm vi |

| --- | --- | --- |

| sales_by_category | 50.665 | Một lần EXPLAIN ANALYZE trên kho hiện tại |

| latest_inventory | 6.122 | Một lần EXPLAIN ANALYZE trên kho hiện tại |

| order_conflicts | 659.1 | Một lần EXPLAIN ANALYZE trên kho hiện tại |

Truy vấn tổng hợp sản lượng theo category đọc phần lớn dòng không hủy và dùng parallel scan cùng aggregation. Truy vấn kiểm tra xung đột phải group các dòng theo order và thực hiện distinct trên nhiều thuộc tính; kế hoạch lần đo ghi nhận sort ra đĩa tạm. Các số đo là execution time của PostgreSQL tại thời điểm kiểm tra, không bao gồm truyền dữ liệu sang Power BI, không phải phân vị P95 và chưa có thí nghiệm đối chứng bỏ index hoặc tải đồng thời.

Nếu view chất lượng gây chậm ở lần Import toàn bộ, có thể lập lịch materialize kết quả hoặc dùng bảng audit được cập nhật theo batch, nhưng phải công bố độ trễ làm mới. Báo cáo không coi đề xuất này là tính năng đã triển khai.

# 8 KHAI THÁC DỮ LIỆU QUA POWER BI

## 8.1 Hợp đồng nguồn Import

Nguồn báo cáo bán hàng mới là bi.fact_order_line, đi cùng dim_date, dim_product, dim_shop, dim_geography và dim_platform. Mỗi dimension nối một nhiều sang fact qua khóa tương ứng, chiều lọc Single từ dimension. Mô hình chi tiết này cho phép phân tích theo SKU và địa lý mà summary ngày sàn cũ không hỗ trợ. Các view cũ vẫn tồn tại để tương thích; không trộn measure của hai grain trong cùng một KPI.

Tồn kho dùng bi.inventory_health cho snapshot hiện tại hoặc bi.fact_inventory_daily cho phân tích lịch sử riêng. Forecast dùng bi.forecast_evaluation và giữ các slicer nguồn dữ liệu. Power BI Import lưu một snapshot trong mô hình; ETL có dữ liệu mới chưa làm biểu đồ tự thay đổi cho tới khi refresh. Nếu đưa lên Power BI Service với PostgreSQL cục bộ, cần gateway và lịch refresh riêng [12].

## 8.2 Định nghĩa measure và xử lý NULL

Số đơn dùng distinct order_business_key trên các dòng đủ điều kiện. Lượng bán và tiền người mua dùng SUM trong cùng phạm vi không hủy. AOV là tỷ số tổng tiền trên tổng đơn, không lấy trung bình các AOV của nhóm. header_conflict và missing_geography là các trường kiểm soát chất lượng, không phải điều kiện được tự thêm để âm thầm loại dữ liệu khỏi KPI.

Trong inventory_health, policy chưa có được biểu diễn bằng demand_policy_available=false và các trường policy có thể NULL. Báo cáo phải hiển thị chưa đủ dữ liệu, không tự quy về NORMAL. Trong forecast, actual_complete=false giữ actual ở trạng thái chưa đánh giá. Không thay NULL actual bằng 0 để vẽ đủ đường hoặc tính WAPE; zero sales cần coverage xác nhận.

## 8.3 Quyền đọc và kiểm tra sau refresh

Tài khoản BI nên chỉ được cấp USAGE cho schema bi và SELECT cho các view cần đọc. Không cần cấp quyền ghi fact hoặc quyền quản trị container cho Power BI. Các file truy vấn không chứa mật khẩu; thông tin xác thực được quản lý trong công cụ kết nối. Việc tạo schema mới không tự cấp quyền cho một tài khoản powerbi_reader đã tồn tại, nên cần admin cấp quyền phù hợp trước khi dùng tài khoản đó.

Sau refresh, đối chiếu distinct order, quantity và buyer amount của bi.fact_order_line với SQL có cùng điều kiện. Kiểm tra bộ lọc SKU thực sự làm thay đổi lượng bán, bộ lọc địa lý không loại nhóm chưa xác định ngoài ý muốn và tổng inventory là snapshot mới nhất. Đợt công việc hiện tại chưa dựng lại và refresh PBIX theo schema bi; báo cáo không dùng kết quả SQL để khẳng định dashboard đã nghiệm thu.

# 9 ĐÁNH GIÁ VÀ HƯỚNG PHÁT TRIỂN

## 9.1 Kết quả đạt được

Thiết kế đã công bố rõ grain thay vì dựa vào tên bảng. Schema báo cáo cung cấp khóa chung và giữ dòng chi tiết; số dư tồn và catalog lịch sử có mô hình riêng. Ràng buộc được đưa xuống PostgreSQL để chặn một số lỗi kể cả khi dữ liệu đi vào ngoài ETL. Migration có rehearsal, kiểm tra conservation và bằng chứng sau commit; các trường hợp sai dữ liệu được kiểm thử bằng fixture cô lập thay vì thử phá dữ liệu đang dùng.

## 9.2 Giới hạn thiết kế và dữ liệu

Giới hạn thứ nhất là nguồn tiền ở cấp dòng cần được đặc tả lại khi chuyển sang API thương mại nhiều SKU. Giới hạn thứ hai là lịch sử thay đổi dimension chưa có SCD Type 2 và fact đơn chưa giữ mọi phiên bản sự kiện. Giới hạn thứ ba là các dòng header xung đột còn được giữ để truy vết; migration không đủ căn cứ sửa hộ nguồn. Giới hạn thứ tư là coverage chưa được xác nhận, nên actual và accuracy có thể trống hợp lệ.

Ngoài ra, hiệu năng chưa được thử ở quy mô lớn hơn, tải đồng thời hay quá trình Power BI refresh đầy đủ. Không có bằng chứng mới cho thời gian phục hồi sau mất volume, phục hồi backup hoặc gián đoạn dài hạn của broker. Snapshot inventory chỉ tồn tại cho ngày có bản tin; chưa có quy trình đóng sổ cuối ngày bảo đảm snapshot đầy đủ mọi SKU. Uniqueness forecast còn cần kiểm tra trường hợp history_end_date NULL và các namespace model khác nhau trùng phiên bản.

## 9.3 Thứ tự hoàn thiện trước khi nộp

Ưu tiên trước hết là truy vết 28 mã đơn và đặc tả lại header so với line, sau đó xác nhận coverage theo ngày và nguồn. Tiếp theo, chạy lại một đợt dữ liệu có nhãn qua Kafka, lake, ETL và schema bi; thực hiện các kịch bản lỗi vận hành có ghi bằng chứng. Khi hợp đồng dữ liệu ổn định, dựng lại Power BI theo schema mới và đối soát sau refresh. Cuối cùng mới mở rộng NiFi để trực quan hóa và cân nhắc SCD Type 2, fact sự kiện hoặc materialized view khi có yêu cầu thực tế.

## 9.4 Minh bạch sử dụng AI

AI hỗ trợ rà soát mã, soạn migration và view, viết kiểm thử, thu thập kết quả chạy công cụ và biên soạn tài liệu. Số liệu trong báo cáo lấy từ các hiện vật có ngày kiểm tra, không tạo kết quả thực nghiệm thay cho dữ liệu đo. Sinh viên chịu trách nhiệm rà soát định nghĩa nghiệp vụ, tái lập kiểm thử, bổ sung minh chứng dashboard và xác nhận nội dung cuối cùng trước khi nộp.

# KẾT LUẬN

Kho dữ liệu phục vụ đồ án được tổ chức quanh grain nghiệp vụ, các khóa nhận diện và quy tắc tính số liệu. Việc bổ sung schema bi khắc phục hạn chế của mô hình báo cáo chỉ có summary ngày sàn, đồng thời giữ tương thích luồng ghi hiện tại. Migration đã được diễn tập, áp dụng và kiểm chứng bảo toàn các tổng được chọn; 81 trường hợp hồi quy liên quan đạt.

Một schema có thể triển khai và một tập dữ liệu đã đủ tin cậy để ra quyết định là hai mức đánh giá khác nhau. Các xung đột header, thời gian stock legacy và coverage vẫn phải được đóng bằng bằng chứng từ nguồn. Chính việc chỉ ra giới hạn và cung cấp đường đối chứng từ lý thuyết tới DDL, SQL và test giúp báo cáo có thể được kiểm tra và tiếp tục hoàn thiện.

# TÀI LIỆU THAM KHẢO

[1] Oracle. Introduction to Data Warehousing Concepts. https://docs.oracle.com/cd/E18283_01/server.112/e16579/concept.htm Truy cập ngày 01/11/2026.

[2] Kimball Group. Four Step Dimensional Design Process. https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/four-4-step-design-process/ Truy cập ngày 01/11/2026.

[3] Ralph Kimball. Fact Tables. https://www.kimballgroup.com/2008/11/fact-tables/ Truy cập ngày 01/11/2026.

[4] Microsoft Learn. Understand star schema and the importance for Power BI. https://learn.microsoft.com/en-us/power-bi/guidance/star-schema Truy cập ngày 01/11/2026.

[5] Microsoft Support. Database design basics. https://support.microsoft.com/en-us/access/database-design-basics Truy cập ngày 01/11/2026.

[6] PostgreSQL Global Development Group. PostgreSQL 16 Constraints. https://www.postgresql.org/docs/16/ddl-constraints.html Truy cập ngày 01/11/2026.

[7] Ralph Kimball. Slowly Changing Dimensions. https://www.kimballgroup.com/2008/08/slowly-changing-dimensions/ Truy cập ngày 01/11/2026.

[8] PostgreSQL Global Development Group. PostgreSQL 16 CREATE VIEW. https://www.postgresql.org/docs/16/sql-createview.html Truy cập ngày 01/11/2026.

[9] PostgreSQL Global Development Group. PostgreSQL 16 Indexes. https://www.postgresql.org/docs/16/indexes.html Truy cập ngày 01/11/2026.

[10] PostgreSQL Global Development Group. PostgreSQL 16 INSERT. https://www.postgresql.org/docs/16/sql-insert.html Truy cập ngày 01/11/2026.

[11] Apache Software Foundation. Apache Kafka Design Delivery Semantics. https://kafka.apache.org/40/design/design/ Truy cập ngày 01/11/2026.

[12] Microsoft Learn. PostgreSQL connector and table storage modes. https://learn.microsoft.com/en-us/power-query/connectors/postgresql Truy cập ngày 01/11/2026.

[13] Đinh Công Minh. Báo cáo tổng kết đồ án 2 về cơ sở dữ liệu không gian đa thời gian mẫu tham khảo bố cục. Tài liệu nội bộ.

Hiện vật dự án dùng để đối chứng: warehouse/ddl/01_star_schema.sql; warehouse/ddl/03_event_versions.sql; warehouse/ddl/04_integrity_constraints.sql; warehouse/reporting/bi_schema.sql; warehouse/etl/load.py; ingestion/consumer_inventory_to_postgres.py; serving/app/data_access.py; scripts/migrate_warehouse_schema.py; scripts/audit_warehouse_schema.py; scripts/check_reporting_schema.py; tests/test_warehouse_schema_v2.py. Các đường dẫn là tương đối từ thư mục gốc repository bàn giao.

Bằng chứng thực nghiệm nằm trong docs/07-bao-cao-do-an/evidence: warehouse_schema_audit_before_2026-11-01.json, warehouse_schema_audit_2026-11-01.json, warehouse_schema_rehearsal_2026-11-01.json, warehouse_schema_apply_2026-11-01.json, reporting_schema_checks_2026-11-01.json và pytest_warehouse_schema_regression_2026-11-01.xml. Đây là hiện vật nội bộ của đồ án, không phải tài liệu học thuật thay cho các nguồn lý thuyết ở trên.

# PHỤ LỤC A TỪ ĐIỂN DỮ LIỆU VẬT LÝ

Từ điển dưới đây được lấy từ metadata PostgreSQL sau migration. PK là khóa chính, FK là khóa ngoại, UQ là thành phần unique; NULL cho biết cột được phép thiếu ở mức cấu trúc. Kiểu SERIAL trong DDL thể hiện thành INTEGER cùng sequence ở metadata. Mô tả không thay thế các CHECK và unique nhiều cột, đã được trình bày ở phần thiết kế.

## dim_products



Cấu trúc dim_products



| Cột | Kiểu PostgreSQL | Ràng buộc cột | Ý nghĩa |

| --- | --- | --- | --- |

| product_key | INTEGER | NOT NULL PK | Khóa surrogate sản phẩm vận hành |

| sku | VARCHAR(50) | NOT NULL UQ | Mã sản phẩm trong phạm vi catalog của bảng |

| product_name | VARCHAR(500) | NULL | Tên sản phẩm hoặc tên nguồn lịch sử |

| category | VARCHAR(100) | NULL | Nhóm sản phẩm hiện hành |

| sub_category | VARCHAR(100) | NULL | Nhóm sản phẩm cấp dưới |

| unit_cost | NUMERIC(14, 2) | NULL | Tên legacy lưu original selling price; không phải giá vốn |

| weight_kg | NUMERIC(8, 3) | NULL | Khối lượng sản phẩm đơn vị kg |

| created_at | TIMESTAMP | NULL | Thời gian tạo bản ghi metadata |

| updated_at | TIMESTAMP | NULL | Thời gian cập nhật metadata |

## dim_shops



Cấu trúc dim_shops



| Cột | Kiểu PostgreSQL | Ràng buộc cột | Ý nghĩa |

| --- | --- | --- | --- |

| shop_key | INTEGER | NOT NULL PK | Khóa surrogate shop |

| shop_id | VARCHAR(50) | NULL UQ | Định danh shop trong sàn |

| shop_name | VARCHAR(255) | NOT NULL | Tên shop |

| platform | VARCHAR(20) | NOT NULL UQ | Nền tảng bán hàng |

| connection_id | VARCHAR(50) | NULL | Định danh kết nối nguồn nếu có |

| created_at | TIMESTAMP | NULL | Thời gian tạo bản ghi metadata |

## dim_geography



Cấu trúc dim_geography



| Cột | Kiểu PostgreSQL | Ràng buộc cột | Ý nghĩa |

| --- | --- | --- | --- |

| geo_key | INTEGER | NOT NULL PK | Khóa surrogate địa chỉ |

| state | VARCHAR(100) | NOT NULL UQ | Tỉnh hoặc thành phố |

| city | VARCHAR(100) | NULL UQ | Đơn vị địa lý cấp city theo nguồn |

| district | VARCHAR(100) | NULL UQ | Quận hoặc huyện theo nguồn |

| country | VARCHAR(10) | NULL | Mã quốc gia |

| region | VARCHAR(50) | NULL | Phân vùng phục vụ báo cáo |

| created_at | TIMESTAMP | NULL | Thời gian tạo bản ghi metadata |

## dim_dates



Cấu trúc dim_dates



| Cột | Kiểu PostgreSQL | Ràng buộc cột | Ý nghĩa |

| --- | --- | --- | --- |

| date_key | INTEGER | NOT NULL PK | Khóa surrogate ngày; không phải YYYYMMDD |

| full_date | DATE | NOT NULL UQ | Ngày lịch thực tế |

| day_of_week | SMALLINT | NULL | Chủ nhật 0 tới Thứ bảy 6 theo PostgreSQL |

| day_name | VARCHAR(20) | NULL | Tên thứ từ lịch PostgreSQL |

| week_of_year | SMALLINT | NULL | Tuần trong năm theo lịch SQL |

| month | SMALLINT | NULL | Tháng dạng số |

| month_name | VARCHAR(20) | NULL | Tên tháng |

| quarter | SMALLINT | NULL | Quý dạng số |

| year | SMALLINT | NULL | Năm |

| is_weekend | BOOLEAN | NULL | Cờ cuối tuần |

| is_holiday | BOOLEAN | NULL | Cờ ngày lễ; cần quản trị danh mục |

| is_mega_sale | BOOLEAN | NULL | Cờ ngày đôi theo quy tắc hiện tại |

| created_at | TIMESTAMP | NULL | Thời gian tạo bản ghi metadata |

## dim_payment



Cấu trúc dim_payment



| Cột | Kiểu PostgreSQL | Ràng buộc cột | Ý nghĩa |

| --- | --- | --- | --- |

| payment_key | INTEGER | NOT NULL PK | Khóa phương thức thanh toán |

| payment_method | VARCHAR(50) | NOT NULL UQ | Tên phương thức thanh toán |

| is_cod | BOOLEAN | NULL | Cờ thanh toán khi nhận hàng |

| created_at | TIMESTAMP | NULL | Thời gian tạo bản ghi metadata |

## dim_carriers



Cấu trúc dim_carriers



| Cột | Kiểu PostgreSQL | Ràng buộc cột | Ý nghĩa |

| --- | --- | --- | --- |

| carrier_key | INTEGER | NOT NULL PK | Khóa đơn vị vận chuyển |

| carrier_name | VARCHAR(100) | NOT NULL UQ | Tên đơn vị vận chuyển |

| created_at | TIMESTAMP | NULL | Thời gian tạo bản ghi metadata |

## fact_orders



Cấu trúc fact_orders



| Cột | Kiểu PostgreSQL | Ràng buộc cột | Ý nghĩa |

| --- | --- | --- | --- |

| order_fact_id | BIGINT | NOT NULL PK | Khóa surrogate dòng fact đơn |

| product_key | INTEGER | NOT NULL FK | Khóa surrogate sản phẩm vận hành |

| shop_key | INTEGER | NULL FK | Khóa surrogate shop |

| geo_key | INTEGER | NULL FK | Khóa surrogate địa chỉ |

| date_key | INTEGER | NOT NULL FK | Khóa surrogate ngày; không phải YYYYMMDD |

| payment_key | INTEGER | NULL FK | Khóa phương thức thanh toán |

| carrier_key | INTEGER | NULL FK | Khóa đơn vị vận chuyển |

| order_id | VARCHAR(50) | NOT NULL | Mã đơn gốc; chỉ duy nhất khi xét thêm sàn |

| platform | VARCHAR(20) | NOT NULL | Nền tảng bán hàng |

| order_status | VARCHAR(30) | NOT NULL | Trạng thái chuẩn hóa hiện hành |

| is_cancelled | BOOLEAN | NOT NULL | Cờ loại hủy theo quy tắc ETL |

| cancel_reason | VARCHAR(255) | NULL | Lý do hủy nếu có |

| quantity | INTEGER | NOT NULL | Số đơn vị ở một dòng hàng; lớn hơn 0 |

| original_price | NUMERIC(14, 2) | NULL | Giá bán gốc đơn vị theo nguồn |

| discounted_price | NUMERIC(14, 2) | NULL | Giá bán sau giảm nếu được cung cấp |

| subtotal | NUMERIC(14, 2) | NULL | Tổng tiền hàng theo schema nguồn |

| buyer_total_amount | NUMERIC(14, 2) | NULL | Số tiền người mua theo nguồn; cần xác nhận grain khi tích hợp thật |

| seller_discount | NUMERIC(14, 2) | NULL | Giảm giá từ người bán |

| platform_discount | NUMERIC(14, 2) | NULL | Giảm giá từ nền tảng |

| voucher_total | NUMERIC(14, 2) | NULL | Tổng giá trị voucher theo chuẩn hóa |

| commission_fee | NUMERIC(14, 2) | NULL | Phí hoa hồng nguồn |

| service_fee | NUMERIC(14, 2) | NULL | Phí dịch vụ nguồn |

| transaction_fee | NUMERIC(14, 2) | NULL | Phí giao dịch nguồn |

| shipping_fee | NUMERIC(14, 2) | NULL | Phí vận chuyển theo nguồn |

| original_shipping | NUMERIC(14, 2) | NULL | Phí vận chuyển trước điều chỉnh nếu có |

| tax_amount | NUMERIC(14, 2) | NULL | Thuế theo nguồn |

| create_time | TIMESTAMP | NOT NULL | Thời gian tạo đơn đã quy về giờ nghiệp vụ |

| pay_time | TIMESTAMP | NULL | Mốc thanh toán theo nguồn |

| shipped_time | TIMESTAMP | NULL | Mốc giao vận theo nguồn |

| completed_time | TIMESTAMP | NULL | Mốc hoàn tất theo nguồn |

| cancel_time | TIMESTAMP | NULL | Mốc hủy theo nguồn |

| loaded_at | TIMESTAMP | NULL | Thời gian ghi warehouse; không thay event time |

| source_line_id | VARCHAR(128) | NOT NULL | Định danh dòng hàng để upsert |

| source_updated_at | TIMESTAMP | NULL | Phiên bản thời gian dùng chống cập nhật lùi |

## fact_inventory_daily



Cấu trúc fact_inventory_daily



| Cột | Kiểu PostgreSQL | Ràng buộc cột | Ý nghĩa |

| --- | --- | --- | --- |

| inventory_fact_id | BIGINT | NOT NULL PK | Khóa surrogate snapshot tồn kho |

| product_key | INTEGER | NOT NULL FK | Khóa surrogate sản phẩm vận hành |

| shop_key | INTEGER | NULL FK | Khóa surrogate shop |

| date_key | INTEGER | NOT NULL FK | Khóa surrogate ngày; không phải YYYYMMDD |

| stock_on_hand | INTEGER | NOT NULL | Số dư tồn kho trung tâm không âm |

| safety_stock | INTEGER | NULL | Giá trị nguồn nếu có; policy BI được tính riêng |

| reorder_point | INTEGER | NULL | Giá trị nguồn nếu có; policy BI được tính riêng |

| loaded_at | TIMESTAMP | NULL | Thời gian ghi warehouse; không thay event time |

| snapshot_at | TIMESTAMP | NULL | Thời điểm sự kiện snapshot; NULL nếu legacy chưa biết |

## fact_forecast_predictions



Cấu trúc fact_forecast_predictions



| Cột | Kiểu PostgreSQL | Ràng buộc cột | Ý nghĩa |

| --- | --- | --- | --- |

| forecast_id | BIGINT | NOT NULL PK | Khóa surrogate forecast issue |

| product_key | INTEGER | NOT NULL FK,UQ | Khóa surrogate sản phẩm vận hành |

| target_date | DATE | NOT NULL UQ | Ngày mục tiêu dự báo |

| predicted_quantity | NUMERIC(14, 3) | NOT NULL | Số lượng dự báo không âm |

| model_name | VARCHAR(255) | NOT NULL UQ | Tên mô hình |

| model_version | VARCHAR(100) | NOT NULL UQ | Phiên bản mô hình |

| model_source | VARCHAR(50) | NOT NULL | Nguồn artifact hoặc registry của mô hình |

| history_source | VARCHAR(50) | NOT NULL | Nguồn lịch sử phục vụ inference |

| history_end_date | DATE | NULL UQ | Ngày kết thúc lịch sử dùng dự báo |

| forecast_generated_at | TIMESTAMP | NOT NULL | Thời điểm tạo issue có múi giờ |

| data_origin | TEXT | NOT NULL UQ | Miền xuất xứ dữ liệu |

| source_datasets | JSONB | NOT NULL UQ | Tập fingerprint dataset của lịch sử dự báo |

## fact_historical_order_lines



Cấu trúc fact_historical_order_lines



| Cột | Kiểu PostgreSQL | Ràng buộc cột | Ý nghĩa |

| --- | --- | --- | --- |

| historical_line_id | BIGINT | NOT NULL PK | Khóa surrogate dòng nguồn lịch sử |

| source_dataset | VARCHAR(80) | NOT NULL UQ | Định danh hoặc fingerprint dataset |

| source_record_id | VARCHAR(80) | NOT NULL UQ | Định danh dòng bên trong dataset |

| data_origin | VARCHAR(80) | NOT NULL | Miền xuất xứ dữ liệu |

| source_order_id | VARCHAR(128) | NULL | Mã đơn nguồn đã khử định danh nếu có |

| platform | VARCHAR(20) | NOT NULL UQ | Nền tảng bán hàng |

| sku | VARCHAR(128) | NOT NULL | Mã sản phẩm trong phạm vi catalog của bảng |

| product_name | VARCHAR(500) | NULL | Tên sản phẩm hoặc tên nguồn lịch sử |

| order_status | VARCHAR(40) | NOT NULL | Trạng thái chuẩn hóa hiện hành |

| is_cancelled | BOOLEAN | NOT NULL | Cờ loại hủy theo quy tắc ETL |

| quantity | INTEGER | NOT NULL | Số đơn vị ở một dòng hàng; lớn hơn 0 |

| create_time | TIMESTAMP | NOT NULL | Thời gian tạo đơn đã quy về giờ nghiệp vụ |

| loaded_at | TIMESTAMP | NOT NULL | Thời gian ghi warehouse; không thay event time |

## warehouse_daily_coverage



Cấu trúc warehouse_daily_coverage



| Cột | Kiểu PostgreSQL | Ràng buộc cột | Ý nghĩa |

| --- | --- | --- | --- |

| data_origin | TEXT | NOT NULL PK | Miền xuất xứ dữ liệu |

| source_dataset | TEXT | NOT NULL PK | Định danh hoặc fingerprint dataset |

| full_date | DATE | NOT NULL PK | Ngày lịch thực tế |

| confirmed_at | TIMESTAMP | NOT NULL | Thời điểm xác nhận coverage đầy đủ |

# PHỤ LỤC B TRUY VẤN TÁI LẬP

## B.1 Đối chiếu số đơn và giá trị theo grain dòng hàng

```sql
SELECT COUNT(*) AS lines,
       COUNT(DISTINCT order_business_key) AS orders,
       SUM(quantity) AS units,
       SUM(buyer_amount) AS buyer_amount
FROM bi.fact_order_line
WHERE is_cancelled = FALSE;
```

Ở snapshot 01/11/2026, kỳ vọng 118.688 dòng, 118.650 đơn, 286.178 đơn vị và 72.953.914.380 tiền người mua. Đây là số liệu theo quy tắc không hủy của dataset hiện tại, không phải doanh thu thương mại đã xác minh.

## B.2 Phân tích theo sản phẩm và địa lý

```sql
SELECT p.category, COALESCE(g.region, 'UNKNOWN') AS region,
       SUM(f.quantity) AS units,
       COUNT(DISTINCT f.order_business_key) AS orders
FROM bi.fact_order_line f
JOIN bi.dim_product p USING (product_key)
LEFT JOIN bi.dim_geography g USING (geo_key)
WHERE NOT f.is_cancelled
GROUP BY p.category, COALESCE(g.region, 'UNKNOWN');
```

LEFT JOIN địa lý giữ các dòng chưa biết geo. Các count theo nhóm có thể giao nhau nếu đơn nhiều dòng thuộc nhiều nhóm; dùng truy vấn B.1 khi cần tổng toàn bộ.

## B.3 Tồn mới nhất và cờ chất lượng

```sql
SELECT COUNT(*) AS skus, SUM(current_stock) AS stock
FROM bi.inventory_latest;

SELECT order_business_key, line_count, shop_count,
       geography_count, cancellation_state_count
FROM bi.quality_order_header_conflicts;

SELECT COUNT(*) AS legacy_unknown_time
FROM bi.fact_inventory_daily
WHERE NOT snapshot_time_known;
```

## B.4 Điều kiện kiểm tra forecast và coverage

```sql
SELECT data_origin, actual_complete, COUNT(*) AS rows
FROM bi.forecast_evaluation
GROUP BY data_origin, actual_complete;

SELECT data_origin, source_dataset, COUNT(*) AS confirmed_days
FROM public.warehouse_daily_coverage
GROUP BY data_origin, source_dataset;
```

Nếu bảng coverage không có dòng, không kết luận ngày không có giao dịch là zero sales đã xác nhận. Không chạy lệnh xác nhận coverage hàng loạt chỉ để làm metric có số.

## B.5 Tái lập audit và kiểm thử

```sql
python scripts/audit_warehouse_schema.py
python scripts/migrate_warehouse_schema.py
python scripts/check_reporting_schema.py

# Khi cần áp dụng một phiên bản migration đã rà soát
python scripts/migrate_warehouse_schema.py --apply
```

Lệnh migration không có --apply chỉ diễn tập và rollback. Kiểm thử PostgreSQL dùng cờ môi trường RUN_WAREHOUSE_SCHEMA_TESTS=1 và RUN_POWERBI_SQL_TESTS=1; chạy lại nhóm test đã ghi trong JUnit để đối chiếu cùng phạm vi. Không dùng --drop-first hoặc down -v để tái lập báo cáo này.
