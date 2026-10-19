# Phần 1 — Bài toán và dữ liệu đầu vào

Ngày đối chiếu: 16/10/2026. Trạng thái: bản thảo dựa trên mã nguồn và artifact cục bộ; chưa nghiệm thu lại dịch vụ đang chạy. Cấu trúc chương sẽ điều chỉnh khi có đề cương của trường.

## 1. Bài toán nghiên cứu

Đồ án xây dựng hệ thống thu thập và xử lý dữ liệu bán hàng đa kênh, kết hợp quy trình MLOps để dự báo lượng bán theo sản phẩm và hỗ trợ quyết định bổ sung tồn kho. Bối cảnh thực nghiệm là một doanh nghiệp bán lẻ mô phỏng có gian hàng Shopee và TikTok Shop, sử dụng kho trung tâm.

Trong kịch bản này, dữ liệu đơn hàng giữa hai kênh có cấu trúc khác nhau, trong khi người quản lý cần một góc nhìn thống nhất về lượng bán và tồn kho. Hệ thống cần bảo toàn dòng hàng, xử lý dữ liệu trùng hoặc không hợp lệ, tổng hợp lịch sử theo ngày và cung cấp đầu ra dự báo có thể truy vết đến dữ liệu và phiên bản mô hình.

Câu hỏi nghiên cứu chính là: làm thế nào xây dựng một luồng dữ liệu có thể kiểm chứng từ sự kiện đầu vào đến dự báo và cảnh báo tồn kho, đồng thời quản lý được chất lượng dữ liệu và vòng đời mô hình?

## 2. Mục tiêu và phạm vi

Các mục tiêu gồm:

1. Thu nhận liên tục sự kiện đơn hàng và snapshot tồn kho của doanh nghiệp mô phỏng.
2. Lưu dữ liệu nguồn, chuẩn hóa và nạp vào kho dữ liệu phục vụ phân tích và đối soát.
3. Xây dựng chuỗi lượng bán theo SKU/ngày và tạo đặc trưng cho dự báo.
4. Huấn luyện, đánh giá, quản lý và triển khai phiên bản mô hình qua API.
5. Hỗ trợ quản trị tồn kho bằng phân nhóm ABC/XYZ, tồn kho an toàn, điểm đặt hàng lại và cảnh báo.
6. Theo dõi trạng thái hệ thống, dữ liệu và mô hình; cung cấp đầu ra cho báo cáo BI.

Khoảng dự báo 7–14 ngày là mục tiêu sử dụng nghiệp vụ. Trong mã tạo đặc trưng hiện tại, nhãn huấn luyện là lượng bán ngày tiếp theo; cách mở rộng nhiều bước và chất lượng theo từng horizon sẽ được kiểm tra ở phần Machine Learning.

Phạm vi thực nghiệm gồm simulator, dữ liệu lịch sử CSV và các thành phần triển khai trong repository. Chưa có căn cứ tại bước này để khẳng định hệ thống đã kết nối tài khoản kinh doanh thật, tự động phát hành đơn mua hàng, tối ưu đa kho hoặc làm giảm chi phí tồn kho trong vận hành thực tế.

## 3. Người sử dụng và đầu ra

| Người sử dụng | Quyết định được hỗ trợ | Đầu ra |
|---|---|---|
| Quản lý kinh doanh | Theo dõi tình hình bán hàng đa kênh | Báo cáo đơn, lượng bán và chỉ tiêu tài chính có định nghĩa rõ |
| Nhân viên kế hoạch | Ước lượng lượng hàng cần chuẩn bị | Dự báo lượng bán theo SKU/ngày |
| Quản lý kho | Ưu tiên sản phẩm cần bổ sung | Snapshot tồn kho, Safety Stock, Reorder Point và cảnh báo |
| Nhân sự dữ liệu/MLOps | Phát hiện lỗi và kiểm soát triển khai | Audit dữ liệu, metrics, phiên bản model và trạng thái phục vụ |

## 4. Các nguồn dữ liệu

### 4.1. Dữ liệu đơn hàng mô phỏng

`data_simulator/data_simulator.py` định nghĩa doanh nghiệp `MOCK-CORP-VN`, hai gian hàng và kho trung tâm `WH-MOCK-CENTRAL`. Danh mục hiện tại có **20 SKU**. Các thông số sản phẩm gồm giá, nhóm hàng, tồn ban đầu, ngưỡng tồn, lượng bổ sung, thời gian chờ và trọng số chọn sản phẩm.

Đếm trực tiếp bằng `dataclasses.fields` cho kết quả:

| Kiểu sự kiện | Số trường khai báo hiện tại | Ý nghĩa |
|---|---:|---|
| `ShopeeOrderEvent` | 61 | Payload đơn hàng Shopee do simulator tạo |
| `TikTokOrderEvent` | 70 | Payload đơn hàng TikTok do simulator tạo |

Đây là số trường dataclass hiện có, không phải xác nhận số cột trong bản xuất chính thức của sàn. Tài liệu cũ ghi 84/71 cột và 19 SKU không khớp với bản triển khai đang kiểm tra.

Simulator chọn sản phẩm theo trọng số và mô phỏng số lượng, trạng thái đơn, giảm giá, phí và vận động tồn kho. Nhịp phát sinh đơn chịu tác động của giờ, ngày trong tuần, flash sale, ngày đôi, payday và xu hướng theo thời gian. Hàm `orders_per_second` tính `max(1, round(base_rate × seasonal_multiplier))`; hàm này không lấy mẫu Poisson. Các hệ số là giả định thực nghiệm của simulator, chưa phải tham số ước lượng từ thị trường.

Producer gửi đơn hàng lên `ecom.orders.raw` và snapshot tồn kho lên `inventory.logs`. Chu kỳ snapshot mặc định trong mã là 60 giây, có thể thay bằng cấu hình môi trường. Tồn kho được giữ trong bộ nhớ tiến trình simulator; tính bền vững và phục hồi sẽ được kiểm tra ở phần ingestion.

### 4.2. Dữ liệu lịch sử CSV

`scripts/import_public_historical_orders.py` đọc CSV qua danh sách cột cho phép. Shopee cho phép 6 tên trường; TikTok cho phép 7 tên trường, trong đó `item_status` được ưu tiên khi cột này tồn tại. Importer không đọc các cột khách hàng, thông tin liên hệ, địa chỉ hoặc tracking vào bảng lịch sử.

| Thông tin | Shopee | TikTok |
|---|---|---|
| Mã đơn nguồn | `order_sn` | `order_id` |
| Mã sản phẩm | `item_sku` | `seller_sku` |
| Tên sản phẩm | `item_name` | `product_name` |
| Thời điểm tạo | `create_time` | `created_time` |
| Số lượng | `quantity` | `quantity` |
| Trạng thái | `order_status` | `item_status` nếu có, nếu không dùng `order_status` |

Điều kiện nhận dòng gồm timestamp đọc được, SKU không rỗng, quantity là số nguyên từ 1 đến 100. Mã đơn nguồn rỗng được lưu thành `None`; không phải điều kiện loại dòng trong importer hiện tại. Các trạng thái `CANCELLED`, `IN_CANCEL`, `CANCEL_REQUESTED` được đánh dấu hủy.

Lịch sử được lưu riêng trong `Fact_Historical_Order_Lines` với origin `public_anonymized_historical_csv`. Định danh nạp gồm dataset theo SHA-256 của tệp, platform và vị trí dòng. Cách này tránh dựa duy nhất vào mã đơn đã khử định danh có thể bị tái sử dụng. Nạp lại cùng tệp dùng cơ chế upsert; thay nội dung tệp làm thay fingerprint, vì vậy cần kiểm soát việc nạp các bản xuất chồng lấn.

Timestamp được parse với `utc=True` rồi đổi về múi giờ nghiệp vụ, mặc định `Asia/Ho_Chi_Minh`. Cần xác nhận múi giờ thực của CSV, đặc biệt nếu timestamp nguồn không kèm offset, trước khi kết luận ngày nghiệp vụ chính xác.

Nhãn origin trong mã là thông tin truy vết kỹ thuật. Chưa xác minh nguồn xuất bản, giấy phép, phạm vi thu thập hoặc mức độ đại diện của CSV tại bước này.

### 4.3. Snapshot tồn kho

Snapshot được tạo từ trạng thái tồn kho của simulator và phát qua topic riêng. Đây là nguồn kiểm tra luồng kỹ thuật giữa bán hàng, biến động tồn và cảnh báo. Snapshot mô phỏng chưa chứng minh số tồn ngoài doanh nghiệp thực tế.

## 5. Định nghĩa đại lượng dự báo

Trong `TimeSeriesFeatureExtractor.aggregate_daily`, `daily_demand` là tổng `quantity` của các dòng không bị đánh dấu hủy, nhóm theo SKU và ngày tạo đơn. Đây là lượng đặt hàng chưa hủy theo dữ liệu quan sát; không đồng nhất với lượng đã giao hoặc nhu cầu tiềm ẩn của thị trường.

Định nghĩa thực nghiệm:

`y(sku, ngày) = tổng quantity của các dòng thuộc SKU/ngày, sau khi loại dòng hủy`.

Hàm tạo grid SKU × ngày và điền 0 vào ngày không có dòng bán. Việc điền 0 chỉ phản ánh đúng lượng bán khi dữ liệu nguồn đã được nạp đủ; ngày thiếu dữ liệu không nên tự diễn giải thành ngày không có nhu cầu. Cơ chế coverage sẽ được đối chiếu ở phần ETL/feature engineering.

Lượng bán quan sát cũng có thể thấp hơn nhu cầu tiềm ẩn khi hết hàng. Vì vậy báo cáo không suy luận rằng dự báo lượng bán đã giải quyết đầy đủ bài toán nhu cầu bị che khuất bởi stockout.

## 6. Kiểm kê artifact cục bộ ngày 16/10/2026

Đã đọc trực tiếp hai tệp Parquet bằng pandas với thư viện từ `.venv/Lib/site-packages`:

| Artifact | Số dòng | Số cột | Số SKU | Số ngày phân biệt | Khoảng ngày |
|---|---:|---:|---:|---:|---|
| `data/daily_history.parquet` | 49.032 | 9 | 216 | 227 | 28/12/2025–11/08/2026 |
| `data/features_daily.parquet` | 42.768 | 51 | 216 | 198 | 25/01/2026–10/08/2026 |

Đây là đặc điểm của artifact đang lưu trên đĩa. Chưa đối soát lại với CSV gốc, warehouse đang chạy hoặc lần huấn luyện sinh ra chúng. Số ngày trong artifact không chứng minh nguồn đã có dữ liệu đầy đủ ở mọi ngày.

216 SKU trong artifact lịch sử và 20 SKU trong simulator là hai phạm vi dữ liệu khác nhau. Cần kiểm tra độ giao nhau và quy tắc ánh xạ trước khi tuyên bố model lịch sử phục vụ được toàn bộ SKU mô phỏng.

Số cột của feature artifact gồm cả định danh và nhãn; không dùng trực tiếp con số 51 làm số feature đầu vào model. Manifest cục bộ khai báo 43 input features và origin lịch sử CSV; tính khớp giữa các artifact sẽ được kiểm tra ở phần ML.

## 7. Kết quả kiểm thử tại bước này

Lệnh đã chạy: `python -m unittest discover -s tests -p test_data_simulator.py -v`.

Kết quả: **17 test được chạy, 16 đạt, 1 không đạt**, khoảng 1,01 giây. Test lỗi là `test_generate_shopee_event`, tại điều kiện `buyer_total_amount > 0`.

Mã tính phí dùng `max(0, subtotal - discounts - vouchers)`, nên số tiền bằng 0 là đầu ra có thể xảy ra. Tái hiện bằng `random.seed(128)`, simulator `shopee_ratio=0.6`, `cancel_rate=0.15`, gọi `_generate_shopee_event` với thời điểm `2026-09-24T10:00:00+00:00`: SKU `CL-SS-S24`, quantity 1, giá gốc 35.000, số tiền người mua bằng 0.

Kết luận giới hạn: hợp đồng giữa test và simulator chưa thống nhất về thanh toán bằng 0. Chưa sửa mã hoặc test trong bước viết báo cáo này. Lỗi test không tự chứng minh pipeline end-to-end ngừng hoạt động, nhưng cần xử lý trước khi ghi nhận simulator đạt toàn bộ kiểm thử.

## 8. Các điểm cần chốt khi nghiệm thu

| ID | Nội dung | Trạng thái |
|---|---|---|
| D-01 | Đồng bộ số SKU và số trường payload trong tài liệu | Đã xác nhận số thực tế; tài liệu cũ còn cần chỉnh |
| D-02 | Thống nhất số tiền người mua bằng 0 có hợp lệ trong mô phỏng hay không | Mở; đã có trường hợp tái hiện |
| D-03 | Xác minh nguồn gốc và múi giờ CSV, thống kê lại tệp nguồn | Chưa thực hiện |
| D-04 | Đối soát artifact với warehouse, coverage và lineage | Chuyển phần ETL/ML |
| D-05 | Kiểm tra SKU giữa lịch sử và simulator, cách phục vụ SKU chưa có lịch sử | Chuyển phần ML/serving |
| D-06 | Đánh giá chất lượng theo thời gian và horizon | Chuyển phần ML |

## 9. Câu hỏi bảo vệ và hướng trả lời

**Vì sao cần cả simulator và lịch sử CSV?** Simulator cho phép kiểm soát kịch bản streaming, tồn kho và lỗi; CSV cung cấp một nguồn lịch sử riêng cho thử nghiệm dự báo. Cần trình bày phạm vi đánh giá riêng và truy vết nguồn khi train/serve.

**Hệ thống có phải xử lý mọi thành phần theo thời gian thực?** Ingestion nhận sự kiện liên tục; ETL, tạo đặc trưng và huấn luyện là các tác vụ riêng. Độ trễ cập nhật dự báo cần đo trên runtime.

**Dự báo nhu cầu ở đây đo cái gì?** Biến mục tiêu hiện tại là tổng số lượng đặt theo SKU/ngày sau loại dòng hủy. Chưa có bước ước lượng phần nhu cầu bị mất do hết hàng.

**Pipeline chạy end-to-end có đồng nghĩa mô hình tốt?** Luồng chạy thành công xác nhận tích hợp kỹ thuật. Chất lượng dự báo cần đánh giá trên dữ liệu ngoài mẫu theo thời gian và so sánh với baseline.

## 10. Minh bạch sử dụng AI

AI hỗ trợ đọc và đối chiếu mã nguồn, chạy kiểm tra cục bộ, kiểm kê artifact, chỉ ra chênh lệch tài liệu và soạn bản thảo. Phần sinh viên tự thực hiện, nguồn dữ liệu và các quyết định nghiệp vụ cần được sinh viên xác nhận để ghi nhận đúng đóng góp; tài liệu này không tự quy kết tác giả của mã nguồn hiện có.

## 11. Căn cứ nội bộ

- `data_simulator/data_simulator.py`: danh mục, schema, mô hình doanh nghiệp, quy tắc sinh dữ liệu và tồn kho.
- `ingestion/producer.py`: topic và chu kỳ snapshot mặc định.
- `scripts/import_public_historical_orders.py`: allowlist, validation, timestamp, fingerprint và upsert lịch sử.
- `ml/features/time_series_features.py`: tổng hợp `daily_demand` và nhãn ngày tiếp theo.
- `ml/features/feature_pipeline.py`: nguồn lịch sử và quy trình tạo đặc trưng.
- `tests/test_data_simulator.py`: các kiểm thử đã chạy tại bước này.
- Hai tệp Parquet và `data/model_manifest.json`: hiện vật cục bộ đã đọc, chưa thay thế xác nhận runtime.
