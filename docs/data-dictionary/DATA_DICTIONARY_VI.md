# Data dictionary — Đơn hàng và tồn kho thương mại điện tử

Ngày lập: 20/10/2026. Phạm vi: hợp đồng dữ liệu cho dự án streaming-mlops-ecommerce; dùng cho thu thập thật và giả lập. Đây là thiết kế tích hợp kèm đối chiếu mã hiện tại, không phải cam kết tất cả các trường đều được API trả về.

## 1. Cách đọc và mức độ xác nhận

- `simulator_shopee`, `simulator_tiktok`: tên trường giả lập hoặc biểu thức trong ETL hiện tại; KHÔNG phải JSON path API. Với tồn kho, cả hai cột cùng trỏ luồng kho chung của doanh nghiệp.
- `real_source_candidate`: nơi dự kiến thu thập. Cần kiểm tra phiên bản API, thị trường, quyền ứng dụng, quyền shop và phản hồi thực tế trước khi chốt ánh xạ.
- `Bắt buộc`: điều kiện luồng tương ứng; `Cần cho...`: yêu cầu nghiệp vụ đề xuất; `Tùy chọn`: có thể thiếu; `Dẫn xuất`: hệ thống tính.
- Trường không biết phải phân biệt với giá trị 0. Chính sách mong muốn ở bảng không đồng nghĩa mã hiện tại đã thực hiện.
- Kiểu decimal là kiểu đích đề xuất cho tiền; simulator hiện dùng float. Mã định danh luôn lưu chuỗi. Thời gian chuẩn UTC, ngày nghiệp vụ theo Asia/Ho_Chi_Minh.

## 2. Đơn vị bản ghi và khóa

- Đơn hàng: một bản ghi chuẩn là một dòng hàng của một đơn tại một lần cập nhật. Khóa nghiệp vụ đề xuất: (platform, shop_id, order_id, source_line_id). Giữ platform SKU ID để ánh xạ về SKU nội bộ; không mặc định cùng mã trên hai sàn là cùng sản phẩm.
- TikTok phân biệt order, SKU và order line item; có thể nhiều item cùng SKU. Cần thống nhất gom nhóm trước khi tính quantity. Xem [Order API overview](https://partner.tiktokshop.com/docv2/page/order-api-overview).
- Tồn kho snapshot: khóa nguồn đề xuất (warehouse_id, sku, thời điểm chụp). Consumer hiện tại gom còn một bản mới nhất mỗi SKU mỗi ngày; bảng hiện chưa phân biệt kho.
- Biến động kho: một bản ghi cho một giao dịch, khóa (hệ thống nguồn, movement_id). Có dataclass và logic giả lập; không mặc định luồng này đã được nạp đầy đủ vào kho dữ liệu.
- Tổng tiền cấp đơn không được cộng lặp theo số dòng hàng. Nên tách header đơn và line hoặc phân bổ có quy tắc, lưu dấu phương pháp phân bổ.

## 3. Cơ sở API và giới hạn đối chiếu

Đã đọc tài liệu chính thức TikTok về khả năng lấy danh sách/chi tiết đơn và nhận webhook. Danh sách trạng thái API có ON_HOLD và PARTIALLY_SHIPPING mà validator dự án hiện chưa chấp nhận. Cần bổ sung ánh xạ trước khi đưa dữ liệu thật vào luồng.

Nguồn chính thức:
- [TikTok Order API overview](https://partner.tiktokshop.com/docv2/page/order-api-overview).
- [TikTok Get Order List 202309](https://partner.tiktokshop.com/docv2/page/get-order-list-202309).
- [TikTok Common parameters](https://partner.tiktokshop.com/docv2/page/common-parameters): yêu cầu token cấp quyền shop/seller.
- [TikTok Get Order Detail 202309](https://partner.tiktokshop.com/docv2/page/get-order-detail-202309): trang chi tiết cần JavaScript; chưa kiểm chứng đầy đủ trường phản hồi trong phiên này.
- [Shopee Get Order Detail](https://open.shopee.com/documents/v2/v2.order.get_order_detail): trang chính thức không truy cập được qua công cụ trong phiên này. Các tên Shopee trong bảng là trường simulator, chưa được chứng nhận là đường dẫn API hiện hành.

Không có tài khoản shop được kết nối và không gọi API thật trong công việc này. Phí quyết toán, tồn vật lý, hàng nhập mở, tồn an toàn và lead time cần thêm API tài chính, WMS/ERP hoặc cấu hình doanh nghiệp. Không suy chúng chỉ từ API đơn hàng. Trường người mua có thể bị che hoặc giới hạn quyền; chỉ thu thập khi cần nghiệp vụ.

## 4. Danh mục trường chuẩn

### orders

| Trường | Kiểu đích | Ý nghĩa / Ví dụ | Shopee giả lập | TikTok giả lập | Nguồn thực tế ứng viên | Yêu cầu | Kiểm tra / khi thiếu |
|---|---|---|---|---|---|---|---|
| platform | string | Nền tảng nguồn; VD: shopee | _platform | _platform | Ngữ cảnh kết nối API | Bắt buộc | Chỉ shopee hoặc tiktok; không suy đoán chỉ từ tên cột |
| shop_id | string | Mã gian hàng; VD: 10000001 | shop_id | shop_name (fallback hiện tại) | Thông tin shop hoặc ngữ cảnh xác thực | Cần cho vận hành thật | Dùng ID ổn định; không dùng tên shop làm khóa lâu dài |
| order_id | string | Mã đơn nguồn; VD: SPE261020000001 | order_sn | order_id | API đơn hàng | Bắt buộc | Không trống; giữ dạng chuỗi |
| source_line_id | string | Định danh dòng hàng nguồn; VD: LINE-001 | Hệ thống suy ra | Hệ thống suy ra | ID dòng hàng từ API hoặc khóa tổng hợp | Cần cho nhiều dòng hàng | Không gộp hai dòng cùng SKU nhưng khác giá; giữ ID nguồn nếu có |
| sku | string | Mã SKU nội bộ dùng chung hai sàn; VD: OL-IP15PM | item_sku | seller_sku | API đơn hàng và bảng ánh xạ SKU | Bắt buộc | Thiếu seller SKU: ánh xạ từ platform SKU ID; không ghép chỉ theo tên |
| product_name | string | Tên sản phẩm; VD: Ốp lưng iPhone | item_name | product_name | API đơn hàng hoặc sản phẩm | Tùy chọn | Thiếu: dùng SKU để hiển thị |
| category | string | Ngành hàng chuẩn; VD: Phụ kiện | model_name | Không sinh trực tiếp | API sản phẩm hoặc danh mục nội bộ | Tùy chọn | model_name của simulator đang chứa category; API thật không được mặc định như vậy |
| order_status | string | Trạng thái đơn đã chuẩn hóa; VD: COMPLETED | order_status | order_status | API đơn hàng hoặc webhook | Bắt buộc | Giữ thêm trạng thái gốc; trạng thái mới chưa ánh xạ đưa vào cách ly |
| quantity | integer | Số đơn vị của dòng hàng; VD: 2 | quantity | quantity | Dòng hàng API; có thể phải đếm hoặc cộng item | Bắt buộc | Số nguyên 1..100 theo validator hiện tại; đơn sỉ cần cấu hình lại |
| create_time | timestamp UTC | Thời điểm tạo đơn; VD: 2026-10-20T05:00:00Z | create_time | created_time | API đơn hàng | Bắt buộc | Chuyển epoch giây hoặc mili giây đúng đơn vị; ngày kinh doanh Asia/Ho_Chi_Minh |
| source_updated_at | timestamp UTC | Thời điểm cập nhật tại nguồn; VD: 2026-10-20T06:00:00Z | update_time | updated_time hoặc updatedAt | API hoặc webhook; fallback từ mốc vòng đời | Cần khi cập nhật trạng thái | Thiếu: không thể bảo đảm thứ tự sự kiện bằng thời điểm nhận |
| pay_time | timestamp UTC | Thời điểm thanh toán; VD: 2026-10-20T05:05:00Z | pay_time | paid_time | API đơn hàng nếu cung cấp | Theo trạng thái | NULL khi chưa biết hoặc chưa phát sinh; không thay bằng thời điểm tạo |
| shipped_time | timestamp UTC | Thời điểm bàn giao vận chuyển; VD: 2026-10-21T05:00:00Z | shipped_time | shipped_time | API đơn hàng hoặc logistics | Theo trạng thái | Không tự điền ngày giả lập khi nguồn không có |
| completed_time | timestamp UTC | Thời điểm hoàn tất; VD: 2026-10-25T05:00:00Z | completed_time | completed_time | API đơn hàng hoặc lịch sử sự kiện | Theo trạng thái | NULL nếu chưa hoàn tất hoặc không được cung cấp |
| cancel_time | timestamp UTC | Thời điểm hủy; VD: 2026-10-20T06:00:00Z | cancel_time | cancelled_time | API đơn hàng hoặc hủy hoàn | Theo trạng thái | Chỉ điền khi có căn cứ từ nguồn |
| cancel_reason | string | Lý do hủy; VD: Buyer Cancel | cancel_reason | item_cancel_reason | API đơn hàng hoặc hủy hoàn | Tùy chọn | Giữ nguyên mã nguồn và ánh xạ mô tả |
| original_price | decimal(18,2) | Đơn giá niêm yết của dòng hàng; VD: 59000 | original_price | item_original_price | API dòng hàng hoặc sản phẩm | Cần cho tài chính | Thiếu khác với 0; hiện code có mặc định 0 |
| discounted_price | decimal(18,2) | Đơn giá bán sau giảm theo định nghĩa nguồn; VD: 55000 | model_discounted_price | item_sale_price | API dòng hàng | Tùy chọn | Không âm; xác minh đã gồm thuế và voucher chưa |
| subtotal | decimal(18,2) | Tiền hàng ở cấp mà nguồn định nghĩa; VD: 118000 | total_order_value | sub_total | API đơn hàng hoặc phép tính được kiểm soát | Cần cho tài chính | Xác định cấp đơn hay dòng; không cộng lặp tổng đơn trên mỗi dòng |
| buyer_total_amount | decimal(18,2) | Tổng tiền người mua trả cấp đơn; VD: 108000 | buyer_total_amount | total_amount | API đơn hàng hoặc thanh toán | Cần cho tài chính | Lưu cấp đơn; nếu phân bổ xuống dòng phải có quy tắc |
| seller_discount | decimal(18,2) | Giảm giá do người bán tài trợ; VD: 5000 | seller_discount | seller_discount | API đơn hàng hoặc tài chính | Tùy chọn | Xác minh cấp đơn và dòng; không cộng trùng voucher |
| platform_discount | decimal(18,2) | Giảm giá do sàn tài trợ; VD: 5000 | shopee_discount | platform_discount | API đơn hàng hoặc tài chính | Tùy chọn | NULL nếu không biết; 0 nếu xác nhận không có |
| voucher_total | decimal(18,2) | Tổng voucher theo quy tắc chuẩn hóa; VD: 10000 | voucher_from_seller + voucher_from_shopee | Gán 0 trong ETL | API khuyến mãi hoặc tính toán | Tùy chọn | TikTok gán 0 là giới hạn mã hiện tại; không chứng minh nguồn không có voucher |
| commission_fee | decimal(18,2) | Phí hoa hồng; VD: 4720 | commission_fee | Gán 0 trong ETL | API tài chính hoặc quyết toán | Cần cho lợi nhuận | Thường cần nguồn tài chính riêng; chưa quyết toán để NULL |
| service_fee | decimal(18,2) | Phí dịch vụ; VD: 3540 | service_fee | Gán 0 trong ETL | API tài chính hoặc quyết toán | Cần cho lợi nhuận | Không dùng tỷ lệ giả lập làm phí thật |
| transaction_fee | decimal(18,2) | Phí giao dịch; VD: 2360 | transaction_fee | Gán 0 trong ETL | API tài chính hoặc quyết toán | Cần cho lợi nhuận | Đối soát theo mã đơn và kỳ quyết toán |
| shipping_fee | decimal(18,2) | Phí vận chuyển theo định nghĩa nguồn; VD: 25000 | actual_shipping_fee | shipping_fee | API đơn hàng hoặc logistics hoặc tài chính | Tùy chọn | Phân biệt người mua trả với người bán chịu |
| original_shipping | decimal(18,2) | Phí vận chuyển trước giảm hoặc ước tính; VD: 30000 | estimated_shipping_fee | original_shipping_fee | API logistics hoặc đơn hàng | Tùy chọn | Hai trường nguồn không hoàn toàn cùng nghĩa; giữ loại phí gốc |
| tax_amount | decimal(18,2) | Thuế theo dữ liệu nguồn; VD: 10000 | Gán 0 trong ETL | tax_amount | API đơn hàng hoặc tài chính | Theo thị trường | Không suy ra thuế thật từ giả định simulator |
| currency | string | Đơn vị tiền tệ; VD: VND | currency | currency | API thanh toán hoặc cấu hình thị trường | Cần cho tài chính | Simulator có; schema chung hiện chưa giữ trường này; cần bổ sung khi tích hợp |
| payment_method | string | Phương thức thanh toán; VD: COD | payment_method | payment_method | API đơn hàng nếu cung cấp | Tùy chọn | Chuẩn hóa danh mục; không mặc định COD khi thiếu |
| is_cod | boolean | Có thu tiền khi giao hàng; VD: true | cod | is_cod | API đơn hàng hoặc chuẩn hóa payment_method | Theo quy tắc vòng đời | Simulator dùng chuỗi; cần chuyển boolean; ETL hiện chưa giữ rõ trường này |
| carrier_name | string | Tên đơn vị vận chuyển; VD: J&T Express | shipping_carrier | shipping_provider | API đơn hàng hoặc logistics | Tùy chọn | Có thể bổ sung sau khi tạo vận đơn |
| state | string | Tỉnh thành giao hàng; VD: Hà Nội | state | region_state | Địa chỉ trong API nếu được phép | Tùy chọn | Chuẩn hóa theo danh mục có phiên bản; không cố định danh sách cũ |
| city | string | Thành phố hoặc cấp hành chính nguồn; VD: Hà Nội | city | city_town | Địa chỉ trong API nếu có | Tùy chọn | Không giả định cấu trúc hành chính giống nhau giữa nguồn |
| district | string | Quận huyện theo dữ liệu nguồn; VD: Cầu Giấy | district | district | Địa chỉ trong API nếu có | Tùy chọn | Chấp nhận NULL nếu nguồn không cung cấp |
| country | string | Quốc gia giao hàng; VD: VN | country | Gán VN trong ETL | API địa chỉ hoặc cấu hình shop | Tùy chọn | Không gán VN nếu thu thập nhiều thị trường |
| weight_kg | decimal(12,3) | Khối lượng một đơn vị sản phẩm kg; VD: 0.05 | item_weight | Không sinh trực tiếp | API sản phẩm hoặc danh mục WMS | Tùy chọn | Đổi g sang kg; phân biệt cân nặng kiện hàng |

### inventory_snapshot

| Trường | Kiểu đích | Ý nghĩa / Ví dụ | Shopee giả lập | TikTok giả lập | Nguồn thực tế ứng viên | Yêu cầu | Kiểm tra / khi thiếu |
|---|---|---|---|---|---|---|---|
| sku | string | SKU nội bộ; VD: OL-IP15PM | sku | sku | WMS ERP hoặc bảng ánh xạ API sản phẩm | Bắt buộc | Phải khớp SKU đơn hàng |
| warehouse_id | string | Mã kho vật lý; VD: WH-MOCK-CENTRAL | warehouse_id | warehouse_id | WMS ERP hoặc API kho | Cần khi nhiều kho | Consumer hiện lưu theo SKU và ngày; chưa tách nhiều kho |
| stock_on_hand | integer | Tồn vật lý hiện tại; VD: 350 | stock_on_hand | stock_on_hand | WMS ERP hoặc kiểm kê | Bắt buộc | Không đồng nhất tồn có thể bán trên sàn với tồn vật lý; số nguyên không âm |
| loaded_at | timestamp UTC | Mốc chụp tồn theo hợp đồng hiện tại; VD: 2026-10-20T05:00:00Z | loaded_at | loaded_at | Đồng hồ snapshot hoặc thời điểm nguồn | Bắt buộc | Hiện consumer dùng để chọn bản mới nhất; cần phân biệt mốc nguồn và lúc nhận |
| safety_stock | integer | Ngưỡng tồn an toàn; VD: 50 | safety_stock | safety_stock | Chính sách doanh nghiệp hoặc tính toán | Cần cho cảnh báo | Không phải trường mặc định của API đơn hàng; không âm |
| reorder_point | integer | Ngưỡng đặt hàng lại; VD: 100 | reorder_point | reorder_point | Chính sách doanh nghiệp hoặc mô hình | Cần cho cảnh báo | Ghi phiên bản chính sách; thiếu thì không khẳng định mức cảnh báo |
| incoming_stock | integer | Hàng nhập đang chờ về; VD: 250 | incoming_stock | incoming_stock | PO trong ERP WMS | Tùy chọn | Simulator có; consumer hiện chưa lưu trường này vào fact tồn |
| pending_return_stock | integer | Hàng hoàn chưa nhập lại kho; VD: 2 | pending_return_stock | pending_return_stock | WMS và hệ thống hoàn hàng | Tùy chọn | Không cộng vào tồn vật lý trước kiểm nhận |
| restock_eta | timestamp UTC | Thời điểm dự kiến hàng nhập về; VD: 2026-10-22T05:00:00Z | restock_eta | restock_eta | PO hoặc nhà cung cấp | Tùy chọn | Có thể thay đổi; chưa được consumer lưu vào fact tồn |
| lead_time_days | integer | Số ngày cung ứng theo cấu hình; VD: 2 | lead_time_days | lead_time_days | Chính sách hoặc lịch sử PO | Cần cho đề xuất nhập | Simulator đặt 2..4; thực tế phải đo theo SKU và nhà cung cấp |
| alert_level | string | Mức cảnh báo theo ngưỡng; VD: NORMAL | alert_level | alert_level | Tính từ tồn và chính sách | Dẫn xuất | CRITICAL nếu tồn <= safety_stock; WARNING nếu <= reorder_point; còn lại NORMAL |
| needs_reorder | boolean | Đã chạm điểm đặt lại; VD: false | needs_reorder | needs_reorder | Tính toán | Dẫn xuất | stock_on_hand <= reorder_point; chưa xét toàn bộ đơn nhập mở |
| recommended_reorder_qty | integer | Lượng nhập đề nghị theo cấu hình; VD: 250 | recommended_reorder_qty | recommended_reorder_qty | Chính sách hoặc mô hình | Dẫn xuất | Simulator dùng reorder_qty cố định khi chạm ngưỡng; không phải đầu ra AI |
| total_sold | integer | Lũy kế số lượng bán theo simulator; VD: 100 | total_sold | total_sold | Tổng hợp sổ giao dịch | Tùy chọn | Phải định nghĩa khoảng tích lũy và xử lý hủy hoàn |
| total_restocked | integer | Lũy kế hàng nhập; VD: 250 | total_restocked | total_restocked | Tổng hợp WMS | Tùy chọn | Không lấy trực tiếp từ API đơn hàng |
| total_returned | integer | Lũy kế hàng hoàn đã nhập lại; VD: 2 | total_returned | total_returned | Tổng hợp WMS | Tùy chọn | Tách hàng hoàn đang vận chuyển |
| out_of_stock_count | integer | Số lần bị từ chối do thiếu tồn; VD: 1 | out_of_stock_count | out_of_stock_count | Nhật ký vận hành hoặc simulator | Tùy chọn | API đơn đã nhận không đo được toàn bộ nhu cầu bị mất |

### inventory_movement

| Trường | Kiểu đích | Ý nghĩa / Ví dụ | Shopee giả lập | TikTok giả lập | Nguồn thực tế ứng viên | Yêu cầu | Kiểm tra / khi thiếu |
|---|---|---|---|---|---|---|---|
| movement_id | string | Mã giao dịch kho; VD: MOV-001 | movement_id | movement_id | Sổ giao dịch WMS hoặc simulator | Bắt buộc cho sổ kho | Duy nhất theo nguồn; chống ghi lặp |
| sku | string | Mã SKU; VD: OL-IP15PM | sku | sku | WMS hoặc simulator | Bắt buộc cho sổ kho | Ánh xạ danh mục chung |
| warehouse_id | string | Kho phát sinh; VD: WH-MOCK-CENTRAL | warehouse_id | warehouse_id | WMS hoặc simulator | Bắt buộc cho sổ kho | Không suy từ shop_id |
| movement_type | string | Loại biến động; VD: OUTBOUND_SALE | movement_type | movement_type | WMS hoặc simulator | Bắt buộc cho sổ kho | OUTBOUND_SALE; INBOUND_RESTOCK; OUT_OF_STOCK_REJECT; RETURN_RESTOCK; CANCEL_RELEASE |
| quantity | integer | Lượng giao dịch theo quy ước nguồn; VD: 2 | quantity | quantity | WMS hoặc simulator | Bắt buộc cho sổ kho | Chuẩn hóa dấu theo loại giao dịch; không mặc định mọi nguồn dùng cùng dấu |
| stock_before | integer | Tồn trước giao dịch; VD: 350 | stock_before | stock_before | WMS hoặc tính từ sổ kho | Cần cho đối soát | Đối chiếu theo thứ tự giao dịch và quy ước xuất nhập |
| stock_after | integer | Tồn sau giao dịch; VD: 348 | stock_after | stock_after | WMS hoặc tính từ sổ kho | Cần cho đối soát | Sự kiện từ chối thiếu hàng không làm giảm tồn |
| reference_order_id | string | Mã đơn tham chiếu; VD: SPE261020000001 | reference_order_id | reference_order_id | WMS hoặc simulator | Tùy chọn | NULL với nhập không gắn đơn bán |
| timestamp | timestamp UTC | Thời điểm giao dịch kho; VD: 2026-10-20T05:00:00Z | timestamp | timestamp | WMS hoặc simulator | Bắt buộc cho sổ kho | Phân biệt thời điểm nghiệp vụ với thời điểm nhận |

## 5. Metadata thu thập đề xuất bổ sung

Các trường sau là đề xuất, không khẳng định đã có trong đầu ra hiện tại:

| Trường | Kiểu | Mục đích |
|---|---|---|
| data_origin | enum: api, simulator, manual | Phân biệt dữ liệu thật và giả lập; không trộn trong đánh giá mô hình |
| schema_version | string | Theo dõi phiên bản hợp đồng dữ liệu |
| api_version | string nullable | Phiên bản API được gọi |
| ingested_at | timestamp UTC | Thời điểm hệ thống nhận dữ liệu |
| source_event_id | string nullable | ID webhook hoặc sự kiện để chống lặp |
| source_status_raw | string | Giữ trạng thái gốc trước ánh xạ |
| field_presence | object | Phân biệt trường vắng, NULL và có giá trị |
| quality_flags | array | Ghi thiếu trường, mặc định, chưa quyết toán, ánh xạ thất bại |
| source_system | string | Nguồn sàn, ERP hay WMS |

## 6. Quy tắc khi thiếu trường và các điểm cần sửa trước tích hợp thật

1. Đơn hàng thiếu order_id, platform, sku, create_time hoặc order_status bị cách ly; quantity phải là số nguyên dương. Mã hiện tại giới hạn quantity <= 100, cần hiệu chỉnh nếu có đơn sỉ.
2. Snapshot tồn yêu cầu sku, loaded_at, stock_on_hand. Consumer kiểm tra theo batch; lỗi có thể làm batch thất bại và không commit offset, khác cơ chế cách ly từng dòng của ETL đơn hàng.
3. Trường thời gian theo vòng đời có thể NULL hợp lệ; không tự dựng timestamp từ quy luật giả lập để lấp dữ liệu thật.
4. ETL đang mặc định 0 cho nhiều khoản tiền, gán 0 cho một số phí TikTok và thuế Shopee. Cần giữ trạng thái chưa biết trước khi dùng báo cáo lợi nhuận. Không diễn giải các giá trị này là số đã đối soát.
5. category Shopee đang lấy model_name là quy ước riêng của simulator; API thật cần tra danh mục sản phẩm. TikTok category và weight hiện bị ETL gán rỗng.
6. Bộ giả lập có currency và cờ COD, nhưng schema chung hiện chưa giữ đầy đủ; cần bổ sung để tránh sai diễn giải tiền tệ và quy tắc thanh toán.
7. Chưa đồng nhất tồn có thể bán trên sàn, tồn vật lý, tồn giữ chỗ và hàng hoàn chờ về. Chỉ WMS/ERP hoặc sổ kho kiểm soát mới là nguồn tồn vật lý có thẩm quyền.
8. Không coi total_sold là nhu cầu thị trường không bị giới hạn: hết hàng có thể làm mất giao dịch quan sát được. Ghi stockout để đánh giá sai lệch dự báo.
9. Dữ liệu cá nhân như điện thoại, địa chỉ chi tiết và tên người nhận không bắt buộc cho dự báo theo SKU/ngày. Giới hạn lưu trữ và quyền truy cập theo nhu cầu thực tế; dữ liệu giả lập phải được gắn nhãn.

## 7. Danh mục trường gốc của simulator

Danh mục dưới được trích trực tiếp từ khai báo dataclass tại thời điểm lập tài liệu. Nó bổ sung các trường phụ chưa có trong bảng chuẩn; không khẳng định ETL sử dụng hoặc API cung cấp các trường này. Optional biểu thị có thể nhận None về mặt kiểu Python; không bảo đảm mọi giá trị khác None đều hợp lệ.

### ShopeeOrderEvent

| Trường gốc | Kiểu khai báo |
|---|---|
| pkId | str |
| user_id | str |
| shop_id | str |
| order_sn | str |
| shop_name | str |
| connection_id | str |
| order_status | str |
| order_status_raw | str |
| create_time | str |
| pay_time | Optional[str] |
| shipped_time | Optional[str] |
| completed_time | Optional[str] |
| cancel_time | Optional[str] |
| cancel_reason | Optional[str] |
| cancel_by | Optional[str] |
| fulfillment_flag | str |
| item_name | str |
| item_sku | str |
| model_name | Optional[str] |
| model_sku | str |
| item_id | str |
| model_id | str |
| quantity | int |
| original_price | float |
| model_discounted_price | Optional[float] |
| item_weight | float |
| total_order_value | float |
| buyer_total_amount | float |
| seller_discount | float |
| shopee_discount | float |
| voucher_from_seller | float |
| voucher_from_shopee | float |
| commission_fee | float |
| service_fee | float |
| transaction_fee | float |
| escrow_amount | float |
| actual_shipping_fee | float |
| estimated_shipping_fee | float |
| payment_method | str |
| cod | Optional[str] |
| currency | str |
| buyer_user_id | str |
| buyer_username | Optional[str] |
| recipient_name | str |
| phone | str |
| full_address | str |
| state | str |
| city | str |
| district | str |
| country | str |
| region | Optional[str] |
| shipping_carrier | Optional[str] |
| shipping_method | Optional[str] |
| ship_by_date | Optional[str] |
| package_number | Optional[str] |
| synced_at | str |
| update_time | Optional[str] |
| is_primary_row | str |
| promotion_type | Optional[str] |
| promotion_id | Optional[str] |
| _platform | str |

### TikTokOrderEvent

| Trường gốc | Kiểu khai báo |
|---|---|
| pkId | str |
| user_id | str |
| order_id | str |
| order_type | str |
| shop_name | str |
| order_status | str |
| created_time | str |
| paid_time | Optional[str] |
| rts_time | Optional[str] |
| shipped_time | Optional[str] |
| delivered_time | Optional[str] |
| completed_time | Optional[str] |
| cancelled_time | Optional[str] |
| updated_time | str |
| rts_sla_time | Optional[str] |
| tts_sla_time | Optional[str] |
| delivery_sla_time | Optional[str] |
| cancel_sla_time | Optional[str] |
| currency | str |
| total_amount | float |
| sub_total | float |
| order_original_price | float |
| shipping_fee | float |
| original_shipping_fee | float |
| tax_amount | float |
| product_tax | float |
| shipping_tax | float |
| small_order_fee | float |
| retail_delivery_fee | float |
| insurance_fee | float |
| seller_discount | float |
| platform_discount | float |
| shipping_seller_discount | float |
| shipping_platform_discount | float |
| payment_method | Optional[str] |
| is_cod | Optional[str] |
| fulfillment_type | Optional[str] |
| delivery_type | Optional[str] |
| delivery_option | Optional[str] |
| shipping_provider | Optional[str] |
| tracking_number | Optional[str] |
| warehouse_id | Optional[str] |
| buyer_uid | Optional[str] |
| buyer_name | Optional[str] |
| buyer_message | Optional[str] |
| recipient_name | Optional[str] |
| recipient_phone | Optional[str] |
| full_address | Optional[str] |
| postal_code | Optional[str] |
| region_state | Optional[str] |
| city_town | Optional[str] |
| district | Optional[str] |
| product_name | str |
| sku_name | str |
| seller_sku | Optional[str] |
| quantity | int |
| item_status | str |
| item_sale_price | float |
| item_original_price | float |
| item_platform_disc | float |
| item_seller_disc | float |
| product_id | str |
| sku_id | str |
| package_id | Optional[str] |
| package_status | Optional[str] |
| item_cancel_reason | Optional[str] |
| createdAt | str |
| updatedAt | str |
| row_idx | int |
| _platform | str |

### InventoryMovementEvent

| Trường gốc | Kiểu khai báo |
|---|---|
| movement_id | str |
| sku | str |
| warehouse_id | str |
| movement_type | str |
| quantity | int |
| stock_before | int |
| stock_after | int |
| reference_order_id | Optional[str] |
| timestamp | Optional[str] |

### InventoryItem

| Trường gốc | Kiểu khai báo |
|---|---|
| sku | str |
| product_name | str |
| category | str |
| price | float |
| weight | float |
| stock_on_hand | int |
| safety_stock | int |
| reorder_point | int |
| reorder_qty | int |
| lead_time_days | int |
| incoming_stock | int |
| reserved_stock | int |
| pending_return_stock | int |
| restock_eta | Optional[str] |
| total_sold | int |
| total_restocked | int |
| total_returned | int |
| out_of_stock_count | int |
| last_updated | Optional[str] |

## 8. Mã nguồn đối chiếu
data_simulator/data_simulator.py; warehouse/etl/transform.py; warehouse/etl/data_validation.py; ingestion/consumer_inventory_to_postgres.py. Tài liệu này mô tả dữ liệu và đề xuất hợp đồng, không thay đổi hành vi chương trình.

