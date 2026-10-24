# Kiểm chứng đủ 6 tình huống trên web — 24/10/2026

**Kết quả: 6/6 đạt.** Đã thao tác trực tiếp trên các nút mô phỏng của FlowLab
và đối chiếu nội dung hiển thị với dữ liệu đọc độc lập từ hệ thống thực.
Không thay mã ứng dụng, thuật toán hoặc model để đạt kết quả này.

Phiên kiểm tra: `six-scenarios-20261024-1344`.
Giao diện: <http://127.0.0.1:8050>.

## Kết quả từng tình huống

| Tình huống | Kết quả trên web | Đối chiếu độc lập | Kết luận |
|---|---|---|---|
| 1. Luồng đơn hàng chuẩn | 12 ACK đơn, 12 raw, 12 dòng hợp lệ, 0 lỗi | Đọc 14 offset Kafka gồm 2 snapshot; SQL có 12 dòng/21 sản phẩm; 3/3 object checkpoint | Khớp |
| 2. Bản tin không hợp lệ | 13 ACK đơn, 13 raw, 12 dòng hợp lệ, 1 lỗi | Đọc 15 offset gồm 2 snapshot; SQL có 12 dòng/25 sản phẩm; lỗi `NON_POSITIVE_QUANTITY`; 3/3 object checkpoint | Khớp |
| 3. Gửi trùng dữ liệu | ACK đơn và raw tăng lên 26; vẫn 12 dòng hợp lệ; 2 lần ghi lỗi | Đọc 30 offset cộng dồn gồm 4 snapshot; toàn bộ dòng SQL và tổng 25 sản phẩm giữ nguyên so với trước replay; 6/6 object checkpoint | Khớp |
| 4. Nhiều đơn liên tiếp | 50 ACK đơn, 50 raw, 50 dòng hợp lệ, 0 lỗi | Đọc 52 offset gồm 2 snapshot; SQL có 50 dòng/112 sản phẩm; 9/9 object checkpoint | Khớp |
| 5. Tồn kho bằng 0 | CRITICAL; SS=6; ROP=17; đề xuất nhập 41 | API Serving gọi trực tiếp trả cùng kết quả; nguồn stock=`request`, demand=`model_forecast`; kho không thay đổi | Khớp |
| 6. Tồn kho bằng 100 | NORMAL; SS=6; ROP=17; đề xuất nhập 0 | API Serving gọi trực tiếp trả cùng kết quả; nguồn stock=`request`, demand=`model_forecast`; kho không thay đổi | Khớp |

Hai tình huống tồn kho dùng cùng SKU `P23*****03`, lead time 3 ngày và mức phục
vụ 95%. Kết quả 41/0 là kết quả thực tế của SKU này trong phiên kiểm tra, không
phải giá trị cố định cho mọi SKU.

## Cách đối chiếu

Tình huống được khởi chạy qua **Mô phỏng** trên trình duyệt. Với các nút cấu
hình đơn, tiếp tục bấm **Phát đợt mới**. Chỉ gửi lại khi đợt lỗi đã đủ dữ liệu
và checkpoint. Các đợt dùng mã riêng:

| Đợt | Mã |
|---|---|
| 12 đơn hợp lệ | `WEB20261024064436db1d4b` |
| 12 đơn + 1 lỗi, sau đó gửi lại | `WEB20261024064728f8b7cb` |
| 50 đơn hợp lệ | `WEB20261024065750d70b55` |

Sau mỗi tình huống:

1. **Kafka:** dùng consumer đọc độc lập, không có consumer group và không commit
   offset. Đọc lại đúng topic/partition/offset của từng ACK; so sánh key và toàn
   bộ payload với manifest đợt phát. Không chỉ tin bộ đếm ACK của server demo.
2. **MinIO:** đọc trực tiếp các tệp Parquet của ngày nghiệp vụ, đếm đúng mã đơn;
   so sánh số lần xuất hiện và quantity với nguồn. Đọc JSONL quarantine theo
   mã đơn, kiểm tra lý do loại bỏ. Không gọi `Console.lake()` để làm chuẩn đối chiếu.
3. **PostgreSQL:** truy vấn Fact_Orders/Dim_Products theo mã đợt. So sánh từng mã
   đơn, sàn, SKU, số lượng và trạng thái; kiểm tra không có dòng trùng. Các
   snapshot của đợt có đúng mức 50 và 8.
4. **Checkpoint và CSV:** kiểm tra các object của đợt trong checkpoint; danh mục
   CSV có hai SKU mới; tổng đơn và sản phẩm trong Fact_Orders_Summary.csv khớp
   SQL theo ngày/sàn. Đây là kiểm tra tệp xuất, không phải kiểm tra PBIX.
5. **Giao diện:** lưu số hiển thị của bốn ô đếm, toàn bộ hàng trong bảng Warehouse
   và đối chiếu lại với kết quả độc lập. Đối với tồn kho, so sánh cả sáu giá trị
   hiển thị, mức cảnh báo, SKU và nhãn nguồn với API Serving trực tiếp.
6. **Không ghi đè tồn kho:** chụp bảng tồn kho của SKU trước khi chạy giả định;
   đối chiếu sau cả hai truy vấn. Kết quả trước/sau giữ nguyên. SKU lịch sử này
   không có snapshot trong bảng tồn kho; hai truy vấn cũng không chèn snapshot mới.

Đã đọc console trình duyệt: không ghi nhận lỗi JavaScript trong phiên kiểm tra.

## Tính nhất quán theo thời gian

Web phản ánh snapshot quan sát, có độ trễ do polling, flush consumer và chu kỳ
worker. Khi đợt 50 đơn đang xử lý, có lúc endpoint demo trả 50 raw nhưng mới
16 dòng Warehouse và 3/9 object checkpoint. Sau khi worker xử lý hết, kết quả
hội tụ về 50/50 và 9/9. Đây là trạng thái đang xử lý, không phải mất dữ liệu.

Các kết luận “đã đối chiếu đủ” trong ảnh cuối đều có đầy đủ raw, đơn hợp lệ,
quarantine nếu có và checkpoint. Không dùng một mẫu đang xử lý để kết luận
pipeline đã hoàn thành hoặc dữ liệu bị thiếu.

Sau gửi lại, quarantine là **2 lần lưu lỗi của cùng 1 mã đơn lỗi**, không phải
2 đơn lỗi duy nhất. UI sử dụng đúng ý nghĩa số bản ghi/lần ghi.

## Bằng chứng lưu trên máy

- Tổng hợp đối chiếu UI và hệ thống: [summary.json](../../outputs/demo-web/six-scenarios-20261024-1344/summary.json).
- Ảnh từng tình huống:
  [chuẩn](../../outputs/demo-web/six-scenarios-20261024-1344/normal.png),
  [lỗi](../../outputs/demo-web/six-scenarios-20261024-1344/invalid.png),
  [gửi lại](../../outputs/demo-web/six-scenarios-20261024-1344/replay.png),
  [50 đơn](../../outputs/demo-web/six-scenarios-20261024-1344/burst.png),
  [tồn 0](../../outputs/demo-web/six-scenarios-20261024-1344/stock0.png),
  [tồn 100](../../outputs/demo-web/six-scenarios-20261024-1344/stock100.png).
- Các bản đọc UI tương ứng: `outputs/demo-web/six-scenarios-20261024-1344/*-ui.json`.
- Payload, ACK và dữ liệu đối chiếu chi tiết: `data/demo/evidence/six-scenarios-20261024-1344/`.
- Manifest đợt phát: `data/demo/<mã đợt>.json`.
- Công cụ kiểm tra chỉ đọc: `scripts/verify_demo_scenarios.py`.

Các tệp `data/` vẫn được Git ignore theo cấu hình dự án. Phiên thử thêm 74 dòng
đơn hợp lệ/158 sản phẩm từ ba đợt mới; gửi lại không thêm dòng đơn. Dữ liệu có
nhãn được giữ lại để đối chiếu, không xóa dữ liệu hay reset offset.

## Phạm vi kết luận

Không phát hiện sai lệch trong 6 tình huống đã chạy và đối chiếu hoàn tất.
Không cần sửa logic ứng dụng trong lượt kiểm tra này.

Kết quả xác nhận luồng và tính trung thực của nội dung hiển thị tại phiên kiểm
tra. Đợt 50 đơn không thay thế benchmark tải lớn, kiểm thử mất dịch vụ hoặc
nghiệm thu chất lượng dự báo. Model vẫn là v2 Staging, nguồn lịch sử riêng;
hai giả định tồn kho không phải tồn thực tế của doanh nghiệp.
