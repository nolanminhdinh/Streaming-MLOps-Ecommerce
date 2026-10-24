# Kịch bản bảo vệ — demo hệ thống Streaming MLOps

Tài liệu dành riêng cho người trình bày. Website chỉ chứa bảng điều khiển,
các tình huống mô phỏng và kết quả vận hành; không hiển thị lời dẫn bảo vệ.

## Chuẩn bị trước buổi bảo vệ

1. Mở Docker Desktop, chạy `start_demo_web.bat`, truy cập
   <http://127.0.0.1:8050>.
2. Kiểm tra trạng thái Kafka, Lake, Warehouse, worker, Serving và model.
3. Chạy thử tình huống **Bản tin không hợp lệ**; đợi đủ raw, đơn hợp lệ,
   bản tin quarantine và checkpoint. Tiếp tục kiểm tra **Gửi trùng dữ liệu**.
4. Chọn một SKU có lịch sử, dùng ngày ngay sau lịch sử và thử dự báo 7 ngày.
5. Kiểm tra hai tình huống tồn kho bằng 0 và 100 trên cùng SKU.
6. Chuẩn bị MLflow/Grafana/NiFi nếu cần trả lời câu hỏi sâu về từng thành phần.

Nếu một dịch vụ chưa sẵn sàng, xử lý trước buổi trình bày. Không dùng số liệu cũ
để khẳng định luồng đang chạy. Hướng dẫn vận hành và nghiệm thu chi tiết nằm ở
[demo-web.md](demo-web.md).

## Trình tự trình bày khoảng 5 phút

### 00:00–00:45 — Bài toán và kiến trúc

Mở **Tổng quan**, chỉ vào sơ đồ và các trạng thái kết nối.

Lời dẫn gợi ý:

> Hệ thống thu thập đơn hàng từ nhiều kênh thương mại điện tử, lưu dữ liệu thô,
> chuẩn hóa vào kho dữ liệu và phục vụ dự báo nhu cầu cùng cảnh báo tồn kho.
> Màn hình này đọc trạng thái từ các thành phần đang vận hành.

Phân biệt rõ kiểm tra cổng Kafka với Kafka ACK, và trạng thái model sẵn sàng
phục vụ với chất lượng dự báo.

### 00:45–01:30 — Phát dữ liệu và kiểm tra chất lượng

Mở **Mô phỏng → Bản tin không hợp lệ → Cấu hình bản tin lỗi**. Website chuyển
sang **Luồng dữ liệu**, chọn sẵn 12 đơn và bật một lỗi. Bấm **Phát đợt mới**.

Lời dẫn gợi ý:

> Tôi tạo 12 đơn hợp lệ và một bản tin có số lượng âm. Dữ liệu là mô phỏng,
> nhưng được gửi vào Kafka và xử lý qua pipeline thật. Tiền tố WEB cho phép
> truy vết riêng đợt này. Partition và offset là phản hồi xác nhận từ broker.

Chỉ ra mã đợt và ACK đang tăng. Không nói rằng ACK đã chứng minh dữ liệu vào kho.

### 01:30–02:45 — Đối soát và chống trùng

Chờ consumer/worker xử lý. Kỳ vọng sau lần phát đầu:

| Vị trí | Kết quả mong đợi |
|---|---|
| Kafka | 13 ACK đơn, 2 ACK snapshot tồn kho |
| Data Lake | 13 bản tin raw |
| Warehouse | 12 dòng đơn hợp lệ |
| Quarantine | 1 bản ghi với `NON_POSITIVE_QUANTITY` |
| Snapshot tồn kho | Hai SKU của đợt, mức 50 và 8 |
| Worker | Các object của đợt đã checkpoint sau ETL/export |

Lời dẫn gợi ý:

> Dữ liệu thô vẫn được giữ để kiểm tra lại. Bản tin lỗi được lưu riêng kèm lý do;
> các đơn hợp lệ tiếp tục đi vào Warehouse.

Khi đợt đủ dữ liệu và checkpoint, mở **Mô phỏng → Gửi trùng dữ liệu**. Nút chỉ
sẵn sàng sau khi đợt hiện tại đã được đối soát. Bấm gửi lại, chờ xử lý xong.

> Tôi gửi lại cùng payload và khóa đơn. Số bản tin raw tăng lên 26 nhưng số
> dòng đơn trong Warehouse vẫn là 12, thể hiện cơ chế xử lý trùng.

Nếu thời gian chờ dài, chuyển sang dự báo rồi quay lại đối chiếu. Chỉ kết luận
chống trùng khi giao diện xác nhận đủ raw và checkpoint sau gửi lại.

### 02:45–04:15 — Dự báo và quyết định nhập hàng

Mở **Dự báo & tồn kho**, chọn SKU có lịch sử, chọn ngày sau lịch sử và dự báo
7 ngày. Đọc nguồn model/lịch sử bên dưới biểu đồ.

> Đây là API sử dụng model đã huấn luyện và nguồn lịch sử đi kèm. Đợt đơn vừa
> phát chứng minh luồng streaming; nó không tự huấn luyện lại model.

Mở **Mô phỏng → Tồn kho bằng 0**, quan sát mức cảnh báo, SS, ROP và lượng đề
xuất nhập. Tiếp tục chọn **Tồn kho bằng 100** trên cùng SKU để so sánh.

> Hai truy vấn giữ nguyên SKU và thời gian chờ, chỉ thay mức tồn kho giả định.
> Khi tồn kho nằm dưới ngưỡng, hệ thống đề xuất nhập hàng. Nguồn request cho biết
> đây là giả định trong truy vấn, không ghi thay đổi vào kho.

Mức 100 không bảo đảm luôn đủ hàng cho mọi SKU; đọc kết quả thực tế từ API.
Nếu stock source là `simulated_demo` hoặc demand source là `fallback_catalog`,
nói rõ đó là dữ liệu dự phòng.

### 04:15–05:00 — Vận hành, MLOps và giới hạn

Quay lại **Tổng quan**, trình bày model version, stage, nguồn huấn luyện, tuổi
lịch sử và các chỉ số hiện có. Có thể mở MLflow/Grafana khi cần.

> Luồng kỹ thuật và kết quả dự báo được theo dõi riêng. Việc model phục vụ được
> chưa thay thế đánh giá chất lượng trên dữ liệu thực tế.

Tải bằng chứng JSON của đợt. Báo cáo chứa snapshot quan sát và các truy vấn gần
nhất từ khi server khởi động; các truy vấn có thể dùng SKU khác với đợt streaming.

## Các tình huống bổ sung khi hội đồng yêu cầu

- **Luồng đơn hàng chuẩn:** 12 đơn, không chèn lỗi; quan sát luồng thành công.
- **Nhiều đơn liên tiếp:** 50 đơn hợp lệ, quan sát batch và các chu kỳ xử lý.
  Đây là mô phỏng đợt dài hơn, không phải bằng chứng tải lớn hay SLA.
- **Dừng phát:** dừng producer của đợt hiện tại; bản tin đã ACK vẫn tiếp tục xử lý.
  Không yêu cầu kho đạt đủ số đơn ban đầu nếu đã dừng sớm.
- **Mất kết nối:** đọc đúng trạng thái chưa sẵn sàng và thời điểm snapshot;
  không tạo số liệu thay thế hoặc tự nhận đã chứng minh cơ chế phục hồi.

## Các điểm cần nói chính xác

- Đơn WEB là fixture có nhãn; dữ liệu được giữ lại để đối chiếu.
- Checkpoint sau export không có nghĩa Power BI Desktop đã refresh PBIX.
- Training, drift, promotion và reload không được tự động chạy bởi các nút mô phỏng.
- Dùng đúng metric/model/lịch sử hiện có trên màn hình, không gán kết quả nghiệm thu
  kỹ thuật thành kết quả chất lượng dự báo hoặc khuyến nghị kinh doanh đã kiểm chứng.
