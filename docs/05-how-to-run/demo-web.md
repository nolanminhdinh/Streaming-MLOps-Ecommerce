# Trang demo và mô phỏng — FlowLab

Trang local tập trung các hoạt động thật của dự án: phát payload simulator vào
Kafka, đọc Parquet theo mã đợt, đọc đơn/snapshot từ PostgreSQL, đối chiếu checkpoint
worker, xem quarantine và gọi API dự báo/tồn kho. Không thay model đang phục vụ.

## Mở demo

1. Mở Docker Desktop và đợi engine hoạt động.
2. Chạy `start_demo_web.bat` ở thư mục gốc dự án. Lần đầu cần build image.
3. Mở <http://127.0.0.1:8050> hoặc <http://localhost:8050>.
4. Đợi các kiểm tra sẵn sàng; mở **Mô phỏng** để chọn tình huống hoặc dùng trực tiếp
   các điều khiển trong **Luồng dữ liệu** và **Dự báo & tồn kho**.

Lời dẫn và trình tự bảo vệ được tách riêng trong
[kich-ban-bao-ve-demo.md](kich-ban-bao-ve-demo.md), không đưa lên website.

## Các tình huống trên web

| Tình huống | Thao tác |
|---|---|
| Luồng đơn hàng chuẩn | Cấu hình 12 đơn hợp lệ, không chèn lỗi; chuyển sang màn hình phát |
| Bản tin không hợp lệ | Cấu hình 12 đơn + 1 lỗi số lượng âm; chuyển sang màn hình phát |
| Gửi trùng dữ liệu | Gửi lại payload của đợt đã đủ dữ liệu và checkpoint |
| Nhiều đơn liên tiếp | Cấu hình 50 đơn hợp lệ, không chèn lỗi |
| Tồn kho bằng 0 | Gọi API với SKU đang chọn, tồn giả định 0 và lead time 3 ngày |
| Tồn kho bằng 100 | Gọi API với cùng SKU, tồn giả định 100 và lead time 3 ngày |

Các nút cấu hình đơn không tự phát dữ liệu: bấm **Phát đợt mới** để gửi.
Hai nút tồn kho chạy truy vấn ngay và không ghi thay đổi vào Warehouse.

[Báo cáo chạy thực tế và đối chiếu đủ 6 tình huống ngày 24/10/2026](../03-testing-and-benchmark/demo_web_six_scenarios_2026-10-24.md).

## Vận hành dịch vụ

Khởi động từ dòng lệnh:

```powershell
docker compose --profile demo up -d --build
docker compose --profile demo ps
docker compose logs --tail 80 demo-web pipeline-worker fastapi
```

`demo-web` chỉ được bật khi chọn profile `demo`, chỉ công bố cổng 8050 trên loopback.
Service dùng image runtime ingestion, mount mã chỉ đọc và chỉ được ghi manifest
đợt phát vào `data/demo`. Không gắn Docker socket, không gọi shell từ giao diện.
Các nút không cung cấp thao tác xóa dữ liệu, reset offset hay triển khai model.

## Diễn giải đúng các màn hình

- **Tổng quan:** Kafka kiểm tra cổng TCP; MinIO kiểm tra bucket, PostgreSQL truy vấn
  thật; worker kiểm tra heartbeat dưới 180 giây; Serving kiểm tra `/health`; model
  kiểm tra `/ready`. Readiness chưa chứng minh chất lượng dự báo.
- **Luồng dữ liệu:** chọn 2–50 đơn hợp lệ (giao diện có preset), thêm tùy chọn một
  bản tin quantity âm. Mỗi đợt có tiền tố `WEB…`, hai SKU riêng và hai snapshot
  tồn kho 50/8. Các đơn mới có trạng thái UNPAID; không bịa thời điểm giao hàng.
  Đây là fixture trình diễn, không phải một phiên mô phỏng đầy đủ vòng đời đơn.
- **Kafka ACK:** chỉ tăng sau khi broker trả partition/offset. Lỗi kết nối hoặc
  timeout hiển thị lỗi. ACK không chứng minh consumer/warehouse đã xử lý xong.
- **Parquet:** đọc object raw theo ngày/sàn, lọc đúng mã đơn của đợt. Số raw có thể
  lớn hơn số đơn hợp lệ do có bản tin lỗi và bản tin gửi lại.
- **Warehouse:** đọc Fact_Orders và Fact_Inventory_Daily theo tiền tố đợt.
  Không lấy tổng dữ liệu từ lần kiểm thử cũ làm kết quả đợt mới.
- **Quarantine:** lọc `order_id` đúng mã đợt trong `quarantine/orders/`, hiển thị
  `rejection_reasons`. Gửi lại có thể tạo thêm lần ghi lỗi, nên đây là số bản ghi
  quarantine quan sát được, không phải số đơn lỗi duy nhất.
- **Checkpoint & CSV:** object được checkpoint nghĩa là worker đã hoàn thành ETL
  và export. Thời gian CSV là thời gian sửa tệp, không chứng minh Power BI Desktop
  đã refresh hoặc render PBIX. Các số `last_etl` là phạm vi toàn worker.
- **Dự báo:** danh mục gợi ý theo nguồn lịch sử của model, tối đa 40 SKU có nhiều
  dòng lịch sử nhất. Chọn ngày sau lịch sử SKU để trình bày đúng phạm vi. Kết quả
  hiển thị model/history source; đường dẫn dự phòng được cảnh báo rõ.
- **Tồn kho:** mặc định gọi API không ghi đè tồn. Có thể bật tồn kho giả định để
  giải thích SS/ROP; nguồn khi đó là `request`, không ghi vào database. SKU WEB
  chưa đủ lịch sử ML; nhu cầu có thể dùng `fallback_catalog`.

## Nghiệm thu trực tiếp trước ngày bảo vệ

1. Chờ worker healthy, lake/warehouse kết nối, model và API sẵn sàng.
2. Phát **12 đơn + 1 lỗi**. Kafka nhận 13 bản tin đơn và 2 snapshot.
3. Chờ ít nhất một chu kỳ consumer/worker (thường 10–60 giây; backlog có thể lâu
   hơn). Kỳ vọng: 13 raw, 12 dòng đơn, 1 lỗi quarantine; hai tồn kho 50/8.
4. Chờ các object của đợt đều checkpoint. Bấm **Gửi lại**. Kỳ vọng sau xử lý:
   26 raw, 12 dòng đơn; Kafka có 26 ACK đơn và 4 ACK snapshot cộng dồn.
   Giao diện chỉ kết luận gửi lại giữ nguyên số đơn khi baseline đủ và toàn bộ
   raw đã checkpoint. Nếu gửi lại sớm, ghi rõ chưa đủ điều kiện kết luận.
5. Phát đợt khác, bấm **Dừng phát**. Không xóa ACK hay thu hồi bản tin; consumer
   tiếp tục xử lý các bản tin đã nhận. Đợt dừng sớm có thể chưa đủ hai snapshot.
6. Chọn SKU lịch sử, dự báo 7 ngày, kiểm tra nguồn model/lịch sử và response JSON.
   Nếu model chưa sẵn sàng hoặc dữ liệu không phù hợp, trình bày đúng lỗi API.
7. Kiểm tra nhập hàng bằng tồn thực tế rồi bật tồn giả định 0/100 để so sánh.
   Ghi rõ nguồn stock/demand, nhất là khi dùng fallback.
8. Tải **bằng chứng JSON** để lưu trạng thái quan sát tại thời điểm nghiệm thu.
   Báo cáo gồm cả truy vấn dự báo/tồn kho gần nhất từ lúc server khởi động;
   các truy vấn này có SKU riêng, không mặc nhiên thuộc đợt streaming hiện tại.

Mỗi đợt giữ dữ liệu để đối chiếu, không tự xóa. Không phát fixture quá nhiều khi
chưa có kế hoạch tách dữ liệu demo khỏi báo cáo tổng hợp dùng cho nghiệp vụ.
Đơn streaming mới không bổ sung tự động vào lịch sử CSV riêng của model v2,
không tự kích hoạt training, drift, model promotion hoặc reload.

## Kiểm thử độc lập

```powershell
python -m unittest tests.test_demo_web -v
node --check demo_web/app.js
docker compose --profile demo config --quiet
```

Unit tests kiểm tra fixture qua transform/validator thật, ACK thất bại,
dừng phát, replay giữ payload, khôi phục đợt gián đoạn, cách ly bằng chứng giữa
hai đợt, HTTP, bảo vệ thao tác local và phản hồi lỗi API. Broker và HTTP serving
được mock trong unit tests; các bước nghiệm thu trực tiếp ở trên vẫn cần chạy.

## Khi dịch vụ không sẵn sàng

Màn hình dùng `—` và thông báo chưa kết nối; không tạo số liệu hay biểu đồ mẫu.
Heartbeat/CSV cũ vẫn có thời gian đi kèm để phân biệt dữ liệu lưu trên đĩa với
hoạt động hiện tại. Snapshot quan sát không phải transaction nguyên tử giữa
Kafka, Lake và Warehouse; chờ lần đọc mới trước khi đối soát.

Nếu Docker báo lỗi `sailor-ingest.sock`, engine chưa chạy được và không thể
nghiệm thu Kafka/MinIO/PostgreSQL. Đây là lỗi môi trường đã từng ghi trong
`docs/04-pipeline-updates/lan-05-pipeline-integrity-2026-10-12.md`.
Không reset factory defaults hay xóa volume để khắc phục khi cần giữ dữ liệu.

Để chỉ xem giao diện chẩn đoán khi Docker không hoạt động, dùng một Python 3.12
hoạt động trên máy chạy `python scripts/demo_web_server.py`, rồi mở địa chỉ local.
Muốn kiểm tra pipeline từ host cần các dependency runtime và biến môi trường
tương ứng; cách chạy Docker profile ở trên là đường vận hành chính.

Dừng riêng trang demo, giữ pipeline:

```powershell
docker compose --profile demo stop demo-web
```
