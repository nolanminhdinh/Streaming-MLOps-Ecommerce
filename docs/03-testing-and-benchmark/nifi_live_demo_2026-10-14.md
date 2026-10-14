# Xem luồng chạy trực tiếp trên NiFi — 14/10/2026

Demo đã chạy và đối soát thành công, run ID `E2E20261014e09628`. NiFi được giữ ở trạng thái chạy sau khi demo kết thúc.

| Giao diện | Localhost |
|---|---|
| NiFi canvas | http://localhost:8080/nifi/ |
| MinIO console | http://localhost:9001/ |
| API Swagger | http://localhost:8000/docs |
| API readiness | http://localhost:8000/ready |
| MLflow Registry | http://localhost:5000/ |
| Grafana | http://localhost:3000/ |

## Quan sát trên canvas

1. Mở NiFi, quan sát sáu processor có trạng thái chạy màu xanh.
2. Nhấp đúp `xem_luong_nifi.bat` ở thư mục gốc dự án, hoặc chạy `.venv\Scripts\python.exe scripts/run_nifi_live_demo.py`. Docker và các dịch vụ dự án cần đang chạy.
3. Demo gửi 64 event cách nhau 0,8 giây, gồm Shopee và TikTok. Bấm Refresh trên NiFi để thấy Out của ConsumeKafka và In/Out của các processor phía sau tăng.
4. Đường trên canvas: ConsumeKafka → EvaluateJsonPath → RouteOnAttribute → UpdateAttribute → PutS3Object → LogAttribute. Route phân biệt shopee/tiktok; UpdateAttribute tạo object key; PutS3Object ghi vào MinIO.
5. Bấm phải PutS3Object → View data provenance để xem sự kiện xử lý và thuộc tính filename/platform của FlowFile. Có thể mở từng provenance event để kiểm tra chi tiết.
6. Queue về 0 khi xử lý hết là bình thường. Bộ đếm trên canvas là cửa sổ thời gian gần đây, không phải tổng lịch sử; muốn nhìn dữ liệu chuyển động, chạy lại demo.

MinIO: bucket `ecom-raw-lake` → `raw/nifi/orders/platform=shopee/` hoặc `platform=tiktok/` → `ingest_date=2026-10-14/`. Lọc tên file có run ID để phân biệt lượt thử. Khi chạy ngày khác, ingest_date theo giờ Việt Nam của ngày chạy.

## Kết quả thực tế

| Chặng | Kết quả |
|---|---|
| NiFi | Cả sáu processor RUNNING/VALID; mỗi processor xử lý liên quan 64 FlowFile |
| NiFi → MinIO JSON | 64 files: Shopee 32, TikTok 32 |
| Python consumer → MinIO Parquet | 64 dòng, 64 files do demo gửi chậm/batch nhỏ; partition đúng giờ Việt Nam |
| ETL → Fact_Orders | 32 đơn duy nhất: Shopee 16 đơn/32 units, TikTok 16 đơn/28 units |
| Replay | Số đơn và quantity không đổi |
| Inventory → PostgreSQL → API | Hai SKU demo có snapshot thật 123 và 47 units, stock_source=warehouse_snapshot |
| Serving model chính | /ready 200; forecast trên SKU thuộc catalog lịch sử dùng mlflow_registry và warehouse |
| Power BI export | Xuất bảy CSV từ warehouse thật; Forecast_vs_Actual có sáu dòng forecast sau lượt này |

**Phạm vi hiển thị:** canvas NiFi hiện hiển thị nhánh Kafka → JSON MinIO. Nhánh Parquet → ETL → PostgreSQL và API do Python xử lý bên ngoài NiFi. Demo chạy cả hai nhánh nhưng không biến toàn bộ pipeline thành processor NiFi.

64 event là 32 đơn được gửi hai lần để kiểm tra replay. Các stats ETL theo ngày còn gồm fixtures cũ: quarantine ghi nhận lỗi đã cài từ các bài trước, không có nghĩa lượt demo mới bị mất dữ liệu.

SKU mới của demo kiểm tra thu nạp và tồn kho; model chính dùng catalog CSV lịch sử, nên forecast model thật được kiểm tra trên một SKU của catalog đó. Demo không huấn luyện lại hoặc thay model đang phục vụ.

Repository API có cache TTL 60 giây. Khi snapshot vừa được nạp, phản hồi có thể còn bản cũ; runner chờ tối đa 75 giây để xác nhận snapshot mới. Lượt đầu đã ghi đủ dữ liệu nhưng assert ngay trong cửa sổ cache không đạt; recheck sau đó đã PASS. Khởi tạo Kafka consumer được đổi sang tuần tự để tránh lỗi startup selector khi tạo hai consumer cùng lúc trên Windows.

Chi tiết lưu ở `data/runtime_checks/E2E20261014e09628/streaming_result.json` và `nifi_live_demo.json`; các lượt chạy lại có ID riêng. Test facts và objects được giữ lại để đối soát. Cần dung lượng phù hợp nếu chạy demo nhiều lần; đây là minh họa luồng, không phải kiểm thử tải production.
