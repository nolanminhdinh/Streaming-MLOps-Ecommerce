# Apache NiFi — Trực quan hóa Luồng Dữ liệu Streaming E-Commerce

Phân hệ này cung cấp giao diện trực quan hóa dòng chảy dữ liệu (Dataflow Canvas) theo thời gian thực bằng **Apache NiFi**, giúp quan sát trực tiếp các gói tin đơn hàng (FlowFiles) di chuyển qua từng giai đoạn từ **Kafka Broker** qua các bước bóc tách, rẽ nhánh sàn TMĐT (**Shopee / TikTok Shop**), và ghi xuống **MinIO Data Lake**.

---

## 1. Sơ đồ Luồng Trực quan trên NiFi Canvas

```
[ Kafka Broker: ecom.orders.raw ]
               │
               ▼
   1. ConsumeKafka_2_6
 (Nhận đơn hàng streaming)
               │ (success)
               ▼
   2. EvaluateJsonPath
 (Bóc tách metadata: platform, order_id, sku, amount)
               │ (matched)
               ▼
   3. RouteOnAttribute
 (Phân nhánh: Shopee vs TikTok Shop)
        ├── shopee_orders ───────┐
        └── tiktok_orders ───────┤
                                 ▼
                       4. UpdateAttribute
                   (Định tuyến S3 Object Key)
                                 │ (success)
                                 ▼
                         5. PutS3Object
                    (Ghi vào MinIO Data Lake)
                                 │ (success)
                                 ▼
                        6. LogAttribute
                   (Theo dõi vòng đời FlowFile)
```

---

## 2. Các Processor trong Luồng

| Bước | Tên Processor | Loại Processor | Nhiệm vụ chính |
| :---: | :--- | :--- | :--- |
| **1** | `ConsumeKafka (ecom.orders.raw)` | `ConsumeKafka_2_6` | Kết nối `kafka:9092`, đọc luồng đơn hàng từ topic `ecom.orders.raw`, nhóm consumer `nifi-ecom-orders-consumer`. |
| **2** | `EvaluateJsonPath (Extract Metadata)` | `EvaluateJsonPath` | Trích xuất các trường `_platform`, `order_sn`, `order_id`, `order_status`, `sku`, `buyer_total_amount` thành các FlowFile Attributes. |
| **3** | `RouteOnAttribute (Shopee vs TikTok)` | `RouteOnAttribute` | Rẽ nhánh dữ liệu dựa vào điều kiện thuộc tính `${platform:equals('shopee')}` hoặc `${platform:equals('tiktok')}`. |
| **4** | `UpdateAttribute (Partitioning & Path)` | `UpdateAttribute` | Tạo đường dẫn phân vùng chuẩn Data Lake: `raw/orders/platform=${platform}/year=YYYY/month=MM/day=DD/order-<ts>-<uuid>.json`. |
| **5** | `PutS3Object (MinIO Data Lake)` | `PutS3Object` | Tải dữ liệu JSON trực tiếp vào MinIO bucket `ecom-raw-lake` (`http://minio:9000`). |
| **6** | `LogAttribute (Order Flow Monitor)` | `LogAttribute` | Ghi log xác nhận và hiển thị trực quan các gói tin đã hoàn tất trên màn hình Canvas. |

---

## 3. Hướng dẫn Khởi chạy và Trực quan hóa

### Bước 1: Khởi động container NiFi trong Docker
Khởi động hạ tầng cùng với service NiFi:
```bash
docker compose up -d nifi
```

Kiểm tra trạng thái container:
```bash
docker compose ps nifi
```
*(Lưu ý: Apache NiFi là ứng dụng Java lớn, thường mất khoảng 40–60 giây để khởi động xong toàn bộ các bundle NAR).*

### Bước 2: Mở Giao diện NiFi Canvas
Mở trình duyệt web và truy cập địa chỉ:
👉 **[http://localhost:8080/nifi](http://localhost:8080/nifi)**

*(Không cần mật khẩu đăng nhập khi chạy ở chế độ HTTP nội bộ).*

### Bước 3: Đưa luồng mẫu lên Canvas (Import Template)
File template mẫu đã được cấu hình sẵn tại:
[`ingestion/nifi/ecommerce_streaming_flow.xml`](ecommerce_streaming_flow.xml)

Nếu template chưa hiển thị tự động trên Canvas:
1. Trên thanh công cụ trên cùng của NiFi, kéo biểu tượng **Template** thả vào vùng trống Canvas.
2. Chọn template: **`ECommerce_Streaming_Pipeline`** $\rightarrow$ nhấn **Add**.
3. Toàn bộ 6 Processor và các đường liên kết (Connections) sẽ xuất hiện trực quan trên màn hình.

### Bước 4: Khởi động Luồng (Start Dataflow)
- Nhấp chuột phải vào khoảng trống trên màn hình Canvas.
- Chọn **Start** (hoặc bấm nút Play màu xanh ở bảng điều khiển bên trái).
- Toàn bộ các icon màu đỏ (Stopped) sẽ chuyển sang màu xanh lá cây (Running).

---

## 4. Cách Quan sát Dữ liệu Chạy Thời gian thực

### 1. Bắn luồng đơn hàng từ máy host
Mở terminal và kích hoạt Producer:
```bash
python ingestion/producer.py
```

### 2. Quan sát trực tiếp trên màn hình NiFi Canvas
- **Bộ đếm thời gian thực (In / Out)**: Bạn sẽ thấy các con số `In: 100 (50 KB)`, `Out: 100` nhảy liên tục trên từng Processor.
- **Hàng đợi động (Animated Queues)**: Các chấm xanh/xám biểu diễn các gói tin FlowFiles di chuyển dọc theo các đường mũi tên kết nối.
- **Xem trực tiếp nội dung đơn hàng (List Queue)**:
  - Nhấp chuột phải vào đường nối giữa `ConsumeKafka` và `EvaluateJsonPath`.
  - Chọn **List Queue**.
  - Nhấp vào biểu tượng **View Content** (con mắt) $\rightarrow$ Bạn sẽ nhìn thấy toàn văn payload JSON của đơn Shopee / TikTok vừa được sinh ra!
- **Theo dõi phả hệ dữ liệu (Data Provenance)**:
  - Nhấp chuột phải vào `PutS3Object` $\rightarrow$ chọn **View data provenance**.
  - Bấm vào biểu tượng **Lineage Graph** $\rightarrow$ NiFi sẽ vẽ đồ thị nguồn gốc của đúng đơn hàng đó từ lúc được Kafka nhận cho đến khi ghi thành công vào MinIO.

---

## 5. Mẹo Vận hành & Trình diễn (Demo)
- **Tạm dừng luồng để giải thích**: Bạn có thể bấm chuột phải vào một processor bất kỳ (ví dụ `PutS3Object`) và chọn **Stop**. Các gói tin sẽ lập tức ứ đọng lại trong hàng đợi (Queue) hiển thị rõ số lượng FlowFiles đang chờ. Khi bấm **Start**, các gói tin sẽ lập tức được giải phóng xuống MinIO.
- **Kiểm tra trên MinIO Console**: Mở [http://localhost:9001](http://localhost:9001) (tài khoản `minioadmin` / `minioadmin`) $\rightarrow$ vào bucket `ecom-raw-lake/raw/orders/` để thấy các file đơn hàng vừa được NiFi ghi nhận.
