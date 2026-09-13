# Hướng dẫn chạy hạ tầng Tuần 2 (Docker Desktop — Windows/Mac)

## 1. Yêu cầu

- Đã cài **Docker Desktop** và đang chạy (icon cá voi ở khay hệ thống).
- Đã clone/tải repo `streaming-mlops-ecommerce` về máy.
- RAM trống tối thiểu khuyến nghị: ~4GB (Kafka + Zookeeper + MinIO + Postgres).

## 2. Tạo file cấu hình `.env`

Dự án đọc cấu hình (port, tài khoản MinIO/Postgres, tên topic...) từ file `.env`.
File này **không được commit lên Git** (chứa thông tin nhạy cảm), nên bạn cần tự
tạo từ file mẫu:

```bash
# Windows PowerShell
copy .env.example .env

# Mac / Linux
cp .env.example .env
```

Mở `.env` lên và chỉnh nếu cần — ví dụ nếu máy bạn đã có ứng dụng khác chiếm
cổng 5432 hoặc 9000, chỉ cần đổi số cổng trong `.env`, không cần sửa
`docker-compose.yml`. Nếu không chỉnh gì, các giá trị mặc định trong
`.env.example` vẫn chạy tốt.

## 3. Khởi động hạ tầng

Mở terminal (PowerShell trên Windows, hoặc Terminal trên Mac) tại thư mục gốc dự án:

```bash
docker compose up -d
```

Lệnh này sẽ tự động:
- Khởi động Zookeeper, Kafka, MinIO, PostgreSQL.
- Chạy `kafka-init` để tạo sẵn 2 topic: `ecom.orders.raw`, `inventory.logs`.
- Chạy `minio-init` để tạo sẵn bucket `ecom-raw-lake`.
- Chạy DDL trong `warehouse/ddl/01_star_schema.sql` để tạo bảng Star Schema
  trong PostgreSQL (chỉ chạy lần đầu khi volume `postgres-data` còn trống).

## 4. Kiểm tra trạng thái

```bash
docker compose ps
```

Kỳ vọng: `zookeeper`, `kafka`, `minio`, `postgres` ở trạng thái `healthy`;
`kafka-init` và `minio-init` ở trạng thái `Exited (0)` (vì chúng chỉ chạy một
lần rồi thoát — đây là điều bình thường, không phải lỗi).

Xem log nếu cần:

```bash
docker compose logs -f kafka-init
docker compose logs -f minio-init
docker compose logs -f kafka
```

## 5. Truy cập các giao diện quản trị

| Service | URL | Tài khoản |
|---|---|---|
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin |
| PostgreSQL | localhost:5432 | ecom / ecom_password (db: ecom_warehouse) |

Kiểm tra bảng đã tạo trong Postgres (dùng psql, DBeaver, hoặc pgAdmin):

```sql
\dt
-- Kỳ vọng thấy: dim_products, dim_warehouses, dim_dates,
-- fact_orders, fact_inventory_daily
```

## 6. Cài Python dependencies và chạy thử Simulator → Kafka

```bash
python -m venv .venv
# Windows: .venv\Scripts\activate
# Mac:     source .venv/bin/activate
pip install -r requirements.txt

python ingestion/producer.py
```

Nếu producer chạy được và không báo lỗi kết nối, nghĩa là Kafka đã sẵn sàng
nhận dữ liệu qua `localhost:29092`.

## 7. Dừng hạ tầng

```bash
docker compose down          # dừng và xóa container (giữ lại volume dữ liệu)
docker compose down -v       # dừng và xóa luôn volume (reset toàn bộ dữ liệu)
```

## 8. Sự cố thường gặp

| Triệu chứng | Nguyên nhân khả dĩ | Cách xử lý |
|---|---|---|
| Port đã được sử dụng (`port is already allocated`) | Cổng 9092/9000/5432/2181 đã bị chương trình khác chiếm | Đổi cổng map bên trái trong `docker-compose.yml`, ví dụ `"15432:5432"` |
| Kafka không lên `healthy`, cứ restart | Máy thiếu RAM hoặc Zookeeper chưa kịp sẵn sàng | Tăng RAM cấp cho Docker Desktop (Settings → Resources), chờ thêm rồi `docker compose up -d` lại |
| Python không kết nối được Kafka từ máy host | Dùng nhầm cổng nội bộ 9092 thay vì 29092 | Đảm bảo code Python dùng `localhost:29092` (biến `KAFKA_BOOTSTRAP_SERVERS` trong `ingestion/producer.py`) |
| PostgreSQL không có bảng nào | DDL chỉ chạy khi volume `postgres-data` **trống lần đầu** | Chạy `docker compose down -v` rồi `docker compose up -d` lại để khởi tạo sạch |
| `docker compose up` báo warning "variable is not set" | Chưa tạo file `.env` (bước 2) | Chạy `cp .env.example .env` rồi thử lại — nếu vẫn bỏ qua bước này, Docker Compose sẽ tự dùng giá trị mặc định nên không lỗi, chỉ có warning |
| Python báo lỗi `ModuleNotFoundError: No module named 'dotenv'` | Chưa cài `python-dotenv` | Chạy lại `pip install -r requirements.txt` |

## 9. Việc cần làm tiếp theo (nằm ngoài phạm vi Tuần 2)

- Hoàn thiện logic sinh dữ liệu thật trong `data_simulator/data_simulator.py`
  và nối trực tiếp với `ingestion/producer.py`.
- Hoàn thiện `ingestion/consumer_to_minio.py` để ghi Parquet thật vào MinIO
  (Tuần 3).
