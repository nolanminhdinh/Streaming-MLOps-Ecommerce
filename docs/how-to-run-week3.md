# Hướng dẫn chạy trọn vẹn luồng Streaming & ETL Tuần 3

Tài liệu này hướng dẫn cách chạy toàn bộ chu trình luồng dữ liệu (End-to-End Streaming Pipeline):
```
Data Simulator → Kafka Producer → Kafka Topic (ecom.orders.raw)
      ↓
Kafka Consumer → Parquet Batches → MinIO Data Lake (ecom-raw-lake)
      ↓
ETL Pipeline (Extract → Transform → Validate → Load) → PostgreSQL (Star Schema)
```

---

## 1. Chuẩn bị môi trường

### Bước 1.1: Tạo file cấu hình `.env`
Nếu chưa tạo ở Tuần 2, hãy copy từ file mẫu:
```bash
# Windows PowerShell
copy .env.example .env

# Mac / Linux
cp .env.example .env
```

### Bước 1.2: Thiết lập môi trường ảo Python (Virtual Environment)
Theo khuyến nghị quản lý thư viện của dự án:
```bash
# Tạo môi trường ảo .venv
python -m venv .venv

# Kích hoạt môi trường ảo:
# Trên Windows PowerShell:
.venv\Scripts\Activate.ps1
# (Nếu gặp lỗi Execution Policy: Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass)

# Trên Mac / Linux:
source .venv/bin/activate

# Cài đặt các thư viện cần thiết
pip install -r requirements.txt
```

---

## 2. Khởi động hạ tầng Docker

Mở terminal tại thư mục gốc dự án và chạy:
```bash
docker compose up -d
```
Kiểm tra các container đã sẵn sàng:
```bash
docker compose ps
```
> **Kỳ vọng:** `zookeeper`, `kafka`, `minio`, `postgres` đều ở trạng thái `healthy`.

---

## 3. Khởi tạo kho dữ liệu PostgreSQL (Star Schema)

Chạy script khởi tạo bảng và seed dữ liệu danh mục ban đầu:

```bash
# Tạo cấu trúc các bảng Fact và Dimension (nếu chưa có)
python scripts/init_warehouse.py

# Nạp dữ liệu mẫu cho Dim_Products, Dim_Shops, Dim_Carriers, Dim_Geography
python scripts/seed_dim_tables.py
```

---

## 4. Chạy luồng thu thập dữ liệu (Streaming Ingestion)

Cần mở **2 cửa sổ Terminal** riêng biệt (đều đã kích hoạt `.venv`):

### Terminal 1: Khởi động Kafka Consumer (Ghi xuống MinIO Data Lake)
```bash
python ingestion/consumer_to_minio.py
```
- Consumer sẽ lắng nghe topic `ecom.orders.raw`.
- Mỗi khi nhận đủ **500 messages** hoặc sau **60 giây**, dữ liệu sẽ được chuyển thành file Parquet (nén Snappy) và đẩy lên MinIO bucket `ecom-raw-lake`.
- Đường dẫn trên MinIO: `raw/orders/platform={shopee|tiktok}/year=YYYY/month=MM/day=DD/part-*.parquet`.

### Terminal 2: Khởi động Kafka Producer (Sinh đơn hàng thời gian thực)
```bash
python ingestion/producer.py
```
- Producer kết nối với `ECommerceSimulator` và bắn liên tục các đơn hàng mô phỏng vào Kafka.
- Các đơn hàng tuân theo quy tắc: Shopee (55%), TikTok Shop (45%), phân bổ giờ vàng Flash Sale (12h, 20h, 21h), phí sàn và vòng đời thực tế.

> **Kiểm tra trên Web UI MinIO:**
> - Mở trình duyệt: `http://localhost:9001`
> - Đăng nhập: `minioadmin` / `minioadmin`
> - Vào bucket `ecom-raw-lake` → kiểm tra các thư mục phân vùng và file `.parquet` được tạo ra liên tục.

Nhấn `Ctrl + C` ở cả hai terminal khi muốn dừng luồng sinh dữ liệu.

---

## 5. Chạy Pipeline ETL (MinIO → PostgreSQL Star Schema)

Sau khi MinIO đã có các file Parquet, mở **Terminal 3** để chạy pipeline ETL:

### Chạy kiểm thử chất lượng trước (Dry Run — không ghi vào DB):
```bash
python warehouse/etl/pipeline.py --dry-run
```
Báo cáo Data Quality Scorecard sẽ hiển thị:
- Số bản ghi thô (Raw) trích xuất từ MinIO.
- Số bản ghi sau khi Unify và Deduplicate.
- Tỷ lệ đạt chuẩn (Pass Rate) và chi tiết các trường bị lỗi/cách ly.

### Chạy nạp dữ liệu chính thức vào PostgreSQL:
```bash
# Xử lý dữ liệu của ngày hôm nay (mặc định)
python warehouse/etl/pipeline.py

# Hoặc xử lý toàn bộ các ngày có trong Data Lake
python warehouse/etl/pipeline.py --all-dates

# Hoặc xử lý một ngày cụ thể trong quá khứ
python warehouse/etl/pipeline.py --date 2026-09-24
```

---

## 6. Truy vấn đối chiếu dữ liệu trong PostgreSQL

Bạn có thể kết nối vào PostgreSQL qua DBeaver, pgAdmin hoặc trực tiếp bằng lệnh sau:
```bash
docker compose exec postgres psql -U ecom -d ecom_warehouse
```

### Một số câu lệnh SQL phân tích mẫu:

1. **Tổng số đơn hàng theo từng sàn và trạng thái:**
```sql
SELECT
    platform,
    order_status,
    COUNT(*) AS total_orders,
    SUM(buyer_total_amount) AS total_revenue_vnd
FROM Fact_Orders
GROUP BY platform, order_status
ORDER BY platform, total_orders DESC;
```

2. **Top 5 sản phẩm có sản lượng bán cao nhất (Demand):**
```sql
SELECT
    p.sku,
    p.product_name,
    p.category,
    SUM(f.quantity) AS total_quantity_demanded,
    SUM(f.buyer_total_amount) AS total_sales_vnd
FROM Fact_Orders f
JOIN Dim_Products p ON f.product_key = p.product_key
WHERE f.is_cancelled = FALSE
GROUP BY p.sku, p.product_name, p.category
ORDER BY total_quantity_demanded DESC
LIMIT 5;
```

3. **Doanh thu và sản lượng theo vùng miền:**
```sql
SELECT
    g.region,
    COUNT(f.order_fact_id) AS order_count,
    SUM(f.buyer_total_amount) AS revenue_vnd
FROM Fact_Orders f
JOIN Dim_Geography g ON f.geo_key = g.geo_key
GROUP BY g.region
ORDER BY revenue_vnd DESC;
```

---

## 7. Chạy Unit Tests

Chạy bộ kiểm thử tự động của dự án:
```bash
# Kiểm tra bộ sinh đơn hàng ECommerceSimulator
python tests/test_data_simulator.py

# Kiểm tra bộ chuẩn hóa và kiểm định dữ liệu ETL
python tests/test_etl.py
```
> Cả 2 bài kiểm thử đều sử dụng thư viện chuẩn của Python và đảm bảo đạt **100% OK**.
