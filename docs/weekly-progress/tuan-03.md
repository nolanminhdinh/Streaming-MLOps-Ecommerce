# Tuần 3: Lưu trữ phân tầng (Data Lake) & Mô hình kho dữ liệu (Star Schema)

## Mục tiêu
Triển khai lưu trữ dữ liệu thô dạng Parquet trên MinIO (Data Lake) và thiết kế, nạp dữ liệu vào kho dữ liệu quan hệ PostgreSQL theo mô hình Chấm sao (Star Schema), kèm pipeline ETL hoàn chỉnh có kiểm định chất lượng dữ liệu.

---

## Công việc đã thực hiện

### 1. Kafka Consumer → MinIO Data Lake (`ingestion/consumer_to_minio.py`)
- Lắng nghe sự kiện từ topic `ecom.orders.raw`.
- Gom nhóm dữ liệu dạng vi mẻ (Micro-batching) theo cấu hình: mỗi **500 messages** hoặc mỗi **60 giây** (tùy điều kiện nào đến trước).
- Chuyển đổi dữ liệu sang định dạng **Apache Parquet** tối ưu hóa cho lưu trữ phân tích cột (columnar storage) với thuật toán nén `Snappy`.
- Thiết lập sơ đồ phân vùng dữ liệu (Partitioning Scheme) theo chuẩn Hive-style trên MinIO:
  ```
  raw/orders/platform={shopee|tiktok}/year=YYYY/month=MM/day=DD/part-<timestamp>-<count>.parquet
  ```
- Tự động tạo bucket `ecom-raw-lake` nếu chưa có; hỗ trợ cơ chế retry kết nối và xử lý tắt ứng dụng an toàn (graceful shutdown với flush bộ nhớ đệm cuối cùng).

### 2. Thiết kế mô hình Star Schema trên PostgreSQL (`warehouse/ddl/01_star_schema.sql`)
- Mở rộng mô hình theo dữ liệu thực tế sàn TMĐT Việt Nam (Shopee + TikTok Shop):
  - **Bảng Fact**:
    - `Fact_Orders`: Lưu trữ chi tiết từng dòng sản phẩm trong đơn hàng. Tích hợp các trường measures: doanh thu gốc, số tiền khách thực trả (`buyer_total_amount`), chiết khấu từ nhà bán/sàn, voucher, phí sàn (hoa hồng, dịch vụ, thanh toán), phí vận chuyển, thuế VAT (TikTok), và toàn bộ các mốc thời gian trong vòng đời đơn hàng.
    - `Fact_Inventory_Daily`: Khung lưu trữ tồn kho hàng ngày theo SKU và kho (chuẩn bị cho Tuần 4).
  - **Các bảng Dimension**:
    - `Dim_Products`: Chuẩn hóa SKU, tên, danh mục, giá gốc, trọng lượng.
    - `Dim_Shops`: Quản lý shop trên từng nền tảng (`shop_id`, `platform`, `connection_id`).
    - `Dim_Geography`: Chuẩn hóa địa lý giao hàng (Tỉnh/Thành, Quận/Huyện, Quốc gia) kèm phân vùng địa lý (Miền Bắc / Miền Trung / Miền Nam).
    - `Dim_Dates`: Bảng thời gian tạo sẵn cho 3 năm (2025–2027) với đầy đủ thứ, tuần, tháng, quý, năm, cờ cuối tuần và cờ ngày đôi Mega-sale (1/1, 2/2, ..., 12/12).
    - `Dim_Payment`: Phương thức thanh toán (COD, Ví Shopee, VNPay, Thẻ, MoMo, ZaloPay, SPayLater...).
    - `Dim_Carriers`: Các đơn vị vận chuyển hàng đầu tại VN (SPX Express, J&T Express, Viettel Post, GHTK, Ninja Van, BEST Express...).
  - Thiết lập chỉ mục (Indexes) trên các khóa ngoại và trường lọc thời gian/trạng thái.

### 3. Pipeline ETL hoàn chỉnh (`warehouse/etl/`)
- **`extract.py`**: Trích xuất dữ liệu Parquet từ MinIO theo partition ngày và sàn, tự động gán metadata sàn nguồn.
- **`transform.py`**:
  - Hợp nhất 2 định dạng dữ liệu khác biệt từ Shopee (84 cột) và TikTok (50 cột) về một **Canonical Schema** duy nhất.
  - Loại bỏ các đơn hàng trùng lặp trên cặp `(order_id, sku)`.
  - Làm sạch các giá trị khuyết (Missing values imputation) và chuẩn hóa kiểu dữ liệu.
- **`data_validation.py`** *(Bổ sung giải quyết triệt để khuyết điểm kiến trúc)*:
  - Kiểm tra Schema Compliance và Null checks trên các cột bắt buộc (`order_id`, `platform`, `sku`, `create_time`, `order_status`).
  - Kiểm tra miền giá trị: `quantity > 0`, `original_price >= 0`, `buyer_total_amount >= 0`.
  - Kiểm tra logic thời gian theo vòng đời: `create_time <= pay_time <= shipped_time <= completed_time`.
  - Cơ chế cách ly dữ liệu lỗi (Quarantine) và xuất báo cáo `ValidationReport` với tỷ lệ đạt chuẩn (Pass Rate).
- **`load.py`**:
  - Upsert idempotent vào các bảng Dimension, tự động phân giải khóa thay thế (Surrogate keys).
  - Nạp dữ liệu vào `Fact_Orders` theo từng batch với SQLAlchemy.
- **`pipeline.py`**:
  - Script điều phối tổng thể (Master Orchestrator), hỗ trợ các tham số dòng lệnh `--date`, `--all-dates`, `--dry-run`, `--batch-size`.

### 4. Công cụ hỗ trợ & Kiểm thử (`scripts/` & `tests/`)
- `scripts/init_warehouse.py`: Tự động áp dụng DDL và kiểm tra các bảng trên PostgreSQL.
- `scripts/seed_dim_tables.py`: Nạp dữ liệu mẫu ban đầu cho danh mục sản phẩm, shop, đơn vị vận chuyển và địa chỉ 3 miền.
- `tests/test_data_simulator.py`: Bộ 6 unit tests kiểm tra Simulator (Shopee, TikTok, Flash sale, Mega-sale, Tỷ lệ sàn).
- `tests/test_etl.py`: Bộ unit tests kiểm tra chuẩn hóa schema, làm sạch và bộ kiểm định DataValidator.

---

## Bảng ánh xạ Schema (Schema Mapping Summary)

| Cột Fact_Orders | Nguồn Shopee (`shopee_orders`) | Nguồn TikTok (`tiktok_orders`) | Ý nghĩa phân tích |
|---|---|---|---|
| `order_id` | `order_sn` | `order_id` | Mã định danh đơn hàng gốc |
| `platform` | `"shopee"` | `"tiktok"` | Phân loại kênh bán |
| `sku` | `item_sku` | `seller_sku` | Khóa liên kết sản phẩm (Dim_Products) |
| `product_name` | `item_name` | `product_name` | Tên sản phẩm hiển thị |
| `category` | `model_name` | Danh mục từ catalog | Phân cấp ngành hàng |
| `quantity` | `quantity` | `quantity` | **Target biến số dự báo (Demand)** |
| `original_price` | `original_price` | `item_original_price` | Đơn giá niêm yết |
| `buyer_total_amount` | `buyer_total_amount` | `total_amount` | Doanh thu thực tế khách trả |
| `commission_fee` | `commission_fee` | `0` (không có trường riêng) | Phí hoa hồng sàn thu |
| `shipping_fee` | `actual_shipping_fee` | `shipping_fee` | Phí vận chuyển |
| `order_status` | `order_status` | `order_status` | Trạng thái (loại bỏ CANCELLED khi dự báo) |
| `create_time` | `create_time` | `created_time` | Mốc thời gian phát sinh nhu cầu mua |

---

## Sản phẩm bàn giao
1. `ingestion/consumer_to_minio.py`
2. `warehouse/ddl/01_star_schema.sql`
3. `warehouse/etl/extract.py`
4. `warehouse/etl/transform.py`
5. `warehouse/etl/data_validation.py`
6. `warehouse/etl/load.py`
7. `warehouse/etl/pipeline.py`
8. `scripts/init_warehouse.py`
9. `scripts/seed_dim_tables.py`
10. `tests/test_data_simulator.py`
11. `tests/test_etl.py`
12. `docs/how-to-run/how-to-run-week3.md`

---

## Kế hoạch tuần tiếp theo (Tuần 4)
- **EDA & Data Quality Audit**: Lập notebook `notebooks/eda.ipynb` đối chiếu dữ liệu trước/sau làm sạch theo quy định Cẩm nang ĐATN.
- **Phân loại ma trận ABC/XYZ**: Phân loại danh mục hàng hóa theo doanh thu đóng góp (ABC) và độ biến động nhu cầu (XYZ) để xác định nhóm sản phẩm ưu tiên cho mô hình Machine Learning.
- **Feature Engineering nền tảng**: Xây dựng các đặc trưng chuỗi thời gian (Lag Features, Rolling Window Statistics, Exponential Moving Average).

---

## Minh bạch sử dụng AI
- **Phần AI hỗ trợ**: Hỗ trợ thiết kế cấu trúc DDL Star Schema, ánh xạ schema Shopee/TikTok, xây dựng các regex và thuật toán kiểm định chất lượng dữ liệu trong `data_validation.py`.
- **Phần sinh viên tự thực hiện**: Thẩm định quy tắc nghiệp vụ phí sàn TMĐT Việt Nam, cấu hình tham số batch size và timeout, chạy kiểm thử thực tế trên cụm Docker container.

