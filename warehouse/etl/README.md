# ETL: MinIO (Raw Parquet) → PostgreSQL (Star Schema)

Thư mục này sẽ chứa pipeline trích xuất dữ liệu thô từ MinIO, làm sạch, xử lý
dữ liệu khuyết/nhiễu, và nạp vào các bảng Fact/Dimension trong PostgreSQL.

## Kế hoạch triển khai (Tuần 3-4)

- `extract.py`: đọc file Parquet mới từ MinIO theo partition ngày.
- `transform.py`: làm sạch dữ liệu (missing values, outliers), chuẩn hóa khóa
  (SKU, warehouse_id, ngày) để map sang các bảng Dimension.
- `load.py`: upsert vào Dim_Products/Dim_Warehouses/Dim_Dates, sau đó insert
  vào Fact_Orders/Fact_Inventory_Daily.
- `data_validation.py` (bổ sung — khuyết điểm đã ghi nhận): kiểm tra schema,
  kiểu dữ liệu, giá trị hợp lệ trước khi nạp vào Fact table.

## Bảng so sánh dữ liệu trước/sau làm sạch

Sẽ được lập ở `notebooks/eda.ipynb` theo đúng yêu cầu của Cẩm nang, đối chiếu:
- Số lượng bản ghi trước/sau khi loại bỏ trùng lặp, ngoại lai.
- Tỷ lệ giá trị khuyết theo từng cột trước/sau xử lý.
- Phân phối các biến số chính (quantity, unit_price) trước/sau chuẩn hóa.
