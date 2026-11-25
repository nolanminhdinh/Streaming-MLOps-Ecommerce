# PostgreSQL → Power BI Import (nguồn chính)

> **Sau tái cấu trúc warehouse 01/11/2026:** các query dưới đây là đường tương thích
> với bộ CSV/public view cũ. Khi dựng dashboard mới, dùng schema `bi` theo
> [hợp đồng warehouse](../../warehouse/README.md), đặc biệt fact dòng hàng để lọc
> bán hàng theo sản phẩm và địa lý. Chưa dựng lại PBIX theo schema mới.

Các file `.pq` là biểu thức Power Query M để dán vào Advanced Editor; không phải
file tự động đổi nguồn cho PBIX. Chúng dùng đúng SELECT của CSV dự phòng, giữ
tên bảng/cột cho DAX và relationships. Tái tạo bằng
`python scripts/build_powerbi_import.py` khi đổi SQL trong `DATASET_QUERIES`.

## Chuẩn bị PostgreSQL

Chạy `docker compose up -d --build`, đợi `pipeline-worker` healthy; worker cài
`powerbi/views_for_powerbi.sql` một lần khi khởi động. ETL cập nhật bảng gốc,
view đọc dữ liệu mới mà không cần xuất CSV.

Server trên máy chạy Power BI Desktop: `localhost:5432`; database:
`ecom_warehouse` (đổi theo `POSTGRES_PORT`/`POSTGRES_DB` thực tế). Host `postgres`
chỉ dùng giữa các container, không dùng trong Power BI Desktop trên Windows.

Nên dùng tài khoản riêng chỉ đọc. Admin PostgreSQL có thể tạo một lần:

```sql
CREATE ROLE powerbi_reader LOGIN;
GRANT CONNECT ON DATABASE ecom_warehouse TO powerbi_reader;
GRANT USAGE ON SCHEMA public TO powerbi_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO powerbi_reader;
```

Đặt mật khẩu trong công cụ quản trị hoặc dùng `\password powerbi_reader` trong
psql. Không đưa mật khẩu vào file `.pq`. Quyền trên áp dụng bảng/view đang có;
sau khi thêm đối tượng mới, admin cần cấp SELECT tương ứng. Tài khoản ETL vẫn
dùng quyền ghi của riêng nó.

## Chuyển nguồn trong Power BI Desktop

1. Lưu bản sao PBIX trước khi đổi nguồn. Chọn **Get Data → PostgreSQL database**,
   nhập server/database, chọn rõ **Import** và đăng nhập bằng tài khoản chỉ đọc.
   Đây là bước kiểm tra kết nối và chế độ lưu trữ; có thể bỏ query thử sau đó.
2. Mở **Transform data**. Tạo hai Blank Query tên `pServer`, `pDatabase`, dán
   biểu thức trong hai file cùng tên, đặt giá trị đúng môi trường, tắt Enable load
   cho hai query cấu hình.
3. Với từng bảng đang dùng, giữ nguyên tên query và thay toàn bộ nội dung
   **Advanced Editor** bằng file tương ứng. Việc thay toàn bộ bỏ các bước
   `File.Contents`, `Csv.Document`, Promote Headers và Changed Type cũ của CSV.
   Nếu chưa có bảng, tạo Blank Query với tên trong bảng dưới và dán biểu thức.
4. Chọn **Close & Apply**, kiểm tra kiểu dữ liệu, relationships và measures,
   xác nhận Storage mode của các bảng là **Import**. Không đổi thành DirectQuery.
5. **Refresh** để nạp snapshot mới từ PostgreSQL; không cần chạy exporter.
   Sau refresh, đối soát KPI với truy vấn SQL bên dưới.

| Tên query/bảng phải giữ | Nguồn PostgreSQL |
|---|---|
| Dim_Dates | Dim_Dates |
| Dim_Products | Dim_Products + vw_powerbi_abc_xyz_matrix |
| Dim_Geography | Dim_Geography + Fact_Orders không hủy |
| Fact_Orders_Summary | vw_powerbi_executive_summary + Dim_Dates |
| Forecast_vs_Actual | vw_powerbi_forecast_vs_actual |
| Inventory_Health_Alerts | vw_powerbi_inventory_health |
| Historical_Demand_Daily | Fact_Historical_Order_Lines + Dim_Dates |

Giữ khóa số nguyên `date_key`, `product_key`, `geo_key`; ngày ở kiểu Date;
quantity/revenue ở kiểu số, `actual_complete` ở kiểu Boolean. Forecast chưa
có dữ liệu có thể trả bảng trống; không tạo forecast/actual giả để lấp biểu đồ.
`Dim_Dates[date_key]` và `Dim_Products[product_key]` là phía một của quan hệ
với các fact có khóa đó, lọc một chiều từ dimension. Summary không có product_key,
nên bộ lọc sản phẩm không thể lọc KPI summary; không nối summary vào sản phẩm.

Import giữ snapshot trong Power BI, không tự cập nhật khi ETL ghi thêm đơn.
Desktop cần Refresh; nếu publish lên Power BI Service với PostgreSQL trên máy
cục bộ, cần cấu hình gateway, thông tin đăng nhập và lịch refresh riêng.

## Đối soát sau refresh

```sql
SELECT SUM(total_orders) AS total_orders,
       SUM(total_units_sold) AS units_sold,
       SUM(total_revenue) AS net_revenue,
       SUM(total_revenue) / NULLIF(SUM(total_orders), 0) AS aov
FROM vw_powerbi_executive_summary;
```

So sánh với các measure Total Orders, Total Units Sold, Total Revenue, AOV trong
cùng bộ lọc. AOV phải tính tổng tiền/tổng đơn, không lấy trung bình cột `aov`.
Khi luồng vẫn ghi dữ liệu, refresh và truy vấn SQL có thể khác thời điểm; nên dừng
producer và chờ hết backlog trước khi đối soát chính xác.

Kiểm tra lại nguồn bằng `python scripts/check_powerbi_import.py`: script kiểm tra
schema cả 7 SELECT và đối soát tổng trong một transaction PostgreSQL chỉ đọc,
không sửa bảng/view và không xuất CSV. Kết quả lưu trong thư mục evidence.

## CSV dự phòng

Chạy khi cần snapshot:

```powershell
docker compose --profile backup run --rm csv-backup
```

Hoặc trên host có Python/dependencies và cấu hình `.env`:

```powershell
python powerbi/export_powerbi_dataset.py --no-install-views
```

CSV không tự xuất theo cycle, không phải điều kiện healthy hoặc checkpoint.
Lệnh backup lỗi trả mã khác 0 để người vận hành biết; luồng chính vẫn tiếp tục.
File ghi lỗi giữ bản cũ, nhưng cả bộ CSV chưa có transaction chung; khi backup
thất bại có thể có file mới và file cũ lẫn nhau. Chỉ dùng bộ snapshot sau khi lệnh
hoàn tất thành công và ghi lại thời điểm xuất.

Khi PostgreSQL ngừng hoạt động, Power BI Import vẫn có snapshot đã nạp nhưng
Refresh sẽ lỗi. Nếu cần CSV, dùng một bản sao báo cáo và chuyển nguồn có chủ ý;
không có cơ chế âm thầm chuyển nguồn hoặc tự trộn CSV cũ với database.

## Trạng thái kiểm chứng

Các query được chuẩn bị trong repository; chưa đổi nguồn, đăng nhập, Apply hay
Refresh file `Report_PowerBI.pbix` bằng Desktop trong lượt này. Không xem bộ `.pq`
hoặc pytest đạt là bằng chứng PBIX đã kết nối thành công.

Ngày 01/11/2026, cả 7 SELECT đã chạy trên PostgreSQL thật, schema đúng và tổng
đơn/sản lượng/doanh thu khớp facts; 5 kiểm thử SQL bằng bảng/view tạm và rollback
đạt. [Kết quả kiểm tra nguồn](../../docs/07-bao-cao-do-an/evidence/powerbi_postgres_import_2026-11-01.json).

Tham khảo: [Microsoft PostgreSQL connector](https://learn.microsoft.com/en-us/power-query/connectors/postgresql),
[PostgreSQL.Database](https://learn.microsoft.com/en-us/powerquery-m/postgresql-database).
