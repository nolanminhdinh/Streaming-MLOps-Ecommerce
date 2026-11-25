# Power BI Executive Dashboard — PostgreSQL Import

> **Mô hình mới sau rà soát schema:** [warehouse/README.md](../warehouse/README.md).
> Dashboard dựng mới đọc schema `bi`, dùng fact dòng hàng thay summary ngày × sàn.
> Bộ query `import/` hiện giữ đường tương thích cũ, chưa phải template PBIX mới.

Nguồn chính của báo cáo là PostgreSQL, chế độ **Import**. CSV chỉ dùng làm snapshot
dự phòng; worker không xuất CSV và không phụ thuộc kết quả backup.

Hướng dẫn đầy đủ: [kết nối, chuyển query, refresh, quyền chỉ đọc và đối soát](import/README.md).

## Tài nguyên

| Tệp/thư mục | Vai trò |
|---|---|
| `import/*.pq` | 7 query PostgreSQL và 2 query cấu hình server/database cho Power Query |
| `views_for_powerbi.sql` | Các view BI, worker cài một lần lúc khởi động |
| `export_powerbi_dataset.py` | SELECT dùng chung cho Import và CSV; CLI xuất dự phòng |
| `data/*.csv` | Snapshot dự phòng, có thể cũ nếu chưa chạy backup |
| `dax_measures.md` | Measure theo tên bảng/schema giữ nguyên khi đổi nguồn |
| `BaoCaoQuanLy/` | Báo cáo chính (PBIP, 6 trang, đọc schema `mart`). Các PBIX cũ đã xóa ngày 22/11/2026 |

## Chạy

1. Chạy `docker compose up -d --build`, đợi worker healthy.
2. Trong Power BI Desktop, chọn PostgreSQL database và **Import**; server mặc định
   `localhost:5432`, database `ecom_warehouse`. Dùng tài khoản chỉ đọc.
3. Dùng bộ query tại `import/`, giữ tên bảng, Apply rồi Refresh. Xem hướng dẫn
   chi tiết trước khi thay query trong PBIX đang có.
4. Nếu cần CSV dự phòng, chạy `docker compose --profile backup run --rm csv-backup`.
   Lệnh backup lỗi không làm worker unhealthy hoặc xử lý lại batch ETL.

KPI số đơn đếm distinct `(platform, order_id)` trên đơn không hủy; AOV tính tổng
`buyer_total_amount` / tổng số đơn. Import và CSV dùng cùng SELECT để giữ định nghĩa.
Forecast chỉ lấy từ dự báo ML đã lưu; actual chỉ hoàn tất sau ngày mục tiêu và có
coverage phù hợp. Bảng forecast trống là trạng thái hợp lệ khi chưa có dự báo.

## Thiết kế Bố cục Dashboard (3 Trang Báo cáo)

### Trang 1: Tổng quan Kinh doanh Đa kênh (Executive Overview)
- **KPI Cards**: Tổng doanh thu (Net Revenue), Sản lượng bán (Units Sold), Tổng đơn hàng (Orders), Giá trị đơn trung bình (AOV).
- **Donut Chart**: Tỷ trọng doanh thu Shopee/TikTok tính từ các đơn hợp lệ trong warehouse.
- **Area Chart**: Xu hướng tăng trưởng doanh thu theo ngày, làm nổi bật các đợt Flash Sale và Mega-sale ngày đôi.
- **Bar Chart**: Top 10 sản phẩm đóng góp doanh thu lớn nhất chuỗi.

### Trang 2: Độ chính xác Dự báo Nhu cầu (Demand Forecasting & Accuracy)
- **Line & Clustered Column Chart**: So sánh nhu cầu thực tế đã phát sinh với forecast ML đã lưu; forecast tương lai hiển thị riêng cho đến khi có actual.
- **Gauge Chart**: Chỉ số độ chính xác dự báo (**Forecast Accuracy %** = $1 - \text{WAPE}$); để trống khi chưa có actual đã hoàn tất.
- **Table / Matrix**: WAPE và Bias tính từ cặp forecast/actual đã hoàn tất, có thể phân rã theo phân khúc sản phẩm. Khoảng tin cậy chưa được hiệu chuẩn nên không dùng làm dải 95%.

### Trang 3: Quản trị Tồn kho & Cảnh báo Đặt hàng lại (Inventory Risk Matrix)
- **Scatter Plot / Matrix Grid**: Trực quan hóa ma trận 9 ô ABC/XYZ (Trục X: Hệ số biến thiên CV, Trục Y: Doanh thu Pareto).
- **Stat Cards**: Đếm nhanh số lượng SKU ở mức báo động đỏ 🔴 **CRITICAL**, cảnh báo vàng 🟡 **WARNING**, và an toàn 🟢 **NORMAL**.
- **Actionable Table**: Danh sách chi tiết các sản phẩm cần nhập hàng gấp, hiển thị Tồn kho hiện tại, Điểm đặt hàng lại (ROP), Tồn kho an toàn (SS), Số ngày còn lại trước khi đứt hàng (`days_until_stockout`) và Số lượng đề xuất đặt thêm (`recommended_reorder_qty`).
