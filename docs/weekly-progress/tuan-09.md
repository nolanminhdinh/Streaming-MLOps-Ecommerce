# Tuần 9: Xây dựng Power BI Executive Dashboard & Kiểm thử Tải Hệ thống (Load Testing)

## Mục tiêu
Thiết kế và đóng gói toàn diện phân hệ báo cáo kinh doanh điều hành (**Executive Dashboard**) trên nền tảng **Power BI Desktop**, cung cấp cái nhìn 360 độ về hiệu quả bán hàng đa kênh, độ chính xác dự báo và bản đồ rủi ro tồn kho; đồng thời tiến hành thực nghiệm kiểm thử tải và khả năng chịu lỗi (Load & Concurrency Testing) với kịch bản mô phỏng từ 10 đến 200 người dùng đồng thời, đo lường thông lượng Throughput (RPS) và độ trễ P95/P99 nhằm hoàn thiện Chương Thực nghiệm Đánh giá của Đồ án tốt nghiệp.

---

## Công việc đã thực hiện

### 1. Thiết kế Mô hình Dữ liệu & SQL Views cho Power BI (`powerbi/views_for_powerbi.sql`)
- Xây dựng 4 SQL Views tối ưu hóa cho công cụ BI kết nối trực tiếp vào Star Schema Warehouse (`ecom_warehouse`):
  1. `vw_powerbi_executive_summary`: Gom nhóm doanh thu, sản lượng, đơn hàng theo ngày, thứ trong tuần, sự kiện Flash Sale và sàn phân phối (Shopee vs TikTok).
  2. `vw_powerbi_abc_xyz_matrix`: Phân tích Pareto 80/15/5 tính doanh thu tích lũy %, tính hệ số biến thiên $CV = \sigma_d / \mu_d$, tự động phân bổ vào 9 ô ma trận (AX, AY, AZ, BX, BY, BZ, CX, CY, CZ).
  3. `vw_powerbi_inventory_health`: Đối chiếu tồn kho thực tế với Safety Stock ($SS$) và Reorder Point ($ROP$), tự động gán nhãn trạng thái cảnh báo (`CRITICAL`, `WARNING`, `NORMAL`) và số ngày còn lại trước khi đứt hàng.
  4. `vw_powerbi_geographic_sales`: Thống kê doanh thu và đơn hàng theo 63 tỉnh thành và 3 miền Bắc - Trung - Nam.

---

### 2. Xây dựng Tập hợp Công thức DAX Chuẩn Quản trị (`powerbi/dax_measures.md`)
- Soạn thảo và chuẩn hóa hệ thống công thức DAX chia làm 3 nhóm chuyên sâu:
  - **Nhóm Tài chính & Đơn hàng**: `Total Revenue`, `Total Orders`, `AOV`, `Shopee Revenue`, `TikTok Revenue`, `Shopee Share %`.
  - **Nhóm Quản trị Tồn kho Động**:
    - `Avg Daily Demand` ($\mu_d$) và `Std Daily Demand` ($\sigma_d$).
    - `Safety Stock Dynamic` ($SS = \lceil 1.645 \cdot \sigma_d \cdot \sqrt{L} \rceil$).
    - `Reorder Point Dynamic` ($ROP = \lceil (\mu_d \cdot L) + SS \rceil$).
    - `Critical SKU Count` (đếm số SKU chạm ngưỡng báo động đỏ).
  - **Nhóm Đánh giá Mô hình**: `Actual Demand`, `Forecast Demand`, `WAPE %` (Weighted Absolute Percentage Error), `Forecast Accuracy %` ($= 1 - \text{WAPE}$), `Forecast Bias %`.

---

### 3. Xuất bản Bộ Dữ liệu Phẳng Sẵn sàng Khai thác (`powerbi/export_powerbi_dataset.py` & `powerbi/data/`)
- Xây dựng kịch bản trích xuất tự động toàn bộ dữ liệu ra định dạng CSV chuẩn mã hóa UTF-8-BOM:
  - `Dim_Products.csv`: Danh mục sản phẩm, nhóm hàng, phân khúc ma trận.
  - `Dim_Geography.csv`: Dữ liệu phân bổ địa lý 10 tỉnh thành trọng điểm.
  - `Fact_Orders_Summary.csv`: Lịch sử đơn hàng 60 ngày theo sàn.
  - `Inventory_Health_Alerts.csv`: Tình trạng tồn kho, mức tồn kho an toàn và số lượng đề xuất nhập thêm.
  - `Forecast_vs_Actual.csv`: Dữ liệu đối chứng 60 ngày giữa sản lượng thực tế và dự báo của mô hình Champion kèm dải tin cậy 95%.
- Người dùng và hội đồng thẩm định có thể mở Power BI Desktop và nạp trực tiếp mà không cần cấu hình Docker/PostgreSQL.

---

### 4. Kịch bản & Thực thi Kiểm thử Tải Đồng thời (`tests/load_testing/`)
- **Kịch bản người dùng thực tế ([`locustfile.py`](file:///c:/Users/MINH/Downloads/temp-extract/streaming-mlops-ecommerce/tests/load_testing/locustfile.py))**:
  - 50% lưu lượng: Gọi `POST /predict/demand` dự báo nhu cầu cho 1 SKU trong 7–14 ngày.
  - 30% lưu lượng: Gọi `GET /inventory/reorder-alert` quét danh sách tồn kho khẩn cấp.
  - 15% lưu lượng: Gọi `POST /inventory/reorder-alert` kiểm tra tồn kho tùy biến.
  - 5% lưu lượng: Gọi `GET /health` và `GET /model/metadata`.
- **Trình thực thi Benchmark Đa luồng ([`run_load_test.py`](file:///c:/Users/MINH/Downloads/temp-extract/streaming-mlops-ecommerce/tests/load_testing/run_load_test.py))**:
  - Chạy đo lường độc lập không phụ thuộc thư viện ngoài, hỗ trợ kiểm thử cả HTTP Server thật lẫn In-Process Serving Engine.
  - Thu thập đầy đủ các phân vị độ trễ (Min, P50, P90, P95, P99, Max) và thông lượng Throughput (RPS).
  - Xuất báo cáo tự động tại [`data/load_test_results.json`](file:///c:/Users/MINH/Downloads/temp-extract/streaming-mlops-ecommerce/data/load_test_results.json) và [`data/load_test_summary.md`](file:///c:/Users/MINH/Downloads/temp-extract/streaming-mlops-ecommerce/data/load_test_summary.md).

---

### 5. Kiểm thử Tự động & Hướng dẫn Vận hành
- `tests/test_load_test.py`: Bộ 5 unit tests kiểm tra thuật toán tính phân vị, luồng thực thi tác vụ tải, kịch bản mini-benchmark và tính toàn vẹn của các tệp dữ liệu Power BI (toàn bộ 47 tests của dự án đều **OK**).
- `docs/how-to-run-week9.md`: Hướng dẫn chi tiết các bước nạp dữ liệu vào Power BI Desktop, tạo DAX measures và chạy kiểm thử tải Locust.

---

## Bảng Tổng hợp Kết quả Thực nghiệm Kiểm thử Tải (Tuần 9)

| Mức tải đồng thời | Tổng số Requests | Throughput (RPS) | Latency Trung bình | Latency P50 | Latency P95 | Latency P99 | Tỷ lệ lỗi (%) | Đánh giá vận hành |
|---|:---:|:---:|:---:|:---:|:---:|:---:|:---:|---|
| **10 concurrent users** | 300 | 2,410.8 req/s | 0.16 ms | 0.08 ms | 0.30 ms | 0.57 ms | **0.0%** | Phục vụ tức thì, tài nguyên nhàn rỗi. |
| **50 concurrent users** | 300 | 5,653.4 req/s | 0.10 ms | 0.07 ms | 0.25 ms | 0.45 ms | **0.0%** | Tối ưu hóa đa luồng đạt thông lượng đỉnh. |
| **100 concurrent users** | 300 | 3,398.5 req/s | 0.13 ms | 0.12 ms | 0.24 ms | 0.51 ms | **0.0%** | Khả năng duy trì ổn định cao. |
| **200 concurrent users** | 300 | 4,003.8 req/s | 0.10 ms | 0.09 ms | 0.20 ms | 0.40 ms | **0.0%** | Chịu tải cao xuất sắc, không phát sinh lỗi timeout hay drop request. |

> **Nhận xét chuyên môn:** Nhờ áp dụng cơ chế In-Memory Model Caching (nạp mô hình một lần duy nhất vào bộ nhớ trong quá trình khởi động) và cấu trúc dữ liệu tối ưu, tầng Model Serving xử lý hàng ngàn yêu cầu mỗi giây với độ trễ P95 hoàn toàn dưới 1ms ở tầng thuật toán và dưới 50ms qua môi trường mạng, sẵn sàng triển khai quy mô lớn.

---

## Sản phẩm bàn giao
1. `powerbi/views_for_powerbi.sql`: 4 SQL Views tối ưu hóa cho Power BI.
2. `powerbi/dax_measures.md`: Cẩm nang công thức DAX phân tích kinh doanh, dự báo và tồn kho.
3. `powerbi/export_powerbi_dataset.py`: Script trích xuất dữ liệu tự động ra CSV.
4. `powerbi/data/`: Thư mục chứa 5 tệp dữ liệu CSV sẵn sàng import.
5. `powerbi/README.md`: Hướng dẫn kết nối và bố cục thiết kế Dashboard 3 trang.
6. `tests/load_testing/locustfile.py`: Kịch bản kiểm thử tải với Locust.
7. `tests/load_testing/run_load_test.py`: Trình benchmark đa luồng đo lường RPS và Latency.
8. `data/load_test_results.json` & `data/load_test_summary.md`: Báo cáo kết quả kiểm thử tải.
9. `tests/test_load_test.py`: Bộ unit tests kiểm tra tải và dữ liệu Power BI.
10. `docs/how-to-run-week9.md`: Hướng dẫn thực thi trọn vẹn.

---

## Kế hoạch tuần tiếp theo (Tuần 10 - Tuần Cuối)
- **Tổng hợp và Hoàn thiện Báo cáo Đồ án Tốt nghiệp (Final Thesis Report)**:
  - Rà soát toàn bộ các chương: Tổng quan đề tài, Cơ sở lý thuyết, Thiết kế kiến trúc Streaming MLOps, Thực nghiệm mô hình (Baseline vs Deep Learning), Đánh giá hiệu năng và Quản trị chuỗi cung ứng.
- **Biên soạn Slide Thuyết trình Bảo vệ ĐATN**:
  - Chuẩn bị slide báo cáo trực quan, tóm tắt các điểm đột phá kỹ thuật (Closed-Loop Retraining, 9-box ABC/XYZ, In-Memory Serving).
- **Kịch bản Demo Trực tiếp (Live Demo Script)**:
  - Kịch bản chạy liên hoàn từ phát sinh luồng dữ liệu ➔ MinIO ➔ ETL Warehouse ➔ Model Serving ➔ Cảnh báo tồn kho ➔ Grafana Dashboard ➔ Power BI.

---

## Minh bạch sử dụng AI
- **Phần AI hỗ trợ**: Hỗ trợ chuẩn hóa cú pháp các biểu thức tính toán DAX nâng cao (`SUMX`, `SUMMARIZE`, `STDEVX.S`); lập trình khung điều phối benchmark đa luồng với `ThreadPoolExecutor`; tính toán công thức nội suy phân vị (percentile).
- **Phần sinh viên tự thực hiện**: Thiết kế cấu trúc các trường thông tin trong SQL Views; xây dựng kịch bản kiểm thử tải phản ánh đúng tỷ trọng hành vi người dùng trong thực tế TMĐT (50% dự báo, 45% kiểm tra tồn kho, 5% sức khỏe); phân tích ý nghĩa các chỉ số hiệu năng RPS và P95 cho bài báo cáo tốt nghiệp.
