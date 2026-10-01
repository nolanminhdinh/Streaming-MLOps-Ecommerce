# Tuần 4: Tiền xử lý dữ liệu, Khám phá EDA & Phân loại danh mục hàng hóa ABC/XYZ

## Mục tiêu
Thực hiện kiểm toán dữ liệu và phân tích khám phá (EDA) trên tập dữ liệu đa kênh (Shopee + TikTok Shop); phân loại danh mục sản phẩm theo ma trận 9 ô ABC/XYZ kết hợp hoạch định chính sách tồn kho an toàn; và xây dựng module trích xuất đặc trưng chuỗi thời gian (Feature Engineering) chuẩn bị cho việc huấn luyện mô hình ở Tuần 5–6.

---

## Công việc đã thực hiện

### 1. Phân tích Khám phá & Kiểm toán Dữ liệu (`notebooks/eda.ipynb`)
- **Bảng đối chiếu Trước vs Sau Làm sạch (Data Quality Audit Table)**:
  - Lập bảng đối chiếu minh bạch theo tiêu chuẩn Cẩm nang ĐATN: số lượng bản ghi ban đầu, số bản ghi trùng lặp bị loại bỏ, tỷ lệ giá trị khuyết (% missing) trước/sau, và thống kê mô tả (Mean, Median, Std, Min, Max) của `quantity`, `original_price`, `buyer_total_amount`.
  - Xác nhận 100% các cột khóa chính (`order_id`, `sku`, `create_time`) không còn khuyết thiếu và toàn vẹn miền giá trị.
- **Phân tích hành vi đa kênh**:
  - Tỷ trọng đơn hàng phản ánh thị phần E-Commerce Việt Nam (55% Shopee, 45% TikTok Shop).
  - Tỷ lệ hủy đơn toàn hệ thống duy trì ở mức ~12%, tập trung phần lớn ở giai đoạn chưa thanh toán (UNPAID).
- **Phân tích chu kỳ thời gian & Mùa vụ**:
  - Phát hiện 2 đỉnh mua sắm rõ rệt trong ngày: **12h trưa** (săn Flash Sale trưa) và **20h–21h tối** (giải trí xem Livestream).
  - Sản lượng đơn hàng tăng 20–30% vào hai ngày cuối tuần (Thứ 7 & Chủ Nhật) so với ngày thường.
- **Phân tích tương quan & Độ nhạy cảm giá**:
  - Lập ma trận tương quan giữa số lượng bán với giá niêm yết, chiết khấu của người bán, voucher sàn và phí vận chuyển.

---

### 2. Phân loại Ma trận 9 ô ABC/XYZ & Hoạch định Tồn kho (`ml/features/abc_xyz.py` & `notebooks/abc_xyz_classification.ipynb`)
- **Phân loại ABC (Nguyên lý Pareto 80/15/5 theo Doanh thu)**:
  - Nhóm A: ~20% SKU đóng góp **80%** tổng doanh thu (sản phẩm chủ lực).
  - Nhóm B: ~30% SKU đóng góp **15%** tổng doanh thu (sản phẩm tiềm năng).
  - Nhóm C: ~50% SKU đuôi dài (long-tail) chỉ đóng góp **5%** doanh thu.
- **Phân loại XYZ (Hệ số biến thiên nhu cầu $CV = \sigma / \mu$)**:
  - Nhóm X ($CV \le 0.5$): Nhu cầu bán rất ổn định, dự báo dễ dàng.
  - Nhóm Y ($0.5 < CV \le 1.0$): Nhu cầu biến động vừa phải (theo mùa vụ / khuyến mãi).
  - Nhóm Z ($CV > 1.0$): Nhu cầu biến động mạnh, phát sinh gián đoạn (lumpy demand).
- **Hoạch định chính sách Tồn kho (Inventory Policies)**:
  - Tính toán **Safety Stock (SS)** và **Reorder Point (ROP)** theo độ tin cậy dịch vụ (Service Level 95%, $Z = 1.65$) và thời gian bổ sung hàng Lead Time (3 ngày):
    $$SS = Z \times \sigma_{\text{daily}} \times \sqrt{L}$$
    $$ROP = (\mu_{\text{daily}} \times L) + SS$$
- **Bản đồ chiến lược Quản trị & Mô hình Machine Learning khuyến nghị**:
  - **Nhóm AX/AY**: Doanh thu cao, biến động thấp/vừa ➔ Áp dụng mô hình **LightGBM / Ridge Regression** độ chính xác cao kết hợp quản trị tồn kho Just-In-Time (JIT) để giảm thiểu chi phí lưu kho.
  - **Nhóm AZ**: Doanh thu cao nhưng nhu cầu biến động lớn ➔ Áp dụng **Deep Learning (LSTM/GRU)** kết hợp duy trì Safety Stock đệm cao để chống đứt hàng.
  - **Nhóm BX/BY**: Doanh thu trung bình ➔ Sử dụng **LightGBM Baseline** kết hợp điểm đặt hàng lại tự động (Automated ROP).
  - **Nhóm CZ**: Hàng đuôi dài, nhu cầu thất thường ➔ Cân nhắc Make-to-Order hoặc Dropshipping, duy trì tồn kho tối thiểu để tránh tồn đọng vốn.

---

### 3. Xây dựng Module Đặc trưng Chuỗi Thời Gian (`ml/features/time_series_features.py`)
- Gom nhóm dữ liệu theo ngày và SKU, lấp đầy các ngày không có đơn với `quantity = 0` để chuỗi thời gian liên tục.
- Trích xuất các nhóm đặc trưng quan trọng:
  - **Lịch & Sự kiện**: `day_of_week`, `is_weekend`, `day_of_month`, `month`, `quarter`, cờ ngày đôi `is_mega_sale`.
  - **Biến trễ (Lag Features)**: `lag_1`, `lag_2`, `lag_3`, `lag_7`, `lag_14`, `lag_28`.
  - **Thống kê trượt (Rolling Statistics)**: `rolling_mean`, `rolling_std`, `rolling_max`, `rolling_min` trên cửa sổ 7, 14, 28 ngày (được áp dụng `shift(1)` triệt để để loại bỏ nguy cơ rò rỉ dữ liệu - Data Leakage).
  - **Trung bình trượt hàm mũ (EMA)**: `ema_7`, `ema_14`.
  - **Xung lực tăng trưởng (Momentum)**: Tốc độ tăng trưởng so với tuần trước (`growth_wow`).
- Xây dựng hàm chia tập dữ liệu **Walk-Forward Validation (Expanding Window Split)** cho chuỗi thời gian.

---

### 4. Công cụ hỗ trợ & Kiểm thử (`scripts/` & `tests/`)
- `scripts/simulate_historical_data.py`: Sinh dữ liệu lịch sử 60–90 ngày đa kênh, lưu file Parquet và hỗ trợ nạp thẳng vào PostgreSQL Star Schema.
- `scripts/generate_notebooks.py`: Tự động khởi tạo cấu trúc notebook chuẩn mực theo hướng dẫn ML Best Practices.
- `tests/test_features.py`: Bộ unit test kiểm định tính chính xác của phân loại ABC/XYZ, công thức tồn kho, biến trễ và kiểm tra không rò rỉ dữ liệu.

---

## Sản phẩm bàn giao
1. `ml/features/abc_xyz.py`: Module phân loại ma trận 9 ô ABC/XYZ và hoạch định tồn kho.
2. `ml/features/time_series_features.py`: Module trích xuất đặc trưng chuỗi thời gian và Walk-Forward split.
3. `notebooks/eda.ipynb`: Notebook phân tích khám phá dữ liệu & bảng kiểm toán chất lượng.
4. `notebooks/abc_xyz_classification.ipynb`: Notebook trực quan hóa Pareto, ma trận 9 ô và chính sách tồn kho.
5. `scripts/simulate_historical_data.py`: Script sinh dữ liệu lịch sử đơn hàng.
6. `tests/test_features.py`: Bộ unit tests cho các tính năng Tuần 4.

---

## Kế hoạch tuần tiếp theo (Tuần 5)
- **Thiết lập MLflow Tracking Server**: Cấu hình backend lưu trữ metadata (PostgreSQL) và artifact store (MinIO).
- **Xây dựng Feature Pipeline tự động**: Tích hợp các đặc trưng chuỗi thời gian vào pipeline tiền xử lý chuẩn mực.
- **Thiết lập Pipeline Huấn luyện Baseline**: Xây dựng mô hình Baseline với LightGBM/XGBoost, đo lường MAE/RMSE/MAPE trên từng fold của Walk-Forward Validation.

---

## Minh bạch sử dụng AI
- **Phần AI hỗ trợ**: Hỗ trợ thiết kế cấu trúc hàm tính hệ số $CV$, công thức toán học tính Safety Stock và Reorder Point, xây dựng kịch bản trực quan hóa trong 2 Jupyter Notebooks theo tiêu chuẩn Storytelling with Data.
- **Phần sinh viên tự thực hiện**: Thẩm định tính phù hợp của các ngưỡng phân loại (Pareto 80/15/5, CV 0.5/1.0), kiểm tra logic không rò rỉ dữ liệu chuỗi thời gian (`shift(1)`), và chạy kiểm thử tự động.

