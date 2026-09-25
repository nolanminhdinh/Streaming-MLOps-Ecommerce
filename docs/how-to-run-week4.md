# Hướng dẫn chạy trọn vẹn EDA, Phân loại ABC/XYZ & Kỹ nghệ Đặc trưng Tuần 4

Tài liệu này hướng dẫn cách vận hành các module của **Tuần 4**:
```
Dữ liệu lịch sử 60 ngày (historical_orders_raw.parquet)
      ↓
Kiểm toán dữ liệu & Bảng đối chiếu Trước vs Sau làm sạch (notebooks/eda.ipynb)
      ↓
Ma trận 9 ô ABC/XYZ Pareto & Hoạch định Tồn kho SS/ROP (ml/features/abc_xyz.py)
      ↓
Trích xuất đặc trưng chuỗi thời gian (Lags, Rolling, Cyclical) (ml/features/time_series_features.py)
      ↓
Xuất tập Feature Store (data/features_daily.parquet) sẵn sàng huấn luyện MLOps
```

---

## 1. Chuẩn bị dữ liệu lịch sử phục vụ nghiên cứu

Trước khi tiến hành phân tích EDA và trích xuất đặc trưng, hệ thống cần tập dữ liệu lịch sử đa kênh (Shopee + TikTok Shop) từ 45 đến 60 ngày:

```bash
# Sinh 60 ngày dữ liệu lịch sử mô phỏng (tạo ra ~10,000 - 15,000 đơn hàng)
python scripts/simulate_historical_data.py --days 60 --orders-per-day 150
```

> **Kết quả kỳ vọng:**
> - File dữ liệu thô: `data/historical_orders_raw.parquet`
> - File dữ liệu sạch sau làm sạch: `data/historical_orders_clean.parquet`

---

## 2. Chạy Phân tích Dữ liệu Khám phá (EDA) trên Jupyter Notebook

Khởi động Jupyter Lab hoặc VS Code / Antigravity IDE để mở notebook:

```bash
jupyter lab notebooks/eda.ipynb
```

### Các nội dung trọng tâm trong `notebooks/eda.ipynb`:
1. **Kiểm toán chất lượng dữ liệu (Data Quality Audit)**: Lập bảng đối chiếu minh bạch số lượng bản ghi, số bản ghi trùng lặp bị loại bỏ, tỷ lệ giá trị khuyết (% missing) trước/sau làm sạch.
2. **Phân tích hành vi đa kênh**: So sánh tỷ trọng đơn hàng giữa Shopee (55%) và TikTok Shop (45%), tỷ lệ hủy đơn (~12%).
3. **Phân tích mùa vụ & chu kỳ**:
   - Biểu đồ nhiệt (Heatmap) đơn hàng theo Giờ trong ngày × Thứ trong tuần: xác định đỉnh Flash Sale 12h trưa và 20h–21h tối.
   - Hiệu ứng bùng nổ doanh thu vào các ngày đôi Mega-sale (8/8, 9/9, 10/10) và cuối tuần.
4. **Phân tích độ nhạy cảm giá & Chiết khấu**: Tương quan Pearson/Spearman giữa voucher sàn, voucher shop, phí ship và số lượng đặt mua (`quantity`).

---

## 3. Thực thi Phân loại Ma trận 9 ô ABC/XYZ & Hoạch định Tồn kho

Chạy notebook phân khúc danh mục sản phẩm:

```bash
jupyter lab notebooks/abc_xyz_classification.ipynb
```

Hoặc kiểm tra thuật toán trực tiếp qua module Python:

```bash
python -c "
import pandas as pd
from ml.features.abc_xyz import ABCXYZClassifier
df = pd.read_parquet('data/historical_orders_clean.parquet')
classifier = ABCXYZClassifier()
matrix = classifier.fit_transform(df)
print(matrix[['sku', 'product_name', 'abc_class', 'xyz_class', 'matrix_class', 'safety_stock', 'reorder_point']].head(10))
"
```

### Các nguyên tắc hoạch định:
- **Phân loại ABC (Pareto)**: Nhóm A ($\le 80\%$ doanh thu), Nhóm B ($80\% - 95\%$), Nhóm C ($> 95\%$).
- **Phân loại XYZ (Hệ số biến thiên $CV = \sigma / \mu$)**:
  - Nhóm X ($CV \le 0.5$): Nhu cầu ổn định.
  - Nhóm Y ($0.5 < CV \le 1.0$): Nhu cầu biến động trung bình (theo mùa vụ/khuyến mãi).
  - Nhóm Z ($CV > 1.0$): Nhu cầu biến động mạnh, phát sinh gián đoạn.
- **Tồn kho an toàn ($SS$) & Điểm đặt hàng lại ($ROP$)**:
  $$SS = \left\lceil Z \times \sigma_{\text{daily}} \times \sqrt{L} \right\rceil$$
  $$ROP = \left\lceil (\mu_{\text{daily}} \times L) + SS \right\rceil$$
  *(Với $Z = 1.65$ ứng với Service Level $95\%$, Lead Time $L = 3$ ngày).*

---

## 4. Trích xuất Đặc trưng Chuỗi Thời Gian & Xuất Feature Store

Chạy pipeline kỹ nghệ đặc trưng hoàn chỉnh để tạo `data/features_daily.parquet`:

```bash
python ml/features/feature_pipeline.py
```

### Các nhóm đặc trưng được sinh ra:
1. **Lịch & Sự kiện**: `day_of_week`, `is_weekend`, `day_of_month`, `month`, `is_mega_sale`.
2. **Biến trễ (Lag Features)**: `lag_1`, `lag_2`, `lag_3`, `lag_7`, `lag_14`, `lag_21`, `lag_28`.
3. **Thống kê trượt (Rolling Statistics)**: `rolling_mean`, `rolling_std`, `rolling_max`, `rolling_min` (cửa sổ 7, 14, 28 ngày, áp dụng `shift(1)` chống rò rỉ dữ liệu).
4. **Trung bình trượt hàm mũ (EMA)**: `ema_7`, `ema_14`.
5. **Đặc trưng phân khúc**: Mã hóa số của `category`, `abc_class`, `xyz_class`, `matrix_class`.
6. **Biến mục tiêu**: `target_t_plus_1` (Nhu cầu bán ngày tiếp theo $t+1$).

---

## 5. Chạy Kiểm thử Tự động cho Tuần 4

Kiểm tra toàn bộ tính toàn vẹn của module tính năng:

```bash
python -m unittest tests/test_features.py
```

> **Kỳ vọng:** `Ran 6 tests in ... OK`.
