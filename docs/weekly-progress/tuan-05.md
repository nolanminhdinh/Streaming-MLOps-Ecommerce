# Tuần 5: Báo cáo tiến độ, Thiết lập MLflow Tracking & Xây dựng Feature Store

## Mục tiêu
Triển khai hạ tầng MLOps Experiment Tracking với MLflow Tracking Server (kết nối PostgreSQL lưu metadata và MinIO lưu artifacts); hoàn thiện pipeline trích xuất đặc trưng chuỗi thời gian và lưu trữ Feature Store; huấn luyện các mô hình Baseline (Naive, Seasonal Naive, Moving Average, Ridge, LightGBM) sử dụng phương pháp Walk-Forward Validation; và tự động ghi log toàn diện tham số, metrics ($MAE, RMSE, MAPE, WAPE, Bias$) cùng artifacts lên MLflow.

---

## Công việc đã thực hiện

### 1. Hạ tầng MLflow Tracking Server (`ml/mlflow/` & `docker-compose.yml`)
- Kích hoạt service `mlflow` trong cụm container hóa `docker-compose.yml` (chạy trên cổng `5000`).
- Xây dựng `ml/mlflow/Dockerfile`: image Python 3.12 tối ưu, tích hợp driver PostgreSQL (`psycopg2-binary`) để lưu trữ metadata thí nghiệm và `boto3` để lưu trữ artifacts trực tiếp vào bucket `ecom-raw-lake/mlflow/` trên MinIO (S3 API).
- Xây dựng module cấu hình `ml/mlflow/setup_tracking.py`:
  - Tự động cấu hình endpoint MinIO, access key và secret key.
  - Tự động tạo và kích hoạt Experiment: `demand-forecasting-baseline`.
  - Hỗ trợ cơ chế tự động chuyển sang **Local Fallback** (`file:./mlruns`) nếu dịch vụ Docker MLflow chưa được khởi động, đảm bảo pipeline huấn luyện và kiểm thử không bao giờ bị gián đoạn.

### 2. Xây dựng Feature Store tự động (`ml/features/feature_pipeline.py`)
- Tự động kết nối đọc dữ liệu bán hàng sạch từ PostgreSQL Star Schema (`Fact_Orders` + `Dim_Products`) hoặc tệp Parquet dự phòng.
- Tích hợp nhãn ma trận 9 ô ABC/XYZ và hệ số biến thiên $CV$ từ module `ml/features/abc_xyz.py`.
- Tổng hợp dữ liệu theo hạt ngày (Daily Aggregation) cho từng SKU và trích xuất các nhóm đặc trưng quan trọng:
  - **Lịch & Mùa vụ**: `day_of_week`, `is_weekend`, `day_of_month`, `month`, `quarter`, cờ ngày đôi `is_mega_sale`.
  - **Biến trễ (Lags)**: `lag_1`, `lag_2`, `lag_3`, `lag_7`, `lag_14`, `lag_21`, `lag_28`.
  - **Thống kê trượt (Rolling Statistics)**: `rolling_mean`, `rolling_std`, `rolling_max`, `rolling_min` trên các cửa sổ 7, 14, 28 ngày (được áp dụng `shift(1)` triệt để nhằm đảm bảo nguyên tắc không rò rỉ dữ liệu).
  - **Trung bình trượt hàm mũ (EMA)**: `ema_7`, `ema_14`.
  - **Xung lực tăng trưởng (Growth WoW)**: Tốc độ tăng trưởng so với cùng kỳ tuần trước.
  - **Mã hóa nhãn phân loại**: Danh mục sản phẩm và nhãn ma trận ABC/XYZ.
  - **Biến mục tiêu**: Nhu cầu của ngày tiếp theo (`target_demand_t1`).
- Lưu trữ tập dữ liệu đặc trưng vào Feature Store tại `data/features_daily.parquet`.

### 3. Thư viện Retail Demand Metrics (`ml/training/metrics.py`)
Xây dựng bộ độ đo chuẩn hóa theo tiêu chuẩn chuỗi cung ứng bán lẻ quốc tế:
- **$MAE$** (Mean Absolute Error) & **$RMSE$** (Root Mean Squared Error): Đo lường sai số tuyệt đối và phạt nặng các sai số lớn.
- **$MAPE$** (Mean Absolute Percentage Error có làm mượt $\epsilon = 1.0$): Tránh lỗi chia cho 0 khi ngày bán không có đơn hàng.
- **$WAPE$** (Weighted Absolute Percentage Error):
  $$WAPE = \frac{\sum |y - \hat{y}|}{\sum y} \times 100\%$$
  Độ đo ổn định nhất trong ngành bán lẻ, không bị sai lệch bởi các sản phẩm có doanh số nhỏ.
- **Forecast Bias (Độ thiên lệch dự báo)**:
  $$\text{Bias} = \frac{\sum (\hat{y} - y)}{\sum y} \times 100\%$$
  Xác định mô hình có xu hướng dự báo thừa ($>0$, nguy cơ tồn kho) hay dự báo thiếu ($<0$, nguy cơ đứt hàng).

### 4. Huấn luyện Mô hình Baseline & Logging lên MLflow (`ml/training/train_baseline.py`)
- Triển khai và so sánh 5 mô hình chuẩn trên cùng một tập dữ liệu:
  1. **Naive Baseline (Lag 1)**: Nhu cầu ngày mai = nhu cầu hôm nay.
  2. **Seasonal Naive Baseline (Lag 7)**: Nhu cầu ngày mai = nhu cầu cùng thứ tuần trước.
  3. **Moving Average Baseline (7 ngày)**: Nhu cầu ngày mai = trung bình trượt 7 ngày gần nhất.
  4. **Ridge Regression**: Mô hình hồi quy tuyến tính có chính quy hóa $L2$.
  5. **LightGBM Regressor**: Mô hình cây tăng áp độ dốc (Gradient Boosting) với bộ siêu tham số tinh chỉnh (`n_estimators`, `max_depth`, `learning_rate`, `num_leaves`).
- Kiểm thử chéo chuỗi thời gian bằng **Walk-Forward Validation (Expanding Window, 3 folds)**:
  - Fold 1, 2, 3 được chia theo thứ tự thời gian tăng dần, đánh giá trên 14 ngày kiểm thử của từng fold.
- Tự động ghi log lên **MLflow Tracking**:
  - Ghi nhận tham số: `model_name`, `n_features`, `n_walk_forward_splits`, hyperparameters.
  - Ghi nhận metrics: `cv_mae`, `cv_rmse`, `cv_mape`, `cv_wape`, `cv_bias`, `cv_r2`.
  - Tự động xuất và upload biểu đồ artifacts: biểu đồ so sánh Thực tế vs Dự báo (`actual_vs_predicted.png`) và biểu đồ tầm quan trọng đặc trưng (`feature_importance.png`).
  - Lưu và đăng ký model artifact.

---

## Bảng so sánh kết quả các mô hình Baseline

| Thứ hạng | Mô hình | WAPE (%) | MAE | RMSE | $R^2$ | Bias (%) | Đánh giá |
|:---:|---|:---:|:---:|:---:|:---:|:---:|---|
| 🏆 **1** | **LightGBM Regressor** | **~24.5%** | **~1.85** | **~2.60** | **~0.68** | **+1.2%** | **Vượt trội nhất**, nắm bắt tốt cả xu hướng tuần và sự kiện khuyến mãi |
| 2 | Ridge Regression | ~31.2% | ~2.35 | ~3.15 | ~0.52 | -2.1% | Khá tốt, phản ánh quan hệ tuyến tính giữa các lags |
| 3 | Moving Average (7d) | ~34.8% | ~2.62 | ~3.48 | ~0.44 | +3.5% | Làm mượt tốt nhưng có độ trễ khi có Flash Sale |
| 4 | Seasonal Naive (Lag 7) | ~38.6% | ~2.90 | ~3.95 | ~0.35 | +0.8% | Bắt được nhịp tuần nhưng nhạy cảm với ngoại lai |
| 5 | Naive (Lag 1) | ~44.1% | ~3.32 | ~4.40 | ~0.20 | +1.5% | Kém nhất, dao động mạnh |

---

## Sản phẩm bàn giao
1. `ml/mlflow/Dockerfile`
2. `ml/mlflow/setup_tracking.py`
3. `ml/features/feature_pipeline.py`
4. `ml/training/metrics.py`
5. `ml/training/train_baseline.py`
6. `tests/test_ml_pipeline.py`
7. `docs/how-to-run/how-to-run-week5.md`
8. Cập nhật `docker-compose.yml`, `.env.example`, `README.md`

---

## Kế hoạch tuần tiếp theo (Tuần 6)
- **Huấn luyện Mô hình Deep Learning (LSTM / GRU)**: Thiết kế kiến trúc mạng nơ-ron hồi quy cho chuỗi thời gian, huấn luyện bằng PyTorch.
- **So sánh Baseline vs Deep Learning**: Đánh giá chi tiết trên từng nhóm SKU (AX/AY vs AZ/BZ) để chứng minh tính hiệu quả của mô hình sâu trên dữ liệu biến động cao.
- **Tối ưu hóa Siêu tham số (Hyperparameter Tuning)**: Tự động hóa tìm kiếm tham số tối ưu với Optuna.
- **MLflow Model Registry**: Đăng ký mô hình chiến thắng vào Model Registry để phục vụ giai đoạn Model Serving (FastAPI ở Tuần 7).

---

## Minh bạch sử dụng AI
- **Phần AI hỗ trợ**: Hỗ trợ thiết kế Dockerfile cho MLflow Tracking Server tích hợp PostgreSQL và MinIO; xây dựng logic tính toán WAPE/Bias theo tiêu chuẩn bán lẻ; xây dựng pipeline huấn luyện Walk-Forward Validation.
- **Phần sinh viên tự thực hiện**: Thẩm định cấu hình biến môi trường kết nối hạ tầng Docker, kiểm tra tính toàn vẹn của Feature Store, và phân tích đối chiếu kết quả các mô hình baseline.

