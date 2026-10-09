# Hướng dẫn chạy trọn vẹn luồng MLOps Tuần 6

Tài liệu này hướng dẫn cách thực hiện tối ưu hóa siêu tham số (Hyperparameter Tuning), huấn luyện & đối chiếu các mô hình Deep Learning (LSTM, GRU) với mô hình Baseline (LightGBM, Ridge, Seasonal Naive) theo phân khúc ma trận ABC/XYZ, và tự động đăng ký mô hình Champion vào **MLflow Model Registry** ở trạng thái `Staging`.

---

## 1. Yêu cầu tiên quyết (Prerequisites)

1. Đảm bảo cụm Docker services đang hoạt động (đặc biệt là MLflow Tracking Server và MinIO):
   ```bash
   docker compose up -d
   docker compose ps
   ```
   > MLflow Web UI sẵn sàng tại: **`http://localhost:5000`**.

2. Đã có tệp dữ liệu đặc trưng chuỗi thời gian `data/features_daily.parquet` (được sinh từ Tuần 5):
   ```bash
   # Nếu chưa có, chạy lệnh sau để trích xuất đặc trưng:
   python ml/features/feature_pipeline.py
   ```

---

## 2. Tối ưu hóa siêu tham số bằng Optuna (Hyperparameter Tuning)

Chạy kịch bản tìm kiếm siêu tham số tối ưu cho mô hình cây (LightGBM) thông qua thuật toán Bayesian Optimization:

```bash
# Chạy tối ưu hóa với 20 trials (mặc định)
python ml/training/tune_hyperparams.py --n-trials 20
```

- Kịch bản sẽ tối ưu hóa chỉ số $WAPE$ trung bình qua 3-fold Walk-Forward Validation.
- Kết quả tham số tối ưu nhất được tự động ghi nhận tại: **`data/best_params_lightgbm.json`**.

---

## 3. Huấn luyện và Đối chiếu Toàn diện các Mô hình (Model Comparison)

Thực hiện huấn luyện và kiểm thử chéo Walk-Forward đồng thời cho 5 nhóm mô hình (Seasonal Naive, Ridge, LightGBM Tuned, LSTM, GRU), phân rã hiệu năng theo từng phân khúc ma trận ABC/XYZ:

```bash
# Huấn luyện và so sánh toàn bộ các mô hình
python ml/training/compare_models.py
```

### Các tùy chọn dòng lệnh nâng cao:
```bash
# Chỉ chạy so sánh các mô hình cụ thể
python ml/training/compare_models.py --models lightgbm_tuned lstm gru

# Thay đổi độ dài chuỗi lịch sử (sequence length) cho Deep Learning (mặc định: 14 ngày)
python ml/training/compare_models.py --seq-len 14 --epochs 25
```

### Kết quả & Biểu đồ trực quan hóa được xuất tại:
- `data/comparison_artifacts/model_comparison_results.csv`: Bảng tổng hợp các chỉ số WAPE, MAE, RMSE, $R^2$ tổng thể và phân khúc.
- `data/comparison_artifacts/model_comparison_wape.png`: Biểu đồ cột so sánh sai số WAPE giữa các mô hình.
- `data/comparison_artifacts/segment_comparison_heatmap.png`: Heatmap đối chiếu hiệu năng theo từng phân khúc ma trận (AX/AY, AZ/BZ, C).

---

## 4. Tạo run có artifact và đăng ký vào MLflow Model Registry

`compare_models.py` chỉ ghi metrics so sánh; các run đó không chứa artifact model để Serving tải. Trước khi train, nạp dữ liệu thật vào `Fact_Orders` và tạo Feature Store từ warehouse. Hãy log model và `feature_spec.json` có `data_source=warehouse`, rồi đăng ký từ cùng experiment:

```bash
# Ghi các run, model artifact và feature spec vào experiment triển khai
python ml/training/train_baseline.py --experiment-name demand-forecasting-baseline

# Đăng ký run hợp lệ có artifact vào Registry ở stage Staging
python ml/training/register_model.py --experiment-name demand-forecasting-baseline --stage Staging
```

- Mô hình được đăng ký vào Registry với tên định danh: **`ECommerceDemandForecastModel`**.
- Trạng thái được chuyển sang: **`Staging`** (sẵn sàng kiểm thử serving).
- Xuất bản tệp Model Manifest cấu hình triển khai tại: **`data/model_manifest.json`**.
  - Tệp này chứa các thông số định mức tồn kho (Lead Time, Service Level, Safety Stock multiplier) và vị trí artifact mô hình để FastAPI service ở Tuần 7 đọc trực tiếp.

---

## 5. Xem kết quả trên giao diện MLflow Web UI

1. Mở trình duyệt web truy cập: **`http://localhost:5000`**
2. **Tab Experiments**:
   - Chọn Experiment `demand-forecasting-baseline` để xem các run có artifact triển khai.
   - Experiment `demand-forecasting-model-comparison` dùng để xem metrics so sánh, không dùng trực tiếp để đăng ký artifact.
   - Kiểm tra metrics `cv_wape`, `cv_mae`, `cv_rmse` cùng thư mục `model/` có model và `feature_spec.json`.
   - Xem các biểu đồ so sánh trong mục Artifacts của Run.
3. **Tab Models (Model Registry)**:
   - Nhấp vào tab **Models** trên thanh menu điều hướng trên cùng.
   - Chọn mô hình **`ECommerceDemandForecastModel`**.
   - Kiểm tra version mới được gán nhãn **Staging** sau khi Registry hoàn tất đăng ký.

---

## 6. Chạy bộ kiểm thử tự động (Unit Tests)

Chạy unit tests cho các thành phần Deep Learning, Sliding Window tensor, và Model Registry:

```bash
# Kiểm tra riêng các module Tuần 6
python -m unittest tests/test_deep_learning.py

# Hoặc kiểm tra toàn bộ test suite dự án
python -m unittest discover tests
```

> **Kỳ vọng:** Toàn bộ các test cases đều đạt trạng thái **OK**.

