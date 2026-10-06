# Hướng dẫn chạy trọn vẹn luồng MLOps Tuần 5

Tài liệu này hướng dẫn cách khởi động MLflow Tracking Server, chạy pipeline trích xuất đặc trưng chuỗi thời gian (Feature Store), huấn luyện các mô hình Baseline và theo dõi thí nghiệm trên giao diện trực quan của MLflow.

---

## 1. Khởi động hạ tầng Docker (Kèm MLflow)

Cập nhật và khởi động toàn bộ cụm container (đã kích hoạt service `mlflow`):

```bash
# Khởi động các container ở chế độ nền
docker compose up -d

# Kiểm tra trạng thái các container
docker compose ps
```

> **Kỳ vọng:** Các container `zookeeper`, `kafka`, `minio`, `postgres`, và `mlflow` đều đang chạy.
> Service `mlflow` sẽ phục vụ Web UI tại địa chỉ: **`http://localhost:5000`**.

---

## 2. Tạo Feature Store cho chuỗi thời gian

Chạy pipeline tự động để tạo các đặc trưng lịch, lags, rolling statistics và nhãn phân khúc ABC/XYZ:

```bash
python ml/features/feature_pipeline.py
```

- Pipeline sẽ tự động nạp dữ liệu từ PostgreSQL Star Schema (hoặc dữ liệu Parquet cục bộ).
- File kết quả sẽ được lưu tại: **`data/features_daily.parquet`**.
- Bạn có thể kiểm tra số lượng bản ghi và các cột đặc trưng trong thông báo terminal.

---

## 3. Huấn luyện các mô hình Baseline & Logging lên MLflow

Chạy pipeline huấn luyện Walk-Forward Validation cho 5 mô hình chuẩn:

```bash
# Huấn luyện toàn bộ các mô hình và tự động log lên MLflow
python ml/training/train_baseline.py
```

### Các tùy chọn dòng lệnh nâng cao:
```bash
# Đổi tên Experiment trên MLflow
python ml/training/train_baseline.py --experiment-name my-demand-experiment

# Chỉ huấn luyện riêng mô hình LightGBM và Ridge
python ml/training/train_baseline.py --models ridge lightgbm

# Thay đổi số folds kiểm thử chéo Walk-Forward
python ml/training/train_baseline.py --n-splits 4
```

---

## 4. Xem kết quả trên giao diện MLflow Web UI

1. Mở trình duyệt web và truy cập: **`http://localhost:5000`**
2. Chọn Experiment: **`demand-forecasting-baseline`** ở thanh điều hướng bên trái.
3. Bạn sẽ thấy danh sách các Runs tương ứng với từng mô hình:
   - `baseline_naive`
   - `baseline_seasonal_naive`
   - `baseline_moving_average`
   - `baseline_ridge`
   - `baseline_lightgbm`
4. **So sánh mô hình**:
   - Chọn tất cả các runs ➔ nhấn nút **Compare**.
   - Chuyển sang biểu đồ so sánh (Bar chart / Scatter plot) của chỉ số **`cv_wape`** hoặc **`cv_mae`**.
5. **Kiểm tra Artifacts**:
   - Nhấp vào Run `baseline_lightgbm` ➔ cuộn xuống mục **Artifacts**:
     - `evaluation_plots/lightgbm_actual_vs_predicted.png`: Biểu đồ so sánh đường nhu cầu thực tế vs dự báo.
     - `evaluation_plots/lightgbm_feature_importance.png`: Top 15 đặc trưng ảnh hưởng lớn nhất tới quyết định dự báo.
     - `model/`: Tệp mô hình đã được đóng gói sẵn sàng tái sử dụng.

---

## 5. Chạy kiểm thử tự động (Unit Tests)

Chạy bộ test kiểm tra tính toán metrics và các mô hình baseline:

```bash
python tests/test_ml_pipeline.py
```

> **Kỳ vọng:** Toàn bộ các test cases đều đạt trạng thái **OK**.

