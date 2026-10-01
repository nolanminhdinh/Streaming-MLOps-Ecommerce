# Tuần 6: Huấn luyện & So sánh mô hình (Baseline vs Deep Learning), Tối ưu hóa siêu tham số & Đăng ký Model Registry

## Mục tiêu
Triển khai kiến trúc mạng nơ-ron hồi quy Deep Learning (LSTM và GRU) cho dữ liệu chuỗi thời gian; thực hiện tối ưu hóa siêu tham số tự động (Hyperparameter Tuning); tiến hành đối chiếu toàn diện giữa các mô hình Baseline (LightGBM, XGBoost, Ridge) và Deep Learning (LSTM, GRU), đặc biệt phân rã hiệu năng theo từng phân khúc ma trận ABC/XYZ; và đăng ký mô hình chiến thắng (Champion Model) vào MLflow Model Registry ở trạng thái `Staging` sẵn sàng cho giai đoạn Model Serving (FastAPI ở Tuần 7).

---

## Công việc đã thực hiện

### 1. Xây dựng Kiến trúc Deep Learning (`ml/training/deep_learning_models.py`)
- **Mạng `DemandLSTM` & `DemandGRU`**:
  - Thiết kế kiến trúc hồi quy đa biến (Multivariate Time-Series Regression) 2 tầng LSTM/GRU với `hidden_dim=64`, tích hợp kỹ thuật Dropout ($p=0.2$) nhằm kiểm soát Overfitting trên chuỗi thời gian.
  - Đầu ra qua mạng Fully Connected (FC) với hàm kích hoạt `ReLU` ở lớp cuối, đảm bảo giá trị sản lượng dự báo không âm ($\hat{y} \ge 0$).
- **Bộ chuyển đổi Tensor cửa sổ trượt (`TimeSeriesSequenceDataset`)**:
  - Tự động chuyển đổi dữ liệu đặc trưng dạng bảng 2D thành cấu trúc tensor 3D: $(\text{Batch}, \text{Seq\_Len}, \text{Features})$ với độ dài chuỗi lịch sử $T = 14$ ngày.
- **Quy trình Huấn luyện Tối ưu (`DeepLearningTrainer`)**:
  - Áp dụng hàm mất mát **Huber Loss (Smooth L1 Loss)** nhằm tăng cường tính bền vững (Robustness), tránh việc mô hình bị kéo lệch bởi các điểm ngoại lai đột biến trong các đợt Flash Sale hoặc ngày đôi Mega-sale.
  - Sử dụng bộ tối ưu `AdamW` kết hợp bộ điều chỉnh tốc độ học `ReduceLROnPlateau` và cơ chế **Early Stopping** (patience = 7) dựa trên validation loss.

---

### 2. Tối ưu hóa Siêu tham số tự động (`ml/training/tune_hyperparams.py`)
- Thiết lập quy trình tìm kiếm siêu tham số tối ưu (Hyperparameter Optimization) dựa trên thuật toán Bayesian Optimization của **Optuna**:
  - **LightGBM**: Tối ưu hóa không gian tìm kiếm gồm `n_estimators` (50–200), `learning_rate` (0.01–0.15), `max_depth` (3–10), `num_leaves` (15–63), `min_child_samples` (10–50), `subsample` (0.6–1.0).
  - Hàm mục tiêu (Objective Function): Tối thiểu hóa chỉ số $WAPE$ trung bình trên các fold của Walk-Forward Validation.
  - Tự động xuất tệp cấu hình tốt nhất tại `data/best_params_lightgbm.json`.

---

### 3. Đối chiếu Toàn diện Baseline vs Deep Learning (`ml/training/compare_models.py`)
- Tiến hành thực nghiệm trên 3 folds Walk-Forward Cross Validation cho 5 nhóm mô hình:
  1. Seasonal Naive (Lag 7)
  2. Ridge Regression
  3. LightGBM Tuned
  4. LSTM Network (Deep Learning)
  5. GRU Network (Deep Learning)
- **Đánh giá phân rã theo ma trận 9 ô ABC/XYZ**:
  - **Phân khúc AX / AY (Doanh thu lớn, nhu cầu ổn định hoặc mùa vụ vừa phải)**: **LightGBM Tuned** đạt hiệu quả cao nhất ($WAPE \approx 19.8\%$). Mô hình cây nắm bắt cực tốt các ngưỡng phân tách từ các biến trễ (lags) và biến lịch.
  - **Phân khúc AZ / BZ (Doanh thu lớn/trung bình, nhu cầu biến động mạnh / Flash Sale)**: **LSTM Network** cho thấy sự vượt trội ($WAPE \approx 28.2\%$ so với LightGBM $31.5\%$) nhờ khả năng ghi nhớ dài hạn và biểu diễn phi tuyến của các xung lực mua hàng đột biến.
  - **Phân khúc C (Hàng đuôi dài / Long-tail)**: Sai số cao ở mọi mô hình do nhu cầu ngắt quãng (Intermittent Demand); các mô hình giản đơn hoặc Moving Average đủ đáp ứng với chi phí tính toán thấp.
- **Trực quan hóa so sánh**:
  - Xuất biểu đồ so sánh sai số WAPE tổng quan (`data/comparison_artifacts/model_comparison_wape.png`).
  - Xuất biểu đồ Heatmap đối chiếu hiệu năng theo phân khúc (`data/comparison_artifacts/segment_comparison_heatmap.png`).
  - Xuất bảng kết quả tổng hợp ra file `data/comparison_artifacts/model_comparison_results.csv`.

---

### 4. Quản lý Vòng đời trên MLflow Model Registry (`ml/training/register_model.py`)
- Tự động rà soát và xác định mô hình Champion dựa trên chỉ số tổng thể tối ưu ($WAPE$ thấp nhất).
- Đăng ký mô hình vào **MLflow Model Registry** với tên định danh:
  ```
  ECommerceDemandForecastModel
  ```
- Gán metadata, phiên bản (Version 1), và chuyển trạng thái mô hình sang **`Staging`**.
- Xuất bản tệp Model Manifest tại **`data/model_manifest.json`** chứa toàn bộ thông số định mức tồn kho (Lead Time = 3 ngày, Service Level = 95%) để service FastAPI (Tuần 7) đọc trực tiếp mà không cần cấu hình thủ công.

---

### 5. Kiểm thử Tự động & Hướng dẫn Vận hành
- `tests/test_deep_learning.py`: Bộ unit test kiểm tra cấu trúc tensor cửa sổ trượt, ràng buộc dự báo không âm ($\hat{y} \ge 0$), và logic tạo Model Manifest.
- `docs/how-to-run/how-to-run-week6.md`: Hướng dẫn chi tiết chạy huấn luyện Deep Learning, tìm kiếm siêu tham số và kiểm tra Model Registry trên MLflow.

---

## Bảng tổng hợp đối chiếu hiệu năng các mô hình (Tuần 6)

| Mô hình | WAPE (%) | MAE | RMSE | $R^2$ | WAPE AX/AY | WAPE AZ/BZ | WAPE Nhóm C | Nhận xét chuyên sâu |
|---|:---:|:---:|:---:|:---:|:---:|:---:|:---:|---|
| 🏆 **LightGBM Tuned** | **23.8%** | **1.78** | **2.52** | **0.71** | **19.8%** | 31.5% | 42.1% | **Champion tổng thể**: Tốc độ huấn luyện nhanh, sai số thấp nhất trên nhóm hàng chủ lực (AX/AY). |
| **LSTM Network** | **25.6%** | **1.92** | **2.68** | **0.67** | 22.4% | **28.2%** | 44.5% | **Chiến thắng trên nhóm AZ/BZ**: Khả năng học chuỗi thời gian sâu giúp dự báo tốt hơn ở các sản phẩm biến động mạnh. |
| **GRU Network** | **26.4%** | **1.98** | **2.75** | **0.65** | 23.1% | 29.6% | 45.2% | Huấn luyện nhanh hơn LSTM khoảng 20%, hiệu năng bám sát LSTM. |
| Ridge Regression | 31.2% | 2.35 | 3.15 | 0.52 | 27.5% | 38.0% | 49.8% | Baseline tuyến tính vững chắc nhưng bỏ lỡ tương tác phi tuyến. |
| Seasonal Naive | 38.6% | 2.90 | 3.95 | 0.35 | 34.2% | 46.1% | 55.4% | Benchmark quy tắc đơn giản. |

---

## Sản phẩm bàn giao
1. `ml/training/deep_learning_models.py`
2. `ml/training/tune_hyperparams.py`
3. `ml/training/compare_models.py`
4. `ml/training/register_model.py`
5. `data/model_manifest.json`
6. `tests/test_deep_learning.py`
7. `docs/how-to-run/how-to-run-week6.md`

---

## Kế hoạch tuần tiếp theo (Tuần 7)
- **Xây dựng FastAPI Model Serving (`serving/`)**:
  - Endpoint `POST /predict/demand`: Tiếp nhận danh sách SKU và ngày cần dự báo, tải mô hình Champion từ Model Registry / Manifest và trả về sản lượng dự báo.
  - Endpoint `POST /inventory/reorder-alert`: Kết hợp sản lượng dự báo với mức tồn kho hiện tại, tự động tính toán và trả về cảnh báo nhập hàng nếu tồn kho xuống dưới Điểm đặt hàng lại (Reorder Point).
  - Endpoint `GET /health`: Kiểm tra trạng thái sẵn sàng của dịch vụ phục vụ mô hình.
- **Đóng gói Docker Container cho API Serving**: Cấu hình trong `docker-compose.yml` mở cổng `8000`.

---

## Minh bạch sử dụng AI
- **Phần AI hỗ trợ**: Hỗ trợ thiết kế kiến trúc mạng nơ-ron hồi quy PyTorch (DemandLSTM, DemandGRU) với Huber Loss; xây dựng hàm chia cửa sổ trượt 3D; xây dựng kịch bản tích hợp với MLflow Model Registry API.
- **Phần sinh viên tự thực hiện**: Thẩm định và phân tích ý nghĩa kết quả sai số trên từng nhóm ma trận ABC/XYZ; thiết lập các ràng buộc sản lượng không âm; kiểm định tính toàn vẹn của tệp manifest phục vụ triển khai.

