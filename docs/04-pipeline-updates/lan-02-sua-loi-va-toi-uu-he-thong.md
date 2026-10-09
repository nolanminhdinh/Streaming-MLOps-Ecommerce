# BÁO CÁO SỬA LỖI & TỐI ƯU HÓA HỆ THỐNG — LẦN 2

> **Ghi chú lịch sử:** Kết quả 72/72 tests là kết quả được ghi tại thời điểm Lần 2. Báo cáo runtime ngày 2026-10-09 sau đó ghi 71 passed, 1 failed, 1 warning; xem [baseline mới hơn](../03-testing-and-benchmark/test_report_2026-10-09.md) và [khắc phục Lần 3](lan-03-e2e-remediation-2026-10-09.md).

> **Mã báo cáo**: `PIPE-UPDATE-RUN-02`
> **Thời điểm cập nhật**: 06/10/2026
> **Phiên bản hệ thống**: `v1.1.0` → `v1.2.0` (End-to-end Data Correctness & Honest Serving)
> **Phạm vi**: Rà soát toàn hệ thống — Ingestion, ETL/Star Schema, Feature Store, Training/Registry, Serving, Monitoring/Retraining, hạ tầng Docker
> **Kết quả kiểm thử tự động**: **72/72 tests PASS**, gồm 58 test cũ và 14 test mới (trước đợt sửa: 58/58 PASS)
> **Ghi chú minh bạch AI**: Đợt rà soát và sửa lỗi này được thực hiện với sự hỗ trợ của Claude (Anthropic).

---

## 1. TỔNG QUAN ĐỢT CẬP NHẬT LẦN 2

Lần 1 tập trung vào **hiệu năng** của luồng dữ liệu. Lần 2 là một đợt **rà soát tính đúng đắn đầu–cuối (end-to-end correctness)**, với câu hỏi trung tâm:

> *"Con số mà API dự báo và cảnh báo tồn kho trả ra có thực sự đi ra từ dữ liệu trong kho và từ mô hình đã huấn luyện không?"*

Trước đợt sửa, câu trả lời là **không**. Bộ test vẫn pass 100% vì các test chỉ kiểm tra *hình dạng* của kết quả (không âm, đúng số ngày, đúng định dạng), không kiểm tra *nguồn gốc* của kết quả. Đợt rà soát phát hiện **15 lỗi**, chia theo mức độ:

| Mức độ | Số lỗi | Ảnh hưởng chính |
|---|:---:|---|
| 🔴 Nghiêm trọng | 7 | Sai dữ liệu Power BI, serving không dùng dữ liệu thật, vòng retrain "giả", artifact mô hình sai |
| 🟠 Trung bình | 5 | Data leakage, lệch ngày Mega-sale, lệch múi giờ, trạng thái đơn không cập nhật |
| 🟡 Vận hành | 3 | Ghi đè file Data Lake, tham số bị bỏ qua, MLflow dùng chung DB với kho dữ liệu |

Kèm theo đó là **7 cải tiến tối ưu**: metrics Prometheus chuẩn có histogram độ trễ, cache và circuit breaker cho truy vấn kho, endpoint reload mô hình, hợp đồng đặc trưng (`feature_spec.json`), v.v.

### Sơ đồ luồng sau khi sửa (các đoạn mới được nối thông được in đậm)

```
Simulator ─► Kafka ecom.orders.raw ─► consumer_to_minio ─► MinIO (partition theo giờ VN)
    │                                                            │
    └─► **Kafka inventory.logs** ─► **consumer_inventory_to_postgres** ─► **Fact_Inventory_Daily**
                                                                 ▼
                                    ETL ─► Fact_Orders (**date_key đã có giá trị**)
                                                                 ▼
                         feature_pipeline ─► features_daily.parquet + **feature_spec.json**
                                                                 ▼
                 train_baseline (**fit lại trên toàn bộ dữ liệu**) ─► MLflow Registry
                                                                 ▼
  FastAPI ◄── **lịch sử nhu cầu thật (Fact_Orders) + tồn kho thật (Fact_Inventory_Daily)**
     ▲                                                           │
     └── **POST /model/reload** ◄── trigger_retraining ◄── drift_detector (**dữ liệu thật**)
```

---

## 2. NHẬT KÝ CHI TIẾT LỖI & PHÂN TÍCH NGUYÊN NHÂN GỐC (RCA)

### 🔴 BUG-01 — PostgreSQL nằm sai mạng Docker

- **Triệu chứng**: Khi chạy cả stack bằng `docker compose up`, MLflow, FastAPI và NiFi có thể không resolve được hostname `postgres`.
- **Nguyên nhân gốc**: Mọi service đều khai báo `networks: [ecom-net]`, **trừ** `postgres`. Service không khai báo mạng sẽ chỉ tham gia mạng `default` của project. Các script chạy trên máy host vẫn kết nối qua `localhost:5432`, nên lỗi không lộ ra khi test thủ công.
- **Cách sửa**: Thêm `networks: [ecom-net]` cho `postgres`. Đồng thời dùng `depends_on: condition: service_healthy` cho FastAPI.
- **Tệp**: `docker-compose.yml`

### 🔴 BUG-02 — `Fact_Orders.date_key` luôn NULL → các view Power BI rỗng

- **Triệu chứng**: Mọi view trong `powerbi/views_for_powerbi.sql` đều `JOIN Dim_Dates ON f.date_key = d.date_key`, nên với dữ liệu nạp qua `pipeline.py` các view không trả về dòng nào.
- **Nguyên nhân gốc**: Cột `order_date` (khóa để tra `date_key`) chỉ được tạo trong hàm `transform()`. Trong khi đó `warehouse/etl/pipeline.py` gọi thẳng `unify_schema()` + `clean_data()` mà không gọi `transform()`, nên `load_fact_orders` luôn nhận `order_date = None`. Script benchmark có tự bù cột này (`run_heavy_pipeline_benchmark.py:186`), vì vậy các số liệu benchmark không phản ánh lỗi.
- **Cách sửa**: Chuyển việc sinh `order_date` vào `clean_data()` để mọi luồng gọi đều có cột này.
- **Tệp**: `warehouse/etl/transform.py`
- **Kiểm chứng**: `tests/test_etl.py::test_clean_data_business_timezone_and_order_date`

### 🔴 BUG-03 — Serving không dùng dữ liệu thật và lệch đặc trưng giữa lúc train và lúc serve (training–serving skew)

- **Triệu chứng**: Dự báo của mọi SKU chỉ phụ thuộc vào một bảng 20 SKU hardcode, không phụ thuộc vào lịch sử bán hàng trong kho.
- **Nguyên nhân gốc** (`serving/app/model_loader.py` bản cũ):
  1. Danh mục SKU, `base_demand` và `sigma` được hardcode trong `CATALOG_METADATA`.
  2. `predict_range()` không truyền `history_demand`, nên mọi lag và rolling đều bằng hằng số `base_demand`.
  3. Ngay cả khi có lịch sử, `lag_2 = lag_3 = lag_1` và `lag_14 = lag_28 = lag_7`. Thêm vào đó `daily_demand` và `lag_1` cùng lấy `history[-1]`, tức **lệch 1 ngày** so với định nghĩa lúc huấn luyện (`daily_demand` = y[t], `lag_1` = y[t−1]).
  4. Khi suy luận lỗi, code chuyển sang công thức heuristic và chỉ ghi log ở mức `DEBUG`, trong khi `/health` vẫn báo `healthy` vì `is_loaded = True` kể cả khi `model is None`.
- **Cách sửa**:
  - `serving/app/features.py` (mới): dựng vector đặc trưng của ngày t **theo đúng định nghĩa** của `TimeSeriesFeatureExtractor` (lag, rolling có shift(1), EMA `adjust=False`, growth WoW, lịch, mã hóa phân khúc).
  - `serving/app/data_access.py` (mới): đọc danh mục từ `Dim_Products`, chuỗi nhu cầu liên tục theo ngày từ `Fact_Orders` (ngày trống điền 0 giống `aggregate_daily`) và tồn kho từ `Fact_Inventory_Daily`.
  - `ModelManager.forecast()`: **dự báo đệ quy**, trong đó giá trị dự báo của ngày trước được dùng làm lag cho ngày sau, tính từ ngày cuối cùng có dữ liệu.
  - Mọi response đều có `model_source` (`mlflow_registry` / `local_joblib` / `heuristic_history` / `heuristic_catalog`) và `history_source`. Lỗi suy luận được log ở mức `WARNING`. `/health` trả `degraded` khi đang chạy heuristic.
- **Kiểm chứng**:
  - `TestOnlineFeatureParity`: so sánh từng cột đặc trưng dựng online với pipeline huấn luyện tại 9 thời điểm, gồm cả các biên t=0, t=7, t=28. Sai số ≤ 1e‑9.
  - `test_first_step_matches_feature_store_prediction`: dự báo ngày d+1 qua API **trùng khớp** với `model.predict` trên dòng Feature Store của ngày d.

### 🔴 BUG-04 — Artifact mô hình được lưu là mô hình của fold *yếu nhất*

- **Nguyên nhân gốc**: `evaluate_walk_forward()` duyệt các fold từ cửa sổ test mới nhất về cũ nhất, mỗi fold lại gọi `fit()` trên cùng một `model_obj`. Sau vòng lặp, `model_obj` mang trọng số của **fold cuối**, tức fold có tập huấn luyện ngắn nhất. Đối tượng này được `joblib.dump` và đẩy lên MLflow. Hệ quả là mô hình đem đi serving yếu hơn mô hình đứng sau các metric đã báo cáo.
- **Cách sửa**: Sau khi đánh giá, fit lại `model_obj` trên **toàn bộ** Feature Store rồi mới lưu artifact. Đồng thời log `feature_spec.json` vào cùng thư mục `model/` của run.
- **Tệp**: `ml/training/train_baseline.py`

### 🔴 BUG-05 — Serving không bao giờ nạp được mô hình từ MLflow Registry

- **Nguyên nhân gốc**: Version trong Registry trỏ tới `runs:/<id>/model`, nhưng thư mục đó chỉ chứa file `*.joblib` (ghi bằng `log_artifact`), không phải một MLmodel. Vì vậy `mlflow.pyfunc.load_model("models:/...")` luôn lỗi, và lỗi bị nuốt ở mức `DEBUG`.
- **Cách sửa**: Dùng `MlflowClient.get_latest_versions()` để lấy `source`, sau đó `mlflow.artifacts.download_artifacts()`, rồi `joblib.load` file mô hình (ưu tiên `model_file` ghi trong manifest) và nạp `feature_spec.json` đi kèm. Đặt `MLFLOW_HTTP_REQUEST_MAX_RETRIES=0` để API không bị treo hàng phút khi MLflow tắt.
- **Tệp**: `serving/app/model_loader.py`, `ml/training/register_model.py` (ghi thêm `run_id` và `model_file` vào manifest)

### 🔴 BUG-06 — Cảnh báo tồn kho dùng số tồn kho giả lập; topic `inventory.logs` và bảng `Fact_Inventory_Daily` bị bỏ trống

- **Nguyên nhân gốc**: Topic `inventory.logs` được tạo trong `kafka-init` nhưng **không có producer hay consumer nào** dùng tới. Bảng `Fact_Inventory_Daily` vì thế không bao giờ có dữ liệu. `InventoryService` tự gán `current_stock` theo phân khúc ("AZ" luôn rơi vào CRITICAL, "AY" luôn WARNING), dù docstring ghi là đọc từ `Fact_Inventory_Daily`.
- **Cách sửa**:
  - `ingestion/producer.py`: định kỳ gửi snapshot tồn kho từ chính simulator đang sinh đơn (`INVENTORY_SNAPSHOT_INTERVAL`, mặc định 60 giây) lên `inventory.logs`.
  - `ingestion/consumer_inventory_to_postgres.py` (mới): upsert vào `Fact_Inventory_Daily` theo `UNIQUE (product_key, date_key)`, chỉ commit offset sau khi DB đã commit.
  - `InventoryService`: tồn kho lấy theo thứ tự ưu tiên *số gửi lên trong request* → *snapshot trong kho* → *giả lập demo*. Nhu cầu `d` và `σ` lấy từ *dự báo mô hình* → *lịch sử 28 ngày* → *bảng demo*. Mỗi dòng cảnh báo ghi rõ `stock_source` và `demand_source`.
- **Kiểm chứng**: `test_inventory_uses_warehouse_stock_and_model_demand`

### 🔴 BUG-07 — Vòng lặp drift → retrain là "giả"

- **Nguyên nhân gốc**:
  1. `run_drift_pipeline()` luôn dùng `generate_synthetic_drift_data(inject_drift=True)`, nên lần nào chạy cũng phát hiện drift.
  2. `trigger_retraining.py`: nếu huấn luyện lỗi thì vẫn ghi metrics hardcode (21.8 / 1.62 / 2.35), vẫn tăng version manifest và vẫn ghi audit `SUCCESS`. Mô hình mới không được đăng ký lên Registry, và serving không được báo để nạp lại.
  3. `register_model.find_best_run()` trả về metrics mặc định (24.5 / 1.85 / 2.60) khi không có run nào. Test cũ còn ghi manifest giả này vào thư mục `data/` của repo.
- **Cách sửa**:
  - Drift mặc định chạy trên **Feature Store thật**: Reference là 28 ngày, Current là 7 ngày gần nhất. Chế độ giả lập chỉ chạy khi gọi `--synthetic`, và summary ghi `data_source`. Summary giả lập không tự kích hoạt retrain.
  - Retrain: dựng lại Feature Store, huấn luyện, đăng ký qua `register_model`, ghi manifest, rồi gọi `POST /model/reload`. **Bất kỳ bước nào lỗi thì trả `FAILED` và giữ nguyên manifest/mô hình đang chạy.**
  - `register_model`: không có run thì trả `{}` và không ghi manifest.
- **Kiểm chứng**: `test_failed_training_keeps_current_model`, `test_synthetic_drift_does_not_auto_retrain`, `test_windows_split_by_date`, `test_register_without_runs_writes_nothing`

### 🟠 BUG-08 — Rò rỉ dữ liệu (data leakage) qua ABC/XYZ

- **Nguyên nhân gốc**: `abc_class`, `xyz_class`, `cv` và `safety_stock` được tính trên **toàn bộ** dữ liệu, kể cả các ngày thuộc cửa sổ test của walk-forward, rồi dùng làm đặc trưng. Ví dụ `cv` chứa độ biến động của chính giai đoạn được dùng để chấm điểm mô hình.
- **Cách sửa**: Chỉ fit ABC/XYZ trên dữ liệu **trước vùng holdout** (mặc định 49 ngày = 3 fold × 14 ngày + 7 ngày val). Nếu dữ liệu quá ngắn, fit trên nửa đầu và ghi cảnh báo. SKU xuất hiện sau mốc này được gán C/Z. Mốc cutoff được ghi vào `feature_spec.json`.
- **Tệp**: `ml/features/feature_pipeline.py`

### 🟠 BUG-09 — Mã hóa phân loại không tái lập được ở serving

- **Nguyên nhân gốc**: `astype("category").cat.codes` gán mã theo thứ tự xuất hiện trong dữ liệu và không được lưu lại, nên serving không thể dựng lại `category_code`, `abc_class_code`, v.v.
- **Cách sửa**: Dùng từ điển cố định (A/B/C, X/Y/Z, 9 ô ma trận; `category` sắp theo thứ tự chữ cái) và lưu vào `feature_spec.json` cùng danh sách cột theo đúng thứ tự huấn luyện. `train_baseline.select_feature_columns` dùng chung định nghĩa `EXCLUDE_COLS` với feature pipeline.

### 🟠 BUG-10 — Hiệu ứng Mega-sale bị lệch 1 ngày

- **Nguyên nhân gốc**: Mỗi dòng ngày t dự báo `target_t_plus_1`, nhưng các đặc trưng lịch (`is_mega_sale`, `day_of_week`, ...) lại mô tả ngày t. Vì vậy mô hình không biết **ngày được dự báo** có phải 10/10 hay 11/11 hay không.
- **Cách sửa**: Thêm `target_day_of_week`, `target_is_weekend`, `target_day_of_month`, `target_is_mega_sale` mô tả ngày t+1. Các cột cũ được giữ lại để tương thích.
- **Kiểm chứng**: `tests/test_features.py::test_target_calendar_describes_forecast_day`
- **Lưu ý trung thực**: Trên tập mô phỏng 62 ngày (xem mục 4), WAPE gần như không đổi (40.42% → 40.78%). Lý do là tập này chỉ có **1** ngày Mega-sale (8/9), và ngày đó không nằm trong cửa sổ test. Lợi ích chỉ đo được trên tập dữ liệu chứa nhiều ngày đôi trong vùng đánh giá.

### 🟠 BUG-11 — Lệch múi giờ giữa phân vùng Data Lake, ETL và lịch nghiệp vụ

- **Nguyên nhân gốc**: Consumer phân vùng MinIO theo **ngày UTC**, ETL mặc định lấy "hôm nay UTC", còn timestamp nạp vào cột `TIMESTAMP` mang giờ UTC. Hệ quả là đơn đặt từ 0h đến 7h sáng giờ Việt Nam rơi vào ngày hôm trước, sai cả `Dim_Dates` lẫn ngày Mega-sale.
- **Cách sửa**: Đưa vào biến `BUSINESS_TZ` (mặc định `Asia/Ho_Chi_Minh`) dùng thống nhất cho phân vùng MinIO, ngày mặc định của ETL và việc quy đổi timestamp trước khi load. Timestamp không có offset được coi là UTC.
- **Tệp**: `ingestion/consumer_to_minio.py`, `warehouse/etl/transform.py`, `warehouse/etl/pipeline.py`

### 🟠 BUG-12 — Trạng thái đơn hàng không bao giờ được cập nhật

- **Nguyên nhân gốc**: `ON CONFLICT (order_id, platform) DO NOTHING` nên một đơn nạp lần đầu ở trạng thái PAID sẽ giữ trạng thái đó mãi, kể cả khi lần ETL sau đơn đã CANCELLED.
- **Cách sửa**: Đổi sang `DO UPDATE` cho các cột vòng đời của đơn (`order_status`, `is_cancelled`, các mốc thời gian, `cancel_reason`). Các cột tiền và khóa dimension giữ nguyên.
- **Tệp**: `warehouse/etl/load.py`

### 🟡 BUG-13 — Nguy cơ ghi đè file Parquet trên MinIO

- **Nguyên nhân gốc**: Tên file có dạng `part-{giây}-{số_dòng}.parquet`, nên hai lần flush trong cùng một giây với cùng số dòng (hoặc nhiều consumer chạy song song) sẽ ghi đè nhau và **mất dữ liệu âm thầm**.
- **Cách sửa**: Thêm hậu tố ngẫu nhiên `batch_id` (uuid) vào tên file.

### 🟡 BUG-14 — Tham số `--batch-size` của ETL bị bỏ qua

- **Nguyên nhân gốc**: `run_etl_pipeline(batch_size=...)` không truyền tham số xuống `load()`, nên hệ thống luôn dùng giá trị mặc định 5000.
- **Cách sửa**: Truyền `batch_size` xuống `load()` và tiếp tới `load_fact_orders()`.

### 🟡 BUG-15 — MLflow dùng chung database với kho dữ liệu

- **Nguyên nhân gốc**: `MLFLOW_BACKEND_STORE_URI` trỏ vào `ecom_warehouse`, nên khoảng 20 bảng nội bộ của MLflow nằm lẫn với Star Schema.
- **Cách sửa**: Tạo database riêng `mlflow`, bằng `warehouse/ddl/00_create_mlflow_db.sql` khi khởi tạo volume mới, hoặc bằng `scripts/init_warehouse.py` với volume đã có.

---

## 3. DANH MỤC CẢI TIẾN & TỐI ƯU HÓA

| # | Cải tiến | Chi tiết | Tệp |
|---|---|---|---|
| OPT-01 | **Metrics Prometheus chuẩn** | Thay chuỗi metrics tự ghép tay bằng `prometheus_client`: counter an toàn đa luồng (dict cũ bị race condition khi endpoint chạy trong threadpool), **histogram độ trễ** `ecommerce_api_request_duration_seconds` theo route template (đủ để tính p95/p99), `ecommerce_model_ready`, và `ecommerce_predictions_total{model_source}` để phát hiện fallback. Tên metric cũ được giữ nguyên nên dashboard Grafana hiện tại vẫn chạy. | `serving/app/main.py` |
| OPT-02 | **Cache + circuit breaker cho truy vấn kho** | Cache TTL 60 giây cho lịch sử theo (sku, ngày), 300 giây cho danh mục, có giới hạn kích thước. Khi DB lỗi, repository ngắt kết nối trong 30 giây để request sau không phải chờ timeout. | `serving/app/data_access.py` |
| OPT-03 | **Đọc tồn kho 1 lần / lượt quét** | `evaluate_all()` đọc snapshot tồn kho một lần cho toàn bộ danh mục thay vì một truy vấn cho mỗi SKU. | `serving/app/inventory_service.py` |
| OPT-04 | **Hợp đồng đặc trưng `feature_spec.json`** | Lưu danh sách cột theo thứ tự, từ điển mã hóa, hồ sơ phân khúc SKU và mốc cutoff. File này được log cùng mô hình lên MLflow. | `ml/features/feature_pipeline.py` |
| OPT-05 | **`POST /model/reload`** | Nạp lại mô hình mà không cần restart container. Có thể bảo vệ bằng `MODEL_RELOAD_TOKEN`. Retrainer gọi endpoint này qua `SERVING_RELOAD_URL`. | `serving/app/main.py` |
| OPT-06 | **Hạ tầng Docker** | Thêm volume cho Prometheus/Grafana (không còn mất dữ liệu khi restart), `restart: unless-stopped` cho MLflow/FastAPI, đưa mật khẩu Grafana/NiFi vào biến môi trường, bỏ khóa `version:` đã lỗi thời. | `docker-compose.yml` |
| OPT-07 | **Dependencies & CORS** | `kafka-python==2.0.2` (không còn bảo trì, lỗi import trên Python 3.12) được thay bằng `kafka-python-ng`. Bỏ `confluent-kafka` vì không được dùng. CORS cấu hình qua `CORS_ALLOW_ORIGINS`, và `allow_credentials` chỉ bật khi liệt kê origin cụ thể. | `requirements.txt`, `serving/` |

---

## 4. ĐO LƯỜNG ĐỐI CHỨNG TRƯỚC & SAU

### 4.1. Kiểm thử tự động

| Chỉ số | Trước | Sau |
|---|:---:|:---:|
| Tổng số test | 58 | **72** |
| Kết quả | 58 PASS | **72 PASS** |
| Có test kiểm chứng parity train ↔ serve | ✗ | ✓ (sai số ≤ 1e‑9) |
| Có test kiểm chứng dự báo dùng dữ liệu kho | ✗ | ✓ |
| Có test kiểm chứng retrain lỗi thì không thay mô hình | ✗ | ✓ |
| Test có ghi file giả vào `data/` của repo | Có (manifest với metrics bịa) | Không (dùng thư mục tạm) |

Môi trường kiểm thử: Python 3.12, venv riêng (pandas 2.2.2, numpy 1.26.4, lightgbm 4.4.0, fastapi 0.111.0). Test không cần PostgreSQL/MLflow nhờ repository giả (`tests/conftest.py`, `tests/test_serving_realdata.py`).

### 4.2. Hành vi hệ thống

| Hành vi | Trước | Sau |
|---|---|---|
| `Fact_Orders.date_key` khi chạy `pipeline.py` | NULL 100% | Có giá trị theo ngày giờ VN |
| Nguồn lag/rolling khi dự báo | Hằng số `base_demand` | Lịch sử thật từ `Fact_Orders`, dự báo đệ quy |
| Nguồn `current_stock` | Gán cứng theo phân khúc | `Fact_Inventory_Daily` (ghi rõ nguồn) |
| `/health` khi không có mô hình | `healthy` | `degraded` + `model_source` |
| Drift detection | Luôn chạy trên dữ liệu giả có tiêm drift | Feature Store thật (giả lập chỉ khi `--synthetic`) |
| Retrain khi huấn luyện lỗi | Vẫn tăng version, ghi metrics bịa, audit SUCCESS | `FAILED`, giữ nguyên mô hình |
| Artifact mô hình | Mô hình của fold ngắn nhất | Fit trên toàn bộ dữ liệu |

### 4.3. Thực nghiệm chạy thật trên dữ liệu simulator

Chạy `ml/features/feature_pipeline.py` trên 10.687 đơn mô phỏng (62 ngày, 20 SKU). Lần chạy này **không** có PostgreSQL nên pipeline dùng nhánh tự sinh dữ liệu lịch sử.

- Pipeline chạy thông qua transform (timezone) → ABC/XYZ (fit đến mốc 2026‑09‑06 vì dữ liệu ngắn hơn holdout) → 660 dòng huấn luyện, 43 đặc trưng → `feature_spec.json` gồm 20 hồ sơ SKU.
- So sánh LightGBM walk-forward (2 fold × 7 ngày):

| Bộ đặc trưng | WAPE | MAE | RMSE |
|---|:---:|:---:|:---:|
| Không có `target_*` | 40.42% | 6.822 | 9.000 |
| Có `target_*` | 40.78% | 6.878 | 9.116 |

  Chênh lệch nằm trong mức nhiễu. Như đã nêu ở BUG-10, tập dữ liệu không có ngày Mega-sale nào trong cửa sổ test, nên thực nghiệm này **chưa** đánh giá được lợi ích của đặc trưng mới. Cần chạy lại trên tập dữ liệu ≥ 120 ngày.

### 4.4. Những gì CHƯA được kiểm chứng trong đợt này

- **Chưa chạy trên stack Docker thật**, vì Docker daemon tắt trong lúc thực hiện. Các câu SQL mới gồm upsert `Fact_Orders`, upsert `Fact_Inventory_Daily`, truy vấn lịch sử/tồn kho trong `data_access.py` và `CREATE DATABASE mlflow` mới chỉ được rà soát cú pháp, **chưa thực thi trên PostgreSQL**. Cách kiểm tra: xem mục 5.1.
- Chưa đo lại hiệu năng bằng Locust: dự báo đệ quy cộng truy vấn kho có thể làm tăng độ trễ `/predict/demand` so với công thức heuristic cũ. Nên chạy lại `tests/load_testing/run_load_test.py` và theo dõi histogram độ trễ mới.

---

## 5. HƯỚNG DẪN TRIỂN KHAI & KẾ HOẠCH TIẾP THEO

### 5.1. Triển khai và kiểm tra trên stack thật

```bash
# 1. Khởi động hạ tầng (volume cũ vẫn dùng được)
docker compose up -d --build

# 2. Áp dụng thay đổi schema cho volume cũ: unique index Fact_Inventory_Daily + database mlflow
python scripts/init_warehouse.py

# 3. Luồng dữ liệu (3 terminal)
python ingestion/producer.py                       # đơn hàng + snapshot tồn kho
python ingestion/consumer_to_minio.py              # đơn hàng → MinIO
python ingestion/consumer_inventory_to_postgres.py # tồn kho → Fact_Inventory_Daily (MỚI)

# 4. ETL → Feature Store → Train → Register
python warehouse/etl/pipeline.py --all-dates
python ml/features/feature_pipeline.py
python ml/training/train_baseline.py --models lightgbm moving_average
python ml/training/register_model.py --experiment-name demand-forecasting-baseline

# 5. Nạp mô hình vào API và kiểm tra
curl -X POST http://localhost:8000/model/reload
curl http://localhost:8000/health        # kỳ vọng: "status": "healthy", "model_source": "mlflow_registry"
```

Câu SQL kiểm tra nhanh:

```sql
SELECT COUNT(*) FILTER (WHERE date_key IS NULL) AS null_date_key, COUNT(*) FROM Fact_Orders;  -- null_date_key = 0
SELECT COUNT(*) FROM Fact_Inventory_Daily;                                                    -- > 0
```

**Lưu ý khi chuyển đổi (migration)**:
- MLflow chuyển sang database `mlflow`, nên các experiment cũ vẫn còn trong `ecom_warehouse` nhưng sẽ không hiện trên UI. Có hai cách: chạy lại bước 4, hoặc đặt `MLFLOW_DB=ecom_warehouse` để dùng tạm database cũ.
- Dữ liệu MinIO đã ghi trước đợt sửa được phân vùng theo ngày UTC. Khi xử lý lại dữ liệu cũ nên dùng `--all-dates`.
- Feature Store và mô hình **phải được tạo lại**, vì số cột thay đổi (thêm `target_*`, mã hóa cố định).

### 5.2. Việc còn tồn đọng (đề xuất cho lần 3)

1. **Pin phiên bản image MinIO** (`minio/minio`, `minio/mc` đang dùng `latest`). Trong đợt này không pin được vì không truy cập được Docker Hub để xác minh tag tồn tại.
2. **Image serving nhẹ hơn**: thử `mlflow-skinny` thay cho `mlflow` đầy đủ.
3. **`powerbi/export_powerbi_dataset.py` và một số script demo** vẫn lặp qua `CATALOG_METADATA` (bảng demo). Nên chuyển sang `ModelManager.get_catalog()`.
4. **Vector hóa `/predict/batch`**: dự báo đệ quy hiện chạy tuần tự theo SKU. Có thể gộp tất cả SKU vào một lần `predict` cho mỗi bước thời gian.
5. **Khoảng tin cậy**: hiện là ±1.96σ của nhu cầu lịch sử. Nên thay bằng quantile regression (LightGBM `objective="quantile"`) hoặc phần dư từ walk-forward.
6. **Bảng điều khiển Grafana**: thêm panel p95/p99 từ `ecommerce_api_request_duration_seconds_bucket`, panel `ecommerce_model_ready` và tỷ lệ `ecommerce_predictions_total{model_source=~"heuristic.*"}`.
7. **Bảo mật**: thêm xác thực cho API, đổi mật khẩu mặc định của MinIO/Grafana/NiFi khi triển khai ngoài môi trường demo.
8. ~~**Liên kết tài liệu**: `README.md` trỏ tới đường dẫn `docs/` cũ~~ — **đã sửa**: 21 link (cả đích lẫn chữ hiển thị) được cập nhật theo cấu trúc `docs/00-…` → `docs/06-…`.
9. Kế hoạch lần 2 cũ (PostgreSQL `COPY` / `execute_values`, tối ưu Uvicorn workers) chuyển sang lần 3.

---

### PHỤ LỤC — Danh sách tệp thay đổi

**Tệp mới**: `ingestion/consumer_inventory_to_postgres.py`, `serving/app/features.py`, `serving/app/data_access.py`, `warehouse/ddl/00_create_mlflow_db.sql`, `tests/conftest.py`, `tests/test_serving_realdata.py`

**Tệp sửa**: `docker-compose.yml`, `.env.example`, `requirements.txt`, `serving/requirements.txt`, `ingestion/producer.py`, `ingestion/consumer_to_minio.py`, `warehouse/ddl/01_star_schema.sql`, `warehouse/etl/{transform,load,pipeline}.py`, `scripts/init_warehouse.py`, `ml/features/{feature_pipeline,time_series_features}.py`, `ml/training/{train_baseline,register_model}.py`, `serving/app/{main,model_loader,inventory_service,schemas}.py`, `monitoring/evidently/{drift_detector,trigger_retraining}.py`, `tests/test_{serving,monitoring,deep_learning,features,etl}.py`
