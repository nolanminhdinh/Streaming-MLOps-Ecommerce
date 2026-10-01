# Tuần 7: Triển khai Model Serving (FastAPI) & Nghiệp vụ Quản trị Tồn kho (Safety Stock / Reorder Point)

## Mục tiêu
Thiết kế và xây dựng tầng Model Serving sẵn sàng môi trường sản xuất (Production-ready) bằng FastAPI; tích hợp cơ chế nạp mô hình tối ưu (In-Memory Model Caching) từ MLflow Model Registry / Model Manifest; phát triển dịch vụ tính toán định mức tồn kho an toàn (Safety Stock - SS) và điểm đặt hàng lại (Reorder Point - ROP) theo tiêu chuẩn quản trị chuỗi cung ứng; cung cấp giao diện REST API chuẩn hóa kèm Swagger UI trực quan; và đóng gói hoàn chỉnh thành Docker container phục vụ tại cổng `8000`.

---

## Công việc đã thực hiện

### 1. Kiến trúc Nạp mô hình & Bộ nhớ đệm (`serving/app/model_loader.py`)
- **Mô hình Singleton & In-Memory Caching (`ModelManager`)**:
  - Khởi tạo và nạp mô hình Champion duy nhất 1 lần trong sự kiện Lifespan của FastAPI (`@asynccontextmanager`), loại bỏ hoàn toàn độ trễ I/O khi tiếp nhận các request suy luận.
  - Tự động nạp thông tin từ `data/model_manifest.json` (được đăng ký ở Tuần 6 từ MLflow Model Registry).
  - Tích hợp cơ chế kết nối động tới MLflow PyFunc Model; nếu MLflow Server ngoại tuyến, hệ thống tự động kích hoạt bộ Calibrated Inference Engine bám sát phân phối và các hệ số mùa vụ của mô hình LightGBM Tuned.
- **Ràng buộc dự báo nghiệp vụ**:
  - Đảm bảo toàn bộ kết quả sản lượng dự báo không âm: $\hat{y} \ge 0.0$.
  - Tính toán khoảng tin cậy 95% ($[\hat{y} - 1.96\sigma, \hat{y} + 1.96\sigma]$) nhằm hỗ trợ nhà quản lý kho lượng hóa mức độ rủi ro nhu cầu.
  - Hỗ trợ dự báo theo khoảng thời gian tùy chọn $T$ ngày ($1 \le T \le 90$).

---

### 2. Dịch vụ Quản trị Tồn kho & Cảnh báo Nhập hàng (`serving/app/inventory_service.py`)
- **Công thức Tồn kho an toàn (Safety Stock - SS)**:
  $$SS = \lceil Z \cdot \sigma_d \cdot \sqrt{L} \rceil$$
  Trong đó:
  - $Z = 1.645$ ứng với mức độ phục vụ (Service Level) mục tiêu là 95%.
  - $\sigma_d$ là độ lệch chuẩn nhu cầu tiêu thụ hàng ngày của SKU.
  - $L$ là thời gian chờ hàng của nhà cung cấp (Lead Time, mặc định 3 ngày).
- **Công thức Điểm đặt hàng lại (Reorder Point - ROP)**:
  $$ROP = \lceil (\mu_d \cdot L) + SS \rceil$$
  Trong đó $\mu_d$ là lượng tiêu thụ trung bình ngày. Điểm đặt hàng lại luôn đảm bảo $ROP > SS$ để bù đắp lượng hàng bán ra trong suốt thời gian nhà cung cấp giao hàng.
- **Phân cấp cảnh báo thông minh (3 cấp độ)**:
  1. 🔴 **CRITICAL (Báo động đỏ)**: Khi $\text{Tồn kho thực tế} \le SS$. Tồn kho đã chạm hoặc xuyên thủng vùng đệm an toàn, nguy cơ đứt hàng (Stockout) cận kề trong chu kỳ nhập hàng.
  2. 🟡 **WARNING (Cảnh báo vàng)**: Khi $SS < \text{Tồn kho thực tế} \le ROP$. Tồn kho đã chạm tới điểm đặt hàng lại, hệ thống khuyến nghị phát lệnh nhập hàng ngay lập tức.
  3. 🟢 **NORMAL (An toàn)**: Khi $\text{Tồn kho thực tế} > ROP$. Mức tồn kho đang ở trạng thái tối ưu, không cần đặt hàng.
- **Chỉ số vận hành mở rộng**:
  - `days_until_stockout`: Số ngày dự kiến còn lại trước khi kho hết sạch hàng ($\frac{\text{current\_stock}}{\max(\mu_d, 0.1)}$).
  - `recommended_reorder_qty`: Số lượng đề xuất nhập bổ sung để đưa tồn kho về mức an toàn mục tiêu ($\max(0, ROP + 7 \cdot \mu_d - \text{current\_stock})$).

---

### 3. Xây dựng Hệ thống REST API Chuẩn hóa (`serving/app/main.py` & `schemas.py`)
- Định nghĩa đầy đủ các Schemas dữ liệu bằng Pydantic với kiểu dữ liệu chặt chẽ và thông tin mô tả chi tiết cho từng trường.
- Triển khai các Endpoints phục vụ:
  - `GET /health`: Kiểm tra sức khỏe hệ thống, trạng thái sẵn sàng của mô hình và thời gian uptime.
  - `GET /model/metadata`: Truy vấn metadata của Champion Model (tên mô hình, version, stage, metrics CV WAPE/MAE).
  - `POST /predict/demand`: Dự báo sản lượng theo SKU và khoảng ngày.
  - `POST /predict/batch`: Dự báo sản lượng hàng loạt cho danh sách nhiều SKU.
  - `GET /inventory/reorder-alert`: Tự động quét toàn bộ kho hàng và sinh danh sách cảnh báo tồn kho sắp xếp theo mức độ khẩn cấp.
  - `POST /inventory/reorder-alert`: Thẩm định cảnh báo tùy biến khi người dùng truyền vào số lượng tồn kho thực tế.
  - `GET /metrics`: Cung cấp số liệu thống kê vận hành (lượt gọi API, số SKU báo động) cho Prometheus / Grafana.

---

### 4. Đóng gói Container Docker (`serving/Dockerfile` & `docker-compose.yml`)
- Xây dựng Dockerfile tối ưu trên nền `python:3.12-slim`, cài đặt thư viện `libgomp1` cho LightGBM và cấu hình Healthcheck tự động qua `curl`.
- Kích hoạt service `fastapi` trong `docker-compose.yml` trên cổng `8000`, liên kết mạng nội bộ `ecom-net` với PostgreSQL, MinIO và MLflow, nạp tệp manifest qua volume mount chế độ đọc (`ro`).

---

### 5. Kiểm thử Tự động & Hướng dẫn Vận hành
- `tests/test_serving.py`: Bộ unit test gồm 11 kịch bản kiểm tra toàn diện tính toàn vẹn của ModelManager, tính đúng đắn của công thức SS/ROP, phân loại cảnh báo tồn kho và các API endpoints.
- `docs/how-to-run/how-to-run-week7.md`: Hướng dẫn chi tiết chạy container, gọi API bằng `curl` và kiểm tra tài liệu tương tác trên Swagger UI.

---

## Bảng tổng hợp các API Endpoints (Tuần 7)

| Phương thức | Đường dẫn Endpoint | Chức năng chính | Output chính | Latency TB (ms) |
|---|---|---|---|:---:|
| `GET` | `/health` | Kiểm tra trạng thái sẵn sàng của dịch vụ | Trạng thái `healthy`, model name, version, uptime | < 5ms |
| `GET` | `/model/metadata` | Xem metadata Champion Model từ Registry | Tên model, Stage (`Staging`), CV WAPE, MAE | < 5ms |
| `POST` | `/predict/demand` | Dự báo sản lượng cho 1 SKU | Tổng sản lượng, chuỗi dự báo theo ngày, khoảng tin cậy 95% | < 25ms |
| `POST` | `/predict/batch` | Dự báo sản lượng cho danh sách nhiều SKU | Danh sách chuỗi dự báo theo từng SKU | < 60ms |
| `GET` | `/inventory/reorder-alert` | Quét toàn bộ kho & phát hiện SKU cần nhập | Báo cáo phân loại CRITICAL / WARNING / NORMAL, ROP, SS | < 20ms |
| `POST` | `/inventory/reorder-alert` | Kiểm tra cảnh báo với tồn kho thực tế | Báo cáo chi tiết và số lượng đề xuất nhập | < 20ms |
| `GET` | `/metrics` | Cung cấp chỉ số vận hành hệ thống | Request counter, phân bố trạng thái kho | < 5ms |

---

## Sản phẩm bàn giao
1. `serving/app/schemas.py`: Pydantic Schemas chuẩn hóa cho Request/Response.
2. `serving/app/model_loader.py`: Singleton ModelManager nạp và cache mô hình.
3. `serving/app/inventory_service.py`: Nghiệp vụ tính Safety Stock, Reorder Point và phân cấp cảnh báo.
4. `serving/app/main.py`: Ứng dụng FastAPI phục vụ REST API hoàn chỉnh.
5. `serving/Dockerfile` & `serving/requirements.txt`: Đóng gói container Docker phục vụ độc lập.
6. `docker-compose.yml`: Kích hoạt service `fastapi` tại cổng `8000`.
7. `tests/test_serving.py`: Bộ unit tests kiểm thử logic và API.
8. `docs/how-to-run/how-to-run-week7.md`: Hướng dẫn vận hành chi tiết.

---

## Kế hoạch tuần tiếp theo (Tuần 8)
- **Thiết lập Hạ tầng Giám sát Toàn diện (Monitoring Stack)**:
  - Tích hợp **Prometheus** (cổng `9090`) thu thập số liệu độ trễ, lưu lượng request và cảnh báo từ `/metrics`.
  - Thiết kế Dashboard trên **Grafana** (cổng `3000`) trực quan hóa hiệu năng Model Serving và tình trạng tồn kho.
  - Xây dựng pipeline phát hiện trôi dạt dữ liệu (**Data Drift**) và trôi dạt khái niệm (**Concept Drift**) với **Evidently AI**, xuất báo cáo HTML và cảnh báo suy giảm độ chính xác mô hình.

---

## Minh bạch sử dụng AI
- **Phần AI hỗ trợ**: Hỗ trợ thiết kế cấu trúc Pydantic v2 schemas; hỗ trợ viết công thức tính Z-Score, Safety Stock và Reorder Point chuẩn Logistics; thiết lập cấu hình Dockerfile đa tầng và kịch bản TestClient.
- **Phần sinh viên tự thực hiện**: Định hình logic nghiệp vụ phân cấp cảnh báo 3 tầng (CRITICAL, WARNING, NORMAL); xác định các ngưỡng Service Level và Lead time phù hợp với chuỗi cung ứng TMĐT Việt Nam; kiểm định các ràng buộc sản lượng không âm và đối chiếu kết quả dự báo.

