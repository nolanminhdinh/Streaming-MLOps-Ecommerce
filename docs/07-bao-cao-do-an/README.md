# Bộ báo cáo kiểm tra đồ án

> **Sửa KPI 20/10:** [Số đơn/AOV được đối soát với warehouse; 114 test đạt](../04-pipeline-updates/lan-08-powerbi-orders-aov-2026-10-20.md). View và CSV đã cập nhật; refresh/render PBIX vẫn cần nghiệm thu riêng.

> **Sau khắc phục 20/10:** [API phục hồi nạp model, readiness 200 và 110 test đạt](../04-pipeline-updates/lan-07-model-startup-recovery-2026-10-20.md). Các bằng chứng audit 16–19/10 được giữ nguyên để đối chiếu trước/sau.

Thực hiện từ 16–19/10/2026. Đây là bản thảo kỹ thuật dựa trên mã nguồn, kiểm thử và bằng chứng runtime có ngày cụ thể; chưa thay cho nghiệm thu production hoặc bản định dạng chính thức của trường.

## Điểm bắt đầu

**Lần 9, 20/10/2026:** Luồng chạy liên tục có checkpoint, nhánh lỗi được lưu, chặn ghi lùi đơn/tồn kho; 145 test đạt. Xem [báo cáo ổn định end-to-end](../04-pipeline-updates/lan-09-on-dinh-end-to-end-2026-10-20.md) và các điều kiện chưa đóng.

Đọc [báo cáo tổng hợp](bao-cao-tong-hop-do-an.md) để nắm kết quả chính, trạng thái hiện tại và các việc cần hoàn thiện. Các chương chi tiết:

1. [Bài toán và dữ liệu](01-bai-toan-va-du-lieu.md).
2. [Ingestion, Kafka, NiFi và MinIO](02-ingestion-kafka-nifi-minio.md).
3. [ETL và data warehouse](03-etl-va-data-warehouse.md).
4. [Feature engineering và Machine Learning](04-feature-engineering-va-machine-learning.md).
5. [Serving, tồn kho và monitoring](05-serving-inventory-va-monitoring.md).
6. [Power BI và thực nghiệm](06-power-bi-va-tong-hop-thuc-nghiem.md).

## Quy tắc sử dụng bằng chứng

- Mỗi con số phải giữ ngày đo, nguồn dữ liệu và phạm vi. Kết quả trước đó không tự trở thành trạng thái hiện hành.
- Kết quả unit test, thử trong bộ nhớ, artifact cục bộ và dịch vụ thật là các mức bằng chứng khác nhau.
- Không cộng số test của từng chương vì có tập trùng. Lượt toàn suite 19/10 đạt 106 test, có JUnit XML riêng.
- Passing tests chưa phủ hết các lỗi biên đã tái hiện; model ready chưa chứng minh độ chính xác; scrape up chưa chứng minh model ready.
- Các vấn đề đã được ghi nhận trong quá trình kiểm tra chưa mặc định đã sửa. Không dùng bản thảo làm tuyên bố toàn hệ thống đã nghiệm thu hoàn chỉnh.

## Thư mục bằng chứng

| Hiện vật | Mục đích |
|---|---|
| [Ingestion offline](evidence/ingestion_offline_2026-10-16.json) | Commit/upload, replay và null platform |
| [ETL/warehouse](evidence/etl_warehouse_2026-10-19.json) | Đối soát raw/valid/duplicate/rejected và facts |
| [ML](evidence/ml_pipeline_2026-10-19.json) | Artifact, nguồn, folds, horizons và Registry |
| [So sánh spec](evidence/ml_spec_comparison_2026-10-19.json) | Profile local khác artifact Registry |
| [Serving/monitoring](evidence/serving_monitoring_2026-10-19.json) | Response thật, nguồn fallback, stocks và Prometheus |
| [Chẩn đoán startup](evidence/serving_startup_diagnosis_2026-10-19.json) | Timeout MLflow lúc API khởi tạo |
| [Power BI](evidence/powerbi_2026-10-19.json) | KPI, CSV và cấu trúc PBIX |
| Bản xuất kiểm chứng cục bộ: `evidence/powerbi_export_2026-10-19/` | CSV riêng bị Git ignore; tái tạo bằng script Power BI audit, không thay powerbi/data |
| [Toàn bộ pytest](evidence/pytest_full_2026-10-19.xml) | 106 passed, 30 warnings, seed 20261019 |

Các script tái lập nằm trong `scripts/check_ingestion_offline.py`, `scripts/check_etl_warehouse_readonly.py`, `scripts/check_ml_pipeline_readonly.py`, `scripts/check_serving_monitoring.py` và `scripts/check_powerbi_readonly.py`. Đọc docstring và tham số trước khi chạy. Script serving gọi API và có thể thực hiện audit forecast theo thiết kế API; lượt 19/10 đã xác nhận số dòng forecast không tăng.

Một số CSV và dữ liệu dưới `data/` bị Git ignore theo cấu hình repository. Bản sao trong workspace vẫn cần được đóng gói chủ động nếu bàn giao ngoài máy hiện tại; không coi chúng đã có trên Git chỉ vì được liên kết trong tài liệu.
