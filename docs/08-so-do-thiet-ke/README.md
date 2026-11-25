# Bộ sơ đồ thiết kế pipeline end-to-end — 09/11/2026

Đã rà soát mã triển khai và dựng 10 sơ đồ. Các hình mô tả thiết kế hiện hành; không phải lượt nghiệm thu runtime mới. Bằng chứng runtime đối chiếu vẫn là các mốc 04/11 và 08/11/2026. Power BI do sinh viên tự hoàn thiện.

## Mở và sử dụng

- Mở `00_MO_XEM_10_SO_DO.html` để xem hình, nội dung cần giải thích và vị trí chèn trong báo cáo.
- Mở `MO_BANG_STARUML_10_SO_DO_PIPELINE.mdj` bằng StarUML để chỉnh sửa các phần tử UML/ERD. Đã cài StarUML 8.0.2 tại `C:/Users/MINH/AppData/Local/Programs/StarUML/StarUML.exe`.
- Trong StarUML, mở từng model ở Model Explorer rồi mở diagram bên trong; nhấn Ctrl+9 để vừa cửa sổ. Tệp gồm phần tử và quan hệ thật, không phải các ảnh được dán vào canvas.
- Thư mục `01_Anh_SVG_de_chen_bao_cao`: vector dựng độc lập từ đặc tả thiết kế, có thể chèn vào Word và phóng to.
- Thư mục `02_Anh_PNG_do_phan_giai_cao`: ảnh dựng độc lập ở kích thước gấp đôi để chèn khi Word không nhận SVG.
- Thư mục `03_Anh_xuat_goc_StarUML_co_dau_dung_thu`: ảnh SVG do StarUML thực sự xuất, giữ nguyên dấu dùng thử. Không xóa dấu hay kích hoạt giấy phép trái phép.
- Hai nhóm ảnh có cùng nội dung thiết kế nhưng có thể khác bố cục/chữ do hai bộ dựng. Sửa tệp StarUML không tự cập nhật ảnh dựng độc lập; cần xuất lại từ StarUML hoặc cập nhật đặc tả và dựng lại.

## Phần nào nên có sơ đồ?

Các sơ đồ dưới đây là lựa chọn phù hợp đề tài, không phải cẩm nang bắt buộc phải có đúng 10 hình. Ưu tiên kiến trúc, ETL, checkpoint/backfill, mô hình dữ liệu và giám sát. Use case, deployment và sequence giúp người phản biện đọc thiết kế rõ hơn.

| Mã | Sơ đồ | Loại | Vị trí đề xuất |
|---|---|---|---|
| 01 | Phạm vi chức năng và tác nhân | UML Use Case | Mục 2.1 — Bài toán và phạm vi xây dựng hệ thống |
| 02 | Kiến trúc thành phần và luồng dữ liệu | UML Component | Mục 2.3 — Kiến trúc và sự phân chia trách nhiệm |
| 03 | Triển khai local bằng Docker Compose | UML Deployment | Mục 2.13 — Tổ chức mã nguồn và môi trường triển khai; dẫn lại tại 3.1 |
| 04 | Quy trình ETL và checkpoint đơn hàng | UML Activity | Mục 2.5 — Kho dữ liệu và quy trình ETL |
| 05 | Hai mốc commit và khả năng phát lại | UML Sequence | Mục 2.6 — Toàn vẹn dữ liệu và phục hồi bằng phiên bản và checkpoint |
| 06 | Chạy lại dữ liệu có giới hạn và lưu audit | UML Activity | Mục 2.6 — Bổ sung thiết kế backfill; dẫn bằng chứng tại 3.13 |
| 07 | Star schema đơn hàng — hạt dòng sản phẩm | ERD | Mục 2.5 — Sau phần mô tả độ hạt và các bảng kho dữ liệu |
| 08 | Tồn kho, dự báo và nguồn lịch sử riêng | ERD | Mục 2.5 — Sau star schema đơn hàng; dẫn lại tại 2.7 và 2.10 |
| 09 | Nhánh ML kích hoạt riêng và kiểm soát phiên bản | UML Activity | Mục 2.8–2.9 — Tổ chức huấn luyện, quản lý thí nghiệm và triển khai |
| 10 | Giám sát và giao nhận cảnh báo nội bộ | UML Sequence | Mục 2.11.1 — Giám sát dịch vụ và dữ liệu; dẫn bằng chứng tại 3.13 |

## Nội dung cần giải thích dưới mỗi hình

### 01 · Phạm vi chức năng và tác nhân

Chỉ rõ ai sử dụng đầu ra và ai vận hành; phân biệt chức năng liên tục với tác vụ kích hoạt riêng.

Nguồn đối chiếu: `docs/05-how-to-run/core-pipeline.md`, `scripts/demo_web_server.py`, `scripts/backfill_pipeline.py`, `serving/app/main.py`.

### 02 · Kiến trúc thành phần và luồng dữ liệu

Tách luồng đơn hàng, tồn kho và CSV lịch sử; biểu diễn rõ các tác vụ ML riêng.

Nguồn đối chiếu: `docker-compose.yml`, `scripts/run_pipeline_worker.py`, `ml/features/feature_pipeline.py`, `serving/app/model_loader.py`.

### 03 · Triển khai local bằng Docker Compose

Phân biệt máy chủ local, mạng container, dịch vụ thường trực và các profile chạy theo yêu cầu.

Nguồn đối chiếu: `docker-compose.yml`, `ingestion/Dockerfile`, `serving/Dockerfile`, `ml/mlflow/Dockerfile`.

### 04 · Quy trình ETL và checkpoint đơn hàng

Làm rõ phân loại dòng lỗi, chống trùng, đối soát số dòng và điều kiện được checkpoint.

Nguồn đối chiếu: `warehouse/etl/pipeline.py`, `warehouse/etl/contracts.py`, `warehouse/etl/load.py`, `scripts/run_pipeline_worker.py`.

### 05 · Hai mốc commit và khả năng phát lại

Phân biệt commit offset Kafka với checkpoint ETL; thể hiện thứ tự ghi raw, quarantine và warehouse.

Nguồn đối chiếu: `ingestion/consumer_to_minio.py`, `scripts/run_pipeline_worker.py`, `warehouse/etl/pipeline.py`.

### 06 · Chạy lại dữ liệu có giới hạn và lưu audit

Thể hiện lựa chọn object, giới hạn 31 ngày, khóa chung, dry-run mặc định và audit độc lập.

Nguồn đối chiếu: `scripts/backfill_pipeline.py`, `warehouse/etl/coordination.py`, `warehouse/etl/pipeline.py`.

### 07 · Star schema đơn hàng — hạt dòng sản phẩm

Thể hiện đúng 6 dimensions và khóa chống trùng ở grain dòng đơn; không nhầm fact với order header.

Nguồn đối chiếu: `warehouse/ddl/01_star_schema.sql`, `warehouse/ddl/04_integrity_constraints.sql`, `warehouse/etl/load.py`.

### 08 · Tồn kho, dự báo và nguồn lịch sử riêng

Phân biệt grain snapshot, forecast issue, historical source line và xác nhận độ phủ ngày.

Nguồn đối chiếu: `warehouse/ddl/01_star_schema.sql`, `warehouse/ddl/04_integrity_constraints.sql`, `warehouse/reporting/bi_schema.sql`.

### 09 · Nhánh ML kích hoạt riêng và kiểm soát phiên bản

Giải thích nguồn lịch sử, chống rò rỉ, baseline/WAPE đúng và việc đăng ký/triển khai là tác vụ riêng.

Nguồn đối chiếu: `ml/features/feature_pipeline.py`, `ml/training/train_baseline.py`, `ml/training/metrics.py`, `ml/training/register_model.py`, `serving/app/model_loader.py`.

### 10 · Giám sát và giao nhận cảnh báo nội bộ

Phân biệt trạng thái pipeline, scrape, dashboard và giao nhận webhook; không tự thêm email/Slack.

Nguồn đối chiếu: `serving/app/pipeline_status.py`, `monitoring/prometheus/alert_rules.yml`, `monitoring/alertmanager/alertmanager.yml`, `scripts/local_alert_receiver.py`.

## Những mô tả cũ cần thay thế hoặc ghi lại phạm vi

- Không vẽ NiFi vào đường chạy chính đã nghiệm thu; phần này đang hoãn.
- Không vẽ Shopee/TikTok API thật khi đầu vào live hiện là simulator.
- Không vẽ drift → retrain → đăng ký → deploy thành vòng tự động hoàn toàn.
- Không ghi một mô hình như LightGBM là champion hoặc chèn WAPE cũ không có bằng chứng phù hợp.
- Kho đơn hàng có 6 dimensions và hạt dòng sản phẩm; khóa duy nhất là platform + order_id + source_line_id.
- Tồn kho hiện là kho trung tâm, snapshot mới nhất theo SKU/ngày; không cộng snapshot nhiều ngày thành stock hiện tại.
- Không gộp CSV ẩn danh vào khóa đơn live; giữ origin/fingerprint và hạt dòng nguồn.
- Phân biệt offset Kafka sau lưu raw với checkpoint ETL sau thành công. Không nhận exactly-once toàn hệ thống.
- Backfill mặc định dry-run, có khóa chung, giới hạn khoảng ngày/object và audit; không reset checkpoint.
- Alertmanager gửi webhook POST /alerts; GET /events chỉ để đọc lại bằng chứng. Không có email/Slack hoặc tự retrain được thêm trong sơ đồ.

## Chèn vào báo cáo và khai báo AI

Chèn bằng Insert → Pictures → This Device, chọn SVG hoặc PNG. Giữ tỷ lệ; sơ đồ rộng như kiến trúc/sequence có thể cần trang ngang. Dùng Caption của Word để đánh số tiếp theo số hình hiện có và cập nhật danh mục hình; không tự nhận các hình đã được chèn vào bản Word.

Nguồn hình có thể ghi: “Thiết kế từ mã nguồn đồ án, có AI hỗ trợ dựng và kiểm tra sơ đồ, ngày 09/11/2026.” Sinh viên cần rà soát và tự giải thích trước khi nộp; không khai báo đã tự vẽ thủ công nếu không đúng quá trình.

## Kiểm tra đã lưu

Xem `Kiem_tra_doc_lai_bang_StarUML.json` và `Kiem_tra_va_danh_muc_tep_so_do.json`. Các kiểm tra này xác nhận tệp và hình, không được cộng thành số bài kiểm thử chức năng pipeline mới.

## Nguồn công cụ

- Tải chính thức và điều kiện dùng thử: https://staruml.io/download/ và https://staruml.io/help/
- API tạo phần tử: https://docs.staruml.io/developing-extensions/creating-deleting-and-modifying-elements/
- Scripting API và serialization MDJ: https://docs.staruml.io/developing-extensions/scripting-api/
- CLI xuất ảnh: https://docs.staruml.io/user-guide/cli-command-line-interface/


Thư mục bàn giao trong dự án: `Tai_lieu_chat_DATN_2026-11-08/09_So_do_thiet_ke_2026-11-09/`.
