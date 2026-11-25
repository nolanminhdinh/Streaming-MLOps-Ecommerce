# Luồng chính và vận hành final — 01/11/2026

**Bổ sung nghiệm thu 08/11/2026:** [schema, backfill, telemetry, cảnh báo nội bộ và dữ liệu kiểm thử](../07-bao-cao-do-an/hoan-thien-pipeline-2026-11-08.md). API có `/pipeline/status`; Grafana có 6 panel pipeline; Prometheus chuyển cảnh báo qua Alertmanager tới receiver local ở `http://localhost:9094/events`. Các bước kế hoạch cuối tài liệu giữ mốc 01/11, xem báo cáo 08/11 để biết phần đã nghiệm thu. Power BI do sinh viên tự hoàn thiện.

Backfill mặc định chỉ kiểm định, không reset checkpoint:

```powershell
python scripts/backfill_pipeline.py --from-date 2026-11-08 --to-date 2026-11-08 --objects-file output/pipeline-completion-2026-11-08/live-run1/source-objects.json
```

Chỉ thêm `--apply` khi cần ghi guarded upsert. Giới hạn 31 ngày; khóa PostgreSQL dùng chung với worker ngăn chạy chồng. Khi chỉ định object, không tự mở rộng sang object khác. Replay có audit riêng và không được đọc như độ trễ live traffic. Xem `output/pipeline-completion-2026-11-08/backfill-comparison.csv` cho ba lượt đã kiểm thử.

## Phạm vi

NiFi được hoãn để mở rộng trực quan hóa sau khi luồng hoàn thiện. Compose mặc định,
trang demo và bài kiểm tra liên tục không cần NiFi. Mã cũ được giữ riêng; tài liệu
và evidence NiFi trước đây là lịch sử, không chứng minh luồng hiện tại.

```text
Simulator → Producer → Kafka ecom.orders.raw
  → consumer_to_minio → MinIO raw/orders (Parquet)
  → ETL: chuẩn hóa / kiểm định / chống trùng → PostgreSQL
                                              ├→ Power BI (PostgreSQL Import)
                                              ├→ CSV dự phòng (tác vụ riêng)
                                              └→ Features → Training → MLflow → FastAPI
Simulator → Producer → Kafka inventory.logs
  → consumer_inventory_to_postgres → PostgreSQL → API tồn kho

Bản ghi lỗi → MinIO quarantine/orders hoặc quarantine/inventory
FastAPI /metrics → Prometheus → Grafana
```

`pipeline-worker` quản lý hai consumer và định kỳ chạy ETL; checkpoint sau ETL,
không phụ thuộc CSV. [Power BI Import](../../powerbi/import/README.md) đọc trực tiếp
PostgreSQL. Training,
đăng ký model, drift detection và retraining là tác vụ riêng, chưa phải scheduler
tự chạy trong Compose. Power BI cần refresh để nạp dữ liệu mới từ PostgreSQL. Không mô tả toàn bộ
đồ án như một chuỗi streaming tự động hoàn toàn.

## Khởi động

1. Mở Docker Desktop và chờ Engine sẵn sàng. Chỉ tạo `.env` từ `.env.example` nếu
   chưa có; giữ cấu hình đang sử dụng.
2. Chạy `docker compose config --quiet`, sau đó `docker compose up -d --build`.
   `start_full_stack.bat` thực hiện hai bước này. Các init service thoát mã 0 là bình thường.
3. Xem `docker compose ps -a` và `docker compose logs --tail 100 pipeline-worker`.
4. Chạy producer bằng môi trường Python có dependencies:
   `python ingestion/producer.py`; hoặc chạy `start_demo_web.bat` và phát một đợt
   đơn có nhãn tại `http://127.0.0.1:8050` để kiểm tra có giới hạn.
5. Chạy `python scripts/check_continuous_pipeline.py` khi các service đã sẵn sàng.
   Script gửi dữ liệu kiểm thử có nhãn, giữ lại trong kho và lưu evidence tại
   `docs/07-bao-cao-do-an/evidence/`. Không tự train model.
6. API `http://localhost:8000/ready` phải trả 200 khi model và lịch sử sẵn sàng.
   Nếu thiếu model/lịch sử, xem hướng dẫn training ở `how-to-run-week5.md` và
   `how-to-run-week6.md`; `/health` 200 không thay thế kiểm tra này.

Không mở thêm hai consumer hoặc ETL thủ công khi worker đang chạy. Kafka group
có thể chia partition giữa các tiến trình, gây khó đối soát và cạnh tranh ghi CSV.

## Lưu ý khi nâng cấp stack cũ

Compose mới khai báo volume Kafka và ZooKeeper để giữ message, consumer offsets
và metadata qua việc tạo lại container; retention Kafka mặc định 240 giờ.
Các volume mới **không tự nhập dữ liệu từ container/volume ẩn danh cũ**. Trước khi
áp dụng trên stack có backlog: dừng producer, đợi xử lý hết backlog, lưu evidence
và sao lưu dữ liệu Kafka/ZooKeeper cũ nếu cần replay. Nếu cần giữ backlog chưa xử lý,
phải di chuyển dữ liệu cũ sang volume mới trước khi tạo lại broker và ZooKeeper.
Không dùng `docker compose down -v` để xử lý sự cố.

Container NiFi đang tồn tại từ cấu hình cũ có thể vẫn chạy. Chỉ dừng nó bằng
`docker stop nifi` nếu có; Compose mặc định không tự dừng container ngoài cấu hình.

## Xử lý các tình huống lỗi

| Tình huống | Hành vi và cách xử lý |
|---|---|
| Kafka tạm ngừng | Producer đợi ACK; lỗi gửi được đẩy lên và dừng tiến trình, không đếm thành công giả. Consumer lỗi làm worker thoát và Docker khởi động lại. Kiểm tra broker rồi chạy lại producer. Simulator chưa có outbox bền vững để gửi lại sự kiện thất bại. |
| MinIO lỗi khi ghi raw/quarantine | Không commit offset cho batch chưa lưu thành công; worker khởi động lại để replay. Khôi phục MinIO và kiểm tra dung lượng/quyền truy cập. |
| PostgreSQL lỗi | Không commit inventory offset hoặc checkpoint ETL chưa hoàn tất. Worker/cycle thử lại; khôi phục PostgreSQL, xem log, đối soát kho. |
| JSON/schema/giá/số lượng không hợp lệ | Giữ raw và/hoặc quarantine kèm lý do; bản ghi hợp lệ vẫn được xử lý. Không chỉnh dữ liệu lỗi thành giá trị mặc định để qua kiểm định. |
| Gửi trùng hoặc sự kiện đến muộn | Upsert theo khóa dòng đơn và phiên bản nguồn; snapshot tồn kho cũ không ghi đè snapshot mới. Đây là at-least-once, không phải exactly-once trên toàn hệ thống. |
| Xuất CSV dự phòng lỗi/file bị khóa | Tác vụ `csv-backup` báo lỗi riêng, không đổi checkpoint hay health của worker. Sửa quyền/file bị khóa rồi chạy lại backup. File ghi lỗi giữ bản cũ; cả bộ CSV chưa có transaction chung. |
| Consumer chết khi ETL đang chạy | Worker kiểm tra trước/sau cycle, không công bố healthy khi consumer đã thoát; ghi stopped khi dừng và Docker restart worker. |
| Worker restart nhưng startup thất bại | Ghi starting ngay đầu tiến trình để vô hiệu heartbeat healthy cũ; xem lỗi migration/kết nối trong log. |
| Parquet hỏng hoặc object mất | ETL báo lỗi và không bỏ qua/checkpoint dữ liệu đó. Sửa/khôi phục object; hiện chưa có cơ chế tự tách object hỏng nên batch có thể bị chặn. |
| API sống nhưng model chưa sẵn sàng | `/health` có thể 200, `/ready` phải 503. Kiểm tra MLflow, manifest và lịch sử; không coi fallback là model đạt chất lượng. |

Healthcheck Docker báo unhealthy không tự restart tiến trình còn sống. Cycle retry
sẽ tiếp tục thử khi MinIO/PostgreSQL trở lại; cần theo dõi heartbeat
`data/pipeline/status.json`, log và cảnh báo pipeline trên Grafana.

## Nghiệm thu trong 10 ngày còn lại

| Ngày | Kết quả cần chốt |
|---|---|
| 1–2 | Stack chạy không NiFi; một đợt có nhãn đi từ Kafka đến warehouse và API; Power BI Import refresh từ PostgreSQL. |
| 3–4 | Thử dừng/bật Kafka, MinIO, PostgreSQL từng service; kiểm tra replay, quarantine, không trùng fact. |
| 5 | Restart worker giữa batch; thử CSV bị khóa, dữ liệu lỗi và sự kiện sai thứ tự. |
| 6 | Chốt dữ liệu training, model, readiness và số đo chất lượng thực tế. |
| 7 | Chạy liên tục vài giờ, ghi backlog, độ trễ, tài nguyên và lỗi. |
| 8–9 | Viết báo cáo dựa trên evidence, cập nhật sơ đồ và giới hạn đã quan sát. |
| 10 | Chạy lại kịch bản bảo vệ, lưu phiên bản cuối và dự phòng. |

Đây là kế hoạch nghiệm thu, chưa phải kết quả runtime. AI hỗ trợ rà soát mã,
sửa cấu hình và viết tài liệu; sinh viên cần chạy/xác nhận thực nghiệm trước khi
đưa các khẳng định phục hồi và hiệu năng vào báo cáo final.
