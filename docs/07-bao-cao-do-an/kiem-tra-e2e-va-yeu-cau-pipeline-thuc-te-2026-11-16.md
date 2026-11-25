# Kiểm tra end-to-end và đối chiếu yêu cầu data pipeline thực tế — 16/11/2026

## 1. Phạm vi

Kiểm tra lại toàn bộ luồng Simulator → Kafka → MinIO → ETL → PostgreSQL → view `bi` → FastAPI → Prometheus/Grafana/Alertmanager trên stack Docker local, rồi đối chiếu với các yêu cầu thường đặt ra cho một data pipeline vận hành thực tế. Tất cả số liệu dưới đây được đo trong ngày 16/11/2026; phần ước tính được ghi rõ.

## 2. Kết quả kiểm tra

| # | Hạng mục | Cách kiểm tra | Kết quả |
|---|---|---|---|
| 1 | Trạng thái stack | `docker compose ps`, so image đang chạy với image mới nhất | **Lỗi:** Prometheus và Grafana tắt (exit 255) sau khi Docker khởi động lại lúc 19:24, do không có `restart` policy. **Đã sửa cùng ngày** (mục 6.1) |
| 2 | Bộ test | `python -m pytest -q` | 240 đạt, 20 bỏ qua (opt-in) |
| 3 | Test PostgreSQL opt-in (rollback-only) | `RUN_PIPELINE_COMPLETION_PG=1 RUN_POWERBI_SQL_TESTS=1 RUN_WAREHOUSE_SCHEMA_TESTS=1` | 50/50 đạt |
| 4 | E2E có nhãn | `scripts/check_continuous_pipeline.py` (`AUTO20261116813191`) | PASS: 2 fact đúng, tồn kho 50, view khớp kho, cách ly 3 đơn + 1 tồn kho, request sai trả 422, API dùng `warehouse_snapshot` |
| 5 | Chạy lại cùng dữ liệu | `scripts/check_replay_determinism.py` (`RPT11161927`) | 20/20 đạt; xem [Lần 12](../04-pipeline-updates/lan-12-chay-lai-du-lieu-va-gioi-han-luu-tru-2026-11-16.md) |
| 6 | Thử lỗi hạ tầng | `scripts/check_fault_recovery.py`, mục 3 | 3/3 kịch bản đạt |
| 7 | Đối soát raw ↔ warehouse | `scripts/check_etl_warehouse_readonly.py --runtime`, ngày nguồn 15/11 và 16/11 | Cân bằng; mọi dòng bị loại có trong quarantine; 0 thiếu; 0 lệch giá trị |
| 8 | Rule cảnh báo | `promtool check rules` và 6 tệp test đã lưu ngày 08/11 | 9 rule hợp lệ; 6/6 SUCCESS |
| 9 | Nhánh ML | `scripts/check_ml_pipeline_readonly.py --runtime`, gọi `/predict/demand` | Feature, nhãn t+1, fingerprint nguồn, Registry khớp manifest; dự báo SKU lịch sử dùng `mlflow_registry` + `warehouse`. `run_feature_spec_matches_local=false` đã biết từ 19/10 (chỉ khác `generated_at`, `sku_profiles`) |

Đối soát raw ↔ warehouse:

| Ngày nguồn | Object | Dòng raw | Hợp lệ | Trùng | Bị loại | Thiếu trong kho | Lệch giá trị |
|---|---|---|---|---|---|---|---|
| 15/11 | 27 | 208 | 74 | 109 | 25 | 0 | 0 |
| 16/11 | 10 | 23 | 17 | 1 | 5 | 0 | 0 |

Ngày 15/11 có nhiều dòng trùng vì là ngày của các lượt chạy lại có chủ đích.

## 3. Thử lỗi hạ tầng

Mỗi kịch bản gửi 5 đơn (tổng số lượng 15) và 1 snapshot tồn kho (77) có nhãn riêng; dịch vụ ngừng 60 giây. Kết quả lưu ở `output/fault-recovery-2026-11-16/results.json`.

| Kịch bản | Trong lúc lỗi | Sau khi khôi phục | Thời gian phục hồi |
|---|---|---|---|
| PostgreSQL ngừng | Worker thoát và được Docker khởi động lại 6 lần. Đơn vẫn vào MinIO (lag nhóm MinIO = 0). Tồn kho không commit (lag = 1). `/ready` trả 503. `EcommercePipelineUnhealthy` ở trạng thái pending | 5 fact, 15 sản phẩm, 0 trùng, tồn kho 77, lag 0, `/ready` 200 | 21,8 giây |
| MinIO ngừng | Đơn không commit (lag nhóm MinIO = 5). Heartbeat `stopped`. API vẫn ready, vì serving không phụ thuộc MinIO. Docker vẫn báo container healthy (healthcheck cần 3 × 30 giây mới đổi) | như trên | 36,9 giây |
| Worker bị kill -9 | Docker khởi động lại container (restart count 8 → 9) | như trên, không trùng | 33,4 giây |

Cùng với bài restart worker ngày 20/10 và sự cố Windows Modern Standby ngày 15/11 (Docker đóng băng 15 phút, tự phục hồi), các bài thử cho thấy: dữ liệu không mất và không trùng khi một dịch vụ lưu trữ hoặc tiến trình xử lý ngừng tạm thời.

Chưa thử: Kafka/ZooKeeper ngừng, đầy đĩa, mất volume hoặc mất host.

## 4. Đối chiếu yêu cầu data pipeline thực tế

| Yêu cầu | Đánh giá | Căn cứ / thiếu sót |
|---|---|---|
| Thu nhận tách lớp, có bộ đệm | **Đạt** | Kafka đệm khi PostgreSQL ngừng; đơn vẫn vào raw lake |
| Hợp đồng dữ liệu, thay đổi schema | **Đạt** | `orders-v1`; thay đổi phá vỡ → quarantine (nghiệm thu 08/11) |
| Kiểm soát chất lượng, cách ly | **Đạt** | Mã lỗi theo từng dòng; đối soát mục 2 |
| Chống trùng, idempotent | **Đạt** | Chạy lại 20/20; 0 trùng sau mọi kịch bản lỗi |
| Thứ tự, phiên bản, dữ liệu đến muộn | **Đạt, phạm vi có giới hạn** | Bản mới thắng trong lô; chặn bản cũ khác lô ở PostgreSQL mới có unit test |
| Xử lý gia tăng, checkpoint | **Đạt ở quy mô demo** | Checkpoint lưu toàn bộ danh sách object (giới hạn giữ lại, Lần 12) |
| Chịu lỗi, phục hồi | **Đạt với lỗi tạm thời** | Mục 3; chưa thử Kafka ngừng, đầy đĩa, mất host |
| Chạy lại, backfill | **Đạt** | Backfill có dry-run, khóa, giới hạn 31 ngày (08/11) |
| Truy vết, đối soát | **Đạt** | run_id, object nguồn, object quarantine, hợp đồng; raw = hợp lệ + trùng + loại |
| Giám sát, cảnh báo | **Đạt một phần** | 9 rule, Alertmanager, receiver nội bộ; Prometheus/Grafana đã có restart policy và healthcheck (6.1). Thiếu: metric lag Kafka; receiver ngoài (email/chat). `EcommercePipelineBatchVolumeAnomaly` chuyển pending sau các lô thử nhỏ |
| Độ trễ / SLA | **Có số đo, chưa có SLA** | p95 ingest → warehouse khoảng 25–35 giây (chu kỳ poll 30 giây) |
| Mở rộng | **Chưa đạt cho tải thật** | 1 broker, replication factor 1, 3 partition, 1 worker; MinIO và checkpoint tăng tuyến tính |
| Sẵn sàng cao, sao lưu, khôi phục thảm họa | **Chưa có** | Không bản sao Kafka/PostgreSQL/MinIO; không quy trình sao lưu `pg_dump` hoặc mirror MinIO |
| Bảo mật | **Chưa đạt cho production** | Mật khẩu mặc định; role PostgreSQL dùng chung là superuser; `/model/reload` không cần token; cổng dịch vụ mở trên mọi địa chỉ; Kafka/PostgreSQL không mã hóa đường truyền (mục 6.10) |
| Dữ liệu cá nhân, lưu giữ | **Chưa có** | Raw/quarantine TikTok giữ `buyer_name`, `recipient_name`, `recipient_phone` không che (dữ liệu giả lập; địa chỉ đã che); bucket chưa bật mã hóa và versioning. Warehouse không lưu các cột này. Không có chính sách lưu giữ (mục 6.11) |
| Điều phối | **Đạt một phần, theo thiết kế** | Worker tự chạy consumer + ETL; training, drift, retrain kích hoạt thủ công |
| Kiểm thử, CI | **Đạt một phần** | 240 test + 50 test PostgreSQL opt-in + script E2E; không có CI tự chạy |
| Tài liệu vận hành | **Đạt** | `core-pipeline.md`, nhật ký từng lần, kịch bản bảo vệ |

**Kết luận.** Về **tính đúng của dữ liệu**, luồng đạt các yêu cầu cốt lõi của một data pipeline: thu nhận đủ, kiểm soát chất lượng, chống trùng, kết quả xác định khi chạy lại, phục hồi sau lỗi tạm thời, truy vết và đối soát. Về **vận hành production**, luồng chưa đạt ở mở rộng, sẵn sàng cao/sao lưu, bảo mật và quản lý dữ liệu cá nhân. Đây là các hạng mục hạ tầng, không phải lỗi logic xử lý, và phù hợp phạm vi demo đồ án nếu được nêu rõ là giới hạn.

## 5. Lỗi và rủi ro cần xử lý

| Mức | Vấn đề | Đề xuất |
|---|---|---|
| Cao | Prometheus và Grafana không có `restart` policy; sau khi Docker khởi động lại, cảnh báo ngừng mà không ai biết | **Đã sửa 16/11** — thêm `restart: unless-stopped` và healthcheck (6.1) |
| Trung bình | `check_pipeline_alert_rules.py` ghi đè thư mục bằng chứng `output/pipeline-completion-2026-11-08/promtool` | **Đã sửa 16/11** — `--out-dir` theo ngày, chặn ghi vào hồ sơ đã chốt (6.2) |
| Trung bình | Không có metric lag consumer Kafka; lỗi MinIO chỉ lộ qua heartbeat `stopped` | Xuất lag theo consumer group lên Prometheus, thêm rule |
| Thấp | `EcommercePipelineBatchVolumeAnomaly` pending sau các lô thử nhỏ | Chấp nhận trong demo; cân nhắc ngưỡng theo khung giờ khi có tải thật |
| Thấp | Healthcheck Docker của worker chậm khoảng 90 giây so với heartbeat | Chấp nhận; giám sát dùng `/pipeline/status` |
| Hướng phát triển | Mở rộng, HA/sao lưu, bảo mật, dữ liệu cá nhân, CI, giới hạn MinIO/checkpoint (Lần 12) | Ghi là giới hạn của bản demo |

Giải thích chi tiết từng mục và cách khắc phục khi đưa lên production ở mục 6.

## 6. Giải thích chi tiết và cách khắc phục khi đưa lên production

Mỗi mục gồm bốn phần:
- **Hiện trạng:** có dẫn tệp, dòng hoặc số đo.
- **Rủi ro khi vận hành thật.**
- **Cách khắc phục.**
- **Cách kiểm chứng sau khắc phục.**

Thứ tự đi từ lỗi đang có, qua các yêu cầu đạt một phần, tới các yêu cầu chưa đạt. Lộ trình gợi ý ở mục 6.14.

### 6.1. Prometheus và Grafana không tự khởi động lại — lỗi, ưu tiên cao — **đã sửa 16/11/2026**

> **Đã sửa và kiểm chứng (16/11/2026).**
> - `docker-compose.yml` thêm cho `prometheus` và `grafana`:
>   - `restart: unless-stopped`;
>   - healthcheck `wget` tới `/-/healthy` và `/api/health` (30 giây, 3 lần thử).
> - `docker compose config --quiet` hợp lệ.
> - Sau khi tạo lại, cả hai container có `RestartPolicy = unless-stopped` và `health = healthy`.
> - Mô phỏng tiến trình chính thoát (`docker exec … kill 1`): cả hai tự chạy lại (restart count 0 → 1), trở lại `healthy`, Prometheus nạp lại đủ 9 rule, Grafana trả 200, dashboard giữ nguyên trong volume `grafana-data`.
> - Phần dead man's switch dưới đây vẫn là đề xuất cho production, chưa triển khai.
> - Ghi chú: tag `grafana/grafana:11.0.0` trên máy thực chất trỏ tới image `grafana-oss:10.1.2` (cùng image ID; Grafana báo phiên bản 10.1.2). Nên ghim lại đúng phiên bản trong mục 6.7.

**Hiện trạng.** Trong `docker-compose.yml`, service `prometheus` (dòng 367) và `grafana` (dòng 414) không khai báo `restart`, trong khi 10 service còn lại dùng `restart: unless-stopped`. Lúc 12:24:35 UTC ngày 16/11, Docker Desktop khởi động lại:
- các service khác tự chạy lại;
- hai service giám sát dừng ở exit 255 khoảng 48 phút, đến khi được bật tay.

Trong khoảng đó, 9 rule cảnh báo không được đánh giá và Alertmanager không nhận được gì.

**Rủi ro.** Hệ thống giám sát hỏng mà không ai biết, nên mọi sự cố pipeline xảy ra cùng lúc đều bị bỏ qua. Đây là lỗi nguy hiểm vì người vận hành tưởng "không có cảnh báo" nghĩa là "không có sự cố".

**Khắc phục.**

```yaml
  prometheus:
    restart: unless-stopped
    healthcheck:
      test: ["CMD", "wget", "-qO-", "http://localhost:9090/-/healthy"]
      interval: 30s
      retries: 3
  grafana:
    restart: unless-stopped
    healthcheck:
      test: ["CMD", "wget", "-qO-", "http://localhost:3000/api/health"]
      interval: 30s
      retries: 3
```

Ở production, thêm cơ chế "dead man's switch":
- Một rule luôn ở trạng thái firing (`alert: Watchdog`, `expr: vector(1)`) gửi đều đặn tới một dịch vụ heartbeat bên ngoài hệ thống, chẳng hạn healthchecks.io hoặc tính năng heartbeat của Opsgenie/PagerDuty.
- Khi tín hiệu ngừng, dịch vụ ngoài báo rằng chính hệ thống giám sát đã chết.
- Prometheus nên chạy trên máy khác với máy nó giám sát, hoặc dùng dịch vụ có quản lý.

**Kiểm chứng.**
- `docker kill prometheus grafana` hoặc khởi động lại Docker: hai container phải tự chạy lại.
- Dừng Prometheus quá ngưỡng heartbeat: phải nhận cảnh báo từ dịch vụ ngoài.

### 6.2. Script kiểm tra rule cảnh báo ghi đè bằng chứng — lỗi, ưu tiên trung bình — **đã sửa 16/11/2026**

> **Đã sửa và kiểm chứng (16/11/2026).** `scripts/check_pipeline_alert_rules.py`:
> - nhận `--out-dir`, mặc định `output/alert-rules-<ngày chạy>/`;
> - từ chối ghi vào thư mục đã có kết quả nếu không có `--force`;
> - **luôn** từ chối ghi vào `output/pipeline-completion-2026-11-08`, kể cả khi có `--force`.
>
> Kết quả kiểm chứng:
>
> | Trường hợp | Kết quả |
> |---|---|
> | Chạy mặc định | 6/6 PASS, ghi vào `output/alert-rules-2026-11-16/` |
> | Chạy lại cùng thư mục | Bị từ chối (exit 1) |
> | Nhắm vào hồ sơ 08/11 kèm `--force` | Bị từ chối (exit 1) |
> | `--force` vào thư mục thường | 6/6 PASS |
>
> Mã băm SHA-256 của toàn bộ 952 tệp trong `output/pipeline-completion-2026-11-08` không đổi trước và sau các lượt chạy. `build_pipeline_completion_report.py` chỉ đọc kết quả promtool đã lưu của gói 08/11, không bị ảnh hưởng. Bộ test: 240 đạt.

**Hiện trạng.** `scripts/check_pipeline_alert_rules.py` dòng 7 cố định `OUT=ROOT/"output/pipeline-completion-2026-11-08/promtool"`. Mỗi lần chạy ghi đè các tệp test và kết quả trong hồ sơ nghiệm thu ngày 08/11.

**Rủi ro.** Bằng chứng nghiệm thu đã nộp có thể bị thay nội dung mà không còn dấu vết; hash trong `file-manifest.json` của đợt 08/11 sẽ không còn khớp.

**Khắc phục.**
- Thêm tham số `--out-dir`, mặc định `output/alert-rules-<ngày chạy>/`.
- Từ chối ghi nếu thư mục đã tồn tại, trừ khi có `--force`.
- Ở production, kết quả kiểm thử được lưu thành artifact bất biến của CI, không ghi vào kho mã.

**Kiểm chứng.** Chạy script hai lần và so hash thư mục 08/11 trước và sau: phải không đổi.

### 6.3. Không có metric lag consumer Kafka — lỗi, ưu tiên trung bình

**Hiện trạng.**
- `monitoring/prometheus/prometheus.yml` chỉ thu thập `prometheus` và `fastapi-serving`; không có exporter cho Kafka.
- Trong bài thử MinIO ngừng, lag nhóm `minio-writer-group` là 5, nhưng chỉ script kiểm tra nhìn thấy.
- Cảnh báo duy nhất có thể kích hoạt là `EcommercePipelineUnhealthy` (`for: 2m`), dựa trên heartbeat mà worker ghi vào `data/pipeline/status.json`.

**Rủi ro.** Nếu consumer bị treo mà không thoát (kẹt mạng, kẹt khóa), heartbeat vẫn có thể báo healthy trong khi dữ liệu dồn ở Kafka. Đến khi phát hiện, message có thể đã gần hết thời gian lưu 240 giờ (mục 6.9).

**Khắc phục.**
- Thêm `kafka-exporter` (image `danielqsj/kafka-exporter`) hoặc Burrow vào Compose. Prometheus thu metric `kafka_consumergroup_lag` theo group và partition.
- Thêm rule:

```yaml
      - alert: EcommerceKafkaConsumerLagGrowing
        expr: sum by (consumergroup) (kafka_consumergroup_lag) > 0
              and deriv(sum by (consumergroup) (kafka_consumergroup_lag)[5m:]) >= 0
        for: 5m
        labels: {severity: critical}
```

- Worker xuất metric trực tiếp bằng `prometheus_client` (số message đọc, object ghi, thời điểm commit cuối), thay vì đi vòng qua `status.json` và API.

**Kiểm chứng.** Dừng MinIO 6 phút trong lúc producer chạy: cảnh báo lag phải chuyển sang firing và tự hết sau khi MinIO trở lại.

### 6.4. Cảnh báo chưa tới người vận hành; ngưỡng và healthcheck — đạt một phần

**Hiện trạng.**
- `monitoring/alertmanager/alertmanager.yml` chỉ có receiver `local-evidence`, một webhook nội bộ ghi tệp. Không ai thực sự nhận được thông báo.
- `EcommercePipelineBatchVolumeAnomaly` so mẻ hiện tại với trung vị 12 mẻ gần nhất, ngưỡng cố định 0,25–4 lần (`warehouse/etl/observability.py` dòng 20). Các lô thử nhỏ trong ngày làm rule này liên tục ở trạng thái pending.
- Healthcheck Docker của worker có `interval: 30s` và `retries: 3`, nên Docker chỉ báo unhealthy sau khoảng 90 giây. Trong bài thử MinIO, heartbeat đã là `stopped` nhưng Docker vẫn báo `healthy`.

**Rủi ro.** Cảnh báo không tới ai thì không có tác dụng. Ngưỡng tĩnh sinh báo động giả vào giờ thấp điểm, làm người vận hành quen bỏ qua cảnh báo.

**Khắc phục.**
- Thêm receiver thật (email, Slack/Teams/Telegram, PagerDuty/Opsgenie) và định tuyến theo `severity`:
  - `critical` → gọi người trực;
  - `warning` → kênh chat.
- Thêm luật ức chế: khi `EcommerceServingDown` đang firing thì không gửi `EcommerceModelUnavailable`.
- Mỗi rule có `annotations.runbook_url` trỏ tới mục xử lý trong `docs/05-how-to-run/core-pipeline.md`.
- Ngưỡng khối lượng: so với cùng khung giờ của các ngày trước, đặt số dòng tối thiểu trước khi đánh giá, hoặc hạ thành `severity: info`.
- Không dùng trạng thái container làm tín hiệu cảnh báo. Trên Kubernetes, tách liveness probe (tiến trình sống) khỏi readiness probe (heartbeat mới); cảnh báo dựa trên metric.

**Kiểm chứng.**
- Gửi cảnh báo thử bằng `amtool alert add` và xác nhận người nhận thật nhận được.
- Chạy một ngày tải bình thường: không có cảnh báo khối lượng giả.

### 6.5. Thứ tự phiên bản giữa các lô và việc ghi lại dòng không đổi — đạt một phần

**Hiện trạng.**
- `warehouse/etl/load.py` dòng 356 và `ingestion/consumer_inventory_to_postgres.py` dòng 172 dùng điều kiện `>=` trên phiên bản nguồn.
- Khi gửi lại cùng phiên bản, toàn bộ dòng vẫn bị ghi lại: 24/24 dòng fact và 5/5 dòng tồn kho ở bài chạy lại, `loaded_at` đổi, `loaded_records` vẫn báo 24.
- Trường hợp bản cũ đến ở lô sau (khi bản mới đã nằm trong kho) mới chỉ có unit test; bài chạy lại không tách được hai lô.

**Rủi ro.**
- Ghi thừa làm tăng tải I/O và WAL. Hệ thống hạ nguồn đọc gia tăng theo `loaded_at` sẽ xử lý lại dòng không đổi.
- Hai sự kiện khác nội dung nhưng trùng `update_time` thì bản nào đến sau sẽ thắng, nên kết quả phụ thuộc thứ tự đến.
- Lineage không phân biệt được dòng mới, dòng cập nhật và dòng không đổi.

**Khắc phục.**
- Đổi điều kiện thành "mới hơn hẳn, hoặc cùng phiên bản nhưng có khóa phụ lớn hơn". Khóa phụ có thể là offset Kafka hoặc số thứ tự nguồn lưu kèm dòng.
- Thêm so sánh `(cột nghiệp vụ) IS DISTINCT FROM (EXCLUDED.cột nghiệp vụ)` để bỏ qua dòng không đổi.
- Đếm riêng dòng mới và dòng cập nhật bằng `RETURNING (xmax = 0) AS inserted`; ghi ba số "mới / cập nhật / không đổi" vào tệp lineage.
- Thêm test tích hợp: gửi bản mới, chờ ETL xong, gửi bản cũ, rồi kiểm tra trạng thái không đổi.

**Kiểm chứng.** Chạy lại `scripts/check_replay_determinism.py`: lượt 2 phải có 0 dòng được ghi và `loaded_at` không đổi. Bài test lô sau phải đạt.

### 6.6. Chưa có mục tiêu độ trễ (SLA/SLO) — đạt một phần

**Hiện trạng.**
- Độ trễ p95 từ consumer nhận đến khi vào kho khoảng 25–35 giây, do chu kỳ poll 30 giây (`PIPELINE_POLL_SECONDS`) và flush 10 giây (`CONSUMER_BATCH_TIMEOUT`).
- Theo `latency_definition`, số đo này **không tính** thời gian từ khi sự kiện phát sinh ở nguồn tới Kafka.
- Cảnh báo duy nhất là `EcommercePipelineBatchSlow` (> 120 giây).

**Rủi ro.** Không có cam kết thì không đánh giá được vận hành tốt hay xấu. Độ trễ thực tế mà người dùng thấy (từ lúc đặt đơn) có thể lớn hơn số đo hiện tại.

**Khắc phục.**
- Định nghĩa SLO, chẳng hạn: 99% dòng đơn vào kho trong 2 phút kể từ `update_time`; dữ liệu trong `bi` không cũ quá 5 phút trong giờ làm việc.
- Đo đầu-cuối từ thời điểm sự kiện ở nguồn.
- Lập dashboard và cảnh báo theo tốc độ tiêu hao ngân sách lỗi (burn rate).
- Nếu cần độ trễ thấp hơn: rút ngắn chu kỳ poll, hoặc kích hoạt ETL ngay khi có object mới (sự kiện của MinIO) thay vì poll.

**Kiểm chứng.** Chạy tải đều trong 24 giờ, đo tỷ lệ dòng đạt SLO và kiểm tra cảnh báo burn rate kích hoạt khi cố tình làm chậm worker.

### 6.7. Chưa có CI — đạt một phần

**Hiện trạng.**
- Repo không có `.github/workflows` hay cấu hình CI khác. Bộ test (240 bài cùng 50 bài PostgreSQL bật thủ công) và các script E2E chỉ chạy tay.
- `minio/minio` và `minio/mc` dùng tag `latest`, nên mỗi lần build có thể nhận phiên bản khác nhau.

**Rủi ro.** Lỗi hồi quy chỉ được phát hiện khi có người nhớ chạy test. Lỗi demo web chập chờn và lỗi lệnh pytest ngày 15/11 đã tồn tại mà không ai biết.

**Khắc phục.**
- **Mỗi pull request:**
  - `docker compose config --quiet`;
  - `python -m pytest -q`;
  - nhóm test PostgreSQL chạy với một service container `postgres:16` và các biến `RUN_*=1`;
  - `promtool check rules` và `promtool test rules`;
  - build image.
- **Hằng đêm:** dựng stack Compose, chạy `check_continuous_pipeline.py`, `check_replay_determinism.py`, `check_fault_recovery.py`, và lưu kết quả thành artifact.
- Ghim phiên bản (hoặc digest) cho mọi image.
- Chặn merge khi CI không đạt.

**Kiểm chứng.** Một pull request cố tình làm hỏng test phải bị chặn; lượt chạy đêm phải sinh artifact có ngày.

### 6.8. Khả năng mở rộng — chưa đạt cho tải thật

**Hiện trạng.**
- Kafka có 1 broker (`KAFKA_BROKER_ID: 1`), replication factor 1 cho cả topic dữ liệu (`kafka-init`, dòng 76–77) và topic offsets.
- Mỗi topic 3 partition, nên mỗi consumer group đọc song song tối đa 3 tiến trình.
- Chỉ có 1 worker. ETL chạy tuần tự, mỗi chu kỳ 30 giây xử lý tối đa `PIPELINE_OBJECT_BATCH_SIZE=100` object.
- Khóa `pipeline_lock` là khóa toàn cục, nên không thể chạy hai ETL song song.
- Checkpoint lưu toàn bộ danh sách object ([Lần 12](../04-pipeline-updates/lan-12-chay-lai-du-lieu-va-gioi-han-luu-tru-2026-11-16.md)).
- Đợt này chưa đo thông lượng tối đa.

**Rủi ro.** Khi lượng đơn tăng (ví dụ ngày sale 11/11), tồn đọng tăng tuyến tính và không thể thêm máy để xử lý nhanh hơn. Độ trễ vượt SLO, và nếu kéo dài thì chạm thời hạn lưu của Kafka.

**Khắc phục.**
- **Kafka:**
  - cụm ít nhất 3 broker, đặt ở các vùng sẵn sàng khác nhau;
  - `replication.factor=3`, `min.insync.replicas=2`, producer giữ `acks=all`;
  - số partition tính theo thông lượng mục tiêu;
  - chuyển sang KRaft để bỏ ZooKeeper.
- Tách consumer khỏi worker ETL thành service riêng, nhân bản theo số partition.
- ETL song song theo phân vùng (ngày, sàn), dùng khóa theo phân vùng thay cho khóa toàn cục.
- Checkpoint theo watermark phân vùng (Lần 12, mục 5).
- Ở quy mô lớn hơn nữa, cân nhắc xử lý luồng (Flink, Spark Structured Streaming) và định dạng bảng có giao dịch (Iceberg, Delta) cho lake.

**Kiểm chứng.** Dùng `scripts/run_heavy_pipeline_benchmark.py` hoặc producer tải đều để tìm thông lượng bão hòa trước và sau khi nhân bản; độ trễ phải đạt SLO ở mức tải mục tiêu.

### 6.9. Sẵn sàng cao, sao lưu và khôi phục thảm họa — chưa có

**Hiện trạng.**
- Mỗi thành phần chạy 1 bản trên 1 máy, dữ liệu nằm trong volume Docker cục bộ.
- PostgreSQL: `archive_mode = off`, nên không khôi phục được về một thời điểm bất kỳ (PITR); repo cũng không có lịch `pg_dump`.
- MinIO chạy một node, không có erasure coding, không bật versioning.
- Kafka giữ message 240 giờ (`KAFKA_LOG_RETENTION_HOURS`).
- Checkpoint của worker là tệp `data/pipeline/checkpoint.json` trên thư mục mount từ máy host, tách khỏi giao dịch của warehouse.

Bài thử lỗi (mục 3) chỉ chứng minh phục hồi khi dịch vụ quay lại với **cùng volume**.

**Rủi ro.**
- Hỏng ổ đĩa hoặc mất máy là mất toàn bộ warehouse, raw lake và message chưa xử lý.
- Luồng dừng quá 10 ngày thì message cũ trên Kafka bị xóa.
- Mất tệp checkpoint thì worker xử lý lại mọi object. Kết quả vẫn đúng nhờ upsert, nhưng tốn thời gian.

**Khắc phục.**
- Xác định RPO/RTO, ví dụ: mất tối đa 15 phút dữ liệu, khôi phục trong 1 giờ.
- **PostgreSQL:**
  - sao chép streaming kèm failover tự động (Patroni), hoặc dùng dịch vụ có quản lý;
  - lưu trữ WAL bằng pgBackRest hoặc WAL-G để có PITR;
  - sao lưu đầy đủ định kỳ ra nơi lưu trữ khác vị trí.
- **MinIO:**
  - chế độ phân tán, ít nhất 4 node có erasure coding;
  - bật versioning và object lock cho `raw/` để giữ bất biến;
  - nhân bản bucket sang site khác.
- **Kafka:** replication factor 3 (mục 6.8); cảnh báo khi luồng dừng gần chạm thời hạn lưu.
- Chuyển checkpoint vào bảng PostgreSQL và cập nhật trong cùng giao dịch với lô ETL, để sao lưu nhất quán.
- Viết runbook khôi phục và diễn tập định kỳ.

**Kiểm chứng.** Diễn tập: xóa volume trên môi trường thử, khôi phục từ bản sao lưu, chạy `check_etl_warehouse_readonly.py` và `check_continuous_pipeline.py`; đo thời gian thực tế so với RTO và lượng dữ liệu mất so với RPO.

### 6.10. Bảo mật — chưa đạt cho production

**Hiện trạng (kiểm tra ngày 16/11):**
- **Mật khẩu mặc định:**
  - `.env` dùng `minioadmin`/`minioadmin` cho MinIO và `ecom_password` cho PostgreSQL;
  - Grafana dùng mặc định `admin`/`admin` vì `.env` không đặt `GRAFANA_ADMIN_PASSWORD`.
- **Quyền quá rộng:**
  - worker, API, MLflow và demo đều kết nối PostgreSQL bằng role `ecom`, là **superuser** (`rolsuper = t`);
  - ứng dụng truy cập MinIO bằng tài khoản root (`AWS_ACCESS_KEY_ID = MINIO_ROOT_USER`).
- **API không xác thực:**
  - `MODEL_RELOAD_TOKEN` không được đặt, nên `/model/reload` nhận request không cần token (`serving/app/main.py`, nhánh `if token and …`);
  - `CORS_ALLOW_ORIGINS` mặc định `*`.
- **Cổng mở rộng:** ZooKeeper 2181, Kafka 9092 và 29092, MinIO 9000 và 9001, PostgreSQL 5432, MLflow 5000, API 8000, Prometheus 9090, Grafana 3000 mở trên mọi địa chỉ của máy. Chỉ demo web, Alertmanager và receiver được giới hạn ở `127.0.0.1`.
- **Không mã hóa đường truyền:** Kafka dùng `PLAINTEXT`, không xác thực, không phân quyền theo topic; PostgreSQL có `ssl = off`; MLflow không có xác thực.

**Rủi ro.**
- Bất kỳ ai vào được mạng của máy đều có thể đọc hoặc ghi Kafka, đọc raw lake có dữ liệu cá nhân, xóa warehouse, thay model hoặc kích hoạt nạp lại model.
- Một lỗ hổng ở API là đủ để chiếm toàn quyền cơ sở dữ liệu, vì API dùng superuser.

**Khắc phục.**
- **Bí mật:** đưa vào trình quản lý bí mật (Vault, Docker/Kubernetes secrets), không để trong `.env`; đổi mật khẩu định kỳ; đặt `MODEL_RELOAD_TOKEN` bắt buộc. Tốt hơn nữa là bỏ endpoint reload ra khỏi mạng công khai.
- **Quyền tối thiểu trên PostgreSQL:**
  - `etl_writer`: `INSERT`/`UPDATE` trên fact và dim;
  - `api_reader`: `SELECT` trên schema `bi` và các bảng cần đọc, cộng `INSERT` vào bảng audit dự báo;
  - `mlflow_owner`: chỉ có quyền trên database `mlflow`.
- **Quyền tối thiểu trên MinIO:** tạo service account kèm bucket policy cho từng vai trò:
  - consumer: chỉ `PutObject` vào `raw/*`;
  - ETL: `GetObject` trên `raw/*` và `PutObject` vào `quarantine/*`;
  - MLflow: chỉ `mlflow/*`.
- **Kafka:** SASL/SCRAM cùng TLS, ACL theo topic và consumer group.
- **PostgreSQL:** `ssl = on`, `scram-sha-256`, chỉ chấp nhận kết nối `hostssl`.
- **Mạng:** bỏ ánh xạ cổng ra host cho các dịch vụ nội bộ. Chỉ công khai API (và Grafana, nếu cần) qua reverse proxy có TLS và xác thực (OIDC/SSO). Giới hạn CORS theo domain thật.
- **Chuỗi cung ứng:** ghim phiên bản image và quét lỗ hổng image trong CI.

**Kiểm chứng.**
- Quét cổng từ máy khác: chỉ còn cổng của reverse proxy.
- Thử ghi Kafka hoặc đọc MinIO không có thông tin xác thực: phải bị từ chối.
- Role API thử `DROP TABLE`: phải bị từ chối.
- Gọi `/model/reload` không có token: phải nhận 403.

### 6.11. Dữ liệu cá nhân và chính sách lưu giữ — chưa có

**Hiện trạng.**
- `ingestion/consumer_to_minio.py` (dòng 168 và 209) ghi nguyên sự kiện vào Parquet.
- Object raw TikTok ngày 15/11 chứa `buyer_name`, `recipient_name`, `recipient_phone` dạng rõ; `full_address` đã được simulator che thành `***`. Tệp quarantine chứa toàn bộ payload gốc.
- Bucket chưa bật mã hóa phía server và chưa bật versioning.
- Không có quy tắc tự xóa, không có quy trình xóa dữ liệu theo yêu cầu của chủ thể dữ liệu.
- Warehouse không lưu các cột cá nhân này.

Dữ liệu hiện là giả lập.

**Rủi ro.** Với dữ liệu thật, đây là lưu trữ dữ liệu cá nhân không giới hạn thời gian, không mã hóa, ai có quyền đọc bucket cũng xem được. Doanh nghiệp tại Việt Nam phải tuân thủ quy định về bảo vệ dữ liệu cá nhân: Nghị định 13/2023/NĐ-CP và Luật Bảo vệ dữ liệu cá nhân có hiệu lực từ 01/01/2026. Cần đối chiếu văn bản hiện hành khi triển khai.

**Khắc phục.**
- **Tối thiểu hóa ngay tại ingestion:**
  - bỏ các trường cá nhân không dùng cho phân tích (tên, số điện thoại người nhận);
  - với định danh cần để nối dữ liệu (mã người mua), thay bằng giá trị băm có khóa (HMAC), khóa lưu trong trình quản lý bí mật.
- Nếu bắt buộc giữ bản gốc: tách sang bucket riêng có mã hóa (SSE-KMS), quyền đọc hạn chế, ghi log truy cập.
- Quy tắc tự xóa cho raw và quarantine, thời gian giữ ngắn nhất mà vẫn lớn hơn cửa sổ backfill 31 ngày (Lần 12, mục 5).
- Xóa theo yêu cầu: vì raw là bất biến, dùng mã hóa theo từng chủ thể và xóa khóa khi cần (crypto-shredding), hoặc token hóa.
- Phân loại cột cá nhân trong từ điển dữ liệu (`docs/data-dictionary/`) và kiểm tra trong CI rằng warehouse không nhận cột bị cấm.

**Kiểm chứng.**
- Quét các object raw mới: không còn cột bị cấm, hoặc giá trị đã bị băm.
- Thử đọc bucket dữ liệu cá nhân bằng tài khoản ETL: phải bị từ chối.
- Object quá hạn phải bị xóa theo quy tắc.

### 6.12. Các tình huống lỗi chưa thử

**Hiện trạng.** Chưa thử:
- Kafka hoặc ZooKeeper ngừng;
- đầy ổ đĩa (MinIO, PostgreSQL, Kafka);
- mất volume hoặc mất máy;
- mạng bị chia cắt;
- lệch đồng hồ giữa các máy.

Theo `core-pipeline.md`, khi Kafka ngừng thì producer dừng do lỗi gửi và simulator **không có outbox** để gửi lại sự kiện thất bại.

**Rủi ro.** Ở hệ thống thật, ứng dụng bán hàng đứng trước Kafka. Kafka ngừng mà phía nguồn không có bộ đệm bền vững thì mất đơn ngay từ nguồn.

**Khắc phục.**
- Mở rộng `scripts/check_fault_recovery.py` với kịch bản `kafka`.
- Thử đầy đĩa bằng volume giới hạn dung lượng.
- Diễn tập mất volume cùng với khôi phục (mục 6.9).
- Phía nguồn dùng mẫu transactional outbox: ghi sự kiện vào bảng outbox trong cùng giao dịch với đơn, rồi một tiến trình riêng đẩy lên Kafka và gửi lại khi lỗi. Hoặc dùng CDC (Debezium) đọc trực tiếp từ cơ sở dữ liệu nguồn.

**Kiểm chứng.** Mỗi kịch bản mới phải có kết quả không mất dòng, không trùng và thời gian phục hồi được ghi lại, giống mục 3.

### 6.13. Điều phối nhánh ML — theo thiết kế, cần bổ sung cho production

**Hiện trạng.** Training, đăng ký model, kiểm tra drift và retrain chạy thủ công (profile `trainer`). Model phục vụ đang ở `Staging`.

**Rủi ro.** Model cũ dần khi dữ liệu thay đổi. Ở bản demo, đây là phạm vi đã thống nhất, không phải lỗi.

**Khắc phục.**
- Lập lịch bằng Airflow, Prefect hoặc Kubernetes CronJob:
  - kiểm tra mức sẵn sàng dữ liệu (đã có `ml/training/data_readiness.py`);
  - huấn luyện, đánh giá theo protocol `origin_t_plus_1_pooled_wape_v2`;
  - chỉ đưa lên khi vượt baseline;
  - triển khai thử một phần lưu lượng (canary) và có đường quay lại phiên bản cũ.

**Kiểm chứng.** Một lượt chạy theo lịch sinh run MLflow mới. Model không đạt cổng đánh giá thì không được đưa lên.

### 6.14. Lộ trình khắc phục gợi ý

| Giai đoạn | Hạng mục | Mục tiêu |
|---|---|---|
| 0 — trước bảo vệ (vài giờ) | 6.1, 6.2 | Giám sát tự phục hồi; không ghi đè bằng chứng |
| 1 — trước khi nhận dữ liệu thật | 6.10, 6.11, sao lưu cơ bản của 6.9 (`pg_dump` định kỳ, mirror MinIO, diễn tập khôi phục), 6.4 (receiver thật), 6.3 | Không lộ dữ liệu; khôi phục được; có người nhận cảnh báo |
| 2 — ổn định vận hành | 6.7, 6.5, 6.6, 6.12, giới hạn lưu trữ ở Lần 12 | Phát hiện hồi quy sớm; có SLO; lineage chính xác |
| 3 — mở rộng | 6.8, HA đầy đủ của 6.9, 6.13 | Chịu tải mùa sale; không có điểm hỏng đơn lẻ |

## 7. Tái lập

```powershell
python -m pytest -q --tb=short
$env:RUN_PIPELINE_COMPLETION_PG="1"; $env:RUN_POWERBI_SQL_TESTS="1"; $env:RUN_WAREHOUSE_SCHEMA_TESTS="1"
python -m pytest tests/test_pipeline_completion_postgres.py tests/test_powerbi_sql_metrics.py tests/test_runtime_integrity_fixes.py tests/test_warehouse_schema_v2.py -q
python scripts/check_continuous_pipeline.py
python scripts/check_fault_recovery.py            # dừng/bật postgres, minio; kill worker — chỉ chạy trên stack local
python scripts/check_etl_warehouse_readonly.py --audit-date <ngày> --source-date <ngày> --runtime
python scripts/check_ml_pipeline_readonly.py --audit-date <ngày> --runtime
```

Không để máy ngủ trong lúc chạy `check_fault_recovery.py` và `check_replay_determinism.py`.

## 8. Minh bạch sử dụng AI

AI (Claude) hỗ trợ:
- chạy toàn bộ kiểm tra trên stack local;
- viết `scripts/check_fault_recovery.py` và thực hiện thử lỗi (dừng/bật PostgreSQL, MinIO, kill worker);
- rà cấu hình bảo mật (role PostgreSQL, SSL, cổng, token, mã hóa bucket), dữ liệu cá nhân, sẵn sàng cao;
- soạn báo cáo này, gồm các đề xuất khắc phục ở mục 6.

Sau khi sinh viên đồng ý, AI sửa 6.1 (`docker-compose.yml`: restart policy và healthcheck cho Prometheus/Grafana) và 6.2 (`scripts/check_pipeline_alert_rules.py`: thư mục đầu ra theo ngày, chặn ghi vào hồ sơ đã chốt), rồi kiểm chứng như ghi trong hai mục đó. Các đề xuất còn lại ở mục 6 là hướng thiết kế, chưa được triển khai hay kiểm chứng. Sinh viên quyết định các hạng mục cần sửa trước bảo vệ và các hạng mục ghi là giới hạn.
