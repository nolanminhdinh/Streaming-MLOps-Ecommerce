# Checklist kiểm thử trước khi vận hành production

Cập nhật: 14/10/2026. Phạm vi: khả năng vận hành, toàn vẹn dữ liệu, phục hồi và mở rộng của dự án Streaming MLOps E-Commerce. Không đánh giá độ chính xác mô hình hoặc dùng MAE/WAPE làm tiêu chí đạt trong tài liệu này.

Luồng cần nghiệm thu: Kafka → Python consumer → MinIO Parquet → ETL → PostgreSQL → features/MLflow/serving → API và báo cáo. Nhánh `inventory.logs` → PostgreSQL cần nghiệm thu riêng. Nhánh NiFi → MinIO JSON là nhánh riêng, không thay thế kiểm tra Parquet → warehouse.

## 1. Trạng thái hiện tại và thứ tự thực hiện

**Chưa đủ bằng chứng để nghiệm thu production.** Bài 1 đã được sửa và đạt 12/12 tiêu chí trên dịch vụ thật ngày 14/10; các bài tải, chịu lỗi và phục hồi vẫn cần thực hiện.

| Bài | Nhóm kiểm thử | Trạng thái tại ngày cập nhật |
|---|---|---|
| 1 | Toàn vẹn dữ liệu và grain dòng hàng | **PASS 12/12**: 20 dòng hợp lệ, 2 dòng quarantine; replay và ngày Việt Nam đúng |
| 2 | Idempotency, cập nhật trạng thái và dữ liệu đến trễ | Replay nhiều dòng đã đạt; đảo thứ tự cập nhật/late arrival chưa nghiệm thu |
| 3 | Tải thường và độ trễ toàn tuyến | Inventory 216 SKU ~0,11–0,19 giây tuần tự sau tối ưu; chưa nghiệm thu tải thường |
| 4 | Tải đột biến, backlog và backpressure | Chưa nghiệm thu |
| 5 | Lỗi dịch vụ và khôi phục sau crash | Chưa nghiệm thu |
| 6 | Mở rộng consumer, ETL và database | Chưa nghiệm thu |
| 7 | Chạy bền dài hạn | Chưa nghiệm thu |
| 8 | Schema, dữ liệu lỗi và quarantine | Đã có quarantine bền vững; schema evolution/poison message/replay sau sửa chưa nghiệm thu |
| 9 | Monitoring, freshness và cảnh báo | Targets hoạt động; chưa kiểm tra đầy đủ cảnh báo và người nhận |
| 10 | Backup/restore, bảo mật và phát hành | Chưa nghiệm thu |

Thứ tự tiếp theo: bài 2 và 8 → bài 3–7 → bài 9–10. Bài 9 cần chuẩn bị sớm để thu metrics. Chạy lại bài 1 khi thay đổi schema/ingestion/ETL; không nghiệm thu throughput nếu đối soát phát hiện mất dòng.

Bằng chứng hiện có:

- [Bài 1 chạy lại](data_integrity_DI2026101400c8fb.md): 12/12 đạt; 22 sự kiện, 20 dòng hợp lệ và 2 dòng quarantine.
- [Báo cáo sửa ngày 14/10](../04-pipeline-updates/lan-06-order-line-quarantine-2026-10-14.md): 106 pytest đạt; migration/backfill/views và tối ưu inventory đã kiểm chứng.
- [Báo cáo runtime](runtime_e2e_report_2026-10-13.md): 97 pytest đạt; GET inventory toàn catalog 216 SKU mất 23,312 giây; catalog CSV còn dùng tồn kho `simulated_demo`.

## 2. Chuẩn bị và thống nhất ngưỡng nghiệm thu

Chạy kiểm thử tải, crash, đầy đĩa và restore trên môi trường staging có cấu hình tương đương dự kiến triển khai. Dùng run ID, topic/group hoặc prefix riêng; chỉ truy vấn tập dữ liệu của lượt chạy khi đối soát. Không reset offset hoặc xóa volume chứa dữ liệu nghiệp vụ để làm sạch bài test.

Trước mỗi lượt chạy, ghi commit SHA, cấu hình dịch vụ, CPU/RAM/disk, số replica/partition, batch size, timeout, kích thước payload và dữ liệu warehouse sẵn có. Test trên Docker Desktop một máy chỉ chứng minh hành vi của cấu hình đó; muốn kết luận cho cụm production phải chạy trên cụm tương ứng.

Điền các thông số dưới đây trước khi chạy bài 3–7. Chưa có mục tiêu tải thì chỉ có số đo, chưa thể kết luận PASS về sức tải.

| Thông số cần chốt | Giá trị / cách xác định |
|---|---|
| Tải thường `R` | ___ sự kiện/giây, ___ giờ/ngày; gồm tỷ lệ Shopee/TikTok và inventory |
| Tải đỉnh `P` | ___ sự kiện/giây trong ___ phút; dựa trên chiến dịch bán hàng dự kiến |
| Quy mô dữ liệu | ___ SKU, ___ shop, ___ dòng/ngày, ___ ngày lịch sử, payload trung bình/P99 ___ KB |
| Tải API | ___ request/giây hoặc người dùng đồng thời; tỷ lệ từng endpoint |
| Độ trễ dữ liệu `L` | P95/P99 từ Kafka ack đến dữ liệu truy vấn được trong warehouse ≤ ___ giây/phút |
| Freshness `F` | Tuổi dữ liệu đã xử lý / watermark nguồn ≤ ___ phút; đo riêng orders và inventory |
| Backlog | Lag lớn nhất ___; thời gian dọn hết sau burst ≤ ___ phút |
| API | P95/P99 ≤ ___ ms; tỷ lệ lỗi ngoài lỗi input dự kiến ≤ ___% |
| RPO / RTO | Dữ liệu được phép mất tối đa ___ phút / thời gian khôi phục tối đa ___ phút |
| Dung lượng và dự phòng | Retention Kafka/MinIO ___ ngày; mức disk cảnh báo ___%; dư CPU/RAM ___% |

RPO là mức mất dữ liệu chấp nhận khi thảm họa; RTO là thời gian khôi phục. Với retry hoặc crash thông thường, yêu cầu giữ đủ sự kiện đã Kafka ack và không cộng trùng số liệu sau phục hồi. Mất toàn máy/cụm cần đánh giá theo RPO đã chốt và cấu hình replication/backup thực tế.

Thời gian nghiệp vụ dùng `Asia/Ho_Chi_Minh` (UTC+7). Dùng mốc ingest/ack/load để đo độ trễ xử lý; không dùng `create_time` của đơn đến trễ làm độ trễ pipeline. Timestamp nên lưu rõ offset hoặc quy ước UTC, rồi quy đổi ngày nghiệp vụ khi partition và tổng hợp.

## 3. Các bài kiểm thử

### Bài 1 — Toàn vẹn dữ liệu

**Thực hiện:** lập danh sách kỳ vọng độc lập trước khi gửi; thử đơn thường, đơn hủy, cùng mã đơn giữa hai sàn, đơn nhiều SKU, hai dòng cùng SKU có line ID khác nhau, quantity/giá/discount khác nhau và mốc 23:59/00:01 Việt Nam. Đối soát Kafka ack → từng event raw → dòng hợp lệ hoặc bản ghi bị từ chối → warehouse → tổng hợp báo cáo. Kiểm tra riêng inventory snapshot theo SKU/ngày.

**Tiêu chí đạt:** mọi sự kiện đã ack có dấu vết bền vững; mọi dòng hợp lệ có đúng khóa dòng, SKU, quantity, tiền và ngày; không mất/cộng trùng dòng; các dimension có nguồn hợp lệ được ánh xạ; đơn hủy được giữ và loại khỏi chỉ số bán hàng theo quy tắc nghiệp vụ. Mỗi dòng lỗi có định danh nguồn và lý do để xử lý lại.

**Đã sửa ngày 14/10:** khóa dòng hàng `(order_id, platform, source_line_id)` và quarantine MinIO bền vững. Nguồn cũ fallback theo SKU; nguồn có hai dòng cùng SKU cần line ID ổn định. Khi chuyển contract khóa nguồn cũ phải migration đối soát.

**Công cụ hiện có:** `scripts/run_data_integrity_check.py` kiểm tra 22 sự kiện thật, gồm hai dòng cùng SKU có line ID khác nhau, kiểm tra nội dung quarantine bền vững và replay. ETL lọc theo file của lượt chạy; chưa bao phủ toàn bộ kịch bản inventory.

### Bài 2 — Idempotency, cập nhật và dữ liệu đến trễ

**Thực hiện:** gửi lặp cùng event 2–10 lần; chạy lại cùng partition và job ETL; retry sau timeout khi database đã commit; gửi các phiên bản trạng thái theo thứ tự và đảo thứ tự; cập nhật quantity/giá; gửi đơn vào ngày cũ sau khi job ngày đó đã chạy. Thử hai snapshot inventory cùng SKU/ngày đến không đúng thứ tự.

**Tiêu chí đạt:** số dòng và tổng measure không đổi khi replay event giống nhau; cập nhật mới thay đổi đúng trường; trạng thái/snapshot cũ không ghi đè phiên bản mới. Có quy tắc version/source timestamp và lịch backfill cho dữ liệu đến trễ; ngày cũ được cập nhật đúng, không chỉ ngày hiện tại. Các hệ thống downstream nhận được thay đổi cần thiết.

**Bằng chứng:** ledger phiên bản event, trạng thái cuối kỳ vọng, SQL trước/sau replay, log retry và lần backfill. Không yêu cầu Kafka không bao giờ có duplicate; yêu cầu kết quả nghiệp vụ không bị cộng trùng.

### Bài 3 — Tải thường và độ trễ toàn tuyến

**Thực hiện:** chạy ở `R`, sau warm-up đo liên tục ít nhất 60 phút. Nạp warehouse với số SKU/shop và lịch sử dự kiến, không chỉ database rỗng. Chạy ingestion, ETL theo lịch thực tế và API đồng thời; đo riêng `/predict/demand`, inventory theo SKU và inventory toàn catalog. Kiểm tra job features/export/training hoàn thành với đúng input/provenance, không đánh giá chất lượng dự đoán.

**Tiêu chí đạt:** throughput hoàn tất ≥ tốc độ đến ở tải thường; lag không tăng liên tục; P95/P99 pipeline và API đạt ngưỡng đã chốt; không lỗi dữ liệu; job batch kết thúc trong cửa sổ lịch, không chồng job ngoài dự kiến. Đối soát bài 1 vẫn đúng ở tải này.

**Đo:** EPS ingest/complete, consumer lag, thời gian batch/ETL, tuổi dữ liệu, API latency/error theo endpoint, DB connections/locks, CPU/RAM/disk/network, số file và kích thước Parquet. Đo throughput dữ liệu hoàn tất, không chỉ tốc độ producer gửi.

**Đã tối ưu:** inventory 216 SKU đo tuần tự ~0,11–0,19 giây, cần đo tiếp dưới tải và catalog lớn hơn. Dùng snapshot warehouse đúng SKU; không lấy fallback demo làm bằng chứng dữ liệu thật.

### Bài 4 — Burst, backlog và backpressure

**Thực hiện:** từ `R` tăng lên `P` trong thời gian đã chốt rồi trở về `R`; tạo backlog khi tạm ngừng worker ở staging; tăng kích thước payload. Nếu chưa có dự báo tải, có thể khảo sát 2×/5× `R`, ghi rõ đây là mức khảo sát.

**Tiêu chí đạt:** hệ thống giới hạn buffer/concurrency và retry có kiểm soát; không OOM hoặc mất sự kiện đã ack; backlog dọn xong trong ngưỡng; tải trở về bình thường thì latency và tài nguyên phục hồi. Kafka retention đủ dài để không hết dữ liệu trước khi consumer bắt kịp.

**Bằng chứng:** đồ thị tốc độ đến/xử lý/lag theo thời gian, lag đỉnh, thời gian drain, tài nguyên đỉnh và đối soát cuối lượt. Một bài chạy xong nhưng còn backlog không được tính PASS.

### Bài 5 — Lỗi dịch vụ và phục hồi sau crash

**Thực hiện:** lần lượt restart/crash producer, consumer, ETL, Kafka, MinIO và PostgreSQL; ngắt kết nối, tạo timeout, từ chối ghi. Crash ở các điểm: trước upload, sau upload nhưng trước commit Kafka offset, giữa batch database, sau commit DB nhưng trước job checkpoint. Kiểm tra lỗi thiếu model/artifact và restart serving riêng. Kiểm tra đầy disk/quota bằng volume kiểm thử có giới hạn.

**Tiêu chí đạt:** không commit offset trước khi dữ liệu đã lưu bền vững; retry không làm sai tổng hợp; dữ liệu dở dang được rollback hoặc replay theo cơ chế đã thiết kế; lỗi được báo rõ thay vì thành công giả. Khi phục hồi, tự nối lại hoặc có runbook thao tác hữu hạn; backlog được xử lý hết, đối soát đúng.

**Giới hạn kiến trúc:** Compose hiện có một Kafka broker và topic replication factor 1. Restart broker thành công không chứng minh chịu mất broker/máy/disk. Yêu cầu HA phải được thử trên cấu hình nhiều node thực sự, cùng storage/database tương ứng.

### Bài 6 — Mở rộng ngang và điểm nghẽn

**Thực hiện:** đo cùng dataset/workload với 1 → 2 → 3 consumer trong cùng group; xác nhận mỗi partition được gán đúng một consumer trong group. Topic hiện có 3 partition; consumer thứ tư có thể không có việc. Thử rebalance khi thêm/bớt replica; thử hot key gây lệch partition. Đánh giá ETL đọc file/nạp DB song song và API replica riêng.

**Tiêu chí đạt:** thay đổi replica không mất/cộng trùng dữ liệu; worker không tranh cùng job/partition ngoài thiết kế; throughput đạt `R`/`P` theo ngưỡng và tìm được điểm giới hạn. Database connections, locks và disk I/O không vượt khả năng chịu tải. Không giả định thêm replica sẽ tăng tốc tuyến tính.

**Bằng chứng:** bảng cấu hình replica/partition → throughput/lag/P95/tài nguyên; phân phối sự kiện theo partition; query plan và thời gian query ở quy mô lịch sử dự kiến. Việc đổi số partition phải kiểm tra ảnh hưởng thứ tự và cách phân phối khóa.

### Bài 7 — Chạy bền dài hạn

**Thực hiện:** chạy ít nhất 24 giờ, ưu tiên 48–72 giờ nếu có thời gian, với tải `R` và burst định kỳ. Bao phủ chuyển ngày Việt Nam, ETL ngày mới/backfill, snapshot inventory, export và job features/training theo lịch. Theo dõi tích lũy file nhỏ, WAL, log và artifacts.

**Tiêu chí đạt:** RAM, file descriptors và connection count không tăng vô hạn; không treo worker/job; lag và freshness giữ ngưỡng; disk growth phù hợp retention/dự toán; không job chồng ngoài thiết kế. Đối soát theo từng ngày vẫn đúng sau khi thử kết thúc.

**Bằng chứng:** đồ thị cả lượt chạy, tốc độ tăng dung lượng, số lần retry/restart, thời gian từng job và đối soát ngày. Cần đo disk capacity cho thời gian lưu trữ dự kiến, không suy ra từ vài phút smoke test.

### Bài 8 — Schema, chất lượng đầu vào và quarantine

**Thực hiện:** thiếu trường bắt buộc, null/chuỗi rỗng, quantity/tiền sai kiểu hoặc âm, timestamp sai/không offset, chronology sai, platform/status lạ, payload lớn, JSON lỗi, field mới hoặc schema version không hỗ trợ. Gửi bản ghi lỗi xen giữa bản ghi tốt; sửa lỗi rồi replay từ quarantine.

**Tiêu chí đạt:** poison message không làm kẹt vô hạn cả partition; không biến dữ liệu sai thành số 0 hợp lệ một cách im lặng; field mới được xử lý theo contract. Dữ liệu lỗi được cách ly bền vững với payload, event/line ID, topic/partition/offset hoặc source object, lý do và run ID; xử lý lại không tạo duplicate. Mọi sự kiện có kết quả terminal được đối soát: hợp lệ, duplicate có lý do, hoặc từ chối có dấu vết.

**Đã sửa:** ETL validate toàn bộ trước dedupe; quarantine lưu payload và lý do; duplicate/rejected được đếm riêng. Cần nghiệm thu thêm schema evolution, poison message và công cụ replay/resolve.

### Bài 9 — Monitoring, freshness và cảnh báo

**Thực hiện:** tạo lag, dừng worker, làm ETL fail, ngừng cấp inventory, làm API lỗi/chậm và đưa disk tới ngưỡng cảnh báo trên staging. Thử không có đơn bán trong một ngày và dữ liệu nguồn thực sự ngừng về: hệ thống phải phân biệt được hai tình huống. Gửi cảnh báo tới kênh/người nhận thử nghiệm đã cấu hình và kiểm tra thông báo phục hồi.

**Tiêu chí đạt:** phát hiện trong thời gian đã chốt; cảnh báo nêu dịch vụ, nguồn/run ID, ảnh hưởng và hướng xử lý; không spam liên tục khi trạng thái không đổi. Có dashboard lag, throughput, batch failures, rejects/quarantine, freshness, API errors/latency và tài nguyên. Health/readiness phải phản ánh điều kiện phục vụ đã thiết kế, không chỉ process đang chạy.

**Kiểm tra coverage:** ngày chưa đủ dữ liệu không được xác nhận hoàn tất hoặc tính là doanh số 0; đơn đến trễ phải có cơ chế cập nhật coverage/tổng hợp. `Warehouse_Daily_Coverage` cần được chứng minh bởi đối soát nguồn, không chỉ ETL trả SUCCESS.

**Bằng chứng:** alert firing/resolved, thời điểm sự cố/phát hiện/người nhận, log thao tác xử lý. Prometheus targets up hoặc rule hợp lệ chưa chứng minh cảnh báo đến được người vận hành.

### Bài 10 — Backup/restore, bảo mật và phát hành

Ba phần dưới đây phải được đánh giá riêng; một phần đạt không thay thế hai phần còn lại.

| Phần | Cách kiểm tra | Tiêu chí đạt |
|---|---|---|
| Backup và restore | Backup PostgreSQL, MinIO, MLflow metadata/artifacts và cấu hình/checkpoint cần thiết; khôi phục lên môi trường trống; replay đoạn dữ liệu quanh mốc backup | Đo và đạt RPO/RTO; model/artifact tải được; DB/raw/provenance khớp; replay không cộng trùng; backup có lịch, retention và người chịu trách nhiệm |
| Bảo mật và quyền | Thử client không có quyền/sai credential với Kafka, MinIO, DB, MLflow, NiFi và API; kiểm tra đường mạng triển khai, TLS nếu đi qua mạng không tin cậy, log/payload nhạy cảm, thay credential | Chỉ tài khoản và mạng được phép truy cập; quyền tối thiểu đúng vai trò; secrets không trong Git/log; chức năng quản trị/reload được giới hạn; thay credential không làm mất dữ liệu |
| Phát hành và rollback | Apply migration trên bản sao DB có dữ liệu; rollout/restart có request đang chạy; rollback code/model; thử job training/register/reload thất bại và scheduler chạy theo giờ VN | Migration không làm mất dữ liệu; code/schema tương thích theo kế hoạch; rollback được trong cửa sổ cho phép; deployment giữ version/provenance rõ; không báo reload thành công giả; job có khóa tránh chạy trùng và retry hữu hạn |

Compose hiện expose nhiều cổng dịch vụ trên host, Kafka cấu hình PLAINTEXT và có credential mặc định làm fallback trong cấu hình. Khi nghiệm thu phải kiểm tra cấu hình triển khai thực tế; cấu hình chạy demo local không phải bằng chứng an toàn cho cổng public.

## 4. Công cụ hiện có và giới hạn

Chạy từ thư mục gốc dự án với Docker và dependencies đã sẵn sàng:

```powershell
# Regression suite; không thay thế kiểm thử tải hoặc chịu lỗi.
.\.venv\Scripts\python.exe -m pytest tests -q

# Bài 1, có gửi sự kiện và ghi MinIO/warehouse thật.
.\.venv\Scripts\python.exe scripts/run_data_integrity_check.py

# Smoke replay orders và inventory trên dịch vụ thật.
.\.venv\Scripts\python.exe scripts/run_e2e_runtime_check.py
```

Script integrity trả exit code 1 nếu không đạt; báo cáo vẫn được lưu. Dữ liệu chi tiết ở `data/runtime_checks/<run_id>/`, báo cáo Markdown ở thư mục này. Scripts tạo test facts và để lại dữ liệu để đối soát. Script E2E xử lý toàn bộ file của những ngày mục tiêu trong lake, nên chạy trên staging riêng để tránh cập nhật dữ liệu nghiệp vụ đang hoạt động.

`tests/load_testing/run_load_test.py` có chế độ HTTP và in-process: xác nhận báo cáo ghi HTTP thật nếu nghiệm thu API. Lượt 300 request/mức tải chỉ là khảo sát ngắn; bài 3–7 cần tải theo thời gian và toàn tuyến. Runner dùng timeout 5 giây; inventory đã tối ưu nhưng vẫn cần đo dưới tải.

`scripts/verify_pipeline_effectiveness.py` và benchmark lịch sử giúp khảo sát thành phần; không lấy số records/giây của phép xử lý cục bộ làm throughput Kafka → MinIO → PostgreSQL. Chỉ bài 1 và smoke E2E đã có runner được dùng trong lượt runtime vừa qua; các bài crash, scale, soak, restore và cảnh báo cần bổ sung kịch bản/automation trước khi nghiệm thu.

## 5. Mẫu ghi kết quả mỗi lượt chạy

Sao chép mẫu sau vào một báo cáo riêng, giữ JSON/SQL/log và đồ thị cùng run ID. Các cột trống phải điền bằng số đo hoặc ghi rõ chưa kiểm tra.

| Trường | Kết quả |
|---|---|
| Bài / kịch bản / run ID | ___ |
| Thời gian bắt đầu–kết thúc, UTC+7 | ___ |
| Commit SHA, môi trường và cấu hình | ___ |
| Tải, số SKU/shop, kích thước và lịch sử dữ liệu | ___ |
| Số event đã ack / raw / hợp lệ / duplicate / rejected / pending | ___ |
| Số dòng warehouse, tổng quantity và tiền sau đối soát | ___ |
| Throughput, lag đỉnh/cuối, thời gian drain | ___ |
| P50/P95/P99 toàn tuyến và từng endpoint | ___ |
| CPU/RAM/disk/DB connections/locks | ___ |
| Thời gian phát hiện/phục hồi; RPO/RTO nếu áp dụng | ___ |
| Link bằng chứng; lỗi còn lại và mức ảnh hưởng | ___ |
| PASS / FAIL / BLOCKED / NOT RUN, người xác nhận | ___ |

Không cộng các event cập nhật hoặc retry thành đơn/dòng mới khi đối soát. Ledger raw đếm event; ledger nghiệp vụ đếm trạng thái cuối theo grain đã thống nhất. Các dòng pending phải về 0 hoặc có giải thích trong cửa sổ xử lý cho phép trước khi kết luận.

## 6. Điều kiện cho phép vận hành

- [ ] Bài 1 và 2 đạt sau khi sửa grain dòng hàng và cập nhật/replay.
- [ ] Bài 8 đạt; mọi dữ liệu bị từ chối có dấu vết và khả năng xử lý lại.
- [ ] Bài 3–7 đạt trên tải và quy mô dự kiến; không còn backlog ngoài giới hạn khi kết thúc.
- [ ] Bài 9 đạt; người vận hành thực sự nhận cảnh báo, có runbook và dashboard freshness.
- [ ] Cả ba phần của bài 10 đạt; restore và rollback đã thực hiện thử.
- [ ] Catalog vận hành dùng đúng SKU và snapshot tồn kho thực; fallback phải được nhận diện và xử lý theo quy tắc đã duyệt, không bị hiểu là dữ liệu thật.
- [ ] Có lịch ETL/backfill/training, retention, người trực vận hành và quy trình xử lý sự cố.
- [ ] Không còn lỗi làm mất dữ liệu, sai số liệu, truy cập trái phép hoặc không thể phục hồi; ngoại lệ còn lại có người chịu trách nhiệm và giới hạn rõ.

Sau khi đạt các điều kiện trên, triển khai có giới hạn theo shop/tỷ lệ traffic, theo dõi SLO và mở rộng từng bước. Nghiệm thu vận hành theo checklist này không đồng nghĩa mô hình đã đạt độ chính xác mong muốn; đánh giá mô hình được thực hiện riêng trên dữ liệu thực tế.
