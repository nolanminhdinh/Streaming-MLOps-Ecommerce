# Phần 5 — Model serving, cảnh báo tồn kho và monitoring

Ngày kiểm tra: **19/10/2026**. Phạm vi: đọc mã, chạy 44 kiểm thử, gọi API đang chạy, đọc log khởi động, Prometheus và Grafana. Không train, đăng ký model, restart, reload hoặc đổi cấu hình dịch vụ trong lượt này.

## 1. Kết quả vận hành cần phân biệt với thiết kế

Tại thời điểm kiểm tra, API còn hoạt động nhưng **chưa nạp được model ML**. `/health` trả HTTP 200 với `status=degraded`, `/ready` trả HTTP 503. Request dự báo vẫn trả 200 bằng heuristic. Đây là trạng thái hiện hành, khác bằng chứng Registry version 2 sẵn sàng đã xác nhận ở phần 4.

| Kiểm tra thực tế | Kết quả |
|---|---|
| `/health` | 200; degraded; model_loaded=false |
| `/ready` | 503; warehouse_connected=true; history_available=true |
| Model trong metadata manifest | Moving average v2, Staging |
| `model_source` đang nạp | heuristic_catalog |
| Lịch sử theo origin model | Đến 11/08/2026, cách ngày kiểm tra 69 ngày |
| Prometheus scrape FastAPI / bản thân | up / up |
| Rule thiếu lịch sử | inactive, health=ok |
| Alertmanager hoạt động | 0 |
| Grafana `/api/health` | 200; database=ok; version=10.1.2 |

Metadata vẫn có `status=READY_FOR_SERVING` đọc từ manifest, dù `/ready` đang 503. Khi báo cáo trạng thái, cần ưu tiên trạng thái nạp thực tế và nguồn dự báo, không dùng riêng status trong manifest hoặc HTTP 200 của request.

## 2. Nguyên nhân model không được nạp

Log khởi động ghi nhận API đọc manifest v2, nhưng request đến MLflow `model-versions/get` bị `ReadTimeoutError`, timeout đọc 5 giây. Sau đó API đọc feature spec cục bộ `/app/data/feature_spec.json`, có 43 cột, và chuyển sang heuristic.

Đây là lỗi truy cập Registry trong lần khởi tạo đã quan sát, chưa chứng minh artifact model hỏng. Trong phần 4, Registry/run/artifact của version 2 vẫn đọc được. Mã `get_model_manager` chỉ tự load khi `is_loaded=false`; `_load_model_candidate` đánh dấu đã khởi tạo ngay cả khi không có model. Vì vậy sau một lỗi startup, manager không tự thử lại chỉ vì MLflow đã truy cập được sau đó.

Hướng khắc phục cần nghiệm thu riêng là retry/backoff nạp Registry hoặc cơ chế phục hồi có kiểm soát; có thể dùng reload để phục hồi vận hành sau khi kiểm tra artifact. Lượt này giữ nguyên trạng thái để ghi nhận và không gọi reload.

## 3. Thiết kế nạp model và feature spec

`ModelManager` được dùng như singleton trong tiến trình serving. Nếu manifest chỉ định version số dương, loader ghim đúng version đó, không tự lấy bản mới nhất trong Registry. Khi đã cấu hình Registry mà tải lỗi, loader không tráo sang joblib cục bộ; local joblib chỉ là đường khác khi không cấu hình Registry.

Feature spec ưu tiên tệp đi cùng artifact tải được, sau đó mới dùng đường cục bộ. Spec xác định thứ tự cột, lag/rolling/EMA, category mappings, hồ sơ SKU, origin và source fingerprints. Như phần 4 đã đối chiếu, local spec khác run spec ở phân nhóm của hai SKU; khi startup lỗi hôm nay, log xác nhận API dùng local spec, không phải spec của artifact model đã nạp thành công.

Reload tạo một manager ứng viên rồi thay trạng thái nếu ứng viên có model. Thử lỗi bằng mock xác nhận reload thất bại giữ lại model cũ. Đây là bằng chứng chức năng cục bộ, chưa phải phép thử tải đồng thời khi đổi version hoặc chứng nhận zero-downtime.

Endpoint reload có kiểm tra token nếu biến `MODEL_RELOAD_TOKEN` được cấu hình. Lượt này không gọi endpoint và chưa nghiệm thu cấu hình token trên runtime.

## 4. Dự báo và fallback

API dự báo một SKU kiểm tra thứ tự ngày và giới hạn khoảng trả về tối đa 90 ngày. Khi có model và lịch sử, manager dựng chuỗi đệ quy từ cuối lịch sử đến ngày yêu cầu; giới hạn mặc định là 180 bước tính cả khoảng trống trước ngày bắt đầu trả kết quả.

Các nguồn được phân biệt:

| Điều kiện | Nhãn nguồn dự báo |
|---|---|
| Model Registry nạp được và có lịch sử | mlflow_registry |
| Joblib cục bộ được nạp trong chế độ cho phép | local_joblib |
| Có lịch sử nhưng không dùng được model | heuristic_history |
| Không có lịch sử phù hợp | heuristic_catalog |

Lịch sử được truy vấn theo origin và fingerprints của spec. `history_end` nội bộ là min(ngày trước from_date, ngày cuối nguồn). Readiness chỉ kiểm tra có model, repository sẵn sàng và ngày lịch sử tồn tại; chưa có kiểm tra tuổi lịch sử, coverage hoặc khả năng dự báo cho từng SKU. Hôm nay readiness đỏ vì model chưa nạp; lịch sử cũ tự nó chưa phải điều kiện làm readiness đỏ theo mã.

### 4.1. Các request thật đã kiểm tra

| Trường hợp | HTTP | Model source | History source |
|---|---:|---|---|
| Gọi lại một khóa forecast đã có cho SKU lịch sử | 200 | heuristic_history | warehouse |
| SKU simulator `OL-IP15PM` | 200 | heuristic_catalog | none |
| SKU không có trong catalog | 200 | heuristic_catalog | none |
| Ngày bắt đầu sau ngày kết thúc | 400 | Không chạy dự báo | — |
| Khoảng yêu cầu 365 ngày | 400 | Không chạy dự báo | — |

SKU không biết vẫn nhận số dự báo mặc định thay vì 404. Response gắn nhãn heuristic, nhưng ứng dụng sử dụng cần đọc nhãn để tránh diễn giải như dự báo model cho sản phẩm đã xác minh. `store_id` có trong request schema nhưng chưa được truyền vào tính dự báo; chưa chứng minh hỗ trợ dự báo tách theo kho/cửa hàng.

Request có thể vẫn mang model version 2 trong metadata ngay khi `model_source=heuristic_*`. Do đó một dòng kết quả có version không đủ để kết luận được tạo bởi model v2.

### 4.2. Audit dự báo và khoảng bất định

API chỉ ghi audit cho nguồn model được cho phép với lịch sử warehouse và origin/fingerprints phù hợp; heuristic không được ghi thành forecast ML. Lượt kiểm tra gọi lại một forecast đã có và hai request fallback: `Fact_Forecast_Predictions` trước/sau đều **6 dòng**, không tăng dòng audit.

Hàm audit yêu cầu target sau `history_end`, nhưng chưa kiểm tra target sau thời điểm phát hành thực tế. Gọi dự báo hồi cứu với history_end cũ vẫn có thể thỏa điều kiện đó nếu model được nạp; cần chốt quy tắc sử dụng forecast phát hành muộn trong đánh giá BI. Không dùng một dự báo tạo sau khi actual đã tồn tại như bằng chứng dự báo tiên nghiệm.

Nếu ghi audit lỗi, serving ghi log và tiếp tục trả kết quả; chưa có cơ chế bảo đảm mọi response ML đều đã lưu audit thành công.

Khoảng lower/upper hiện tính xấp xỉ `prediction ± 1,96 × sigma` từ độ lệch chuẩn nhu cầu lịch sử, có điều chỉnh ở heuristic. Đây chưa phải prediction interval đã hiệu chuẩn theo sai số/horizon. Không khẳng định đạt coverage 95% chỉ từ tham số 1,96.

## 5. Công thức và nguồn dữ liệu tồn kho

Inventory service lấy nhu cầu từ forecast model nếu có; nếu forecast đang heuristic nhưng có lịch sử, dùng thống kê lịch sử 28 ngày; nếu thiếu lịch sử, dùng catalog fallback.

Thiết kế công thức:

```text
SS  = max(1, ceil(Z × sigma_daily × sqrt(L)))
ROP = max(SS + 1, ceil(mean_daily_demand × L + SS))
```

Đây là quy tắc tham số hóa để hỗ trợ quyết định, chưa phải nghiệm tối ưu chi phí. Mã áp sàn SS=1 và ROP≥SS+1; thử demand=0, sigma=0 vẫn trả SS=1, ROP=2. Độ lệch chuẩn lấy từ nhu cầu quan sát, chưa phải sai số dự báo lead-time; service level và lead time là giả định cần kiểm chứng trước ứng dụng thật.

Quy tắc cảnh báo: stock≤SS là CRITICAL; SS<stock≤ROP là WARNING; stock>ROP là NORMAL. Lượng nhập đề xuất đưa tồn về `ROP + ceil(7 × mean_daily_demand)` nếu cần nhập. `days_until_stockout` dùng `stock / max(mean_demand, 0,1)`.

Thứ tự ưu tiên nguồn tồn: giá trị request → snapshot warehouse → tồn mô phỏng demo. Các nhãn `stock_source` và `demand_source` là hai chiều độc lập.

### 5.1. Đối chiếu nguồn tồn và nhu cầu trên API

| SKU kiểm tra | Stock | Stock source | Demand source |
|---|---:|---|---|
| Một SKU lịch sử | 0 | simulated_demo | warehouse_history |
| `OL-IP15PM` | 350 | warehouse_snapshot:2026-10-09 | fallback_catalog |
| SKU E2E ngày 14/10 | 123 | warehouse_snapshot:2026-10-14 | fallback_catalog |

Snapshot E2E khớp truy vấn DB: 123 units, ngày 14/10, cũ 5 ngày tại thời điểm kiểm tra. Snapshot OL-IP15PM cũ 10 ngày. Hàm chọn snapshot mới nhất theo SKU nhưng chưa từ chối snapshot quá cũ. Nhãn warehouse không đồng nghĩa tồn đã đủ mới để nhập hàng.

Đã gọi thêm request override stock=0 cho SKU lịch sử; response dùng nguồn request. Không cập nhật bảng tồn kho qua các request kiểm tra.

GET inventory ràng buộc lead time 1–30, service level 0,50–0,99. POST schema hiện chấp nhận lead time âm, service level 2,0 và stock âm; điều này đã tái hiện ở bước dựng schema trong bộ nhớ. Chưa gửi các giá trị lỗi này đến dịch vụ thật. Cần đồng nhất validation của hai phương thức để tránh lỗi tính toán hoặc kết quả vô nghĩa.

### 5.2. Cache và độ mới

Repository mặc định cache truy vấn 60 giây, catalog 300 giây, snapshot stock 30 giây; cache tổng hợp cảnh báo phục vụ metrics là 30 giây. Khi truy vấn DB lỗi, repository ngắt mạch mặc định 30 giây và trả None để tầng gọi dùng fallback.

`available()` phản ánh cờ bật và trạng thái circuit breaker, không tự ping DB ở mỗi lần gọi. Nhãn kết nối nên được diễn giải cùng kết quả truy vấn gần nhất. Các cache giúp giảm truy vấn nhưng có thể làm API chưa phản ánh ngay snapshot hoặc dữ liệu vừa nạp.

## 6. Monitoring thật và các khoảng trống

Prometheus đang scrape được FastAPI và chính nó; Grafana có database health tốt. Việc kiểm tra `/api/health` chưa xác nhận từng panel/dashboard hiển thị đúng. API Grafana báo version 10.1.2, khác image 11.0.0 ghi trong Compose; cần chốt môi trường/image thực khi tái lập demo.

`/metrics` có counter request, histogram latency, gauge model ready/info, tồn kho, drift và readiness huấn luyện. Trước khi xuất gauge tồn kho, endpoint tính/quét cảnh báo theo cache. Các số cảnh báo có thể gồm stock demo hoặc demand fallback; không tự coi đó là số SKU thực sự cần nhập trong doanh nghiệp.

Hiện chỉ có rule `EcommerceTrainingDataInsufficient`, biểu thức missing history days > 0 trong 30 giây. Rule inactive là phù hợp với file trạng thái huấn luyện gần nhất báo đủ ngày. Nó không theo dõi lỗi nạp model hôm nay.

Chưa có rule trong cấu hình đọc được cho model_ready=0, readiness 503, fallback tăng, lịch sử/snapshot cũ, latency vượt ngưỡng hoặc API lỗi. **Target up trong Prometheus chỉ xác nhận scrape thành công**; trường hợp API fallback hôm nay vẫn có target up là bằng chứng cụ thể.

Prometheus báo 0 active Alertmanager. Training status cho biết webhook chưa cấu hình, chưa thử gửi. Vì vậy lượt này xác nhận thu thập/hiển thị và rule được nạp, chưa xác nhận thông báo đến người vận hành.

## 7. Drift và vòng tái huấn luyện

Detector so sánh hai cửa sổ của feature store: mặc định 28 ngày tham chiếu và 7 ngày gần nhất, tính tương đối theo ngày cuối artifact, không mặc định theo ngày chạy thực tế. Nếu artifact chưa cập nhật, detector vẫn có thể phân tích lịch sử cũ.

Mã tự triển khai KS với p-value xấp xỉ và PSI. Cờ drift từng feature dựa vào p-value dưới ngưỡng, dataset drift dựa vào tỷ lệ feature drift; PSI được báo kèm, không phải điều kiện quyết định trong nhánh đã đọc. Báo cáo Evidently tương tác là đầu ra tùy chọn khi thư viện tương thích; không dùng sự tồn tại thư mục `evidently` để khẳng định mọi quyết định đều do Evidently thực hiện.

Biến `target_drift_detected` kiểm tra thay đổi phân phối daily demand. Đây là target distribution drift; chưa tự chứng minh quan hệ P(y|X) đã đổi hoặc sai số model đã tăng, nên không đồng nhất với bằng chứng concept drift đã xác nhận.

Hiện vật cục bộ hôm nay:

| Hiện vật | Giá trị đã đọc |
|---|---|
| Drift summary | Đánh giá ngày 12/10; feature_store từ warehouse, origin CSV |
| Drift share | 0,375; dataset drift=true; target drift=false |
| Giá trị drift xuất qua metrics | 0,375 |
| Retraining audit hiện có | SUCCESS ngày 24/09 |
| Training status hiện có | TRAINING_SUCCEEDED ngày 12/10, chưa cấu hình webhook |

Drift summary và training status là trạng thái đã lưu, chưa xác nhận một lượt đánh giá/retrain mới ngày 19/10. Retraining log ngày 24/09 không chứng minh drift ngày 12/10 đã dẫn đến vòng cập nhật model hiện hành.

Coordinator có các bước: đọc drift → kiểm tra nguồn → readiness → training → register version thật → ghi manifest → gọi reload → lưu audit. Nếu thiếu summary hoặc không drift thì skip; synthetic demo không tự trigger khi không force. Fingerprint/origin khác deployment bị chặn trong đường mặc định.

Khi training/registration lỗi, mã giữ/khôi phục manifest; khi reload không xác nhận ready, khôi phục manifest và ghi thất bại. Điều đó chưa xóa version mới đã tạo trong Registry hoặc chứng minh API đã rollback nếu request reload timeout sau khi phía server thực sự đổi model. Cần nghiệm thu version live sau reload và các kịch bản mạng lỗi.

Chưa thấy scheduler/service định kỳ gọi detector/coordinator trong Compose và startup script đã đọc. Có mã closed-loop không đồng nghĩa vòng tự chạy liên tục đã được triển khai. Audit coordinator hiện ghi đè một tệp JSON trạng thái mới nhất, chưa phải lịch sử append-only của mọi lần chạy.

## 8. Kiểm thử và phạm vi bằng chứng

Các tệp `test_serving.py`, `test_serving_realdata.py`, `test_monitoring.py`, `test_pipeline_integrity.py`: **44 passed, 1 warning trong 3,40 giây**. Warning là Starlette TestClient/httpx deprecation.

Lượt chạy sandbox bị treo ở việc tạo loopback socketpair của asyncio trong FastAPI TestClient; stack dump sau 45 giây xác nhận vị trí này. Chạy lại với quyền loopback phù hợp đạt 44 test. Không sửa thuật toán hoặc test để có kết quả cuối.

Các test bao gồm tính tương đương feature online/offline, forecast đệ quy, nhãn fallback, stock từ warehouse giả lập, exposition metrics và các nhánh retraining thành công/lỗi có mock. Chúng chưa thay thế nghiệm thu model live: API thật hôm nay vẫn chưa ready.

Thử lỗi bổ sung trong bộ nhớ xác nhận giữ model cũ khi reload ứng viên thất bại, sàn SS/ROP và khoảng trống POST validation. Không kích hoạt retrain hoặc cố ý làm gián đoạn dịch vụ thật.

Hiện vật:

- `scripts/check_serving_monitoring.py`: script kiểm chứng API/monitoring và đối chiếu DB.
- `evidence/serving_monitoring_2026-10-19.json`: response, nguồn dữ liệu, trạng thái target/rule và số dòng audit trước/sau.
- `evidence/serving_startup_diagnosis_2026-10-19.json`: nguyên nhân startup từ log.

## 9. Các việc cần hoàn thiện trước nghiệm thu

1. Phục hồi việc nạp model và nghiệm thu lại `/ready` cùng forecast dùng `mlflow_registry`; bổ sung retry startup có giới hạn.
2. Theo dõi model readiness/fallback, tuổi dữ liệu và snapshot; chốt routing thông báo ra ngoài.
3. Đồng nhất validation inventory GET/POST, quy định SKU lạ và dữ liệu fallback trong giao diện/API.
4. Chốt thời điểm phát hành forecast cho BI và kiểm định khoảng bất định; phân biệt số liệu demo với vận hành.
5. Triển khai lịch chạy drift/retraining nếu nằm trong phạm vi đồ án; xác nhận nguồn, version live và rollback bằng thử nghiệm có bằng chứng.

## 10. Câu hỏi bảo vệ

**Vì sao dự báo trả 200 mà readiness 503?** Endpoint dự báo cho phép heuristic dự phòng. Readiness yêu cầu model ML đã nạp và có lịch sử warehouse; phải đọc model_source trong từng response.

**Prometheus up có nghĩa model chạy đúng không?** Không. Hôm nay Prometheus scrape API thành công trong khi model_loaded=false. Cần metric/rule riêng cho model và dữ liệu.

**Stock từ warehouse có đủ để tin cảnh báo không?** Chưa. Phải kiểm tra tuổi snapshot và demand_source; snapshot thật vẫn có thể kết hợp với nhu cầu catalog demo.

**Vòng retraining đã tự chạy liên tục chưa?** Đã có mã và test coordinator, nhưng lượt này chưa xác nhận lịch chạy tự động hoặc một vòng runtime mới. Các log cũ có mốc thời gian khác nhau cần được trình bày đúng phạm vi.

**Khoảng 95% trong response đã hiệu chuẩn chưa?** Chưa. Hiện là xấp xỉ dựa trên độ lệch chuẩn nhu cầu; cần đánh giá coverage ngoài mẫu theo horizon.

## 11. Minh bạch sử dụng AI

AI hỗ trợ đọc mã/log, chạy test, gọi các request kiểm tra, đối chiếu nguồn dữ liệu và viết báo cáo. Không train/reload hoặc đổi cấu hình triển khai trong lượt này; số dòng audit forecast được kiểm tra không tăng. Sinh viên xác nhận đóng góp tự thực hiện và quyết định phạm vi khắc phục trước báo cáo cuối.
