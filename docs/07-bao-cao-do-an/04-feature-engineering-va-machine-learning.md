# Phần 4 — Feature engineering, đánh giá mô hình và MLflow

Ngày kiểm tra: **19/10/2026**. Phạm vi: đọc mã, chạy 18 kiểm thử, đối soát artifact với warehouse và MLflow, đánh giá lại ba baseline có sẵn. Không huấn luyện lại model triển khai, đăng ký version, chuyển stage hoặc reload API.

## 1. Bài toán học máy và nguồn dữ liệu

Đơn vị dự báo là SKU/ngày. Lượng bán quan sát được tổng hợp bằng tổng quantity của các dòng không hủy theo ngày tạo đơn. Dòng đặc trưng ngày `t` có nhãn `target_t_plus_1 = y[t+1]`; các đặc trưng lịch `target_*` mô tả ngày được dự báo.

Đây là dự báo lượng đặt hàng chưa hủy, chưa phải nhu cầu tiềm ẩn đã điều chỉnh theo hết hàng. Nguồn đang gắn với manifest là `public_anonymized_historical_csv`, đọc từ `Fact_Historical_Order_Lines`.

Khi đọc nguồn lịch sử, loader gán các trường giá/doanh thu/giảm giá bằng 0 do importer không đưa dữ liệu tài chính vào bảng này. Vì vậy không được diễn giải feature tài chính hoặc phân nhóm ABC hiện tại như thông tin doanh thu vận hành thực tế. `ABCXYZClassifier` có fallback từ doanh thu không dương sang tổng quantity, nên ABC trên nguồn này là phân nhóm theo lượng bán.

Đường nạp khác có fallback từ warehouse sang Parquet cục bộ và dữ liệu mô phỏng. Những origin như `unverified_warehouse`, `synthetic_demo`, local/unknown bị chặn ở bước đăng ký theo mã hiện tại. Khi chỉ định origin lịch sử CSV, lỗi đọc nguồn được truyền ra thay vì tự đổi sang simulator.

## 2. Đối soát dataset và artifact

Đã dựng lại chuỗi ngày trong bộ nhớ từ bảng lịch sử thật, không ghi đè artifact:

| Nội dung | Kết quả ngày 19/10 |
|---|---:|
| Dòng lịch sử trong warehouse | 7.000 |
| SKU trong toàn bộ lịch sử | 228 |
| SKU còn lại sau loại hủy | 216 |
| SKU chỉ có dòng bị hủy | 12 |
| Dòng `daily_history.parquet` | 49.032 |
| Khoảng ngày của chuỗi | 28/12/2025–11/08/2026, 227 ngày |
| Dòng `features_daily.parquet` | 42.768 |
| Khoảng ngày đặc trưng | 25/01/2026–10/08/2026, 198 ngày |
| Tổng số cột artifact đặc trưng | 51 |
| Cột đầu vào model theo spec | 43 |

Chuỗi dựng lại có cùng **49.032 khóa SKU/ngày**, không có khóa thừa/thiếu và không có chênh lệch daily demand so với artifact. Fingerprint dataset trong warehouse khớp feature spec cục bộ. Như vậy chênh lệch 228 → 216 SKU được giải thích bởi bước loại hủy.

Thứ tự 43 cột đầu vào khớp feature spec. Kiểm tra nhãn với demand ngày tiếp theo trong daily history có **0 chênh lệch**. Số dòng đặc trưng khớp phép tính `216 × (227 − 28 − 1) = 42.768`: bỏ 28 ngày khởi động lag và ngày cuối chưa có nhãn.

Tập lịch sử không giao với **20 SKU simulator** hiện tại: số SKU chung bằng 0. Vì vậy không được suy ra model này đã có lịch sử cho các SKU simulator chỉ từ việc ingestion chạy tốt. Cần kiểm tra fallback/SKU chưa có lịch sử trong phần serving.

## 3. Tổng hợp ngày và readiness

`aggregate_daily` tạo grid SKU × ngày từ ngày nhỏ nhất đến lớn nhất, điền 0 cho demand, revenue, order count và discount ở ô không có dòng nguồn. Trong artifact hiện tại, **95,49% ô SKU/ngày có daily demand bằng 0**. Đây là tỷ lệ sau tổng hợp/zero-fill, chưa chứng minh mọi ô đó là ngày không bán đã được nguồn xác nhận đầy đủ.

Readiness mặc định yêu cầu:

```text
Ngày đánh giá tối thiểu = 3 folds × 14 ngày test + 7 ngày val + 14 ngày train = 63
Lịch sử tối thiểu = 63 + 28 ngày lag + 1 ngày nhãn = 92 ngày
```

Gate trước feature engineering dùng khoảng từ ngày có đơn đầu tiên đến cuối cùng, có thống kê riêng `active_source_days`, nhưng không bắt buộc mọi ngày trong khoảng có coverage đã xác nhận. Thử hai ngày nguồn 01/01 và 20/04/2026 cho kết quả `data_sufficient=true`, available history 110 ngày, active source chỉ 2 ngày.

Đây là gate độ dài chuỗi, chưa phải gate đầy đủ dữ liệu. Bảng coverage thật hiện có 0 bản ghi. Cần hoàn tất đối soát nguồn trước khi mặc định ngày trống là 0 để đánh giá chất lượng dự báo.

## 4. Thiết kế đặc trưng và rò rỉ dữ liệu

Các nhóm chính trong hợp đồng hiện tại:

| Nhóm | Nội dung |
|---|---|
| Quan sát ngày t | Demand, revenue, price, discount, order count |
| Lịch ngày t và t+1 | Thứ, cuối tuần, ngày tháng, tháng/quý, ngày đôi |
| Lag | 1, 2, 3, 7, 14, 21, 28 ngày theo spec |
| Rolling | Mean, std, max, min trên cửa sổ 7/14/28, sau shift(1) |
| EMA và momentum | EMA 7/14 sau shift(1), tăng trưởng từ lag 1 và lag 7 |
| Phân nhóm | ABC/XYZ, CV, safety stock và biến phân loại đã mã hóa |

Đặc trưng demand hiện tại `y[t]` dùng cho nhãn `y[t+1]` là hợp lệ nếu thời điểm phát hành dự báo đã quan sát xong ngày t. Nếu API dùng trong ngày khi ngày chưa đóng, cần xác định lại dữ liệu sẵn có và cutoff vận hành; lượt này chưa nghiệm thu kịch bản đó.

Các lag/rolling/EMA được xử lý theo từng SKU. Các biến phân nhóm được fit trên dữ liệu trước **23/06/2026** theo feature spec, sớm hơn kỳ test đầu tiên 01/07, rồi giữ cố định. Không thấy việc dùng nhãn test để cập nhật lag trong vòng dự báo đệ quy baseline.

Tuy nhiên, bước giá dùng `groupby(...).ffill().bfill()`. Thử SKU B chỉ có giao dịch đầu tiên ngày 04/01 với giá 99 cho thấy ô B ngày 01/01 đã mang giá 99 do backfill. Đây là khả năng đưa giá tương lai vào ngày trước khi bán. Nguồn CSV hiện gán giá 0 nên không kết luận lỗi này đã làm tăng metric hiện tại; cần sửa quy tắc khi dùng nguồn có giá thực.

Việc grid chứa các SKU trước ngày xuất hiện thực tế cũng cần được phân biệt với ngày đã hoạt động nhưng không bán. Chính sách launch date/coverage chưa được chứng minh trong phép zero-fill này.

## 5. Walk-forward và dự báo đệ quy

Baseline chia theo `target_date`, không random split. Mỗi kỳ test dài 14 ngày. Hàm đánh giá nối train và val rồi fit trước kỳ test; val trong đường chạy này không còn là tập validation độc lập dùng để báo điểm riêng.

Forecast bắt đầu ở ngày trước test. Sau mỗi bước, dự báo vừa tạo được đưa vào lịch sử để dựng đặc trưng cho bước tiếp theo; actual của kỳ test dùng để tính sai số. Bối cảnh lịch sử trong lượt tái đánh giá này là 240 ngày, phù hợp cấu hình huấn luyện cho origin CSV lịch sử.

| Fold theo thứ tự mã hiện tại | Nhãn cuối dùng fit | Kỳ test | SKU được chấm / 216 | Dòng được chấm / 3.024 |
|---|---|---|---:|---:|
| 1 | 28/07/2026 | 29/07–11/08/2026 | 215 | 3.010 |
| 2 | 14/07/2026 | 15/07–28/07/2026 | 208 | 2.912 |
| 3 | 30/06/2026 | 01/07–14/07/2026 | 183 | 2.562 |

Các fold được duyệt từ mới về cũ. Tổng cộng **8.484/9.072 dòng kỳ vọng được chấm**, khoảng 93,52%. Hàm bỏ SKU không có lịch sử phù hợp hoặc không có demand dương trong cửa sổ trước forecast origin. Metric do đó có điều kiện trên nhóm đủ lịch sử; chưa bao gồm toàn bộ cold-start/fallback.

Model được chọn bằng các fold test trong quá trình so sánh; chưa có bằng chứng trong lượt này về một holdout cuối chưa dùng để chọn model/tune. Vì vậy điểm dùng chọn champion chưa thay thế đánh giá tổng quát hóa độc lập.

## 6. Tái đánh giá ba baseline

Không fit lại hoặc tải lại model triển khai. Ba baseline không học trọng số được khởi tạo trực tiếp từ lớp đang dùng trong repository, chạy qua cùng hàm `forecast_fold_recursively` trên artifact hiện có.

| Model theo tên trong mã | Trung bình WAPE từng fold | WAPE gộp mọi dòng được chấm |
|---|---:|---:|
| Naive | 126,73% | 123,95% |
| Seasonal naive | 153,97% | 153,88% |
| Moving average | **126,08%** | 124,41% |

WAPE tính theo `sum(abs(y − prediction)) / sum(y) × 100`. Pipeline lưu `cv_wape` bằng trung bình số học WAPE của các fold, không phải WAPE tính lại trên tất cả actual/prediction nối chung. Fold có quy mô actual khác nhau làm hai phép tổng hợp khác nhau. Trong ba baseline này, thứ hạng naive và moving average đổi khi chuyển cách tổng hợp.

Moving average tái lập đúng **126,08%** của run MLflow. Các WAPE theo fold lần lượt 140,24%, 112,70%, 125,31%. MAE/RMSE trung bình fold cũng khớp mức 0,1599/0,5253 ghi trong Registry run. Giá trị WAPE gộp 124,41% không phải metric mới được ghi vào MLflow.

Đối với moving average, WAPE gộp theo bước dự báo ở horizon 1 là **107,44%**, horizon 14 là **147,80%**. Kết quả chi tiết từng horizon nằm trong JSON bằng chứng. Không diễn giải chỉ hai con số này như một đường sai số luôn tăng theo horizon.

### 6.1. Căn chỉnh baseline cần làm rõ

Với dòng ngày t dự báo t+1:

- `NaiveModel` ưu tiên `lag_1 = y[t−1]`, thay vì quan sát mới nhất `y[t]`.
- `SeasonalNaiveModel` dùng `lag_7 = y[t−7]`; cùng thứ của ngày t+1 ở tuần trước tương ứng `y[t−6]`.
- Moving average dùng mean của `y[t−7..t−1]`, bỏ quan sát ngày t.

Thử chuỗi demand 1 đến 10, forecast ngày 11/01 tại origin 10/01: naive trả 9, seasonal trả 3, moving average trả 6, trong khi quan sát mới nhất là 10. Đây là hành vi đúng theo mã hiện tại; báo cáo không gọi chúng là các baseline chuẩn dùng quan sát mới nhất trước khi thống nhất lại alignment.

### 6.2. Diễn giải chất lượng

WAPE trên 100% nghĩa tổng sai số tuyệt đối lớn hơn tổng actual trong tập được chấm. Đây chưa phải bằng chứng model đủ tốt để ra quyết định nhập hàng. Với actual không âm và tổng actual dương, dự báo toàn 0 có WAPE 100% theo chính công thức này. Đó là mốc chẩn đoán cần bổ sung, không phải khuyến nghị đặt hàng bằng 0.

Moving average đứng đầu theo tiêu chí trung bình fold trong ba baseline được kiểm tra, nhưng chưa chứng minh hiệu quả hơn mọi baseline phù hợp cho nhu cầu thưa. Các chỉ số MAE nhỏ và MAPE có smoothing không bù cho giới hạn WAPE/coverage.

## 7. Các giới hạn của metric

`calculate_all_metrics` cắt dự báo âm về 0 và làm tròn đầu ra. MAPE dùng mẫu số `actual + 1`, vì vậy là biến thể làm mượt, không trình bày như MAPE thông thường mà không giải thích.

Hàm WAPE và bias hiện trả 0 khi tổng actual bằng 0. Thử actual `[0,0]`, prediction `[4,4]` cho MAE 4 nhưng WAPE/bias đều 0. Cần quy định trạng thái không xác định hoặc báo riêng sai số tuyệt đối của tập zero-demand; không dùng WAPE 0 trong trường hợp này làm bằng chứng dự báo hoàn hảo.

Tổng WAPE toàn tập có thể che nhóm SKU quan trọng; cần bổ sung metric theo SKU/ABC-XYZ, sai lệch dự báo thừa/thiếu và tỷ lệ được chấm. Các subgroup có tổng actual 0 phải được xử lý rõ ràng.

## 8. Mô hình hiện có và phạm vi Deep Learning

Đường `run_training_pipeline` mặc định so sánh naive, seasonal naive, moving average, Ridge và LightGBM; model sau đánh giá được fit lại trên toàn tập trước khi lưu artifact. Lượt kiểm tra này chỉ tái đánh giá ba baseline, chưa tái đo Ridge/LightGBM hoặc chạy tuning.

Repository có mã LSTM/GRU và đường so sánh riêng. Tuy nhiên, chưa được phép suy ra kết quả học sâu thực nghiệm từ việc tồn tại lớp hoặc test khởi tạo:

- Dataset cửa sổ nhận mảng phẳng, không nhận SKU để chặn cửa sổ vượt ranh giới sản phẩm.
- Đường so sánh nối train+val làm dữ liệu fit, rồi còn dùng chính val để theo dõi validation loss.
- Callback đệ quy gửi một dòng đặc trưng mỗi lần; dataset suy luận sẽ pad/repeat khi ngắn hơn seq_len, chưa tái dựng đúng chuỗi nhiều ngày như lúc huấn luyện.
- Nếu PyTorch không có hoặc model chưa được tạo, `predict` dùng heuristic cột đầu; test không âm hiện có chạy được cả nhánh đó.

Các điểm trên được xác định từ mã, chưa nghiệm thu huấn luyện/suy luận mạng thật trong lượt này. Cần xử lý trước khi đưa bảng kết quả LSTM/GRU vào so sánh học thuật cùng baseline.

## 9. Đối chiếu MLflow Registry và provenance

Đã đọc MLflow qua API và artifact spec qua MinIO:

| Nội dung | Giá trị xác nhận |
|---|---|
| Model theo manifest | `ECommerceDemandForecastModel`, version 2 |
| Stage / trạng thái Registry | Staging / READY |
| Thuật toán | moving_average |
| Trạng thái run | FINISHED |
| Chiến lược đánh giá | recursive_multi_step, 3 splits |
| Origin | public_anonymized_historical_csv |
| WAPE / MAE / RMSE trong run | 126,08% / 0,1599 / 0,5253 |
| Run ID so với manifest | Khớp |
| Artifact | Có model joblib và feature_spec.json |
| Thứ tự feature / fingerprint so local spec | Khớp / khớp |

Manifest cục bộ chưa có `source_datasets`, dù feature spec và run lưu nguồn để truy vết. Đây là thiếu thông tin ở manifest; chưa thể kết luận Registry artifact thiếu fingerprint.

Feature spec run ngày 10/10 và local spec ngày 12/10 không hoàn toàn giống nhau. Khác `generated_at` và `sku_profiles`: hai SKU khác `abc_class`/`matrix_class`; danh sách 216 SKU giống nhau. Moving average hiện chỉ dùng rolling mean nên các profile này không tham gia công thức dự báo của nó. Tuy vậy, cần đóng gói và sử dụng đúng spec đi cùng model, đặc biệt với model dùng feature phân nhóm. Phần serving sẽ kiểm tra nguồn spec thực sự được nạp.

Mã đăng ký hiện yêu cầu đúng strategy, origin/fingerprint, artifact nạp được và WAPE hữu hạn. Chưa thấy ngưỡng chất lượng bắt buộc hoặc điều kiện thắng baseline chuẩn trước đăng ký. Trạng thái READY trong Registry xác nhận artifact sẵn sàng theo lifecycle kỹ thuật, chưa xác nhận chất lượng nghiệp vụ hoặc Production.

## 10. Kết quả kiểm thử và tái hiện

Nhóm `test_features.py`, `test_ml_pipeline.py`, `test_data_readiness.py`, `test_deep_learning.py`: **18 passed trong 1,45 giây** sau cấu hình thư mục tạm trong workspace. Lượt đầu có 15 đạt, 3 lỗi quyền ghi/xóa thư mục tạm; không sửa thuật toán hoặc test để có kết quả cuối.

Kiểm thử xác nhận các hợp đồng chức năng hiện tại; không có nghĩa toàn bộ các giới hạn phát hiện ở trên đã được bao phủ. Các dữ liệu biên được tái hiện bởi `scripts/check_ml_pipeline_readonly.py`.

Hiện vật:

- `evidence/ml_pipeline_2026-10-19.json`: profile dữ liệu, fold/horizon metrics, kiểm tra nguồn và Registry.
- `evidence/ml_spec_comparison_2026-10-19.json`: các trường khác giữa spec local và run.
- `scripts/check_ml_pipeline_readonly.py`: tái chạy kiểm chứng, không train hoặc thay Registry.

Lệnh thực nghiệm dùng Python runtime của Codex, `PYTHONPATH=.venv/Lib/site-packages` và `OMP_NUM_THREADS=1`. Ví dụ tham số script: `--audit-date 2026-10-19 --runtime`. Script cần quyền đọc localhost và ghi file bằng chứng cục bộ; không gọi training/register/reload.

## 11. Kết luận kiểm tra và nội dung bảo vệ

Đã kiểm chứng được chuỗi lượng bán và nhãn, hợp đồng 43 cột, nguồn dataset, đường đánh giá baseline đệ quy và khả năng tái lập metric của version 2. Chất lượng hiện tại còn hạn chế; chưa đủ căn cứ tuyên bố dự báo đã đạt yêu cầu quyết định tồn kho.

Ưu tiên tiếp theo: xác nhận coverage; sửa căn chỉnh và bổ sung baseline cho nhu cầu thưa; xử lý zero-demand metrics; tách holdout cuối; thống nhất spec theo model; kiểm tra cold-start và nguồn dữ liệu thực dùng khi phục vụ. Các kết quả học sâu cần một lượt nghiệm thu riêng sau khi sửa hợp đồng chuỗi.

**Vì sao 228 SKU chỉ còn 216?** Có 12 SKU chỉ có dòng hủy; artifact lượng bán được dựng sau loại hủy.

**Tại sao model đơn giản được chọn?** Champion là ứng viên tốt nhất trong nhóm đã so sánh theo metric và protocol được ghi nhận. Nó không mặc định là model tốt cho mọi dữ liệu hoặc đã tốt hơn mọi baseline.

**126,08% và 124,41% có mâu thuẫn không?** Không. Một giá trị là trung bình WAPE từng fold, giá trị kia là WAPE gộp. Báo cáo phải gắn rõ phương pháp tổng hợp.

**Không dùng random split đã đủ tránh leakage chưa?** Chưa. Cần kiểm tra thời điểm sẵn có của feature, backfill, cách fit phân nhóm, phạm vi tuning và việc dùng lịch sử trong dự báo nhiều bước.

**API ready có chứng minh model tốt không?** Không. Readiness kỹ thuật và chất lượng ngoài mẫu là hai tiêu chí nghiệm thu riêng.

## 12. Minh bạch sử dụng AI

AI hỗ trợ đọc mã, chạy kiểm thử, tái đánh giá baseline, đối soát nguồn/artifact và soạn báo cáo. Không huấn luyện hoặc thay model triển khai trong lượt này. Sinh viên xác nhận đóng góp tự thực hiện, ý nghĩa nguồn dữ liệu và quyết định khắc phục trước khi dùng bản cuối.
