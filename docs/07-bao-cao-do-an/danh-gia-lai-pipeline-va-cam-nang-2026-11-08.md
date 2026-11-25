# Đánh giá lại pipeline theo 29 câu hỏi và cẩm nang

Ngày đối chiếu: 08/11/2026. Trọng tâm do sinh viên xác định là pipeline dữ liệu end-to-end. Power BI Desktop do sinh viên tự hoàn thiện, được loại khỏi các kết luận chưa đạt trong lần đánh giá này. Đợt đánh giá đọc mã, báo cáo và bằng chứng đã lưu; không chạy thêm thử tải hoặc phát thêm dữ liệu.

## Kết luận

**Đạt phần triển khai pipeline cốt lõi trong phạm vi demo local đã kiểm chứng. Chưa đủ căn cứ xác nhận đạt toàn bộ cẩm nang hoặc đủ điều kiện bảo vệ chính thức.**

Đã có bằng chứng cho thu nạp, lưu raw, kiểm định, cách ly lỗi, chống trùng tại sink, xử lý phiên bản đến muộn, xử lý gia tăng/checkpoint, backfill có giới hạn, giám sát và giao nhận cảnh báo nội bộ. Lượt hồi quy chốt có 134 bài đạt, không lỗi/bỏ qua; 6 ca promtool đạt. Lô CMP đối soát đủ 8 Kafka/8 raw, 2 fact duy nhất với quantity 10, 3 sự kiện lỗi được cách ly; tồn kho giữ 50. Backfill dry-run/apply/replay trên đúng hai object giữ nguyên nội dung nghiệp vụ. Những phần này không tiếp tục được liệt kê như chưa thực hiện.

Độ chính xác mô hình không phải tiêu chí chính của đánh giá pipeline. Các công thức, nguồn dữ liệu, phiên bản, tính tái lập, hợp đồng đầu vào/đầu ra và xử lý lỗi của nhánh ML vẫn phải đúng. Không dùng WAPE cao để kết luận toàn pipeline không đạt; cũng không dùng pipeline chạy thông để tuyên bố dự báo đủ cho nhập hàng thật.

## Cách đọc 29 câu hỏi

Bộ 29 câu là kiến thức Data Engineering, không phải 29 công nghệ phải triển khai. “Đủ bằng chứng” dưới đây chỉ nói về triển khai và hồ sơ, không thay việc sinh viên tự trả lời được khi bảo vệ.

| Nhóm câu | Đánh giá hiện tại | Cách trình bày đúng |
|---|---|---|
| 1, 2, 3, 6, 7, 9, 10, 12, 13, 20, 22, 24, 25, 27 | Đủ bằng chứng kỹ thuật trong phạm vi đã chọn | Có kiến trúc, ETL, Parquet/partition, grain, schema contract, Kafka, idempotence, incremental, backfill, DQ, telemetry và đối soát dữ liệu nguồn BI. Câu 27 chỉ đánh giá tầng dữ liệu, không đánh giá Power BI Desktop. |
| 4, 5, 8 | Đáp ứng khi nêu đúng lựa chọn và giới hạn | Lake + Warehouse; ánh xạ vai trò raw/clean/serving; cập nhật dimension kiểu Type 1. Không nhận đã có Lakehouse ACID, ba tầng bảng Medallion hoặc SCD2. |
| 14, 15 | Có xử lý thời gian và dữ liệu đến muộn; chưa triển khai toàn bộ nội dung window | Source version guard khác watermark. Micro-batch/rolling feature khác tumbling/sliding/session window. Không cần thêm window engine nếu phạm vi xử lý đơn hàng không yêu cầu đóng cửa sổ theo event time. |
| 19, 21 | Còn giới hạn về quy mô và điều phối toàn vòng | Có micro-batch, worker và khóa ETL/backfill; chưa có compaction và chưa nghiệm thu lịch tự động/phục hồi toàn vòng ML. Không bắt buộc dùng Airflow, nhưng không nhận toàn hệ thống tự động liên tục. |
| 11, 16, 17, 18, 23, 26, 28, 29 | Ngoài công nghệ/bài toán triển khai hiện tại | CDC, Spark, dbt, clickstream và RAG không phải điều kiện bắt buộc chỉ vì xuất hiện trong bộ câu hỏi. Sinh viên vẫn cần hiểu khái niệm để giải thích lựa chọn của mình. |

## Những điểm còn chưa đóng theo cẩm nang

Đối chiếu chính là nhóm “Xây dựng kiến trúc Data Lake/Data Warehouse”, trang 10–11. Cẩm nang cho phép nội dung tương đương và yêu cầu giải trình khi không khớp chính xác yêu cầu tối thiểu. Việc ưu tiên pipeline không tự loại bỏ các yêu cầu về dữ liệu, thiết lập thực nghiệm và đánh giá kiến trúc.

| Ưu tiên | Điểm còn thiếu hoặc chưa đủ bằng chứng | Vì sao còn mở | Bằng chứng cần để đóng |
|---|---|---|---|
| 1 | Đối chứng hiệu năng có kiểm soát | Đã có số đo nhưng chưa có phép so sánh cấu hình/phương pháp trên cùng dữ liệu, cùng phần cứng và cùng ranh giới đo cho kiến trúc hiện tại | Một bảng A/B, chẳng hạn hai cấu hình batch/poll hoặc pipeline hiện tại với phương pháp đối chứng phù hợp; ghi cấu hình, thông lượng xử lý hết, phân bố độ trễ và đối soát dữ liệu của từng phương án |
| 1 | Giải trình quy mô dữ liệu lớn và lựa chọn kiến trúc tương đương | Có nhiều dữ liệu lưu và hai schema nguồn, nhưng ca live mới 8 bản tin; ca soak kiểm chứng 600 đơn ở 1 đơn/giây trong 10 phút. Các ca này chứng minh chức năng/ổn định tại mức tải đã thử, chưa chứng minh khả năng mở rộng | Bảng 5V, dung lượng/tốc độ mục tiêu, lý do chọn Pandas/SQL và giới hạn một máy; thử nhiều mức tải hoặc giải trình prototype tương đương để GVHD/GVPB xem xét. Không cần thêm Spark chỉ để có tên công nghệ |
| 1 | Bảng môi trường thực nghiệm gắn với phép đo | Báo cáo đã có phiên bản phần mềm, log build/image và mẫu tài nguyên, nhưng chưa có bảng CPU/RAM/OS/giới hạn Docker/node gắn với từng phép đối chứng | Chốt bản kiểm kê máy, image digest, số tiến trình/partition, batch/poll, giới hạn tài nguyên, cache và dữ liệu đầu vào cho lượt so sánh |
| 1 | Hồ sơ nguồn và quyền dùng CSV | Sinh viên đã khai báo được admin Xóm Data cho phép; hash khớp chỉ chứng minh đúng tệp. Chưa có bản sao đồng ý/phạm vi sử dụng trong bộ hồ sơ đang xem | Ảnh hoặc bản lưu trao đổi còn thật, mô tả phạm vi được cho phép và ghi nguồn/link gốc đã gỡ một cách trung thực |
| 2 | Tính đầy đủ và ý nghĩa một số dữ liệu nguồn/legacy | Đã kiểm chứng đủ lô Kafka và đọc đủ tệp CSV, nhưng chưa xác nhận đủ thu thập mỗi ngày, múi giờ CSV hoặc ánh xạ 2 SKU trùng mã giữa nguồn. Audit 01/11 còn 28 mã đơn xung đột header và 24 snapshot thiếu event time, chưa thấy bằng chứng đóng các mục này | Xác nhận từ nguồn nếu có; nếu không, giữ cờ chất lượng, phạm vi loại trừ/giới hạn và lý do xử lý. Không bịa timezone, lịch sử hoặc xóa bản ghi để làm sạch số liệu |
| 2 | Hồ sơ AI và khả năng tự làm chủ | Đã có bảng công cụ, mục đích và khai báo đóng góp; chưa có đầy đủ ảnh quá trình AI theo checklist, và chưa kiểm chứng khả năng sinh viên tự chạy/giải thích mã sau sửa | Ảnh thật ở phụ lục, nhật ký phần dùng AI/phần tự kiểm tra và diễn tập giải thích bằng chứng. Không tự gán tỷ lệ đóng góp khi chưa có căn cứ |
| 2 | Slide, diễn tập và xác nhận học vụ | Bản Word đã được dựng theo mẫu và kiểm tra bố cục; điều đó không chứng minh slide đúng mẫu, thuyết trình 15 phút hay phản hồi GVHD/GVPB | Sinh viên kiểm tra slide, diễn tập và có các xác nhận cần thiết; yêu cầu phản hồi GVPB trong 24h áp dụng khi đã nhận thông báo, chưa có căn cứ nói sinh viên vi phạm hạn này |

### Lưu ý về các số đo cũ

Không dùng các bảng mô tả kiến trúc để thay cho phép đối chứng thực nghiệm. Trong script benchmark cũ, chặng ghi nhãn “ingestion throughput” tính thời gian sinh dữ liệu simulator trong bộ nhớ, không phải ACK Kafka đến sink. Báo cáo ngày 01/10 ghi cách ly 100% trong bảng kết luận, trong khi JSON của mốc đó ghi quarantine_accuracy_pct=34.0. Các tài liệu này đã có ghi chú lịch sử; chúng không thể được dùng làm bằng chứng mới về hiệu năng/độ chính xác của pipeline hiện hành.

Cần thống nhất bộ tài liệu bàn giao: giữ mốc lịch sử và ghi rõ đã sửa/chưa tái kiểm chứng; bản trình bày và bảng kết quả chính chỉ dùng số đo có raw evidence khớp. Không gán các nhãn production-grade, zero-loss cho mọi tình huống hoặc nhanh hơn hệ thống khác khi chưa đo so sánh tương ứng.

## Giới hạn kỹ thuật còn lại và mức độ liên quan

1. **Small files và cách quét raw còn hạn chế khi tăng quy mô.** Worker vẫn liệt kê các object raw rồi trừ tập checkpoint; số object và danh sách đã hoàn tất tăng theo thời gian. Có micro-batch nhưng chưa có compaction hoặc phép đo ở số lượng object lớn. Chưa làm hỏng ca demo, nhưng là điểm phải nêu khi nói về scalability.
2. **MLOps không phải một vòng tự động hoàn toàn.** Ingestion–ETL chạy liên tục; training/retraining là tác vụ kích hoạt riêng, có thử Registry/API cô lập. Đây là lựa chọn hợp lệ nếu công bố đúng phạm vi. Chỉ là chưa đạt nếu báo cáo vẫn cam kết tự phát hiện drift → retrain → đăng ký → deploy liên tục mà không cần can thiệp.
3. **Các bảo đảm là có phạm vi.** Đích nghiệp vụ chống trùng; raw/quarantine có thể có bản phát lại. Producer chưa có outbox bền vững cho mọi sự kiện chưa ACK; chưa thử mất volume/đĩa, Parquet hỏng hoặc khôi phục từ backup toàn hệ thống. Các phần này là giới hạn tăng độ bền, không tự trở thành điều kiện trượt đồ án pipeline demo nếu không cam kết chúng.
4. **Thông báo chỉ giao nội bộ.** Đã có giao nhận thật qua Alertmanager tới local receiver; chưa gửi tới người khác. Nếu phạm vi bảo vệ chỉ cần quan sát và lưu cảnh báo, không còn thiếu chức năng này. Không tự coi việc chưa cấu hình email/Slack là lỗi pipeline.
5. **SLA là ngưỡng local.** 31,03 giây là P95 consumer nhận đến hoàn tất mẻ cuối 7 dòng; 33,20 giây là thời gian quan sát hoàn tất lô có polling. Không số nào thay được phân bố độ trễ toàn tuyến từ nguồn cho mọi bản tin ở tải lớn.

## Checklist học vụ theo trang 23–25

- Bố cục báo cáo và slide đúng mẫu: Word có bằng chứng định dạng/kiểm tra; slide và diễn tập chưa được nghiệm thu trong hồ sơ đang xem.
- Xác nhận thái độ và điều kiện bảo vệ từ GVHD/GVPB: chưa được cung cấp bằng chứng, không suy ra sinh viên không có hoặc đã vi phạm.
- Nội dung tối thiểu hoặc tương đương: phần pipeline cốt lõi có bằng chứng; đối chứng kiến trúc, giải trình quy mô và môi trường còn cần chốt.
- Trả lời câu hỏi cơ bản: chưa thể chấm bằng cách đọc code hay số test. Sinh viên cần trực tiếp giải thích luồng và các quyết định.
- Khai báo AI và ảnh quá trình: có khai báo, còn thiếu minh chứng ảnh/phần tự kiểm tra theo checklist.

## Những điểm không tiếp tục coi là chưa đạt

- Không đặt mục tiêu WAPE thấp nhất hoặc đòi đổi sang mô hình phức tạp hơn để công nhận pipeline.
- Không đòi thêm CDC, Spark, Airflow, dbt, SCD2, Lakehouse, clickstream/RAG cho đủ 29 câu.
- Không liệt kê schema contract, backfill, lineage theo object/run hoặc giao nhận cảnh báo nội bộ là chưa triển khai; các phần này đã có kết quả mới.
- Không đánh giá Power BI Desktop trong phần còn thiếu của tác vụ này; sinh viên đã nhận tự hoàn thiện.

## Các câu sinh viên cần tự giải thích khi bảo vệ

Vì sao raw có thể trùng mà fact không tăng? Khi nào commit offset và khi nào checkpoint ETL? Dòng lỗi đi đâu và bằng chứng nào cho thấy không bị mất? Version guard khác watermark thế nào? Vì sao 8 raw không trừ thẳng 3 quarantine để suy số fact? Khi backfill, vì sao tuổi replay không phải SLA live? API đang lấy model và lịch sử từ nguồn nào? Phép đối chứng cấu hình công bằng phải giữ những yếu tố nào giống nhau?

## Nguồn và phạm vi đối chiếu

- Cẩm nang ĐATN KHDL do sinh viên cung cấp: trang 3–4 về AI/nội dung tương đương, trang 10–11 về Data Lake/Data Warehouse, trang 23–25 về đánh giá và checklist. Tệp gốc: `C:/Users/MINH/Downloads/Cẩm nang ĐATN - KHDL.pdf`.
- [Bảng đầy đủ 29 câu và bằng chứng triển khai](doi-chieu-29-cau-hoi-pipeline-2026-11-08.md).
- [Tổng hợp kiểm thử chốt](../../output/pipeline-completion-2026-11-08/summary.json), [bảng từng bài và dữ liệu](../../output/pipeline-completion-2026-11-08/Ket_qua_kiem_thu.html), [kiểm tra tính toàn vẹn gói bàn giao](evidence/pipeline-completion-2026-11-08.json).
- [Kiểm thử Docker ngày 04/11](../../output/e2e-2026-11-04/Bao-cao-kiem-thu-end-to-end.md), [giới hạn dữ liệu nguồn](evidence/forecast-improvement-2026-11-08/data-quality.json), [các cờ chất lượng warehouse](../../warehouse/README.md).
- [Báo cáo hiện hành](../../output/docx/Bao_cao_DATN_Dinh_Cong_Minh_2026-11-08_pipeline.md), mục 3.1 về thiết lập, 3.11–3.13 về các lần cập nhật và phụ lục AI.
- Đối chiếu số liệu cũ: [báo cáo benchmark 01/10](../03-testing-and-benchmark/bao_cao_kiem_thu_hieu_nang_lan_1.md), [JSON cùng mốc](../../data/benchmark_results_run1.json), [script cách đo](../../scripts/run_heavy_pipeline_benchmark.py).

Kết luận này là đánh giá kỹ thuật và hồ sơ theo bằng chứng hiện có; quyết định chấp nhận nội dung tương đương và đủ điều kiện bảo vệ thuộc GVHD/GVPB/hội đồng.
