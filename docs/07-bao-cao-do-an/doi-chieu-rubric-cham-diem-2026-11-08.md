# Đối chiếu đồ án với bảng tiêu chí chấm điểm cụ thể

Ngày đối chiếu: 08/11/2026.

Nguồn tiêu chí: ảnh bảng điểm sinh viên cung cấp, tệp `C:/Users/MINH/Downloads/1794155408313_3816641782313057918_3816641782313057918_7fc8a0f710694872ae8407975475e30c.jpg`. Tám trọng số lần lượt là 15%, 15%, 10%, 15%, 15%, 10%, 10%, 10%, tổng 100%.

Phạm vi đánh giá là pipeline dữ liệu end-to-end. Power BI Desktop do sinh viên tự hoàn thiện; chưa chấm sản phẩm Power BI trong lần đối chiếu này. Đánh giá đọc hồ sơ, mã và kết quả đã lưu, không chạy thêm kiểm thử. Các điểm dưới đây là nhận định tham khảo theo bằng chứng, không phải điểm chính thức của hội đồng. Khả năng tự giải thích, thao tác và trả lời của sinh viên chưa được kiểm chứng trực tiếp.

## Kết luận đánh giá

Phần pipeline đã có bằng chứng đạt các yêu cầu chức năng chính trong môi trường local đã thử. Mức phù hợp nhất hiện tại là **Khá**. Một số phần, đặc biệt kiểm thử và vận hành thử, có bằng chứng để đề xuất mức Tốt. Chưa đủ căn cứ xếp toàn đồ án ở mức 9–10.

Bảng chấm không có mục riêng bắt buộc độ chính xác dự báo đạt một ngưỡng WAPE. Nhánh ML cần đúng công thức, không rò rỉ dữ liệu, có baseline, đánh giá trung thực và hợp đồng phục vụ rõ ràng. WAPE cao giới hạn ứng dụng dự báo, nhưng không tự làm pipeline không đạt. Cũng không thể từ việc pipeline chạy thông suy ra dự báo đủ tin cậy để quyết định nhập hàng thực tế.

## Đối chiếu tám tiêu chí

| Tiêu chí | Trọng số | Điểm tham khảo /10 | Điểm quy đổi /100 | Bằng chứng hiện có và điểm còn hạn chế |
|---|---:|---:|---:|---|
| 1. Kiến thức & áp dụng CNTT | 15% | 8 | 12 | Đã liên kết Kafka, lưu raw trên MinIO, ETL, PostgreSQL, API và MLflow; có hợp đồng dữ liệu, chống trùng ở đích, checkpoint, kiểm soát phiên bản cũ. Đủ cơ sở mức Khá. Chưa có kết quả tối ưu có đối chứng để chứng minh mức áp dụng sâu 9–10; sinh viên vẫn phải tự giải thích được các cơ chế. |
| 2. Phân tích bài toán & tư duy | 15% | 8 | 12 | Mục tiêu pipeline, đầu vào/đầu ra, lỗi dữ liệu và phạm vi đã rõ; đã phân biệt dữ liệu mô phỏng với lịch sử CSV và giới hạn dự báo. Cần chốt yêu cầu định lượng về tải/độ trễ, giải trình 5V và các giả định nguồn còn chưa xác nhận. Quyền dùng CSV mới có khai báo của sinh viên, chưa có chứng từ trong bộ hồ sơ đã xem. |
| 3. Lựa chọn phương pháp & công nghệ | 10% | 7 | 7 | Các thành phần phù hợp vai trò, nhưng chưa có so sánh thực nghiệm công bằng giữa các cấu hình/phương án. Cần giải thích chi phí vận hành nhiều dịch vụ so với một cách làm đơn giản hơn ở quy mô hiện tại. Không cần thêm Spark/Airflow chỉ để có tên công nghệ. |
| 4. Thiết kế hệ thống | 15% | 8 | 12 | Có kiến trúc, mô hình dữ liệu, luồng lỗi, lineage theo object/run, backfill có giới hạn và khóa phối hợp. Phần mở rộng và hiệu năng chưa được kiểm chứng ở số lượng object/tải lớn; chưa có compaction và cơ chế quét raw còn giới hạn. |
| 5. Xây dựng & triển khai | 15% | 8 | 12 | Đã triển khai local bằng Docker và có API chạy thật; lô nghiệm thu đối soát đủ Kafka–raw–warehouse, chạy lại không làm tăng dữ liệu nghiệp vụ. Snapshot cuối của đợt nghiệm thu ghi các dịch vụ liên quan hoạt động. Chưa chứng minh tối ưu hiệu năng và độ ổn định ở tải lớn/dài hạn. Triển khai cloud không phải điều kiện tự suy thêm từ bảng này. |
| 6. Đề xuất giải pháp & ứng dụng | 10% | 7 | 7 | Pipeline có khả năng ứng dụng vào chuẩn hóa dữ liệu đa nguồn, truy vết lỗi và cung cấp dữ liệu cho báo cáo/dịch vụ. Chưa lượng hóa lợi ích thực tế hoặc có xác nhận triển khai nghiệp vụ. Nhánh dự báo còn giới hạn; không dùng nó để cam kết hiệu quả nhập hàng. |
| 7. Đánh giá, vận hành & cải tiến | 10% | 9 | 9 | Có 134 bài hồi quy đạt, 6 ca luật cảnh báo đạt, đối soát live, backfill/replay, lưu đầu vào–kỳ vọng–thực tế và giao nhận cảnh báo nội bộ thật. Có vận hành thử và cải tiến gắn với lỗi cụ thể. Mức 9 là đề xuất trong phạm vi demo đã kiểm chứng, không phải xác nhận mọi tình huống vận hành hay năng lực chịu tải tối đa. |
| 8. Báo cáo & trình bày | 10% | Chưa chấm trọn | Chưa chốt | Báo cáo hiện hành đã cập nhật bằng chứng và kiểm tra bố cục. Chưa quan sát slide, thuyết trình, demo do sinh viên tự thao tác, trả lời câu hỏi và sử dụng tiếng Anh chuyên môn. Không lấy sự tồn tại của báo cáo thay cho các yêu cầu này. |

Bốn tiêu chí 1, 2, 4, 5 chiếm 60%: kiến thức, phân tích, thiết kế và xây dựng hệ thống. Vì vậy hướng pipeline end-to-end phù hợp trực tiếp với phần lớn trọng số. Công nghệ phức tạp hơn hoặc WAPE thấp hơn không tự bảo đảm điểm cao hơn.

## Cách tính điểm

Điểm quy đổi = điểm tiêu chí / 10 × trọng số.

Bảy tiêu chí đầu được đề xuất **71 điểm trên phần trọng số 90 điểm**. Đây là tổng điểm tham khảo theo hồ sơ, không phải 71/100 cho toàn đồ án.

| Giả sử tiêu chí 8 đạt | Tổng điểm /100 | Quy về /10 |
|---|---:|---:|
| 7/10 | 78 | 7,8 |
| 8/10 | 79 | 7,9 |
| 9/10 | 80 | 8,0 |
| 10/10 | 81 | 8,1 |

Theo kịch bản trình bày đạt 8/10, tổng tham khảo là **7,9/10**. Muốn đạt mức 9 toàn đồ án cần cải thiện cả các tiêu chí kỹ thuật và ứng dụng, không chỉ làm đẹp báo cáo. Nếu sinh viên không tự giải thích hoặc vận hành được, các điểm kỹ thuật tham khảo cũng phải được xem xét lại.

## Việc còn cần ưu tiên ngoài Power BI

1. **Bổ sung đối chứng hiệu năng có kiểm soát.** Chọn hai cấu hình hoặc một phương pháp đối chứng hợp lý; giữ cùng dữ liệu, phần cứng và ranh giới đo. Ghi CPU/RAM/OS, tài nguyên Docker, số partition/process, batch/poll, phiên bản phần mềm; đo thông lượng xử lý hoàn tất, độ trễ và đối soát dữ liệu. Không dùng thời gian sinh dữ liệu trong bộ nhớ để nhận là thông lượng Kafka đến warehouse.
2. **Chốt quy mô mục tiêu và giới hạn kiến trúc.** Có bảng 5V, mục tiêu định lượng và thử vài mức tải; phân tích quét object, small files và phương án mở rộng. Ca nghiệm thu 8 sự kiện và ca soak 600 đơn ở 1 đơn/giây trong 10 phút chứng minh chức năng/ổn định tại tải đã thử, chưa chứng minh công suất tối đa. Giải trình lựa chọn prototype/Pandas/SQL theo cẩm nang thay vì mặc nhiên coi đã đáp ứng quy mô phân tán.
3. **Hoàn thiện tính xác thực của nguồn và giá trị sử dụng.** Bổ sung bằng chứng cấp phép nếu còn; trình bày trung thực khi link gốc đã gỡ. Ghi rõ múi giờ chưa xác nhận, độ phủ ngày và ánh xạ SKU giữa hai nguồn; xử lý hoặc công bố các cờ chất lượng legacy chưa có bằng chứng đóng. Mô tả ai dùng đầu ra, tác vụ nào được hỗ trợ và đo lợi ích trong phạm vi có thể chứng minh, không tự gán ROI.
4. **Hoàn thiện hồ sơ và khả năng bảo vệ.** Thống nhất số đo mới giữa báo cáo/slide; không dùng các khẳng định production-grade, zero-loss hay nhanh hơn phương án khác khi chưa có thực nghiệm tương ứng. Bổ sung ảnh sử dụng AI thật và phần tự kiểm tra theo cẩm nang; diễn tập demo, giải thích luồng và hỏi đáp trong thời lượng quy định. Xác nhận học vụ do sinh viên kiểm tra với GVHD/GVPB.

## Liên hệ với cẩm nang và bộ 29 câu hỏi

Đánh giá theo rubric không thay thế các điều kiện hồ sơ của cẩm nang. Những điểm còn mở về đối chứng hiệu năng, quy mô/5V, bảng môi trường thực nghiệm, nguồn và quyền sử dụng, hồ sơ AI và trình bày vẫn cần xử lý hoặc giải trình tương đương. Chưa có bằng chứng trong hồ sơ không đồng nghĩa sinh viên đã vi phạm.

Bộ 29 câu là khung kiến thức để giải thích lựa chọn, không phải danh sách 29 công nghệ bắt buộc phải triển khai. Không thêm lỗi chưa đạt chỉ vì không có CDC, Spark, dbt, SCD2, Lakehouse ACID, RAG hoặc hệ thống gửi thông báo ra ngoài. Ingestion–ETL chạy liên tục; training/retraining là tác vụ kích hoạt riêng, nên không mô tả là vòng ML tự động hoàn toàn.

## Bằng chứng dùng cho đối chiếu

- [Tổng hợp kiểm thử 08/11](../../output/pipeline-completion-2026-11-08/summary.json): 134 hồi quy đạt; 6 ca luật cảnh báo; lô 8 raw thành 2 fact duy nhất, quantity 10; 3 sự kiện lỗi được cách ly; tồn kho 50; backfill giữ nguyên nội dung nghiệp vụ.
- [Dữ liệu và kết quả từng bài kiểm tra](../../output/pipeline-completion-2026-11-08/Ket_qua_kiem_thu.html).
- [Kiểm tra tính toàn vẹn bàn giao](evidence/pipeline-completion-2026-11-08.json).
- [Báo cáo hiện hành](../../output/docx/Bao_cao_DATN_Dinh_Cong_Minh_2026-11-08_pipeline.md).
- [Đánh giá theo cẩm nang và 29 câu](danh-gia-lai-pipeline-va-cam-nang-2026-11-08.md).
- [Đối chiếu chi tiết 29 câu](doi-chieu-29-cau-hoi-pipeline-2026-11-08.md).

Các số đo trên thuộc các lượt đã lưu bằng chứng. Không suy ra hệ thống vẫn đang chạy vào thời điểm đọc tài liệu, không suy ra đã đạt tải doanh nghiệp và không suy ra đã được hội đồng chấp nhận điều kiện bảo vệ.
