# Hoàn thiện và kiểm thử pipeline ngày 08 tháng 11 năm 2026

Luồng cốt lõi có hợp đồng dữ liệu phiên bản hóa, kiểm thử schema thay đổi, backfill giới hạn theo ngày/object, khóa chống chạy chồng, dữ liệu giám sát hàng đợi/độ trễ/khối lượng, lineage theo object và run, cùng đường giao nhận cảnh báo nội bộ. Mục tiêu là pipeline end-to-end; không thay mô hình đang phục vụ hoặc tối ưu WAPE trong đợt này. Power BI do sinh viên tự hoàn thiện theo yêu cầu.

## Kết quả chốt

- Hồi quy liên quan: **134 đạt, 0 lỗi, 0 bỏ qua**, gồm PostgreSQL thật trong schema riêng được rollback.
- Sáu ca promtool: backlog, trạng thái nhàn rỗi, độ trễ có đo/không đo, volume đã đủ/chưa đủ tham chiếu đều đúng mong đợi.
- Lô `CMP20261108c96f7bfb`: 8 ACK/8 Kafka đọc lại/8 raw trong 2 object; đích có 2 dòng duy nhất, tổng quantity 10; 3 sự kiện lỗi được cách ly; stock mới 50 giữ nguyên khi bản cũ đến sau. Hai object đã checkpoint.
- Hoàn tất lô được quan sát sau 33.20 giây từ lúc bắt đầu gửi. Đây là thời gian quan sát có polling, không phải P95 sự kiện đầu cuối.
- Mẻ ETL cuối có P95 consumer nhận đến hoàn tất mẻ **31.03 giây**, đo trên 7 dòng; không gồm độ trễ ở nguồn/producer. Không thay số chưa đo của raw cũ bằng 0.
- Backfill chỉ đúng 2 object của lô thử: dry-run, apply và apply lần hai đều giữ nguyên hash nội dung nghiệp vụ trước–sau. Không xóa/reset checkpoint. Tuổi replay được ghi riêng, không dùng làm SLA live traffic.
- Grafana đã nạp 6 panel pipeline. Cảnh báo `EcommerceDemandHistoryStale` thực sự firing và được Alertmanager chuyển tới receiver nội bộ. Không gửi email/Slack hay thông báo cho người khác.
- Dữ liệu Parquet, feature spec và manifest mô hình giữ nguyên hash; API vẫn phục vụ model Registry v2 với lịch sử warehouse, trả 7 điểm dự báo.

## Cách đọc dữ liệu

Mở [bảng kết quả HTML](Ket_qua_kiem_thu.html). Lượt chốt ở `regression-complete`; `test-index.csv` liệt kê từng bài và thư mục bằng chứng. Mỗi bài có `result.json` ghi tham số, các pha setup/call/teardown, trạng thái và log. Các ca nghiệp vụ mới có JSON đầu vào/mong đợi/thực tế; bảng dữ liệu có thêm CSV. Mã nguồn kiểm thử nằm trong `test-sources`.

Trong `live-run1`, đọc lần lượt input-orders, kafka-acknowledgements, kafka-readback, minio-raw, quarantine-output, warehouse-output và etl-run-lineage. Không suy số fact bằng raw trừ quarantine: còn khử trùng trong mẻ và upsert giữa các mẻ. Tổng hai mẻ live là 8 raw = 3 dòng sạch sau dedup trong mẻ + 2 bản trùng trong mẻ + 3 dòng bị loại; 3 lần ghi hợp lệ hội tụ về 2 dòng nghiệp vụ duy nhất.

Các thư mục `backfill-dry`, `backfill-apply`, `backfill-replay` giữ audit JSON và bảng before/after CSV. `backfill-comparison.csv` đối chiếu ba lượt. Các lượt trước và lỗi ghi CSV do giới hạn đường dẫn Windows được giữ nguyên để kiểm tra lịch sử; không cộng các lượt chạy trùng thành tổng test mới.

## Hợp đồng và giới hạn

Hợp đồng `ecommerce.orders/1.0` chấp nhận nguồn legacy không khai báo version và phiên bản 1.x bổ sung trường tùy chọn; thiếu trường bắt buộc, kiểu cấu trúc sai hoặc major version lạ được đưa vào quarantine. Timestamp không offset vẫn theo giả định UTC cũ; chưa thay giả định này bằng xác nhận của nguồn.

Ngưỡng demo đang cấu hình: raw chờ >180 giây; P95 mẻ live >120 giây; volume mẻ nằm ngoài [0,25; 4] lần trung vị tối đa 12 mẻ trước, chỉ xét sau ít nhất 6 mẻ tham chiếu. Pipeline nhàn rỗi không tự thành lỗi freshness. Volume là số dòng mẻ xử lý, không phải bộ phát hiện biến động nhu cầu thị trường. Các ngưỡng là chính sách kiểm thử local, không phải SLA doanh nghiệp đã cam kết.

Ingestion và ETL chạy liên tục. Training/registration/retraining là tác vụ kích hoạt riêng; không giả định mọi bản tin tạo một model mới. Nhánh dự báo dùng CSV lịch sử có fingerprint riêng; sự kiện simulator đi qua warehouse và API tồn kho. Cảnh báo lịch sử model cũ vẫn có ý nghĩa và không bị tắt để làm dashboard xanh. Chưa tuyên bố có SCD2, watermark/window engine, CDC, Spark, compaction, HA hoặc lịch retraining tự động nếu chưa triển khai.
