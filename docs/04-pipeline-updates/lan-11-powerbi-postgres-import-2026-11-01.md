# Power BI PostgreSQL Import và CSV dự phòng — 01/11/2026

## Hành vi sau sửa

Luồng chính: Kafka → consumer → MinIO → ETL → PostgreSQL → Power BI Import.
`pipeline-worker` checkpoint sau ETL thành công, không gọi exporter. Nếu ETL hoặc
ghi checkpoint lỗi, batch chưa hoàn tất vẫn được thử lại. CSV bị khóa, query
backup lỗi hoặc không có CSV không ảnh hưởng checkpoint/health của luồng chính.

CSV được giữ với CLI hiện có và tác vụ Compose một lần:
`docker compose --profile backup run --rm csv-backup`. Profile này không bật mặc
định. Backup đọc các view có sẵn, không cài lại view khi chạy từ container.
Thư mục CSV không còn được mount cho phép ghi vào worker.

Bộ `powerbi/import/*.pq` có 7 query giữ cùng tên/schema bảng và 2 query cấu hình
server/database. SQL SELECT nằm tại một nguồn `DATASET_QUERIES` dùng chung với
CSV, tránh lệch định nghĩa KPI. Có script tái tạo query khi cập nhật SQL.
Không có thông tin đăng nhập trong các file Power Query.

Bài kiểm tra liên tục đối chiếu SQL view với facts; không đọc CSV và ghi rõ
`powerbi_desktop_refresh_verified=false`. Demo web/hướng dẫn bảo vệ không còn
gắn checkpoint với thành công xuất CSV.

## Kiểm chứng

- 47 pytest đạt, 1 kiểm thử cần PostgreSQL được bỏ qua trong nhóm offline.
  Bao gồm kiểm tra lỗi backup không khiến batch ETL đã checkpoint bị chạy lại,
  lỗi ETL vẫn không checkpoint, schema/query Import khớp SQL dùng cho backup,
  và ghi CSV lỗi giữ nội dung file cũ.
  [JUnit offline](../07-bao-cao-do-an/evidence/pytest_postgres_import_2026-11-01.xml).
- 5 kiểm thử SQL đạt trên PostgreSQL thật, sử dụng bảng/view tạm và rollback;
  kiểm tra distinct order, AOV, geography và tồn kho.
  [JUnit SQL](../07-bao-cao-do-an/evidence/pytest_postgres_import_sql_2026-11-01.xml).
- Cả 7 SELECT chạy được trong một snapshot PostgreSQL `REPEATABLE READ`,
  `READ ONLY`, đúng tên/thứ tự cột. Tổng summary khớp facts: 118.650 đơn,
  286.178 đơn vị, `buyer_total_amount` 72.953.914.380. Đây là dữ liệu hiện có trong
  kho, không phải số liệu kinh doanh thực đã xác minh nguồn.
  [Evidence](../07-bao-cao-do-an/evidence/powerbi_postgres_import_2026-11-01.json).
- Compose mặc định và profile backup hợp lệ; mặc định không có `csv-backup`.
  `git diff --check` đạt.

Không thay dữ liệu warehouse, không tạo lại broker/ZooKeeper, không chạy export
ghi đè CSV chính và không khởi chạy lại toàn stack trong lượt này.

## Nghiệm thu còn lại trong Power BI Desktop

PBIX hiện có chứa DataModel nhị phân; không sửa ZIP/binary để tránh làm hỏng file.
Chưa đổi nguồn, nhập thông tin đăng nhập, Apply hoặc Refresh PBIX. Cần thực hiện
theo [hướng dẫn chuyển nguồn](../../powerbi/import/README.md), chọn Import, kiểm tra
relationships, kiểu dữ liệu và đối soát KPI sau Refresh. Power BI Service cần
gateway/lịch refresh riêng nếu truy cập PostgreSQL cục bộ.

Backup là thao tác có chủ ý, chưa có lịch tự chạy. Từng file CSV ghi atomically,
nhưng cả bộ chưa có transaction chung; chỉ dùng sau khi export hoàn tất thành công.

## Minh bạch sử dụng AI

AI hỗ trợ sửa mã, tạo bộ query, kiểm thử và viết hướng dẫn. Sinh viên cần trực tiếp
xác nhận Power BI Desktop và lưu evidence dashboard sau Refresh trước khi nộp final.
