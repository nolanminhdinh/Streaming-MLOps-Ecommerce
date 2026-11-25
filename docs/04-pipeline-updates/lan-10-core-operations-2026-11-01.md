# Phạm vi final và sửa vận hành luồng chính — 01/11/2026

## Thay đổi

- Tách service NiFi sang `docker-compose.nifi.yml`, bỏ liên kết NiFi ở demo web,
  cập nhật launcher và tài liệu. Giữ mã/template/evidence cũ cho hướng mở rộng.
- Bỏ điều kiện `quarantine/nifi` trong `check_continuous_pipeline.py`.
  Trước sửa, luồng Python có thể đúng nhưng kiểm tra vẫn FAIL khi không chạy NiFi.
- Producer đợi ACK của Kafka, đẩy lỗi gửi lên thay vì chỉ ghi log rồi đếm là đã gửi.
  Đây là đánh đổi throughput để dễ kiểm chứng ở tốc độ simulator; chưa có outbox
  bền vững nên không tuyên bố tự khôi phục sự kiện gửi thất bại.
- Vô hiệu heartbeat healthy cũ ngay khi worker bắt đầu; kiểm tra consumer trước
  và sau cycle; ghi stopped khi kết thúc. Việc consumer lỗi làm worker thoát vẫn
  dùng restart policy của Docker để replay offset chưa commit.
- Thêm restart policy cho Kafka/ZooKeeper/MinIO/PostgreSQL; khai báo volume cho
  Kafka/ZooKeeper và retention mặc định 240 giờ. Worker và MLflow đợi tạo bucket
  thành công trước khi chạy.

## Kiểm chứng

- `docker compose config --quiet`: đạt.
- `docker compose -f docker-compose.yml -f docker-compose.nifi.yml config --quiet`: đạt.
- `git diff --check`: đạt sau chuẩn hóa cuối dòng.
- Nhóm `test_core_operations.py`, `test_runtime_integrity_fixes.py`,
  `test_ingestion.py`: **42 passed, 1 skipped**. Test bị bỏ qua cần PostgreSQL thật.
  [JUnit](../07-bao-cao-do-an/evidence/pytest_core_operations_2026-11-01.xml).
- Bộ test toàn dự án chưa đạt kiểm chứng: lượt chạy dừng ở nhóm HTTP demo web
  vì môi trường chặn kết nối loopback (`WinError 10013`), sau 42 test đạt và
  5 subtest HTTP lỗi. [JUnit](../07-bao-cao-do-an/evidence/pytest_core_full_2026-11-01.xml).
  Lượt thăm dò trước đó bị dừng, không dùng làm số liệu nghiệm thu.
- Chưa chạy được Kafka → API trên dịch vụ thật: Docker Engine không có pipe
  hoạt động; `docker desktop start` thất bại vì không tìm thấy registry launcher.
  Không có thay đổi dữ liệu runtime hay migration warehouse trong lượt này.

Python mặc định và launcher `.venv` không hoạt động trên máy hiện tại. Kiểm thử
dùng Python 3.12.14 được đóng gói sẵn và dependencies đang có của `.venv`;
không cài/đổi phiên bản thư viện. Có cảnh báo tương thích PyArrow/Pandas,
chưa xác nhận môi trường này trùng phiên bản dependencies của Docker.

## Điều kiện còn lại

Volume Kafka/ZooKeeper mới không tự chuyển dữ liệu từ container cũ. Phải xử lý
backlog hoặc di chuyển volume trước khi tạo lại stack đang có dữ liệu cần giữ.
Xem [hướng dẫn vận hành, xử lý lỗi và kế hoạch 10 ngày](../05-how-to-run/core-pipeline.md).

Runtime phục hồi khi dừng/bật service, throughput sau khi producer đợi ACK,
training, model quality và refresh PBIX vẫn cần đo thực tế. NiFi không còn thuộc
tiêu chí này. Không đưa kết quả unit test thành kết luận đã nghiệm thu toàn hệ thống.

## Minh bạch sử dụng AI

AI hỗ trợ rà soát, chỉnh mã/cấu hình, viết kiểm thử và tài liệu. Sinh viên cần xác
nhận các kết quả runtime trước khi đưa vào báo cáo final.
