# THƯ VIỆN BÁO CÁO CẬP NHẬT, KHẮC PHỤC LỖI & TỐI ƯU HÓA LUỒNG DỮ LIỆU (PIPELINE UPDATES & CHANGELOG)

> **Mục đích**: Thư mục này là trung tâm lưu trữ toàn diện lịch sử nâng cấp, phân tích nguyên nhân gốc rễ (Root Cause Analysis - RCA), các bản vá lỗi (Bug Fixes) qua từng đợt kiểm thử thực tế, cùng danh mục các tính năng và kỹ thuật tối ưu hóa luồng dữ liệu của hệ thống Streaming MLOps E-Commerce.

---

## 1. CƠ CẤU & MỤC LỤC TÀI LIỆU CẬP NHẬT THEO ĐỢT KIỂM THỬ

Mỗi đợt kiểm thử (Test Run) có sự thay đổi về mã nguồn hoặc tối ưu hóa luồng dữ liệu đều được biên soạn thành một tài liệu độc lập tương ứng:

| Đợt kiểm thử | Tệp báo cáo cập nhật & tối ưu | Quy mô thử nghiệm | Trọng tâm sửa đổi & Tối ưu hóa | Trạng thái |
| :---: | :--- | :---: | :--- | :---: |
| **Lần 1** | [`lan-01-khac-phuc-loi-va-toi-uu-luong.md`](lan-01-khac-phuc-loi-va-toi-uu-luong.md) | 10,000 đơn hàng | • Khắc phục lỗi ép kiểu TIMESTAMP PostgreSQL (`NaN` handling)<br/>• Sửa lỗi seed Dimension tables & đồng bộ schema<br/>• **Tối ưu hóa Vectorized Validation tăng tốc 18 lần (49.6s → 2.7s)**<br/>• Xây dựng cơ chế DLQ 2 tầng & Engine Benchmark chuyên sâu | **ĐÃ HOÀN THÀNH** |
| **Lần 2** | [`lan-02-sua-loi-va-toi-uu-he-thong.md`](lan-02-sua-loi-va-toi-uu-he-thong.md) | Rà soát toàn hệ thống, 72 unit tests lịch sử | • Sửa 15 lỗi: `date_key` NULL, training–serving skew, artifact sai fold, vòng retrain giả, tồn kho giả lập, leakage ABC/XYZ, múi giờ...<br/>• Một phần serving/data path đã được chạy lại trong nghiệm thu Lần 3; model ML và retraining chưa đủ điều kiện xác nhận | **CÁC MẮT XÍCH ĐÃ CHẠY — MODEL CHƯA READY** |
| **Lần 3** | [`lan-03-e2e-remediation-2026-10-09.md`](lan-03-e2e-remediation-2026-10-09.md) | Rà soát điểm nối; chạy nghiệm thu runtime một phần | • Ingestion → MinIO/PostgreSQL và ETL đã chạy; Power BI export, Prometheus/Grafana, API fallback và HTTP load đã kiểm tra<br/>• Phát hiện warehouse chỉ có 10 ngày lịch, feature lag 28 tạo 0 dòng; không train/register model<br/>• FastAPI `/ready` 503 và healthcheck `unhealthy` đúng readiness contract | **LUỒNG DỮ LIỆU ĐẠT — MODEL CHỜ ĐỦ LỊCH SỬ** |

---

## 2. QUY CHUẨN GHI CHÉP BÁO CÁO (DOCUMENTATION STANDARD)

Mỗi báo cáo sửa đổi kỹ thuật trong thư mục này bắt buộc tuân thủ mẫu chuẩn gồm 5 phần:

1. **Tổng quan Đợt Cập nhật (Overview & Metadata)**: Mã phiên bản, ngày thực hiện, quy mô dữ liệu thử nghiệm, mục tiêu cải tiến.
2. **Nhật ký Chi tiết các Lỗi phát hiện & Cách khắc phục (Bug Fixes & Root Cause Analysis - RCA)**:
   - *Mã lỗi & Tên vấn đề*.
   - *Môi trường & Ngữ cảnh phát sinh*.
   - *Nguyên nhân gốc rễ (Root Cause Analysis)*: Phân tích kỹ thuật ở cấp độ thư viện và câu lệnh SQL/Python.
   - *Mã nguồn đã điều chỉnh (Code Diff & Solution)*: Dẫn chiếu chính xác tệp và dòng mã đã sửa.
3. **Danh mục Tính năng Mới & Cải tiến Tối ưu hóa (New Features & Optimizations)**:
   - Mô tả thuật toán / kiến trúc mới áp dụng.
   - Phân tích hiệu quả kỹ thuật (I/O, CPU, RAM, Latency).
4. **Bảng Đo lường So sánh Đối chứng Trước & Sau (Before vs. After Metrics)**:
   - So sánh định lượng các chỉ số Throughput, Latency, Data Loss, Memory Peak.
5. **Kế hoạch & Đề xuất Nâng cấp cho Đợt kế tiếp (Next Action Items)**.

---
*Thư mục được quản lý và bảo trì bởi Bộ phận Kỹ thuật Dữ liệu & MLOps.*
