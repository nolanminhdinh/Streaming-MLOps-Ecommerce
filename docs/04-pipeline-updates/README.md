# THƯ VIỆN BÁO CÁO CẬP NHẬT, KHẮC PHỤC LỖI & TỐI ƯU HÓA LUỒNG DỮ LIỆU (PIPELINE UPDATES & CHANGELOG)

> **Mục đích**: Thư mục này là trung tâm lưu trữ toàn diện lịch sử nâng cấp, phân tích nguyên nhân gốc rễ (Root Cause Analysis - RCA), các bản vá lỗi (Bug Fixes) qua từng đợt kiểm thử thực tế, cùng danh mục các tính năng và kỹ thuật tối ưu hóa luồng dữ liệu của hệ thống Streaming MLOps E-Commerce.

---

## 1. CƠ CẤU & MỤC LỤC TÀI LIỆU CẬP NHẬT THEO ĐỢT KIỂM THỬ

Mỗi đợt kiểm thử (Test Run) có sự thay đổi về mã nguồn hoặc tối ưu hóa luồng dữ liệu đều được biên soạn thành một tài liệu độc lập tương ứng:

| Đợt kiểm thử | Tệp báo cáo cập nhật & tối ưu | Quy mô thử nghiệm | Trọng tâm sửa đổi & Tối ưu hóa | Trạng thái |
| :---: | :--- | :---: | :--- | :---: |
| **Lần 15 — 17/11/2026** | [`lan-15-xung-dot-ma-don-va-khoi-phuc-2026-11-17.md`](lan-15-xung-dot-ma-don-va-khoi-phuc-2026-11-17.md) | 159 mã dùng chung → 358 đơn khôi phục từ kho thô; 261 kiểm thử + PG/mart | Mã đơn gắn mã lượt chạy producer; ETL cách ly xung đột danh tính `ORDER_IDENTITY_CONFLICT` (đơn đến trước giữ mã); khôi phục đơn bị gộp với mã `-HHMMSS`; Power BI readiness PASS 16/16 | **ĐÃ CHẠY VÀ KIỂM CHỨNG** |
| **Lần 14 — 16/11/2026** | [`lan-14-data-mart-star-schema-2026-11-16.md`](lan-14-data-mart-star-schema-2026-11-16.md) | 14 bảng mart; 157.901 dòng hàng khớp `public`; 254 + 54 + 8 kiểm thử | Star schema vật lý `mart` (9 bảng chiều, 5 bảng fact), khóa ngày yyyymmdd, lịch sử đề xuất theo ngày, giữ mọi phiên bản dự báo kèm thực tế để so sánh; làm mới gia tăng khoảng 2 giây | **ĐÃ CHẠY VÀ KIỂM CHỨNG — PBIP TIẾP THEO** |
| **Lần 13 — 16/11/2026** | [`lan-13-lop-du-lieu-bao-cao-quan-ly-2026-11-16.md`](lan-13-lop-du-lieu-bao-cao-quan-ly-2026-11-16.md) | 129.541 dòng đơn, 319 ngày xác nhận, 143 SKU dự báo; 251 + 54 kiểm thử | Lead time từ bản tin tồn kho; tự xác nhận coverage có kiểm tra khối lượng; huấn luyện lại trên `warehouse_live` (Naive v4, WAPE 58,22%); dự báo theo lô; view `bi` cho đề xuất đặt hàng ABC 98/95/90, đơn hàng sàn, độ mới dữ liệu; đặc tả Power BI | **ĐÃ CHẠY VÀ KIỂM CHỨNG — PBIX DO SINH VIÊN DỰNG** |
| **Lần 12 — 15–16/11/2026** | [`lan-12-chay-lai-du-lieu-va-gioi-han-luu-tru-2026-11-16.md`](lan-12-chay-lai-du-lieu-va-gioi-han-luu-tru-2026-11-16.md) | 2 lượt × 41 message cùng bộ dữ liệu; 240 kiểm thử | Chạy lại cùng dữ liệu đạt 20/20 (cùng SHA-256 nội dung fact); sửa API tồn kho trả số giả lập khi cache thiếu SKU mới và lỗi 403 demo web trên Windows; ghi nhận giới hạn MinIO tăng theo số lần nhận và hướng giải quyết | **ĐÃ CHẠY VÀ KIỂM CHỨNG — GIỚI HẠN LƯU TRỮ GIỮ LẠI CHO DEMO** |
| **Lần 8 — 31/10/2026** | [`lan-08-simulator-seasonality-2026-10-31.md`](lan-08-simulator-seasonality-2026-10-31.md) | 125.423 đơn mới, 304 ngày; 24 kiểm thử | Nhu cầu theo SKU, mùa vụ và nhiễu; nhập kho thích ứng; thay 106.131 đơn mô phỏng, bảo toàn 19.428 đơn cũ và 38 dòng tồn kho; kèm số liệu/SQL kiểm chứng | **ĐÃ CHẠY VÀ KIỂM CHỨNG POSTGRESQL — CHƯA ĐÁNH GIÁ LẠI MODEL** |
| **Lần 1** | [`lan-01-khac-phuc-loi-va-toi-uu-luong.md`](lan-01-khac-phuc-loi-va-toi-uu-luong.md) | 10,000 đơn hàng | • Khắc phục lỗi ép kiểu TIMESTAMP PostgreSQL (`NaN` handling)<br/>• Sửa lỗi seed Dimension tables & đồng bộ schema<br/>• **Tối ưu hóa Vectorized Validation tăng tốc 18 lần (49.6s → 2.7s)**<br/>• Xây dựng cơ chế DLQ 2 tầng & Engine Benchmark chuyên sâu | **ĐÃ HOÀN THÀNH** |
| **Lần 2** | [`lan-02-sua-loi-va-toi-uu-he-thong.md`](lan-02-sua-loi-va-toi-uu-he-thong.md) | Rà soát toàn hệ thống, 72 unit tests lịch sử | • Sửa 15 lỗi: `date_key` NULL, training–serving skew, artifact sai fold, vòng retrain giả, tồn kho giả lập, leakage ABC/XYZ, múi giờ...<br/>• Một phần serving/data path đã được chạy lại trong nghiệm thu Lần 3; model ML và retraining chưa đủ điều kiện xác nhận | **CÁC MẮT XÍCH ĐÃ CHẠY — MODEL CHƯA READY** |
| **Lần 3** | [`lan-03-e2e-remediation-2026-10-09.md`](lan-03-e2e-remediation-2026-10-09.md) | Rà soát điểm nối; chạy nghiệm thu runtime một phần | • Ingestion → MinIO/PostgreSQL và ETL đã chạy; Power BI export, Prometheus/Grafana, API fallback và HTTP load đã kiểm tra<br/>• Phát hiện warehouse chỉ có 10 ngày lịch, feature lag 28 tạo 0 dòng; không train/register model<br/>• FastAPI `/ready` 503 và healthcheck `unhealthy` đúng readiness contract | **LUỒNG DỮ LIỆU ĐẠT — MODEL CHỜ ĐỦ LỊCH SỬ** |
| **Lần 4** | [`lan-04-training-data-readiness-2026-10-10.md`](lan-04-training-data-readiness-2026-10-10.md) | Nạp hai CSV lịch sử; bổ sung data readiness gate và supervisor notification | • Dừng train/retrain trước model khi coverage walk-forward chưa đủ; lưu `INSUFFICIENT_DATA` và audit rõ số ngày cần nạp<br/>• `/training/status`, metric Prometheus, alert, Grafana; webhook gửi khi status thay đổi nếu đã cấu hình<br/>• Toàn bộ 76 pytest đạt; FastAPI runtime `/ready` 200, model v2 ở Staging | **READINESS ĐÃ CÓ — CHẤT LƯỢNG MODEL CÒN Ở STAGING** |

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
