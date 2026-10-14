# Báo cáo kiểm thử và benchmark

[Checklist kiểm thử trước vận hành production](checklist-truoc-van-hanh-production.md): 10 nhóm kiểm tra vận hành/mở rộng, cách thực hiện, tiêu chí đạt và trạng thái hiện tại; không đánh giá độ chính xác mô hình.

[Báo cáo đầy đủ sửa lỗi và demo luồng ngày 14/10/2026](../04-pipeline-updates/bao-cao-tong-hop-fix-loi-va-demo-luong-2026-10-14.md): tổng hợp nguyên nhân, bản sửa, migration, kiểm thử, NiFi/API, hướng dẫn xem và việc còn lại.

| Ngày | Tài liệu | Kết quả chính |
|---|---|---|
| 2026-10-14 | [Sửa dòng hàng/quarantine và inventory](../04-pipeline-updates/lan-06-order-line-quarantine-2026-10-14.md) | Bài 1 đạt 12/12; 106 pytest đạt; đã migration/backfill/views; inventory 216 SKU ~0,11–0,19 giây, vẫn thiếu tồn kho thực. |
| 2026-10-13 | [Bài 1 — Toàn vẹn dữ liệu](data_integrity_DI20261013ba6be6.md) | FAIL: MinIO đủ 18 sự kiện; warehouse chỉ giữ 12/16 dòng hợp lệ, mất SKU của đơn nhiều dòng; hai dòng lỗi không vào quarantine. 7/10 tiêu chí đạt. |
| 2026-10-13 | [Kiểm tra runtime end-to-end](runtime_e2e_report_2026-10-13.md) | Migration đã áp dụng; 64 message/32 đơn, training/Registry/reload thật, NiFi 4 JSON; 97 pytest đạt. Inventory toàn catalog chậm và dùng tồn kho demo. |
| 2026-10-12 | [Báo cáo chỉnh sửa hệ thống](../04-pipeline-updates/bao-cao-chinh-sua-he-thong-2026-10-12.md) | 96 pytest đạt, 8 kiểm tra PostgreSQL cô lập đạt; chờ migration và nghiệm thu Docker. |
| 2026-10-10 | [Báo cáo kiểm thử hệ thống](test_report_2026-10-10.md) | 76 pytest passed; FastAPI Docker ready; alert readiness được Prometheus nạp; model v2 còn ở Staging. |
| 2026-10-09 | [Baseline và nghiệm thu runtime](test_report_2026-10-09.md) | Baseline cũ 71 passed, 1 failed; có ghi riêng nghiệm thu runtime sau các sửa đổi Lần 3. |
| 2026-10-01 | [Kiểm thử hiệu năng lần 1](bao_cao_kiem_thu_hieu_nang_lan_1.md) | Số đo lịch sử; không đại diện runtime hiện tại. |
| — | [Tổng hợp hiệu năng toàn hệ thống](bao_cao_tong_hop_hieu_nang_toan_he_thong.md) | Tài liệu tổng hợp lịch sử; cần đối chiếu với báo cáo kiểm thử mới nhất. |

Các kết luận về trạng thái hiện tại phải tham chiếu báo cáo có ngày mới nhất; không dùng số liệu lịch sử như nghiệm thu mới.
