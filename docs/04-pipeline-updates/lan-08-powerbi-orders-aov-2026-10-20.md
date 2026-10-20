# Khắc phục lần 8 — Số đơn hàng và AOV trong Power BI

Ngày thực hiện: **20/10/2026**. Phạm vi: SQL view, exporter CSV, công thức DAX tài liệu, regression SQL và đối soát dữ liệu runtime. Giữ nguyên các thay đổi phục hồi model của lần 7.

## 1. Nguyên nhân

`Fact_Orders` có hạt dòng hàng nhưng exporter dùng COUNT dòng cho `total_orders`, AVG tiền mỗi dòng cho AOV. View executive đếm distinct đơn, dẫn tới CSV/view không khớp. Join bắt buộc với shop còn bỏ các dòng chưa có shop_key, dù platform đã có trong fact. Query địa lý cũng đếm dòng dưới tên số đơn.

## 2. Quy tắc sau sửa

- Đơn được định danh theo `(platform, order_id)`, chỉ gồm dòng không hủy.
- Số tiền và quantity giữ tổng ở hạt dòng; không lấy DISTINCT trên tiền hoặc quantity vì các dòng hợp lệ có thể trùng giá trị.
- AOV = tổng `buyer_total_amount` / số đơn trên cùng phạm vi dữ liệu, làm tròn hai chữ số trong SQL.
- Platform lấy từ fact; không cần lookup shop thành công để tính KPI platform. Vẫn dùng dimension ngày, và đã xác nhận dữ liệu hiện có không thiếu date_key.
- CSV summary lấy số đơn, tiền, quantity và AOV từ cùng view executive. View bổ sung gross revenue/discounts ở cuối danh sách cột để không đổi thứ tự cột cũ khi cập nhật.
- Cả query địa lý CSV và view geographic dùng distinct business order key; query LEFT JOIN loại nhóm địa lý không có dòng bán, không đếm composite NULL thành đơn.
- DAX cho CSV dùng đúng bảng `Fact_Orders_Summary`; AOV được tính lại từ SUM tiền / SUM số đơn trong filter context. Không dùng AVERAGE của cột aov giữa các nhóm có quy mô khác nhau.
- Bộ measure raw facts được tách riêng với cùng điều kiện không hủy, tránh trộn mẫu số đơn hợp lệ với tiền của cả đơn hủy.

Tổng số đơn ở mart ngày/platform có thể cộng khi các dòng cùng đơn có cùng ngày tạo nghiệp vụ. Nếu nguồn vi phạm hợp đồng này cần xử lý ở tầng dữ liệu; không tuyên bố aggregate distinct luôn cộng được qua mọi loại nhóm hoặc filter SKU.

## 3. Kiểm thử SQL với PostgreSQL thật

Thêm `tests/test_powerbi_sql_metrics.py`, chạy khi `RUN_POWERBI_SQL_TESTS=1`. Các bảng/view fixture được tạo trong session tạm rồi rollback; không ghi vào facts, dimensions, view public hoặc sequence dữ liệu thật.

Fixture gồm đơn nhiều dòng, cùng ID trên hai sàn, dòng thiếu shop, đơn hủy, địa lý không có giao dịch và nhóm ngày có giá trị đơn khác nhau. Kỳ vọng:

| Chỉ tiêu fixture | Kết quả |
|---|---:|
| Dòng hợp lệ | 6 |
| Đơn distinct | 4 |
| Quantity | 7 |
| Tổng tiền | 450 |
| AOV toàn bộ | 112,50 |
| AOV từng nhóm | 177,50 / 45,00 / 50,00 |

AOV toàn bộ khác trung bình không trọng số của ba AOV nhóm. Cả bốn test SQL đạt; exporter và view trả cùng số đơn/tiền/AOV trên các khóa ngày/platform.

## 4. Cập nhật runtime và đối soát

Đã sao lưu CSV cũ và định nghĩa view vào `scratch/powerbi-before-order-fix-2026-10-20/`, sau đó chạy exporter chuẩn để cập nhật view và làm mới `powerbi/data/`. Fact_Orders trước/sau vẫn **19.269 dòng**, không sửa facts hoặc coverage.

| Chỉ tiêu | Trước sửa theo CSV | Sau sửa / nguồn warehouse không hủy |
|---|---:|---:|
| Tổng orders | 11.628, thực chất là dòng sau join shop | **11.646 đơn distinct** |
| Quantity | 24.337 | **24.392** |
| Tổng buyer_total_amount | 7.597.764.230 | **7.609.968.840** |
| AOV | Trung bình tiền theo dòng ở mỗi nhóm | **653.440,57** toàn bộ, tính lại từ tổng tiền/tổng đơn |
| Nhóm CSV/view khác số đơn | 4 | **0** |
| Nhóm CSV AOV khác tỷ số theo đơn | Chưa đúng công thức | **0** |

Hai thay đổi tác động ngược chiều đến tổng số đơn: bỏ 14 lượt đếm thừa do dòng hàng; đồng thời đưa 32 đơn fixture thiếu shop trở lại phạm vi. Bởi vậy tổng sau sửa tăng từ 11.628 lên 11.646. Nếu chỉ giữ scope join shop cũ, số đơn đúng là 11.614; phạm vi mới có thêm 32 đơn và 55 units, 12.204.610 tiền từ dữ liệu fixture.

Đã đối soát số đơn, units và tiền với SELECT trực tiếp từ Fact_Orders không hủy; tất cả khớp. Bộ bảy CSV kiểm chứng riêng khớp nội dung `powerbi/data/` mới. Các con số tiền vẫn là dữ liệu mô phỏng/kiểm thử, không phải doanh thu thị trường đã xác minh.

## 5. Regression và bằng chứng

Toàn bộ suite chạy cả SQL integration, seed 20261019: **114 passed, 30 warnings trong 5,22 giây**. Warning thuộc Starlette/httpx và PyArrow/pandas. Không test nào bị skip trong lượt này.

- `docs/07-bao-cao-do-an/evidence/powerbi_2026-10-20.json`: đối soát độc lập, số nhóm chênh lệch và AOV toàn bộ.
- `docs/07-bao-cao-do-an/evidence/powerbi_export_2026-10-20/`: snapshot CSV kiểm chứng, bị Git ignore theo chính sách hiện có.
- `docs/07-bao-cao-do-an/evidence/pytest_powerbi_order_fix_2026-10-20.xml`: JUnit toàn bộ 114 test.
- `scripts/check_powerbi_readonly.py`: thay kết luận hardcode bằng kiểm tra số đơn/tiền/units với nguồn và AOV thực tế.

## 6. Giới hạn và thao tác trong Power BI Desktop

SQL view, CSV chính và DAX tài liệu đã cập nhật. Lượt này chưa mở/refresh Power BI Desktop, chưa chạy DAX engine hoặc sửa measure nhúng trong DataModel PBIX. PBIX hiện có một trang inventory, không có visual số đơn/AOV đã được xác nhận trong audit trước. Sau khi mở PBIX, cần Refresh để nạp CSV mới và dùng bộ measure đúng đường import khi bổ sung visual kinh doanh.

Không thay đổi các vấn đề khác như thiếu mapping SKU lịch sử, coverage actual, độ mới tồn kho hoặc thiết kế ba trang. Các mốc audit 19/10 được giữ nguyên làm bằng chứng trước sửa.

## 7. Minh bạch sử dụng AI

AI hỗ trợ sửa SQL/exporter và hướng dẫn DAX, xây test fixture session-local, cập nhật view/CSV theo yêu cầu, đối soát nguồn và lưu bằng chứng. Không chỉnh dữ liệu giao dịch hoặc tự xác nhận coverage.
