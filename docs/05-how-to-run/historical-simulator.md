# Mô phỏng nhu cầu lịch sử theo SKU

`scripts/simulate_historical_data.py` dùng `data_simulator/history.py` và các cấu hình
trong `data_simulator/demand.py`. Các hệ số là giả định của một kịch bản mô phỏng,
chưa được ước lượng từ doanh số thực tế. Dữ liệu này phục vụ thử nghiệm pipeline,
phân tích mùa vụ và thử mô hình; không chứng minh độ chính xác ngoài thực tế.

## Cách sinh nhu cầu

- Mỗi SKU có hệ số mùa vụ theo tháng, xu hướng năm, độ biến động, mức phản ứng
  với sale và hiệu ứng cuối tuần riêng. Quạt/kem chống nắng có đỉnh mùa nóng;
  balo có đỉnh tựu trường; đồ trang trí khác đồ thiết yếu.
- Số đơn mỗi SKU/ngày được lấy mẫu Gamma–Poisson (negative binomial), cho phép
  phương sai lớn hơn trung bình và có ngày không bán được hàng.
- Có biến động chung của thị trường, biến động theo nhóm hàng, nhiễu có tương
  quan giữa các ngày và các đợt tăng nhu cầu ngắn của từng SKU.
- Sale có hiệu ứng trước, trong và sau chiến dịch; không buộc tháng nào cũng
  đạt cùng số đơn. `--orders-per-day` chỉ là quy mô nền, không phải chỉ tiêu.
- Sự kiện được sắp xếp theo thời gian trước khi cập nhật kho; hết hàng vẫn có
  thể làm đơn bị hủy. Nhu cầu tiềm ẩn và doanh số không hủy là hai đại lượng khác nhau.
- Chính sách nhập kho dùng nhu cầu quan sát của 14 ngày trước và quy luật mùa vụ
  đã cấu hình để điều chỉnh điểm đặt hàng/lượng nhập, không đọc trước số đơn sẽ
  phát sinh. Tồn đầu kỳ được đặt theo quy mô kịch bản; đột biến vẫn có thể gây thiếu kho.
- Shopee/TikTok có tỷ trọng khác nhau theo nhóm hàng. Mọi SKU dùng chung động
  cơ đơn hàng, phí và tồn kho của simulator hiện có.

## Chạy

```powershell
python scripts/simulate_historical_data.py --start-date 2026-01-01 --end-date 2026-10-31 --orders-per-day 300 --seed 20261031 --order-prefix SIM26YTD-
```

Ngày đầu và cuối tính theo `Asia/Ho_Chi_Minh`; ngày cuối được chặn tại thời điểm
hiện tại. Các bước vòng đời sau mốc chặn được loại bỏ và trạng thái được cập nhật
tương ứng. Cùng khoảng ngày và seed tái lập số đơn/SKU; UUID phục vụ ingestion
vẫn là định danh duy nhất.

Thêm `--load-db` để nạp dữ liệu. Lệnh này **không tự xóa đợt mô phỏng cũ**;
khi cần thay thế, phải xác định các dòng thuộc đúng đợt chạy, sao lưu và thực hiện
trong một giao dịch. Không xóa theo khoảng ngày vì có thể trùng dữ liệu cũ.

Kiểm thử không cần PostgreSQL:

```powershell
python -m unittest tests.test_historical_demand tests.test_data_simulator -q
```
