"""Update the thesis content while preserving the existing Word template/images.

Run after evaluate_baseline_remediation.py. Historical evidence remains untouched.
Creates a dated Word revision and updates the editable Markdown source.
"""
from pathlib import Path
from datetime import date
import json
import re
import shutil
import xml.etree.ElementTree as ET

from docx import Document
from docx.shared import Cm, Pt
from docx.oxml import OxmlElement
from docx.oxml.ns import qn

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "output/docx"
EVIDENCE = ROOT / "docs/07-bao-cao-do-an/evidence/remediation-2026-11-07"
R = json.loads((EVIDENCE / "baseline-evaluation.json").read_text(encoding="utf-8"))
EDA = json.loads((EVIDENCE / "historical-eda.json").read_text(encoding="utf-8"))
TEST_XML = ROOT / "docs/07-bao-cao-do-an/evidence/pytest_baseline_remediation_2026-11-07.xml"
SUITE = ET.parse(TEST_XML).getroot().find("testsuite")
TESTS, SKIPPED = int(SUITE.get("tests")), int(SUITE.get("skipped"))
PASSED = TESTS - SKIPPED - int(SUITE.get("failures")) - int(SUITE.get("errors"))
assert int(SUITE.get("failures")) == int(SUITE.get("errors")) == 0


def vi(value, places=2):
    return f"{value:.{places}f}".replace(".", ",")


source_note = ("Theo xác nhận của sinh viên ngày 07/11/2026, hai CSV lịch sử được nhận từ cộng đồng Xóm Data "
               "trên Facebook và đã được quản trị viên cho phép sử dụng; liên kết gốc hiện đã bị gỡ. "
               "Đây là thông tin do sinh viên khai báo, chưa có bản sao trao đổi cấp phép trong bộ hồ sơ này. "
               "Chưa xác nhận phạm vi tái phân phối, thời hạn hoặc điều kiện cấp phép. Fingerprint của hai tệp "
               "được lưu để truy vết; không dùng fingerprint thay cho bằng chứng quyền sử dụng.")

replacements = {
"Lấy cột lag1 nếu tồn tại": "Mới: y[t]; artifact cũ: ưu tiên lag_1",
"Lấy cột lag7": "Mới: lag_6 cho target t+1; cũ: lag_7",
"Trung bình trượt 7 ngày đã dịch": "Mới: mean(y[t−6..t]); cũ: mean(y[t−7..t−1])",
"Tên estimator chưa đủ để giải thích đầu ra. Dòng t dự báo ngày t+1 nhưng lag1 trong mã bằng y(t−1), lag7 bằng y(t−7); trung bình trượt cũng bỏ ngày t do chuỗi đã dịch. Khi tùy biến mô hình, các quy ước này phải nhất quán với target và bước tạo đặc trưng. Em giữ đúng mô tả cách tính để người tiếp quản có thể kiểm tra và điều chỉnh có chủ đích, không sử dụng các baseline này làm thước đo nghiệm thu độ chính xác. [R1], [R2]":
"Bản khắc phục ngày 07/11 căn chỉnh baseline theo origin ngày t cho target t+1. Bộ đặc trưng mới có lag_6; Naive dùng daily_demand, Seasonal dùng lag_6, Moving Average điều chỉnh rolling_mean_7 để bao gồm ngày t. Artifact cũ giữ cách tính trước sửa qua cơ chế version. Các baseline mới được so sánh theo pooled WAPE và holdout ở mục 3.11; không gán kết quả mới cho model v2 đang phục vụ. [R1, R5]",
"Lớp: [Bổ sung lớp]. Hệ đào tạo: [Bổ sung hệ đào tạo].": "Lớp: DCCTKH67A. Hệ đào tạo: Đại học.",
"Điện thoại: [Bổ sung số điện thoại]. Email: [Bổ sung email].": "Điện thoại và email: chưa được sinh viên cung cấp trong bản cập nhật.",
"Thời gian thực hiện: năm 2026. Các kết quả thực nghiệm chính trong báo cáo thuộc những lần thử từ ngày 09 đến ngày 24 tháng 10 năm 2026. Mục tiêu và phạm vi được cập nhật ngày 29/10/2026; việc chỉnh báo cáo không tạo thêm một lần thực nghiệm.":
"Thời gian thực hiện: năm 2026. Bản cập nhật ngày 07/11/2026 hợp nhất hồ sơ tháng 10, thay đổi kho dữ liệu ngày 01/11, kiểm thử dịch vụ ngày 04/11 và đánh giá baseline offline ngày 07/11. Mỗi kết quả giữ ngày, môi trường và phạm vi riêng; không xem chỉnh văn bản là nghiệm thu lại runtime.",
"Đồ án không đặt mục tiêu nghiên cứu thuật toán mới, tối ưu hay đánh giá độ chính xác dự báo. Các giá trị sai số có thể được mã huấn luyện ghi lại như thông tin của lần chạy, nhưng không được dùng làm tiêu chí nghiệm thu trong phạm vi này.":
"Đồ án tập trung kiến trúc dữ liệu và MLOps, không đề xuất thuật toán mới. Để đối chiếu cẩm nang, phần thực nghiệm bổ sung đánh giá dự báo baseline ngoài mẫu, WAPE gộp, mức bao phủ và holdout cuối. Các kết quả sai số được dùng để đánh giá giới hạn mô hình, tách khỏi tiêu chí tích hợp hệ thống.",
"nhưng việc so sánh chất lượng các thuật toán không thuộc nội dung nghiệm thu.": "và ba baseline được đánh giá lại theo cùng protocol ngày 07/11; nhánh học sâu chưa được nghiệm thu chất lượng.",
"không sử dụng nó để xếp hạng độ chính xác mô hình.": "sử dụng các fold phát triển để chọn baseline và giữ 14 ngày cuối làm tập đánh giá riêng.",
"Trong bản cài đặt đang dùng, NaiveModel ưu tiên lag_1 và SeasonalNaiveModel lấy lag_7, trong khi target ở t+1. Đây là quy ước thực tế của mã, cần được ghi trong hợp đồng đầu vào khi thay mô hình hoặc điều chỉnh khoảng dự báo. Tên lớp mô hình không thay thế cho việc kiểm tra cách nó sử dụng đặc trưng. [R1]":
"Đợt sửa ngày 07/11 thống nhất dòng đặc trưng ở ngày t dự báo ngày t+1. Baseline mới dùng Naive=y[t], Seasonal Naive=y[t−6] và Moving Average=mean(y[t−6..t]). lag_6 được bổ sung vào hợp đồng đặc trưng mới. Artifact cũ không có alignment_version vẫn giữ hành vi lịch sử; model v2 đang triển khai không tự thay đổi thuật toán hoặc metric. [R1, R5]",
"Mã huấn luyện còn có khả năng tính và ghi các chỉ số MAE, RMSE, WAPE và bias. Trong phạm vi đồ án, các trường này chỉ được xem là metadata sẵn có của lần chạy. Báo cáo không dùng chúng để đánh giá độ chính xác, lựa chọn thuật toán tốt nhất hoặc xác nhận hiệu quả nghiệp vụ. Khi tùy biến cho doanh nghiệp, bộ chỉ số và điều kiện phê duyệt mô hình cần được xác định theo nhu cầu sử dụng cụ thể.":
"Các chỉ số chính gồm MAE, RMSE, WAPE gộp và bias. WAPE=100×Σ|y−ŷ|/Σy; bias=100×Σ(ŷ−y)/Σy. Khi Σy=0, hai tỷ số không xác định và được xuất JSON null, không ghi 0 để diễn giải hoàn hảo. WAPE gộp tính trên tất cả cặp được chấm; trung bình WAPE các fold có mẫu số dương chỉ là chỉ số chẩn đoán. MAPE hiện dùng mẫu số y+1 nên được ghi rõ là MAPE làm mượt. Kết quả mô hình và tính thông suốt hệ thống là hai nội dung đánh giá riêng. [R5]",
"Kết quả kiểm tra tập trung vào sự nhất quán của dữ liệu và khả năng nối các công đoạn, không đánh giá độ chính xác dự báo.":
"Kết quả gồm tính nhất quán và liên kết các công đoạn; phần bổ sung 07/11 đánh giá riêng ba baseline và holdout cuối, với giới hạn coverage được giữ rõ.",
"Hệ thống được triển khai trên Docker Desktop với Kafka, MinIO, PostgreSQL, NiFi và các dịch vụ MLOps.":
"Luồng chính hiện hành dùng Kafka, MinIO, PostgreSQL, worker và các dịch vụ MLOps trên Docker Desktop. Từ 01/11, NiFi được tách khỏi Compose mặc định và chuyển thành hướng mở rộng.",
"NiFi đọc topic đơn bằng consumer group độc lập, phục vụ quan sát và lưu JSON.":
"NiFi là nhánh tùy chọn dùng consumer group riêng; từ 01/11 không thuộc tuyến chạy và nghiệm thu mặc định.",
"Mỗi chu kỳ mặc định nhận tối đa 100 object, thực hiện ETL, xuất dữ liệu BI rồi mới lưu tiến độ bằng tệp tạm và thay thế. Nếu ETL hoặc export lỗi, checkpoint chưa tiến nên mẻ sẽ được đọc lại.":
"Mỗi chu kỳ mặc định nhận tối đa 100 object, thực hiện ETL rồi lưu checkpoint bằng tệp tạm và thay thế. Từ 01/11, CSV là tác vụ dự phòng riêng; lỗi export không chặn checkpoint của worker. Nếu ETL lỗi, checkpoint chưa tiến nên object sẽ được đọc lại.",
"Độ chính xác dự báo không được sử dụng làm tiêu chí đánh giá đồ án.":
"Phần bổ sung 07/11 đánh giá baseline ngoài mẫu; hiệu quả nhập hàng thực tế chưa được xác nhận.",
"Đầu ra dự báo và cảnh báo tồn kho là điểm cuối để kiểm tra luồng MLOps, không phải đối tượng so sánh độ chính xác hay tối ưu thuật toán. MAE, RMSE và WAPE không được dùng làm tiêu chí nghiệm thu; các chỉ số sẵn có chỉ là metadata của run.":
"Đầu ra dự báo và cảnh báo tồn kho được kiểm tra về tích hợp; đánh giá baseline ngoài mẫu được bổ sung độc lập. WAPE gộp, MAE, RMSE và bias được dùng để phân tích chất lượng và giới hạn dự báo; không suy ra hiệu quả nhập hàng chỉ từ HTTP thành công.",
"Môi trường thực nghiệm sử dụng Docker Desktop để triển khai Kafka, ZooKeeper, NiFi, MinIO, PostgreSQL, MLflow, FastAPI, Prometheus, Grafana và pipeline worker.":
"Các phép thử tháng 10 có nhánh NiFi. Luồng mặc định từ 01/11 dùng Kafka, ZooKeeper, MinIO, PostgreSQL, MLflow, FastAPI, Prometheus, Grafana và pipeline worker; NiFi là tùy chọn. Đánh giá baseline 07/11 chạy offline trên snapshot Parquet, không khởi động lại Docker.",
"MAE, RMSE hoặc WAPE trong manifest và MLflow được giữ tại nơi lưu để truy vết, nhưng không đưa vào bảng so sánh hoặc kết luận đạt, chưa đạt của đồ án.":
"Metric trong manifest v2 vẫn được giữ nguyên để truy vết lịch sử. Bảng trước–sau và holdout của baseline mới nằm tại phần bổ sung 07/11; không gán metric mới cho model v2 đang triển khai.",
"không dùng sai số của các vùng làm thước đo nghiệm thu.": "đánh giá sai số của các vùng bằng cùng protocol; không dùng holdout cuối để chọn lại mô hình.",
"Những điểm này định hướng mở rộng hạ tầng, không chuyển báo cáo thành nghiên cứu so sánh độ chính xác.":
"Những điểm này là giới hạn của triển khai; phần baseline bổ sung cung cấp đánh giá dự báo ngoài mẫu trong phạm vi đã công bố.",
"Đồ án không đặt mục tiêu đánh giá hay tối ưu độ chính xác; vì vậy em đánh giá kết quả theo khả năng chạy xuyên suốt, truy vết đầu vào và đầu ra, xử lý tình huống lỗi và cấu hình lại từng thành phần.":
"Đồ án đánh giá khả năng chạy xuyên suốt, truy vết, xử lý lỗi và cấu hình hệ thống, đồng thời bổ sung so sánh baseline ngoài mẫu. Kết quả holdout chưa chứng minh model đủ tốt cho quyết định nhập hàng. [R5]",
"Phần đối chiếu được thực hiện trong phạm vi mẫu đã có; để đánh giá các ngưỡng yêu cầu cụ thể, em cần bổ sung cẩm nang chính thức của ngành. [R4]":
"Phần đối chiếu đã bổ sung cẩm nang 25 trang do sinh viên cung cấp, trọng tâm trang 9–11 và 23–25; các điều kiện chưa đủ minh chứng được giữ ở phụ lục, không tự xác nhận đạt toàn bộ. [R4, R6]",
"Việc chúng có vượt yêu cầu tối thiểu hay không vẫn cần được đối chiếu với cẩm nang chính thức, thay vì tự đánh giá chỉ từ số lượng chức năng.":
"Mức đáp ứng được đối chiếu theo từng tiêu chí cẩm nang; số lượng chức năng không thay thế đánh giá hiệu năng, dữ liệu, minh chứng AI và năng lực giải thích.",
"nguồn xuất bản, giấy phép và tính đại diện của dữ liệu vẫn cần được bổ sung.":
"nguồn và việc xin phép đã được sinh viên khai báo ngày 07/11; bản sao cấp phép, phạm vi tái phân phối và tính đại diện vẫn cần được bổ sung.",
"Nhãn kỹ thuật; nguồn xuất bản và giấy phép chưa xác minh":
"Sinh viên xác nhận nguồn Xóm Data/Facebook và đã xin phép admin; link gốc đã gỡ; chưa có bản sao cấp phép trong hồ sơ",
"Chỉ dùng để truy vết trong đồ án; không dùng nghiệm thu độ chính xác":
"Metric v2 giữ nguyên lịch sử; baseline mới có bảng đánh giá riêng ngày 07/11",
"Có đầu vào, đầu ra và metadata liên kết; không đánh giá độ chính xác":
"Có liên kết dữ liệu–model; đã bổ sung đánh giá baseline và holdout riêng ngày 07/11",
"Chưa có rubric/cẩm nang ngành để đối chiếu từng ngưỡng":
"Đã đọc cẩm nang 25 trang; còn đối chứng hiệu năng, coverage và minh chứng cấp phép/AI cần hoàn thiện",
"Chưa đủ thông tin để kết luận": "Chưa đủ minh chứng để xác nhận đạt toàn bộ điều kiện",
"chưa thống kê tỷ lệ chỉnh sửa": "mức chỉnh sửa được mô tả định tính theo khai báo sinh viên, không ước lượng phần trăm",
}

ai_rows = [
    ["Công cụ", "Mục đích", "Phần việc có sử dụng", "Mức chỉnh sửa và kiểm chứng"],
    ["Codex, Antigrafity, Claude", "Sinh mã, giải thích và điều chỉnh nội dung", "Các đoạn mã của hệ thống; chưa có log phân định công cụ cho từng đoạn", "Theo sinh viên: đọc và kiểm tra mã được sinh, yêu cầu AI kê khai và Q&A để điều chỉnh cho phù hợp. Chưa có thống kê hoặc ảnh đối chiếu từng lần sửa."],
    ["Codex, ngày 07/11", "Khắc phục lỗi và kiểm tra hồi quy", "Baseline theo ngày mục tiêu; WAPE/bias null; protocol đánh giá; giữ tương thích model cũ", f"AI sửa mã và chạy {PASSED} test đạt, {SKIPPED} test tích hợp bỏ qua; sinh viên cần tự chạy và giải thích trước bảo vệ. Không tự nhận đây là mã sinh viên viết tay."],
    ["Codex, ngày 07/11", "Tổng hợp bằng chứng và cập nhật báo cáo", "Đánh giá offline, bảng trước–sau/holdout, sơ đồ, EDA, ma trận cẩm nang và định dạng", "AI tổng hợp và soạn cập nhật; sinh viên xác nhận nguồn, lớp và đóng góp trong trao đổi. Bản nộp cuối cần sinh viên duyệt; ảnh quá trình AI chưa nằm trong hồ sơ."],
]

selected = R["holdout"]["selected_using_development_only"]
holdout = R["holdout"]["scores"][selected]
same = R["same_original_folds_before_after"]
comparison_rows = [["Baseline", "WAPE gộp trước sửa (%)", "WAPE gộp sau sửa (%)"]]
for label, old, new in [("Naive", "naive_legacy", "naive_v2"), ("Seasonal Naive", "seasonal_naive_legacy", "seasonal_naive_v2"), ("Moving Average", "moving_average_legacy", "moving_average_v2")]:
    comparison_rows.append([label, vi(same[old]["pooled_metrics"]["wape"]), vi(same[new]["pooled_metrics"]["wape"])])
comparison_rows.append(["Dự báo 0 (chẩn đoán)", "100,00", "100,00"])

rubric_rows = [
    ["Yêu cầu cẩm nang", "Nội dung và bằng chứng", "Trạng thái còn lại"],
    ["Bài toán, input/output và mục tiêu", "SKU/ngày; các tầng nguồn–Lake–Warehouse–model–API; mục tiêu toàn vẹn", "Cần chốt ngưỡng thông lượng và độ trễ đầu cuối"],
    ["Nguồn, quy mô và hợp lệ dữ liệu", "Nguồn Xóm Data theo khai báo; fingerprint; 7.000 dòng nguồn; thẻ dữ liệu", "Cần bản sao cấp phép, phạm vi quyền và xác nhận coverage"],
    ["EDA và tiền xử lý có diễn giải", "EDA thực đo trên snapshot ngày; JSON thống kê và biểu đồ trong bộ 07/11", "Chưa thay cho audit toàn bộ CSV thô trước/sau làm sạch"],
    ["Xây dựng và đánh giá mô hình", "Baseline căn chỉnh t+1; WAPE gộp; giữ holdout cuối; so sánh cùng cặp SKU/ngày", "Kết quả kém mốc zero; chưa xác nhận lợi ích nghiệp vụ"],
    ["Kiến trúc và đánh giá hệ thống", "Migration 01/11; đối soát 600 đơn; 2.400 HTTP requests; phục hồi 04/11", "Thiếu so sánh cấu hình có kiểm soát và P95 đầu cuối"],
    ["Báo cáo, AI và làm chủ nội dung", "Bảng công cụ/mục đích/phần việc/mức sửa; lớp DCCTKH67A; nguồn bằng chứng", "Cần ảnh AI thật, sinh viên duyệt và trả lời trực tiếp; kiểm tra mẫu/slide và xác nhận GVHD/GVPB"],
]

runtime_rows = [
    ["Mốc và phép kiểm tra", "Kết quả", "Giới hạn"],
    ["01/11 – migration warehouse", "144.851 dòng đơn; 6.098 snapshot; 13 forecast; 7.000 lịch sử được bảo toàn", "28 mã đơn xung đột header; 24 snapshot thiếu event time; coverage chưa xác nhận"],
    ["04/11 – tải hỗn hợp 10 phút", "600 ACK = 600 fact; không thiếu/thừa/trùng; 2.400/2.400 HTTP đạt; P95 42,201 ms", "1 đơn/giây, 4 HTTP/giây; có thể hưởng cache; chưa suy ra giới hạn chịu tải"],
    ["04/11 – phục hồi và tái huấn luyện", "Ngắt/khôi phục service; reload lỗi giữ model; rollback; retrain ép tay trong Registry cô lập", "Chưa xác nhận drift thực tế hoặc scheduler vận hành dài hạn"],
    ["07/11 – hồi quy mã sau sửa", f"{PASSED} test đạt, {SKIPPED} test tích hợp bỏ qua; không failure/error", "Không phải nghiệm thu lại Docker hoặc PostgreSQL integration"],
    ["07/11 – baseline offline", f"Moving Average mới: WAPE gộp {vi(same['moving_average_v2']['pooled_metrics']['wape'])}%; holdout {vi(holdout['pooled_metrics']['wape'])}%", "Không train/register/reload, SHA-256 artifact triển khai trước/sau không đổi"],
]


def table_md(rows):
    return "\n".join(["| " + " | ".join(rows[0]) + " |", "|" + "---|" * len(rows[0])] +
                     ["| " + " | ".join(row) + " |" for row in rows[1:]])


blocks = [
    ("h2", "3.11 Cập nhật thực nghiệm và đối chiếu cẩm nang ngày 07 tháng 11 năm 2026"),
    ("p", "Các kết quả ở mục 3.2–3.10 giữ mốc lịch sử của từng lần đo. Phần này hợp nhất bằng chứng mới mà không sửa ngược log gốc, đồng thời công bố protocol đánh giá sau khắc phục baseline và WAPE. [R5]"),
    ("caption", "Bảng 3-10 Bằng chứng bổ sung theo ngày và phạm vi kiểm tra"),
    ("table", runtime_rows),
    ("h3", "3.11.1 Căn chỉnh baseline và xử lý metric không xác định"),
    ("p", "Dòng ngày t dự báo t+1: Naive dùng y[t]; Seasonal Naive dùng y[t−6]; Moving Average dùng trung bình y[t−6..t]. Kiểm tra chuỗi 1..41 tại origin 10/02 cho ngày 11/02 thu được lần lượt 41, 35 và 38. Mã cũ cho 40, 34 và 37. Model mới lưu alignment_version=2; model cũ thiếu trường này giữ hành vi lịch sử, tránh thay đổi ngầm artifact v2 đang phục vụ."),
    ("p", "Khi tổng actual bằng 0, WAPE và bias xuất null. Ví dụ actual=[0,0], prediction=[4,4] có MAE=4, WAPE=null và bias=null. Metric chính là WAPE gộp mọi cặp được chấm; không lấy trung bình các fold làm điểm chính và không biến fold không xác định thành 0. Bước đăng ký chỉ xét run có protocol origin_t_plus_1_pooled_wape_v2, metric hữu hạn và nguồn phù hợp."),
    ("h3", "3.11.2 Kết quả trước sau và holdout cuối"),
    ("p", "Thí nghiệm offline đọc daily_history và feature snapshot đang lưu, bổ sung lag_6 trong bộ nhớ, giữ cửa sổ lịch sử 240 ngày. Ba test window gốc là 01–14/07, 15–28/07 và 29/07–11/08/2026. Tất cả baseline được chấm trên cùng 8.484/9.072 cặp, tương ứng 93,52%; SKU không đủ lịch sử bị loại theo hợp đồng phục vụ. Các tỷ lệ không được hiểu là coverage nguồn đã được chứng nhận."),
    ("caption", "Bảng 3-11 WAPE gộp của baseline trên cùng các cửa sổ trước và sau sửa"),
    ("table", comparison_rows),
    ("image", ("baseline-wape.png", "Hình 3-3 WAPE gộp trên cùng ba cửa sổ kiểm tra trước và sau sửa")),
    ("p", f"Để tách đánh giá cuối khỏi chọn mô hình, giữ riêng 29/07–11/08/2026. Ba fold phát triển nằm trong 17/06–28/07. Chọn Moving Average theo WAPE gộp phát triển {vi(R['development'][selected]['pooled_metrics']['wape'])}%; chỉ sau đó chấm holdout với WAPE {vi(holdout['pooled_metrics']['wape'])}%, MAE {vi(holdout['pooled_metrics']['mae'],4)}, RMSE {vi(holdout['pooled_metrics']['rmse'],4)} trên {holdout['scored_rows']}/{holdout['expected_rows']} cặp. Không chọn lại mô hình từ holdout; kết quả của mô hình khác chỉ là chẩn đoán. Baseline không dùng biến phân nhóm ABC/XYZ nên không sử dụng profile fit từ thời gian sau các origin phát triển."),
    ("p", "Moving Average sau sửa cải thiện trên các fold gốc, nhưng WAPE holdout vẫn cao hơn mốc dự báo toàn 0 là 100%. Vì vậy, sửa đúng công thức chưa đồng nghĩa model đáp ứng nhập hàng. Cần xác nhận dữ liệu ngày trống, đánh giá nhu cầu thưa và bổ sung dữ liệu trước khi kết luận hiệu quả. Zero chỉ là mốc chẩn đoán, không phải đề xuất chính sách nhập hàng."),
    ("h3", "3.11.3 EDA trên snapshot lịch sử và hồ sơ nguồn"),
    ("p", f"Snapshot gồm 49.032 ô SKU/ngày, 216 SKU, giai đoạn 28/12/2025–11/08/2026. Tỷ lệ demand=0 là {vi(EDA['zero_fraction']*100)}%; mean={vi(EDA['daily_demand_description']['mean'],4)}, median={vi(EDA['daily_demand_median'],2)}, std={vi(EDA['daily_demand_description']['std'],4)}, min={vi(EDA['daily_demand_description']['min'],0)}, max={vi(EDA['daily_demand_description']['max'],0)}. Không có khóa SKU/ngày trùng trong snapshot. Các con số mô tả dữ liệu đã tổng hợp, không phải 49.032 giao dịch gốc hoặc bằng chứng mọi ngày đã đủ thu thập."),
    ("image", ("historical-eda.png", "Hình 3-4 Tổng lượng bán quan sát theo ngày trong snapshot lịch sử")),
    ("p", source_note),
    ("h3", "3.11.4 Mức đáp ứng cẩm nang và điều kiện chưa đóng"),
    ("caption", "Bảng 3-12 Ma trận đáp ứng cẩm nang và điều kiện còn mở"),
    ("table", rubric_rows),
    ("p", "Cẩm nang cho phép nội dung tương đương. Đồ án lấy kiến trúc Data Lake/Data Warehouse làm trọng tâm và bổ sung đánh giá chuỗi thời gian. Không tự xác nhận đạt toàn bộ chỉ từ số test hoặc số công nghệ. Các điều kiện còn mở phải được đóng bằng bằng chứng thật và kiểm tra trực tiếp của GVHD/GVPB. [R6]"),
]

appendix_blocks = [
    ("h2", "Phụ lục E Hồ sơ cập nhật và đóng góp của sinh viên"),
    ("p", "Theo khai báo ngày 07/11/2026, sinh viên trực tiếp xây dựng ý tưởng; lựa chọn công cụ và quy trình hệ thống; lựa chọn mô hình sao; xây dựng báo cáo Power BI và data dictionary. Với mã AI sinh, sinh viên đọc kiểm tra, yêu cầu kê khai nội dung và trao đổi Q&A để điều chỉnh phù hợp. Khai báo này không tự xác nhận toàn bộ dashboard đã nghiệm thu hoặc mọi dòng mã đã được kiểm chứng độc lập."),
    ("p", "Các công cụ được sinh viên khai báo gồm Codex, Antigrafity và Claude; tên Antigrafity được giữ theo thông tin cung cấp. Chưa có log phân định công cụ cho từng đoạn mã. Ảnh quá trình dùng AI và trao đổi cấp phép dữ liệu chưa được đưa vào bộ hồ sơ. Cần bổ sung ảnh gốc còn lưu và chú thích trung thực; không dựng lại bằng chứng giả."),
    ("p", "Bộ bằng chứng mới: docs/07-bao-cao-do-an/evidence/remediation-2026-11-07/baseline-evaluation.json, baseline-comparison.csv, historical-eda.json và hai biểu đồ; JUnit pytest_baseline_remediation_2026-11-07.xml. SHA-256 trong JSON xác nhận không đổi feature snapshot, feature_spec hoặc manifest triển khai trong phép đánh giá. Notebook historical_eda_verified.ipynb lưu output chạy trên snapshot, tách khỏi notebook mẫu mô phỏng."),
    ("p", "Cần hoàn thiện trước bản nộp: hồ sơ cấp phép và phạm vi quyền; xác nhận coverage từ nguồn; ảnh quá trình AI; nghiệm thu Power BI Desktop; đối chứng hai cấu hình hệ thống cùng môi trường; ngưỡng thông lượng/độ trễ; kiểm tra mẫu báo cáo và slide; xác nhận GVHD/GVPB và khả năng trả lời trực tiếp. Các điều kiện này không được đánh dấu đạt chỉ vì báo cáo đã bổ sung mô tả."),
]


def markdown_blocks(items):
    result = []
    for kind, value in items:
        if kind == "h2": result.append("## " + value)
        elif kind == "h3": result.append("### " + value)
        elif kind in ["p", "caption"]: result.append(value)
        elif kind == "table": result.append(table_md(value))
        elif kind == "image": result.append(f"![{value[1]}](../../docs/07-bao-cao-do-an/evidence/remediation-2026-11-07/{value[0]})")
    return "\n\n".join(result)


def replace_text(text):
    for old, new in replacements.items():
        text = text.replace(old, new)
    return text


def configure_table(table):
    table.autofit = False
    for i, row in enumerate(table.rows):
        for cell in row.cells:
            for p in cell.paragraphs:
                p.paragraph_format.first_line_indent = Cm(0)
                p.paragraph_format.line_spacing = 1.05
                p.paragraph_format.space_after = Pt(3)
                for run in p.runs:
                    run.font.name = "Times New Roman"; run.font.size = Pt(10.5)
                    run.font.bold = i == 0
        if i == 0:
            repeat = OxmlElement("w:tblHeader"); row._tr.get_or_add_trPr().append(repeat)


def add_table(doc, rows, anchor=None):
    t = doc.add_table(rows=1, cols=len(rows[0]))
    t.style = doc.tables[0].style
    borders = OxmlElement("w:tblBorders")
    for edge in ["top", "left", "bottom", "right", "insideH", "insideV"]:
        border = OxmlElement("w:" + edge)
        border.set(qn("w:val"), "single"); border.set(qn("w:sz"), "4")
        border.set(qn("w:color"), "000000"); borders.append(border)
    t._tbl.tblPr.append(borders)
    for i, value in enumerate(rows[0]): t.rows[0].cells[i].text = value
    for row in rows[1:]:
        cells = t.add_row().cells
        for i, value in enumerate(row): cells[i].text = value
    width = 15.5 / len(rows[0])
    for column in t.columns: column.width = Cm(width)
    configure_table(t)
    if anchor is not None: anchor.addprevious(t._tbl)
    return t


def add_blocks(doc, items, anchor=None):
    def place(p):
        if anchor is not None: anchor.addprevious(p._p)
        return p
    def caption(value):
        caption_style = next(p.style for p in doc.paragraphs if p.text.startswith("Hình 3-1"))
        p = place(doc.add_paragraph(style=caption_style))
        match = re.match(r"(Bảng|Hình) 3-(\d+) (.*)", value)
        label, number, title = match.groups()
        p.add_run(label + " 3-")
        field = OxmlElement("w:fldSimple")
        field.set(qn("w:instr"), f"SEQ {label.lower()} \\n")
        run = OxmlElement("w:r"); txt = OxmlElement("w:t"); txt.text = number
        run.append(txt); field.append(run); p._p.append(field)
        p.add_run(" " + title)
        p.paragraph_format.first_line_indent = Cm(0)
        p.paragraph_format.space_before = Pt(5)
        return p
    for kind, value in items:
        if kind in ["h2", "h3"]:
            # Keep new headings in the TOC while cancelling inherited numbering
            # because these additions carry an explicit section number.
            p = place(doc.add_paragraph(value, style="Heading 2" if kind == "h2" else "Heading 3"))
            num = OxmlElement("w:numPr"); num_id = OxmlElement("w:numId")
            num_id.set(qn("w:val"), "0"); num.append(num_id); p._p.get_or_add_pPr().append(num)
            p.paragraph_format.first_line_indent = Cm(0)
            p.paragraph_format.keep_with_next = True
            p.paragraph_format.space_before = Pt(12)
            for r in p.runs: r.bold = True
        elif kind == "p": place(doc.add_paragraph(value))
        elif kind == "caption":
            p = caption(value)
            p.paragraph_format.keep_with_next = True
        elif kind == "table": add_table(doc, value, anchor)
        elif kind == "image":
            p = place(doc.add_paragraph())
            p.paragraph_format.first_line_indent = Cm(0)
            p.paragraph_format.keep_with_next = True
            p.paragraph_format.line_spacing = 1.0
            p.paragraph_format.space_before = Pt(8)
            p.paragraph_format.space_after = Pt(3)
            p.add_run().add_picture(str(EVIDENCE / value[0]), width=Cm(15))
            caption(value[1])


def main():
    md = OUT / "Bao_cao_DATN_Dinh_Cong_Minh.md"
    original = md.read_text(encoding="utf-8")
    backup = ROOT / "tmp/remediation-20261107/Bao_cao_DATN_truoc_sua.md"
    backup.parent.mkdir(parents=True, exist_ok=True)
    if not backup.exists(): backup.write_text(original, encoding="utf-8")
    else: original = backup.read_text(encoding="utf-8")
    text = replace_text(original)
    # Replace the AI table without asserting which unlogged tool wrote each line.
    pattern = r"\| Nội dung có dùng AI \|[^\n]*\n(?:\|[^\n]*\n)+"
    text, count = re.subn(pattern, table_md(ai_rows) + "\n", text)
    assert count == 1, "AI declaration table was not uniquely identified."
    if "## 3.11 Cập nhật" not in text:
        text = text.replace("# KẾT LUẬN", markdown_blocks(blocks) + "\n\n# KẾT LUẬN", 1)
    if "## Phụ lục E " not in text: text += "\n\n" + markdown_blocks(appendix_blocks) + "\n"
    refs = ("[R5] Hồ sơ cập nhật 01–07/11/2026. Báo cáo kho dữ liệu ngày 01/11; output/e2e-2026-11-04; "
            "evidence/remediation-2026-11-07 và JUnit pytest_baseline_remediation_2026-11-07.xml.\n\n"
            "[R6] Khoa Công nghệ thông tin, Trường Đại học Mỏ - Địa chất. Cẩm nang thực hiện ĐATN dành cho sinh viên ngành Khoa học dữ liệu, 25 trang, bản do sinh viên cung cấp ngày 07/11/2026; không ghi năm xuất bản.\n\n")
    if "[R5] Hồ sơ cập nhật" not in text:
        text = text.replace("## Công trình và tài liệu chuyên môn bằng tiếng Anh", refs + "## Công trình và tài liệu chuyên môn bằng tiếng Anh", 1)
    md.write_text(text, encoding="utf-8")
    (OUT / "Bao_cao_DATN_Dinh_Cong_Minh_2026-11-07.md").write_text(text, encoding="utf-8")

    doc = Document(OUT / "Bao_cao_DATN_Dinh_Cong_Minh.docx")
    original_tables = list(doc.tables)
    for p in doc.paragraphs:
        value = replace_text(p.text)
        if value != p.text: p.text = value
    for table in original_tables:
        for row in table.rows:
            for cell in row.cells:
                for p in cell.paragraphs:
                    value = replace_text(p.text)
                    if value != p.text: p.text = value
    old = original_tables[19]._tbl
    add_table(doc, ai_rows, old)
    old.getparent().remove(old)
    # Correct the algorithm configuration table; historical numerical tables stay dated.
    for row in original_tables[6].rows[1:]:
        name = row.cells[0].text
        if name == "Naive": row.cells[1].text = "Mới: y[t]; artifact cũ: ưu tiên lag_1"
        elif name == "Seasonal naive": row.cells[1].text = "Mới: lag_6 cho target t+1; cũ: lag_7"
        elif name == "Moving average": row.cells[1].text = "Mới: mean(y[t−6..t]); cũ: mean(y[t−7..t−1])"
    conclusion = next(p for p in doc.paragraphs if p.text.strip() == "KẾT LUẬN")
    add_blocks(doc, blocks, conclusion._p)
    english_refs = next(p for p in doc.paragraphs if p.text == "Công trình và tài liệu chuyên môn bằng tiếng Anh")
    for ref in refs.strip().split("\n\n"):
        p = doc.add_paragraph(ref); english_refs._p.addprevious(p._p)
    add_blocks(doc, appendix_blocks)
    # Update remaining source/AI statements whose paragraphs need current context.
    for p in doc.paragraphs:
        if p.text.startswith("Hiện nhật ký chưa thống kê"):
            p.text = "Mức chỉnh sửa được mô tả định tính theo khai báo ngày 07/11; không tự ước lượng tỷ lệ phần trăm. Ảnh quá trình sử dụng AI và bản đối chiếu từng thay đổi vẫn cần bổ sung."
    doc.core_properties.modified = __import__("datetime").datetime(2026, 11, 7)
    doc.core_properties.comments = "Cập nhật 07/11: baseline, metric, bằng chứng và khai báo của sinh viên; cần duyệt bản nộp."
    setting = OxmlElement("w:updateFields"); setting.set(qn("w:val"), "true"); doc.settings.element.append(setting)
    dest = OUT / "Bao_cao_DATN_Dinh_Cong_Minh_2026-11-07.docx"
    doc.save(dest)
    print(json.dumps({"docx": str(dest), "tables": len(doc.tables), "paragraphs": len(doc.paragraphs),
                      "passed": PASSED, "skipped": SKIPPED}, ensure_ascii=False))


if __name__ == "__main__": main()
