"""Append verified pipeline results; preserve the earlier modelling revision."""
from datetime import datetime
import json
from pathlib import Path
import sys

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/"scripts"))
import update_thesis_remediation as layout
from docx import Document
from docx.oxml import OxmlElement
from docx.oxml.ns import qn
from docx.shared import Cm,Pt

OUT=ROOT/"output/docx"
RESULTS=ROOT/"output/pipeline-completion-2026-11-08"
S=json.loads((RESULTS/"summary.json").read_text(encoding="utf-8"))

test_rows=[
    ["Nhóm kiểm tra","Kết quả","Phạm vi bằng chứng"],
    ["Hồi quy liên quan",f"{S['primary_regression']['passed']} đạt; 0 lỗi; 0 bỏ qua","Có PostgreSQL thật trong schema riêng, rollback sau thử; không gọi đây là toàn bộ test của mọi phân hệ"],
    ["Schema evolution","Thêm trường/1.x được nhận; đổi kiểu/thiếu trường/major lạ bị cách ly","Giữ raw nguyên giá trị; dòng tốt cùng mẻ tiếp tục được xử lý"],
    ["Backfill","Dry-run, apply và replay giữ nguyên nội dung nghiệp vụ","Chọn đúng 2 object; không reset checkpoint; khóa phiên PostgreSQL chặn tác vụ chồng"],
    ["Quy tắc cảnh báo","6/6 ca promtool đạt","Backlog, idle, độ trễ có/chưa đo, volume đủ/chưa đủ tham chiếu"],
    ["Giao nhận cảnh báo","Đã nhận cảnh báo thật tại receiver nội bộ","Prometheus → Alertmanager → nhật ký local; không gửi tới người khác"],
]
flow_rows=[
    ["Tầng hoặc chỉ tiêu","Kết quả quan sát","Đối soát"],
    ["Kafka","8 ACK; đọc lại đủ 8 bản tin","Có topic, partition, offset từng bản tin"],
    ["MinIO raw","8 dòng trong 2 object","Giữ dữ liệu gốc và thời điểm consumer nhận; cả 2 object được checkpoint"],
    ["ETL live trong 2 mẻ","3 dòng sạch sau dedup trong mẻ; 2 bản trùng trong mẻ; 3 dòng lỗi","8 = 3 + 2 + 3; 3 lần ghi sạch hội tụ về 2 dòng nghiệp vụ"],
    ["Warehouse","2 dòng duy nhất; tổng quantity 10","Đơn A giữ 7, đơn B là 3; bản cũ không làm giảm đầu ra"],
    ["API tồn kho","Stock = 50","Snapshot cũ stock 5 đến sau không ghi đè bản mới"],
    ["API dự báo","7 điểm, nguồn mlflow_registry, lịch sử warehouse","Nhánh dữ liệu lịch sử riêng; không lấy HTTP 200 làm bằng chứng độ chính xác"],
]
sla_rows=[
    ["Chỉ số","Chính sách local","Kết quả và cách diễn giải"],
    ["Tuổi raw chờ ETL",">180 giây khi pending >0, duy trì 1 phút","Lô đã xử lý hết, pending =0; pipeline nhàn rỗi không tự thành lỗi dữ liệu cũ"],
    ["P95 consumer đến hoàn tất mẻ",">120 giây, có dòng đo được, duy trì 1 phút",f"Mẻ cuối 7 dòng: {S['monitoring']['last_batch_p95_seconds']:.2f} giây; không gồm độ trễ nguồn/producer".replace("31.03","31,03")],
    ["Volume mẻ xử lý","Ngoài 0,25–4 lần trung vị tối đa 12 mẻ trước; cần ít nhất 6 mẻ tham chiếu","Chưa đủ tham chiếu thì để chưa xác định; không gọi đây là phát hiện biến động nhu cầu thị trường"],
    ["Thời gian hoàn tất lô quan sát","Đo riêng từ bắt đầu gửi đến lúc đối soát xong",f"{S['live']['actual']['observed_completion_seconds']:.2f} giây, có polling; không phải P95 sự kiện đầu cuối".replace("33.20","33,20")],
]
blocks=[
    ("h2","3.13 Hoàn thiện và nghiệm thu luồng pipeline end to end"),
    ("p","Ngày 08/11/2026, em xác định tiêu chí cao nhất của đồ án là luồng dữ liệu end-to-end có đầu vào, chuyển đổi, kiểm định, lưu trữ, đầu ra và cơ chế vận hành có thể kiểm chứng. Mô hình dự báo là nhánh tiêu thụ dữ liệu để minh họa MLOps. Chất lượng dự báo ở mục 3.12 là giới hạn ứng dụng; không dùng WAPE cao để tự phủ định tính thông suốt kỹ thuật của pipeline. Các công thức, metric và metadata vẫn phải đúng. [R11]"),
    ("h3","3.13.1 Ranh giới và hợp đồng dữ liệu"),
    ("p","Nhánh sự kiện gồm simulator → producer → Kafka → consumer → MinIO Parquet → ETL/kiểm định/cách ly → PostgreSQL → SQL phục vụ BI và API tồn kho. Worker tự chạy ingestion và ETL gia tăng. Nhánh CSV lịch sử → warehouse lịch sử → feature → training → MLflow → FastAPI được kích hoạt riêng; không giả định mỗi sự kiện mới tự sinh một model mới. Power BI Desktop do sinh viên tự hoàn thiện theo phân công ngày 08/11; đợt này không xác nhận Refresh/render."),
    ("p","Hợp đồng ecommerce.orders/1.0 được lưu trong warehouse/contracts/orders-v1.json. Nguồn legacy không khai báo phiên bản và phiên bản 1.x bổ sung trường tùy chọn được chấp nhận; giá trị gốc vẫn được giữ. Thiếu trường bắt buộc, kiểu cấu trúc sai hoặc major version không hỗ trợ được gắn lỗi và cách ly trước khi khử trùng. Timestamp không offset tiếp tục theo giả định UTC cũ, chưa được coi là xác nhận múi giờ từ nguồn."),
    ("h3","3.13.2 Backfill và dữ liệu giám sát"),
    ("p","Tác vụ backfill mặc định chỉ kiểm định, giới hạn tối đa 31 ngày và chỉ ghi khi có cờ apply. Có thể chọn danh sách object cụ thể; object ngoài khoảng ngày bị từ chối, ngày không có object được chọn không tự mở rộng thành toàn partition. Worker, CLI ETL và backfill dùng cùng khóa phiên PostgreSQL để tránh ghi chồng. Quy tắc source_updated_at tiếp tục chặn ghi lùi. Audit lưu snapshot trước/sau, ngày lỗi và các bước đã hoàn tất; không xóa hoặc reset checkpoint."),
    ("p","Consumer bổ sung thời điểm nhận, topic, partition và offset vào metadata raw. ETL lưu run ID, accounting theo object và độ trễ đo được; worker giữ nhật ký từng run. API có /pipeline/status; Prometheus theo dõi hàng đợi, tuổi raw chờ, lần hoàn tất gần nhất, P95 mẻ và volume tham chiếu. Số chưa đo dùng null hoặc giá trị chỉ thị −1, không đổi thành độ trễ 0. Grafana đã nạp sáu panel pipeline. Alertmanager chuyển cảnh báo tới receiver nội bộ và lưu JSONL, không gửi email/Slack ra ngoài."),
    ("caption","Bảng 3-16 Kết quả kiểm thử hoàn thiện các yêu cầu pipeline"),
    ("table",test_rows),
    ("h3","3.13.3 Đối soát lô dữ liệu trên dịch vụ thật"),
    ("p",f"Lô thử {S['live']['run_id']} dùng dữ liệu có nhãn riêng và được giữ để đối soát. Dữ liệu gồm bản hợp lệ, bản cập nhật cũ đến sau, bản phát lại, trường mới, quantity sai kiểu, major version lạ và khóa SKU bị thiếu. Đọc lại Kafka độc lập từ offset trước lúc phát; sau đó đối chiếu raw, quarantine, fact, checkpoint và API. Không lấy số request thành công thay cho đối soát dữ liệu."),
    ("caption","Bảng 3-17 Dữ liệu đi qua các tầng trong lô nghiệm thu ngày 08 tháng 11"),
    ("table",flow_rows),
    ("p","Sau ca live, backfill chỉ đọc đúng hai object của lô này: dry-run, apply và apply lần hai đều cho hash nội dung nghiệp vụ trước–sau bằng nhau. Loaded_at hoặc nhật ký kiểm định có thể cập nhật khi chạy lại; tính idempotent được xét ở nội dung nghiệp vụ và khóa đích. Độ trễ của lần replay được ghi là tuổi replay, không đưa vào chỉ số SLA live của worker. Trong thử nghiệm PostgreSQL cô lập, bản mới được nạp trước rồi bản cũ được đưa qua ETL, xác nhận quy tắc tại sink không ghi lùi."),
    ("caption","Bảng 3-18 Ngưỡng giám sát local và số đo trong lần nghiệm thu"),
    ("table",sla_rows),
    ("h3","3.13.4 Hồ sơ kiểm thử và các giới hạn giữ lại"),
    ("p","Bộ kết quả nằm tại output/pipeline-completion-2026-11-08. Trang Ket_qua_kiem_thu.html lập chỉ mục từng bài, tham số, trạng thái và log. Ca nghiệp vụ mới có JSON đầu vào/mong đợi/thực tế và CSV cho bảng dữ liệu; các lượt backfill có before.csv, after.csv và bảng đối soát. Mã kiểm thử được chụp trong test-sources. Giữ cả các lượt trước; bảy lỗi xuất CSV do đường dẫn Windows quá dài đã sửa bằng tên thư mục ngắn, không xóa lịch sử lỗi. Lượt chốt regression-complete đạt 134/134, không cộng các lượt trùng thành số kiểm thử mới."),
    ("p","Bốn hash của daily_history, features_daily, feature_spec và model_manifest không đổi. API vẫn dùng Registry v2 và không có thao tác thay model trong đợt này. Cảnh báo EcommerceDemandHistoryStale thực sự firing vì nhánh lịch sử model đã cũ; cảnh báo này được giao tới receiver nội bộ và không bị tắt để làm trạng thái đẹp hơn. Các ngưỡng local chưa phải SLA doanh nghiệp hay kết quả thử tải dài hạn. Lịch retraining tự động, compaction, SCD2, CDC, Spark và RAG không được tuyên bố đã triển khai khi nằm ngoài phạm vi hiện tại."),
]
appendix=[
    ("h2","Phụ lục G Bộ dữ liệu và kết quả kiểm thử pipeline"),
    ("p","Lượt chốt: regression-complete/junit.xml, test-results.json, test-index.csv và cases/*/result.json. Các ca mới lưu snapshot dữ liệu có thể đọc lại; test-sources giữ mã nguồn của những bài đã chạy. Lượt regression-final được lưu nguyên trạng để đối chiếu lỗi xuất CSV và kết quả sau sửa. Dữ liệu kiểm thử nghiệp vụ mới là dữ liệu tổng hợp có nhãn; không xuất toàn bộ warehouse hoặc hai CSV gốc vào gói kết quả."),
    ("p","Trong live-run1: input-orders.json, input-inventory.json, kafka-acknowledgements.json, kafka-readback.json, minio-raw.json/csv, quarantine-output.json/csv, warehouse-output.json/csv, etl-run-lineage.json, inventory-api.json và forecast-api.json. source-objects.json là danh sách chính xác hai object cho ba ca backfill. Các audit backfill ghi status, dữ liệu trước/sau và business_output_unchanged; bảng tổng hợp là backfill-comparison.csv."),
    ("p","Trong monitoring: metrics.prom, pipeline-status.json, Prometheus rules/alerts/targets, Grafana dashboard, Alertmanager alerts và alert-delivery.json. Sáu trường hợp promtool có mỗi trường hợp một specification và một tệp kết quả. summary.json ghi phạm vi; file-manifest.json giữ SHA-256 cho các tệp bằng chứng. Gói ZIP bàn giao được kiểm tra CRC sau đóng gói."),
    ("p","Codex hỗ trợ viết hợp đồng, mã kiểm thử, backfill, telemetry, chỉ mục kết quả và cập nhật văn bản trong đợt 08/11. Sinh viên cần tự chạy lại, đọc đối soát và giải thích thiết kế trước bảo vệ. Power BI do sinh viên tiếp tục hoàn thiện; không có kết quả Desktop do AI xác nhận trong đợt này."),
]
reference="[R11] Hồ sơ hoàn thiện pipeline ngày 08/11/2026. output/pipeline-completion-2026-11-08; warehouse/contracts/orders-v1.json; scripts/backfill_pipeline.py; /pipeline/status; metadata và nhật ký ETL theo object/run. Lượt chốt regression-complete, promtool, lô CMP và audit backfill được lưu riêng theo phạm vi."
focus="Tiêu chí cao nhất của đồ án là pipeline end-to-end xử lý đúng, có kiểm soát chất lượng, chống trùng, cập nhật gia tăng, phục hồi, truy vết và đầu ra hạ nguồn kiểm chứng được. Mô hình dự báo đóng vai trò ứng dụng tiêu thụ dữ liệu để nghiệm thu MLOps; tối đa hóa độ chính xác dự báo không phải mục tiêu chính."
conclusion="Bổ sung nghiệm thu pipeline ngày 08/11/2026: lượt hồi quy chốt có 134 bài đạt, không lỗi hoặc bỏ qua; sáu ca rule cảnh báo đạt. Lô live được đọc lại và đối soát đủ Kafka/raw/fact/quarantine/checkpoint/API; backfill có giới hạn chạy lại giữ nguyên nội dung nghiệp vụ. Đây là căn cứ đánh giá mục tiêu luồng dữ liệu end-to-end. Power BI được sinh viên tự hoàn thiện; giới hạn chất lượng và độ mới dữ liệu dự báo vẫn được công bố riêng."

base_configure=layout.configure_table


def configure_table(table):
    base_configure(table)
    for index,row in enumerate(table.rows):
        for cell in row.cells:
            for paragraph in cell.paragraphs:
                paragraph.paragraph_format.keep_with_next=index==0


def markdown(items):
    parts=[]
    for kind,value in items:
        if kind in ("h2","h3"):parts.append(("## " if kind=="h2" else "### ")+value)
        elif kind=="table":parts.append(layout.table_md(value))
        else:parts.append(value)
    return "\n\n".join(parts)


def main():
    layout.configure_table=configure_table
    source=OUT/"Bao_cao_DATN_Dinh_Cong_Minh_2026-11-08"
    text=source.with_suffix(".md").read_text(encoding="utf-8")
    text=text.replace("bổ sung đối soát CSV gốc và đánh giá nhu cầu thưa, kế thừa hồ sơ đã hợp nhất hồ sơ tháng 10",
                      "bổ sung nghiệm thu pipeline, đối soát CSV gốc và đánh giá nhu cầu thưa, kế thừa hồ sơ tháng 10")
    text=text.replace("## 2. Mục tiêu và phạm vi","## 2. Mục tiêu và phạm vi\n\n"+focus,1)
    text=text.replace("# KẾT LUẬN",markdown(blocks)+"\n\n# KẾT LUẬN\n\n"+conclusion,1)
    text=text.replace("## Công trình và tài liệu chuyên môn bằng tiếng Anh",reference+"\n\n## Công trình và tài liệu chuyên môn bằng tiếng Anh",1)
    text+="\n\n"+markdown(appendix)+"\n"
    dest=OUT/"Bao_cao_DATN_Dinh_Cong_Minh_2026-11-08_pipeline"
    dest.with_suffix(".md").write_text(text,encoding="utf-8")
    (OUT/"Bao_cao_DATN_Dinh_Cong_Minh.md").write_text(text,encoding="utf-8")
    doc=Document(source.with_suffix(".docx"))
    for paragraph in doc.paragraphs:
        if "kế thừa hồ sơ đã hợp nhất hồ sơ tháng 10" in paragraph.text:
            paragraph.text=paragraph.text.replace("bổ sung đối soát CSV gốc và đánh giá nhu cầu thưa, kế thừa hồ sơ đã hợp nhất hồ sơ tháng 10",
                "bổ sung nghiệm thu pipeline, đối soát CSV gốc và đánh giá nhu cầu thưa, kế thừa hồ sơ tháng 10")
    target=next(p for p in doc.paragraphs if p.text.strip()=="KẾT LUẬN")
    layout.add_blocks(doc,blocks,target._p)
    p=doc.add_paragraph(conclusion);target._p.addnext(p._p)
    heading=next(p for p in doc.paragraphs if p.text.strip()=="2. Mục tiêu và phạm vi") if any(p.text.strip()=="2. Mục tiêu và phạm vi" for p in doc.paragraphs) else None
    if heading is None:
        heading=next(p for p in doc.paragraphs if p.text.strip()=="Mục tiêu và phạm vi")
    p=doc.add_paragraph(focus);heading._p.addnext(p._p)
    english=next(p for p in doc.paragraphs if p.text=="Công trình và tài liệu chuyên môn bằng tiếng Anh")
    p=doc.add_paragraph(reference);english._p.addprevious(p._p)
    layout.add_blocks(doc,appendix)
    doc.core_properties.modified=datetime(2026,11,8)
    doc.core_properties.comments="Nghiệm thu pipeline end-to-end, toàn bộ kết quả test được lưu; Power BI do sinh viên tự làm."
    setting=OxmlElement("w:updateFields");setting.set(qn("w:val"),"true");doc.settings.element.append(setting)
    doc.save(dest.with_suffix(".docx"))
    print(json.dumps({"docx":str(dest.with_suffix('.docx')),"tables":len(doc.tables)},ensure_ascii=False))


if __name__=="__main__":main()
