"""Build a portable results index and preserve every test attempt, including failures."""
from datetime import datetime,timezone
import csv
import hashlib
import html
import json
from pathlib import Path
import shutil

ROOT=Path(__file__).resolve().parents[1]
OUT=ROOT/"output/pipeline-completion-2026-11-08"


def read(path):return json.loads(path.read_text(encoding="utf-8-sig"))


def main():
    regression=read(OUT/"regression-complete/test-results.json")
    live=read(OUT/"live-run1/result.json")
    monitoring=read(OUT/"monitoring/result.json")
    promtool=read(OUT/"promtool/result.json")
    assert regression["counts"]["failed"]==regression["counts"]["skipped"]==0
    assert all(x["status"]=="PASS" for x in (live,monitoring,promtool))
    backfill=[]
    for label in ("dry","apply","replay"):
        paths=sorted((OUT/("backfill-"+label)).glob("*.json"))
        audit=read(paths[-1]);assert audit["business_output_unchanged"] is True
        assert len(audit["selected_objects"])==2
        before,after=audit["before"],audit["after"]
        backfill.append({"case":label,"status":audit["status"],"business_output_unchanged":True,
                         "rows_before":sum(r["rows"] for r in before),"rows_after":sum(r["rows"] for r in after),
                         "quantity_before":sum(float(r["quantity"]) for r in before),"quantity_after":sum(float(r["quantity"]) for r in after),
                         "audit_file":paths[-1].relative_to(OUT).as_posix()})
        for name,values in (("before",before),("after",after)):
            with (paths[-1].parent/(name+".csv")).open("w",encoding="utf-8-sig",newline="") as target:
                writer=csv.DictWriter(target,fieldnames=["date","rows","quantity","buyer_amount","business_sha"])
                writer.writeheader();writer.writerows(values)
    with (OUT/"backfill-comparison.csv").open("w",encoding="utf-8-sig",newline="") as target:
        writer=csv.DictWriter(target,fieldnames=list(backfill[0]));writer.writeheader();writer.writerows(backfill)
    summary={"status":"PASS","completed_at_utc":datetime.now(timezone.utc).isoformat(),
        "primary_regression":regression["counts"],"primary_regression_tests":regression["tests"],
        "alert_rule_cases":promtool["cases"],"live":live,"monitoring":monitoring,"backfill":backfill,
        "power_bi":"student will finish independently, per instruction on 2026-11-08",
        "historical_attempts_retained":True,
        "prior_failure_resolved":"Seven CSV export failures in regression-final caused by Windows path length; shorter case directories fixed; regression-complete passed",
        "no_prospective_forecast_accuracy_claim":True}
    (OUT/"summary.json").write_text(json.dumps(summary,ensure_ascii=False,indent=2),encoding="utf-8")
    sources=OUT/"test-sources";sources.mkdir(exist_ok=True)
    for name in sorted({r["nodeid"].split("::")[0] for r in regression["cases"]}):
        shutil.copy2(ROOT/name,sources/Path(name).name)
    shutil.copy2(ROOT/"tests/pipeline_evidence_plugin.py",sources/"pipeline_evidence_plugin.py")
    sections=[]
    for path in sorted(OUT.glob("*/test-results.json")):
        report=read(path);run=path.parent.name
        rows=[]
        for case in report["cases"]:
            folder=path.parent/case["file"].replace("\\","/")
            detail_files=sorted(p for p in folder.glob("*") if p.name!="result.json")
            links=" ".join('<a href="'+html.escape(p.relative_to(OUT).as_posix(),quote=True)+'">'+html.escape(p.name)+'</a>' for p in detail_files)
            result_link=(folder/"result.json").relative_to(OUT).as_posix()
            source_link="test-sources/"+Path(case["nodeid"].split("::")[0]).name
            rows.append('<tr><td>'+html.escape(case["nodeid"])+'</td><td class="'+case["outcome"]+'">'+case["outcome"]+'</td><td><a href="'+result_link+'">Kết quả và log</a> · <a href="'+source_link+'">Mã kiểm thử hiện hành</a><br>'+links+'</td></tr>')
        sections.append('<h2>'+html.escape(run)+'</h2><p>'+html.escape(str(report["counts"]))+'</p><table><thead><tr><th>Bài kiểm tra</th><th>Kết quả</th><th>Dữ liệu và bằng chứng</th></tr></thead><tbody>'+"".join(rows)+'</tbody></table>')
    page='''<!doctype html><html lang="vi"><meta charset="utf-8"><title>Kết quả kiểm thử pipeline ngày 08 tháng 11 năm 2026</title>
<style>body{font:16px/1.55 system-ui;margin:32px auto;max-width:1250px;padding:0 24px;color:#172b4d}h1{font-size:29px}h2{margin-top:34px}table{border-collapse:collapse;width:100%;font-size:14px}td,th{border:1px solid #d5dce5;padding:9px;vertical-align:top;text-align:left;overflow-wrap:anywhere}th{background:#edf1f6}.passed{color:#176c45}.failed{color:#a3342b}.skipped{color:#936b1c}a{color:#175f9b}input{padding:10px;width:95%;font:inherit}li{margin:6px 0}code{background:#f2f4f7;padding:2px 5px}</style>
<h1>Kết quả kiểm thử pipeline ngày 08 tháng 11 năm 2026</h1>
<p><strong>Lượt hồi quy chốt: '''+str(regression["counts"]["passed"])+''' đạt, 0 lỗi, 0 bỏ qua.</strong> Sáu ca kiểm tra rule cảnh báo đạt; lô Kafka đến warehouse/API và ba bước backfill có đối soát đều đạt.</p>
<p>Mục tiêu là tính đúng và khả năng chạy end-to-end của pipeline. Bộ kết quả này không tuyên bố mô hình dự báo tối ưu. Power BI do sinh viên tự hoàn thiện.</p>
<ul><li><a href="summary.json">Tổng hợp máy đọc được</a> · <a href="README.md">Hướng dẫn đọc bộ kết quả</a></li>
<li><a href="live-run1/input-orders.json">Đầu vào đơn hàng</a> · <a href="live-run1/kafka-readback.json">Đọc lại Kafka</a> · <a href="live-run1/minio-raw.csv">Raw CSV</a> · <a href="live-run1/warehouse-output.csv">Đầu ra warehouse CSV</a> · <a href="live-run1/quarantine-output.csv">Dữ liệu lỗi CSV</a></li>
<li><a href="live-run1/etl-run-lineage.json">Lineage và thời gian từng mẻ ETL</a> · <a href="live-run1/inventory-api.json">API tồn kho</a> · <a href="live-run1/forecast-api.json">Nhánh API dự báo</a></li>
<li><a href="backfill-comparison.csv">Đối soát backfill trước sau CSV</a> · <a href="promtool/result.json">Sáu ca rule cảnh báo</a> · <a href="monitoring/alert-delivery.json">Cảnh báo thực đã nhận nội bộ</a> · <a href="monitoring/pipeline-status.json">Trạng thái pipeline đo được</a></li></ul>
<p>Giữ cả các lượt thử trước để đối chiếu. Lượt <code>regression-final</code> ban đầu có 7 lỗi ghi CSV do đường dẫn Windows quá dài; đã sửa tên thư mục. Lượt chốt là <code>regression-complete</code>. Không cộng các lượt trùng thành một số test mới.</p>
<p>Mỗi bài có kết quả, tham số và log. Các ca nghiệp vụ mới kèm snapshot đầu vào, mong đợi, thực tế và CSV khi dữ liệu dạng bảng; mã kiểm thử nằm trong <code>test-sources</code>. Không lấy biến môi trường hoặc mật khẩu làm dữ liệu xuất.</p>
<label>Lọc tên bài kiểm tra <input id="filter" placeholder="schema, backfill, latency, postgres..."></label>'''+"".join(sections)+'''
<script>document.getElementById('filter').addEventListener('input',function(){const q=this.value.toLowerCase();document.querySelectorAll('tbody tr').forEach(r=>r.hidden=!r.textContent.toLowerCase().includes(q));});</script></html>'''
    (OUT/"Ket_qua_kiem_thu.html").write_text(page,encoding="utf-8")
    md=f'''# Hoàn thiện và kiểm thử pipeline ngày 08 tháng 11 năm 2026

Luồng cốt lõi có hợp đồng dữ liệu phiên bản hóa, kiểm thử schema thay đổi, backfill giới hạn theo ngày/object, khóa chống chạy chồng, dữ liệu giám sát hàng đợi/độ trễ/khối lượng, lineage theo object và run, cùng đường giao nhận cảnh báo nội bộ. Mục tiêu là pipeline end-to-end; không thay mô hình đang phục vụ hoặc tối ưu WAPE trong đợt này. Power BI do sinh viên tự hoàn thiện theo yêu cầu.

## Kết quả chốt

- Hồi quy liên quan: **{regression['counts']['passed']} đạt, 0 lỗi, 0 bỏ qua**, gồm PostgreSQL thật trong schema riêng được rollback.
- Sáu ca promtool: backlog, trạng thái nhàn rỗi, độ trễ có đo/không đo, volume đã đủ/chưa đủ tham chiếu đều đúng mong đợi.
- Lô `{live['run_id']}`: 8 ACK/8 Kafka đọc lại/8 raw trong 2 object; đích có 2 dòng duy nhất, tổng quantity 10; 3 sự kiện lỗi được cách ly; stock mới 50 giữ nguyên khi bản cũ đến sau. Hai object đã checkpoint.
- Hoàn tất lô được quan sát sau {live['actual']['observed_completion_seconds']:.2f} giây từ lúc bắt đầu gửi. Đây là thời gian quan sát có polling, không phải P95 sự kiện đầu cuối.
- Mẻ ETL cuối có P95 consumer nhận đến hoàn tất mẻ **{monitoring['last_batch_p95_seconds']:.2f} giây**, đo trên 7 dòng; không gồm độ trễ ở nguồn/producer. Không thay số chưa đo của raw cũ bằng 0.
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
'''
    (OUT/"README.md").write_text(md,encoding="utf-8")
    (ROOT/"docs/07-bao-cao-do-an/hoan-thien-pipeline-2026-11-08.md").write_text(md.replace("(Ket_qua_kiem_thu.html)","(../../output/pipeline-completion-2026-11-08/Ket_qua_kiem_thu.html)"),encoding="utf-8")
    manifest={p.relative_to(OUT).as_posix():hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(OUT.rglob("*")) if p.is_file() and p.name!="file-manifest.json"}
    (OUT/"file-manifest.json").write_text(json.dumps(manifest,ensure_ascii=False,indent=2),encoding="utf-8")
    print(json.dumps({"status":"PASS","tests":regression['counts'],"files":len(manifest),"html":str(OUT/"Ket_qua_kiem_thu.html")},ensure_ascii=False))


if __name__=="__main__":main()
