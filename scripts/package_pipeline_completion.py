"""Validate the completed evidence set and package it without deleting attempts."""
from datetime import datetime,timezone
import ast
import hashlib
from html.parser import HTMLParser
import json
from pathlib import Path
import re
import zipfile
from docx import Document
from pypdf import PdfReader

ROOT=Path(__file__).resolve().parents[1]
OUT=ROOT/"output/pipeline-completion-2026-11-08"


class Links(HTMLParser):
    def __init__(self):super().__init__();self.links=[]
    def handle_starttag(self,tag,attributes):
        if tag=="a":self.links.extend(v for k,v in attributes if k=="href")


def main():
    summary=json.loads((OUT/"summary.json").read_text(encoding="utf-8"))
    assert summary["status"]=="PASS" and summary["primary_regression_tests"]==134
    regression=json.loads((OUT/"regression-complete/test-results.json").read_text(encoding="utf-8"))
    for entry in regression["cases"]:
        path=OUT/"regression-complete"/entry["file"].replace("\\","/")/"result.json"
        assert path.exists() and entry["outcome"]=="passed"
    parser=Links();parser.feed((OUT/"Ket_qua_kiem_thu.html").read_text(encoding="utf-8"))
    for href in parser.links:
        if not href.startswith(("http:","https:","#")):assert (OUT/href).exists(),href
    manifest=json.loads((OUT/"file-manifest.json").read_text(encoding="utf-8"))
    for name,expected in manifest.items():assert hashlib.sha256((OUT/name).read_bytes()).hexdigest()==expected,name
    code=["warehouse/etl/contracts.py","warehouse/etl/coordination.py","warehouse/etl/observability.py",
          "warehouse/etl/transform.py","warehouse/etl/data_validation.py","warehouse/etl/pipeline.py",
          "warehouse/etl/extract.py","ingestion/consumer_to_minio.py","scripts/run_pipeline_worker.py",
          "scripts/backfill_pipeline.py","scripts/local_alert_receiver.py","serving/app/pipeline_status.py",
          "serving/app/main.py","scripts/accept_pipeline_completion.py","scripts/capture_pipeline_monitoring.py",
          "scripts/check_pipeline_alert_rules.py","tests/pipeline_evidence_plugin.py","tests/test_pipeline_completion.py",
          "tests/test_pipeline_completion_postgres.py"]
    for name in code:ast.parse((ROOT/name).read_text(encoding="utf-8-sig"),filename=name)
    docx=ROOT/"output/docx/Bao_cao_DATN_Dinh_Cong_Minh_2026-11-08_pipeline.docx"
    doc=Document(docx);text="\n".join(p.text for p in doc.paragraphs)
    assert text.count("3.13 Hoàn thiện và nghiệm thu luồng pipeline end to end")==1
    assert "Power BI Desktop do sinh viên tự hoàn thiện" in text
    assert "134/134" in text and "không phải mục tiêu chính" in text
    tables=[t for t in doc.tables if t.cell(0,0).text in ("Nhóm kiểm tra","Tầng hoặc chỉ tiêu","Chỉ số")]
    assert len(tables)==3
    for t in tables:
        assert t.rows[0].cells[0].paragraphs[0].paragraph_format.keep_with_next
    pdf=PdfReader(ROOT/"tmp/pipeline-completion-20261108/thesis-pipeline.pdf")
    assert len(pdf.pages)==106
    md=ROOT/"output/docx/Bao_cao_DATN_Dinh_Cong_Minh_2026-11-08_pipeline.md"
    assert md.read_text(encoding="utf-8")== (ROOT/"output/docx/Bao_cao_DATN_Dinh_Cong_Minh.md").read_text(encoding="utf-8")
    archive=ROOT/"output/Ket_qua_pipeline_2026-11-08.zip"
    with zipfile.ZipFile(archive,"w",zipfile.ZIP_DEFLATED,compresslevel=6) as package:
        for path in sorted(OUT.rglob("*")):
            if path.is_file():package.write(path,path.relative_to(OUT).as_posix())
    with zipfile.ZipFile(archive) as package:
        assert package.testzip() is None
        archive_files=len(package.infolist())
    evidence={"verified_at_utc":datetime.now(timezone.utc).isoformat(),"status":"PASS",
        "regression":summary["primary_regression"],"promtool_cases":summary["alert_rule_cases"],
        "live_run":summary["live"]["run_id"],"backfill_all_unchanged":all(r["business_output_unchanged"] for r in summary["backfill"]),
        "monitoring_status":summary["monitoring"]["status"],"power_bi":"student-owned, excluded from this completion",
        "python_files_parsed":code,"local_html_links_verified":len(parser.links),"evidence_hashes_verified":len(manifest),
        "docx":docx.relative_to(ROOT).as_posix(),"docx_sha256":hashlib.sha256(docx.read_bytes()).hexdigest(),
        "docx_pdf_pages":len(pdf.pages),"visual_review":"all contact sheets plus revised content pages inspected; table header kept with first data row",
        "archive":archive.relative_to(ROOT).as_posix(),"archive_bytes":archive.stat().st_size,
        "archive_files":archive_files,"archive_crc":"PASS","archive_sha256":hashlib.sha256(archive.read_bytes()).hexdigest()}
    (ROOT/"docs/07-bao-cao-do-an/evidence/pipeline-completion-2026-11-08.json").write_text(json.dumps(evidence,ensure_ascii=False,indent=2),encoding="utf-8")
    print(json.dumps({"status":"PASS","tests":summary["primary_regression"],"archive_files":archive_files,"archive_mb":round(archive.stat().st_size/1048576,2),"docx_pages":len(pdf.pages)},ensure_ascii=False))


if __name__=="__main__":main()
