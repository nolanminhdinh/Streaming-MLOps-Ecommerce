"""Verify report/evidence consistency and preserve a compact release record."""
from pathlib import Path
import ast
import hashlib
import json
import re
from datetime import datetime, timezone
from docx import Document
from pypdf import PdfReader

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "docs/07-bao-cao-do-an/evidence/forecast-improvement-2026-11-08"
summary = json.loads((OUT / "candidate-decisions.json").read_text(encoding="utf-8"))
evaluation = json.loads((OUT / "model-evaluation.json").read_text(encoding="utf-8"))
files = ["serving/app/sparse_demand.py", "serving/app/model_loader.py", "scripts/audit_forecast_data.py",
         "scripts/improve_sparse_forecast.py", "scripts/evaluate_volume_calibration.py",
         "scripts/summarize_forecast_improvement.py", "scripts/update_thesis_sparse_forecast.py",
         "scripts/qa_forecast_thesis.py", "scripts/validate_forecast_delivery.py", "tests/test_sparse_demand.py"]
for name in files:
    ast.parse((ROOT / name).read_text(encoding="utf-8-sig"), filename=name)
docx = ROOT / "output/docx/Bao_cao_DATN_Dinh_Cong_Minh_2026-11-08.docx"
md = ROOT / "output/docx/Bao_cao_DATN_Dinh_Cong_Minh_2026-11-08.md"
doc = Document(docx)
text = "\n".join(p.text for p in doc.paragraphs)
assert text.count("3.12 Phân tích nguyên nhân sai số và thử nghiệm nhu cầu thưa") == 1
assert "Phụ lục F Đối soát CSV và quyết định ứng viên dự báo" in text
for number in ["96,73%", "65,86%", "102,15%", "231,46%", "124 kiểm thử", "122/1.191"]:
    assert number in text, number
new_tables = [t for t in doc.tables if t.cell(0, 0).text in ["Kiểm tra", "Phương án", "Cửa sổ nhãn 2026"]]
assert len(new_tables) == 3
for table in new_tables:
    assert table.rows[0]._tr.trPr.find("{http://schemas.openxmlformats.org/wordprocessingml/2006/main}tblHeader") is not None
markdown = md.read_text(encoding="utf-8")
assert markdown == (ROOT / "output/docx/Bao_cao_DATN_Dinh_Cong_Minh.md").read_text(encoding="utf-8")
for relative in re.findall(r"!\[[^\]]*\]\(([^)]+)\)", markdown):
    # Earlier illustrations may be absolute host paths; both forms are supported.
    assert (md.parent / relative).resolve().exists(), relative
hashes = evaluation["protocol"]["source_sha256"]
unchanged = {p: hashlib.sha256((ROOT/p).read_bytes()).hexdigest() == expected for p, expected in hashes.items()}
assert all(unchanged.values())
model_dir = ROOT / "output/models/forecast-improvement-2026-11-08"
card = json.loads((model_dir / "model-card.json").read_text(encoding="utf-8"))
assert hashlib.sha256((ROOT/card["artifact"]).read_bytes()).hexdigest() == card["artifact_sha256"]
assert card["acceptance"]["inventory_approved"] is False
assert card["runtime_compatibility"]["container_load_validated"] is False
pdf = ROOT / "tmp/forecast-improvement-20261108/thesis-revision.pdf"
reader = PdfReader(pdf)
pages = [p.extract_text() for p in reader.pages]
assert "01/07" in pages[82] and "14/07" in pages[82]
assert "scikit-learn1.9.1" in re.sub(r"\s+", "", "\n".join(pages[-3:]))
result = {
    "verified_at_utc": datetime.now(timezone.utc).isoformat(), "tests": summary["test_result"],
    "python_files_parsed": files, "git_diff_check": "passed with native CRLF handling",
    "docx": docx.relative_to(ROOT).as_posix(), "docx_sha256": hashlib.sha256(docx.read_bytes()).hexdigest(),
    "markdown_matches_canonical": True, "markdown_image_links_exist": True,
    "pdf_pages": len(reader.pages), "renderer": "native Microsoft Word, then Poppler PNG",
    "packaged_renderer_limitation": "LibreOffice soffice.exe missing; diagnosed before Word fallback",
    "new_tables_repeat_headers": True, "deployment_hashes_unchanged": unchanged,
    "model_registered": False, "serving_reloaded": False,
    "model_artifact_hash_matches_card": True, "cross_version_container_load_tested": False,
    "visual_review": "contact sheets of all pages; revised pages, tables, charts, references and appendices inspected at full page size",
    "evaluation_type": "retrospective; final window previously exposed",
}
(OUT / "delivery-validation.json").write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
print(json.dumps({"pages": result["pdf_pages"], "tests": result["tests"], "deployment_unchanged": all(unchanged.values()), "links_valid": True}, ensure_ascii=False))
