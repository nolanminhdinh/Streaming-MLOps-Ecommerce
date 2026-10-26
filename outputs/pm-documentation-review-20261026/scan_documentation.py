"""Read-only repository documentation inventory; writes a separate audit summary."""

from __future__ import annotations

import ast
import csv
import hashlib
import json
from pathlib import Path
import posixpath
import re
import sys
from urllib.parse import unquote
import xml.etree.ElementTree as ET
import zipfile


ROOT = Path(__file__).resolve().parents[2]
OUT = Path(__file__).with_name("static-audit.json")
FOLDERS = (
    "docs", "ingestion/nifi", "ml", "monitoring", "notebooks", "powerbi",
    "scripts", "tests", "warehouse/etl",
)
REVIEW_NAME = "bao-cao-ra-soat-tai-lieu-pm-2026-10-26.md"
NS = {"s": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}


def rel(path: Path) -> str:
    return path.relative_to(ROOT).as_posix()


def workbook_inventory(path: Path) -> dict:
    sheets = []
    with zipfile.ZipFile(path) as archive:
        strings = []
        if "xl/sharedStrings.xml" in archive.namelist():
            strings = [
                "".join(t.text or "" for t in item.findall(".//s:t", NS))
                for item in ET.fromstring(archive.read("xl/sharedStrings.xml")).findall("s:si", NS)
            ]
        relationships = {
            item.attrib["Id"]: item.attrib["Target"]
            for item in ET.fromstring(archive.read("xl/_rels/workbook.xml.rels"))
        }
        workbook = ET.fromstring(archive.read("xl/workbook.xml"))
        for sheet in workbook.findall("s:sheets/s:sheet", NS):
            rid = sheet.attrib["{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id"]
            target = relationships[rid]
            target = target.lstrip("/") if target.startswith("/") else posixpath.normpath("xl/" + target)
            xml = ET.fromstring(archive.read(target))
            cells = []
            for cell in xml.findall(".//s:sheetData/s:row/s:c", NS):
                value_node = cell.find("s:v", NS)
                value = value_node.text or "" if value_node is not None else ""
                if cell.attrib.get("t") == "s" and value:
                    value = strings[int(value)]
                elif cell.attrib.get("t") == "inlineStr":
                    value = "".join(t.text or "" for t in cell.findall(".//s:t", NS))
                if value:
                    cells.append({"cell": cell.attrib["r"], "value": value})
            selected = [
                item for item in cells
                if re.search(r"19[.,]8|source_line|COUNT\(|AVERAGE\(|WAPE|YYYYMMDD|Dim_Dates\[Date\]|stockout|đứt hàng", item["value"], re.I)
            ]
            sheets.append({"name": sheet.attrib["name"], "nonempty_cells": len(cells), "selected_text_cells": selected})
    return {"path": rel(path), "sheets": sheets, "method": "Read stored OOXML text only; no recalculation or visual QA."}


def main() -> None:
    files = {ROOT / "README.md", ROOT / "PROJECT_CONTEXT.md"}
    for folder in FOLDERS:
        files.update(
            path for path in (ROOT / folder).rglob("*.md")
            if "node_modules" not in path.parts and path.name != REVIEW_NAME
        )
    inventory, broken, file_urls = [], [], []
    relative_count = 0
    for path in sorted(files):
        content = path.read_text(encoding="utf-8-sig")
        inventory.append({"path": rel(path), "lines": len(content.splitlines()), "sha256": hashlib.sha256(path.read_bytes()).hexdigest()})
        for match in re.finditer(r"!?\[[^\]\n]*\]\(([^)\n]+)\)", content):
            target = match.group(1).strip().strip("<>")
            item = {"path": rel(path), "line": content[:match.start()].count("\n") + 1, "target": target}
            if target.startswith("file:"):
                file_urls.append(item)
            if re.match(r"^[a-zA-Z][a-zA-Z0-9+.-]*:", target) or target.startswith("#"):
                continue
            target_path = unquote(target.split("#")[0].split("?")[0])
            if target_path:
                relative_count += 1
                if not (path.parent / target_path).exists():
                    broken.append(item)

    tree = ast.parse((ROOT / "data_simulator/data_simulator.py").read_text(encoding="utf-8-sig"))
    schemas = []
    for node in tree.body:
        if isinstance(node, ast.ClassDef):
            fields = [item.target.id for item in node.body if isinstance(item, ast.AnnAssign) and isinstance(item.target, ast.Name)]
            if fields:
                schemas.append({"name": node.name, "line": node.lineno, "declared_fields": len(fields)})

    with zipfile.ZipFile(ROOT / "powerbi/Report_PowerBI.pbix") as archive:
        pages = [json.loads(archive.read(name).decode("utf-8-sig")) for name in archive.namelist() if name.endswith("/page.json")]
        visuals = [json.loads(archive.read(name).decode("utf-8-sig")).get("visual", {}) for name in archive.namelist() if name.endswith("/visual.json")]

    csv_headers = {}
    for path in sorted((ROOT / "powerbi/data").glob("*.csv")):
        with path.open(encoding="utf-8-sig", newline="") as stream:
            csv_headers[rel(path)] = next(csv.reader(stream), [])

    junit = []
    for path in sorted((ROOT / "docs/07-bao-cao-do-an/evidence").glob("pytest*.xml")):
        xml = ET.parse(path).getroot()
        junit.append({"path": rel(path), "suites": [node.attrib for node in xml.iter("testsuite")]})

    notebooks = []
    for path in sorted((ROOT / "notebooks").glob("*.ipynb")):
        obj = json.loads(path.read_text(encoding="utf-8-sig"))
        cells = [cell for cell in obj["cells"] if cell["cell_type"] == "code"]
        notebooks.append({"path": rel(path), "code_cells": len(cells), "saved_executed_cells": sum(cell.get("execution_count") is not None for cell in cells)})

    result = {
        "review_date_local": "2026-10-26", "timezone": "Asia/Bangkok",
        "scope": "Static documentation audit. No runtime services, tests, external URLs or credentials inspected.",
        "markdown_count": len(inventory), "markdown_lines": sum(item["lines"] for item in inventory),
        "relative_link_occurrences": relative_count, "broken_relative_links": broken,
        "nonportable_file_urls": file_urls, "markdown_inventory": inventory,
        "simulator_classes": schemas,
        "ddl_tables": re.findall(r"CREATE TABLE IF NOT EXISTS\s+(\w+)", (ROOT / "warehouse/ddl/01_star_schema.sql").read_text(encoding="utf-8-sig"), re.I),
        "saved_model_manifest": json.loads((ROOT / "data/model_manifest.json").read_text(encoding="utf-8-sig")),
        "powerbi_report": {"pages": [page.get("displayName") for page in pages], "visual_types": [visual.get("visualType") for visual in visuals], "visuals_with_query": sum(bool(visual.get("query")) for visual in visuals), "method": "Stored report definitions; no Desktop refresh/render or DataModel execution."},
        "csv_headers": csv_headers, "junit_evidence_not_rerun": junit, "notebooks": notebooks,
        "workbooks": [workbook_inventory(ROOT / name) for name in ("docs/02-warehouse-etl-feature-store/data_management_specification.xlsx", "outputs/data-dictionary-20261020/Data_Dictionary_Ecommerce.xlsx")],
    }
    OUT.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"markdown_count": len(inventory), "markdown_lines": result["markdown_lines"], "relative_link_occurrences": relative_count, "broken_relative_links": broken, "file_url_count": len(file_urls), "powerbi_report": result["powerbi_report"], "evidence_path": rel(OUT)}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8")
    main()
