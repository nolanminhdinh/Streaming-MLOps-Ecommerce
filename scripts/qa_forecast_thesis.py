"""Rasterize the native Word PDF and locate revised content for visual review."""
from pathlib import Path
import json
import argparse
import re
import shutil
import subprocess
from PIL import Image, ImageDraw
from pypdf import PdfReader

ROOT = Path(__file__).resolve().parents[1]
TMP = ROOT / "tmp/forecast-improvement-20261108"
parser=argparse.ArgumentParser()
parser.add_argument("--pdf",type=Path,default=TMP/"thesis-revision.pdf")
parser.add_argument("--output",type=Path,default=TMP/"qa")
args=parser.parse_args()
OUT = args.output
OUT.mkdir(parents=True, exist_ok=True)
pdf = args.pdf
reader = PdfReader(pdf)
needles = ["3.12 Phân tích", "3.13 Hoàn thiện", "Bảng 3-16", "Bảng 3-17", "Bảng 3-18", "Phụ lục G", "[R11]", "132", "Tiêu chí cao nhất"]
locations = {s: [] for s in needles}
for i, page in enumerate(reader.pages, 1):
    text = page.extract_text()
    for s in needles:
        if re.sub(r"\s+","",s) in re.sub(r"\s+","",text): locations[s].append(i)
(OUT / "locations.json").write_text(json.dumps({"pages": len(reader.pages), "locations": locations}, ensure_ascii=False, indent=2), encoding="utf-8")
subprocess.run([shutil.which("pdftoppm"), "-r", "105", "-png", str(pdf), str(OUT / "page")], check=True, capture_output=True)
pages = sorted(OUT.glob("page-*.png"))
for offset in range(0, len(pages), 16):
    sheet = Image.new("RGB", (1100, 1592), "#dce0e5")
    draw = ImageDraw.Draw(sheet)
    for j, path in enumerate(pages[offset:offset+16]):
        im = Image.open(path).convert("RGB"); im.thumbnail((262, 366))
        x, y = (j%4)*275+6, (j//4)*398+25
        sheet.paste(im, (x, y)); draw.text((x, y-18), path.stem, fill="black")
    sheet.save(OUT / f"contact-{offset//16+1}.png")
print(json.dumps({"pages": len(reader.pages), "locations": locations}, ensure_ascii=False))
