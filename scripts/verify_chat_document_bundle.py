"""Check the reading bundle and archive it without changing any source file."""
from pathlib import Path
import csv
import hashlib
from html.parser import HTMLParser
import json
import os
import sys
from urllib.parse import unquote, urlsplit
import zipfile

ROOT = Path.cwd().resolve()
BUNDLE = ROOT / 'Tai_lieu_chat_DATN_2026-11-08'
ARCHIVE = ROOT / (BUNDLE.name + '.zip')
sys.stdout.reconfigure(encoding='utf-8')

def native(p):
    # Extended Windows paths require dot segments to be normalized first.
    text = os.path.abspath(str(p))
    return '\\\\?\\' + text if os.name == 'nt' and not text.startswith('\\\\?\\') else text

def digest(p):
    value = hashlib.sha256()
    with open(native(p), 'rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            value.update(block)
    return value.hexdigest()

class Links(HTMLParser):
    def __init__(self):
        super().__init__()
        self.links = []

    def handle_starttag(self, tag, attrs):
        for name, value in attrs:
            if name in ('href', 'src') and value:
                self.links.append(value)

def bundle_files():
    for directory, _, names in os.walk(native(BUNDLE)):
        for name in names:
            raw = str(Path(directory) / name)
            if raw.startswith('\\\\?\\'):
                raw = raw[4:]
            yield Path(raw)

if not BUNDLE.is_dir():
    raise SystemExit('Không có bộ tài liệu cần kiểm tra.')
if ARCHIVE.exists():
    raise SystemExit('Tệp ZIP đã tồn tại; dừng để không ghi đè.')

hash_mismatches = []
source_changes = []
with open(native(BUNDLE / '02_DANH_SACH_TOAN_BO_TEP.csv'), encoding='utf-8-sig', newline='') as stream:
    rows = list(csv.DictReader(stream))
for row in rows:
    file = BUNDLE / row['Tên tệp trong bộ']
    if digest(file) != row['SHA256 trong bộ']:
        hash_mismatches.append(row['Tên tệp trong bộ'])
    if row['Nguồn gốc'] and digest(Path(row['Nguồn gốc'])) != row['SHA256 nguồn']:
        source_changes.append(row['Nguồn gốc'])

links_checked = 0
broken_links = []
for file in list(bundle_files()):
    if file.suffix.lower() != '.html':
        continue
    parser = Links()
    parser.feed(file.read_text(encoding='utf-8', errors='replace'))
    for href in parser.links:
        if not href or href.startswith('#'):
            continue
        split = urlsplit(href)
        if split.scheme or split.netloc:
            continue
        local = unquote(split.path)
        if not local:
            continue
        target = file.parent / local
        links_checked += 1
        if not os.path.exists(native(target)):
            broken_links.append({'file': file.relative_to(BUNDLE).as_posix(), 'link': href})

report = {
    'status': 'PASS' if not (hash_mismatches or source_changes or broken_links) else 'FAIL',
    'kind': 'Kiểm tra sao chép và đóng gói tài liệu; không phải lượt kiểm thử pipeline mới',
    'catalog_file_hashes_checked': len(rows),
    'source_originals_unchanged': not source_changes,
    'hash_mismatches': hash_mismatches,
    'source_changes': source_changes,
    'html_local_links_checked': links_checked,
    'broken_html_local_links': broken_links,
}
verification = BUNDLE / '04_KIEM_TRA_BAN_SAO_VA_LIEN_KET.json'
verification.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding='utf-8')
if report['status'] != 'PASS':
    print(json.dumps(report, ensure_ascii=False, indent=2))
    raise SystemExit(1)

files = sorted(bundle_files())
with zipfile.ZipFile(native(ARCHIVE), 'w', compression=zipfile.ZIP_DEFLATED, compresslevel=6, allowZip64=True) as archive:
    for file in files:
        archive.write(native(file), arcname=(Path(BUNDLE.name) / file.relative_to(BUNDLE)).as_posix())
with zipfile.ZipFile(native(ARCHIVE), 'r') as archive:
    bad_member = archive.testzip()
    archived_count = len(archive.infolist())
if bad_member or archived_count != len(files):
    raise SystemExit(f'ZIP không toàn vẹn: {bad_member}, {archived_count}/{len(files)}')

archive_hash = digest(ARCHIVE)
(ROOT / (ARCHIVE.name + '.sha256.txt')).write_text(f'{archive_hash}  {ARCHIVE.name}\n', encoding='utf-8')
print(json.dumps({
    'status': 'PASS', 'folder': str(BUNDLE), 'zip': str(ARCHIVE),
    'primary_documents': 57, 'total_files': len(files),
    'html_local_links_checked': links_checked, 'source_hashes_checked': len(rows),
    'archive_crc': 'PASS', 'archive_bytes': ARCHIVE.stat().st_size,
    'archive_sha256': archive_hash,
}, ensure_ascii=False, indent=2))
