"""Document, verify and package the new diagrams in the user's reading folder."""
from pathlib import Path
from collections import Counter
import csv
import hashlib
from html.parser import HTMLParser
import html
import json
import os
from urllib.parse import unquote, urlsplit
import xml.etree.ElementTree as ET
import zipfile
from PIL import Image

ROOT=Path(__file__).resolve().parents[1]
BUNDLE=ROOT/'Tai_lieu_chat_DATN_2026-11-08'
OUT=BUNDLE/'09_So_do_thiet_ke_2026-11-09'
SOURCE=ROOT/'docs/08-so-do-thiet-ke'
spec=json.loads((SOURCE/'design-spec.json').read_text(encoding='utf-8'))
native=OUT/'MO_BANG_STARUML_10_SO_DO_PIPELINE.mdj'
project=json.loads(native.read_text(encoding='utf-8'))
readback=json.loads((OUT/'Kiem_tra_doc_lai_bang_StarUML.json').read_text(encoding='utf-8'))

def native_path(p):
    value=os.path.abspath(str(p))
    return '\\\\?\\'+value if os.name=='nt' and not value.startswith('\\\\?\\') else value
def sha(p):
    with open(native_path(p),'rb') as stream:return hashlib.sha256(stream.read()).hexdigest()
def size(p):return os.stat(native_path(p)).st_size
def esc(value):return html.escape(str(value),quote=True)

placements={
 '01':'Mục 2.1 — Bài toán và phạm vi xây dựng hệ thống',
 '02':'Mục 2.3 — Kiến trúc và sự phân chia trách nhiệm',
 '03':'Mục 2.13 — Tổ chức mã nguồn và môi trường triển khai; dẫn lại tại 3.1',
 '04':'Mục 2.5 — Kho dữ liệu và quy trình ETL',
 '05':'Mục 2.6 — Toàn vẹn dữ liệu và phục hồi bằng phiên bản và checkpoint',
 '06':'Mục 2.6 — Bổ sung thiết kế backfill; dẫn bằng chứng tại 3.13',
 '07':'Mục 2.5 — Sau phần mô tả độ hạt và các bảng kho dữ liệu',
 '08':'Mục 2.5 — Sau star schema đơn hàng; dẫn lại tại 2.7 và 2.10',
 '09':'Mục 2.8–2.9 — Tổ chức huấn luyện, quản lý thí nghiệm và triển khai',
 '10':'Mục 2.11.1 — Giám sát dịch vụ và dữ liệu; dẫn bằng chứng tại 3.13',
}
types={'usecase':'UML Use Case','component':'UML Component','deployment':'UML Deployment','activity':'UML Activity','sequence':'UML Sequence','erd':'ERD'}
lines=['# Bộ sơ đồ thiết kế pipeline end-to-end — 09/11/2026','',
 'Đã rà soát mã triển khai và dựng 10 sơ đồ. Các hình mô tả thiết kế hiện hành; không phải lượt nghiệm thu runtime mới. Bằng chứng runtime đối chiếu vẫn là các mốc 04/11 và 08/11/2026. Power BI do sinh viên tự hoàn thiện.','',
 '## Mở và sử dụng','',
 '- Mở `00_MO_XEM_10_SO_DO.html` để xem hình, nội dung cần giải thích và vị trí chèn trong báo cáo.',
 '- Mở `MO_BANG_STARUML_10_SO_DO_PIPELINE.mdj` bằng StarUML để chỉnh sửa các phần tử UML/ERD. Đã cài StarUML 8.0.2 tại `C:/Users/MINH/AppData/Local/Programs/StarUML/StarUML.exe`.',
 '- Trong StarUML, mở từng model ở Model Explorer rồi mở diagram bên trong; nhấn Ctrl+9 để vừa cửa sổ. Tệp gồm phần tử và quan hệ thật, không phải các ảnh được dán vào canvas.',
 '- Thư mục `01_Anh_SVG_de_chen_bao_cao`: vector dựng độc lập từ đặc tả thiết kế, có thể chèn vào Word và phóng to.',
 '- Thư mục `02_Anh_PNG_do_phan_giai_cao`: ảnh dựng độc lập ở kích thước gấp đôi để chèn khi Word không nhận SVG.',
 '- Thư mục `03_Anh_xuat_goc_StarUML_co_dau_dung_thu`: ảnh SVG do StarUML thực sự xuất, giữ nguyên dấu dùng thử. Không xóa dấu hay kích hoạt giấy phép trái phép.',
 '- Hai nhóm ảnh có cùng nội dung thiết kế nhưng có thể khác bố cục/chữ do hai bộ dựng. Sửa tệp StarUML không tự cập nhật ảnh dựng độc lập; cần xuất lại từ StarUML hoặc cập nhật đặc tả và dựng lại.',
 '', '## Phần nào nên có sơ đồ?', '',
 'Các sơ đồ dưới đây là lựa chọn phù hợp đề tài, không phải cẩm nang bắt buộc phải có đúng 10 hình. Ưu tiên kiến trúc, ETL, checkpoint/backfill, mô hình dữ liệu và giám sát. Use case, deployment và sequence giúp người phản biện đọc thiết kế rõ hơn.','',
 '| Mã | Sơ đồ | Loại | Vị trí đề xuất |', '|---|---|---|---|']
for d in spec['diagrams']:
    code=d['slug'][:2]
    lines.append(f"| {code} | {d['title'].split(' · ',1)[1]} | {types[d['kind']]} | {placements[code]} |")
lines += ['', '## Nội dung cần giải thích dưới mỗi hình','']
for d in spec['diagrams']:
    lines += [f"### {d['title']}",'',d['purpose'],'',
      'Nguồn đối chiếu: '+', '.join('`'+r+'`' for r in d['refs'])+'.','']
lines += ['## Những mô tả cũ cần thay thế hoặc ghi lại phạm vi','',
 '- Không vẽ NiFi vào đường chạy chính đã nghiệm thu; phần này đang hoãn.',
 '- Không vẽ Shopee/TikTok API thật khi đầu vào live hiện là simulator.',
 '- Không vẽ drift → retrain → đăng ký → deploy thành vòng tự động hoàn toàn.',
 '- Không ghi một mô hình như LightGBM là champion hoặc chèn WAPE cũ không có bằng chứng phù hợp.',
 '- Kho đơn hàng có 6 dimensions và hạt dòng sản phẩm; khóa duy nhất là platform + order_id + source_line_id.',
 '- Tồn kho hiện là kho trung tâm, snapshot mới nhất theo SKU/ngày; không cộng snapshot nhiều ngày thành stock hiện tại.',
 '- Không gộp CSV ẩn danh vào khóa đơn live; giữ origin/fingerprint và hạt dòng nguồn.',
 '- Phân biệt offset Kafka sau lưu raw với checkpoint ETL sau thành công. Không nhận exactly-once toàn hệ thống.',
 '- Backfill mặc định dry-run, có khóa chung, giới hạn khoảng ngày/object và audit; không reset checkpoint.',
 '- Alertmanager gửi webhook POST /alerts; GET /events chỉ để đọc lại bằng chứng. Không có email/Slack hoặc tự retrain được thêm trong sơ đồ.',
 '', '## Chèn vào báo cáo và khai báo AI','',
 'Chèn bằng Insert → Pictures → This Device, chọn SVG hoặc PNG. Giữ tỷ lệ; sơ đồ rộng như kiến trúc/sequence có thể cần trang ngang. Dùng Caption của Word để đánh số tiếp theo số hình hiện có và cập nhật danh mục hình; không tự nhận các hình đã được chèn vào bản Word.','',
 'Nguồn hình có thể ghi: “Thiết kế từ mã nguồn đồ án, có AI hỗ trợ dựng và kiểm tra sơ đồ, ngày 09/11/2026.” Sinh viên cần rà soát và tự giải thích trước khi nộp; không khai báo đã tự vẽ thủ công nếu không đúng quá trình.','',
 '## Kiểm tra đã lưu','',
 'Xem `Kiem_tra_doc_lai_bang_StarUML.json` và `Kiem_tra_va_danh_muc_tep_so_do.json`. Các kiểm tra này xác nhận tệp và hình, không được cộng thành số bài kiểm thử chức năng pipeline mới.','',
 '## Nguồn công cụ','',
 '- Tải chính thức và điều kiện dùng thử: https://staruml.io/download/ và https://staruml.io/help/',
 '- API tạo phần tử: https://docs.staruml.io/developing-extensions/creating-deleting-and-modifying-elements/',
 '- Scripting API và serialization MDJ: https://docs.staruml.io/developing-extensions/scripting-api/',
 '- CLI xuất ảnh: https://docs.staruml.io/user-guide/cli-command-line-interface/','']
guide='\n'.join(lines)
(OUT/'Huong_dan_chon_va_chen_so_do_vao_bao_cao.md').write_text(guide,encoding='utf-8')
(SOURCE/'README.md').write_text(guide+'\n\nThư mục bàn giao trong dự án: `Tai_lieu_chat_DATN_2026-11-08/09_So_do_thiet_ke_2026-11-09/`.\n',encoding='utf-8')

cards=[]
for d in spec['diagrams']:
    slug=d['slug'];code=slug[:2]
    svg=f'01_Anh_SVG_de_chen_bao_cao/{slug}.svg'
    png=f'02_Anh_PNG_do_phan_giai_cao/{slug}.png'
    native_svg=f'03_Anh_xuat_goc_StarUML_co_dau_dung_thu/{slug}.svg'
    cards.append(f'<section id="so-do-{code}"><h2>{esc(d["title"])}</h2><p class="type">{types[d["kind"]]} · {esc(placements[code])}</p><p>{esc(d["purpose"])}</p><p><a href="{svg}">Mở SVG</a> · <a href="{png}">Mở PNG</a> · <a href="{native_svg}">Ảnh xuất gốc StarUML (dùng thử)</a></p><a href="{svg}"><img src="{svg}" alt="{esc(d["title"])}"></a><details><summary>Mã nguồn đã đối chiếu</summary><ul>'+''.join('<li>'+esc(r)+'</li>' for r in d['refs'])+'</ul></details></section>')
css='''body{font:17px/1.65 "Segoe UI",Arial,sans-serif;color:#17334d;background:#f0f4f9;margin:0}main{max-width:1320px;margin:auto;padding:36px 28px 70px}h1{line-height:1.25}h2{font-size:25px}a{color:#096ab4;text-decoration:none}a:hover{text-decoration:underline}section,.note{background:white;border:1px solid #d6e1ed;border-radius:12px;padding:24px;margin:24px 0}section img{width:100%;height:auto;border:1px solid #e1e8ef}.note{border-left:5px solid #2b78ad;background:#e8f2fb}.type{color:#526d85;font-size:14px}details{font-size:14px;color:#526d85}.links{display:flex;gap:20px;flex-wrap:wrap}@media print{body{background:white}main{padding:0}section{break-before:page;border:0;padding:0}section img{max-height:85vh;object-fit:contain}.note,details,.links{display:none}}'''
index=f'''<!doctype html><html lang="vi"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>10 sơ đồ thiết kế pipeline — 09/11/2026</title><style>{css}</style></head><body><main><a href="../00_MO_MUC_LUC_TAI_LIEU.html">← Mục lục toàn bộ tài liệu</a><h1>10 sơ đồ thiết kế pipeline end-to-end</h1><p>Đinh Công Minh · Bổ sung ngày 09/11/2026 · Power BI do sinh viên tự hoàn thiện.</p><div class="note"><strong>Chỉnh sửa:</strong> mở tệp StarUML bên dưới, chọn diagram trong Model Explorer và nhấn Ctrl+9. <strong>Đọc/chèn báo cáo:</strong> dùng SVG hoặc PNG từ cùng đặc tả thiết kế, dựng độc lập. Ảnh xuất trực tiếp từ StarUML được giữ riêng với dấu dùng thử.</div><div class="links"><a href="MO_BANG_STARUML_10_SO_DO_PIPELINE.mdj">Mở dự án StarUML chỉnh sửa được</a><a href="Huong_dan_chon_va_chen_so_do_vao_bao_cao.md">Hướng dẫn và vị trí chèn</a><a href="BO_10_SO_DO_THIET_KE_2026-11-09.zip">Gói ZIP toàn bộ sơ đồ</a></div><ol>{''.join(f'<li><a href="#so-do-{d["slug"][:2]}">{esc(d["title"])}</a></li>' for d in spec['diagrams'])}</ol>{''.join(cards)}<p>Các hình mô tả thiết kế đối chiếu mã nguồn, không thay thế bằng chứng nghiệm thu runtime. Nguồn công cụ: <a href="https://staruml.io/download/">StarUML chính thức</a>.</p></main></body></html>'''
(OUT/'00_MO_XEM_10_SO_DO.html').write_text(index,encoding='utf-8')

# Structural round-trip checks on the actual native project.
ids={};references=[];duplicates=[];counts=Counter()
def visit(value):
    if isinstance(value,dict):
        if '_id' in value:
            if value['_id'] in ids:duplicates.append(value['_id'])
            ids[value['_id']]=value
        if '_type' in value:counts[value['_type']]+=1
        if '$ref' in value:references.append(value['$ref'])
        for child in value.values():visit(child)
    elif isinstance(value,list):
        for child in value:visit(child)
visit(project)
missing_refs=sorted(set(references)-ids.keys())
expected_types={'usecase':'UMLUseCaseDiagram','component':'UMLComponentDiagram','deployment':'UMLDeploymentDiagram','activity':'UMLActivityDiagram','sequence':'UMLSequenceDiagram','erd':'ERDDiagram'}
actual={d['name']:d for d in readback['diagrams']}
assert len(actual)==10 and not duplicates and not missing_refs
for d in spec['diagrams']:
    assert actual[d['slug']]['type']==expected_types[d['kind']]
    assert actual[d['slug']]['views']>0
    for source in d['refs']:assert (ROOT/source).is_file(),source
    for directory in ['01_Anh_SVG_de_chen_bao_cao','03_Anh_xuat_goc_StarUML_co_dau_dung_thu']:
        ET.parse(OUT/directory/(d['slug']+'.svg'))
    with Image.open(OUT/'02_Anh_PNG_do_phan_giai_cao'/(d['slug']+'.png')) as picture:
        assert picture.size==(d['width']*2,d['height']*2)

# Update the existing reading index with an explicit new dated section.
main_index=BUNDLE/'00_MO_MUC_LUC_TAI_LIEU.html'
main_text=main_index.read_text(encoding='utf-8')
addition='<section id="bo-sung-so-do-20261109" class="panel"><h2>9. Sơ đồ thiết kế — bổ sung 09/11/2026</h2><p>10 sơ đồ UML/ERD; tệp StarUML chỉnh sửa được, ảnh SVG/PNG, nguồn thiết kế và hướng dẫn chèn báo cáo.</p><p><a href="09_So_do_thiet_ke_2026-11-09/00_MO_XEM_10_SO_DO.html">Mở bộ sơ đồ và hướng dẫn</a> · <a href="09_So_do_thiet_ke_2026-11-09/MO_BANG_STARUML_10_SO_DO_PIPELINE.mdj">Dự án StarUML</a></p></section>'
if 'id="bo-sung-so-do-20261109"' not in main_text:
    main_text=main_text.replace('<input id="search"',addition+'<input id="search"',1)
    main_index.write_text(main_text,encoding='utf-8')

class LocalLinks(HTMLParser):
    def __init__(self):super().__init__();self.links=[]
    def handle_starttag(self,tag,attrs):
        self.links.extend(value for name,value in attrs if name in ['href','src'] and value)
local_links_checked=0
for page in [OUT/'00_MO_XEM_10_SO_DO.html',main_index]:
    parser=LocalLinks();parser.feed(page.read_text(encoding='utf-8'))
    for value in parser.links:
        parsed=urlsplit(value)
        if parsed.scheme or parsed.netloc or not parsed.path:continue
        target=page.parent/unquote(parsed.path)
        if target.name=='BO_10_SO_DO_THIET_KE_2026-11-09.zip':continue
        assert os.path.exists(native_path(target)),str(target)
        local_links_checked+=1

report={
 'status':'PASS','date':'2026-11-09','scope':'Kiểm tra tệp sơ đồ; không phải lượt kiểm thử pipeline mới',
 'staruml_version':readback['version'],'staruml_cli_roundtrip':True,'staruml_gui_open_observed':True,
 'expected_diagrams':10,'actual_diagrams':len(actual),'diagram_types':dict(Counter(d['type'] for d in actual.values())),
 'duplicate_element_ids':duplicates,'unresolved_native_references':missing_refs,'native_elements_with_ids':len(ids),
 'independent_svg_xml_valid':10,'native_svg_xml_valid':10,'png_dimensions_match_spec':10,
 'reading_index_local_links_checked':local_links_checked,'reading_index_broken_local_links':[],
 'native_exports_watermark_preserved':True,'independent_images_are_not_modified_native_exports':True,
 'source_files':[{'path':p,'sha256':sha(ROOT/p)} for p in sorted(set(p for d in spec['diagrams'] for p in d['refs']))],
 'installer':{'official_url':'https://files.staruml.io/releases-v8/StarUML-Setup-8.0.2.exe','authenticode_status_observed':'Valid','publisher_observed':'MKLabs Co.,Ltd.','sha256':'acc4820ab574dbf08fc8fe3b1bc25b2210a2262757569533127f3e59c3d642f1'},
 'visual_review':'Đã xem bảng tổng quan 10 hình, kiểm tra riêng các hình quy trình, ERD và kiến trúc; quan sát dự án mở và Fit to Window trong StarUML.',
 'runtime_pipeline_tests_run':False,'power_bi_modified':False,
 'native_source_templates':['scripts/design_diagrams_factory.ejs','docs/08-so-do-thiet-ke/tao_so_do_bang_StarUML.ejs'],
 'manifest_excludes_itself_and_archive':True,
 'files':[{'path':p.relative_to(OUT).as_posix(),'bytes':p.stat().st_size,'sha256':sha(p)} for p in sorted(OUT.rglob('*')) if p.is_file() and p.suffix not in ['.zip'] and p.name not in ['Kiem_tra_va_danh_muc_tep_so_do.json','BO_10_SO_DO_THIET_KE_2026-11-09.zip.sha256.txt']],
}
(OUT/'Kiem_tra_va_danh_muc_tep_so_do.json').write_text(json.dumps(report,ensure_ascii=False,indent=2),encoding='utf-8')

archive=OUT/'BO_10_SO_DO_THIET_KE_2026-11-09.zip'
files=[p for p in sorted(OUT.rglob('*')) if p.is_file() and p!=archive and p.name!=archive.name+'.sha256.txt']
with zipfile.ZipFile(archive,'w',zipfile.ZIP_DEFLATED,compresslevel=6) as z:
    for p in files:z.write(p,arcname=(Path(OUT.name)/p.relative_to(OUT)).as_posix())
with zipfile.ZipFile(archive) as z:assert z.testzip() is None
(OUT/(archive.name+'.sha256.txt')).write_text(sha(archive)+'  '+archive.name+'\n',encoding='utf-8')

# Refresh the overall file inventory after adding this dated section.
catalog=BUNDLE/'02_DANH_SACH_TOAN_BO_TEP.csv'
with catalog.open(encoding='utf-8-sig',newline='') as stream:
    old_rows=list(csv.reader(stream));header=old_rows[0];old={r[0]:r for r in old_rows[1:]}
new_rows=[]
for directory,_,names in os.walk(native_path(BUNDLE)):
    for name in sorted(names):
        full=str(Path(directory)/name)
        p=Path(full[4:] if full.startswith('\\\\?\\') else full)
        if p==catalog:continue
        rel=p.relative_to(BUNDLE).as_posix()
        row=old.get(rel,[rel,'Sơ đồ thiết kế — bổ sung 09/11/2026','Tệp thiết kế / mục lục / kiểm tra mới','','','',''])
        row[5]=sha(p);row[6]=str(size(p));new_rows.append(row)
with catalog.open('w',encoding='utf-8-sig',newline='') as stream:
    writer=csv.writer(stream);writer.writerow(header);writer.writerows(sorted(new_rows))

print(json.dumps({'status':'PASS','diagrams':len(actual),'native_types':report['diagram_types'],'native_ids':len(ids),'roundtrip_messages':readback['messages'],'zip_bytes':archive.stat().st_size,'zip_crc':'PASS','folder':str(OUT)},ensure_ascii=True))
