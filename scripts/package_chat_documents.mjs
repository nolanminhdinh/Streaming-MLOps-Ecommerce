import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import { pathToFileURL } from 'node:url';

// Copies only: source documents and running services are never changed.
const root = process.cwd();
const folderName = 'Tai_lieu_chat_DATN_2026-11-08';
const bundle = path.join(root, folderName);
const ns = p => path.toNamespacedPath(path.resolve(p));
const exists = p => fs.existsSync(ns(p));
if (exists(bundle)) throw new Error('Thư mục đích đã tồn tại; dừng để không ghi đè.');
const markedPath = 'C:/Program Files/WindowsApps/OpenAI.CodexPrimaryRuntime.v26-1105-1297-0_26.1105.1297.0_x64__3k8sg7r9htsxt/dependencies/node/node_modules/marked/lib/marked.esm.js';
const { marked } = await import(pathToFileURL(markedPath).href);
const abs = p => path.isAbsolute(p) ? path.resolve(p) : path.resolve(root, p);
const key = p => abs(p).toLowerCase();
const posix = p => p.replaceAll('\\', '/');
const esc = s => String(s).replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const url = p => posix(p).split('/').map(s => encodeURIComponent(s)).join('/');
const hash = p => crypto.createHash('sha256').update(fs.readFileSync(ns(p))).digest('hex');
const hashCache = new Map();
const sourceHash = p => { const k = key(p); if (!hashCache.has(k)) hashCache.set(k, hash(p)); return hashCache.get(k); };
const metadata = new Map();
const raw = new Map();
const preferred = new Map();
const previews = new Map();
const primary = [];
const linkChecks = [];
const omittedLinks = new Map();
const rebasedLinks = new Map();
const generated = new Set();
const mkdir = p => fs.mkdirSync(ns(p), {recursive:true});
const write = (rel, text) => { const target=path.join(bundle, rel); mkdir(path.dirname(target)); fs.writeFileSync(ns(target), text, 'utf8'); generated.add(rel); };
function walk(dir) {
  const out=[];
  for (const entry of fs.readdirSync(ns(dir), {withFileTypes:true})) {
    const p=path.join(dir,entry.name);
    if (entry.isSymbolicLink() || ['.git','__pycache__','.pytest_cache','node_modules'].includes(entry.name)) continue;
    if (entry.isDirectory()) out.push(...walk(p)); else if (entry.isFile()) out.push(p);
  }
  return out.sort();
}
function copy(source, rel, title='', group='Bản lưu nguồn') {
  const sourcePath=abs(source);
  if (!exists(sourcePath)) throw new Error(`Không tìm thấy: ${sourcePath}`);
  const target=path.join(bundle,rel);
  mkdir(path.dirname(target));
  if (exists(target)) throw new Error(`Trùng tên đích: ${rel}`);
  fs.copyFileSync(ns(sourcePath),ns(target));
  metadata.set(rel,{source:sourcePath,title,group,mode:'Bản sao nguyên nội dung',source_sha256:sourceHash(sourcePath)});
  return rel;
}
function snapshot(source) {
  const sourcePath=abs(source), k=key(sourcePath);
  if (raw.has(k)) return raw.get(k);
  const relative=path.relative(root,sourcePath);
  if (relative.startsWith('..') || path.isAbsolute(relative)) throw new Error('Nguồn snapshot phải nằm trong dự án.');
  const rel=path.join('08_Luu_tru_nguon_va_bang_chung',relative);
  copy(sourcePath,rel,path.basename(sourcePath)); raw.set(k,rel);
  return rel;
}
const groups = [
  ['01_Doc_truoc_Bao_cao_va_danh_gia','Đọc trước: báo cáo mới nhất và đánh giá'],
  ['02_Tai_lieu_ban_cung_cap','Cẩm nang, tiêu chí và nội dung đã cung cấp'],
  ['03_Qua_trinh_sua_va_hoan_thien','Quá trình sửa và hoàn thiện đồ án'],
  ['04_Kiem_thu_va_du_lieu_ket_qua','Kiểm thử và dữ liệu kết quả'],
  ['05_Huong_dan_va_thiet_ke','Hướng dẫn vận hành và tài liệu thiết kế'],
  ['06_Bao_cao_cac_phien_ban_truoc','Các phiên bản trước — đọc để đối chiếu'],
  ['07_Du_lieu_tu_dien_va_Power_BI','Dữ liệu gốc, từ điển dữ liệu và bản Power BI đã lưu'],
];
const groupTitle = g => groups.find(x=>x[0]===g)?.[1] ?? g;
function add(g, filename, source, title, status='Tham khảo') {
  const rel=path.join(g,filename), sourcePath=abs(source);
  copy(sourcePath,rel,title,groupTitle(g));
  preferred.set(key(sourcePath),rel);
  primary.push({group:g,rel,source:sourcePath,title,status});
}
const G=groups.map(x=>x[0]);
mkdir(bundle);
// Retain source layouts so original relative evidence links can be followed.
for (const directory of ['docs','output/docx','output/review-2026-11-07','output/e2e-2026-11-04','output/pipeline-completion-2026-11-08','outputs/data-dictionary-20261020','notebooks']) {
  for (const file of walk(abs(directory))) snapshot(file);
}

add(G[0],'01_BAO_CAO_DATN_MOI_NHAT_Trong_tam_pipeline_08-11-2026.docx','output/docx/Bao_cao_DATN_Dinh_Cong_Minh_2026-11-08_pipeline.docx','Báo cáo ĐATN mới nhất — trọng tâm pipeline','MỚI NHẤT');
add(G[0],'01_BAO_CAO_DATN_MOI_NHAT_Trong_tam_pipeline_08-11-2026.pdf','tmp/pipeline-completion-20261108/thesis-pipeline.pdf','Báo cáo ĐATN mới nhất — bản PDF để đọc','MỚI NHẤT');
add(G[0],'01_BAO_CAO_DATN_MOI_NHAT_Trong_tam_pipeline_08-11-2026.md','output/docx/Bao_cao_DATN_Dinh_Cong_Minh_2026-11-08_pipeline.md','Báo cáo ĐATN mới nhất — nội dung văn bản','MỚI NHẤT');
add(G[0],'02_Danh_gia_theo_bang_cham_diem_8_tieu_chi_08-11-2026.md','docs/07-bao-cao-do-an/doi-chieu-rubric-cham-diem-2026-11-08.md','Đối chiếu 8 tiêu chí và tính điểm theo trọng số','ĐÁNH GIÁ MỚI');
add(G[0],'03_Danh_gia_lai_pipeline_Cam_nang_va_viec_con_thieu_08-11-2026.md','docs/07-bao-cao-do-an/danh-gia-lai-pipeline-va-cam-nang-2026-11-08.md','Đánh giá theo cẩm nang và các việc còn thiếu','ĐÁNH GIÁ MỚI');
add(G[0],'04_Doi_chieu_day_du_29_cau_hoi_Data_Pipeline_08-11-2026.md','docs/07-bao-cao-do-an/doi-chieu-29-cau-hoi-pipeline-2026-11-08.md','Đối chiếu đầy đủ 29 câu hỏi Data Pipeline','ĐÁNH GIÁ MỚI');
add(G[0],'05_Tong_ket_hoan_thien_va_nghiem_thu_pipeline_08-11-2026.md','docs/07-bao-cao-do-an/hoan-thien-pipeline-2026-11-08.md','Tổng kết hoàn thiện và nghiệm thu pipeline','KẾT QUẢ MỚI');

add(G[1],'01_Cam_nang_Do_an_tot_nghiep_Khoa_hoc_du_lieu_BAN_GOC.pdf','C:/Users/MINH/Downloads/Cẩm nang ĐATN - KHDL.pdf','Cẩm nang ĐATN Khoa học dữ liệu — bản gốc','TÀI LIỆU GỐC');
add(G[1],'02_Bang_cham_diem_8_tieu_chi_va_trong_so_BAN_GOC.jpg','C:/Users/MINH/Downloads/1794155408313_3816641782313057918_3816641782313057918_7fc8a0f710694872ae8407975475e30c.jpg','Ảnh bảng chấm điểm cụ thể — 8 tiêu chí','TÀI LIỆU GỐC');
add(G[1],'03_Bo_29_cau_hoi_va_goi_y_Data_Pipeline_BAN_GOC.txt','C:/Users/MINH/.codex/attachments/d8461348-6f32-4af9-a072-1aca4110c408/Pasted text.txt','Bộ 29 câu hỏi và gợi ý Data Pipeline — nguyên văn','TÀI LIỆU GỐC');
add(G[1],'04_Mo_ta_muc_tieu_du_an_Da_luu_truoc_do.txt','C:/Users/MINH/.codex/attachments/25698b17-a5c5-4674-8b0d-62ecdd901563/Pasted text.txt','Tài liệu mô tả mục tiêu dự án đã lưu trước đó','BỐI CẢNH CŨ');
add(G[1],'05_Cam_nang_Ban_trich_van_ban_de_tra_cuu.txt','tmp/handbook-review-20261107/handbook.txt','Cẩm nang — bản trích văn bản để tìm kiếm','BẢN TRÍCH');

add(G[2],'01_Danh_gia_ban_dau_theo_cam_nang_TRUOC_KHAC_PHUC_07-11-2026.md','output/review-2026-11-07/Danh-gia-DATN-theo-cam-nang.md','Đánh giá ban đầu trước đợt khắc phục','LỊCH SỬ — 07/11');
add(G[2],'02_Khac_phuc_baseline_WAPE_va_ho_so_cam_nang_07-11-2026.md','docs/07-bao-cao-do-an/khac-phuc-cam-nang-2026-11-07.md','Khắc phục baseline, WAPE và hồ sơ cẩm nang','LỊCH SỬ — 07/11');
add(G[2],'03_Kiem_tra_CSV_nguyen_nhan_WAPE_va_thu_nghiem_du_bao_08-11-2026.md','docs/07-bao-cao-do-an/cai-thien-du-bao-2026-11-08.md','Đối soát CSV, nguyên nhân WAPE và thử nghiệm dự báo','NGHIÊN CỨU — 08/11');
add(G[2],'04_Bao_cao_xay_dung_kho_du_lieu_01-11-2026.md','docs/07-bao-cao-do-an/bao-cao-xay-dung-kho-du-lieu-2026-11-01.md','Báo cáo xây dựng kho dữ liệu','THAM KHẢO — 01/11');
add(G[2],'04_Bao_cao_xay_dung_kho_du_lieu_01-11-2026.pdf','tmp/warehouse-report-20261101/report-qa.pdf','Báo cáo kho dữ liệu — bản PDF đã lưu','THAM KHẢO — 01/11');
add(G[2],'05_Cap_nhat_van_hanh_pipeline_01-11-2026.md','docs/04-pipeline-updates/lan-10-core-operations-2026-11-01.md','Cập nhật vận hành pipeline ngày 01/11','LỊCH SỬ — 01/11');
add(G[2],'06_Cap_nhat_nguon_du_lieu_Power_BI_01-11-2026.md','docs/04-pipeline-updates/lan-11-powerbi-postgres-import-2026-11-01.md','Cập nhật nguồn PostgreSQL cho Power BI','THAM KHẢO — SINH VIÊN TỰ HOÀN THIỆN');

add(G[3],'01_MO_KET_QUA_KIEM_THU_PIPELINE_08-11-2026.html','output/pipeline-completion-2026-11-08/Ket_qua_kiem_thu.html','Mở bảng kết quả và dữ liệu từng bài kiểm thử','KẾT QUẢ CHỐT');
add(G[3],'02_Huong_dan_doc_ket_qua_kiem_thu_pipeline_08-11-2026.md','output/pipeline-completion-2026-11-08/README.md','Hướng dẫn đọc kết quả kiểm thử pipeline','KẾT QUẢ CHỐT');
add(G[3],'03_Tom_tat_so_lieu_kiem_thu_pipeline_08-11-2026.json','output/pipeline-completion-2026-11-08/summary.json','Tóm tắt số liệu kiểm thử pipeline','DỮ LIỆU CHỐT');
add(G[3],'04_Kiem_tra_tinh_toan_ven_goi_ban_giao_08-11-2026.json','docs/07-bao-cao-do-an/evidence/pipeline-completion-2026-11-08.json','Kiểm tra tính toàn vẹn gói bàn giao','DỮ LIỆU CHỐT');
add(G[3],'05_TOAN_BO_KET_QUA_TEST_PIPELINE_08-11-2026.zip','output/Ket_qua_pipeline_2026-11-08.zip','Gói đầy đủ kết quả test pipeline ngày 08/11','GÓI KẾT QUẢ');
add(G[3],'06_Bao_cao_kiem_thu_end_to_end_04-11-2026.md','output/e2e-2026-11-04/Bao-cao-kiem-thu-end-to-end.md','Báo cáo kiểm thử end-to-end ngày 04/11','BẰNG CHỨNG — 04/11');
add(G[3],'06_Bao_cao_kiem_thu_end_to_end_04-11-2026.html','output/e2e-2026-11-04/Bao-cao-kiem-thu-end-to-end.html','Báo cáo kiểm thử end-to-end — HTML gốc','BẰNG CHỨNG — 04/11');
add(G[3],'07_TOAN_BO_BANG_CHUNG_END_TO_END_04-11-2026.zip','output/e2e-2026-11-04/Bang-chung-end-to-end-2026-11-04.zip','Gói bằng chứng end-to-end ngày 04/11','GÓI KẾT QUẢ');

add(G[4],'01_Huong_dan_chay_pipeline_chinh_va_xu_ly_su_co.md','docs/05-how-to-run/core-pipeline.md','Hướng dẫn chạy pipeline chính và xử lý sự cố');
add(G[4],'02_Kich_ban_demo_va_bao_ve_do_an.md','docs/05-how-to-run/kich-ban-bao-ve-demo.md','Kịch bản demo và bảo vệ đồ án');
add(G[4],'03_Huong_dan_su_dung_giao_dien_demo.md','docs/05-how-to-run/demo-web.md','Hướng dẫn giao diện demo');
add(G[4],'04_Kien_truc_tong_the_he_thong.md','docs/00-architecture-overview/architecture.md','Kiến trúc tổng thể hệ thống');
add(G[4],'05_Tong_hop_bao_cao_ky_thuat_theo_chuong.md','docs/07-bao-cao-do-an/bao-cao-tong-hop-do-an.md','Tổng hợp báo cáo kỹ thuật theo chương');
const chapters=[
 ['01-bai-toan-va-du-lieu.md','06_Chuong_1_Bai_toan_va_du_lieu.md','Chương 1 — Bài toán và dữ liệu'],
 ['02-ingestion-kafka-nifi-minio.md','07_Chuong_2_Thu_nap_Kafka_va_MinIO.md','Chương 2 — Thu nạp, Kafka và MinIO'],
 ['03-etl-va-data-warehouse.md','08_Chuong_3_ETL_va_kho_du_lieu.md','Chương 3 — ETL và kho dữ liệu'],
 ['04-feature-engineering-va-machine-learning.md','09_Chuong_4_Dac_trung_va_hoc_may.md','Chương 4 — Đặc trưng và học máy'],
 ['05-serving-inventory-va-monitoring.md','10_Chuong_5_API_ton_kho_va_giam_sat.md','Chương 5 — API, tồn kho và giám sát'],
 ['06-power-bi-va-tong-hop-thuc-nghiem.md','11_Chuong_6_Power_BI_va_thuc_nghiem_Tham_khao.md','Chương 6 — Power BI và thực nghiệm, tài liệu tham khảo'],
];
for (const [src,name,title] of chapters) add(G[4],name,'docs/07-bao-cao-do-an/'+src,title,'TÀI LIỆU THEO CHƯƠNG');
add(G[4],'12_Huong_dan_giam_sat_va_canh_bao.md','monitoring/README.md','Hướng dẫn giám sát và cảnh báo');
add(G[4],'13_Tai_lieu_kho_du_lieu_va_co_chat_luong.md','warehouse/README.md','Tài liệu kho dữ liệu và cờ chất lượng');
add(G[4],'14_Tai_lieu_du_an_tong_quan.docx','output/docx/Tai_lieu_du_an_Streaming_MLOps_Dinh_Cong_Minh.docx','Tài liệu dự án tổng quan — Word');
add(G[4],'14_Tai_lieu_du_an_tong_quan.pdf','tmp/project-document/render/Tai_lieu_du_an_Streaming_MLOps_Dinh_Cong_Minh.pdf','Tài liệu dự án tổng quan — PDF đã lưu');

for (const [base,label,title] of [
 ['Bao_cao_DATN_Dinh_Cong_Minh','01_Bao_cao_Word_truoc_dot_sua_baseline_WAPE','Báo cáo Word trước đợt sửa baseline/WAPE'],
 ['Bao_cao_DATN_Dinh_Cong_Minh_2026-11-07','02_Bao_cao_sau_sua_baseline_WAPE_07-11-2026','Báo cáo sau sửa baseline/WAPE ngày 07/11'],
 ['Bao_cao_DATN_Dinh_Cong_Minh_2026-11-08','03_Bao_cao_sau_nghien_cuu_du_bao_TRUOC_chot_pipeline_08-11-2026','Báo cáo sau nghiên cứu dự báo, trước chốt pipeline'],
]) {
  add(G[5],label+'.docx','output/docx/'+base+'.docx',title,'PHIÊN BẢN CŨ');
  if (base!=='Bao_cao_DATN_Dinh_Cong_Minh') add(G[5],label+'.md','output/docx/'+base+'.md',title+' — văn bản','PHIÊN BẢN CŨ');
}
add(G[5],'02_Bao_cao_sau_sua_baseline_WAPE_07-11-2026.pdf','tmp/remediation-20261107/thesis-revision.pdf','Báo cáo sau sửa baseline/WAPE — PDF','PHIÊN BẢN CŨ');
add(G[5],'03_Bao_cao_sau_nghien_cuu_du_bao_TRUOC_chot_pipeline_08-11-2026.pdf','tmp/forecast-improvement-20261108/thesis-revision.pdf','Báo cáo nghiên cứu dự báo — PDF','PHIÊN BẢN CŨ');

add(G[6],'01_Du_lieu_goc_Shopee_202608141448.csv','C:/Users/MINH/Downloads/sho_order_anonymized_202608141448.csv','CSV gốc Shopee đã đối soát','DỮ LIỆU GỐC');
add(G[6],'02_Du_lieu_goc_TikTok_Shop_202608141408.csv','C:/Users/MINH/Downloads/tts_order_anonymized_202608141408.csv','CSV gốc TikTok Shop đã đối soát','DỮ LIỆU GỐC');
add(G[6],'03_Tu_dien_du_lieu_Ecommerce.xlsx','outputs/data-dictionary-20261020/Data_Dictionary_Ecommerce.xlsx','Từ điển dữ liệu Ecommerce — Excel');
add(G[6],'04_Tu_dien_du_lieu_Tieng_Viet.md','docs/data-dictionary/DATA_DICTIONARY_VI.md','Từ điển dữ liệu tiếng Việt');
add(G[6],'05_Dac_ta_mo_hinh_du_lieu_va_data_dictionary.md','docs/02-warehouse-etl-feature-store/data_dictionary_data_model_specification.md','Đặc tả mô hình dữ liệu và data dictionary');
add(G[6],'06_Dac_ta_quan_ly_du_lieu.xlsx','docs/02-warehouse-etl-feature-store/data_management_specification.xlsx','Đặc tả quản lý dữ liệu — Excel');
add(G[6],'07_EDA_du_lieu_lich_su_Da_doi_soat.ipynb','notebooks/historical_eda_verified.ipynb','Notebook EDA dữ liệu lịch sử đã đối soát');
add(G[6],'08_Power_BI_Ban_sao_hien_co_SINH_VIEN_TU_HOAN_THIEN.pbix','powerbi/Report_PowerBI_Postgres.pbix','Power BI — bản sao hiện có, sinh viên tự hoàn thiện','CHƯA NGHIỆM THU POWER BI');

// The canonical Markdown is an alias of the final pipeline text, unlike the old canonical Word.
if (sourceHash(abs('output/docx/Bao_cao_DATN_Dinh_Cong_Minh.md'))===sourceHash(abs('output/docx/Bao_cao_DATN_Dinh_Cong_Minh_2026-11-08_pipeline.md'))) preferred.set(key('output/docx/Bao_cao_DATN_Dinh_Cong_Minh.md'),preferred.get(key('output/docx/Bao_cao_DATN_Dinh_Cong_Minh_2026-11-08_pipeline.md')));
// Readable HTML reports forward to the unchanged evidence layout.
for (const doc of primary.filter(x=>path.extname(x.rel)==='.html')) {
  const target=raw.get(key(doc.source));
  if (!target) throw new Error('Thiếu bản lưu HTML gốc.');
  const href=url(path.relative(path.dirname(doc.rel),target));
  write(doc.rel,`<!doctype html><html lang="vi"><meta charset="utf-8"><meta http-equiv="refresh" content="0;url=${esc(href)}"><title>${esc(doc.title)}</title><p><a href="${esc(href)}">Mở ${esc(doc.title)}</a></p></html>`);
  metadata.get(doc.rel).mode='Trang mở bản HTML nguyên gốc trong bộ bằng chứng';
}

const allowExtensions=new Set(['.md','.txt','.pdf','.docx','.xlsx','.csv','.json','.jsonl','.xml','.png','.jpg','.jpeg','.svg','.html','.sql','.py','.yml','.yaml','.ipynb','.zip','.ps1','.mjs','.js','.bat','.pbix']);
function localLink(href, source) {
  let text=href.trim();
  if (!text || text.startsWith('#') || /^(https?:|mailto:|tel:|data:|plugin:|app:|codex:|javascript:)/i.test(text)) return null;
  let suffix=''; const at=text.search(/[?#]/); if (at>=0) {suffix=text.slice(at);text=text.slice(0,at);}
  try { text=decodeURIComponent(text); } catch {}
  if (/^file:\/\//i.test(text)) { try {text=decodeURIComponent(new URL(text).pathname).replace(/^\/(?=[A-Z]:)/i,'');} catch {return null;} }
  return {source:path.isAbsolute(text)?abs(text):path.resolve(path.dirname(source),text),suffix};
}
function ensureTarget(p) {
  const k=key(p); if (preferred.has(k)) return preferred.get(k); if (raw.has(k)) return raw.get(k);
  // Some older documents still refer to the project's previous Downloads location.
  const projectMarker=path.basename(root).toLowerCase()+path.sep;
  const markerAt=p.toLowerCase().indexOf(projectMarker);
  if (markerAt>=0 && path.relative(root,p).startsWith('..')) {
    const candidate=path.join(root,p.slice(markerAt+projectMarker.length));
    if (exists(candidate) || posix(candidate).toLowerCase().endsWith('/docs/test-pipeline/bao_cao_kiem_thu_hieu_nang_lan_1.md')) { rebasedLinks.set(p,candidate); return ensureTarget(candidate); }
  }
  const oldBenchmark=path.join(root,'docs','test-pipeline','bao_cao_kiem_thu_hieu_nang_lan_1.md');
  if (key(p)===key(oldBenchmark)) {
    const candidate=path.join(root,'docs','03-testing-and-benchmark','bao_cao_kiem_thu_hieu_nang_lan_1.md');
    rebasedLinks.set(p,candidate); return ensureTarget(candidate);
  }
  const rel=path.relative(root,p);
  if (!rel.startsWith('..') && !path.isAbsolute(rel) && exists(p)) {
    const stat=fs.statSync(ns(p));
    if (stat.isFile() && (allowExtensions.has(path.extname(p).toLowerCase()) || path.basename(p)==='Dockerfile') && stat.size<=25*1024*1024) return snapshot(p);
    if (stat.isDirectory() && ['monitoring/grafana/provisioning','powerbi/data'].includes(posix(rel))) {
      const files=walk(p);
      if (files.reduce((n,f)=>n+fs.statSync(ns(f)).size,0)<=25*1024*1024) for (const f of files) snapshot(f);
    }
    if (stat.isDirectory() && exists(path.join(bundle,'08_Luu_tru_nguon_va_bang_chung',rel))) return path.join('08_Luu_tru_nguon_va_bang_chung',rel);
  }
  return null;
}
function rewriteHref(href, source, destination, htmlMode=false) {
  const found=localLink(href,source); if (!found) return href;
  let target=ensureTarget(found.source);
  if (!target) { omittedLinks.set(key(found.source),found.source); return href; }
  if (htmlMode && path.extname(target).toLowerCase()==='.md') target=previewFor(metadata.get(target)?.source||found.source,target);
  linkChecks.push(target);
  const relative=path.relative(path.dirname(destination),target);
  return url(relative)+found.suffix;
}
function previewFor(source,rel) {
  const k=key(source);
  if (!previews.has(k)) previews.set(k,{source:abs(source),rel:rel.replace(/\.md$/i,'.doc.html')});
  return previews.get(k).rel;
}
function rewriteMarkdown(text,source,rel,htmlMode=false) {
  return text.replace(/(!?\[[^\]\n]*\]\()([^\n]*?)(\))/g,(whole,open,body,close)=>{
    const parts=body.trim().match(/^(<[^>]+>|.*?)(\s+["'][^"']*["'])?$/);
    if (!parts) return whole;
    const href=parts[1];
    const unwrapped=href.startsWith('<')?href.slice(1,-1):href;
    return open+rewriteHref(unwrapped,source,rel,htmlMode)+(parts[2]||'')+close;
  }).replace(/((?:src|href)=["'])([^"']+)(["'])/g,(whole,open,href,close)=>open+rewriteHref(href,source,rel,htmlMode)+close);
}
const css=`:root{color-scheme:light}*{box-sizing:border-box}body{margin:0;background:#f2f5f9;color:#182435;font:16px/1.65 "Segoe UI",Arial,sans-serif}main{max-width:1120px;margin:auto;padding:34px 30px 70px}h1{line-height:1.25;font-size:32px}h2{margin-top:35px;color:#123a60}h3{color:#234a70}a{color:#0866b2;text-decoration:none}a:hover{text-decoration:underline}.note{background:#e7f0fa;border-left:4px solid #3577b5;padding:14px 18px;border-radius:6px;margin:20px 0}.history{background:#fff1d9;border-left-color:#bf8a20}.panel,article{background:white;border:1px solid #dce4ee;border-radius:12px;padding:24px;margin-top:18px;box-shadow:0 2px 7px #18243508}table{border-collapse:collapse;width:100%;font-size:14px;display:block;overflow-x:auto}th,td{border:1px solid #dce4ee;padding:9px 12px;text-align:left;vertical-align:top}th{background:#edf3fa}tr:nth-child(even){background:#f9fbfd}.tag{font-size:11px;font-weight:700;letter-spacing:.3px;color:#476178;white-space:nowrap}img{max-width:100%;height:auto}pre{background:#eef2f6;border-radius:7px;padding:16px;overflow:auto;font-size:13px}code{font-size:.9em}blockquote{border-left:3px solid #adc4dc;padding-left:18px;color:#476178}input{width:100%;padding:13px;border:1px solid #b3c5d9;border-radius:7px;font:inherit}small{color:#52667b}li{margin:6px 0}nav{margin-bottom:18px}section{scroll-margin-top:15px}`;
const page=(title,body)=>`<!doctype html><html lang="vi"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>${esc(title)}</title><style>${css}</style></head><body><main>${body}</main></body></html>`;
for (const doc of primary) {
  const ext=path.extname(doc.rel).toLowerCase();
  if (ext==='.md') {
    write(doc.rel,rewriteMarkdown(fs.readFileSync(ns(doc.source),'utf8'),doc.source,doc.rel));
    metadata.get(doc.rel).mode='Bản đọc: chỉ đổi đường dẫn liên kết; bản nguồn nguyên nội dung được giữ ở mục 08';
    previewFor(doc.source,doc.rel);
  } else if (ext==='.txt') {
    doc.preview=doc.rel.replace(/\.txt$/i,'.doc.html');
    const back=url(path.relative(path.dirname(doc.preview),'00_MO_MUC_LUC_TAI_LIEU.html'));
    write(doc.preview,page(doc.title,`<nav><a href="${back}">← Mục lục</a></nav><h1>${esc(doc.title)}</h1><article><pre style="white-space:pre-wrap">${esc(fs.readFileSync(ns(doc.source),'utf8'))}</pre></article>`));
  }
}
// Also render archived Markdown, so links between chapters remain readable offline.
for (const [sourceKey,rel] of raw) if (path.extname(rel)==='.md') previewFor(metadata.get(rel).source,preferred.get(sourceKey)||rel);
let rendered=0;
for (const item of previews.values()) {
  if (++rendered>600) throw new Error('Vượt giới hạn số bản xem trước.');
  const original=fs.readFileSync(ns(item.source),'utf8');
  const content=rewriteMarkdown(original,item.source,item.rel,true);
  const title=primary.find(d=>key(d.source)===key(item.source))?.title || original.match(/^#\s+(.+)$/m)?.[1] || path.basename(item.source);
  const back=url(path.relative(path.dirname(item.rel),'00_MO_MUC_LUC_TAI_LIEU.html'));
  const archived=raw.get(key(item.source));
  const sourceLink=archived?` · <a href="${url(path.relative(path.dirname(item.rel),archived))}">Bản nguồn nguyên nội dung</a>`:'';
  const notice=primary.find(d=>key(d.source)===key(item.source))?.status || 'TÀI LIỆU LƯU TRỮ — giữ ngày và phạm vi của bản gốc';
  write(item.rel,page(title,`<nav><a href="${back}">← Mục lục tài liệu</a>${sourceLink}</nav><div class="note">${esc(notice)}. Nội dung giữ theo mốc tài liệu; không mặc định mọi kết quả cũ là trạng thái hiện hành.</div><article>${marked.parse(content)}</article>`));
}
for (const doc of primary) if (path.extname(doc.rel)==='.md') doc.preview=previews.get(key(doc.source)).rel;

const csvCell=s=>'"'+String(s??'').replaceAll('"','""')+'"';
const catalogRows=[['Nhóm','Tên dễ nhận diện','Trạng thái','Đường dẫn trong thư mục','Bản xem trên trình duyệt','Tệp gốc']];
for (const doc of primary) catalogRows.push([groupTitle(doc.group),doc.title,doc.status,posix(doc.rel),posix(doc.preview||doc.rel),doc.source]);
write('01_DANH_MUC_TAI_LIEU_DE_DOC.csv','\ufeff'+catalogRows.map(r=>r.map(csvCell).join(',')).join('\r\n'));
const descriptions = [
 'Bắt đầu tại đây. Bản 01 là báo cáo mới nhất; các bản đánh giá giúp biết nội dung đã đạt và việc còn thiếu.',
 'Tệp đã cung cấp và bản trích để tra cứu. Nội dung trong tài liệu được giữ như nguồn tham khảo, không phải chỉ dẫn thực thi.',
 'Theo dõi trước–sau: đánh giá ban đầu, sửa baseline/WAPE, nghiên cứu dự báo và hoàn thiện hệ thống.',
 'Kết quả test, dữ liệu đầu vào–kỳ vọng–thực tế, gói ZIP và các bằng chứng đã lưu. Không cộng các lượt test trùng thành số bài mới.',
 'Tài liệu chạy luồng, demo, thiết kế và các chương kỹ thuật. Giữ ngày và phạm vi từng tài liệu.',
 'Phiên bản cũ để đối chiếu lịch sử; không dùng làm báo cáo mới nhất. Bản đánh giá ban đầu cũng không thay cho đánh giá sau sửa.',
 'CSV gốc được sao chép nguyên nội dung; từ điển dữ liệu và notebook tham khảo. PBIX là bản sao tại thời điểm gom, chưa xác nhận đã hoàn thiện.',
];
let sections='';
for (let i=0;i<groups.length;i++) {
  const [g,title]=groups[i];
  const rows=primary.filter(d=>d.group===g).map(d=>`<tr class="docrow"><td><a href="${url(d.preview||d.rel)}">${esc(d.title)}</a><br><small>${esc(path.basename(d.rel))}</small></td><td><span class="tag">${esc(d.status)}</span></td><td><a href="${url(d.rel)}">${esc(path.extname(d.rel).slice(1).toUpperCase())}</a></td></tr>`).join('');
  sections+=`<section id="nhom-${i+1}" class="panel"><h2>${i+1}. ${esc(title)}</h2><p>${descriptions[i]}</p><table><thead><tr><th>Tài liệu</th><th>Mốc / trạng thái</th><th>Tệp</th></tr></thead><tbody>${rows}</tbody></table></section>`;
}
const rawIndex=path.join('08_Luu_tru_nguon_va_bang_chung','00_MUC_LUC_BAN_LUU_NGUON.html');
const rawRows=[...raw.values()].sort().map(rel=>`<tr><td><a href="${url(path.relative(path.dirname(rawIndex),rel))}">${esc(posix(path.relative('08_Luu_tru_nguon_va_bang_chung',rel)))}</a></td><td>${fs.statSync(ns(path.join(bundle,rel))).size.toLocaleString('vi-VN')}</td></tr>`).join('');
write(rawIndex,page('Mục lục bản lưu nguồn và bằng chứng',`<nav><a href="../00_MO_MUC_LUC_TAI_LIEU.html">← Mục lục chính</a></nav><h1>Bản lưu nguồn và bằng chứng</h1><p>Giữ cấu trúc và tên kỹ thuật để truy vết và giữ liên kết. Các bản dễ nhận diện nằm trong nhóm 01–07.</p><input id="search" placeholder="Tìm tên tệp…"><table><thead><tr><th>Tệp</th><th>Byte</th></tr></thead><tbody>${rawRows}</tbody></table><script>document.getElementById('search').addEventListener('input',e=>{const q=e.target.value.toLocaleLowerCase('vi');document.querySelectorAll('tbody tr').forEach(r=>r.hidden=!r.textContent.toLocaleLowerCase('vi').includes(q));});</script>`));
write('00_MO_MUC_LUC_TAI_LIEU.html',page('Toàn bộ tài liệu đồ án trong cuộc trao đổi',`<header><small>ĐINH CÔNG MINH · ĐỒ ÁN TỐT NGHIỆP · TỔNG HỢP 08/11/2026</small><h1>Toàn bộ tài liệu để đọc lại</h1><p>Mục tiêu: pipeline dữ liệu chạy end-to-end. Power BI do sinh viên tự hoàn thiện.</p></header><div class="note"><strong>Thứ tự đọc gợi ý:</strong> Báo cáo mới nhất → Đánh giá 8 tiêu chí → Đánh giá theo cẩm nang → Đối chiếu 29 câu → Kết quả kiểm thử. Bấm tên tài liệu để đọc bản HTML/PDF; cột “Tệp” mở bản định dạng gốc.</div><div class="note history"><strong>Phân biệt phiên bản:</strong> Báo cáo chốt pipeline mang nhãn MỚI NHẤT. Điểm số và vấn đề trong bản đánh giá ban đầu thuộc thời điểm trước sửa. Các kết quả dự báo chỉ là nghiên cứu trong phạm vi đã công bố.</div><input id="search" placeholder="Tìm theo tên, nội dung tên tệp hoặc trạng thái…">${sections}<section class="panel"><h2>8. Bản lưu nguồn và toàn bộ bằng chứng</h2><p>Các tệp gốc trong dự án, biểu đồ, nhật ký, dữ liệu kết quả test và tài liệu tham khảo được giữ theo cấu trúc nguồn.</p><p><a href="${url(rawIndex)}">Mở danh mục đầy đủ các bản lưu</a> · <a href="01_DANH_MUC_TAI_LIEU_DE_DOC.csv">Danh mục tài liệu dễ đọc (CSV)</a> · <a href="02_DANH_SACH_TOAN_BO_TEP.csv">Toàn bộ tệp và mã kiểm tra (CSV)</a> · <a href="03_KIEM_TRA_GOI_TAI_LIEU.json">Kết quả kiểm tra bộ tài liệu</a></p></section><script>document.getElementById('search').addEventListener('input',e=>{const q=e.target.value.toLocaleLowerCase('vi');document.querySelectorAll('.docrow').forEach(r=>r.hidden=!r.textContent.toLocaleLowerCase('vi').includes(q));});</script>`));

write('00_DOC_TOI_TRUOC.txt',`BỘ TÀI LIỆU ĐỒ ÁN — TỔNG HỢP CUỘC TRAO ĐỔI 08/11/2026\n\nMỞ ĐẦU TIÊN: 00_MO_MUC_LUC_TAI_LIEU.html\n\n01: Báo cáo mới nhất và các bản đánh giá.\n02: Cẩm nang, bảng chấm điểm và nội dung đã cung cấp.\n03: Quá trình đánh giá, sửa baseline/WAPE và hoàn thiện.\n04: Kết quả kiểm thử và gói dữ liệu.\n05: Hướng dẫn vận hành, demo và thiết kế.\n06: Các phiên bản báo cáo trước, chỉ để đối chiếu.\n07: CSV gốc, từ điển dữ liệu, notebook và bản Power BI hiện có.\n08: Bản lưu nguyên nội dung và cấu trúc kỹ thuật để giữ liên kết.\n\nBáo cáo mới nhất là bản có tên BAO_CAO_DATN_MOI_NHAT_Trong_tam_pipeline.\nBản HTML của tài liệu Markdown chỉ đổi cách trình bày và đường dẫn đọc; không cập nhật lại kết quả nghiên cứu.\nNguồn gốc/tên mới/mã SHA-256 được ghi trong các CSV danh mục.\nCác tệp gốc của dự án không bị di chuyển hoặc đổi tên. Power BI không được chỉnh sửa.\nĐây là bộ tài liệu và bằng chứng đã lưu, không phải bản xuất nguyên văn toàn bộ tin nhắn trong chat.\nTài liệu kỹ thuật tham khảo cũ được giữ trong mục 08, không mặc nhiên phản ánh trạng thái hiện hành.\n`);

// Verify exact source copies and every rewritten link before packaging.
const copyMismatches=[];
for (const [rel,record] of metadata) if (record.mode==='Bản sao nguyên nội dung' && hash(path.join(bundle,rel))!==record.source_sha256) copyMismatches.push(rel);
const brokenRewritten=[...new Set(linkChecks)].filter(rel=>!exists(path.join(bundle,rel)));
if (copyMismatches.length || brokenRewritten.length) throw new Error(JSON.stringify({copyMismatches,brokenRewritten}));
const fileRows=[['Tên tệp trong bộ','Nhóm / nội dung','Kiểu bản sao','Nguồn gốc','SHA256 nguồn','SHA256 trong bộ','Dung lượng byte']];
for (const file of walk(bundle)) {
  const rel=path.relative(bundle,file), info=metadata.get(rel);
  fileRows.push([posix(rel),info?.title||'Mục lục hoặc bản xem HTML',info?.mode||'Tạo mục lục / bản xem',info?.source||'',info?.source_sha256||'',hash(file),fs.statSync(ns(file)).size]);
}
write('02_DANH_SACH_TOAN_BO_TEP.csv','\ufeff'+fileRows.map(r=>r.map(csvCell).join(',')).join('\r\n'));
const result={status:'PASS',bundle:bundle,primary_documents:primary.length,source_layout_files:raw.size,markdown_previews:rendered,exact_source_copies_verified:[...metadata.values()].filter(x=>x.mode==='Bản sao nguyên nội dung').length,rewritten_link_checks:linkChecks.length,broken_rewritten_links:brokenRewritten,copy_mismatches:copyMismatches,old_links_rebased:Object.fromEntries(rebasedLinks),source_links_not_bundled:[...omittedLinks.values()].sort(),source_link_note:'Liên kết chưa đóng gói được giữ nguyên từ tài liệu cũ; danh sách này không phải lỗi mới của pipeline.',originals_unchanged_by_operation:true,power_bi:'Chỉ sao chép tệp hiện có; không chỉnh sửa hoặc nghiệm thu',chat_transcript_exported:false};
write('03_KIEM_TRA_GOI_TAI_LIEU.json',JSON.stringify(result,null,2));
const finalFiles=walk(bundle);
console.log(JSON.stringify({status:result.status,bundle,primary_documents:primary.length,files:finalFiles.length,bytes:finalFiles.reduce((n,p)=>n+fs.statSync(ns(p)).size,0),markdown_previews:rendered,rewritten_link_checks:linkChecks.length,source_links_not_bundled:omittedLinks.size},null,2));
