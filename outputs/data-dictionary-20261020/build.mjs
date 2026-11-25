import fs from 'node:fs/promises';
import {createRequire} from 'node:module';
import {pathToFileURL} from 'node:url';
const require=createRequire('C:/Users/MINH/.cache/codex-runtimes/codex-primary-runtime/dependencies/node/package.json');
const {Workbook,SpreadsheetFile}=await import(pathToFileURL(require.resolve('@oai/artifact-tool')).href);
const rows=JSON.parse((await fs.readFile('../../docs/data-dictionary/data_dictionary.json','utf8')).replace(/^\uFEFF/,''));
const w=Workbook.create(),s=w.worksheets.add('DATA DICTIONARY');
s.showGridLines=false;
s.getRange('A1:H67').format.font={name:'Calibri',size:11,color:'#000000'};
s.getRange('A1:H1').merge();s.getRange('A1').values=[['DATA DICTIONARY - THÔNG TIN DỮ LIỆU THƯƠNG MẠI ĐIỆN TỬ']];
s.getRange('A1:H1').format={fill:'#44546A',font:{name:'Calibri',size:13,bold:true,color:'#FFFFFF'},rowHeight:35,verticalAlignment:'center'};
s.getRange('A2:H2').merge();s.getRange('A2').values=[['64 trường chuẩn. Nguồn API là ứng viên thu thập, chưa xác nhận JSON path bằng phản hồi shop thật. Tên Shopee/TikTok trong Ghi chú là trường simulator hoặc ánh xạ ETL hiện tại.']];s.getRange('A2:H2').format={rowHeight:32,wrapText:true,font:{name:'Calibri',size:11,italic:true,color:'#44546A'}};
s.getRange('A3:H3').values=[['STT','Nguồn','Tên bảng','Mô tả bảng','Tên cột','Mô tả cột','Loại dữ liệu','Ghi chú']];
s.getRange('A3:H3').format={fill:'#44546A',font:{name:'Calibri',size:13,bold:true,color:'#FFFFFF'},horizontalAlignment:'center',verticalAlignment:'center',rowHeight:30,borders:{preset:'all',style:'thin',color:'#000000'}};
const desc={orders:'Dòng hàng của đơn hàng tại mỗi lần cập nhật. Khóa đề xuất: platform + shop_id + order_id + source_line_id. Không cộng lặp tổng tiền cấp đơn.',inventory_snapshot:'Ảnh chụp tồn kho theo SKU và thời điểm. Nguồn thật từ WMS/ERP. Consumer hiện lưu bản mới nhất mỗi SKU/ngày, chưa tách kho.',inventory_movement:'Sổ giao dịch biến động kho. Mỗi bản ghi là một giao dịch. Khóa: hệ thống nguồn + movement_id. Simulator sinh được, không mặc định đã nạp đầy đủ vào warehouse.'};
const sources={orders:'API Shopee / TikTok Shop và Data Simulator',inventory_snapshot:'WMS / ERP, chính sách kho và Data Simulator',inventory_movement:'WMS / ERP và Data Simulator'};
let start=4,index=1;
for(const dataset of ['orders','inventory_snapshot','inventory_movement']){
 const group=rows.filter(r=>r.dataset===dataset),end=start+group.length-1;
 const vals=group.map((r,i)=>[i===0?index:null,i===0?sources[dataset]:null,i===0?dataset:null,i===0?desc[dataset]:null,r.field,r.description,r.data_type,`${r.requirement}. Ví dụ: ${r.example}.\nNguồn thật: ${r.real_source_candidate}.\nGiả lập/ETL: Shopee = ${r.simulator_shopee}; TikTok = ${r.simulator_tiktok}.\n${r.validation_and_missing_policy}.`]);
 s.getRange(`A${start}:H${end}`).values=vals;
 s.getRange(`A${start}:H${end}`).format={fill:index%2?'#D9E2F3':'#FFFFFF',wrapText:true,verticalAlignment:'top',rowHeight:112,borders:{preset:'all',style:'thin',color:'#7F8EA3'}};
 for(const col of ['A','B','C','D']){s.getRange(`${col}${start}:${col}${end}`).merge();s.getRange(`${col}${start}:${col}${end}`).format.verticalAlignment='top';}
 s.getRange(`A${start}:C${end}`).format.font={name:'Calibri',size:11,bold:true};s.getRange(`A${start}:C${end}`).format.horizontalAlignment='center';
 for(let i=0;i<group.length;i++){const r=group[i],n=start+i;const text=vals[i][7];const lines=text.split('\n').reduce((a,t)=>a+Math.max(1,Math.ceil(t.length/88)),0);s.getRange(`E${n}:H${n}`).format.rowHeight=Math.max(100,lines*15+12);}
 start=end+1;index++;
}
const widths=[7,19,24,32,32,45,21,91];widths.forEach((v,i)=>s.getRange(`${String.fromCharCode(65+i)}1:${String.fromCharCode(65+i)}67`).format.columnWidth=v);
s.freezePanes.freezeRows(3);
const notes=[
 'Quy ước: NULL = chưa biết, khác với 0. Kiểu decimal là kiểu đích đề xuất; timestamp chuẩn UTC, ngày nghiệp vụ Asia/Ho_Chi_Minh. Bộ giả lập hiện dùng float cho tiền.',
 'Giới hạn hiện tại: ETL mặc định 0 cho nhiều khoản phí. Currency và cờ COD chưa được giữ đầy đủ trong schema chung. ON_HOLD và PARTIALLY_SHIPPING của TikTok chưa có trong validator. Cần chỉnh ánh xạ trước tích hợp thật.',
 'Cơ sở dự án: data_simulator/data_simulator.py; warehouse/etl/transform.py; warehouse/etl/data_validation.py; ingestion/consumer_inventory_to_postgres.py. Chi tiết trong DATA_DICTIONARY_VI.md.',
 'Nguồn API: https://partner.tiktokshop.com/docv2/page/order-api-overview và https://partner.tiktokshop.com/docv2/page/get-order-list-202309 . Tài liệu Shopee https://open.shopee.com/documents/v2/v2.order.get_order_detail chưa truy cập được trong phiên đối chiếu. Chưa gọi API shop thật.'
];
notes.forEach((t,i)=>{const n=69+i;s.getRange(`A${n}:H${n}`).merge();s.getRange(`A${n}`).values=[[t]];s.getRange(`A${n}:H${n}`).format={wrapText:true,rowHeight:i===3?36:30,font:{name:'Calibri',size:11,color:'#44546A'},verticalAlignment:'center'};});
w.recalculate();
console.log((await w.inspect({kind:'region',sheetId:'DATA DICTIONARY',range:'E4:H6',maxChars:1200,tableMaxRows:3,tableMaxCols:4})).ndjson);
console.log((await w.inspect({kind:'match',searchTerm:'#REF!|#DIV/0!|#VALUE!|#NAME\\?|#NUM!',options:{useRegex:true,maxResults:10},maxChars:500})).ndjson);
for(const [name,range] of [['top','A1:H7'],['inventory','A42:H45'],['movement','A59:H62']]){const p=await w.render({sheetName:s.name,range,scale:1,format:'png'});await fs.writeFile(`preview-${name}.png`,new Uint8Array(await p.arrayBuffer()));}
await (await SpreadsheetFile.exportXlsx(w)).save('Data_Dictionary_Ecommerce.xlsx');
console.log('Exported 64 fields in 3 table groups');


