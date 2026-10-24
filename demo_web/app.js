'use strict';
const $ = id => document.getElementById(id);
const esc = value => String(value ?? '—').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const number = value => value == null ? '—' : new Intl.NumberFormat('vi-VN',{maximumFractionDigits:2}).format(Number(value));
const timestamp = value => value ? new Date(value * 1000).toLocaleString('vi-VN',{day:'2-digit',month:'2-digit',hour:'2-digit',minute:'2-digit',second:'2-digit'}) : '—';
const empty = text => `<div class="empty-state">${esc(text)}</div>`;
const pill = (text, style='light') => `<span class="pill ${style}">${esc(text)}</span>`;
let state = {}, selectedStage = 'kafka', initializedSku = false, busy = false, toastTimer;
const titles = {overview:'Tổng quan hệ thống',stream:'Luồng dữ liệu trực tiếp',forecast:'Dự báo & quản trị tồn kho',scenarios:'Các tình huống mô phỏng'};
const stages = [
  {key:'source',name:'Nguồn dữ liệu',icon:'⇢',description:'Simulator tạo payload Shopee và TikTok. Mỗi đợt có mã WEB… và hai SKU riêng. Hai snapshot tồn kho được gửi trên topic inventory.logs.',evidence:()=>state.run?.run_id ? `Đợt ${state.run.run_id}; ${number(state.run.sent_orders)} bản tin đơn đã được Kafka xác nhận.` : 'Chưa phát đợt demo. Mở Luồng dữ liệu để bắt đầu.'},
  {key:'kafka',name:'Kafka',icon:'≋',description:'Broker nhận bản tin, phân partition và cấp offset. ACK là bằng chứng Kafka đã ghi; trạng thái kết nối chỉ kiểm tra cổng TCP.',evidence:()=>`${number(state.run?.sent_orders || 0)} ACK đơn; ${number(state.run?.sent_inventory || 0)} ACK tồn kho trong đợt hiện tại.`},
  {key:'minio',name:'MinIO Lake',icon:'▱',description:'Consumer lưu dữ liệu thô thành Parquet, phân vùng theo sàn và ngày nghiệp vụ. Bằng chứng đếm bản tin theo mã đơn của đợt demo.',evidence:()=>state.lake?.ok ? `${number(state.lake.raw_rows)} bản tin của đợt trong ${number(state.lake.objects?.length)} tệp Parquet.` : 'Chưa đọc được Data Lake. Không có số liệu thay thế.'},
  {key:'worker',name:'ETL & kiểm định',icon:'⇄',description:'Worker tự chuẩn hóa, kiểm định, cách ly lỗi và nạp dữ liệu. Checkpoint chỉ tăng sau khi ETL và xuất CSV thành công.',evidence:()=>state.worker?.ok ? `Heartbeat ${timestamp(state.worker.checked_at)}; ${number(state.lake?.processed_objects)} object của đợt đã checkpoint.` : 'Worker chưa sẵn sàng hoặc heartbeat đã cũ.'},
  {key:'postgres',name:'PostgreSQL',icon:'▤',description:'Star Schema lưu dòng đơn và snapshot tồn kho. Khóa nghiệp vụ và upsert xử lý bản tin gửi trùng. Bảng chi tiết lọc đúng mã đợt demo.',evidence:()=>state.warehouse?.ok ? `${number(state.warehouse.run_count)} dòng đơn của đợt; ${number(state.warehouse.orders)} đơn trong toàn bộ kho streaming.` : 'Chưa truy vấn được PostgreSQL.'},
  {key:'model',name:'Model & API',icon:'⌁',description:'FastAPI phục vụ model hiện có và tính cảnh báo nhập hàng. Dự báo dùng nguồn lịch sử theo model; đợt streaming mới không tự huấn luyện lại model.',evidence:()=>{const m=state.api?.model?.data;return m ? `${m.champion_algorithm} · v${m.version} · ${m.model_source}.` : 'Chưa đọc được metadata từ API phục vụ.'}}
];
function showView(name){
  if(!titles[name]) name='overview';
  document.querySelectorAll('.view').forEach(el=>el.hidden=el.id!==`view-${name}`);
  document.querySelectorAll('[data-view]').forEach(el=>{const active=el.dataset.view===name;el.classList.toggle('active',active);el.setAttribute('aria-current',active?'page':'false');});
  $('page-title').textContent=titles[name];
  if(location.hash!==`#${name}`) history.replaceState(null,'',`#${name}`);
  window.scrollTo({top:0,behavior:'instant'});
}
document.querySelectorAll('[data-view],[data-go]').forEach(el=>el.addEventListener('click',()=>showView(el.dataset.view||el.dataset.go)));
window.addEventListener('hashchange',()=>showView(location.hash.slice(1)));
showView(location.hash.slice(1));
document.addEventListener('keydown',event=>{
  if(['INPUT','SELECT','TEXTAREA','BUTTON'].includes(document.activeElement.tagName)||event.ctrlKey||event.altKey||event.metaKey)return;
  if(['1','2','3','4'].includes(event.key))showView(Object.keys(titles)[Number(event.key)-1]);
});
$('present-button').addEventListener('click',()=>{document.body.classList.toggle('presentation');$('present-button').textContent=document.body.classList.contains('presentation')?'⛶ Thoát tập trung':'⛶ Tập trung';});
function toast(message){$('toast').textContent=message;$('toast').hidden=false;clearTimeout(toastTimer);toastTimer=setTimeout(()=>$('toast').hidden=true,7500);}
function metric(label,value,note,icon='↗'){return `<div class="metric"><div class="metric-top">${esc(label)}<span>${icon}</span></div><div class="metric-value">${number(value)}</div><div class="metric-note">${esc(note)}</div></div>`;}
function kv(label,value){return `<div class="kv"><span>${esc(label)}</span><strong>${esc(value)}</strong></div>`;}
function renderPipeline(){
  $('pipeline').innerHTML=stages.map(stage=>{
    const ok=stage.key==='source'?!!state.run?.sent_orders:state.services?.[stage.key]?.ok;
    const label=stage.key==='source'?(ok?'Đã phát dữ liệu':'Chờ đợt demo'):(state.loading?'Đang kiểm tra':ok?(stage.key==='kafka'?'Cổng kết nối mở':'Đã xác nhận'):'Chưa sẵn sàng');
    return `<button class="stage ${selectedStage===stage.key?'selected':''}" data-stage="${stage.key}" aria-pressed="${selectedStage===stage.key}"><span class="stage-icon">${stage.icon}</span><strong>${stage.name}</strong><small><span class="tiny-dot ${ok?'':'muted'}"></span>${label}</small></button>`;
  }).join('');
  $('pipeline').querySelectorAll('[data-stage]').forEach(el=>el.addEventListener('click',()=>{selectedStage=el.dataset.stage;renderPipeline();}));
  const selected=stages.find(s=>s.key===selectedStage);
  $('stage-detail').innerHTML=`<strong>${esc(selected.name)}.</strong> ${esc(selected.description)}<br><strong>Quan sát:</strong> ${esc(selected.evidence())}`;
}
function render(){
  const run=state.run||{}, lake=state.lake||{}, wh=state.warehouse||{}, worker=state.worker||{};
  const ready=state.api?.ready?.data||{}, model=state.api?.model?.data;
  const serviceCount=Object.values(state.services||{}).filter(service=>service.ok).length;
  $('connection-label').textContent=state.loading?'Đang kiểm tra kết nối':`${serviceCount}/6 kiểm tra sẵn sàng`;
  $('connection-dot').className=`tiny-dot ${serviceCount===6?'':serviceCount?'amber':'muted'}`;
  $('updated-at').textContent=state.checked_at?`Lần đọc ${timestamp(state.checked_at)} · tự cập nhật`:'Đang kết nối các thành phần…';
  const metrics=metric('Kafka đã nhận',run.sent_orders||0,'Bản tin đơn · gồm gửi lại','≋')+metric('Đã lưu Data Lake',lake.ok?lake.raw_rows:null,'Bản tin raw của đợt','▱')+metric('Đơn vào Warehouse',wh.ok?wh.run_count:null,`Mục tiêu ${run.valid_orders||0} dòng hợp lệ`,'▤')+metric('Đã cách ly lỗi',lake.ok?lake.rejected_count:null,`Gửi lần đầu: ${run.invalid_orders||0} bản tin lỗi`,'◇');
  $('overview-metrics').innerHTML=metrics;$('run-metrics').innerHTML=metrics;
  renderPipeline();
  $('model-badge').className=`pill ${state.api?.ready?.ok?'light':'amber'}`;
  $('model-badge').textContent=state.api?.ready?.ok?'Sẵn sàng phục vụ':'Chưa sẵn sàng';
  $('model-summary').innerHTML=model?kv('Thuật toán',model.champion_algorithm)+kv('Phiên bản / giai đoạn',`v${model.version} · ${model.stage}`)+kv('Nguồn model',model.model_source||'Chưa xác định')+kv('Nguồn huấn luyện',model.training_data_origin||'Chưa xác định')+kv('CV WAPE',model.metrics?.cv_wape!=null?`${number(model.metrics.cv_wape)}%`:'Chưa công bố')+kv('Lịch sử gần nhất',ready.history_end||'Chưa có'):empty('Chưa kết nối được API model. Khởi động FastAPI và kiểm tra MLflow.');
  $('readiness-note').textContent=ready.history_end?`Lịch sử đến ${ready.history_end} · ${number(ready.history_age_days)} ngày tuổi${ready.history_stale?' · Dữ liệu đã cũ.':'.'} Giai đoạn model: ${model?.stage||'chưa xác định'}.`:'Chưa xác nhận được lịch sử và model.';
  $('run-id').textContent=run.run_id||'Chưa có đợt demo';$('run-message').textContent=run.message||(state.services?.kafka?.ok?'Bấm “Phát đợt mới” để bắt đầu.':'Kafka chưa kết nối. Chạy start_demo_web.bat sau khi Docker Desktop sẵn sàng.');
  const rawExpected=run.sent_orders||0, objects=lake.objects||[];
  const allProcessed=lake.ok&&objects.length>0&&lake.processed_objects===objects.length&&lake.raw_rows>=rawExpected;
  if(run.status==='published'&&allProcessed&&wh.ok&&wh.run_count===run.valid_orders&&(!run.invalid_orders||lake.rejected_count>0))$('run-message').textContent='Đã đối chiếu đủ dữ liệu: raw, đơn hợp lệ, kiểm định và checkpoint sau xuất CSV.';
  const steps=[
    [!!run.sent_orders,'Kafka xác nhận bản tin',`${number(run.sent_orders||0)} ACK đơn`],
    [lake.ok&&lake.raw_rows>=rawExpected&&rawExpected>0,'Consumer lưu Parquet',`${number(lake.ok?lake.raw_rows:null)} bản tin`],
    [wh.ok&&wh.run_count===run.valid_orders,'ETL nạp đơn hợp lệ',`${number(wh.ok?wh.run_count:null)} / ${number(run.valid_orders||0)}`],
    [lake.ok&&(run.invalid_orders?lake.rejected_count>0:!!run.run_id),'Validation cách ly bản tin lỗi',run.invalid_orders?`${number(lake.ok?lake.rejected_count:null)} lần ghi lỗi`:'Không yêu cầu lỗi'],
    [allProcessed,'Checkpoint sau ETL & xuất CSV',`${number(lake.processed_objects||0)} / ${objects.length} object`]
  ];
  $('run-progress').innerHTML=steps.map(([done,label,note],i)=>`<div class="progress-step ${done?'done':''}"><span>${done?'✓':String(i+1).padStart(2,'0')}</span>${label}<small>${esc(note)}</small></div>`).join('');
  if(run.replay_count){
    const verified=run.replay_baseline_complete&&run.status==='published'&&allProcessed&&wh.ok&&wh.run_count===run.replay_baseline;
    $('replay-note').innerHTML=`<div class="notice">${verified?'Đã đối chiếu gửi lại: toàn bộ bản tin đã checkpoint, số dòng đơn giữ nguyên.':!run.replay_baseline_complete?'Lượt gửi lại bắt đầu khi đợt trước chưa đủ đơn: quan sát được upsert nhưng chưa đủ điều kiện kết luận phép thử chống trùng.':'Đang chờ đủ raw, checkpoint và số đơn sau gửi lại để kết luận.'} Số lượt gửi lại: ${number(run.replay_count)}.</div>`;
  }else $('replay-note').innerHTML='';
  const publishing=run.status==='publishing';
  $('start-button').disabled=busy||publishing||!state.services?.kafka?.ok;
  $('stop-button').disabled=busy||!publishing;
  $('replay-button').disabled=busy||publishing||!run.sent_orders||!state.services?.kafka?.ok;
  const acks=run.acks||[];$('ack-count').textContent=String(acks.length);
  $('ack-rows').innerHTML=acks.length?acks.slice().reverse().map(ack=>`<tr><td>${esc(ack.key)}${ack.invalid?' '+pill('lỗi','amber'):''}</td><td>${ack.topic==='inventory.logs'?'Tồn kho':'Đơn'}</td><td>${ack.partition} / ${ack.offset}</td></tr>`).join(''):'<tr><td colspan="3">Chưa có ACK của đợt demo.</td></tr>';
  $('warehouse-count').textContent=number(wh.ok?wh.run_count:null);
  $('warehouse-rows').innerHTML=wh.rows?.length?wh.rows.map(row=>`<tr><td>${esc(row.order_id)}</td><td>${esc(row.platform)}</td><td>${number(row.quantity)}</td><td>${esc(row.order_status)}</td></tr>`).join(''):`<tr><td colspan="4">${wh.pending?'Đang đọc bằng chứng cho đợt mới…':wh.ok?'Chưa có đơn của đợt trong kho.':'Chưa truy vấn được kho dữ liệu.'}</td></tr>`;
  $('lake-evidence').innerHTML=(objects.length?objects.slice(-8).map(obj=>`<div class="evidence-line"><strong>${number(obj.rows)} bản tin đúng mã đợt</strong> · ${completedLabel(obj.name,worker,lake)}<code>${esc(obj.name)}</code></div>`).join(''):empty(lake.ok?'Chờ consumer lưu bản tin của đợt thành Parquet.':'Chưa đọc được MinIO.'))+`<div class="evidence-line">Worker: <strong>${worker.ok?'heartbeat còn hiệu lực':'chưa sẵn sàng'}</strong> · Lần đọc ${timestamp(worker.checked_at)}<small>ETL gần nhất toàn worker: ${esc(worker.last_etl?.status||'chưa có')} · checkpoint ${timestamp(worker.checkpoint_at)}. Các số ETL toàn worker có thể gồm đợt khác.</small></div>`;
  $('quarantine-evidence').innerHTML=lake.rejected?.length?lake.rejected.map(row=>`<div class="evidence-line"><strong>${esc(row.order_id)}</strong><small>Lý do: ${esc(Array.isArray(row.reasons)?row.reasons.join(', '):row.reasons)}</small><code>${esc(row.object)}</code></div>`).join(''):empty(lake.ok?'Chưa ghi nhận bản tin lỗi của đợt trong quarantine.':'Chưa đọc được vùng cách ly.');
  $('stock-evidence').innerHTML=wh.stocks?.length?wh.stocks.map(row=>`<div class="evidence-line"><strong>${esc(row.sku)}</strong> · ${number(row.stock_on_hand)} sản phẩm<small>Nguồn: Fact_Inventory_Daily · ${esc(row.snapshot_at)}</small></div>`).join(''):empty('Chưa thấy snapshot của đợt demo.');
  $('csv-evidence').innerHTML=state.csvs?.length?state.csvs.map(file=>`<div class="evidence-line"><strong>${esc(file.name)}</strong><small>Sửa lần cuối ${timestamp(file.modified_at)} · ${number(file.bytes)} byte</small></div>`).join(''):empty('Chưa thấy tệp CSV Power BI.');
  renderCatalog(wh.catalog||[],ready);
  renderScenarios();
}
function renderScenarios(){
  const run=state.run||{},lake=state.lake||{},wh=state.warehouse||{};
  const canReplay=!busy&&run.status==='published'&&state.services?.kafka?.ok&&wh.ok&&wh.run_count===run.valid_orders&&lake.ok&&lake.objects?.length>0&&lake.raw_rows>=run.sent_orders&&lake.processed_objects===lake.objects.length;
  $('scenario-replay').disabled=!canReplay;
  $('scenario-replay-state').textContent=canReplay?`Đợt ${run.run_id} · ${number(wh.run_count)} dòng hợp lệ.`:'Chờ một đợt đã xử lý đủ và có checkpoint.';
  document.querySelectorAll('[data-scenario="stockout"],[data-scenario="stocked"]').forEach(button=>button.disabled=!$('sku').value.trim()||$('inventory-button').disabled);
  document.querySelectorAll('[data-scenario="normal"],[data-scenario="invalid"],[data-scenario="burst"]').forEach(button=>button.disabled=busy||run.status==='publishing');
  $('scenario-current-sku').textContent=`SKU tồn kho: ${$('sku').value.trim()||'chưa chọn'}`;
}
document.querySelectorAll('[data-scenario]').forEach(button=>button.addEventListener('click',()=>{
  const scenario=button.dataset.scenario;
  if(['normal','invalid','burst'].includes(scenario)){
    $('order-count').value=scenario==='burst'?'50':'12';
    $('include-invalid').checked=scenario==='invalid';
    showView('stream');
    toast(`Đã cấu hình ${$('order-count').value} đơn${scenario==='invalid'?' + 1 bản tin lỗi':''}. Bấm “Phát đợt mới” để gửi.`);
  }else if(scenario==='replay'){
    showView('stream');action('/api/demo/replay',{});
  }else{
    $('override-stock').checked=true;$('current-stock').disabled=false;
    $('current-stock').value=scenario==='stockout'?'0':'100';$('lead-time').value='3';
    showView('forecast');$('inventory-button').click();
  }
}));
$('sku').addEventListener('input',renderScenarios);
function completedLabel(name,worker,lake){return lake.processed_objects===lake.objects?.length?'đã checkpoint':'đang đối chiếu checkpoint';}
function renderCatalog(catalog,ready){
  const run=state.run||{};
  $('sku-list').innerHTML=catalog.map(row=>`<option value="${esc(row.sku)}">${esc(row.name)} · lịch sử ${esc(row.history_end)}</option>`).join('')+(run.run_id?['S','T'].map(suffix=>`<option value="${run.run_id}-${suffix}">SKU đợt streaming · chưa đủ lịch sử ML</option>`).join(''):'');
  if(catalog.length&&!initializedSku){$('sku').value=catalog[0].sku;initializedSku=true;setHistoryDate();}
  $('catalog-note').textContent=catalog.length?`${catalog.length} SKU gợi ý theo nguồn model. SKU WEB… chỉ dùng kiểm tra luồng và tồn kho.`:'Chưa đọc được danh mục lịch sử. Có thể nhập SKU, nhưng API có thể dùng nguồn dự phòng hoặc từ chối.';
}
function setHistoryDate(){
  const row=state.warehouse?.catalog?.find(item=>item.sku===$('sku').value.trim());
  const end=row?.history_end||state.api?.ready?.data?.history_end;
  if(end){const date=new Date(String(end).slice(0,10)+'T12:00:00Z');date.setUTCDate(date.getUTCDate()+1);$('forecast-start').value=date.toISOString().slice(0,10);}
  else{const now=new Date();now.setDate(now.getDate()+1);$('forecast-start').value=`${now.getFullYear()}-${String(now.getMonth()+1).padStart(2,'0')}-${String(now.getDate()).padStart(2,'0')}`;}
}
$('history-date').addEventListener('click',setHistoryDate);
$('sku').addEventListener('change',setHistoryDate);
$('override-stock').addEventListener('change',()=>{$('current-stock').disabled=!$('override-stock').checked;});
async function post(path,body){
  const response=await fetch(path,{method:'POST',headers:{'Content-Type':'application/json','X-Demo-Token':state.csrf||''},body:JSON.stringify(body),signal:AbortSignal.timeout(25000)});
  const payload=await response.json();
  if(!response.ok){const detail=payload.error||payload.data?.detail||payload.error||'API chưa sẵn sàng. Kiểm tra các dịch vụ.';throw new Error(typeof detail==='string'?detail:JSON.stringify(detail));}
  return payload;
}
async function action(path,body){
  busy=true;render();
  try{await post(path,body);await poll();}catch(error){toast(error.message);}finally{busy=false;render();}
}
$('publish-form').addEventListener('submit',event=>{event.preventDefault();action('/api/demo/start',{count:Number($('order-count').value),invalid:$('include-invalid').checked});});
$('replay-button').addEventListener('click',()=>action('/api/demo/replay',{}));
$('stop-button').addEventListener('click',()=>action('/api/demo/stop',{}));
$('predict-button').addEventListener('click',async()=>{
  const button=$('predict-button');button.disabled=true;button.textContent='Đang gọi API…';
  $('forecast-subtitle').textContent='Đang truy vấn; kết quả cũ đã được xóa.';
  $('forecast-chart').className='chart-empty';$('forecast-chart').innerHTML=empty('Đang chờ model trả kết quả…');
  $('forecast-sources').innerHTML='';$('forecast-details').hidden=true;$('forecast-total').textContent='— sản phẩm';
  try{
    const result=await post('/api/predict',{sku:$('sku').value.trim(),from_date:$('forecast-start').value,days:Number($('forecast-days').value)}), data=result.data;
    $('forecast-subtitle').textContent=`${data.sku} · ${data.from_date} → ${data.to_date}`;
    $('forecast-total').textContent=`${number(data.total_predicted_demand)} sản phẩm`;
    drawChart(data.forecast||[]);
    $('forecast-sources').innerHTML=`<div class="source-tags">${pill('Model: '+data.model_source)}${pill('Lịch sử: '+data.history_source)}${pill(`v${data.model_version} · ${data.model_stage}`)}</div>${data.history_source!=='warehouse'||!['mlflow_registry','local_artifact'].includes(data.model_source)?'<div class="notice">Kết quả có nguồn dự phòng. Không dùng kết quả này để chứng minh chất lượng ML trên dữ liệu thực.</div>':''}`;
    $('forecast-rows').innerHTML=(data.forecast||[]).map(row=>`<tr><td>${esc(row.date)}</td><td>${number(row.predicted_quantity)}</td><td>${number(row.lower_bound)}</td><td>${number(row.upper_bound)}</td></tr>`).join('');
    $('forecast-json').textContent=JSON.stringify(data,null,2);$('forecast-details').hidden=false;
  }catch(error){$('forecast-subtitle').textContent='Truy vấn chưa thành công.';$('forecast-chart').className='chart-empty';$('forecast-chart').innerHTML=empty(error.message);toast(error.message);}
  finally{button.disabled=false;button.textContent='⌁ Chạy dự báo';}
});
function drawChart(rows){
  if(!rows.length){$('forecast-chart').innerHTML=empty('API không trả ngày dự báo.');return;}
  const width=760,height=265,left=45,right=18,top=22,bottom=43;
  const max=Math.max(1,...rows.map(row=>Number(row.upper_bound??row.predicted_quantity))) * 1.12;
  const x=index=>left+(index+.5)*(width-left-right)/rows.length;
  const y=value=>height-bottom-Math.max(0,Number(value))/max*(height-bottom-top);
  const barWidth=Math.min(45,(width-left-right)/rows.length*.48);
  let svg=`<svg viewBox="0 0 ${width} ${height}" class="chart" role="img" aria-label="Dự báo nhu cầu theo ngày. Số liệu chi tiết có trong bảng bên dưới.">`;
  for(let i=0;i<5;i++){const value=max*i/4;svg+=`<line x1="${left}" y1="${y(value)}" x2="${width-right}" y2="${y(value)}" stroke="#e7eee5" stroke-dasharray="3 5"/><text x="${left-9}" y="${y(value)+4}" text-anchor="end">${number(value)}</text>`;}
  rows.forEach((row,i)=>{const value=Number(row.predicted_quantity),date=String(row.date).slice(5).split('-').reverse().join('/');svg+=`<rect x="${x(i)-barWidth/2}" y="${y(value)}" width="${barWidth}" height="${Math.max(1,y(0)-y(value))}" rx="4" fill="${i===0?'#0e8278':'#9bcfb3'}"><title>${esc(row.date)}: ${number(value)} sản phẩm</title></rect><text x="${x(i)}" y="${y(value)-9}" text-anchor="middle" fill="#326b50">${number(value)}</text><text x="${x(i)}" y="${height-18}" text-anchor="middle">${esc(date)}</text>`;});
  $('forecast-chart').className='';$('forecast-chart').innerHTML=svg+'</svg>';
}
$('inventory-button').addEventListener('click',async()=>{
  const button=$('inventory-button');button.disabled=true;button.textContent='Đang tính toán…';
  $('inventory-result').className='empty-state';$('inventory-result').textContent='Đang gọi API đánh giá tồn kho…';$('inventory-badge').textContent='Đang đánh giá';$('inventory-badge').className='pill light';
  try{
    const result=await post('/api/inventory',{sku:$('sku').value.trim(),lead_time_days:Number($('lead-time').value),current_stock:$('override-stock').checked?Number($('current-stock').value):null}), row=result.data.alerts?.[0];
    if(!row)throw new Error('API không trả kết quả cho SKU này.');
    $('inventory-badge').textContent=row.alert_level;$('inventory-badge').className=`pill ${row.alert_level==='CRITICAL'?'red':row.alert_level==='WARNING'?'amber':'light'}`;
    $('inventory-result').className='';$('inventory-result').innerHTML=`<div class="inventory-grid">${kv('Tồn kho hiện tại',number(row.current_stock))}${kv('Tồn kho an toàn · SS',number(row.safety_stock))}${kv('Điểm đặt hàng lại · ROP',number(row.reorder_point))}${kv('Đề xuất nhập thêm',number(row.recommended_reorder_qty))}${kv('Nhu cầu trung bình/ngày',number(row.avg_daily_demand))}${kv('Phân khúc ABC/XYZ',row.matrix_segment)}</div><div class="source-tags">${pill('Tồn kho: '+row.stock_source)}${pill('Nhu cầu: '+row.demand_source)}</div><p class="inventory-explain">SS = Z × σ × √L · ROP = nhu cầu trung bình × L + SS.<br>${row.needs_reorder?'Tồn kho đã chạm ngưỡng đề nghị nhập hàng.':'Tồn kho chưa chạm ngưỡng đề nghị nhập hàng.'} ${row.stock_source==='request'?'Tồn kho giả định từ truy vấn.':''}</p>${row.demand_source==='fallback_catalog'||row.stock_source==='simulated_demo'?'<div class="notice">Có sử dụng nguồn demo hoặc dự phòng. Kết quả minh họa logic tính toán; chưa phải khuyến nghị kinh doanh đã kiểm chứng.</div>':''}<details><summary>Phản hồi API</summary><pre>${esc(JSON.stringify(row,null,2))}</pre></details>`;
  }catch(error){$('inventory-badge').textContent='Chưa có kết quả';$('inventory-result').textContent=error.message;toast(error.message);}
  finally{button.disabled=false;button.textContent='Kiểm tra nhập hàng →';}
});
let polling=false;
async function poll(){
  if(polling)return;polling=true;
  try{const response=await fetch('/api/state',{cache:'no-store',signal:AbortSignal.timeout(8000)});if(!response.ok)throw new Error('Không đọc được trạng thái');state=await response.json();render();}
  catch(error){$('connection-label').textContent='Mất kết nối trang demo';$('connection-dot').className='tiny-dot muted';$('updated-at').textContent='Dữ liệu đang hiển thị có thể đã cũ.';$('start-button').disabled=true;$('replay-button').disabled=true;}
  finally{polling=false;}
}
setHistoryDate();render();poll();setInterval(poll,4000);
function tick(){$('clock').textContent=new Date().toLocaleTimeString('vi-VN',{hour:'2-digit',minute:'2-digit'})+' · LOCAL';}tick();setInterval(tick,1000);
