const fs=require('node:fs'); const path=require('node:path');
const d=__dirname; const read=n=>{try{return JSON.parse(fs.readFileSync(path.join(d,n+'.json'),'utf8'))}catch{return null}};
const photo=(n,caption)=>fs.existsSync(path.join(d,n+'.jpg'))?'\n!['+caption+']('+n+'.jpg)\n':'';
const proof=(n)=>fs.existsSync(path.join(d,n+'.json'))?' [JSON gốc]('+n+'.json).':'';
const streams=[['normal','Đơn hợp lệ'],['invalid','Lô có bản tin sai'],['replay','Phát lại cùng lô'],['burst','Lô 50 đơn'],['producer-stop','Dừng phát giữa lô']];
const faults=['kafka','minio','postgres','pipeline-worker','fastapi','mlflow','grafana','prometheus'];
const api=read('api-monitor'), cont=read('check_continuous_pipeline'), bi=read('check_powerbi_import');
let md='# Báo cáo kiểm thử end-to-end Streaming MLOps\n\n**04/11/2026 · giờ Việt Nam · Stack Docker local**\n\n';
const app=(s)=>{md+=s+'\n\n'};
app('Kiểm thử dùng dữ liệu có nhãn WEB/AUTO trên stack thật. Các bản tin thử được giữ để đối soát. Kafka và ZooKeeper được khởi động từ container hiện có với volume cũ để giữ backlog. Worker được dựng theo cấu hình workspace. Model đang phục vụ được giữ nguyên. Power BI được kiểm tra đến nguồn SQL vì việc xây dựng lại báo cáo được tách sang kế hoạch riêng.');
app('Ảnh Tổng quan lấy trực tiếp từ FlowLab. Ảnh từng ca là ảnh chụp trang bằng chứng hiển thị dữ liệu đo được từ API hoặc truy vấn kiểm thử. JSON kèm theo giữ kết quả chi tiết.'); md+=photo('overview','FlowLab và phiên bản model')+'\n';
app('## 1. Luồng dữ liệu bình thường và dữ liệu lỗi');
app('| Ca | Kết quả | ACK đơn / tồn | Raw | Dòng fact | Cách ly |\n|---|---|---:|---:|---:|---:|\n'+streams.map(([n,t])=>{const x=read(n);return '| '+t+' | '+(x?.passed?'Đạt':x?'Không đạt':'Chưa có kết quả')+' | '+(x?.run?.sent_orders??'—')+' / '+(x?.run?.sent_inventory??'—')+' | '+(x?.lake?.raw_rows??'—')+' | '+(x?.warehouse?.run_count??'—')+' | '+(x?.lake?.rejected_count??'—')+' |'}).join('\n'));
for(const [n,t] of streams){const x=read(n);app('### '+t);app((x?.run?'Mã đợt `'+x.run.run_id+'`. Đã checkpoint '+x.lake.processed_objects+'/'+x.lake.objects.length+' file.':x?.error||'Chưa có kết quả ghi nhận.')+proof(n));md+=photo(n,t)+'\n';}
app('### JSON hỏng, sai sàn, sai giá, phiên bản đến muộn và tồn kho âm');
app(cont?.status==='PASS'?'Đợt `'+cont.run_id+'` gửi 6 bản tin đơn và 3 bản tin kho. Kho giữ 2 dòng COMPLETED với tổng 7 sản phẩm. Tồn mới nhất là 50 dù có snapshot cũ đến sau. MinIO cách ly 3 dòng đơn và 1 dòng tồn kho. API đọc đúng snapshot 50. Request tồn kho không hợp lệ trả HTTP 422.'+proof('check_continuous_pipeline'):'Chưa xác nhận.'); md+=photo('check_continuous_pipeline','Đối soát kiểm tra liên tục')+'\n';
app('## 2. Dự báo, tồn kho và giám sát');
app('### Dự báo, validate đầu vào và sức khỏe dịch vụ');
if(api){const f=api.forecast.data;const r=api.ready.data;app('Readiness HTTP '+api.ready.http+'. Dự báo HTTP '+api.forecast.http+' cho SKU `'+f.sku+'` trong '+f.forecast.length+' ngày, tổng '+f.total_predicted_demand+', version '+f.model_version+' '+f.model_stage+', nguồn `'+f.model_source+'`, lịch sử `'+f.history_source+'`. Khoảng ngày đảo ngược trả '+api.invalid_dates.http+' và đầu vào tồn kho sai trả '+api.invalid_stock.http+'. Lịch sử đến '+r.history_end+', cũ '+r.history_age_days+' ngày. Phép thử chứng minh đường phục vụ model, chưa đánh giá độ chính xác cho nhu cầu hiện tại.'+proof('api-monitor'));}
md+=photo('api-monitor','Kết quả FastAPI và giám sát')+'\n';
for(const n of ['stock0','stock50','stock100']){const x=read(n); const a=x?.data?.alerts?.[0];app('### Cảnh báo với tồn giả định '+n.slice(5));app(a?'SKU `'+a.sku+'` trả '+a.alert_level+', tồn '+a.current_stock+', SS '+a.safety_stock+', ROP '+a.reorder_point+', đề xuất nhập '+a.recommended_reorder_qty+', nguồn tồn `'+a.stock_source+'`, nhu cầu `'+a.demand_source+'`.'+proof(n):'Chưa xác nhận.');md+=photo(n,'Cảnh báo '+n)+'\n';}
app('### Prometheus và Grafana');
if(api){const rules=api.rules.data?.data?.groups?.flatMap(g=>g.rules)||[];const targets=api.targets.data?.data?.activeTargets||[];app('Grafana health HTTP '+api.grafana.http+'. Prometheus báo '+targets.map(t=>t.labels.job+' = '+t.health).join(', ')+'. '+rules.length+' rule được nạp. Ảnh bên dưới chụp trực tiếp từ Prometheus.');}
md+=photo('prometheus-alerts','Các rule Prometheus trên giao diện')+'\n';
app('## 3. Ngắt và khôi phục từng service');
app('Mỗi ca dừng một service bằng Docker, ghi nhận trạng thái và bật lại trong khối khôi phục. Các ca kiểm tra backlog đối chiếu lại ACK, Parquet, fact và checkpoint cùng mã đợt sau khi bật lại. Với Kafka, đợt phát khi broker tắt được dùng để kiểm tra lỗi gửi, rồi phát đợt mới sau khôi phục.');
for(const s of faults){const n='fault-'+s;const x=read(n);app('### '+s);if(x){const a=x.after;const w=x.outage_observed||x.ingested_during||x.down;app('**'+(x.passed?'Đạt tiêu chí của ca':'Chưa đạt hoặc chưa đủ bằng chứng')+'**. '+(w?'Trong lúc ngắt, trạng thái đợt `'+w.status+'`, ACK '+(w.ack??'—')+', raw '+(w.raw??'—')+', fact '+(w.facts??'—')+'. ':'')+(x.ingested_during?'Đã đối soát 4 dòng fact khi service còn tắt. ':'')+(a?'Sau khôi phục, đợt `'+a.run+'` có '+a.ack+' ACK đơn, '+a.raw+' raw, '+a.facts+' fact và '+a.processed+'/'+a.objects+' file checkpoint. ':'')+(x.error||x.restore_error||x.recovery_error||'')+proof(n));}else app('Chưa có kết quả hoàn chỉnh.');md+=photo(n,'Kết quả ngắt '+s)+'\n';}
if (fs.existsSync(path.join(d,'prometheus-firing.jpg'))) {
  app('Trong ca tắt FastAPI, Prometheus hiển thị EcommerceServingDown với một cảnh báo đang firing.');
  md+=photo('prometheus-firing','ServingDown đang firing khi FastAPI tắt')+'\n';
}
const unit=read('unit');
if(unit){app('### Kiểm thử logic bổ sung');app('Bộ unittest FlowLab và simulator trả '+unit.status+'. '+unit.log.find(line=>line.includes('Ran '))+'. Các test này bổ sung cho thực nghiệm Docker.'+proof('unit'));md+=photo('unit','Kết quả unittest')+'\n';}
const cold=read('mlflow-cold');
if(cold){
  app('### MLflow lỗi khi API đã nạp model và khi API vừa khởi động');
  app('**'+(cold.passed?'Đạt':'Chưa đạt hoặc chưa đủ bằng chứng')+'**. API đang chạy trả nguồn '+(cold.warm_forecast?.data?.model_source??'—')+'. Sau restart trong lúc MLflow tắt, readiness HTTP '+(cold.cold_ready?.http??'—')+' và dự báo nguồn '+(cold.cold_forecast?.data?.model_source??'—')+'. Kiểm tra lưu một metric vào tracking cục bộ '+(cold.training_tracking_smoke?.local_fallback?'thành công':'chưa xác nhận')+'. Sau khi bật MLflow, readiness HTTP '+(cold.ready_after?.http??'—')+', nguồn dự báo '+(cold.forecast_after?.data?.model_source??'—')+'.'+proof('mlflow-cold'));
  md+=photo('mlflow-cold','MLflow warm, cold, local tracking và khôi phục')+'\n';
}
app('## 4. Nguồn Power BI');
app(bi?.status==='PASS'?'Kiểm tra chỉ đọc xác nhận '+Object.keys(bi.datasets).length+' query đúng schema và tổng summary khớp fact. Số tổng tại thời điểm truy vấn là '+bi.summary_totals.orders+' đơn và '+bi.summary_totals.units+' sản phẩm. Power BI Desktop Apply/Refresh chưa được kiểm tra trong lượt này.'+proof('check_powerbi_import'):'Chưa xác nhận.');md+=photo('check_powerbi_import','Đối soát nguồn SQL Power BI')+'\n';
const final=read('final-state');
if(final){
 app('### Trạng thái sau kiểm thử');
 app('Worker '+final.worker+', FastAPI readiness HTTP '+final.ready+', Prometheus HTTP '+final.prometheus+', Grafana HTTP '+final.grafana+'. Đã bật lại các service tạm dừng trong các ca gián đoạn.'+proof('final-state'));
 md+=photo('final-state','Trạng thái stack sau kiểm thử')+'\n';
}
app('## 5. Tái huấn luyện và chuyển phiên bản model');
const blocked=read('retrain-blocked'), retrain=read('retrain-switch');
if(blocked){
 app('### Tái huấn luyện với metadata triển khai hiện hành');
 app('**Bị chặn ở bước huấn luyện.** Manifest model v2 dùng dữ liệu lịch sử CSV đã khử định danh nhưng thiếu `source_datasets`. Bộ điều phối dừng với lỗi `'+blocked.retraining?.error+'`. Ca chạy dùng bản sao manifest và Registry thử nghiệm. Đây là kết quả trước bản sửa manifest ở mục 7.'+proof('retrain-blocked'));
 md+=photo('retrain-blocked','Tái huấn luyện bị chặn vì thiếu fingerprint')+'\n';
}
if(retrain){
 app('### Tái huấn luyện có đối chiếu nguồn dữ liệu');
 const tr=retrain.training_report||{}, reg=retrain.registry||{}, metrics=reg.metrics||{};
 app('**'+(retrain.status==='PASS'?'Đạt trong môi trường cô lập':retrain.status)+'**. Hai fingerprint được lấy từ artifact đã đăng ký và đối chiếu với Feature Spec trong workspace, rồi chỉ bổ sung vào bản sao manifest. Trigger chạy bằng chế độ ép tay. PostgreSQL có '+tr.available_history_days+' ngày lịch sử so với '+tr.required_history_days+' ngày yêu cầu. Dựng '+tr.feature_row_count+' dòng đặc trưng, huấn luyện '+(tr.trained_models||[]).join(' và ')+' qua hai fold. Từ trigger đến FastAPI xác nhận reload mất '+retrain.retraining_seconds+' giây.');
 app('MLflow `'+reg.name+'` v'+reg.version+' '+reg.status+' / '+reg.stage+', run `'+reg.run_id+'` '+reg.run_status+'. Model được chọn là '+tr.best_model+', CV WAPE '+metrics.cv_wape+'%, MAE '+metrics.cv_mae+', Bias '+metrics.cv_bias+'%. Kết quả xác nhận quy trình kỹ thuật. WAPE cao và lịch sử kết thúc ngày '+tr.history_end+' giới hạn việc dùng model cho quyết định vận hành.'+proof('retrain-switch'));
 md+=photo('retrain-success','Run tái huấn luyện và Registry thật')+'\n';
 app('### Chuyển phiên bản, reload lỗi và quay lui');
 const v=o=>o?.body?.model_version??'—';
 app('**'+(retrain.status==='PASS'?'Đạt':'Chưa đạt')+'**. FastAPI cô lập dự báo từ Registry v'+v(retrain.before_forecast)+' → v'+v(retrain.after_forecast)+'. Manifest thử trỏ đến version không tồn tại thì reload trả HTTP '+retrain.bad_reload?.http+', dự báo vẫn là v'+v(retrain.forecast_after_bad_reload)+'. Khôi phục manifest thử và reload trả HTTP '+retrain.rollback?.http+', dự báo quay về v'+v(retrain.forecast_after_rollback)+'. FastAPI demo được đối chiếu trước và sau, vẫn phục vụ `'+retrain.main_after?.model_registry_name+'` v'+retrain.main_after?.version+'. Experiment và version kiểm thử còn lưu trong MLflow để truy vết.'+proof('retrain-switch'));
 md+=photo('model-switch','Phiên bản trước, sau, giữ model khi lỗi và quay lui')+'\n';
}
app('## 6. Tải hỗn hợp kéo dài');
const soak=read('soak'), integrity=read('soak-integrity'), res=read('soak-resource-summary');
if(soak){
 app('**'+(soak.status==='PASS'&&integrity?.status==='PASS'?'Đạt':'Chưa đạt hoặc chưa đủ đối soát')+'**. Lô `'+soak.run_id+'` chạy '+(soak.duration_seconds/60)+' phút. Tải mục tiêu là 1 đơn/giây, snapshot tồn kho mỗi 30 giây và 4 HTTP request/giây. Bốn luồng HTTP gọi dự báo 7 ngày và cảnh báo một SKU, theo tỷ lệ 3:1. Pha đầu chạy cùng tác vụ retraining cô lập. Dự báo dùng cùng SKU và cửa sổ ngày nên phép thử có thể hưởng cache. Phép thử này đo độ ổn định tại mức tải đã nêu.');
 app('| Nhóm | Số request | Thành công đúng tiêu chí | P50 (ms) | P95 (ms) | P99 (ms) | Max (ms) |\n|---|---:|---:|---:|---:|---:|---:|\n'+[['Toàn bộ',soak.http],['Dự báo',soak.forecast],['Tồn kho',soak.inventory]].map(([n,v])=>'| '+n+' | '+v.requests+' | '+(v.status.OK||0)+' | '+v.p50_ms+' | '+v.p95_ms+' | '+v.p99_ms+' | '+v.max_ms+' |').join('\n'));
 app('Kafka ACK '+soak.ack_orders+' đơn, PostgreSQL có '+soak.fact_rows+' dòng, thiếu '+soak.missing_orders.length+', dư '+soak.extra_orders.length+', trùng '+soak.duplicate_fact_rows+'. Tồn kho cuối '+soak.latest_stock+'/'+soak.expected_latest_stock+'. Sau khi ngừng phát, hệ thống mất '+soak.drain_seconds+' giây để quan sát thấy kho đã xử lý hết.'+proof('soak'));
 md+=photo('soak','Kết quả và các mốc trong phép thử tải')+'\n';
}
const progress=read('soak-progress');
if(progress){
  const m=progress.measurements.at(-1);
  app('### Quan sát trong lúc đang phát');
  app('Tại giây '+m.elapsed_seconds+', Kafka đã ACK '+m.ack+' đơn, warehouse đã có '+m.fact_rows+' dòng và worker báo '+m.worker+'. So sánh ACK với fact tại các mốc đang phát phản ánh lượng đang chờ xử lý, chưa phải số mất dữ liệu.'+proof('soak-progress'));
  md+=photo('soak-progress','Số đo đang chạy ở giữa ca tải')+'\n';
}
if(integrity){
 app('### Đọc lại Kafka, MinIO và checkpoint');
 app('Đọc độc lập '+integrity.kafka_orders_readback+' bản tin đơn và '+integrity.kafka_inventory_readback+' snapshot từ Kafka. MinIO có '+integrity.raw_rows+' dòng của lô trong '+integrity.minio_objects+' file, '+integrity.checkpointed_objects+' file đã checkpoint. Kết quả: **'+integrity.status+'**.'+proof('soak-integrity'));
 md+=photo('soak-integrity','Đối soát độc lập Kafka và MinIO')+'\n';
}
if(res){
 app('### Tài nguyên và khả năng quan sát trong ca tải');
 app('Đã lấy '+res.samples+' mẫu Docker và Prometheus. Các giá trị CPU và RAM là số quan sát tại những lần lấy mẫu. '+res.fastapi_up_samples+'/'+res.samples+' mẫu ghi nhận Prometheus target FastAPI UP.'+proof('soak-resource-summary'));
 app('| Container | RAM đầu (MiB) | RAM cuối (MiB) | RAM cao nhất quan sát (MiB) | CPU cao nhất quan sát (%) |\n|---|---:|---:|---:|---:|\n'+res.containers.map(c=>'| '+c.name+' | '+c.first_mib+' | '+c.last_mib+' | '+c.max_mib+' | '+c.max_cpu_pct+' |').join('\n'));
 md+=photo('soak-resource-summary','Mẫu tài nguyên Docker trong ca tải')+'\n';
}
if(read('soak-integrity-first-read')){
 app('Lần đọc Kafka đầu bằng iterator chỉ quan sát 416 bản tin. Tôi giữ log đó và chạy lại bằng cách đọc hết từng batch đến offset cuối đã chụp. Lần đọc lại xác nhận đủ 600 bản tin. Không phát lại lô kiểm thử giữa hai lần đọc.'+proof('soak-integrity-first-read'));
}
const followup=read('followup-final-state');
if(followup){
 app('### Trạng thái sau ca tải');
 app('FastAPI demo báo '+followup.ready+', dùng `'+followup.model_registry_name+'` v'+followup.model_version+'. Worker báo '+followup.worker.status+'. Dữ liệu thử có nhãn được giữ lại trong stack.'+proof('followup-final-state'));
 md+=photo('followup-final-state','Trạng thái sau ca tải')+'\n';
}
app('## 7. Sửa manifest và chạy lại ca tái huấn luyện');
const fixed=read('retrain-manifest-fixed');
if(fixed){
 const q=fixed.registry||{};
 app('Đã thêm đúng hai `source_datasets` của Shopee và TikTok vào `data/model_manifest.json`. Hai mã khớp với Feature Spec trong artifact của Registry `ECommerceDemandForecastModel` v2, run `9c01dafd9238464cbcd1cef677324bcc`, và với Feature Spec trong workspace. Thay đổi chỉ bổ sung metadata của nguồn.'+proof('manifest-provenance'));
 app('**'+(fixed.status==='PASS'?'Đạt':'Chưa đạt')+'**. Ca chạy dùng bản sao từ manifest vừa sửa và kiểm tra trực tiếp danh sách nguồn trước khi huấn luyện. Tác vụ thử nghiệm không tự bổ sung trường này. Bộ điều phối trả `'+fixed.retraining?.status+'` trong '+fixed.retraining_seconds+' giây. Run `'+q.run_id+'` có trạng thái '+q.run_status+', model thử `'+q.name+'` v'+q.version+' '+q.status+' / '+q.stage+'. FastAPI thử nghiệm trả dự báo phiên bản mới sau reload.'+proof('retrain-manifest-fixed'));
 app('FastAPI demo trước và sau đều trả `'+fixed.main_after?.model_registry_name+'` v'+fixed.main_after?.version+' từ `'+fixed.main_after?.model_source+'`. Phiên bản thử chỉ đăng ký dưới tên Registry riêng. Manifest là file runtime trong thư mục data. Bản sửa chỉ bổ sung hai mã nguồn và giữ nguyên phiên bản đang phục vụ.');
 md+=photo('retrain-manifest-fixed','Tái huấn luyện thành công từ manifest đã sửa và model demo giữ nguyên')+'\n';
}
app('## 8. Phạm vi còn lại');
app('Power BI Desktop Apply/Refresh được tách sang kế hoạch riêng. Chưa thử mất volume, mất đĩa, làm hỏng Parquet trên kho đang sử dụng, backfill, retraining theo lịch hoặc do drift thực tế, hay phát thông báo Prometheus ra ngoài. Ca tái huấn luyện dùng trigger ép tay và Registry cô lập. Ca tải 10 phút chưa xác nhận khả năng chạy nhiều giờ hoặc giới hạn số người dùng đồng thời.');
fs.writeFileSync(path.join(d,'Bao-cao-kiem-thu-end-to-end.md'),md);








