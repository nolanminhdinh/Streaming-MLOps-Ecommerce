const http = require('node:http');
const fs = require('node:fs');
const path = require('node:path');
const root = __dirname;
const esc = s => String(s).replace(/[&<>"]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c]));
http.createServer((req,res)=>{
  const name=new URL(req.url,'http://127.0.0.1').pathname.slice(1);
  if(!/^[a-zA-Z0-9_-]+$/.test(name)){res.writeHead(404); return res.end();}
  try {
    const source=['retrain-success','model-switch'].includes(name)?'retrain-switch':name;
    const full=JSON.parse(fs.readFileSync(path.join(root,source+'.json'),'utf8'));
    const brief=r=>r&&{http:r.http,model_name:r.body?.model_name,model_version:r.body?.model_version,model_source:r.body?.model_source,total_predicted_demand:r.body?.total_predicted_demand};
    const j=name==='retrain-blocked'?{status:full.status,experiment:full.experiment,retraining:full.retraining,before:brief(full.before_forecast)}:
    name==='retrain-success'?{status:full.status,experiment:full.experiment,source:full.isolated_manifest_fingerprints,training:{seconds:full.retraining_seconds,status:full.training_report?.status,history_days:full.training_report?.available_history_days,required_days:full.training_report?.required_history_days,feature_rows:full.training_report?.feature_row_count,models:full.training_report?.trained_models},retraining:full.retraining?.status,metrics:full.retraining?.audit_log?.train_metrics,registry:full.registry,error:full.error}:
    name==='retrain-manifest-fixed'?{status:full.status,source:full.isolated_manifest_fingerprints,experiment:full.experiment,retraining:full.retraining?.status,training_status:full.training_report?.status,seconds:full.retraining_seconds,registry:full.registry,before:brief(full.before_forecast),after:brief(full.after_forecast),demo_before:full.main_before,demo_after:full.main_after,error:full.error}:
    name==='model-switch'?{status:full.status,before:brief(full.before_forecast),after:brief(full.after_forecast),bad_reload:full.bad_reload,after_bad_reload:brief(full.forecast_after_bad_reload),rollback:full.rollback,after_rollback:brief(full.forecast_after_rollback),demo_before:full.main_before,demo_after:full.main_after,error:full.error}:
    name==='soak'?{...full,samples:full.samples?.filter((_,i)=>i%4===0||i===full.samples.length-1)}:full;
    const data=j.run&&j.lake ? {passed:j.passed,run:j.run.run_id,status:j.run.status,orders_ack:j.run.sent_orders,inventory_ack:j.run.sent_inventory,raw:j.lake.raw_rows,fact_lines:j.warehouse.run_count,quarantine:j.lake.rejected_count,checkpointed:j.lake.processed_objects,objects:j.lake.objects?.length,captured:j.captured} :
    name==='api-monitor' ? {ready:j.ready,forecast:j.forecast,invalid_dates:j.invalid_dates.http,invalid_stock:j.invalid_stock.http,grafana:j.grafana,targets:j.targets.data?.data?.activeTargets?.map(t=>({job:t.labels.job,health:t.health,lastError:t.lastError})),rules:j.rules.data?.data?.groups?.flatMap(g=>g.rules.map(r=>({name:r.name,state:r.state,health:r.health})))} : j;
    const info=name.startsWith('fault-')?{service:j.service,passed:j.passed,started:j.started,while_offline:j.outage_observed||j.ingested_during||j.down,endpoint_down:j.endpoint_down,serving_during:j.serving_during,other_monitor_during:j.prometheus_during||j.grafana_during,after:j.after&&{run:j.after.run,ack:j.after.ack,raw:j.after.raw,facts:j.after.facts,checkpointed:j.after.processed},ready_after:j.ready_after,error:j.error||j.recovery_error||j.restore_error}:data;
    res.setHeader('Content-Type','text/html; charset=utf-8');
    res.end('<!doctype html><html lang="vi"><meta charset="utf-8"><title>'+esc(name)+'</title><style>body{margin:0;background:#111827;color:#e5edf8;font:16px system-ui}main{max-width:880px;margin:auto;padding:30px}h1{color:#fff;margin:8px 0}p{color:#a8bad2}pre{margin-top:22px;background:#1c283b;border:1px solid #31435e;border-radius:12px;padding:18px;color:#b8e6d3;white-space:pre-wrap;font:14px/1.32 monospace}</style><main><small>STREAMING MLOPS / EVIDENCE / 04.11.2026</small><h1>'+esc(name)+'</h1><p>Ảnh chụp trang bằng chứng hiển thị dữ liệu đo từ lượt kiểm thử thực tế.</p><pre>'+esc(JSON.stringify(info,null,2))+'</pre></main></html>');
  } catch {res.writeHead(404);res.end('Evidence not found');}
}).listen(8765,'127.0.0.1');


