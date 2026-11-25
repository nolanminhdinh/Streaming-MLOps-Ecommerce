const fs=require('node:fs'), path=require('node:path');
const d=__dirname;
const rows=fs.readFileSync(path.join(d,'soak-resources.jsonl'),'utf8').trim().split(/\r?\n/).filter(Boolean).map(x=>JSON.parse(x));
const groups={};
function mib(usage) {
  const m=String(usage).match(/^([\d.]+)\s*(KiB|MiB|GiB|B)\s*\//);
  if (!m) return NaN;
  return Number(m[1])*({KiB:1/1024,MiB:1,GiB:1024,B:1/1048576}[m[2]]);
}
const r=x=>Math.round(x*100)/100;
for(const row of rows)for(const c of row.containers??[]){
  const mem=mib(c.MemUsage), cpu=parseFloat(c.CPUPerc);
  if(Number.isFinite(mem)&&Number.isFinite(cpu))(groups[c.Name]??=[]).push({mem,cpu});
}
const o={samples:rows.length,first_at:rows[0]?.timestamp,last_at:rows.at(-1)?.timestamp,fastapi_up_samples:rows.filter(r=>Array.isArray(r.prometheus)&&r.prometheus.some(t=>t.job==='fastapi-serving'&&t.health==='up')).length,containers:Object.entries(groups).map(([name,rr])=>({name,first_mib:r(rr[0].mem),last_mib:r(rr.at(-1).mem),max_mib:r(Math.max(...rr.map(x=>x.mem))),max_cpu_pct:r(Math.max(...rr.map(x=>x.cpu)))}))};
fs.writeFileSync(path.join(d,'soak-resource-summary.json'),JSON.stringify(o,null,2));
