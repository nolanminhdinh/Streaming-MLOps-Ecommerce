"""Add operational pipeline panels to the existing Grafana provisioning file."""
import json
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]
path=ROOT/"monitoring/grafana/provisioning/dashboards/ecommerce_mlops_dashboard.json"
dashboard=json.loads(path.read_text(encoding="utf-8"))
owned={105,*range(17,23)}
panels=[p for p in dashboard["panels"] if p["id"] not in owned]
y=max(p.get("gridPos",{}).get("y",0)+p.get("gridPos",{}).get("h",1) for p in panels)
panels.append({"id":105,"type":"row","title":"DATA PIPELINE END TO END","gridPos":{"x":0,"y":y,"w":24,"h":1},"collapsed":False,"panels":[]})
definitions=[("Pipeline Ready","ready","short",1),("Objects awaiting ETL","pending_objects","short",100),
             ("Oldest pending raw","oldest_pending_seconds","s",180),("Last batch ingest to warehouse P95","ingest_to_warehouse_p95_seconds","s",120),
             ("Last batch volume ratio","batch_volume_ratio","short",4),("Last batch volume anomaly","batch_volume_anomaly","short",1)]
for offset,(title,metric,unit,threshold) in enumerate(definitions):
    panels.append({"id":17+offset,"type":"stat","title":title,"description":"Operational demo metrics; -1 means not measured. Batch latency starts at Kafka consumer receipt.",
        "gridPos":{"x":offset*4,"y":y+1,"w":4,"h":5},
        "targets":[{"expr":"ecommerce_pipeline_"+metric,"format":"time_series","legendFormat":title}],
        "fieldConfig":{"defaults":{"unit":unit,"decimals":2,
            "mappings":[{"type":"value","options":{"-1":{"text":"NOT MEASURED","color":"gray"}}}],
            "thresholds":{"mode":"absolute","steps":[{"color":"green","value":None},{"color":"red","value":threshold}]}},"overrides":[]},
        "options":{"reduceOptions":{"values":False,"calcs":["lastNotNull"]},"textMode":"value"}})
panels[-6]["fieldConfig"]["defaults"]["mappings"]=[{"type":"value","options":{"0":{"text":"NOT READY","color":"red"},"1":{"text":"READY","color":"green"}}}]
panels[-6]["fieldConfig"]["defaults"]["thresholds"]["steps"]=[{"color":"red","value":None},{"color":"green","value":1}]
dashboard["panels"]=panels
path.write_text(json.dumps(dashboard,ensure_ascii=False,indent=2)+"\n",encoding="utf-8")
print("Provisioned six pipeline panels")
