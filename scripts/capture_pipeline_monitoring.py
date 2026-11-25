"""Save actual monitoring/API responses and verify local alert delivery."""
import base64
import hashlib
import json
import os
from pathlib import Path
from urllib.request import Request,urlopen
from urllib.parse import urlencode

ROOT=Path(__file__).resolve().parents[1]
OUT=ROOT/"output/pipeline-completion-2026-11-08/monitoring"
OUT.mkdir(parents=True,exist_ok=True)


def fetch(name,url,headers=None):
    with urlopen(Request(url,headers=headers or {}),timeout=30) as response:
        data=response.read()
        (OUT/name).write_bytes(data)
        return json.loads(data) if name.endswith(".json") else data.decode()


def main():
    from dotenv import load_dotenv
    load_dotenv(ROOT/".env")
    result={"status":"RUNNING","external_notifications_sent":False}
    try:
        status=fetch("pipeline-status.json","http://localhost:8000/pipeline/status")
        metrics=fetch("metrics.prom","http://localhost:8000/metrics")
        ready=fetch("api-ready.json","http://localhost:8000/ready")
        fetch("model-metadata.json","http://localhost:8000/model/metadata")
        alerts=fetch("prometheus-alerts.json","http://localhost:9090/api/v1/alerts")
        rules=fetch("prometheus-rules.json","http://localhost:9090/api/v1/rules")
        fetch("prometheus-targets.json","http://localhost:9090/api/v1/targets")
        fetch("prometheus-alertmanagers.json","http://localhost:9090/api/v1/alertmanagers")
        fetch("alertmanager-alerts.json","http://localhost:9093/api/v2/alerts")
        delivered=fetch("alert-delivery.json","http://localhost:9094/events")
        measurements={}
        for metric in ("pending_objects","oldest_pending_seconds","ingest_to_warehouse_p95_seconds","batch_volume_reference_batches"):
            query=urlencode({"query":"ecommerce_pipeline_"+metric})
            measurements[metric]=fetch("query-"+metric+".json","http://localhost:9090/api/v1/query?"+query)
        auth=base64.b64encode((os.getenv("GRAFANA_ADMIN_USER","admin")+":"+os.getenv("GRAFANA_ADMIN_PASSWORD","admin")).encode()).decode()
        dashboard=fetch("grafana-dashboard.json","http://localhost:3000/api/dashboards/uid/ecom-mlops-dashboard",{"Authorization":"Basic "+auth})
        panels=[p for p in dashboard["dashboard"]["panels"] if p["id"] in range(17,23)]
        names={r["name"] for group in rules["data"]["groups"] for r in group["rules"]}
        required={"EcommercePipelineBacklogTooOld","EcommercePipelineBatchSlow","EcommercePipelineBatchVolumeAnomaly"}
        fired={a["labels"]["alertname"] for a in alerts["data"]["alerts"] if a["state"]=="firing"}
        received={a["labels"]["alertname"] for e in delivered["events"] for a in e["payload"]["alerts"]}
        assert status["ready"] and status["pending_objects"]==0
        assert status["ingest_to_warehouse_p95_seconds"] is not None and status["latency_measured_rows"]>0
        assert required<=names and fired&received and len(panels)==6
        assert "ecommerce_pipeline_oldest_pending_seconds" in metrics
        expected=json.loads((ROOT/"docs/07-bao-cao-do-an/evidence/forecast-improvement-2026-11-08/model-evaluation.json").read_text(encoding="utf-8"))["protocol"]["source_sha256"]
        unchanged={name:hashlib.sha256((ROOT/name).read_bytes()).hexdigest()==sha for name,sha in expected.items()}
        assert all(unchanged.values())
        forecast=json.loads((ROOT/"output/pipeline-completion-2026-11-08/live-run1/forecast-api.json").read_text(encoding="utf-8"))["output"]["body"]
        assert forecast["model_source"]=="mlflow_registry" and forecast["history_source"]=="warehouse"
        assert len(forecast["forecast"])==7 and all(row["predicted_quantity"]>=0 for row in forecast["forecast"])
        result.update(status="PASS",grafana_pipeline_panels=len(panels),pipeline_ready=status["ready"],
            pending_objects=status["pending_objects"],last_batch_p95_seconds=status["ingest_to_warehouse_p95_seconds"],
            verified_delivered_alerts=sorted(fired&received),protected_artifacts_unchanged=unchanged,
            forecast_source=forecast["model_source"],forecast_points=len(forecast["forecast"]),
            alert_delivery_destination="local receiver on localhost:9094 only")
    except Exception as exc:
        result.update(status="FAIL",error_type=type(exc).__name__,error=str(exc));raise
    finally:
        (OUT/"result.json").write_text(json.dumps(result,ensure_ascii=False,indent=2),encoding="utf-8")
        print(json.dumps(result,ensure_ascii=False))


if __name__=="__main__":main()
