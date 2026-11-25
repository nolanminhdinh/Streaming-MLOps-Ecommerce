"""Export and execute one promtool acceptance file for each pipeline alert case.

Kết quả ghi vào thư mục theo ngày chạy (mặc định output/alert-rules-<YYYY-MM-DD>/) và
không ghi vào thư mục đã có kết quả, trừ khi có --force. Trước 16/11/2026 script ghi cố định
vào output/pipeline-completion-2026-11-08/promtool, làm thay đổi bằng chứng nghiệm thu đã nộp.

    python scripts/check_pipeline_alert_rules.py [--out-dir output/alert-rules-<ngày>] [--force]
"""
import argparse
from datetime import date
import json
from pathlib import Path
import subprocess
import sys

ROOT=Path(__file__).resolve().parents[1]
# Hồ sơ nghiệm thu đã chốt; không bao giờ ghi vào đây, kể cả khi có --force.
PROTECTED=[ROOT/"output/pipeline-completion-2026-11-08"]


def output_dir(argv=None):
    parser=argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--out-dir",default=f"output/alert-rules-{date.today().isoformat()}")
    parser.add_argument("--force",action="store_true",help="Cho phép ghi đè thư mục kết quả đã có (không áp dụng cho hồ sơ đã chốt)")
    args=parser.parse_args(argv)
    out=(ROOT/args.out_dir).resolve()
    if any(out==p.resolve() or p.resolve() in out.parents for p in PROTECTED):
        raise SystemExit(f"Từ chối ghi vào hồ sơ nghiệm thu đã chốt: {out}")
    if out.exists() and any(out.iterdir()) and not args.force:
        raise SystemExit(f"Thư mục đã có kết quả: {out}. Chọn --out-dir khác hoặc thêm --force.")
    out.mkdir(parents=True,exist_ok=True)
    return out


OUT=output_dir(sys.argv[1:])
CASES=[
    ("backlog-too-old","EcommercePipelineBacklogTooOld",{"pending_objects":2,"oldest_pending_seconds":181},True,"Raw đang chờ ETL quá 180 giây"),
    ("idle-is-not-backlog","EcommercePipelineBacklogTooOld",{"pending_objects":0,"oldest_pending_seconds":999},False,""),
    ("measured-batch-slow","EcommercePipelineBatchSlow",{"latency_measured_rows":8,"ingest_to_warehouse_p95_seconds":121},True,"P95 từ consumer đến hoàn tất mẻ ETL vượt 120 giây"),
    ("legacy-unmeasured-not-zero-latency","EcommercePipelineBatchSlow",{"latency_measured_rows":0,"ingest_to_warehouse_p95_seconds":-1},False,""),
    ("volume-reference-ready","EcommercePipelineBatchVolumeAnomaly",{"batch_volume_anomaly":1,"batch_volume_reference_batches":6},True,"Khối lượng mẻ ETL lệch khỏi mức tham chiếu"),
    ("volume-reference-warming-up","EcommercePipelineBatchVolumeAnomaly",{"batch_volume_anomaly":1,"batch_volume_reference_batches":5},False,"")
]
results=[]
for name,alert,metrics,firing,summary in CASES:
    expected=[{"exp_labels":{"instance":"acceptance","severity":"warning"},"exp_annotations":{"summary":summary}}] if firing else []
    specification={"rule_files":["/etc/prometheus/alert_rules.yml"],"evaluation_interval":"15s",
        "tests":[{"name":name,"interval":"15s","input_series":[{"series":'ecommerce_pipeline_'+key+'{instance="acceptance"}',"values":str(value)+"+0x12"} for key,value in metrics.items()],
            "alert_rule_test":[{"eval_time":"2m","alertname":alert,"exp_alerts":expected}]}]}
    path=OUT/(name+".yml");path.write_text(json.dumps(specification,ensure_ascii=False,indent=2),encoding="utf-8")
    subprocess.run(["docker","cp",str(path),"prometheus:/tmp/"+path.name],check=True,capture_output=True)
    process=subprocess.run(["docker","exec","prometheus","promtool","test","rules","/tmp/"+path.name],capture_output=True,text=True,encoding="utf-8")
    result={"case":name,"status":"PASS" if process.returncode==0 else "FAIL","inputs":metrics,"expected_firing":firing,
            "exit_code":process.returncode,"stdout":process.stdout,"stderr":process.stderr,"specification":specification}
    (OUT/(name+".json")).write_text(json.dumps(result,ensure_ascii=False,indent=2),encoding="utf-8")
    results.append(result)
summary={"status":"PASS" if all(r["status"]=="PASS" for r in results) else "FAIL","cases":len(results),"results":results}
(OUT/"result.json").write_text(json.dumps(summary,ensure_ascii=False,indent=2),encoding="utf-8")
print(json.dumps({"status":summary["status"],"cases":len(results)}))
if summary["status"]!="PASS":raise SystemExit(1)
