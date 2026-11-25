"""Bounded daily replay of raw data, with dry-run default and independent audit.

Never deletes facts, raw objects or the worker checkpoint. Source-version guards
remain active. A process-level PostgreSQL lock serializes this job with the worker.
"""
from datetime import date, timedelta, datetime, timezone
import argparse
import json
import re
from pathlib import Path
import sys
import uuid

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from sqlalchemy import text
from warehouse.etl.load import get_engine
from warehouse.etl.pipeline import run_etl_pipeline
from warehouse.etl.coordination import pipeline_lock


class BackfillFailure(RuntimeError):
    def __init__(self,audit):
        super().__init__("Backfill stopped on "+audit["failed_date"])
        self.audit=audit


def days_between(start, end, max_days=31):
    if end < start or (end-start).days+1 > max_days:
        raise ValueError(f"Backfill must be an ordered interval of at most {max_days} days")
    return [start+timedelta(days=i) for i in range((end-start).days+1)]


def snapshot(engine, start, end):
    with engine.connect() as conn:
        rows = conn.execute(text('''SELECT d.full_date::text AS date,COUNT(*) AS rows,
            SUM(f.quantity) AS quantity,SUM(f.buyer_total_amount) AS buyer_amount,
            md5(string_agg(jsonb_build_array(f.platform,f.order_id,f.source_line_id,
              f.quantity,f.order_status,f.is_cancelled,f.original_price,f.subtotal,
              f.buyer_total_amount,f.source_updated_at)::text,'|' ORDER BY
              f.platform,f.order_id,f.source_line_id)) AS business_sha
            FROM Fact_Orders f JOIN Dim_Dates d ON d.date_key=f.date_key
            WHERE d.full_date BETWEEN :start AND :end GROUP BY d.full_date ORDER BY d.full_date'''),
            {"start":start,"end":end}).mappings().all()
    return [dict(r) for r in rows]


def run_backfill(start, end, *, apply=False, etl=run_etl_pipeline, snapshot_fn=None, max_days=31, object_names=None):
    days = days_between(start,end,max_days)
    selected={day:[] for day in days}
    if object_names is not None:
        if not isinstance(object_names,list) or not object_names:raise ValueError("Explicit object selection must be a nonempty JSON list")
        for name in object_names:
            match=re.fullmatch(r"raw/orders/platform=(shopee|tiktok|unknown)/year=(\d{4})/month=(\d{2})/day=(\d{2})/[^/]+\.parquet",name) if isinstance(name,str) else None
            if not match:raise ValueError("Selected object is not a supported raw orders partition")
            day=date(*map(int,match.groups()[1:]))
            if day not in selected:raise ValueError("Selected object lies outside the requested date interval")
            selected[day].append(name)
    before = snapshot_fn(start,end) if snapshot_fn else None
    results = []
    allowed = {"SUCCESS","EMPTY_CLEAN_DATA","NO_DATA"} if apply else {"DRY_RUN_SUCCESS","NO_DATA"}
    for day in days:
        try:
            extra={"object_names":selected[day]} if object_names is not None else {}
            result = dict(etl(target_date=day,dry_run=not apply,**extra))
            result["execution_mode"]="backfill"
            result["latency_interpretation"]="replay age, not a new live-traffic SLA measurement"
        except Exception as exc:
            result={"status":"FAILED","error_type":type(exc).__name__,"error":str(exc)}
        results.append(result)
        if result["status"] not in allowed:
            raise BackfillFailure({"status":"FAILED","start":str(start),"end":str(end),"failed_date":str(day),
                "checkpoint_modified":False,"results":results,"before":before,
                "completed_dates_may_have_committed":apply and len(results)>1})
    after = snapshot_fn(start,end) if snapshot_fn else None
    return {"status":"APPLIED" if apply else "DRY_RUN", "start":str(start),"end":str(end),
            "checkpoint_modified":False,"source_version_guard_enabled":True,
            "results":results,"before":before,"after":after,"selected_objects":object_names,
            "business_output_unchanged":before==after if snapshot_fn else None}


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--from-date",type=date.fromisoformat,required=True)
    parser.add_argument("--to-date",type=date.fromisoformat,required=True)
    parser.add_argument("--apply",action="store_true",help="Commit guarded upserts; default validates only")
    parser.add_argument("--objects-file",type=Path,help="Optional JSON list of raw object names; never falls back to a full partition")
    parser.add_argument("--output",type=Path,default=ROOT/"output/pipeline-completion-2026-11-08/backfill")
    args=parser.parse_args()
    days_between(args.from_date,args.to_date)
    args.output.mkdir(parents=True,exist_ok=True)
    audit=args.output/(datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")+"-"+uuid.uuid4().hex[:8]+".json")
    engine=get_engine()
    try:
        with pipeline_lock(engine):
            objects=json.loads(args.objects_file.read_text(encoding="utf-8-sig")) if args.objects_file else None
            result=run_backfill(args.from_date,args.to_date,apply=args.apply,object_names=objects,
                snapshot_fn=lambda start,end:snapshot(engine,start,end))
    except Exception as exc:
        result=getattr(exc,"audit",{"status":"FAILED","error_type":type(exc).__name__,"error":str(exc),
                "checkpoint_modified":False,"partial_dates_may_have_committed":args.apply})
        raise
    finally:
        engine.dispose()
        audit.write_text(json.dumps(result,ensure_ascii=False,indent=2,default=str),encoding="utf-8")
        print(str(audit))


if __name__=="__main__":main()
