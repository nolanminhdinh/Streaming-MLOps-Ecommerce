"""Measurable batch lineage and latency; unavailable measurements stay undefined."""
from datetime import datetime, timezone
import statistics
import pandas as pd


def latency_summary(raw, finished_at):
    total = len(raw)
    if "_pipeline_ingested_at" not in raw:
        return {"measured_rows":0,"total_rows":total,"p50":None,"p95":None,"max":None}
    stamps=pd.to_datetime(raw["_pipeline_ingested_at"],format="mixed",errors="coerce",utc=True)
    lag=(pd.Timestamp(finished_at)-stamps).dt.total_seconds()
    valid=lag[lag.ge(0) & lag.notna()]
    return {"measured_rows":len(valid),"total_rows":total,
            "p50":float(valid.quantile(.5)) if len(valid) else None,
            "p95":float(valid.quantile(.95)) if len(valid) else None,
            "max":float(valid.max()) if len(valid) else None}


def batch_volume(current, history, min_batches=6, lower=.25, upper=4.):
    prior=[int(x) for x in history[-12:] if x>0]
    median=statistics.median(prior) if len(prior)>=min_batches else None
    ratio=current/median if median else None
    return {"ratio":ratio,"anomaly":int(ratio is not None and (ratio<lower or ratio>upper)),
            "reference_median":median,"reference_batches":len(prior),
            "history":(prior+[int(current)])[-12:]}


def object_accounting(raw, valid_before_dedupe, valid_after_dedupe, rejected):
    def counts(frame):
        return frame["source_object"].fillna("unknown").value_counts().to_dict() if "source_object" in frame else {}
    source=raw["_source_object"].fillna("unknown").value_counts().to_dict() if "_source_object" in raw else {"unknown":len(raw)}
    before,after,bad=counts(valid_before_dedupe),counts(valid_after_dedupe),counts(rejected)
    return [{"source_object":str(name),"raw":int(count),"valid":int(after.get(name,0)),
             "duplicates":int(before.get(name,0)-after.get(name,0)),"rejected":int(bad.get(name,0))}
            for name,count in source.items()]
