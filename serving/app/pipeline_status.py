"""Read worker telemetry without mistaking an idle pipeline for stale ingestion."""
import json
import math
import time
from pathlib import Path


def finite(value):
    if isinstance(value,bool) or not isinstance(value,(int,float)) or not math.isfinite(value):
        return None
    return float(value)


def read_pipeline_status(path, *, now=None, max_heartbeat_age=180):
    now=time.time() if now is None else now
    try:
        state=json.loads(Path(path).read_text(encoding="utf-8"))
        checked=finite(state.get("checked_at"))
        age=max(0.,now-checked) if checked is not None else None
        volume=state.get("last_batch_volume") or {}
        latency=state.get("ingest_to_warehouse_seconds") or {}
        succeeded=finite(state.get("last_success_at"))
        return {"status":state.get("status","unknown"),
                "ready":state.get("status")=="healthy" and age is not None and checked<=now+5 and age<max_heartbeat_age,
                "heartbeat_age_seconds":age,"pending_objects":finite(state.get("pending_objects")),
                "oldest_pending_seconds":finite(state.get("oldest_pending_seconds")),
                "last_success_age_seconds":max(0.,now-succeeded) if succeeded is not None else None,
                "last_run_id":state.get("last_run_id"),
                "ingest_to_warehouse_p95_seconds":finite(latency.get("p95")),
                "latency_measured_rows":finite(latency.get("measured_rows")),
                "batch_volume_ratio":finite(volume.get("ratio")),
                "batch_volume_anomaly":int(volume.get("anomaly")==1),
                "batch_volume_reference_batches":finite(volume.get("reference_batches")),
                "latency_definition":"Kafka consumer receipt to completion of the ETL batch; excludes upstream source/producer delay",
                "unknown_numeric_metric_value":-1}
    except (OSError,ValueError,TypeError,AttributeError):
        return {"status":"unavailable","ready":False,"unknown_numeric_metric_value":-1}
