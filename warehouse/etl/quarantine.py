"""Persist rejected source records before reporting ETL success."""
import io
import json
import math
from numbers import Real
from decimal import Decimal
from datetime import datetime, date
import pandas as pd
from warehouse.etl.extract import MINIO_BUCKET


def json_value(value):
    if isinstance(value, dict):
        return {str(key): json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [json_value(item) for item in value]
    if hasattr(value, "tolist"):
        return json_value(value.tolist())
    if value is None or pd.isna(value):
        return None
    if isinstance(value, Real) and not math.isfinite(value):
        return str(value)
    if isinstance(value, (date, datetime, pd.Timestamp)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if hasattr(value, "item"):
        return value.item()
    return value


def persist_quarantine(client, frame, run_id, stream="orders"):
    if frame.empty:
        return None
    name = f"quarantine/{stream}/run_id={run_id}/rejected.jsonl"
    rows = [{"run_id": run_id, "status": "rejected", **json_value(row)} for row in frame.to_dict("records")]
    payload = ("\n".join(json.dumps(row, ensure_ascii=False, allow_nan=False) for row in rows) + "\n").encode("utf-8")
    client.put_object(bucket_name=MINIO_BUCKET, object_name=name, data=io.BytesIO(payload),
                      length=len(payload), content_type="application/x-ndjson")
    return name
