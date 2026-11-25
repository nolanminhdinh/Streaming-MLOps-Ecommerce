"""Supervise Kafka consumers and checkpoint incremental MinIO -> warehouse.

Object names are immutable (UUIDs). Checkpoints advance only after ETL succeeds.
CSV backups run separately and cannot block ingestion. A crash may replay a batch;
warehouse upserts and event versions make it
safe. Neither model training nor historical-source coverage is fabricated here.
"""
import json
import logging
import os
from pathlib import Path
import signal
import subprocess
import sys
import threading
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from warehouse.etl.extract import build_minio_client, list_parquet_files
from warehouse.etl.pipeline import run_etl_pipeline
from powerbi.export_powerbi_dataset import _install_views
from warehouse.etl.load import get_engine
from scripts.apply_event_version_migration import main as migrate
from warehouse.reporting.install import install_bi_schema
from warehouse.reporting.mart import install_mart, refresh_mart
from warehouse.etl.coordination import pipeline_lock
from warehouse.etl.coverage import confirm_completed_days, kafka_lag
from warehouse.etl.observability import batch_volume

logger = logging.getLogger("pipeline.worker")
STOP = threading.Event()


def write_state(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(value, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(temporary, path)


def maybe_confirm_coverage(engine, state):
    """Xác nhận ngày đã qua khi không còn object chờ và lag Kafka = 0; lỗi ở đây không làm hỏng chu kỳ ETL."""
    from datetime import datetime
    from zoneinfo import ZoneInfo
    try:
        today = datetime.now(ZoneInfo(os.getenv("BUSINESS_TZ", "Asia/Ho_Chi_Minh"))).date()
        bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS_HOST", "localhost:29092")
        with engine.begin() as conn:
            confirm_completed_days(conn, today, state.get("pending_objects"), lambda: kafka_lag(bootstrap))
    except Exception as exc:
        logger.warning("Bỏ qua xác nhận coverage lượt này (%s: %s)", type(exc).__name__, exc)


def maybe_refresh_mart(engine):
    """Làm mới gia tăng data mart sau chu kỳ ETL; lỗi ở đây không làm hỏng ingestion/ETL."""
    try:
        with engine.begin() as conn:
            stats = refresh_mart(conn)
        if any(stats.get(k) for k in ("fact_sales_line", "fact_inventory_daily", "fact_demand_forecast", "forecast_actuals_scored")):
            logger.info("Mart refresh: %s", stats)
    except Exception as exc:
        logger.warning("Bỏ qua làm mới mart lượt này (%s: %s)", type(exc).__name__, exc)


def check_consumers(consumers):
    """A successful ETL cycle does not imply that ingestion is still alive."""
    for process in consumers:
        code = process.poll()
        if code is not None:
            raise RuntimeError(f"Consumer exited (code={code}); restart worker to replay uncommitted offsets")


def run_cycle(state, checkpoint):
    metadata = {}
    files = sorted(set(list_parquet_files(build_minio_client(), metadata=metadata)) - set(state.get("completed_objects", [])))
    limit = max(1, int(os.getenv("PIPELINE_OBJECT_BATCH_SIZE", "100")))
    batch = files[:limit]
    if batch:
        stats = run_etl_pipeline(object_names=batch)
        if stats["status"] not in {"SUCCESS", "EMPTY_CLEAN_DATA", "NO_DATA"}:
            raise RuntimeError("ETL did not complete: " + stats["status"])
    else:
        stats = state.get("last_etl")
    now = time.time()
    remaining = files[len(batch):]
    timestamps = [metadata.get(name, {}).get("last_modified") for name in remaining]
    oldest = max(0., now-min(timestamps)) if timestamps and all(isinstance(x,(int,float)) for x in timestamps) else (0. if not remaining else None)
    volume = batch_volume(stats.get("raw_records",0), state.get("volume_history",[])) if batch and "raw_records" in stats else state.get("last_batch_volume",{})
    updated = {"completed_objects": sorted(set(state.get("completed_objects", [])) | set(batch)),
               "last_etl": stats, "pending_objects": len(remaining), "updated_at": now,
               "oldest_pending_seconds":oldest,"last_success_at":now if batch else state.get("last_success_at"),
               "last_batch_volume":{k:v for k,v in volume.items() if k!="history"},
               "volume_history":volume.get("history",state.get("volume_history",[]))}
    if batch and stats.get("run_id"):
        write_state(checkpoint.parent/"runs"/(stats["run_id"]+".json"),stats)
    write_state(checkpoint, updated)
    return updated


def main():
    signal.signal(signal.SIGTERM, lambda *_: STOP.set())
    signal.signal(signal.SIGINT, lambda *_: STOP.set())
    checkpoint = ROOT / "data/pipeline/checkpoint.json"
    heartbeat = ROOT / "data/pipeline/status.json"
    # Invalidate the previous process's heartbeat even if startup fails.
    write_state(heartbeat, {"status": "starting", "checked_at": time.time()})
    state = json.loads(checkpoint.read_text(encoding="utf-8")) if checkpoint.exists() else {}
    migrate()
    # Install once before starting writers. Replacing views every polling cycle
    # takes exclusive locks and can deadlock concurrent BI readers.
    engine = get_engine()
    try:
        with engine.begin() as conn:
            _install_views(conn)
            install_bi_schema(conn)
            install_mart(conn)
    finally:
        engine.dispose()
    consumers = []
    try:
        for script in ("consumer_to_minio.py", "consumer_inventory_to_postgres.py"):
            consumers.append(subprocess.Popen([sys.executable, str(ROOT / "ingestion" / script)], cwd=ROOT))
        while not STOP.is_set():
            check_consumers(consumers)
            try:
                with pipeline_lock(engine):
                    state = run_cycle(state, checkpoint)
                check_consumers(consumers)
                maybe_confirm_coverage(engine, state)
                maybe_refresh_mart(engine)
                write_state(heartbeat, {"status": "healthy", "checked_at": time.time(),
                    **{k:state.get(k) for k in ("pending_objects","oldest_pending_seconds","last_success_at","last_batch_volume")},
                    "last_run_id":(state.get("last_etl") or {}).get("run_id"),
                    "ingest_to_warehouse_seconds":(state.get("last_etl") or {}).get("ingest_to_warehouse_seconds")})
            except Exception as exc:
                logger.exception("Pipeline cycle failed; checkpoint unchanged, will retry")
                write_state(heartbeat, {"status": "retrying", "checked_at": time.time(), "error_type": type(exc).__name__})
            STOP.wait(max(1, int(os.getenv("PIPELINE_POLL_SECONDS", "30"))))
    finally:
        engine.dispose()
        try:
            write_state(heartbeat, {"status": "stopped", "checked_at": time.time()})
        except OSError:
            logger.exception("Could not write shutdown status")
        for process in consumers:
            if process.poll() is None:
                process.terminate()
        for process in consumers:
            try:
                process.wait(timeout=20)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()


if __name__ == "__main__":
    main()
