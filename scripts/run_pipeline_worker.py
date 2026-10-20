"""Supervise Kafka consumers and checkpoint incremental MinIO -> warehouse -> CSV.

Object names are immutable (UUIDs). Checkpoints advance only after ETL and export
succeed. A crash may replay a batch; warehouse upserts and event versions make it
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
from powerbi.export_powerbi_dataset import export_powerbi_data, _install_views
from warehouse.etl.load import get_engine
from scripts.apply_event_version_migration import main as migrate

logger = logging.getLogger("pipeline.worker")
STOP = threading.Event()


def write_state(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(value, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(temporary, path)


def run_cycle(state, checkpoint):
    files = sorted(set(list_parquet_files(build_minio_client())) - set(state.get("completed_objects", [])))
    limit = max(1, int(os.getenv("PIPELINE_OBJECT_BATCH_SIZE", "100")))
    batch = files[:limit]
    if batch:
        stats = run_etl_pipeline(object_names=batch)
        if stats["status"] not in {"SUCCESS", "EMPTY_CLEAN_DATA", "NO_DATA"}:
            raise RuntimeError("ETL did not complete: " + stats["status"])
    else:
        stats = state.get("last_etl")
    # Also refresh snapshots and forecast audits when no new order file arrives.
    export_powerbi_data(install_views=False)
    updated = {"completed_objects": sorted(set(state.get("completed_objects", [])) | set(batch)),
               "last_etl": stats, "pending_objects": len(files) - len(batch), "updated_at": time.time()}
    write_state(checkpoint, updated)
    return updated


def main():
    signal.signal(signal.SIGTERM, lambda *_: STOP.set())
    signal.signal(signal.SIGINT, lambda *_: STOP.set())
    checkpoint = ROOT / "data/pipeline/checkpoint.json"
    heartbeat = ROOT / "data/pipeline/status.json"
    state = json.loads(checkpoint.read_text(encoding="utf-8")) if checkpoint.exists() else {}
    migrate()
    # Install once before starting writers. Replacing views every polling cycle
    # takes exclusive locks and can deadlock concurrent BI readers.
    engine = get_engine()
    try:
        with engine.begin() as conn:
            _install_views(conn)
    finally:
        engine.dispose()
    consumers = []
    try:
        for script in ("consumer_to_minio.py", "consumer_inventory_to_postgres.py"):
            consumers.append(subprocess.Popen([sys.executable, str(ROOT / "ingestion" / script)], cwd=ROOT))
        while not STOP.is_set():
            if any(process.poll() is not None for process in consumers):
                raise RuntimeError("Consumer exited; restarting worker preserves uncommitted offsets")
            try:
                state = run_cycle(state, checkpoint)
                write_state(heartbeat, {"status": "healthy", "checked_at": time.time(),
                                       "pending_objects": state["pending_objects"]})
            except Exception as exc:
                logger.exception("Pipeline cycle failed; checkpoint unchanged, will retry")
                write_state(heartbeat, {"status": "retrying", "checked_at": time.time(), "error_type": type(exc).__name__})
            STOP.wait(max(1, int(os.getenv("PIPELINE_POLL_SECONDS", "30"))))
    finally:
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
