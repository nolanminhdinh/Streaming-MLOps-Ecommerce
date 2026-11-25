"""Dự báo theo lô 14 ngày cho mọi SKU đang hoạt động, lưu vào Fact_Forecast_Predictions qua API serving.

Chạy khi có ngày mới được xác nhận đủ dữ liệu (history_end của /ready đổi) hoặc model đổi phiên bản;
nếu không có gì đổi thì bỏ qua. Gọi POST /predict/batch (có khoảng tin cậy) nên dùng đúng logic dự báo
và cơ chế lưu provenance của serving; serving chỉ lưu dự báo từ model thật trên lịch sử warehouse.
SKU đang hoạt động: có đơn trong ACTIVE_DAYS ngày gần nhất, hoặc có snapshot tồn kho trong 7 ngày gần nhất.
Lỗi ở job này không ảnh hưởng ingestion/ETL.

    python scripts/run_batch_forecast.py --once       # một lượt (dùng khi kiểm tra)
    python scripts/run_batch_forecast.py               # vòng lặp dịch vụ (Compose: batch-forecaster)
"""
import argparse
from datetime import datetime, timedelta
import json
import logging
import os
from pathlib import Path
import sys
import threading
import time
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from sqlalchemy import text

from warehouse.etl.load import get_engine

API = os.getenv("FORECAST_API_BASE", "http://localhost:8000")
HORIZON_DAYS = int(os.getenv("FORECAST_HORIZON_DAYS", "14"))
ACTIVE_DAYS = int(os.getenv("FORECAST_ACTIVE_DAYS", "28"))
CHUNK = int(os.getenv("FORECAST_BATCH_CHUNK", "20"))
INTERVAL = int(os.getenv("FORECAST_POLL_SECONDS", "900"))
BUSINESS_TZ = ZoneInfo(os.getenv("BUSINESS_TZ", "Asia/Ho_Chi_Minh"))
STATE = ROOT / "data/forecast/batch_state.json"
STOP = threading.Event()
logger = logging.getLogger("forecast.batch")


def http_json(path, body=None, timeout=300):
    data = json.dumps(body).encode() if body is not None else None
    request = Request(API + path, data=data, headers={"Content-Type": "application/json"} if data else {})
    with urlopen(request, timeout=timeout) as response:
        return json.load(response)


def active_skus(engine, today):
    with engine.connect() as conn:
        return list(conn.execute(text("""
            SELECT DISTINCT p.sku FROM Fact_Orders f JOIN Dim_Products p USING(product_key)
            WHERE f.create_time >= CAST(:today AS DATE) - :days AND NOT f.is_cancelled
            UNION
            SELECT DISTINCT p.sku FROM Fact_Inventory_Daily i JOIN Dim_Products p USING(product_key)
            JOIN Dim_Dates d USING(date_key) WHERE d.full_date >= CAST(:today AS DATE) - 7
            ORDER BY 1"""), {"today": today, "days": ACTIVE_DAYS}).scalars())


def backtest_metrics(run_id):
    """Metric walk-forward của run đang phục vụ; rỗng nếu MLflow không trả lời (không chặn lượt dự báo)."""
    tracking = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000").rstrip("/")
    try:
        with urlopen(f"{tracking}/api/2.0/mlflow/runs/get?run_id={run_id}", timeout=20) as response:
            run = json.load(response)["run"]["data"]
        metrics = {m["key"]: m["value"] for m in run.get("metrics", [])}
        params = {p["key"]: p["value"] for p in run.get("params", [])}
        return {"cv_wape": metrics.get("cv_wape"), "cv_bias": metrics.get("cv_bias"), "cv_mae": metrics.get("cv_mae"),
                "cv_rmse": metrics.get("cv_rmse"), "cv_scored_rows": int(metrics["cv_scored_rows"]) if "cv_scored_rows" in metrics else None,
                "evaluation_protocol": params.get("evaluation_protocol")}
    except Exception as exc:
        logger.warning("Không đọc được metric MLflow cho run %s (%s)", run_id, type(exc).__name__)
        return {}


def record_snapshot(engine, metadata, summary):
    manifest_path = ROOT / "data/model_manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8")) if manifest_path.exists() else {}
    run_id = manifest.get("run_id") if str(manifest.get("version")) == summary["model_version"] else None
    row = {"name": metadata.get("model_registry_name") or "unknown", "version": summary["model_version"],
           "stage": metadata.get("stage"), "algorithm": metadata.get("champion_algorithm"),
           "origin": metadata.get("training_data_origin"), "run_id": run_id,
           "cv_wape": None, "cv_bias": None, "cv_mae": None, "cv_rmse": None, "cv_scored_rows": None,
           "evaluation_protocol": manifest.get("evaluation_protocol"),
           "history_end": summary["history_end"], "from": summary["from_date"], "to": summary["to_date"],
           "requested": summary["skus"], "model": summary["by_model_source"].get("mlflow_registry", 0)
           + summary["by_model_source"].get("local_joblib", 0),
           "failed": len(summary["failed_skus"])}
    row["heuristic"] = summary["skus"] - row["model"] - row["failed"]
    if run_id:
        row.update({k: v for k, v in backtest_metrics(run_id).items() if v is not None})
    with engine.begin() as conn:
        conn.execute(text("""INSERT INTO Model_Serving_Snapshot (model_registry_name, model_version, model_stage, algorithm,
            training_data_origin, run_id, evaluation_protocol, cv_wape, cv_bias, cv_mae, cv_rmse, cv_scored_rows,
            history_end, forecast_from, forecast_to, skus_requested, skus_model, skus_heuristic, skus_failed)
            VALUES (:name, :version, :stage, :algorithm, :origin, :run_id, :evaluation_protocol, :cv_wape, :cv_bias,
                    :cv_mae, :cv_rmse, :cv_scored_rows, :history_end, :from, :to, :requested, :model, :heuristic, :failed)"""), row)


def run_once(force=False):
    ready = http_json("/ready", timeout=30)
    metadata = http_json("/model/metadata", timeout=30)
    if ready.get("status") != "ready":
        logger.warning("API chưa ready (%s); bỏ qua lượt dự báo", ready.get("status"))
        return {"status": "SKIPPED_NOT_READY"}
    key = {"model_version": str(metadata.get("model_version") or metadata.get("version")), "history_end": ready.get("history_end")}
    previous = json.loads(STATE.read_text(encoding="utf-8")) if STATE.exists() else {}
    if not force and {k: previous.get(k) for k in key} == key:
        return {"status": "UP_TO_DATE", **key}
    today = datetime.now(BUSINESS_TZ).date()
    from_date, to_date = today, today + timedelta(days=HORIZON_DAYS - 1)
    engine = get_engine()
    try:
        skus = active_skus(engine, today)
    except Exception:
        engine.dispose()
        raise
    started = time.monotonic()
    sources, failed = {}, []
    for i in range(0, len(skus), CHUNK):
        chunk = skus[i:i + CHUNK]
        body = {"requests": [{"sku": s, "from_date": str(from_date), "to_date": str(to_date), "confidence_interval": True} for s in chunk]}
        try:
            for result in http_json("/predict/batch", body)["results"]:
                sources[result["model_source"]] = sources.get(result["model_source"], 0) + 1
        except Exception as exc:  # một nhóm lỗi không chặn các nhóm còn lại
            failed.extend(chunk)
            logger.warning("Nhóm SKU %d–%d lỗi: %s", i, i + len(chunk) - 1, exc)
    summary = {"status": "SUCCESS" if not failed else "PARTIAL", **key, "run_at": datetime.now(BUSINESS_TZ).isoformat(),
               "from_date": str(from_date), "to_date": str(to_date), "skus": len(skus), "by_model_source": sources,
               "failed_skus": failed, "seconds": round(time.monotonic() - started, 1)}
    STATE.parent.mkdir(parents=True, exist_ok=True)
    (STATE.parent / "runs").mkdir(exist_ok=True)
    (STATE.parent / "runs" / f"{datetime.now(BUSINESS_TZ):%Y%m%dT%H%M%S}.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    try:
        record_snapshot(engine, metadata, summary)
    except Exception as exc:
        logger.warning("Không ghi được Model_Serving_Snapshot (%s: %s)", type(exc).__name__, exc)
    try:
        from warehouse.reporting.mart import refresh_mart
        with engine.begin() as conn:
            summary["mart_refresh"] = refresh_mart(conn)
    except Exception as exc:
        logger.warning("Không làm mới được mart sau lượt dự báo (%s: %s)", type(exc).__name__, exc)
    finally:
        engine.dispose()
    if not failed:  # chỉ ghi nhận đã xong khi mọi SKU thành công, để lượt sau thử lại phần lỗi
        STATE.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    logger.info("Dự báo theo lô %s: %d SKU, nguồn %s, %ss", summary["status"], len(skus), sources, summary["seconds"])
    return summary


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--force", action="store_true", help="Chạy dù model/history_end không đổi")
    args = parser.parse_args()
    if args.once:
        print(json.dumps(run_once(args.force), ensure_ascii=False))
        return 0
    import signal
    signal.signal(signal.SIGTERM, lambda *_: STOP.set())
    while not STOP.is_set():
        try:
            run_once()
        except Exception as exc:
            logger.warning("Lượt dự báo lỗi, thử lại sau %ss (%s: %s)", INTERVAL, type(exc).__name__, exc)
        STOP.wait(INTERVAL)
    return 0


if __name__ == "__main__":
    sys.exit(main())
