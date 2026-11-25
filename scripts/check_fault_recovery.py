"""Thử lỗi hạ tầng có kiểm soát trên stack Docker local và đo khả năng phục hồi.

Ba kịch bản, mỗi kịch bản một lô dữ liệu có nhãn riêng:
  postgres  — dừng PostgreSQL, gửi dữ liệu, chờ, bật lại;
  minio     — dừng MinIO, gửi dữ liệu, chờ, bật lại;
  worker    — gửi dữ liệu rồi kill -9 tiến trình worker (Docker restart policy khởi động lại).
Sau mỗi kịch bản kiểm tra: đủ fact, không trùng khóa, tồn kho đúng, lag Kafka về 0,
pipeline healthy. Dữ liệu có nhãn FLT… được giữ lại trong kho. Không train/đăng ký model,
không xóa volume, không reset checkpoint/offset.

    python scripts/check_fault_recovery.py [--scenarios postgres minio worker] [--outage-seconds 60]
"""
import argparse
from datetime import datetime, timedelta, timezone
import json
import logging
from pathlib import Path
import subprocess
import sys
import time
from urllib.request import urlopen

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from kafka import KafkaProducer
from sqlalchemy import create_engine, text

from ingestion.producer import KAFKA_BOOTSTRAP_SERVERS, TOPIC_INVENTORY, TOPIC_ORDERS
from scripts.check_replay_determinism import group_lag
from warehouse.etl.load import DATABASE_URL

API = "http://localhost:8000"
STATUS = ROOT / "data/pipeline/status.json"
WORKER = "streaming-mlops-ecommerce-pipeline-worker-1"
ORDERS, QUANTITY = 5, [1, 2, 3, 4, 5]


def docker(*args, check=True):
    return subprocess.run(["docker", *args], capture_output=True, text=True, check=check).stdout.strip()


def container_state(name):
    return docker("inspect", "-f", "{{.State.Status}}|{{.RestartCount}}|{{if .State.Health}}{{.State.Health.Status}}{{end}}", name, check=False)


def publish(label):
    now = datetime.now(timezone.utc)
    producer = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS, acks="all", retries=3)
    try:
        for i, qty in enumerate(QUANTITY):
            event = dict(_platform="shopee", order_sn=f"{label}-{i}", item_sku=f"{label}-SKU", item_name="Fault recovery fixture",
                         shop_id="FAULT-TEST", shop_name="Fault test", quantity=qty, original_price=10000,
                         total_order_value=10000 * qty, buyer_total_amount=10000 * qty, order_status="COMPLETED",
                         create_time=(now - timedelta(hours=1)).isoformat(), update_time=now.isoformat())
            producer.send(TOPIC_ORDERS, key=event["order_sn"].encode(), value=json.dumps(event).encode()).get(timeout=30)
        stock = dict(sku=f"{label}-SKU", stock_on_hand=77, loaded_at=now.isoformat())
        producer.send(TOPIC_INVENTORY, key=stock["sku"].encode(), value=json.dumps(stock).encode()).get(timeout=30)
        producer.flush(timeout=30)
    finally:
        producer.close(timeout=5)


def warehouse(engine, label):
    try:
        with engine.connect() as conn:
            facts = conn.execute(text("SELECT COUNT(*), COALESCE(SUM(quantity),0) FROM Fact_Orders WHERE order_id LIKE :p"),
                                 {"p": label + "-%"}).one()
            dup = conn.execute(text("""SELECT COUNT(*) FROM (SELECT 1 FROM Fact_Orders WHERE order_id LIKE :p
                GROUP BY order_id, platform, source_line_id HAVING COUNT(*)>1) d"""), {"p": label + "-%"}).scalar()
            stock = conn.execute(text("""SELECT stock_on_hand FROM Fact_Inventory_Daily i JOIN Dim_Products p USING(product_key)
                WHERE p.sku=:s ORDER BY snapshot_at DESC LIMIT 1"""), {"s": label + "-SKU"}).scalar()
        return {"facts": facts[0], "units": int(facts[1]), "duplicate_keys": dup, "stock": stock}
    except Exception as exc:
        return {"error": exc.__class__.__name__}


def http(path):
    try:
        with urlopen(API + path, timeout=10) as response:
            return response.status, json.load(response)
    except Exception as exc:
        code = getattr(exc, "code", None)
        return code, {"error": exc.__class__.__name__}


def observe():
    status = json.loads(STATUS.read_text(encoding="utf-8")) if STATUS.exists() else {}
    ready_code, _ = http("/ready")
    _, pipeline = http("/pipeline/status")
    try:
        with urlopen("http://localhost:9090/api/v1/alerts", timeout=5) as response:
            alerts = sorted({f"{a['labels']['alertname']}:{a['state']}" for a in json.load(response)["data"]["alerts"]})
    except Exception as exc:
        alerts = [f"unavailable:{exc.__class__.__name__}"]
    try:
        lag = group_lag()
    except Exception as exc:
        lag = {"error": exc.__class__.__name__}
    return {"at": datetime.now(timezone.utc).isoformat(), "worker": container_state(WORKER),
            "heartbeat_status": status.get("status"), "pending_objects": status.get("pending_objects"),
            "api_ready_http": ready_code, "api_pipeline_status": pipeline.get("status"), "kafka_lag": lag, "alerts": alerts}


def wait_recovered(engine, label, timeout=420):
    started = time.monotonic()
    while time.monotonic() - started < timeout:
        state = warehouse(engine, label)
        lag = group_lag() if "error" not in state else {"x": 1}
        if state.get("facts") == ORDERS and state.get("stock") == 77 and not any(lag.values()):
            status = json.loads(STATUS.read_text(encoding="utf-8"))
            if status.get("status") == "healthy" and not status.get("pending_objects"):
                return round(time.monotonic() - started, 1), state
        time.sleep(3)
    return None, warehouse(engine, label)


def run_scenario(name, outage, engine):
    label = "FLT" + datetime.now().strftime("%m%d%H%M%S") + name[:2].upper()
    result = {"scenario": name, "label": label, "before": observe()}
    if name in ("postgres", "minio"):
        docker("stop", name)
        result["service_stopped_at"] = datetime.now(timezone.utc).isoformat()
        publish(label)
        time.sleep(outage)
        result["during_outage"] = observe()
        result["warehouse_during_outage"] = warehouse(engine, label)
        docker("start", name)
        result["service_started_at"] = datetime.now(timezone.utc).isoformat()
    else:
        publish(label)
        time.sleep(3)
        before = container_state(WORKER)
        # SIGKILL mọi tiến trình trừ PID 1 (tini); tini thoát theo tiến trình chính, restart policy chạy lại.
        docker("exec", WORKER, "python", "-c", "import os,signal; os.kill(-1, signal.SIGKILL)", check=False)
        time.sleep(5)
        result["after_kill"] = {"before": before, "after": container_state(WORKER)}
    seconds, final = wait_recovered(engine, label)
    result["recovery_seconds_after_restore"] = seconds
    result["final_warehouse"] = final
    result["after"] = observe()
    result["passed"] = (seconds is not None and final.get("facts") == ORDERS and final.get("units") == sum(QUANTITY)
                        and final.get("duplicate_keys") == 0 and final.get("stock") == 77
                        and result["after"]["api_ready_http"] == 200)
    return result


def main():
    logging.disable(logging.CRITICAL)
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--scenarios", nargs="+", default=["postgres", "minio", "worker"], choices=["postgres", "minio", "worker"])
    parser.add_argument("--outage-seconds", type=int, default=60)
    parser.add_argument("--out-dir", default="output/fault-recovery-" + datetime.now().strftime("%Y-%m-%d"))
    args = parser.parse_args()
    out_dir = ROOT / args.out_dir
    out_dir.mkdir(parents=True, exist_ok=True)
    engine = create_engine(DATABASE_URL, pool_pre_ping=True, connect_args={"connect_timeout": 3})
    results = []
    try:
        for name in args.scenarios:
            print(f"Kịch bản {name}…", flush=True)
            try:
                results.append(run_scenario(name, args.outage_seconds, engine))
            finally:
                for service in ("postgres", "minio"):
                    docker("start", service, check=False)  # luôn khôi phục dịch vụ dù kịch bản lỗi
            print(f"  passed={results[-1]['passed']} phục hồi sau {results[-1]['recovery_seconds_after_restore']} s", flush=True)
            time.sleep(10)
    finally:
        engine.dispose()
        (out_dir / "results.json").write_text(json.dumps(results, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    print(json.dumps(results, ensure_ascii=False, indent=2, default=str))
    return 0 if results and all(r["passed"] for r in results) else 1


if __name__ == "__main__":
    sys.exit(main())
