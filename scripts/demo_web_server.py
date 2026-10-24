"""Local defense console. Real Kafka writes; read-only probes; no Docker socket.

Run through ``docker compose --profile demo up -d --build demo-web``.
The standard-library HTTP server also renders an honest disconnected UI without
pipeline dependencies, useful for diagnosing an unavailable Docker installation.
"""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import logging
import os
from pathlib import Path
import re
import secrets
import socket
import sys
import threading
import time
from urllib.error import HTTPError
from urllib.parse import urlsplit
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
WEB = ROOT / "demo_web"
RUNS = ROOT / "data" / "demo"
TOKEN = secrets.token_urlsafe(32)
API_BASE = os.getenv("DEMO_SERVING_URL", "http://127.0.0.1:8000").rstrip("/")
LOG = logging.getLogger("demo.web")


def read_json(path, default=None):
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return default


def public_run(run):
    return {k: v for k, v in run.items() if k != "events"}


def api_call(path, body=None):
    request = Request(API_BASE + path, data=json.dumps(body).encode() if body is not None else None,
                      headers={"Content-Type": "application/json"})
    try:
        with urlopen(request, timeout=12) as response:
            return {"ok": True, "status": response.status, "data": json.load(response)}
    except HTTPError as exc:
        try:
            detail = json.loads(exc.read())
        except ValueError:
            detail = {"detail": "API trả lỗi không phải JSON."}
        return {"ok": False, "status": exc.code, "data": detail}
    except Exception as exc:
        return {"ok": False, "status": 0, "error": type(exc).__name__}


def make_events(run_id, count, invalid):
    from data_simulator.data_simulator import ECommerceSimulator
    simulator = ECommerceSimulator(cancel_rate=0)
    # A freshly-created order is UNPAID: do not invent future delivery timestamps.
    simulator._resolve_shopee_status = lambda _: ("UNPAID", 0)
    simulator._resolve_tiktok_status = lambda _: ("UNPAID", 0)
    now = datetime.now(timezone.utc)
    events = []
    for i in range(count + int(invalid)):
        platform = "shopee" if i % 2 == 0 else "tiktok"
        bad = i == count
        sku = run_id + ("-S" if platform == "shopee" else "-T")
        order_id = f"{run_id}-{i:03d}"
        if platform == "shopee":
            event = asdict(simulator._generate_shopee_event(now))
            event.update(order_sn=order_id, item_sku=sku, item_name="DEMO · Sản phẩm Shopee",
                         shop_id=run_id, shop_name="DEMO bảo vệ")
        else:
            event = asdict(simulator._generate_tiktok_event(now))
            event.update(order_id=order_id, seller_sku=sku, product_name="DEMO · Sản phẩm TikTok",
                         shop_name="DEMO " + run_id)
        event.update(_platform=platform, _demo_run_id=run_id, order_line_id=order_id + "-1",
                     update_time=now.isoformat())
        if bad:
            event["quantity"] = -1
        events.append({"topic": "orders", "key": order_id, "payload": event, "invalid": bad})
    # Dedicated SKUs prevent demo snapshots from replacing business inventory.
    for suffix, stock in (("-S", 50), ("-T", 8)):
        sku = run_id + suffix
        events.append({"topic": "inventory", "key": sku, "payload": {
            "sku": sku, "product_name": "DEMO bảo vệ " + suffix[-1], "category": "DEMO",
            "stock_on_hand": stock, "loaded_at": now.isoformat()}, "invalid": False})
    return events


class Console:
    def __init__(self):
        self.lock = threading.RLock()
        self.stop = threading.Event()
        self.job = None
        self.queries = {}
        self.snapshot = {"checked_at": None, "services": {}, "loading": True}
        self.run = {}
        for path in sorted(RUNS.glob("WEB*.json"), reverse=True):
            value = read_json(path, {})
            if re.fullmatch(r"WEB\d{14}[a-f0-9]{6}", value.get("run_id", "")):
                self.run = value
                if self.run.get("status") == "publishing":
                    self.run.update(status="interrupted", message="Lần phát trước bị gián đoạn; số ACK đã lưu là mốc xác nhận cuối.")
                break

    def save(self):
        RUNS.mkdir(parents=True, exist_ok=True)
        path = RUNS / (self.run["run_id"] + ".json")
        temporary = path.with_suffix(".tmp")
        temporary.write_text(json.dumps(self.run, ensure_ascii=False, default=str), encoding="utf-8")
        temporary.replace(path)

    def start(self, body, replay=False):
        with self.lock:
            if self.job and self.job.is_alive():
                raise ValueError("Đang phát dữ liệu. Hãy đợi hoặc dừng đợt hiện tại.")
            if replay:
                if not self.run.get("events") or not self.run.get("sent_orders"):
                    raise ValueError("Chưa có đợt dữ liệu đã được Kafka xác nhận để gửi lại.")
                events = self.run["events"]
                self.run["replay_count"] = self.run.get("replay_count", 0) + 1
                self.run["replay_baseline"] = (self.snapshot.get("warehouse") or {}).get("run_count")
                self.run["replay_baseline_complete"] = (
                    self.run["replay_baseline"] == self.run["valid_orders"])
            else:
                count = body.get("count", 12)
                if type(count) is not int or not 2 <= count <= 50:
                    raise ValueError("Số đơn hợp lệ phải từ 2 đến 50.")
                if type(body.get("invalid", True)) is not bool:
                    raise ValueError("Tùy chọn bản tin lỗi phải là true hoặc false.")
                run_id = "WEB" + datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S") + secrets.token_hex(3)
                events = make_events(run_id, count, body.get("invalid", True))
                self.run = {"run_id": run_id, "started_at": time.time(), "valid_orders": count,
                            "invalid_orders": int(body.get("invalid", True)), "events": events,
                            "sent_orders": 0, "sent_inventory": 0, "replay_count": 0, "acks": []}
            self.run.update(status="publishing", message="Đang gửi và chờ Kafka xác nhận từng bản tin.")
            self.stop.clear()
            self.save()
            self.job = threading.Thread(target=self.publish, args=(events,), daemon=True)
            self.job.start()
            return public_run(self.run)

    def publish(self, events):
        producer = None
        try:
            from kafka import KafkaProducer
            producer = KafkaProducer(
                bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS_HOST", "localhost:29092"),
                value_serializer=lambda value: json.dumps(value, ensure_ascii=False).encode(),
                acks="all", retries=2, request_timeout_ms=10000, max_block_ms=10000,
                api_version_auto_timeout_ms=5000)
            for event in events:
                if self.stop.is_set():
                    break
                topic = os.getenv("KAFKA_TOPIC_ORDERS", "ecom.orders.raw") if event["topic"] == "orders" else os.getenv("KAFKA_TOPIC_INVENTORY", "inventory.logs")
                ack = producer.send(topic, key=event["key"].encode(), value=event["payload"]).get(timeout=15)
                with self.lock:
                    counter = "sent_orders" if event["topic"] == "orders" else "sent_inventory"
                    self.run[counter] += 1
                    self.run["acks"].append({"key": event["key"], "topic": topic,
                                             "partition": ack.partition, "offset": ack.offset,
                                             "invalid": event["invalid"], "at": time.time()})
                    self.run["acks"] = self.run["acks"][-160:]
                    self.save()
                self.stop.wait(0.65)
            with self.lock:
                self.run.update(status="stopped" if self.stop.is_set() else "published",
                                message="Đã dừng phát; các bản tin Kafka đã nhận vẫn tiếp tục được xử lý." if self.stop.is_set() else "Kafka đã xác nhận. Đang chờ consumer và worker xử lý.")
        except Exception as exc:
            LOG.exception("Demo publisher failed")
            with self.lock:
                self.run.update(status="error", message=f"Không phát được dữ liệu ({type(exc).__name__}). Kiểm tra Kafka và cấu hình kết nối.")
        finally:
            if producer:
                producer.close(timeout=5)
            with self.lock:
                self.save()

    def warehouse(self, model, run):
        from sqlalchemy import create_engine, text
        from sqlalchemy.engine import URL
        url = URL.create("postgresql+psycopg2", username=os.getenv("POSTGRES_USER", "ecom"),
                         password=os.getenv("POSTGRES_PASSWORD", "ecom_password"),
                         host=os.getenv("POSTGRES_HOST", "localhost"),
                         port=int(os.getenv("POSTGRES_PORT", "5432")), database=os.getenv("POSTGRES_DB", "ecom_warehouse"))
        engine = create_engine(url, connect_args={"connect_timeout": 3, "options": "-c statement_timeout=5000"})
        try:
            with engine.connect() as conn:
                totals = dict(conn.execute(text("SELECT COUNT(*) AS order_lines, COUNT(DISTINCT (order_id, platform)) AS orders, COALESCE(SUM(quantity),0) AS units FROM Fact_Orders")).mappings().one())
                rows = [dict(r) for r in conn.execute(text("""
                    SELECT f.order_id, f.platform, p.sku, f.quantity, f.order_status, f.create_time
                    FROM Fact_Orders f JOIN Dim_Products p USING(product_key)
                    WHERE f.order_id LIKE :prefix ORDER BY f.order_id LIMIT 60
                """), {"prefix": run.get("run_id", "NO_ACTIVE_RUN") + "-%"}).mappings()]
                stocks = [dict(r) for r in conn.execute(text("""
                    SELECT DISTINCT ON (p.sku) p.sku, i.stock_on_hand, i.snapshot_at
                    FROM Fact_Inventory_Daily i JOIN Dim_Products p USING(product_key)
                    WHERE p.sku LIKE :prefix ORDER BY p.sku, i.snapshot_at DESC NULLS LAST
                """), {"prefix": run.get("run_id", "NO_ACTIVE_RUN") + "-%"}).mappings()]
                origin = model.get("training_data_origin")
                if origin == "public_anonymized_historical_csv":
                    datasets = model.get("source_datasets") or []
                    catalog = [dict(r) for r in conn.execute(text("""
                        SELECT sku, COALESCE(MAX(product_name), sku) AS name, MAX(create_time)::date AS history_end
                        FROM Fact_Historical_Order_Lines WHERE data_origin=:origin AND NOT is_cancelled
                        AND (:all_sources OR source_dataset = ANY(CAST(:datasets AS TEXT[])))
                        GROUP BY sku ORDER BY COUNT(*) DESC LIMIT 40
                    """), {"origin": origin, "all_sources": not datasets, "datasets": datasets}).mappings()]
                else:
                    catalog = [dict(r) for r in conn.execute(text("""
                        SELECT p.sku, p.product_name AS name, MAX(f.create_time)::date AS history_end
                        FROM Dim_Products p JOIN Fact_Orders f USING(product_key)
                        WHERE NOT f.is_cancelled AND p.sku NOT LIKE 'WEB%'
                        GROUP BY p.sku, p.product_name ORDER BY COUNT(*) DESC LIMIT 40
                    """)).mappings()]
                return {"ok": True, **totals, "run_count": len(rows), "rows": rows, "stocks": stocks, "catalog": catalog}
        finally:
            engine.dispose()

    def lake(self, run):
        from minio import Minio
        import urllib3
        client = Minio(os.getenv("MINIO_ENDPOINT_HOST", "localhost:9000"),
                       access_key=os.getenv("MINIO_ROOT_USER", "minioadmin"),
                       secret_key=os.getenv("MINIO_ROOT_PASSWORD", "minioadmin"),
                       secure=os.getenv("MINIO_SECURE", "false").lower() == "true",
                       http_client=urllib3.PoolManager(timeout=urllib3.Timeout(connect=2, read=5), retries=False))
        bucket = os.getenv("MINIO_BUCKET_RAW", "ecom-raw-lake")
        if not client.bucket_exists(bucket):
            raise RuntimeError("MissingBucket")
        result = {"ok": True, "raw_rows": 0, "objects": [], "rejected": [], "rejected_count": 0}
        if not run.get("run_id"):
            return result
        from warehouse.etl.extract import read_parquet_from_minio
        prefix = run["run_id"] + "-"
        dates = {str(e["payload"].get("create_time") or e["payload"].get("created_time"))[:10]
                 for e in run.get("events", []) if e["topic"] == "orders"}
        # Source timestamps are UTC; inspect both possible business dates.
        days = set()
        for value in dates:
            date = datetime.fromisoformat(value)
            days.update((date, date + timedelta(days=1)))
        for day in sorted(days):
            for platform in ("shopee", "tiktok"):
                path = f"raw/orders/platform={platform}/year={day.year:04d}/month={day.month:02d}/day={day.day:02d}/"
                for obj in client.list_objects(bucket, prefix=path, recursive=True):
                    if not obj.object_name.endswith(".parquet") or obj.last_modified.timestamp() < run["started_at"] - 3:
                        continue
                    frame = read_parquet_from_minio(client, bucket, obj.object_name)
                    records = frame.to_dict("records")
                    count = sum(any(str(r.get(k, "")).startswith(prefix) for k in ("order_sn", "order_id")) for r in records)
                    if count:
                        result["raw_rows"] += count
                        result["objects"].append({"name": obj.object_name, "rows": count})
        # Match exact labelled records, never present total quarantine as this run.
        for obj in client.list_objects(bucket, prefix="quarantine/orders/", recursive=True):
            if obj.last_modified.timestamp() < run["started_at"] - 3:
                continue
            response = client.get_object(bucket, obj.object_name)
            try:
                for line in response.read().decode("utf-8").splitlines():
                    row = json.loads(line)
                    if str(row.get("order_id", "")).startswith(prefix):
                        result["rejected_count"] += 1
                        if len(result["rejected"]) < 20:
                            result["rejected"].append({"order_id": row["order_id"], "reasons": row.get("rejection_reasons"), "object": obj.object_name})
            finally:
                response.close()
                response.release_conn()
        return result

    @staticmethod
    def safe_probe(fn, *args):
        try:
            return fn(*args)
        except Exception as exc:
            LOG.warning("Probe unavailable: %s", type(exc).__name__)
            return {"ok": False, "error": type(exc).__name__}

    def refresh(self):
        while True:
            with self.lock:
                run = json.loads(json.dumps(self.run))
            with ThreadPoolExecutor(max_workers=6) as pool:
                calls = {name: pool.submit(api_call, path) for name, path in (
                    ("health", "/health"), ("ready", "/ready"), ("model", "/model/metadata"), ("training", "/training/status"))}
                lake_future = pool.submit(self.safe_probe, self.lake, run)
                api = {name: future.result() for name, future in calls.items()}
                model = api["model"].get("data") or {}
                warehouse = self.safe_probe(self.warehouse, model, run)
                lake = lake_future.result()
            heartbeat = read_json(ROOT / "data/pipeline/status.json", {})
            checkpoint = read_json(ROOT / "data/pipeline/checkpoint.json", {})
            age = max(0, time.time() - heartbeat.get("checked_at", 0)) if heartbeat else None
            worker_ok = heartbeat.get("status") == "healthy" and age is not None and age < 180
            completed = set(checkpoint.get("completed_objects", []))
            lake["processed_objects"] = sum(obj["name"] in completed for obj in lake.get("objects", []))
            csvs = [{"name": path.name, "modified_at": path.stat().st_mtime, "bytes": path.stat().st_size}
                    for path in sorted((ROOT / "powerbi/data").glob("*.csv"))]
            try:
                kafka_address = os.getenv("KAFKA_BOOTSTRAP_SERVERS_HOST", "localhost:29092").split(",")[0]
                host, port = kafka_address.rsplit(":", 1)
                with socket.create_connection((host, int(port)), timeout=2):
                    kafka_ok = True
            except (OSError, ValueError):
                kafka_ok = False
            snapshot = {"checked_at": time.time(), "loading": False, "api": api,
                        "run_id": run.get("run_id"), "warehouse": warehouse, "lake": lake,
                        "worker": {**heartbeat, "age_seconds": age, "ok": worker_ok,
                                   "checkpoint_at": checkpoint.get("updated_at"), "last_etl": checkpoint.get("last_etl")},
                        "csvs": csvs, "services": {
                            "kafka": {"ok": kafka_ok, "detail": "Cổng kết nối; ACK của đợt phát là bằng chứng ghi."},
                            "minio": {"ok": lake.get("ok", False)},
                            "postgres": {"ok": warehouse.get("ok", False)},
                            "worker": {"ok": worker_ok},
                            "serving": {"ok": api["health"]["ok"]},
                            "model": {"ok": api["ready"]["ok"]}}}
            with self.lock:
                self.snapshot = snapshot
            time.sleep(4)

    def state(self):
        with self.lock:
            state = {**self.snapshot, "run": public_run(self.run), "csrf": TOKEN, "last_queries": dict(self.queries),
                     "notes": ["Dữ liệu WEB… là dữ liệu mô phỏng có nhãn, được gửi qua Kafka thật.",
                               "Model hiện có dùng lịch sử riêng. Đơn vừa phát không tự huấn luyện model.",
                               "Readiness và kiểm tra luồng không phải bằng chứng chất lượng dự báo."]}
            # Do not combine evidence from an old run with a newly-started run.
            if self.snapshot.get("run_id") != self.run.get("run_id"):
                state["warehouse"] = {"ok": False, "pending": True}
                state["lake"] = {"ok": False, "pending": True}
            return state

    def record_query(self, name, request, result):
        with self.lock:
            self.queries[name] = {"checked_at": time.time(), "request": request, "response": result}


class Handler(BaseHTTPRequestHandler):
    console: Console

    def log_message(self, fmt, *args):
        LOG.debug(fmt, *args)

    def json_response(self, value, code=200):
        payload = json.dumps(value, ensure_ascii=False, default=str, allow_nan=False).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.end_headers()
        self.wfile.write(payload)

    def allowed_host(self):
        return self.headers.get("Host") in {f"localhost:{self.server.server_port}", f"127.0.0.1:{self.server.server_port}"}

    def do_GET(self):
        if not self.allowed_host():
            return self.json_response({"error": "Trang demo chỉ phục vụ trên localhost."}, 403)
        path = urlsplit(self.path).path
        if path == "/api/state":
            return self.json_response(self.console.state())
        if path == "/api/report":
            report = self.console.state()
            report.pop("csrf", None)
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Disposition", 'attachment; filename="demo-evidence.json"')
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(json.dumps(report, ensure_ascii=False, indent=2, default=str).encode())
            return
        assets = {"/": ("index.html", "text/html"), "/app.js": ("app.js", "text/javascript"), "/style.css": ("style.css", "text/css")}
        if path not in assets:
            return self.json_response({"error": "Không tìm thấy."}, 404)
        name, mime = assets[path]
        payload = (WEB / name).read_bytes()
        self.send_response(200)
        self.send_header("Content-Type", mime + "; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Security-Policy", "default-src 'self'; style-src 'self'; script-src 'self'; connect-src 'self'; frame-ancestors 'none'; base-uri 'none'")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.end_headers()
        self.wfile.write(payload)

    def do_POST(self):
        if not self.allowed_host() or self.headers.get("Origin") not in {None, "http://" + self.headers.get("Host", "")} or not secrets.compare_digest(self.headers.get("X-Demo-Token", ""), TOKEN):
            return self.json_response({"error": "Yêu cầu phải được gửi từ trang demo này."}, 403)
        try:
            size = int(self.headers.get("Content-Length", "0"))
            if not 0 < size <= 8192:
                raise ValueError("Nội dung yêu cầu không hợp lệ.")
            body = json.loads(self.rfile.read(size))
            if not isinstance(body, dict):
                raise ValueError("Nội dung phải là một đối tượng JSON.")
            path = urlsplit(self.path).path
            if path == "/api/demo/start":
                return self.json_response(self.console.start(body), 202)
            if path == "/api/demo/replay":
                return self.json_response(self.console.start(body, replay=True), 202)
            if path == "/api/demo/stop":
                self.console.stop.set()
                return self.json_response({"message": "Đã yêu cầu dừng phát; chờ bản tin đang gửi được xác nhận."})
            if path in {"/api/predict", "/api/inventory"}:
                sku = body.get("sku")
                if not isinstance(sku, str) or not 1 <= len(sku) <= 50:
                    raise ValueError("Hãy chọn một mã SKU hợp lệ.")
                if path == "/api/predict":
                    date = datetime.fromisoformat(body["from_date"]).date()
                    days = body.get("days", 7)
                    if type(days) is not int or not 1 <= days <= 14:
                        raise ValueError("Khoảng dự báo từ 1 đến 14 ngày.")
                    result = api_call("/predict/demand", {"sku": sku, "from_date": str(date), "to_date": str(date + timedelta(days=days-1)), "confidence_interval": True})
                else:
                    lead = body.get("lead_time_days", 3)
                    if type(lead) is not int or not 1 <= lead <= 30:
                        raise ValueError("Thời gian giao hàng từ 1 đến 30 ngày.")
                    payload = {"skus": [sku], "lead_time_days": lead, "service_level": 0.95}
                    if body.get("current_stock") is not None:
                        stock = body["current_stock"]
                        if type(stock) is not int or not 0 <= stock <= 1000000:
                            raise ValueError("Tồn kho phải là số nguyên từ 0 đến 1.000.000.")
                        payload["current_stocks"] = {sku: stock}
                    result = api_call("/inventory/reorder-alert", payload)
                self.console.record_query("forecast" if path == "/api/predict" else "inventory", body, result)
                return self.json_response(result, 200 if result["ok"] else 502)
            return self.json_response({"error": "Không tìm thấy."}, 404)
        except (ValueError, KeyError) as exc:
            return self.json_response({"error": str(exc)}, 400)
        except Exception as exc:
            LOG.exception("Action failed")
            return self.json_response({"error": f"Chưa thực hiện được ({type(exc).__name__}). Kiểm tra kết nối hệ thống."}, 503)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8050)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)
    Handler.console = Console()
    threading.Thread(target=Handler.console.refresh, daemon=True).start()
    server = ThreadingHTTPServer((args.host, args.port), Handler)
    print(f"DEMO: http://localhost:{args.port}", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        Handler.console.stop.set()
        if Handler.console.job:
            Handler.console.job.join(timeout=25)
        server.server_close()


if __name__ == "__main__":
    main()
