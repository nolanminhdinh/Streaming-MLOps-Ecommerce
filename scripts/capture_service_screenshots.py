"""Chụp màn hình giao diện và kết quả vận hành của từng service trong luồng (Chrome headless qua DevTools Protocol).

- Service có giao diện web (FlowLab, Grafana, Prometheus, Alertmanager, MLflow, FastAPI, MinIO Console) được chụp
  trực tiếp; Grafana đăng nhập bằng HTTP Basic, MinIO Console đăng nhập qua API login của chính console.
- Service không có giao diện (Kafka, ZooKeeper, PostgreSQL, pipeline-worker và hai consumer, batch-forecaster,
  producer) được chụp từ trang hiển thị kết quả lệnh kiểm tra thật chạy tại thời điểm chụp.
Chỉ đọc; tài khoản lấy từ .env và không được ghi ra ảnh hay log.

    python scripts/capture_service_screenshots.py --out output/streaming-soak-2026-11-19/screenshots --tag giua-test
"""
import argparse
import base64
from datetime import datetime
import html
import json
import os
from pathlib import Path
import random
import socket
import struct
import subprocess
import sys
import tempfile
import time
import urllib.request
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
CHROME = r"C:\Program Files\Google\Chrome\Application\chrome.exe"
PORT = 9333


class WS:
    """WebSocket client tối giản (RFC 6455) đủ cho Chrome DevTools Protocol."""

    def __init__(self, url):
        host_port, path = url[len("ws://"):].split("/", 1)
        host, port = host_port.split(":")
        self.sock = socket.create_connection((host, int(port)), timeout=60)
        key = base64.b64encode(os.urandom(16)).decode()
        self.sock.sendall((f"GET /{path} HTTP/1.1\r\nHost: {host_port}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"
                           f"Sec-WebSocket-Key: {key}\r\nSec-WebSocket-Version: 13\r\n\r\n").encode())
        buf = b""
        while b"\r\n\r\n" not in buf:
            buf += self.sock.recv(4096)
        self.rest = buf.split(b"\r\n\r\n", 1)[1]
        self.next_id = 0

    def _read(self, n):
        while len(self.rest) < n:
            chunk = self.sock.recv(1 << 20)
            if not chunk:
                raise ConnectionError("websocket closed")
            self.rest += chunk
        data, self.rest = self.rest[:n], self.rest[n:]
        return data

    def send(self, text):
        payload = text.encode()
        header = bytes([0x81])
        n = len(payload)
        if n < 126:
            header += bytes([0x80 | n])
        elif n < 65536:
            header += bytes([0x80 | 126]) + struct.pack(">H", n)
        else:
            header += bytes([0x80 | 127]) + struct.pack(">Q", n)
        mask = os.urandom(4)
        self.sock.sendall(header + mask + bytes(b ^ mask[i % 4] for i, b in enumerate(payload)))

    def recv(self):
        message = b""
        while True:
            b1, b2 = self._read(2)
            n = b2 & 0x7F
            if n == 126:
                n = struct.unpack(">H", self._read(2))[0]
            elif n == 127:
                n = struct.unpack(">Q", self._read(8))[0]
            message += self._read(n)
            if b1 & 0x80:
                return json.loads(message.decode())

    def call(self, method, **params):
        self.next_id += 1
        self.send(json.dumps({"id": self.next_id, "method": method, "params": params}))
        while True:
            msg = self.recv()
            if msg.get("id") == self.next_id:
                if "error" in msg:
                    raise RuntimeError(f"{method}: {msg['error']}")
                return msg.get("result", {})


def env_values():
    values = {}
    for line in (ROOT / ".env").read_text(encoding="utf-8").splitlines():
        if "=" in line and not line.lstrip().startswith("#"):
            key, value = line.split("=", 1)
            values[key.strip()] = value.strip().strip('"')
    return values


def sh(cmd, timeout=90):
    out = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8", errors="replace", timeout=timeout, cwd=ROOT)
    return (out.stdout + out.stderr).strip()


def evidence_page(path, title, sections):
    now = datetime.now(ZoneInfo("Asia/Ho_Chi_Minh")).strftime("%d/%m/%Y %H:%M:%S")
    blocks = "".join(f"<h2>{html.escape(name)}</h2><pre>{html.escape(body)}</pre>" for name, body in sections)
    path.write_text(f"""<!doctype html><meta charset="utf-8"><title>{html.escape(title)}</title>
<style>body{{font:14px/1.45 Segoe UI,Arial;margin:24px;background:#fff;color:#1d2433}}
h1{{font-size:22px;margin:0 0 4px}} .meta{{color:#5b6475;margin-bottom:14px}}
h2{{font-size:15px;margin:16px 0 6px;color:#24508f}}
pre{{background:#f4f6fa;border:1px solid #d9dee8;border-radius:6px;padding:10px;font:12.5px/1.4 Consolas,monospace;
white-space:pre-wrap;word-break:break-all;margin:0}}</style>
<h1>{html.escape(title)}</h1><div class="meta">Kết quả lệnh kiểm tra chạy lúc {now} (giờ Việt Nam)</div>{blocks}""",
                    encoding="utf-8")
    return path.resolve().as_uri()


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--out", required=True)
    parser.add_argument("--tag", default="")
    parser.add_argument("--soak-dir", default=None, help="thư mục test để lấy producer.log")
    parser.add_argument("--only", default="", help="chỉ chụp các ảnh có tên bắt đầu bằng các tiền tố này (phân cách dấu phẩy)")
    args = parser.parse_args()
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    pages_dir = out / "_pages"
    pages_dir.mkdir(exist_ok=True)
    env = env_values()
    suffix = f"-{args.tag}" if args.tag else ""
    pg = 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c'

    def psql(sql):
        return sh(["docker", "compose", "exec", "-T", "postgres", "sh", "-c", f"{pg} \"{sql}\""])

    producer_log = ""
    if args.soak_dir and (Path(args.soak_dir) / "producer.log").exists():
        producer_log = "\n".join((Path(args.soak_dir) / "producer.log").read_text(encoding="utf-8", errors="replace")
                                 .splitlines()[-25:])
    evidence = {
        "05-kafka": ("Kafka – topic và consumer group", [
            ("kafka-topics --describe (hai topic của luồng)", "\n".join(
                sh(["docker", "compose", "exec", "-T", "kafka", "kafka-topics", "--bootstrap-server", "localhost:9092",
                    "--describe", "--topic", topic]) for topic in ("ecom.orders.raw", "inventory.logs"))),
            ("kafka-consumer-groups --describe --all-groups (LAG = số bản tin chưa xử lý)",
             sh(["docker", "compose", "exec", "-T", "kafka", "kafka-consumer-groups", "--bootstrap-server", "localhost:9092",
                 "--describe", "--all-groups"]))]),
        "06-zookeeper": ("ZooKeeper – điều phối broker Kafka", [
            ("zookeeper-shell: broker đang đăng ký", sh(["docker", "compose", "exec", "-T", "zookeeper", "zookeeper-shell",
                                                        "localhost:2181", "ls", "/brokers/ids"])),
            ("docker compose ps zookeeper kafka", sh(["docker", "compose", "ps", "zookeeper", "kafka"]))]),
        "08-postgresql": ("PostgreSQL – kho dữ liệu và data mart", [
            ("Số dòng các bảng chính", psql("SELECT 'Fact_Orders' bang, COUNT(*) FROM Fact_Orders UNION ALL SELECT "
                                            "'Fact_Inventory_Daily', COUNT(*) FROM Fact_Inventory_Daily UNION ALL SELECT "
                                            "'mart.fact_sales_line', COUNT(*) FROM mart.fact_sales_line UNION ALL SELECT "
                                            "'mart.fact_replenishment_daily', COUNT(*) FROM mart.fact_replenishment_daily;")),
            ("10 dòng đơn nạp gần nhất", psql("SELECT order_id, platform, order_status, quantity, create_time, loaded_at "
                                              "FROM Fact_Orders ORDER BY loaded_at DESC LIMIT 10;")),
            ("Số dòng nạp theo phút (10 phút gần nhất)", psql("SELECT date_trunc('minute', loaded_at) phut, COUNT(*) "
                                                              "FROM Fact_Orders WHERE loaded_at > NOW() - interval '10 minutes' "
                                                              "GROUP BY 1 ORDER BY 1;")),
            ("Kết nối đang mở", psql("SELECT state, COUNT(*) FROM pg_stat_activity GROUP BY 1;"))]),
        "09-pipeline-worker": ("pipeline-worker – điều phối consumer, ETL, coverage, mart", [
            ("data/pipeline/status.json (heartbeat)", (ROOT / "data" / "pipeline" / "status.json").read_text(encoding="utf-8")),
            ("docker compose logs --tail 30 pipeline-worker", sh(["docker", "compose", "logs", "--tail", "30", "--no-color",
                                                                  "pipeline-worker"]))]),
        "10-consumers": ("Consumer đơn hàng (→ MinIO) và consumer tồn kho (→ PostgreSQL)", [
            ("Log consumer trong pipeline-worker", "\n".join(
                line for line in sh(["docker", "compose", "logs", "--tail", "400", "--no-color", "pipeline-worker"]).splitlines()
                if "consumer" in line.lower() or "minio" in line.lower() or "inventory" in line.lower())[-6000:]),
            ("Snapshot tồn kho mới nhất", psql("SELECT p.sku, f.stock_on_hand, f.snapshot_at FROM Fact_Inventory_Daily f "
                                               "JOIN Dim_Products p USING (product_key) ORDER BY f.snapshot_at DESC NULLS LAST LIMIT 8;"))]),
        "12-batch-forecaster": ("batch-forecaster – dự báo 14 ngày theo lô", [
            ("Model_Serving_Snapshot gần nhất", psql("SELECT snapshot_at, model_version, algorithm, history_end, forecast_from, "
                                                     "forecast_to, skus_model, skus_heuristic, skus_failed FROM Model_Serving_Snapshot "
                                                     "ORDER BY snapshot_at DESC LIMIT 5;")),
            ("docker compose logs --tail 20 batch-forecaster", sh(["docker", "compose", "logs", "--tail", "20", "--no-color",
                                                                   "batch-forecaster"]))]),
        "13-docker-stats": ("Tài nguyên container (docker stats)", [
            ("docker compose ps", sh(["docker", "compose", "--profile", "demo", "ps", "--format",
                                      "table {{.Service}}\t{{.Status}}"])),
            ("docker stats --no-stream", sh(["docker", "stats", "--no-stream", "--format",
                                             "table {{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}\t{{.NetIO}}"]))]),
    }
    if producer_log:
        evidence["04-producer"] = ("Producer – sinh và gửi sự kiện vào Kafka", [("producer.log (25 dòng cuối)", producer_log)])

    auth = "Basic " + base64.b64encode(
        f"{env.get('GRAFANA_ADMIN_USER', 'admin')}:{env.get('GRAFANA_ADMIN_PASSWORD', 'admin')}".encode()).decode()
    shots = [
        ("01-flowlab-demo", "http://localhost:8050/", 6, None),
        # Grafana cuộn trong khung riêng: đặt sẵn khung nhìn đủ cao để hiện toàn bộ panel.
        ("02-grafana-dashboard", "http://localhost:3000/d/ecom-mlops-dashboard?orgId=1&from=now-1h&to=now&kiosk", 15, "grafana",
         1450, None),
        ("03-prometheus-targets", "http://localhost:9090/targets", 5, None),
        ("03b-prometheus-alerts", "http://localhost:9090/alerts", 5, None),
        # Hộp thoại giấy phép AGPL của MinIO Console che nội dung: bấm Acknowledge trước khi chụp.
        ("07-minio-raw-orders", "http://localhost:9001/browser/ecom-raw-lake/raw%2Forders%2F", 7, "minio", None,
         "[...document.querySelectorAll('button')].filter(b=>b.innerText.trim()==='Acknowledge').forEach(b=>b.click())"),
        ("11-fastapi-pipeline-status", "http://localhost:8000/pipeline/status", 3, None),
        ("11b-fastapi-ready", "http://localhost:8000/ready", 3, None),
        ("11c-fastapi-docs", "http://localhost:8000/docs", 5, None),
        ("14-mlflow-models", "http://localhost:5000/#/models", 7, None),
        ("15-alertmanager", "http://localhost:9093/#/alerts", 5, None),
    ]
    for name, (title, sections) in evidence.items():
        shots.append((name, evidence_page(pages_dir / f"{name}.html", title, sections), 2, None))

    profile = tempfile.mkdtemp(prefix="chrome-cdp-")
    chrome = subprocess.Popen([CHROME, "--headless=new", f"--remote-debugging-port={PORT}", f"--user-data-dir={profile}",
                               "--hide-scrollbars", "--disable-gpu", "--no-first-run", "--lang=vi", "about:blank"],
                              stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        for _ in range(60):
            try:
                targets = json.load(urllib.request.urlopen(f"http://127.0.0.1:{PORT}/json/list"))
                page = [t for t in targets if t["type"] == "page"][0]
                break
            except Exception:
                time.sleep(0.5)
        ws = WS(page["webSocketDebuggerUrl"])
        ws.call("Page.enable")
        ws.call("Network.enable")
        ws.call("Emulation.setDeviceMetricsOverride", width=1440, height=900, deviceScaleFactor=1, mobile=False)
        logged_minio = False
        only = [x for x in args.only.split(",") if x]
        for shot_def in shots:
            name, url, wait, login = shot_def[:4]
            fixed_height, script = (list(shot_def[4:]) + [None, None])[:2]
            if only and not any(name.startswith(o) for o in only):
                continue
            if fixed_height:
                ws.call("Emulation.setDeviceMetricsOverride", width=1440, height=fixed_height, deviceScaleFactor=1, mobile=False)
            ws.call("Network.setExtraHTTPHeaders", headers={"Authorization": auth} if login == "grafana" else {})
            if login == "minio" and not logged_minio:
                ws.call("Page.navigate", url="http://localhost:9001/login")
                time.sleep(4)
                body = json.dumps({"accessKey": env.get("MINIO_ROOT_USER", ""), "secretKey": env.get("MINIO_ROOT_PASSWORD", "")})
                ws.call("Runtime.evaluate", expression=f"fetch('/api/v1/login',{{method:'POST',headers:{{'Content-Type':"
                                                       f"'application/json'}},body:{json.dumps(body)}}}).then(r=>r.status)",
                        awaitPromise=True)
                logged_minio = True
            ws.call("Page.navigate", url=url)
            time.sleep(wait)
            if script:
                ws.call("Runtime.evaluate", expression=script)
                time.sleep(2)
            height = fixed_height or ws.call("Runtime.evaluate", expression="Math.max(document.body.scrollHeight, 900)",
                                             returnByValue=True)["result"].get("value", 900)
            height = int(min(max(height, 900), 3400))
            ws.call("Emulation.setDeviceMetricsOverride", width=1440, height=height, deviceScaleFactor=1, mobile=False)
            time.sleep(1)
            shot = ws.call("Page.captureScreenshot", format="png")
            (out / f"{name}{suffix}.png").write_bytes(base64.b64decode(shot["data"]))
            ws.call("Emulation.setDeviceMetricsOverride", width=1440, height=900, deviceScaleFactor=1, mobile=False)
            print("đã chụp", name, height, flush=True)
    finally:
        chrome.terminate()


if __name__ == "__main__":
    main()
