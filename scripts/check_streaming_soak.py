"""Chạy luồng streaming liên tục trong một khoảng thời gian và ghi số đo vận hành, tài nguyên máy.

Script khởi động producer, mỗi --interval giây ghi lại:
  - CPU và RAM của máy (Windows, qua CIM) và bộ nhớ của máy ảo Docker (tiến trình vmmem);
  - CPU và RAM của từng container (docker stats);
  - lag Kafka theo consumer group, số object chờ ETL, độ trễ ingest → warehouse (data/pipeline/status.json);
  - số dòng trong Fact_Orders, Fact_Inventory_Daily và mart.fact_sales_line;
  - số sự kiện producer đã gửi.
Hết thời gian thì dừng producer, chờ hệ thống xử lý hết tồn đọng (lag 0, không còn object chờ) và ghi tổng kết.
Chỉ đọc kho dữ liệu; không chạy consumer hay ETL thủ công (pipeline-worker xử lý như vận hành thật).

    python scripts/check_streaming_soak.py --minutes 60 --interval 30
"""
import argparse
import csv
from datetime import datetime
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import time
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

from powerbi.export_powerbi_dataset import _database_url

VN = ZoneInfo("Asia/Ho_Chi_Minh")
HOST_PS = ("$p=(Get-CimInstance Win32_PerfFormattedData_PerfOS_Processor -Filter \"Name='_Total'\").PercentProcessorTime;"
           "$o=Get-CimInstance Win32_OperatingSystem;"
           "$v=(Get-Process vmmem*,VmmemWSL -ErrorAction SilentlyContinue | Measure-Object WorkingSet64 -Sum).Sum;"
           "$q=(Get-Process -Id PRODUCER_PID -ErrorAction SilentlyContinue).WorkingSet64;"
           "'{0}|{1}|{2}|{3}|{4}' -f $p,$o.TotalVisibleMemorySize,$o.FreePhysicalMemory,$v,$q")


def run(cmd, timeout=60):
    out = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8", errors="replace", timeout=timeout, cwd=ROOT)
    return out.stdout


def host_metrics(producer_pid=0):
    raw = run(["powershell", "-NoProfile", "-Command", HOST_PS.replace("PRODUCER_PID", str(producer_pid))]).strip().split("|")
    cpu, total_kb, free_kb, vmmem, producer_ws = (float(x or 0) for x in raw)
    return {"host_cpu_pct": cpu, "host_ram_total_gb": round(total_kb / 1048576, 2),
            "host_ram_used_gb": round((total_kb - free_kb) / 1048576, 2),
            "host_ram_used_pct": round((total_kb - free_kb) / total_kb * 100, 1),
            "docker_vm_ram_gb": round(vmmem / 1073741824, 2), "producer_ram_mb": round(producer_ws / 1048576, 1)}


def to_mb(value):
    number, unit = re.match(r"([\d.]+)\s*([KMGT]?i?B)", value).groups()
    scale = {"B": 1 / 1048576, "KiB": 1 / 1024, "MiB": 1, "GiB": 1024, "TiB": 1048576, "kB": 1 / 1000, "MB": 1, "GB": 1000}
    return float(number) * scale.get(unit, 1)


def container_metrics():
    """Mọi container đang chạy; cờ in_stack đánh dấu container thuộc stack đồ án (docker compose)."""
    stack = set(run(["docker", "compose", "--profile", "demo", "ps", "-q"]).split())
    rows = []
    for line in run(["docker", "stats", "--no-stream", "--format", "{{json .}}"], timeout=90).splitlines():
        s = json.loads(line)
        rows.append({"container": s["Name"], "in_stack": any(i.startswith(s["ID"]) for i in stack),
                     "cpu_pct": float(s["CPUPerc"].rstrip("%") or 0),
                     "mem_mb": round(to_mb(s["MemUsage"].split("/")[0].strip()), 1)})
    return rows


def kafka_lag():
    out = run(["docker", "compose", "exec", "-T", "kafka", "kafka-consumer-groups", "--bootstrap-server", "localhost:9092",
               "--describe", "--all-groups"], timeout=90)
    lag, end_offsets = {}, {}
    for line in out.splitlines():
        parts = line.split()
        if len(parts) >= 6 and parts[5].isdigit():
            group, topic = parts[0], parts[1]
            lag[group] = lag.get(group, 0) + int(parts[5])
            end_offsets[(topic, parts[2])] = int(parts[4])
    topics = {}
    for (topic, _), value in end_offsets.items():
        topics[topic] = topics.get(topic, 0) + value
    return lag, topics


def warehouse(conn):
    one = lambda sql: conn.execute(text(sql)).scalar()
    return {"fact_orders": one("SELECT COUNT(*) FROM Fact_Orders"),
            "fact_inventory": one("SELECT COUNT(*) FROM Fact_Inventory_Daily"),
            "mart_sales_line": one("SELECT COUNT(*) FROM mart.fact_sales_line"),
            "last_loaded_age_s": round(one("SELECT EXTRACT(EPOCH FROM NOW() - MAX(loaded_at)) FROM Fact_Orders") or 0, 1)}


def worker_status():
    s = json.loads((ROOT / "data" / "pipeline" / "status.json").read_text(encoding="utf-8"))
    latency = s.get("ingest_to_warehouse_seconds") or {}
    return {"worker_status": s.get("status"), "pending_objects": s.get("pending_objects"),
            "last_success_age_s": round(time.time() - (s.get("last_success_at") or 0), 1),
            "ingest_to_wh_p50_s": latency.get("p50"), "ingest_to_wh_p95_s": latency.get("p95")}


def producer_sent(log_path):
    found = re.findall(r"Đã gửi (\d+) events \(([\d.]+) evt/s\)", log_path.read_text(encoding="utf-8", errors="replace"))
    return (int(found[-1][0]), float(found[-1][1])) if found else (0, 0.0)


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--minutes", type=float, default=60)
    parser.add_argument("--interval", type=float, default=30)
    parser.add_argument("--out", default=None)
    parser.add_argument("--fixed-rate", type=int, default=None,
                        help="Thử tải: producer sinh đúng N sự kiện mỗi giây, bỏ hệ số mùa vụ (mặc định: tốc độ mô phỏng thường)")
    args = parser.parse_args()
    load_dotenv(ROOT / ".env")
    start = datetime.now(VN)
    out = Path(args.out) if args.out else ROOT / "output" / f"streaming-soak-{start:%Y-%m-%d-%H%M}"
    out.mkdir(parents=True, exist_ok=True)
    engine = create_engine(_database_url(), pool_pre_ping=True)
    conn = engine.connect()

    lag0, topics0 = kafka_lag()
    base = {"started_at": start.isoformat(), **warehouse(conn), "kafka_end_offsets": topics0, "kafka_lag": lag0,
            "host": host_metrics()}
    log_path = out / "producer.log"
    env = dict(os.environ, PYTHONIOENCODING="utf-8", PYTHONUNBUFFERED="1")
    producer_cmd = [sys.executable, str(ROOT / "ingestion" / "producer.py")]
    if args.fixed_rate:
        # Thử tải cao điểm: bộ phát dùng lại producer.py, giữ đúng N sự kiện mỗi giây (xem peak_load_producer.py).
        producer_cmd = [sys.executable, str(ROOT / "scripts" / "peak_load_producer.py"), "--rate", str(args.fixed_rate)]
        base["fixed_rate_evt_s"] = args.fixed_rate
    producer = subprocess.Popen(producer_cmd, cwd=ROOT, env=env,
                                stdout=open(log_path, "w", encoding="utf-8"), stderr=subprocess.STDOUT)
    (out / "producer.pid").write_text(str(producer.pid))
    print("producer pid", producer.pid, "→", out, flush=True)

    samples = open(out / "samples.csv", "w", newline="", encoding="utf-8")
    containers = open(out / "containers.csv", "w", newline="", encoding="utf-8")
    sample_writer = container_writer = None
    deadline = time.time() + args.minutes * 60
    tick, last_lag = 0, (lag0, topics0)
    try:
        while time.time() < deadline:
            t0 = time.time()
            now = datetime.now(VN)
            if tick % 2 == 0:
                last_lag = kafka_lag()
            lag, topics = last_lag
            sent, rate = producer_sent(log_path)
            row = {"time": now.strftime("%H:%M:%S"), "elapsed_min": round((time.time() - deadline) / 60 + args.minutes, 2),
                   **host_metrics(producer.pid), "producer_sent": sent, "producer_rate": rate,
                   "lag_minio": lag.get("minio-writer-group"), "lag_inventory": lag.get("inventory-postgres-writer-group"),
                   "orders_topic_offset": topics.get("ecom.orders.raw"), **worker_status(), **warehouse(conn),
                   "producer_alive": producer.poll() is None}
            stats = container_metrics()
            ours = [s for s in stats if s["in_stack"]]
            others = [s for s in stats if not s["in_stack"]]
            row["stack_containers"] = len(ours)
            row["stack_cpu_pct"] = round(sum(s["cpu_pct"] for s in ours), 1)
            row["stack_mem_gb"] = round(sum(s["mem_mb"] for s in ours) / 1024, 2)
            row["other_containers"] = len(others)
            row["other_cpu_pct"] = round(sum(s["cpu_pct"] for s in others), 1)
            row["other_mem_gb"] = round(sum(s["mem_mb"] for s in others) / 1024, 2)
            if sample_writer is None:
                sample_writer = csv.DictWriter(samples, fieldnames=list(row))
                sample_writer.writeheader()
                container_writer = csv.DictWriter(containers, fieldnames=["time", "container", "in_stack", "cpu_pct", "mem_mb"])
                container_writer.writeheader()
            sample_writer.writerow(row)
            for s in stats:
                container_writer.writerow({"time": row["time"], **s})
            samples.flush()
            containers.flush()
            print(json.dumps({k: row[k] for k in ("time", "elapsed_min", "host_cpu_pct", "host_ram_used_gb", "stack_cpu_pct", "stack_mem_gb", "producer_sent",
                                                  "lag_minio", "pending_objects", "fact_orders", "producer_alive")},
                             ensure_ascii=False), flush=True)
            tick += 1
            time.sleep(max(0, args.interval - (time.time() - t0)))
    finally:
        producer.terminate()
        producer.wait(timeout=30)
        stopped = datetime.now(VN)
        sent, rate = producer_sent(log_path)
        drain_start = time.time()
        while time.time() - drain_start < 600:
            lag, topics = kafka_lag()
            status = worker_status()
            # Chỉ xét hai consumer group của luồng chính (group NiFi ngoài phạm vi nếu còn trong Kafka thì bỏ qua).
            main_lag = lag.get("minio-writer-group", 0) + lag.get("inventory-postgres-writer-group", 0)
            if main_lag == 0 and status["pending_objects"] == 0:
                break
            time.sleep(15)
        time.sleep(45)  # một chu kỳ ETL nữa cho object cuối
        lag, topics = kafka_lag()
        final = {"stopped_at": stopped.isoformat(), "drained_seconds": round(time.time() - drain_start, 1),
                 **warehouse(conn), "kafka_end_offsets": topics, "kafka_lag": lag, **worker_status()}
        summary = {"baseline": base, "final": final, "producer_events_logged": sent, "producer_rate_evt_s": rate,
                   "orders_messages_in_kafka": topics.get("ecom.orders.raw", 0) - topics0.get("ecom.orders.raw", 0),
                   "inventory_messages_in_kafka": topics.get("inventory.logs", 0) - topics0.get("inventory.logs", 0),
                   "fact_orders_added": final["fact_orders"] - base["fact_orders"],
                   "fact_inventory_added": final["fact_inventory"] - base["fact_inventory"],
                   "mart_sales_line_added": final["mart_sales_line"] - base["mart_sales_line"]}
        (out / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
        print("SUMMARY", json.dumps(summary, ensure_ascii=False, default=str), flush=True)


if __name__ == "__main__":
    main()
