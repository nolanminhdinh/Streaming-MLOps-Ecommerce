"""Dựng trang tổng hợp (biểu đồ SVG) từ kết quả check_streaming_soak.py và chụp thành ảnh PNG bằng Chrome headless.

    python scripts/render_soak_report.py output/streaming-soak-2026-11-19
"""
import csv
import html
import json
from pathlib import Path
import statistics
import subprocess
import sys

CHROME = r"C:\Program Files\Google\Chrome\Application\chrome.exe"
COLORS = ["#2a6fdb", "#e8912d", "#3a9d5d", "#d64545", "#7a5ba6"]


def capture_page(url, target, width, height):
    """Chụp trang bằng Chrome DevTools (dùng lại client WebSocket của capture_service_screenshots)."""
    import base64, json as _json, tempfile, time, urllib.request
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    from capture_service_screenshots import WS, PORT
    chrome = subprocess.Popen([CHROME, "--headless=new", f"--remote-debugging-port={PORT}",
                               f"--user-data-dir={tempfile.mkdtemp(prefix='chrome-cdp-')}", "--hide-scrollbars", "about:blank"],
                              stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        for _ in range(60):
            try:
                page = [t for t in _json.load(urllib.request.urlopen(f"http://127.0.0.1:{PORT}/json/list")) if t["type"] == "page"][0]
                break
            except Exception:
                time.sleep(0.5)
        ws = WS(page["webSocketDebuggerUrl"])
        ws.call("Page.enable")
        ws.call("Emulation.setDeviceMetricsOverride", width=width, height=height, deviceScaleFactor=1, mobile=False)
        ws.call("Page.navigate", url=url)
        time.sleep(3)
        target.write_bytes(base64.b64decode(ws.call("Page.captureScreenshot", format="png")["data"]))
    finally:
        chrome.terminate()


def num(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def chart(title, unit, xs, series, width=640, height=230):
    left, right, top, bottom = 52, 14, 30, 34
    values = [v for _, ys in series for v in ys if v is not None]
    ymax = max(values) * 1.1 if values and max(values) > 0 else 1
    xmax = max(xs) if xs and max(xs) > 0 else 1
    sx = lambda x: left + (width - left - right) * x / xmax
    sy = lambda y: height - bottom - (height - top - bottom) * y / ymax
    parts = [f'<text x="{left}" y="18" class="t">{html.escape(title)}</text>']
    for i in range(5):
        y = ymax * i / 4
        parts.append(f'<line x1="{left}" x2="{width - right}" y1="{sy(y):.1f}" y2="{sy(y):.1f}" class="g"/>'
                     f'<text x="{left - 6}" y="{sy(y) + 4:.1f}" class="a" text-anchor="end">{y:,.1f}</text>')
    for m in range(0, int(xmax) + 1, 10):
        parts.append(f'<text x="{sx(m):.1f}" y="{height - 14}" class="a" text-anchor="middle">{m}</text>')
    parts.append(f'<text x="{width - right}" y="{height - 2}" class="a" text-anchor="end">phút</text>'
                 f'<text x="6" y="{top - 6}" class="a">{html.escape(unit)}</text>')
    legend = []
    for i, (name, ys) in enumerate(series):
        pts = " ".join(f"{sx(x):.1f},{sy(y):.1f}" for x, y in zip(xs, ys) if y is not None)
        parts.append(f'<polyline points="{pts}" fill="none" stroke="{COLORS[i]}" stroke-width="2"/>')
        legend.append(f'<span><i style="background:{COLORS[i]}"></i>{html.escape(name)}</span>')
    return (f'<div class="c"><svg width="{width}" height="{height}">{"".join(parts)}</svg>'
            f'<div class="l">{"".join(legend)}</div></div>')


def main():
    folder = Path(sys.argv[1])
    rows = list(csv.DictReader(open(folder / "samples.csv", encoding="utf-8")))
    crow = list(csv.DictReader(open(folder / "containers.csv", encoding="utf-8")))
    summary = json.loads((folder / "summary.json").read_text(encoding="utf-8"))
    xs = [num(r["elapsed_min"]) for r in rows]
    col = lambda k: [num(r[k]) for r in rows]
    cores = [v / 100 if v is not None else None for v in col("stack_cpu_pct")]

    def stats(values):
        v = [x for x in values if x is not None]
        return {"tb": round(statistics.mean(v), 2), "max": round(max(v), 2), "min": round(min(v), 2)} if v else {}

    per = {}
    for r in crow:
        if r["in_stack"] != "True":
            continue
        per.setdefault(r["container"], {"cpu": [], "mem": []})
        per[r["container"]]["cpu"].append(float(r["cpu_pct"]))
        per[r["container"]]["mem"].append(float(r["mem_mb"]))
    table = sorted(((n, statistics.mean(v["cpu"]), max(v["cpu"]), statistics.mean(v["mem"]), max(v["mem"]))
                    for n, v in per.items()), key=lambda t: -t[3])
    metrics = {
        "samples": len(rows), "duration_min": xs[-1] if xs else 0,
        "host_cpu_pct": stats(col("host_cpu_pct")), "host_ram_used_gb": stats(col("host_ram_used_gb")),
        "host_ram_total_gb": num(rows[0]["host_ram_total_gb"]), "docker_vm_ram_gb": stats(col("docker_vm_ram_gb")),
        "stack_cpu_cores": stats(cores), "stack_mem_gb": stats(col("stack_mem_gb")), "other_mem_gb": stats(col("other_mem_gb")),
        "other_cpu_pct": stats(col("other_cpu_pct")), "producer_ram_mb": stats(col("producer_ram_mb")),
        "producer_rate": stats(col("producer_rate")), "lag_minio": stats(col("lag_minio")),
        "lag_inventory": stats(col("lag_inventory")), "pending_objects": stats(col("pending_objects")),
        "p50_s": stats(col("ingest_to_wh_p50_s")), "p95_s": stats(col("ingest_to_wh_p95_s")),
        "containers": [{"container": n, "cpu_tb": round(a, 2), "cpu_max": round(b, 2), "mem_tb_mb": round(c, 1),
                        "mem_max_mb": round(d, 1)} for n, a, b, c, d in table],
    }
    (folder / "metrics.json").write_text(json.dumps(metrics, ensure_ascii=False, indent=2), encoding="utf-8")
    base = summary["baseline"]
    added = [num(r["fact_orders"]) - base["fact_orders"] for r in rows]
    trs = "".join(f"<tr><td>{html.escape(n)}</td><td>{a:.2f}</td><td>{b:.2f}</td><td>{c:,.0f}</td><td>{d:,.0f}</td></tr>"
                  for n, a, b, c, d in table)
    page = f"""<!doctype html><meta charset="utf-8"><style>
body{{font:13px Segoe UI,Arial;margin:20px;color:#1d2433;background:#fff}} h1{{font-size:20px;margin:0 0 4px}}
.m{{color:#5b6475;margin-bottom:10px}} .grid{{display:grid;grid-template-columns:repeat(2,660px);gap:10px}}
.c{{border:1px solid #d9dee8;border-radius:6px;padding:6px}} .t{{font:600 13px Segoe UI}} .a{{font:11px Segoe UI;fill:#5b6475}}
.g{{stroke:#e6e9ef}} .l span{{margin-right:14px;font-size:12px}} .l i{{display:inline-block;width:10px;height:10px;margin-right:4px}}
table{{border-collapse:collapse;margin-top:12px}} td,th{{border:1px solid #d9dee8;padding:4px 8px;text-align:right}}
td:first-child,th:first-child{{text-align:left}} th{{background:#f0f3f8}}</style>
<h1>Kiểm tra vận hành luồng streaming liên tục</h1>
<div class="m">Bắt đầu {html.escape(base['started_at'][:19])} · {len(rows)} mẫu, mỗi 30 giây · producer gửi
{summary['producer_events_logged']:,} sự kiện ghi trong log · {summary['orders_messages_in_kafka']:,} bản tin đơn vào Kafka</div>
<div class="grid">
{chart("CPU toàn máy", "%", xs, [("CPU máy (%)", col("host_cpu_pct"))])}
{chart("CPU của stack đồ án (13 container)", "lõi", xs, [("Stack (lõi CPU)", cores)])}
{chart("RAM", "GB", xs, [("RAM máy đang dùng", col("host_ram_used_gb")), ("Máy ảo Docker", col("docker_vm_ram_gb")),
                          ("Container đồ án", col("stack_mem_gb")), ("Container khác (CVAT)", col("other_mem_gb"))])}
{chart("Sự kiện và dữ liệu vào kho", "dòng", xs, [("Producer đã gửi", col("producer_sent")), ("Dòng Fact_Orders tăng thêm", added)])}
{chart("Độ trễ nhận → kho (lượt ETL gần nhất)", "giây", xs, [("p50", col("ingest_to_wh_p50_s")), ("p95", col("ingest_to_wh_p95_s"))])}
{chart("Tồn đọng", "bản tin / object", xs, [("Lag Kafka đơn hàng", col("lag_minio")), ("Lag Kafka tồn kho", col("lag_inventory")),
                                          ("Object chờ ETL", col("pending_objects"))])}
</div>
<table><tr><th>Container</th><th>CPU TB (%)</th><th>CPU max (%)</th><th>RAM TB (MB)</th><th>RAM max (MB)</th></tr>{trs}</table>"""
    (folder / "report.html").write_text(page, encoding="utf-8")
    shots = folder / "screenshots"
    shots.mkdir(exist_ok=True)
    capture_page((folder / "report.html").resolve().as_uri(), shots / "00-tai-nguyen-va-van-hanh.png", 1400, 1450)
    print(json.dumps({k: v for k, v in metrics.items() if k != "containers"}, ensure_ascii=False))


if __name__ == "__main__":
    main()
