"""Gửi cùng một bộ dữ liệu cố định hai lần qua pipeline đang chạy và so sánh kết quả.

Bộ dữ liệu được sinh một lần (seed cố định), gắn nhãn riêng và đóng băng thành tệp
JSONL có SHA-256; hai lượt gửi đúng cùng chuỗi byte vào Kafka. Script không chạy
ETL/consumer thủ công: pipeline-worker xử lý như vận hành thật. Dữ liệu có nhãn được
giữ lại trong kho giống check_continuous_pipeline.py. Không train/đăng ký model.

    python scripts/check_replay_determinism.py            # dựng bộ dữ liệu + chạy 2 lượt + so sánh
    python scripts/check_replay_determinism.py --out-dir output/replay-determinism-2026-11-15
"""
import argparse
from collections import Counter
from datetime import datetime, timedelta, timezone
import hashlib
import io
import json
import logging
from pathlib import Path
import random
import sys
import time
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
import pyarrow.parquet as pq
from kafka import KafkaAdminClient, KafkaConsumer, KafkaProducer, TopicPartition
from sqlalchemy import text

from data_simulator.data_simulator import ECommerceSimulator
from ingestion.producer import KAFKA_BOOTSTRAP_SERVERS, TOPIC_INVENTORY, TOPIC_ORDERS
from warehouse.etl.extract import MINIO_BUCKET, build_minio_client
from warehouse.etl.load import get_engine

API = "http://localhost:8000"
STATUS = ROOT / "data/pipeline/status.json"
RUNS = ROOT / "data/pipeline/runs"
GROUPS = {TOPIC_ORDERS: "minio-writer-group", TOPIC_INVENTORY: "inventory-postgres-writer-group"}
SKU_FIELDS = ("item_sku", "model_sku", "seller_sku")
BASE_TIME = datetime(2026, 11, 15, 1, 0, tzinfo=timezone.utc)  # 08:00 giờ Việt Nam
TECHNICAL_COLUMNS = {"order_fact_id", "loaded_at", "inventory_fact_id", "order_line_key", "inventory_snapshot_key"}


def iso(value):
    return value.isoformat()


def relabel(event, label):
    event = dict(event)
    for key in ("order_sn", "order_id"):
        if event.get(key):
            event[key] = f"{label}-{event[key]}"
    for key in SKU_FIELDS:
        if event.get(key):
            event[key] = f"{label}-{event[key]}"
    return event


def set_version(event, status, updated):
    event = dict(event, order_status=status)
    if event.get("_platform") == "shopee":
        event.update(order_status_raw=status, update_time=iso(updated))
    else:
        event.update(updated_time=iso(updated), updatedAt=iso(updated))
    return event


def build_dataset(label, seed):
    """Trả về danh sách message (topic, key, value) và mô tả kỳ vọng."""
    random.seed(seed)
    simulator = ECommerceSimulator(start_date=BASE_TIME)
    events = [relabel(simulator.generate_event(BASE_TIME + timedelta(seconds=37 * i)), label) for i in range(24)]
    shopee = [e for e in events if e["_platform"] == "shopee"]
    tiktok = [e for e in events if e["_platform"] == "tiktok"]
    assert len(shopee) >= 4 and tiktok, "seed phải sinh đủ đơn Shopee và TikTok"
    messages, cases = [], {}

    def order(event, case=None):
        key = event.get("order_sn") or event.get("order_id")
        messages.append((TOPIC_ORDERS, key, json.dumps(event, ensure_ascii=False, sort_keys=True)))
        if case:
            cases.setdefault(case, []).append(key)

    for event in events:
        order(event)
    # Trùng tuyệt đối trong cùng lô: phải loại, không tạo thêm fact.
    order(shopee[0], "exact_duplicate")
    order(tiktok[0], "exact_duplicate")
    # Phiên bản sự kiện: bản mới hơn thắng, bản cũ đến sau không được ghi đè.
    for event in (shopee[1], tiktok[0]):
        newer = set_version(event, "COMPLETED", BASE_TIME + timedelta(hours=6))
        stale = set_version(event, "CANCELLED", BASE_TIME - timedelta(hours=6))
        order(newer, "newer_version_wins")
        order(stale, "stale_version_ignored")
    # Bản ghi lỗi: phải cách ly kèm lý do, không nạp kho.
    bad_price = dict(shopee[2], order_sn=f"{label}-BAD-PRICE", original_price="khong-phai-so")
    zero_qty = dict(shopee[3], order_sn=f"{label}-ZERO-QTY", quantity=0)
    no_platform = dict(shopee[2], order_sn=f"{label}-NO-PLATFORM", _platform=None)
    for event, case in ((bad_price, "invalid_price"), (zero_qty, "invalid_quantity"), (no_platform, "unknown_platform")):
        order(event, case)
    messages.append((TOPIC_ORDERS, f"{label}-BAD-JSON", '{"order_sn": "' + label + '-BAD-JSON", hong'))
    cases["invalid_json"] = [f"{label}-BAD-JSON"]

    # Tồn kho: 5 SKU có nhãn; một snapshot cũ đến sau và một snapshot âm.
    stock_rows = simulator.get_inventory_snapshot(as_of=BASE_TIME + timedelta(hours=1))[:5]
    expected_stock = {}
    for row in stock_rows:
        row = dict(row, sku=f"{label}-{row['sku']}", loaded_at=iso(BASE_TIME + timedelta(hours=1)))
        expected_stock[row["sku"]] = row["stock_on_hand"]
        messages.append((TOPIC_INVENTORY, row["sku"], json.dumps(row, ensure_ascii=False, sort_keys=True)))
    first, second = list(expected_stock)[:2]
    messages.append((TOPIC_INVENTORY, first, json.dumps({"sku": first, "stock_on_hand": 1, "loaded_at": iso(BASE_TIME)})))
    messages.append((TOPIC_INVENTORY, second, json.dumps({"sku": second, "stock_on_hand": -5, "loaded_at": iso(BASE_TIME + timedelta(hours=2))})))
    cases.update(stale_inventory_ignored=[first], negative_inventory=[second])

    valid_keys = {(e.get("order_sn") or e.get("order_id"), e["_platform"],
                   e.get("item_sku") if e["_platform"] == "shopee" else e.get("seller_sku")) for e in events}
    expected = {"order_messages": sum(t == TOPIC_ORDERS for t, _, _ in messages),
                "inventory_messages": sum(t == TOPIC_INVENTORY for t, _, _ in messages),
                "valid_order_lines": len(valid_keys), "expected_stock": expected_stock,
                "rejected_orders": 4, "rejected_inventory": 1, "cases": cases}
    return messages, expected


def freeze(out_dir, label, seed):
    path = out_dir / "dataset.jsonl"
    meta_path = out_dir / "dataset-manifest.json"
    if path.exists() and meta_path.exists():
        meta = json.loads(meta_path.read_text(encoding="utf-8"))
        if hashlib.sha256(path.read_bytes()).hexdigest() != meta["sha256"]:
            raise SystemExit("dataset.jsonl đã bị sửa so với manifest; không dùng để so sánh")
        return meta
    messages, expected = build_dataset(label, seed)
    body = "".join(json.dumps({"topic": t, "key": k, "value": v}, ensure_ascii=False) + "\n" for t, k, v in messages)
    path.write_text(body, encoding="utf-8", newline="\n")
    meta = {"label": label, "seed": seed, "base_time": iso(BASE_TIME), "messages": len(messages),
            "sha256": hashlib.sha256(path.read_bytes()).hexdigest(), "expected": expected}
    meta_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
    return meta


def end_offsets():
    consumer = KafkaConsumer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
    try:
        result = {}
        for topic in GROUPS:
            parts = [TopicPartition(topic, p) for p in consumer.partitions_for_topic(topic)]
            result[topic] = sum(consumer.end_offsets(parts).values())
        return result
    finally:
        consumer.close()


def group_lag():
    admin = KafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
    consumer = KafkaConsumer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
    try:
        lag = {}
        for topic, group in GROUPS.items():
            committed = {tp: meta.offset for tp, meta in admin.list_consumer_group_offsets(group).items() if tp.topic == topic}
            ends = consumer.end_offsets(list(committed))
            lag[group] = sum(ends[tp] - committed[tp] for tp in committed)
        return lag
    finally:
        admin.close(); consumer.close()


def publish(dataset_path):
    producer = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS, acks="all", retries=3)
    sent = Counter()
    try:
        for line in dataset_path.read_text(encoding="utf-8").splitlines():
            message = json.loads(line)
            producer.send(message["topic"], key=message["key"].encode(), value=message["value"].encode()).get(timeout=30)
            sent[message["topic"]] += 1
        producer.flush(timeout=30)
    finally:
        producer.close(timeout=5)
    return dict(sent)


def read_object(client, name):
    response = client.get_object(MINIO_BUCKET, name)
    try:
        return response.read()
    finally:
        response.close(); response.release_conn()


def lake_rows(client, label, since):
    """Đếm dòng có nhãn trong object raw/quarantine được tạo từ thời điểm `since`."""
    raw_rows, raw_objects, quarantine = 0, {}, {"orders": Counter(), "inventory": Counter()}
    quarantine_objects = []
    for obj in client.list_objects(MINIO_BUCKET, prefix="raw/orders/", recursive=True):
        if obj.last_modified < since or not obj.object_name.endswith(".parquet"):
            continue
        table = pq.read_table(io.BytesIO(read_object(client, obj.object_name))).to_pylist()
        count = sum(label in json.dumps(row, default=str, ensure_ascii=False) for row in table)
        if count:
            raw_rows += count
            raw_objects[obj.object_name] = (count, obj.last_modified.timestamp())
    for stream in quarantine:
        for obj in client.list_objects(MINIO_BUCKET, prefix=f"quarantine/{stream}/", recursive=True):
            if obj.last_modified < since:
                continue
            hits = [json.loads(line) for line in read_object(client, obj.object_name).decode("utf-8").splitlines()
                    if label in line]
            if hits:
                quarantine_objects.append(obj.object_name)
            for hit in hits:
                reasons = hit.get("rejection_reasons") or hit.get("reasons") or "UNKNOWN"
                codes = reasons if isinstance(reasons, list) else [c.strip() for c in str(reasons).split(";") if c.strip()]
                quarantine[stream]["records"] += 1
                for code in codes:
                    quarantine[stream][code[:80]] += 1
    return raw_rows, raw_objects, {k: dict(v) for k, v in quarantine.items()}, sorted(quarantine_objects)


def wait_until_processed(client, label, since, expected, timeout=360):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        raw_rows, raw_objects, quarantine, _ = lake_rows(client, label, since)
        status = json.loads(STATUS.read_text(encoding="utf-8"))
        newest = max((ts for _, ts in raw_objects.values()), default=None)
        etl_done = newest is not None and (status.get("last_success_at") or 0) > newest and not status.get("pending_objects")
        inventory_done = quarantine["inventory"].get("records", 0) >= expected["rejected_inventory"]
        if raw_rows >= expected["order_messages"] and etl_done and inventory_done and quarantine["orders"].get("records", 0) >= expected["rejected_orders"]:
            time.sleep(5)  # cho phép consumer tồn kho commit nốt snapshot hợp lệ
            return True
        time.sleep(3)
    return False


def digest(rows):
    business = [{k: v for k, v in row.items() if k not in TECHNICAL_COLUMNS} for row in rows]
    return hashlib.sha256(json.dumps(business, default=str, sort_keys=True).encode()).hexdigest()


def warehouse_state(engine, label):
    with engine.connect() as conn:
        facts = [dict(r) for r in conn.execute(text("""
            SELECT f.*, p.sku FROM Fact_Orders f JOIN Dim_Products p USING(product_key)
            WHERE f.order_id LIKE :p ORDER BY f.order_id, f.platform, f.source_line_id"""), {"p": label + "%"}).mappings()]
        bi_rows = [dict(r) for r in conn.execute(text("""
            SELECT * FROM bi.fact_order_line WHERE order_id LIKE :p ORDER BY order_id, platform, source_line_id"""),
            {"p": label + "%"}).mappings()]
        inventory = [dict(r) for r in conn.execute(text("""
            SELECT p.sku, i.date_key, i.stock_on_hand, i.snapshot_at, i.inventory_fact_id, i.loaded_at
            FROM Fact_Inventory_Daily i JOIN Dim_Products p USING(product_key)
            WHERE p.sku LIKE :p ORDER BY p.sku, i.date_key"""), {"p": label + "%"}).mappings()]
        products = conn.execute(text("SELECT COUNT(*) FROM Dim_Products WHERE sku LIKE :p"), {"p": label + "%"}).scalar()
        duplicate_keys = conn.execute(text("""SELECT COUNT(*) FROM (SELECT 1 FROM Fact_Orders WHERE order_id LIKE :p
            GROUP BY order_id, platform, source_line_id HAVING COUNT(*) > 1) d"""), {"p": label + "%"}).scalar()
    return {
        "fact_rows": len(facts), "dim_products": products, "duplicate_business_keys": duplicate_keys,
        "units": sum(r["quantity"] for r in facts),
        "buyer_amount": float(sum(r["buyer_total_amount"] or 0 for r in facts)),
        "status_mix": dict(sorted(Counter(r["order_status"] for r in facts).items())),
        "cancelled_rows": sum(bool(r["is_cancelled"]) for r in facts),
        "fact_business_sha256": digest(facts), "bi_business_sha256": digest(bi_rows), "bi_rows": len(bi_rows),
        "bi_units": sum(r["quantity"] for r in bi_rows), "bi_buyer_amount": float(sum(r["buyer_amount"] or 0 for r in bi_rows)),
        "inventory_business_sha256": digest(inventory),
        "latest_stock": {r["sku"]: r["stock_on_hand"] for r in inventory},
        "order_status_by_id": {r["order_id"]: r["order_status"] for r in facts},
        "technical": {"order_fact_ids": [r["order_fact_id"] for r in facts],
                      "fact_loaded_at": [str(r["loaded_at"]) for r in facts],
                      "inventory_fact_ids": [r["inventory_fact_id"] for r in inventory],
                      "inventory_loaded_at": [str(r["loaded_at"]) for r in inventory]},
    }


def lineage(since_epoch, raw_objects):
    runs = []
    for path in sorted(RUNS.glob("*.json")):
        run = json.loads(path.read_text(encoding="utf-8"))
        done = datetime.fromisoformat(run.get("completed_at", "1970-01-01T00:00:00+00:00")).timestamp()
        if done < since_epoch:
            continue
        mine = [a for a in run.get("object_accounting", []) if a["source_object"] in raw_objects]
        if mine:
            runs.append({"run_id": run["run_id"], "status": run["status"], "raw": run["raw_records"],
                         "clean": run["clean_records"], "quarantined": run["quarantined_records"],
                         "loaded_records": run["loaded_records"], "duplicates_removed": run["duplicates_removed"],
                         "quarantine_object": run.get("quarantine_object"), "contract": run.get("contract"),
                         "validation_errors": run.get("validation_errors"), "object_accounting": mine,
                         "accounting_balanced": all(a["raw"] == a["valid"] + a["duplicates"] + a["rejected"] for a in mine)})
    return runs


def api_state(skus):
    request = Request(API + "/inventory/reorder-alert", data=json.dumps({"skus": sorted(skus)}).encode(),
                      headers={"Content-Type": "application/json"})
    with urlopen(request, timeout=60) as response:
        alerts = json.load(response)
    alerts.pop("generated_at", None)
    with urlopen(API + "/pipeline/status", timeout=15) as response:
        status = json.load(response)
    return {"inventory_alerts_sha256": hashlib.sha256(json.dumps(alerts, sort_keys=True).encode()).hexdigest(),
            "inventory_alerts": alerts, "pipeline_status": status.get("status"), "ready": status.get("ready")}


def alerts_by_sku(response):
    return {a["sku"]: a for a in response["alerts"]}


def run_once(number, out_dir, meta, client, engine):
    label, expected = meta["label"], meta["expected"]
    before = end_offsets()
    since = datetime.now(timezone.utc) - timedelta(seconds=2)
    started = time.monotonic()
    sent = publish(out_dir / "dataset.jsonl")
    after = end_offsets()
    processed = wait_until_processed(client, label, since, expected)
    elapsed = round(time.monotonic() - started, 1)
    raw_rows, raw_objects, quarantine, quarantine_objects = lake_rows(client, label, since)
    state = {
        "run": number, "processed_within_timeout": processed, "seconds_to_processed": elapsed,
        "kafka": {"sent": sent, "offset_delta": {t: after[t] - before[t] for t in after}, "consumer_lag_after": group_lag()},
        "lake": {"raw_rows": raw_rows, "raw_objects": sorted(raw_objects), "quarantine": quarantine,
                 "quarantine_objects": quarantine_objects},
        "warehouse": warehouse_state(engine, label),
        "lineage": lineage(since.timestamp(), raw_objects),
        "api": api_state(expected["expected_stock"]),
    }
    (out_dir / f"run{number}.json").write_text(json.dumps(state, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    return state


def compare(meta, r1, r2):
    exp, w1, w2 = meta["expected"], r1["warehouse"], r2["warehouse"]
    l1 = {k: sum(run[k] for run in r1["lineage"]) for k in ("raw", "clean", "quarantined", "duplicates_removed", "loaded_records")}
    l2 = {k: sum(run[k] for run in r2["lineage"]) for k in ("raw", "clean", "quarantined", "duplicates_removed", "loaded_records")}
    cases = exp["cases"]
    newer_ok = all(w2["order_status_by_id"].get(k) == "COMPLETED" for k in cases["newer_version_wins"])
    stale_inv_sku, neg_sku = cases["stale_inventory_ignored"][0], cases["negative_inventory"][0]
    checks = []

    def check(group, name, passed, run1, run2, note=""):
        checks.append({"requirement": group, "check": name, "result": "PASS" if passed else "FAIL",
                       "run1": run1, "run2": run2, "note": note})

    check("0. Điều kiện thử", "Mỗi lượt được xử lý xong trước khi gửi lượt sau",
          r1["processed_within_timeout"] and r2["processed_within_timeout"],
          r1["seconds_to_processed"], r2["seconds_to_processed"],
          "nếu FAIL, hai lượt có thể bị ETL gộp và các so sánh bên dưới không tách bạch")
    check("1. Thu nhận đầy đủ", "Kafka nhận đủ message mỗi lượt",
          r1["kafka"]["offset_delta"] == r1["kafka"]["sent"] and r2["kafka"]["offset_delta"] == r2["kafka"]["sent"],
          r1["kafka"]["offset_delta"], r2["kafka"]["offset_delta"])
    check("1. Thu nhận đầy đủ", "Consumer không còn lag sau xử lý",
          not any(r1["kafka"]["consumer_lag_after"].values()) and not any(r2["kafka"]["consumer_lag_after"].values()),
          r1["kafka"]["consumer_lag_after"], r2["kafka"]["consumer_lag_after"])
    check("1. Thu nhận đầy đủ", "Raw lake lưu đủ mọi message đơn (kể cả lỗi)",
          r1["lake"]["raw_rows"] == r2["lake"]["raw_rows"] == exp["order_messages"],
          r1["lake"]["raw_rows"], r2["lake"]["raw_rows"], f"kỳ vọng {exp['order_messages']} mỗi lượt")
    check("2. Đối soát (reconciliation)", "raw = valid + duplicates + rejected theo từng object",
          all(run["accounting_balanced"] for run in r1["lineage"] + r2["lineage"]), l1, l2)
    check("3. Chống trùng / idempotent", "Số dòng fact không tăng sau lượt 2",
          w1["fact_rows"] == w2["fact_rows"] == exp["valid_order_lines"], w1["fact_rows"], w2["fact_rows"],
          f"kỳ vọng {exp['valid_order_lines']} dòng đơn hợp lệ")
    check("3. Chống trùng / idempotent", "Không có khóa nghiệp vụ trùng trong Fact_Orders",
          w1["duplicate_business_keys"] == w2["duplicate_business_keys"] == 0,
          w1["duplicate_business_keys"], w2["duplicate_business_keys"])
    check("3. Chống trùng / idempotent", "Trùng tuyệt đối trong cùng lô bị loại",
          l1["duplicates_removed"] == l2["duplicates_removed"] >= len(cases["exact_duplicate"]),
          l1["duplicates_removed"], l2["duplicates_removed"])
    check("3. Chống trùng / idempotent", "Không tạo thêm SKU trong Dim_Products",
          w1["dim_products"] == w2["dim_products"], w1["dim_products"], w2["dim_products"])
    check("4. Xác định (deterministic)", "Nội dung nghiệp vụ Fact_Orders giống hệt (SHA-256)",
          w1["fact_business_sha256"] == w2["fact_business_sha256"], w1["fact_business_sha256"][:16], w2["fact_business_sha256"][:16],
          "loại trừ order_fact_id, loaded_at")
    check("4. Xác định (deterministic)", "Tổng số lượng / doanh thu không đổi",
          (w1["units"], w1["buyer_amount"]) == (w2["units"], w2["buyer_amount"]),
          [w1["units"], w1["buyer_amount"]], [w2["units"], w2["buyer_amount"]])
    check("4. Xác định (deterministic)", "Kết quả kiểm định giống nhau giữa hai lượt",
          {k: l1[k] for k in ("raw", "clean", "quarantined", "duplicates_removed")}
          == {k: l2[k] for k in ("raw", "clean", "quarantined", "duplicates_removed")}
          and r1["lake"]["quarantine"] == r2["lake"]["quarantine"],
          r1["lake"]["quarantine"], r2["lake"]["quarantine"])
    check("5. Thứ tự / phiên bản sự kiện", "Phiên bản mới hơn thắng, bản cũ đến sau không ghi đè",
          newer_ok and w1["order_status_by_id"] == w2["order_status_by_id"],
          {k: w1["order_status_by_id"].get(k) for k in cases["newer_version_wins"]},
          {k: w2["order_status_by_id"].get(k) for k in cases["newer_version_wins"]})
    check("5. Thứ tự / phiên bản sự kiện", "Snapshot tồn kho cũ không ghi đè snapshot mới",
          w1["latest_stock"].get(stale_inv_sku) == w2["latest_stock"].get(stale_inv_sku) == exp["expected_stock"][stale_inv_sku],
          w1["latest_stock"].get(stale_inv_sku), w2["latest_stock"].get(stale_inv_sku),
          f"kỳ vọng {exp['expected_stock'][stale_inv_sku]}, bản cũ mang giá trị 1")
    check("6. Chất lượng dữ liệu", "Bản ghi lỗi bị cách ly, không vào kho",
          r1["lake"]["quarantine"]["orders"].get("records") == r2["lake"]["quarantine"]["orders"].get("records") == exp["rejected_orders"]
          and r1["lake"]["quarantine"]["inventory"].get("records") == r2["lake"]["quarantine"]["inventory"].get("records") == exp["rejected_inventory"]
          and not any(k in w2["order_status_by_id"] for c in ("invalid_price", "invalid_quantity", "unknown_platform", "invalid_json") for k in cases[c])
          and w2["latest_stock"].get(neg_sku) == exp["expected_stock"][neg_sku],
          {s: r1["lake"]["quarantine"][s].get("records") for s in ("orders", "inventory")},
          {s: r2["lake"]["quarantine"][s].get("records") for s in ("orders", "inventory")},
          f"kỳ vọng {exp['rejected_orders']} đơn, {exp['rejected_inventory']} tồn kho mỗi lượt")
    check("7. Truy vết (lineage)", "Mỗi lượt có run_id, object nguồn, object cách ly và hợp đồng",
          all(run["run_id"] and run["quarantine_object"] and run["contract"] for run in r1["lineage"] + r2["lineage"])
          and bool(r1["lineage"]) and bool(r2["lineage"]),
          [run["run_id"] for run in r1["lineage"]], [run["run_id"] for run in r2["lineage"]])
    check("8. Nhất quán hạ nguồn", "View bi.fact_order_line khớp Fact_Orders",
          w1["bi_rows"] == w1["fact_rows"] and w2["bi_rows"] == w2["fact_rows"]
          and (w2["bi_units"], w2["bi_buyer_amount"]) == (w2["units"], w2["buyer_amount"])
          and w1["bi_business_sha256"] == w2["bi_business_sha256"],
          [w1["bi_rows"], w1["bi_units"], w1["bi_buyer_amount"]], [w2["bi_rows"], w2["bi_units"], w2["bi_buyer_amount"]])
    a1, a2 = (alerts_by_sku(r["api"]["inventory_alerts"]) for r in (r1, r2))
    check("8. Nhất quán hạ nguồn", "API tồn kho trả kết quả giống hệt (so theo SKU)", a1 == a2,
          {sku: [a["current_stock"], a["stock_source"]] for sku, a in a1.items()},
          {sku: [a["current_stock"], a["stock_source"]] for sku, a in a2.items()})
    check("8. Nhất quán hạ nguồn", "API dùng tồn kho thật trong kho, không dùng số giả lập",
          all(a["current_stock"] == exp["expected_stock"][sku] and a["stock_source"].startswith("warehouse_snapshot")
              for alerts in (a1, a2) for sku, a in alerts.items()),
          sorted({a["stock_source"] for a in a1.values()}), sorted({a["stock_source"] for a in a2.values()}),
          "serving cache latest_stock 30 s; SKU thiếu trong cache rơi về simulated_demo")
    check("9. Giám sát", "Pipeline healthy và API ready sau mỗi lượt",
          all(r["api"]["pipeline_status"] == "healthy" and r["api"]["ready"] for r in (r1, r2)),
          [r1["api"]["pipeline_status"], r1["api"]["ready"]], [r2["api"]["pipeline_status"], r2["api"]["ready"]])

    t1, t2 = w1["technical"], w2["technical"]
    observations = [
        {"topic": "Khóa kỹ thuật", "detail": "order_fact_id giữ nguyên" if t1["order_fact_ids"] == t2["order_fact_ids"] else "order_fact_id thay đổi",
         "same": t1["order_fact_ids"] == t2["order_fact_ids"]},
        {"topic": "loaded_at Fact_Orders", "same": t1["fact_loaded_at"] == t2["fact_loaded_at"],
         "detail": f"{sum(a != b for a, b in zip(t1['fact_loaded_at'], t2['fact_loaded_at']))}/{len(t1['fact_loaded_at'])} dòng bị ghi lại ở lượt 2",
         "loaded_records": [l1["loaded_records"], l2["loaded_records"]]},
        {"topic": "loaded_at Fact_Inventory_Daily", "same": t1["inventory_loaded_at"] == t2["inventory_loaded_at"],
         "detail": f"{sum(a != b for a, b in zip(t1['inventory_loaded_at'], t2['inventory_loaded_at']))}/{len(t1['inventory_loaded_at'])} dòng bị ghi lại ở lượt 2"},
        {"topic": "Dung lượng lake", "same": False,
         "detail": f"lượt 2 tạo thêm {len(r2['lake']['raw_objects'])} object raw và {len(r2['lake']['quarantine_objects'])} object cách ly (at-least-once ở tầng lake)"},
        {"topic": "Thời gian xử lý", "same": None,
         "detail": f"lượt 1 {r1['seconds_to_processed']} s, lượt 2 {r2['seconds_to_processed']} s (phụ thuộc chu kỳ poll worker 30 s)"},
    ]
    return {"label": meta["label"], "dataset_sha256": meta["sha256"], "messages_per_run": meta["messages"],
            "verdict": "PASS" if all(c["result"] == "PASS" for c in checks) else "FAIL",
            "checks": checks, "observations": observations}


def main():
    logging.disable(logging.CRITICAL)
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    stamp = datetime.now().strftime("%Y-%m-%d")
    parser.add_argument("--out-dir", default=f"output/replay-determinism-{stamp}")
    parser.add_argument("--label", default="RPT" + datetime.now().strftime("%m%d%H%M"))
    parser.add_argument("--seed", type=int, default=20261115)
    args = parser.parse_args()
    out_dir = ROOT / args.out_dir
    out_dir.mkdir(parents=True, exist_ok=True)
    meta = freeze(out_dir, args.label, args.seed)
    client, engine = build_minio_client(), get_engine()
    try:
        r1 = run_once(1, out_dir, meta, client, engine)
        print(f"Lượt 1: xử lý xong={r1['processed_within_timeout']} sau {r1['seconds_to_processed']} s", flush=True)
        r2 = run_once(2, out_dir, meta, client, engine)
        print(f"Lượt 2: xử lý xong={r2['processed_within_timeout']} sau {r2['seconds_to_processed']} s", flush=True)
    finally:
        engine.dispose()
    result = compare(meta, r1, r2)
    (out_dir / "comparison.json").write_text(json.dumps(result, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    return 0 if result["verdict"] == "PASS" else 1


if __name__ == "__main__":
    sys.exit(main())
