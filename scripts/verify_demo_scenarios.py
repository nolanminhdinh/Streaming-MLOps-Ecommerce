"""Read-only independent verification of a scenario triggered from the demo UI.

Run inside demo-web (has Kafka/MinIO/PostgreSQL network access). This never
publishes, commits consumer offsets, runs ETL, or changes inventory/model state.
Evidence is written only to data/demo/evidence/<suite>/.
"""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
import csv
from datetime import datetime, timezone
import io
import json
import math
import os
from pathlib import Path
import re
import sys
import time
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


def get_json(url, body=None):
    request = Request(url, data=json.dumps(body).encode() if body is not None else None,
                      headers={"Content-Type": "application/json"})
    with urlopen(request, timeout=20) as response:
        return json.load(response)


def save(path, value):
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2, default=str), encoding="utf-8")


def engine():
    from sqlalchemy import create_engine
    from sqlalchemy.engine import URL
    url = URL.create("postgresql+psycopg2", username=os.getenv("POSTGRES_USER", "ecom"),
                     password=os.getenv("POSTGRES_PASSWORD", "ecom_password"),
                     host=os.getenv("POSTGRES_HOST", "postgres"),
                     port=int(os.getenv("POSTGRES_PORT", "5432")), database=os.getenv("POSTGRES_DB", "ecom_warehouse"))
    return create_engine(url, connect_args={"connect_timeout": 3, "options": "-c statement_timeout=10000"})


def stock_rows(conn, sku):
    from sqlalchemy import text
    return [dict(r) for r in conn.execute(text("""
        SELECT d.full_date, i.stock_on_hand, i.safety_stock, i.reorder_point, i.snapshot_at
        FROM Fact_Inventory_Daily i JOIN Dim_Products p USING(product_key)
        JOIN Dim_Dates d USING(date_key) WHERE p.sku=:sku ORDER BY d.full_date, i.snapshot_at
    """), {"sku": sku}).mappings()]


def verify_kafka(run):
    from kafka import KafkaConsumer, TopicPartition
    wanted = defaultdict(dict)
    by_key = {(e["topic"], e["key"]): e for e in run["events"]}
    order_topic = os.getenv("KAFKA_TOPIC_ORDERS", "ecom.orders.raw")
    for ack in run["acks"]:
        wanted[TopicPartition(ack["topic"], ack["partition"])][ack["offset"]] = ack
    consumer = KafkaConsumer(bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS_HOST", "kafka:9092"),
                             group_id=None, enable_auto_commit=False,
                             request_timeout_ms=10000, api_version_auto_timeout_ms=5000)
    found = []
    try:
        consumer.assign(list(wanted))
        for partition, offsets in wanted.items():
            consumer.seek(partition, min(offsets))
        deadline = time.monotonic() + 20
        while len(found) < len(run["acks"]) and time.monotonic() < deadline:
            for partition, messages in consumer.poll(timeout_ms=500, max_records=1000).items():
                for message in messages:
                    ack = wanted[partition].pop(message.offset, None)
                    if ack is None:
                        continue
                    key = message.key.decode()
                    assert key == ack["key"], "Kafka ACK key differs from stored message"
                    event = by_key[("orders" if message.topic == order_topic else "inventory", key)]
                    assert json.loads(message.value) == event["payload"], "Kafka payload differs from manifest"
                    found.append({"topic": message.topic, "partition": message.partition,
                                  "offset": message.offset, "key": key})
        assert len(found) == len(run["acks"]), "Some acknowledged offsets could not be read"
        return {"verified_messages": len(found), "offsets": found, "consumer_group": None, "commits": False}
    finally:
        consumer.close()


def verify_lake(run):
    from minio import Minio
    import pyarrow.parquet as pq
    import urllib3
    client = Minio(os.getenv("MINIO_ENDPOINT_HOST", "minio:9000"),
                   access_key=os.getenv("MINIO_ROOT_USER", "minioadmin"),
                   secret_key=os.getenv("MINIO_ROOT_PASSWORD", "minioadmin"),
                   secure=False,
                   http_client=urllib3.PoolManager(timeout=urllib3.Timeout(connect=3, read=8), retries=False))
    bucket = os.getenv("MINIO_BUCKET_RAW", "ecom-raw-lake")
    order_events = [e for e in run["events"] if e["topic"] == "orders"]
    ids = {e["key"] for e in order_events}
    days = {datetime.fromisoformat(e["payload"].get("create_time") or e["payload"].get("created_time"))
            .astimezone(ZoneInfo("Asia/Ho_Chi_Minh")).date() for e in order_events}
    raw = []
    objects = []
    for day in sorted(days):
        for platform in ("shopee", "tiktok"):
            prefix = f"raw/orders/platform={platform}/year={day.year:04d}/month={day.month:02d}/day={day.day:02d}/"
            for obj in client.list_objects(bucket, prefix=prefix, recursive=True):
                if not obj.object_name.endswith(".parquet") or obj.last_modified.timestamp() < run["started_at"] - 3:
                    continue
                response = client.get_object(bucket, obj.object_name)
                try:
                    rows = pq.read_table(io.BytesIO(response.read())).to_pylist()
                finally:
                    response.close()
                    response.release_conn()
                if rows and "_raw_payload_json" in rows[0]:
                    rows = [json.loads(r["_raw_payload_json"]) for r in rows]
                selected = []
                for row in rows:
                    order_id = row.get("order_sn") if platform == "shopee" else row.get("order_id")
                    if order_id in ids:
                        selected.append({"order_id": order_id, "platform": platform, "quantity": row["quantity"]})
                if selected:
                    raw.extend(selected)
                    objects.append({"name": obj.object_name, "rows": len(selected)})
    rejected = []
    for obj in client.list_objects(bucket, prefix="quarantine/orders/", recursive=True):
        if obj.last_modified.timestamp() < run["started_at"] - 3:
            continue
        response = client.get_object(bucket, obj.object_name)
        try:
            for line in response.read().decode().splitlines():
                row = json.loads(line)
                if row.get("order_id") in ids:
                    rejected.append({"order_id": row["order_id"], "reasons": row.get("rejection_reasons"), "object": obj.object_name})
        finally:
            response.close()
            response.release_conn()
    return {"raw_rows": len(raw), "objects": objects, "records": raw,
            "rejected_count": len(rejected), "rejected": rejected}


def verify_database(run):
    from sqlalchemy import text
    database = engine()
    try:
        with database.connect() as conn:
            rows = [dict(r) for r in conn.execute(text("""
                SELECT f.order_id,f.source_line_id,f.platform,p.sku,f.quantity,f.order_status,f.is_cancelled
                FROM Fact_Orders f JOIN Dim_Products p USING(product_key)
                WHERE f.order_id LIKE :prefix ORDER BY f.order_id
            """), {"prefix": run["run_id"] + "-%"}).mappings()]
            stocks = {run["run_id"]+suffix: stock_rows(conn, run["run_id"]+suffix) for suffix in ("-S", "-T")}
        expected = {e["key"]: e["payload"] for e in run["events"] if e["topic"] == "orders" and not e["invalid"]}
        assert len(rows) == len(expected) == run["valid_orders"], "Warehouse count mismatch"
        assert len({r["order_id"] for r in rows}) == len(rows), "Duplicate order rows"
        for row in rows:
            source = expected[row["order_id"]]
            assert int(row["quantity"]) == int(source["quantity"]), "Warehouse quantity mismatch"
            assert row["platform"] == source["_platform"] and row["order_status"] == source["order_status"]
            assert row["sku"] == (source.get("item_sku") or source.get("seller_sku"))
        for suffix, stock in (("-S", 50), ("-T", 8)):
            assert stocks[run["run_id"]+suffix][-1]["stock_on_hand"] == stock
        return {"run_count": len(rows), "units": sum(int(r["quantity"]) for r in rows), "rows": rows, "stocks": stocks}
    finally:
        database.dispose()


def verify_exports(run, lake):
    from sqlalchemy import text
    checkpoint = json.loads((ROOT / "data/pipeline/checkpoint.json").read_text())
    completed = set(checkpoint["completed_objects"])
    assert all(obj["name"] in completed for obj in lake["objects"]), "Object not checkpointed"
    directory = ROOT / "powerbi/data"
    with (directory / "Dim_Products.csv").open(encoding="utf-8-sig", newline="") as source:
        skus = {r["sku"] for r in csv.DictReader(source)}
    assert {run["run_id"]+"-S", run["run_id"]+"-T"} <= skus, "CSV missing demo products"
    with (directory / "Fact_Orders_Summary.csv").open(encoding="utf-8-sig", newline="") as source:
        exported = { (r["order_date"], r["platform"]): r for r in csv.DictReader(source)}
    database = engine()
    try:
        with database.connect() as conn:
            totals = [dict(r) for r in conn.execute(text("""
                SELECT d.full_date::text AS day,f.platform,COUNT(DISTINCT f.order_id) AS orders,SUM(f.quantity) AS units
                FROM Fact_Orders f JOIN Dim_Dates d USING(date_key)
                WHERE NOT f.is_cancelled AND d.full_date IN (
                    SELECT DISTINCT d2.full_date FROM Fact_Orders f2 JOIN Dim_Dates d2 USING(date_key)
                    WHERE f2.order_id LIKE :prefix)
                GROUP BY d.full_date,f.platform
            """), {"prefix":run["run_id"]+"-%"}).mappings()]
        for total in totals:
            row = exported[(total["day"], total["platform"])]
            assert int(row["total_orders"]) == total["orders"], "CSV order count differs from SQL"
            assert int(float(row["units_sold"])) == total["units"], "CSV units differ from SQL"
        return {"checkpoint_objects": len(lake["objects"]), "summary_groups_verified": totals,
                "files": [{"name":p.name,"modified_at":p.stat().st_mtime} for p in directory.glob("*.csv")]}
    finally:
        database.dispose()


def wait_for_report(case, run_id, seconds):
    deadline = time.monotonic() + seconds
    previous = None
    while True:
        report = get_json("http://localhost:8050/api/report")
        run, lake, warehouse = report.get("run",{}),report.get("lake",{}),report.get("warehouse",{})
        assert run.get("run_id") == run_id, "Active run changed during verification"
        progress = (run.get("status"),run.get("sent_orders"),lake.get("raw_rows"),warehouse.get("run_count"),lake.get("rejected_count"),lake.get("processed_objects"))
        if progress != previous:
            print(json.dumps({"case":case,"progress":progress}), flush=True)
            previous = progress
        expected = (50,0,1) if case=="burst" else (12,0,1) if case=="normal" else (12,1,2) if case=="replay" else (12,1,1)
        valid,invalid,repeats = expected
        if run.get("status")=="error":
            raise AssertionError(run.get("message"))
        if (run.get("status")=="published" and run.get("sent_orders")== (valid+invalid)*repeats
            and run.get("sent_inventory")==2*repeats and lake.get("ok") and warehouse.get("ok")
            and lake.get("raw_rows")== (valid+invalid)*repeats and warehouse.get("run_count")==valid
            and lake.get("rejected_count",0)>=invalid*repeats and lake.get("objects")
            and lake.get("processed_objects")==len(lake["objects"])):
            return report
        if time.monotonic()>deadline:
            raise TimeoutError("Scenario did not converge: "+str(progress))
        time.sleep(2)


def verify_stream(case, run_id, output, seconds):
    report = wait_for_report(case,run_id,seconds)
    run = json.loads((ROOT / "data/demo" / (run_id+".json")).read_text())
    kafka = verify_kafka(run)
    lake = verify_lake(run)
    warehouse = verify_database(run)
    exports = verify_exports(run,lake)
    assert lake["raw_rows"] == report["lake"]["raw_rows"] == run["sent_orders"]
    assert lake["rejected_count"] == report["lake"]["rejected_count"]
    assert warehouse["run_count"] == report["warehouse"]["run_count"]
    repeats = run.get("replay_count",0)+1
    assert Counter(r["order_id"] for r in lake["records"]) == Counter({e["key"]:repeats for e in run["events"] if e["topic"]=="orders"})
    bad = {e["key"] for e in run["events"] if e["invalid"]}
    assert {r["order_id"] for r in lake["rejected"]} == bad
    assert all("NON_POSITIVE_QUANTITY" in str(r["reasons"]) for r in lake["rejected"])
    if case=="replay":
        baseline = json.loads((output/"invalid.json").read_text())
        assert baseline["independent"]["warehouse"]["rows"] == json.loads(json.dumps(warehouse["rows"],default=str)), "Replay changed warehouse rows"
    result = {"case":case,"status":"PASS","checked_at":datetime.now(timezone.utc).isoformat(),"run_id":run_id,
              "web_report":report,"independent":{"kafka":kafka,"lake":lake,"warehouse":warehouse,"exports":exports}}
    save(output/(case+".json"),result)
    print(json.dumps({"case":case,"status":"PASS","run_id":run_id,"kafka_messages":kafka["verified_messages"],
                      "raw_rows":lake["raw_rows"],"warehouse_lines":warehouse["run_count"],"quarantine":lake["rejected_count"],"units":warehouse["units"]}),flush=True)


def verify_inventory(case, output):
    report = get_json("http://localhost:8050/api/report")
    query = report.get("last_queries",{}).get("inventory",{})
    request,wrapped = query["request"],query["response"]
    stock = 0 if case=="stock0" else 100
    assert request["current_stock"] == stock and request["lead_time_days"] == 3
    row = wrapped["data"]["alerts"][0]
    direct = get_json(os.getenv("DEMO_SERVING_URL","http://fastapi:8000")+"/inventory/reorder-alert", {
        "skus":[request["sku"]],"lead_time_days":3,"service_level":.95,"current_stocks":{request["sku"]:stock}})
    assert row == direct["alerts"][0], "Demo proxy response differs from serving API"
    assert row["current_stock"] == stock and row["stock_source"] == "request"
    expected_level = "CRITICAL" if stock<=row["safety_stock"] else "WARNING" if stock<=row["reorder_point"] else "NORMAL"
    assert row["alert_level"] == expected_level
    assert row["needs_reorder"] == (stock<=row["reorder_point"])
    if row["alert_level"] == "NORMAL":
        assert row["recommended_reorder_qty"] == 0
    elif stock==0:
        # Mean exposed by the API is rounded to one decimal; allow that rounding.
        low = row["reorder_point"]+math.ceil((row["avg_daily_demand"]-.05)*7)
        high = row["reorder_point"]+math.ceil((row["avg_daily_demand"]+.05)*7)
        assert low<=row["recommended_reorder_qty"]<=high
    database = engine()
    try:
        with database.connect() as conn:
            snapshot = stock_rows(conn,request["sku"])
        before = json.loads((output/"inventory-before.json").read_text())
        assert before["sku"] == request["sku"]
        assert before["rows"] == json.loads(json.dumps(snapshot,default=str)), "Hypothetical query changed stock compared with the pre-query baseline"
        if case=="stock100":
            baseline = json.loads((output/"stock0.json").read_text())
            assert baseline["independent"]["warehouse_stock_rows"] == json.loads(json.dumps(snapshot,default=str)), "Hypothetical queries modified stock"
    finally:
        database.dispose()
    result = {"case":case,"status":"PASS","checked_at":datetime.now(timezone.utc).isoformat(),"web_query":query,
              "independent":{"direct_serving":direct,"warehouse_stock_rows":snapshot}}
    save(output/(case+".json"),result)
    print(json.dumps({"case":case,"status":"PASS","sku":row["sku"],"stock":stock,"level":row["alert_level"],
                      "safety_stock":row["safety_stock"],"reorder_point":row["reorder_point"],"reorder_qty":row["recommended_reorder_qty"],
                      "stock_source":row["stock_source"],"demand_source":row["demand_source"]}),flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--case",required=True,choices=("normal","invalid","replay","burst","stock0","stock100"))
    parser.add_argument("--run-id")
    parser.add_argument("--suite",required=True)
    parser.add_argument("--wait-seconds",type=int,default=150)
    args = parser.parse_args()
    if not re.fullmatch(r"[a-zA-Z0-9_-]{1,80}",args.suite):
        parser.error("suite must be a short directory name")
    output = ROOT / "data/demo/evidence" / args.suite
    output.mkdir(parents=True,exist_ok=True)
    try:
        if args.case.startswith("stock"):
            verify_inventory(args.case,output)
        else:
            if not re.fullmatch(r"WEB\d{14}[a-f0-9]{6}",args.run_id or ""):
                parser.error("a valid run ID is required for stream cases")
            verify_stream(args.case,args.run_id,output,args.wait_seconds)
    except Exception as exc:
        save(output/(args.case+"-failure.json"),{"case":args.case,"status":"FAIL","error_type":type(exc).__name__,"message":str(exc)})
        raise


if __name__ == "__main__":
    main()
