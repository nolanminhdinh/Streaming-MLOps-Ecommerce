"""Bounded real Kafka/MinIO/PostgreSQL check using the production consumer loops.

Publishes uniquely labelled simulator orders twice to verify warehouse dedupe.
Uses independent consumer groups and separate test SKUs. Test facts remain in
the warehouse for inspection; this script does not train or deploy models.
"""
import io
import json
import os
import sys
import threading
import time
import uuid
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import pandas as pd
import pyarrow.parquet as pq
from sqlalchemy import text
from data_simulator.data_simulator import ECommerceSimulator
from ingestion import producer, consumer_to_minio as orders, consumer_inventory_to_postgres as inventory
from warehouse.etl.pipeline import run_etl_pipeline
from warehouse.etl.load import get_engine


def main():
    run_id = "E2E" + datetime.now(ZoneInfo("Asia/Ho_Chi_Minh")).strftime("%Y%m%d") + uuid.uuid4().hex[:6]
    output = Path("data/runtime_checks") / run_id
    output.mkdir(parents=True, exist_ok=True)
    result = {"run_id": run_id, "status": "RUNNING", "business_timezone": "Asia/Ho_Chi_Minh"}
    errors, object_names, batches = [], [], []
    ready = [threading.Event(), threading.Event()]
    threads = []
    client = orders.build_minio_client()
    original_put = client.put_object

    def put(*args, **kwargs):
        response = original_put(*args, **kwargs)
        object_names.append(kwargs["object_name"])
        return response
    client.put_object = put
    orders.build_minio_client = lambda: client
    original_flush = orders.flush_batch_to_minio
    def flush(batch, minio_client):
        batches.append({"rows": len(batch), "platforms": sorted({row.get("_platform", "unknown") for row in batch})})
        return original_flush(batch, minio_client)
    orders.flush_batch_to_minio = flush
    orders.BATCH_SIZE, orders.BATCH_TIMEOUT_SEC = 8, 2
    inventory.BATCH_SIZE, inventory.BATCH_TIMEOUT_SEC = 8, 2

    def install_consumer(module, index):
        original = module.KafkaConsumer
        def factory(*args, **kwargs):
            kwargs.update(group_id=f"{run_id}-{index}", auto_offset_reset="latest")
            consumer = original(*args, **kwargs)
            deadline = time.monotonic() + 30
            while not consumer.assignment():
                consumer.poll(timeout_ms=1000)
                if time.monotonic() > deadline:
                    consumer.close()
                    raise TimeoutError("Kafka partition assignment timed out")
            consumer.seek_to_end(*consumer.assignment())
            ready[index].set()
            return consumer
        module.KafkaConsumer = factory
        def worker():
            try:
                module.main()
            except Exception as exc:
                errors.append(f"consumer {index}: {type(exc).__name__}: {exc}")
        thread = threading.Thread(target=worker, daemon=True)
        thread.start()
        threads.append(thread)

    kafka_producer = None
    try:
        for index, module in enumerate((orders, inventory)):
            install_consumer(module, index)
        assert all(event.wait(35) for event in ready), "Consumer startup failed"
        assert not errors, errors
        kafka_producer = producer.build_producer()
        simulator = ECommerceSimulator(cancel_rate=0)
        today = datetime.now(ZoneInfo("Asia/Ho_Chi_Minh")).date()
        # UTC 16:59 is 23:59 VN; UTC 17:01 is 00:01 on the next VN day.
        boundary = datetime.combine(today - timedelta(days=1), datetime.min.time(), timezone.utc)
        timestamps = [boundary + timedelta(hours=16, minutes=59), boundary + timedelta(hours=17, minutes=1)]
        events = []
        skus = {"shopee": run_id + "-S", "tiktok": run_id + "-T"}
        for i in range(32):
            platform = "shopee" if i % 2 == 0 else "tiktok"
            timestamp = timestamps[(i // 2) % 2]
            if platform == "shopee":
                event = asdict(simulator._generate_shopee_event(timestamp))
                event.update(order_sn=f"{run_id}-S-{i:03d}", item_sku=skus[platform], shop_id=run_id, shop_name="E2E test shop")
            else:
                event = asdict(simulator._generate_tiktok_event(timestamp))
                event.update(order_id=f"{run_id}-T-{i:03d}", seller_sku=skus[platform], shop_name="E2E test shop " + run_id)
            event["_platform"] = platform
            events.append(event)
        for event in events + events:
            key = (event.get("order_sn") or event["order_id"]).encode()
            kafka_producer.send(producer.TOPIC_ORDERS, key=key, value=event).get(timeout=15)
        snapshots = [
            {"sku": skus["shopee"], "product_name": "E2E Shopee", "category": "E2E", "stock_on_hand": 123, "loaded_at": datetime.now(timezone.utc).isoformat()},
            {"sku": skus["tiktok"], "product_name": "E2E TikTok", "category": "E2E", "stock_on_hand": 47, "loaded_at": datetime.now(timezone.utc).isoformat()},
        ]
        for snapshot in snapshots:
            kafka_producer.send(producer.TOPIC_INVENTORY, key=snapshot["sku"].encode(), value=snapshot).get(timeout=15)
        kafka_producer.flush(timeout=15)
        deadline = time.monotonic() + 20
        while sum(batch["rows"] for batch in batches) < 64 and time.monotonic() < deadline and not errors:
            time.sleep(0.25)
    finally:
        orders._running = inventory._running = False
        for thread in threads:
            thread.join(timeout=12)
        if kafka_producer is not None:
            kafka_producer.close(timeout=5)
    assert not errors, errors
    assert all(not thread.is_alive() for thread in threads), "Consumers did not stop cleanly"
    assert any(len(batch["platforms"]) == 2 for batch in batches), "No mixed-platform batch exercised"
    raw_count = 0
    for object_name in object_names:
        response = client.get_object(orders.MINIO_BUCKET, object_name)
        try:
            frame = pq.read_table(io.BytesIO(response.read())).to_pandas()
        finally:
            response.close()
            response.release_conn()
        platform = object_name.split("platform=")[1].split("/")[0]
        time_column = "create_time" if platform == "shopee" else "created_time"
        dates = pd.to_datetime(frame[time_column], format="mixed", utc=True).dt.tz_convert("Asia/Ho_Chi_Minh")
        expected = dates.dt.strftime("year=%Y/month=%m/day=%d")
        assert all(partition in object_name for partition in expected), object_name
        raw_count += len(frame)
    assert raw_count == 64, f"Expected 64 persisted records, got {raw_count}"
    dates = [today - timedelta(days=1), today]
    result["etl"] = [run_etl_pipeline(datetime.combine(day, datetime.min.time())) for day in dates]
    engine = get_engine()
    def totals():
        with engine.connect() as conn:
            return [dict(row) for row in conn.execute(text("""
                SELECT platform, COUNT(*) AS rows, SUM(quantity) AS units
                FROM Fact_Orders WHERE order_id LIKE :prefix GROUP BY platform ORDER BY platform
            """), {"prefix": run_id + "%"}).mappings()]
    first = totals()
    assert sum(row["rows"] for row in first) == 32, first
    for day in dates:
        run_etl_pipeline(datetime.combine(day, datetime.min.time()))
    assert totals() == first, "Warehouse replay changed order counts/quantities"
    with engine.connect() as conn:
        stocks = [dict(row) for row in conn.execute(text("""
            SELECT p.sku, i.stock_on_hand, d.full_date::TEXT AS day
            FROM Fact_Inventory_Daily i JOIN Dim_Products p ON p.product_key=i.product_key
            JOIN Dim_Dates d ON d.date_key=i.date_key WHERE p.sku = ANY(:skus)
        """), {"skus": list(skus.values())}).mappings()]
    assert {row["sku"]: row["stock_on_hand"] for row in stocks} == {skus["shopee"]: 123, skus["tiktok"]: 47}, stocks
    result.update(status="PASS", kafka_orders_sent=64, unique_orders=32, parquet_rows=raw_count, parquet_objects=len(object_names), mixed_batches=sum(len(b["platforms"])==2 for b in batches), warehouse=first, inventory=stocks, warehouse_replay="PASS", partition_check="PASS", skus=skus)
    (output / "streaming_result.json").write_text(json.dumps(result, indent=2, default=str), encoding="utf-8")
    print("RUNTIME_RESULT", json.dumps(result, ensure_ascii=False, default=str))


if __name__ == "__main__":
    main()
