"""Publish labelled fixtures; verify the running worker without invoking ETL/export.

Includes stale versions, invalid JSON, mixed numeric types, unknown platform and
invalid stock. Test facts are retained. No training or source-coverage changes.
"""
from datetime import datetime, timedelta, timezone
import json
import logging
import os
from pathlib import Path
import sys
import time
import uuid
from urllib.request import Request, urlopen
from urllib.error import HTTPError

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from kafka import KafkaProducer
from sqlalchemy import text
from ingestion.producer import KAFKA_BOOTSTRAP_SERVERS, TOPIC_ORDERS, TOPIC_INVENTORY
from warehouse.etl.extract import build_minio_client, MINIO_BUCKET
from warehouse.etl.load import get_engine


def main():
    logging.disable(logging.CRITICAL)
    run_id = "AUTO" + datetime.now().strftime("%Y%m%d") + uuid.uuid4().hex[:6]
    now = datetime.now(timezone.utc)
    sku = run_id + "-A"
    base = dict(_platform="shopee", order_sn=run_id + "-order", item_sku=sku, item_name="Continuous pipeline fixture",
                shop_id="CONTINUOUS-TEST", shop_name="Continuous test", quantity=3, original_price=10000,
                total_order_value=30000, buyer_total_amount=30000, order_status="COMPLETED",
                create_time=(now - timedelta(hours=2)).isoformat(), update_time=now.isoformat())
    events = [base, dict(base, order_status="READY_TO_SHIP", quantity=1, update_time=(now - timedelta(hours=1)).isoformat()),
              dict(base, order_sn=run_id + "-unknown", _platform=None),
              dict(base, order_sn=run_id + "-bad-price", original_price="bad"),
              dict(base, order_sn=run_id + "-second", quantity=4)]
    publisher = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS, acks="all", retries=3)
    try:
        for event in events:
            publisher.send(TOPIC_ORDERS, key=event["order_sn"].encode(), value=json.dumps(event).encode()).get(timeout=15)
        publisher.send(TOPIC_ORDERS, key=run_id.encode(), value=('{invalid_json "' + run_id + '"').encode()).get(timeout=15)
        for stock, ts in [(50, now), (10, now - timedelta(seconds=1)), (-3, now)]:
            publisher.send(TOPIC_INVENTORY, key=sku.encode(), value=json.dumps(dict(sku=sku, stock_on_hand=stock, loaded_at=ts.isoformat())).encode()).get(timeout=15)
        publisher.flush(timeout=15)
    finally:
        publisher.close(timeout=5)
    result = {"run_id": run_id, "status": "FAIL", "manual_etl_or_export": False, "orders_sent": 6, "inventory_sent": 3}
    engine = get_engine()
    deadline = time.monotonic() + 180
    try:
        while time.monotonic() < deadline:
            with engine.connect() as conn:
                facts = [dict(row) for row in conn.execute(text("SELECT order_id,order_status,quantity FROM Fact_Orders WHERE order_id LIKE :prefix ORDER BY order_id"), {"prefix": run_id + "%"}).mappings()]
                stock = conn.execute(text("SELECT stock_on_hand FROM Fact_Inventory_Daily i JOIN Dim_Products p USING(product_key) WHERE p.sku=:sku ORDER BY snapshot_at DESC LIMIT 1"), {"sku": sku}).scalar()
                # Reconcile the Import summary view against persisted fact totals;
                # CSV backup existence/freshness is deliberately irrelevant.
                summary = conn.execute(text("""SELECT SUM(v.total_orders) AS orders,
                    SUM(v.total_units_sold) AS units, SUM(v.total_revenue) AS revenue
                    FROM vw_powerbi_executive_summary v JOIN Dim_Dates d ON d.full_date=v.order_date
                    WHERE EXISTS (SELECT 1 FROM Fact_Orders f WHERE f.order_id LIKE :prefix
                        AND f.date_key=d.date_key AND f.platform=v.platform)"""),
                    {"prefix": run_id + "%"}).mappings().one()
                actual = conn.execute(text("""SELECT COUNT(DISTINCT (f.platform,f.order_id)) AS orders,
                    SUM(f.quantity) AS units, SUM(f.buyer_total_amount) AS revenue FROM Fact_Orders f
                    WHERE f.is_cancelled=FALSE AND EXISTS (SELECT 1 FROM Fact_Orders labelled
                        WHERE labelled.order_id LIKE :prefix AND labelled.date_key=f.date_key
                        AND labelled.platform=f.platform)"""), {"prefix": run_id + "%"}).mappings().one()
                view_matches = summary == actual and bool(summary["orders"])
            if len(facts) == 2 and sum(row["quantity"] for row in facts) == 7 and all(row["order_status"] == "COMPLETED" for row in facts) and stock == 50 and view_matches:
                result.update(status="PASS", facts=facts, latest_stock=stock,
                              powerbi_source="postgresql_import", sql_view_matches_warehouse=True,
                              csv_backup_required=False, powerbi_desktop_refresh_verified=False)
                break
            time.sleep(2)
        client = build_minio_client()
        quarantined = {"orders": 0, "inventory": 0}
        # Only inspect new small quarantine files, by object modification time.
        for stream in quarantined:
            for obj in client.list_objects(MINIO_BUCKET, prefix=f"quarantine/{stream}/", recursive=True):
                if obj.last_modified < now - timedelta(seconds=5):
                    continue
                response = client.get_object(MINIO_BUCKET, obj.object_name)
                try:
                    payload = response.read().decode("utf-8")
                    quarantined[stream] += sum(run_id in line for line in payload.splitlines())
                finally:
                    response.close()
                    response.release_conn()
        result["quarantined_labelled_records"] = quarantined
        if quarantined["orders"] < 3 or quarantined["inventory"] < 1:
            result["status"] = "FAIL"
        api = os.getenv("PIPELINE_CHECK_API_BASE", "http://localhost:8000")
        with urlopen(api + "/ready", timeout=15) as response:
            result["ready"] = {"http_status": response.status, **json.load(response)}
        request = Request(api + "/inventory/reorder-alert", data=json.dumps({"skus": [sku]}).encode(), headers={"Content-Type": "application/json"})
        with urlopen(request, timeout=30) as response:
            result["inventory_api"] = json.load(response)
            assert result["inventory_api"]["alerts"][0]["current_stock"] == 50
        request = Request(api + "/inventory/reorder-alert", data=b'{"lead_time_days":-1,"service_level":2}', headers={"Content-Type": "application/json"})
        try:
            urlopen(request, timeout=15)
        except HTTPError as exc:
            result["invalid_request_http_status"] = exc.code
        assert result.get("invalid_request_http_status") == 422
    finally:
        engine.dispose()
        path = ROOT / "docs/07-bao-cao-do-an/evidence" / f"continuous_pipeline_{run_id}.json"
        path.write_text(json.dumps(result, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
        print(json.dumps({**result, "evidence": str(path)}, ensure_ascii=False, default=str))
    return 0 if result["status"] == "PASS" else 1


if __name__ == "__main__":
    sys.exit(main())
