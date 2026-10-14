"""Visible NiFi traffic plus the real Kafka/Parquet/warehouse/inventory path.

Emits 64 order events slowly so the canvas counters can be watched. Leaves
labelled test records for inspection and verifies NiFi JSON separately from
the Python consumer's Parquet. Does not train or change the deployed model.
"""
import json
import argparse
import sys
import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from pathlib import Path
from urllib.request import urlopen, Request

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from scripts.repair_nifi_routing import api
from scripts.run_e2e_runtime_check import main as streaming_check
from warehouse.etl.extract import build_minio_client, MINIO_BUCKET


def snapshot():
    flow = api("/flow/process-groups/root")["processGroupFlow"]["flow"]
    return {
        "processors": [{"id": p["id"], "name": p["component"]["name"],
                        "state": p["component"]["state"],
                        "validation": p["component"].get("validationStatus"),
                        "stats": p.get("status", {}).get("aggregateSnapshot", {})} for p in flow["processors"]],
        "queues": [{"id": c["id"], "queued": c.get("status", {}).get("aggregateSnapshot", {}).get("queued")}
                   for c in flow["connections"]],
    }


def http(path, body=None):
    request = Request("http://localhost:8000" + path,
                      data=json.dumps(body).encode() if body is not None else None,
                      headers={"Content-Type": "application/json"})
    with urlopen(request, timeout=30) as response:
        return json.load(response)


def main():
    before = snapshot()
    for processor in before["processors"]:
        if processor["validation"] != "VALID":
            raise RuntimeError(f"NiFi processor invalid: {processor['name']}")
        if processor["state"] != "RUNNING":
            current = api(f"/processors/{processor['id']}")
            api(f"/processors/{processor['id']}/run-status", "PUT",
                {"revision": current["revision"], "state": "RUNNING"})
    print("Watch NiFi canvas at http://localhost:8080/nifi/ ; emitting one event every 0.8 seconds.", flush=True)
    streaming = streaming_check(publish_interval=0.8)
    verify(streaming, before)


def verify(streaming, before):
    run_id = streaming["run_id"]
    result = {"run_id": run_id, "status": "RUNNING", "streaming": streaming, "nifi_before": before}
    output = Path("data/runtime_checks") / run_id
    try:
        client = build_minio_client()
        deadline = time.monotonic() + 45
        names = []
        while time.monotonic() < deadline:
            names = [obj.object_name for obj in client.list_objects(MINIO_BUCKET, prefix="raw/nifi/orders/", recursive=True)
                     if run_id in obj.object_name and obj.object_name.endswith(".json")]
            if len(names) >= 64:
                break
            time.sleep(2)
        nifi_records = []
        for name in names:
            response = client.get_object(MINIO_BUCKET, name)
            try:
                nifi_records.append(json.loads(response.read()))
            finally:
                response.close()
                response.release_conn()
        counts = {platform: sum(row.get("_platform") == platform for row in nifi_records)
                  for platform in ("shopee", "tiktok")}
        result.update(nifi_after=snapshot(), nifi_json_objects=names, nifi_by_platform=counts)
        assert counts == {"shopee": 32, "tiktok": 32}, counts
        result["ready"] = http("/ready")
        # Repository cache has a 60-second TTL; wait for a fresh snapshot instead
        # of treating a documented cache window as lost inventory data.
        stock_deadline = time.monotonic() + 75
        while True:
            result["inventory"] = http("/inventory/reorder-alert", {"skus": list(streaming["skus"].values())})
            if {row["sku"]: row["current_stock"] for row in result["inventory"]["alerts"]} == {
                streaming["skus"]["shopee"]: 123, streaming["skus"]["tiktok"]: 47} and all(
                    row["stock_source"].startswith("warehouse_snapshot:") for row in result["inventory"]["alerts"]):
                break
            if time.monotonic() > stock_deadline:
                raise TimeoutError("API did not expose the new warehouse inventory within cache TTL + margin")
            time.sleep(2)
        assert all(row["stock_source"].startswith("warehouse_snapshot:") for row in result["inventory"]["alerts"])
        catalog = http("/inventory/reorder-alert")
        sku = catalog["alerts"][0]["sku"]
        tomorrow = datetime.now(ZoneInfo("Asia/Ho_Chi_Minh")).date() + timedelta(days=1)
        result["model_forecast"] = http("/predict/demand", {"sku": sku, "from_date": tomorrow.isoformat(),
                                                             "to_date": (tomorrow + timedelta(days=2)).isoformat()})
        assert result["model_forecast"]["history_source"] == "warehouse"
        assert result["model_forecast"]["model_source"] == "mlflow_registry"
        result["note"] = "Demo SKUs verify streaming/stock. Deployed model uses the historical CSV catalog; its forecast is checked on a separate catalog SKU. No training or model deployment change."
        result["status"] = "PASS"
    except Exception as exc:
        result.update(status="FAIL", error=f"{type(exc).__name__}: {exc}")
        raise
    finally:
        (output / "nifi_live_demo.json").write_text(json.dumps(result, indent=2, ensure_ascii=False, default=str), encoding="utf-8")
        print("NIFI_LIVE_DEMO_RESULT", json.dumps({"run_id": run_id, "status": result["status"],
              "json_objects": len(result.get("nifi_json_objects", [])), "result": str(output / "nifi_live_demo.json")}), flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--verify-run", help="Recheck a completed streaming demo without publishing more events")
    args = parser.parse_args()
    if args.verify_run:
        # Run IDs, not paths, select saved results inside the fixed output root.
        if not args.verify_run.isalnum():
            parser.error("run ID must be alphanumeric")
        directory = Path("data/runtime_checks") / args.verify_run
        streaming = json.loads((directory / "streaming_result.json").read_text(encoding="utf-8"))
        previous = json.loads((directory / "nifi_live_demo.json").read_text(encoding="utf-8"))
        verify(streaming, previous["nifi_before"])
    else:
        main()
