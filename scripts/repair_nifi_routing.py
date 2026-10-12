"""Update the deployed NiFi flow in place without deleting processors or queues."""
import json
import os
import time
import urllib.request

from dotenv import load_dotenv
load_dotenv()
BASE = os.getenv("NIFI_API_URL", "http://localhost:8080/nifi-api").rstrip("/")


def api(path, method="GET", payload=None):
    request = urllib.request.Request(BASE + path, method=method,
        data=None if payload is None else json.dumps(payload).encode(),
        headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(request, timeout=20) as response:
        return json.load(response)


def main():
    flow = api("/flow/process-groups/root")["processGroupFlow"]["flow"]
    selected = [p for p in flow["processors"] if p["component"]["name"].startswith(("1.", "2.", "3.", "4."))]
    assert len(selected) == 4, "Expected exactly four named ingestion processors"
    running = []
    try:
        for processor in selected:
            if processor["component"]["state"] == "RUNNING":
                api(f"/processors/{processor['id']}/run-status", "PUT", {"revision": processor["revision"], "state": "STOPPED"})
                running.append(processor["id"])
        for processor in selected:
            deadline = time.monotonic() + 15
            while True:
                current = api(f"/processors/{processor['id']}")
                if current.get("status", {}).get("aggregateSnapshot", {}).get("activeThreadCount", 0) == 0:
                    break
                if time.monotonic() > deadline:
                    raise TimeoutError("NiFi processor did not stop")
                time.sleep(.2)
            name = current["component"]["name"]
            if name.startswith("2."):
                properties = {"platform": "$._platform", "order_id": None,
                    "shopee_order_id": "$.order_sn", "tiktok_order_id": "$.order_id",
                    "user_id": None, "created_at": None}
            elif name.startswith("3."):
                properties = {"shopee": "${platform:equals('shopee')}", "tiktok": "${platform:equals('tiktok')}"}
            elif name.startswith("4."):
                properties = {
                    "s3.bucket": os.getenv("MINIO_BUCKET_RAW", "ecom-raw-lake"),
                    "filename": "raw/nifi/orders/platform=${platform}/ingest_date=${now():format('yyyy-MM-dd', 'Asia/Ho_Chi_Minh')}/${shopee_order_id:isEmpty():ifElse(${tiktok_order_id},${shopee_order_id})}-${uuid}.json",
                }
            else:
                continue
            changed = api(f"/processors/{processor['id']}", "PUT", {
                "revision": current["revision"], "component": {"id": processor["id"], "config": {"properties": properties}},
            })
            assert changed["component"].get("validationStatus") != "INVALID", changed["component"].get("validationErrors")
    finally:
        for identifier in reversed(running):
            current = api(f"/processors/{identifier}")
            api(f"/processors/{identifier}/run-status", "PUT", {"revision": current["revision"], "state": "RUNNING"})
    print("NiFi routing updated in place; running processors restored.")


if __name__ == "__main__":
    main()
