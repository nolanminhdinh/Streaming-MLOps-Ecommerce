"""Add lossless NiFi error routes in place; never delete processors or queues."""
import json
import os
from pathlib import Path
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from scripts.repair_nifi_routing import api


def main():
    group = api("/flow/process-groups/root")["processGroupFlow"]
    group_id, flow = group["id"], group["flow"]
    processors = flow["processors"]
    selected = {}
    for number in (2, 3, 5):
        matches = [p for p in processors if p["component"]["name"].startswith(str(number) + ".")]
        if len(matches) != 1:
            raise RuntimeError("Expected existing standard ecommerce flow; nothing was removed")
        selected[number] = matches[0]["id"]
    running = [p["id"] for p in processors if p["component"]["state"] == "RUNNING"]
    new_id = None
    try:
        for identifier in running:
            current = api("/processors/" + identifier)
            api(f"/processors/{identifier}/run-status", "PUT", {"revision": current["revision"], "state": "STOPPED"})
        for identifier in running:
            deadline = time.monotonic() + 30
            while api("/processors/" + identifier).get("status", {}).get("aggregateSnapshot", {}).get("activeThreadCount", 0):
                if time.monotonic() > deadline:
                    raise TimeoutError("NiFi processors did not stop")
                time.sleep(.2)
        quarantine = next((p for p in processors if p["component"]["name"] == "7. Quarantine invalid JSON/platform"), None)
        if quarantine is None:
            quarantine = api(f"/process-groups/{group_id}/processors", "POST", {
                "revision": {"version": 0}, "component": {
                    "name": "7. Quarantine invalid JSON/platform", "type": "org.apache.nifi.processors.attributes.UpdateAttribute",
                    "position": {"x": 900, "y": 400}, "config": {"properties": {
                        "filename": "quarantine/nifi/ingest_date=${now():format('yyyy-MM-dd', 'Asia/Ho_Chi_Minh')}/${uuid}.json",
                        "quarantine.reason": "invalid_json_or_unsupported_platform"}}}})
            new_id = quarantine["id"]
        quarantine_id = quarantine["id"]
        current = api("/processors/" + quarantine_id)
        api("/processors/" + quarantine_id, "PUT", {"revision": current["revision"], "component": {
            "id": quarantine_id, "config": {"properties": {"s3.bucket": os.getenv("MINIO_BUCKET_RAW", "ecom-raw-lake")}}}})
        for identifier in selected.values():
            current = api("/processors/" + identifier)
            config = {"autoTerminatedRelationships": []}
            if identifier == selected[5]:
                config.update(retriedRelationships=["failure"], retryCount=10,
                              backoffMechanism="PENALIZE_FLOWFILE", maxBackoffPeriod="1 min", penaltyDuration="30 sec",
                              properties={"Bucket": os.getenv("MINIO_BUCKET_RAW", "ecom-raw-lake")})
            api("/processors/" + identifier, "PUT", {"revision": current["revision"], "component": {"id": identifier, "config": config}})
        connections = flow["connections"]
        for source, destination, relationships, name in [
            (selected[2], quarantine_id, ["failure", "unmatched"], "Invalid JSON to quarantine"),
            (selected[3], quarantine_id, ["unmatched"], "Unknown platform to quarantine"),
            (quarantine_id, selected[5], ["success"], "Persist quarantine in MinIO"),
            (selected[5], selected[5], ["failure"], "Retry MinIO without discarding content"),
        ]:
            if any(c["component"]["source"]["id"] == source and c["component"]["destination"]["id"] == destination
                   and set(c["component"]["selectedRelationships"]) == set(relationships) for c in connections):
                continue
            api(f"/process-groups/{group_id}/connections", "POST", {"revision": {"version": 0}, "component": {
                "name": name, "source": {"id": source, "groupId": group_id, "type": "PROCESSOR"},
                "destination": {"id": destination, "groupId": group_id, "type": "PROCESSOR"},
                "selectedRelationships": relationships, "backPressureObjectThreshold": 10000,
                "backPressureDataSizeThreshold": "1 GB", "flowFileExpiration": "0 sec"}})
    finally:
        for identifier in list(reversed(running)) + ([new_id] if new_id and running else []):
            current = api("/processors/" + identifier)
            api(f"/processors/{identifier}/run-status", "PUT", {"revision": current["revision"], "state": "RUNNING"})
    updated = api("/flow/process-groups/root")["processGroupFlow"]["flow"]
    evidence = {"processors": [{"name": p["component"]["name"], "state": p["component"]["state"],
                    "validation": p["component"].get("validationStatus"), "relationships": p["component"].get("relationships")}
                    for p in updated["processors"]], "connections": len(updated["connections"]),
                "existing_processors_and_queues_preserved": True}
    path = Path(__file__).resolve().parents[1] / "docs/07-bao-cao-do-an/evidence/nifi_error_routes_2026-10-20.json"
    path.write_text(json.dumps(evidence, indent=2, ensure_ascii=False), encoding="utf-8")
    print(json.dumps(evidence, ensure_ascii=False))


if __name__ == "__main__":
    main()
