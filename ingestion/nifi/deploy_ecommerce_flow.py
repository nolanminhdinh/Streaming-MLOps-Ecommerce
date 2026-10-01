"""
Deploy E-Commerce Streaming Pipeline directly onto Apache NiFi Canvas via NiFi REST API.
"""
import sys
import httpx

NIFI_BASE_URL = "http://localhost:8080/nifi-api"

def deploy():
    client = httpx.Client(base_url=NIFI_BASE_URL, timeout=30.0)
    
    # 1. Get Root Process Group ID
    resp = client.get("/process-groups/root")
    resp.raise_for_status()
    root_group = resp.json()
    root_id = root_group["id"]
    print(f"Connected to NiFi. Root Process Group ID: {root_id}")

    # 2. Check and clear any existing processors on root if needed
    flow_resp = client.get(f"/flow/process-groups/{root_id}")
    flow_resp.raise_for_status()
    flow_data = flow_resp.json().get("processGroupFlow", {}).get("flow", {})
    
    existing_conns = flow_data.get("connections", [])
    for conn in existing_conns:
        cid = conn["id"]
        cver = conn["revision"]["version"]
        client.delete(f"/connections/{cid}?version={cver}")
    
    existing_procs = flow_data.get("processors", [])
    for proc in existing_procs:
        pid = proc["id"]
        pver = proc["revision"]["version"]
        # Stop processor first if running
        try:
            client.put(f"/processors/{pid}/run-status", json={
                "revision": {"version": pver},
                "state": "STOPPED"
            })
        except Exception:
            pass
        client.delete(f"/processors/{pid}?version={pver}")

    print("Canvas cleared for fresh pipeline deployment.")

    # 3. Define processors
    processors_def = [
        {
            "key": "kafka",
            "type": "org.apache.nifi.processors.kafka.pubsub.ConsumeKafka_2_6",
            "name": "1. ConsumeKafka (ecom.orders.raw)",
            "x": 100.0,
            "y": 200.0,
            "properties": {
                "bootstrap.servers": "kafka:9092",
                "topic": "ecom.orders.raw",
                "group.id": "nifi-ecom-orders-consumer",
                "auto.offset.reset": "earliest",
                "key-attribute-encoding": "utf-8",
                "message-header-encoding": "UTF-8"
            },
            "autoTerminated": []
        },
        {
            "key": "json",
            "type": "org.apache.nifi.processors.standard.EvaluateJsonPath",
            "name": "2. EvaluateJsonPath (Extract Fields)",
            "x": 480.0,
            "y": 200.0,
            "properties": {
                "Destination": "flowfile-attribute",
                "Return Type": "auto-detect",
                "order_id": "$.order_id",
                "user_id": "$.user_id",
                "platform": "$.platform",
                "total_amount": "$.total_amount",
                "created_at": "$.created_at"
            },
            "autoTerminated": ["failure", "unmatched"]
        },
        {
            "key": "route",
            "type": "org.apache.nifi.processors.standard.RouteOnAttribute",
            "name": "3. RouteOnAttribute (Branch Shopee/TikTok)",
            "x": 860.0,
            "y": 200.0,
            "properties": {
                "Routing Strategy": "Route to Property name",
                "shopee": "${platform:equals('Shopee')}",
                "tiktok": "${platform:equals('TikTok')}"
            },
            "autoTerminated": ["unmatched"]
        },
        {
            "key": "update",
            "type": "org.apache.nifi.processors.attributes.UpdateAttribute",
            "name": "4. UpdateAttribute (MinIO S3 Key)",
            "x": 1240.0,
            "y": 200.0,
            "properties": {
                "s3.bucket": "ecom-raw-data",
                "filename": "${platform:toLower()}/${now():format('yyyy/MM/dd')}/${order_id}.json",
                "mime.type": "application/json"
            },
            "autoTerminated": []
        },
        {
            "key": "s3",
            "type": "org.apache.nifi.processors.aws.s3.PutS3Object",
            "name": "5. PutS3Object (MinIO Data Lake)",
            "x": 1620.0,
            "y": 200.0,
            "properties": {
                "Bucket": "${s3.bucket}",
                "Object Key": "${filename}",
                "Endpoint Override URL": "http://minio:9000",
                "Access Key": "minioadmin",
                "Secret Key": "minioadmin",
                "Signer Override": "AWSS3V4SignerType",
                "use-path-style-access": "true",
                "Region": "us-east-1",
                "Content Type": "application/json"
            },
            "autoTerminated": ["failure"]
        },
        {
            "key": "log",
            "type": "org.apache.nifi.processors.standard.LogAttribute",
            "name": "6. LogAttribute (Provenance & Metrics)",
            "x": 2000.0,
            "y": 200.0,
            "properties": {
                "Log Level": "info",
                "Log Payload": "false",
                "Attributes to Log": "order_id,platform,total_amount,s3.bucket,filename"
            },
            "autoTerminated": ["success"]
        }
    ]

    created_procs = {}

    for p in processors_def:
        # Create processor
        create_resp = client.post(f"/process-groups/{root_id}/processors", json={
            "revision": {"version": 0},
            "component": {
                "type": p["type"],
                "name": p["name"],
                "position": {"x": p["x"], "y": p["y"]}
            }
        })
        create_resp.raise_for_status()
        p_data = create_resp.json()
        pid = p_data["id"]
        pver = p_data["revision"]["version"]
        
        # Configure processor properties and auto-terminated relationships
        config_update = {
            "revision": {"version": pver},
            "component": {
                "id": pid,
                "config": {
                    "properties": p["properties"],
                    "autoTerminatedRelationships": p["autoTerminated"]
                }
            }
        }
        update_resp = client.put(f"/processors/{pid}", json=config_update)
        if update_resp.status_code != 200:
            print(f"Error configuring processor {p['name']}: {update_resp.text}")
            update_resp.raise_for_status()

        created_procs[p["key"]] = update_resp.json()
        print(f"Created & configured: {p['name']}")

    # 4. Wire connections
    connections_def = [
        ("kafka", "json", ["success"], "1 -> 2: Raw Kafka Order"),
        ("json", "route", ["matched"], "2 -> 3: Extracted Attributes"),
        ("route", "update", ["shopee", "tiktok"], "3 -> 4: Shopee & TikTok Orders"),
        ("update", "s3", ["success"], "4 -> 5: Tagged MinIO FlowFile"),
        ("s3", "log", ["success"], "5 -> 6: Stored Data Lake Provenance")
    ]

    for src_key, dest_key, rels, conn_name in connections_def:
        src_id = created_procs[src_key]["id"]
        dest_id = created_procs[dest_key]["id"]
        conn_resp = client.post(f"/process-groups/{root_id}/connections", json={
            "revision": {"version": 0},
            "component": {
                "name": conn_name,
                "source": {
                    "id": src_id,
                    "groupId": root_id,
                    "type": "PROCESSOR"
                },
                "destination": {
                    "id": dest_id,
                    "groupId": root_id,
                    "type": "PROCESSOR"
                },
                "selectedRelationships": rels
            }
        })
        if conn_resp.status_code != 201:
            print(f"Error creating connection {conn_name}: {conn_resp.text}")
            conn_resp.raise_for_status()
        print(f"Connected: {conn_name} (relationships: {rels})")

    print("\nSUCCESS: Entire streaming pipeline has been drawn and connected on the NiFi Canvas!")
    return created_procs

if __name__ == "__main__":
    deploy()
