"""Inspect ingestion contracts without connecting to Kafka, MinIO, or PostgreSQL.

Uses in-memory clients and records both verified behavior and reproduced risks.
Run from the repository root with the project's Python dependencies available.
"""
import io
import json
import logging
from pathlib import Path
import sys
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pyarrow.parquet as pq
from ingestion import consumer_to_minio as orders
from ingestion import consumer_inventory_to_postgres as inventory
from ingestion import producer


class MemoryMinio:
    def __init__(self, fail_on=None):
        self.calls = 0
        self.fail_on = fail_on
        self.objects = {}

    def put_object(self, *, bucket_name, object_name, data, length, content_type):
        self.calls += 1
        if self.calls == self.fail_on:
            raise RuntimeError("injected upload failure")
        payload = data.read()
        assert len(payload) == length
        self.objects[object_name] = pq.read_table(io.BytesIO(payload)).to_pylist()


def inspect_loop(upload_fails=False):
    trace = []
    consumer = MagicMock()

    def messages():
        yield SimpleNamespace(value={"id": "one"})
        orders._running = False

    def flush(*args):
        trace.append("upload_failed" if upload_fails else "uploaded")
        if upload_fails:
            raise RuntimeError("injected upload failure")
        return 1

    consumer.__iter__.side_effect = messages
    consumer.commit.side_effect = lambda: trace.append("committed")
    consumer.close.side_effect = lambda: trace.append("closed")
    with patch.object(orders, "build_consumer", return_value=consumer), \
         patch.object(orders, "build_minio_client", return_value=object()), \
         patch.object(orders, "ensure_bucket"), \
         patch.object(orders, "flush_batch_to_minio", side_effect=flush), \
         patch.object(orders, "BATCH_SIZE", 1), \
         patch.object(orders, "_running", True):
        try:
            orders.main()
        except RuntimeError:
            if not upload_fails:
                raise
    assert "closed" in trace
    if upload_fails:
        assert consumer.commit.call_count == 0
    else:
        assert trace == ["uploaded", "committed", "closed"]
    return trace


def main():
    logging.disable(logging.CRITICAL)
    with patch.object(producer, "KafkaProducer") as factory:
        producer.build_producer()
        config = factory.call_args.kwargs
        assert config["acks"] == "all" and config["retries"] == 3
        producer_settings = {key: config[key] for key in
                             ["acks", "retries", "linger_ms", "batch_size", "compression_type"]}

    success_trace = inspect_loop()
    failure_trace = inspect_loop(upload_fails=True)

    records = [{"id": platform, "_platform": platform,
                "create_time": "2026-10-16T00:00:00+07:00"}
               for platform in ["shopee", "tiktok"]]
    client = MemoryMinio(fail_on=2)
    try:
        orders.flush_batch_to_minio(records, client)
    except RuntimeError:
        pass
    assert len(client.objects) == 1
    client.fail_on = None
    orders.flush_batch_to_minio(records, client)
    assert len(client.objects) == 3
    stored_rows = [row for rows in client.objects.values() for row in rows]
    assert len(stored_rows) == 3 and len({r["id"] for r in stored_rows}) == 2

    null_client = MemoryMinio()
    null_uploaded = orders.flush_batch_to_minio(
        [{"id": "null-platform", "_platform": None,
          "create_time": "2026-10-16T00:00:00+07:00"}], null_client)
    assert null_uploaded == 0 and not null_client.objects

    snapshots = inventory.prepare_snapshots([
        {"sku": "AUDIT", "loaded_at": "2026-10-15T18:00:00Z", "stock_on_hand": 10},
        {"sku": "AUDIT", "loaded_at": "2026-10-15T17:00:00Z", "stock_on_hand": 20},
    ])
    assert len(snapshots) == 1
    assert snapshots.iloc[0]["stock_on_hand"] == 10
    assert str(snapshots.iloc[0]["snapshot_date"]) == "2026-10-16"

    result = {
        "audit_date": "2026-10-16",
        "mode": "offline_in_memory_no_service_connections",
        "verified_behaviors": {
            "producer_configuration": producer_settings,
            "successful_upload_commit_order": success_trace,
            "failed_upload_no_commit": failure_trace,
            "inventory_latest_in_batch_business_date": str(snapshots.iloc[0]["snapshot_date"]),
        },
        "reproduced_risks": {
            "partial_upload_then_replay": {"input_rows": 2, "stored_rows": 3,
                                           "unique_ids": 2, "objects": 3},
            "null_platform_dropped_without_exception": {"input_rows": 1, "objects": null_uploaded},
        },
    }
    output = Path(__file__).resolve().parents[1] / "docs/07-bao-cao-do-an/evidence/ingestion_offline_2026-10-16.json"
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(result, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(json.dumps(result, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
