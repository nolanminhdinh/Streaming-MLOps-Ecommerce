"""Test 1: real Kafka -> MinIO -> warehouse reconciliation, without model calls.

Uses a private consumer group and unique IDs. ETL selects only the Parquet
objects written by this consumer, using real listing, reads, transforms and
database writes. Test records remain available for inspection. Exit 1 means
the integrity gate failed; JSON and Markdown retain the evidence.
"""
import json
import sys
import threading
import time
import uuid
from collections import Counter
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch
from zoneinfo import ZoneInfo

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import pandas as pd
from sqlalchemy import text
from data_simulator.data_simulator import ECommerceSimulator
from ingestion import producer, consumer_to_minio as consumer_module
from warehouse.etl import extract, pipeline
from warehouse.etl.transform import unify_schema, clean_data, deduplicate_lines
from warehouse.etl.data_validation import DataValidator
from warehouse.etl.load import get_engine

TZ = ZoneInfo("Asia/Ho_Chi_Minh")


def fixtures(run_id):
    simulator = ECommerceSimulator(cancel_rate=0)
    today = datetime.now(TZ).date()
    base = datetime.combine(today - timedelta(days=1), datetime.min.time(), timezone.utc)
    events, expected, invalid = [], [], []
    for platform in ("shopee", "tiktok"):
        for case, line in [("normal", 0), ("cancelled", 0), ("shared", 0),
                           ("before_midnight", 0), ("after_midnight", 0),
                           ("multi", 0), ("multi", 1), ("multi", 2),
                           ("same_sku", 0), ("same_sku", 1),
                           ("invalid", 0)]:
            stamp = base + timedelta(hours=17, minutes=1)
            if case == "before_midnight":
                stamp = base + timedelta(hours=16, minutes=59)
            order_id = f"{run_id}-{case}" + ("" if case == "shared" else f"-{platform[0]}")
            sku = f"{run_id}-{platform[0]}-{case}-{line}"
            if case == "same_sku":
                sku = f"{run_id}-{platform[0]}-{case}"
            qty, price = line + 2, 10000 + 1000 * line
            status = "CANCELLED" if case == "cancelled" else "COMPLETED"
            event = asdict(simulator._generate_shopee_event(stamp) if platform == "shopee"
                           else simulator._generate_tiktok_event(stamp))
            event.update(_platform=platform, _test_event_id=f"{run_id}-{len(events):02d}",
                         quantity=qty, order_status=status, shop_id=run_id,
                         shop_name="Integrity test " + run_id,
                         shipped_time=None, completed_time=None,
                         seller_discount=0, commission_fee=0, service_fee=0,
                         transaction_fee=0, shipping_fee=0, original_shipping_fee=0,
                         tax_amount=0)
            if platform == "shopee":
                event.update(order_sn=order_id, item_sku=sku, create_time=stamp.isoformat(),
                             pay_time=None, cancel_time=(stamp + timedelta(minutes=10)).isoformat()
                             if status == "CANCELLED" else None,
                             cancel_reason="Integrity test" if status == "CANCELLED" else None,
                             original_price=price, model_discounted_price=price,
                             total_order_value=qty * price, buyer_total_amount=qty * price,
                             shopee_discount=0, voucher_from_seller=0, voucher_from_shopee=0,
                             actual_shipping_fee=0, estimated_shipping_fee=0)
            else:
                event.update(order_id=order_id, seller_sku=sku, created_time=stamp.isoformat(),
                             paid_time=None, cancelled_time=(stamp + timedelta(minutes=10)).isoformat()
                             if status == "CANCELLED" else None,
                             item_cancel_reason="Integrity test" if status == "CANCELLED" else None,
                             item_original_price=price, item_sale_price=price,
                             sub_total=qty * price, total_amount=qty * price, platform_discount=0)
            if case == "invalid":
                if platform == "shopee":
                    event["item_sku"] = None
                else:
                    event["quantity"] = 0
                invalid.append({"order_id": order_id, "platform": platform,
                                "reason": "missing SKU" if platform == "shopee" else "quantity=0"})
            else:
                source_line_id = "sku:" + sku
                if case == "same_sku":
                    event["order_line_id"] = f"line-{line}"
                    source_line_id = "id:" + event["order_line_id"]
                expected.append({"order_id": order_id, "platform": platform, "sku": sku,
                                 "source_line_id": source_line_id,
                                 "quantity": qty, "original_price": price,
                                 "subtotal": qty * price, "buyer_total_amount": qty * price,
                                 "order_status": status, "is_cancelled": status == "CANCELLED",
                                 "create_time": stamp.astimezone(TZ).replace(tzinfo=None).isoformat(),
                                 "day": stamp.astimezone(TZ).date().isoformat(), "case": case})
            events.append(event)
    return events, expected, invalid


def main():
    run_id = "DI" + datetime.now(TZ).strftime("%Y%m%d") + uuid.uuid4().hex[:6]
    output = Path("data/runtime_checks") / run_id
    output.mkdir(parents=True, exist_ok=True)
    result = {"run_id": run_id, "status": "RUNNING", "timezone": str(TZ),
              "scope": "22 synthetic events; real services; isolated Parquet cohort; no model accuracy test",
              "checks": []}
    events, expected, invalid = fixtures(run_id)
    result.update(expected_lines=expected, invalid_inputs=invalid, kafka_acks=[])
    (output / "input_events.json").write_text(json.dumps(events, indent=2, default=str), encoding="utf-8")
    objects, errors = [], []
    ready = threading.Event()
    client = consumer_module.build_minio_client()
    original_put, original_consumer = client.put_object, consumer_module.KafkaConsumer
    original_list = extract.list_parquet_files

    def record_put(*args, **kwargs):
        response = original_put(*args, **kwargs)
        objects.append(kwargs["object_name"])
        return response

    def private_consumer(*args, **kwargs):
        kwargs.update(group_id=run_id, auto_offset_reset="latest")
        consumer = original_consumer(*args, **kwargs)
        deadline = time.monotonic() + 30
        while not consumer.assignment():
            consumer.poll(timeout_ms=1000)
            if time.monotonic() > deadline:
                consumer.close()
                raise TimeoutError("Kafka assignment timeout")
        consumer.seek_to_end(*consumer.assignment())
        ready.set()
        return consumer

    def worker():
        try:
            consumer_module.main()
        except Exception as exc:
            errors.append(f"{type(exc).__name__}: {exc}")

    def check(name, passed, evidence):
        result["checks"].append({"name": name, "status": "PASS" if passed else "FAIL", "evidence": evidence})

    def cohort_listing(*args, **kwargs):
        return [name for name in original_list(*args, **kwargs) if name in objects]

    thread, kafka_producer = None, None
    try:
        with patch.object(client, "put_object", record_put), \
             patch.object(consumer_module, "build_minio_client", lambda: client), \
             patch.object(consumer_module, "KafkaConsumer", private_consumer), \
             patch.object(consumer_module, "BATCH_SIZE", 6), \
             patch.object(consumer_module, "BATCH_TIMEOUT_SEC", 2):
            consumer_module._running = True
            thread = threading.Thread(target=worker, daemon=True)
            thread.start()
            try:
                if not ready.wait(35):
                    raise TimeoutError(f"Consumer not ready: {errors}")
                kafka_producer = producer.build_producer()
                for event in events:
                    metadata = kafka_producer.send(producer.TOPIC_ORDERS,
                        key=(event.get("order_sn") or event["order_id"]).encode(), value=event).get(timeout=15)
                    result["kafka_acks"].append({"event_id": event["_test_event_id"], "topic": metadata.topic,
                                                "partition": metadata.partition, "offset": metadata.offset})
                kafka_producer.flush(timeout=15)
                # Final consumer flush also commits offsets, so wait for it before reads.
                time.sleep(4)
            finally:
                consumer_module._running = False
                thread.join(timeout=15)
                if kafka_producer is not None:
                    kafka_producer.close(timeout=5)
            if thread.is_alive() or errors:
                raise RuntimeError(f"Consumer shutdown/errors: alive={thread.is_alive()}, errors={errors}")
        frames = [extract.read_parquet_from_minio(client, consumer_module.MINIO_BUCKET, name) for name in objects]
        raw = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
        actual_ids = Counter(raw.get("_test_event_id", pd.Series(dtype=str)).tolist())
        wanted_ids = Counter(event["_test_event_id"] for event in events)
        check("kafka_to_minio_exactly_preserved", actual_ids == wanted_ids,
              {"acked": len(result["kafka_acks"]), "raw_rows": len(raw), "objects": objects,
               "missing": list((wanted_ids - actual_ids).elements()), "extra": list((actual_ids - wanted_ids).elements())})
        partition_errors = []
        for name, frame in zip(objects, frames):
            column = "create_time" if "platform=shopee/" in name else "created_time"
            dates = pd.to_datetime(frame[column], format="mixed", utc=True).dt.tz_convert(str(TZ))
            if not all(value in name for value in dates.dt.strftime("year=%Y/month=%m/day=%d")):
                partition_errors.append(name)
        check("vietnam_partition", not partition_errors, partition_errors)
        unified = unify_schema(raw.copy())
        transformed, cleaning = clean_data(unified, return_report=True, preserve_invalid=True)
        valid, quarantine, validation = DataValidator().validate(transformed)
        valid, cleaning["duplicates_removed"] = deduplicate_lines(valid)
        result["stages"] = {"raw": len(raw), "unified": len(unified), "after_cleaning": len(transformed),
                            "valid": len(valid), "quarantined": len(quarantine), "cleaning": cleaning,
                            "validation": asdict(validation)}
        with patch.object(extract, "list_parquet_files", cohort_listing):
            result["etl"] = [pipeline.run_etl_pipeline(datetime.fromisoformat(day))
                             for day in sorted({row["day"] for row in expected})]
        engine = get_engine()
        with engine.connect() as conn:
            rows = [dict(row) for row in conn.execute(text("""
                SELECT f.order_id, f.source_line_id, f.platform, p.sku, f.quantity, f.original_price,
                       f.subtotal, f.buyer_total_amount, f.order_status, f.is_cancelled,
                       f.create_time, d.full_date::TEXT AS day,
                       f.product_key, f.shop_key, f.geo_key, f.date_key, f.payment_key, f.carrier_key
                FROM Fact_Orders f LEFT JOIN Dim_Products p ON p.product_key=f.product_key
                LEFT JOIN Dim_Dates d ON d.date_key=f.date_key
                WHERE f.order_id LIKE :prefix ORDER BY f.platform, f.order_id, p.sku
            """), {"prefix": run_id + "%"}).mappings()]
        result["warehouse_rows"] = rows
        key = lambda row: (row["platform"], row["order_id"], row["source_line_id"])
        observed = {key(row): row for row in rows}
        missing = [row for row in expected if key(row) not in observed]
        check("all_valid_order_lines_preserved", len(rows) == len(expected) and not missing,
              {"expected": len(expected), "actual": len(rows), "missing_lines": missing})
        expected_keys = {key(row) for row in expected}
        mismatch = []
        for row in expected:
            actual = observed.get(key(row))
            if actual is None:
                continue
            for field in ("quantity", "original_price", "subtotal", "buyer_total_amount", "order_status", "is_cancelled", "day", "create_time"):
                value = actual[field].isoformat() if field == "create_time" else actual[field]
                if value != row[field]:
                    mismatch.append({"key": key(row), "field": field, "expected": row[field], "actual": value})
        check("retained_line_fields_and_vietnam_dates", not mismatch, mismatch)
        check("cancelled_orders_preserved", all(key(row) in observed and observed[key(row)]["is_cancelled"]
              for row in expected if row["case"] == "cancelled"),
              [row for row in rows if row["order_status"] == "CANCELLED"])
        check("same_order_id_across_platforms", all(key(row) in observed for row in expected if row["case"] == "shared"),
              [row for row in rows if "-shared" in row["order_id"]])
        check("warehouse_totals", sum(row["quantity"] for row in expected) == sum(row["quantity"] for row in rows)
              and sum(row["subtotal"] for row in expected) == sum(row["subtotal"] for row in rows),
              {"expected_units": sum(row["quantity"] for row in expected), "actual_units": sum(row["quantity"] for row in rows),
               "expected_subtotal_vnd": sum(row["subtotal"] for row in expected), "actual_subtotal_vnd": sum(row["subtotal"] for row in rows)})
        source_by_key = {key(row): row for row in unified.to_dict("records")}
        nulls, absent_optional_sources = [], []
        for row in rows:
            for field in ("product_key", "shop_key", "geo_key", "date_key", "payment_key", "carrier_key"):
                if row[field] is not None:
                    continue
                source_field = {"carrier_key": "carrier_name", "payment_key": "payment_method"}.get(field)
                source_value = source_by_key[key(row)].get(source_field) if source_field else None
                if source_field and (pd.isna(source_value) or not str(source_value).strip()):
                    absent_optional_sources.append({"key": key(row), "field": field, "reason": "source value absent"})
                else:
                    nulls.append({"key": key(row), "field": field})
        check("dimension_keys_when_source_present", not nulls,
              {"unresolved_keys": nulls, "optional_source_absences": absent_optional_sources})
        check("no_unexpected_warehouse_lines", all(key(row) in expected_keys for row in rows), [])
        invalid_keys = {(row["platform"], row["order_id"]) for row in invalid}
        rejection_keys = {(row["platform"], row["order_id"]) for row in quarantine.to_dict("records")}
        durable_rejects = []
        for stats in result["etl"]:
            if stats["quarantine_object"]:
                response = client.get_object(consumer_module.MINIO_BUCKET, stats["quarantine_object"])
                try:
                    durable_rejects.extend(json.loads(line) for line in response.read().decode("utf-8").splitlines())
                finally:
                    response.close()
                    response.release_conn()
        durable_keys = {(row["platform"], row["order_id"]) for row in durable_rejects}
        check("invalid_records_reach_quarantine_with_reason", invalid_keys <= rejection_keys and invalid_keys == durable_keys
              and all(row.get("rejection_reasons") and row.get("raw_payload") and row.get("source_object") for row in durable_rejects),
              {"invalid_inputs": invalid, "quarantine": quarantine.to_dict("records"),
               "durable_rejections": durable_rejects})
        with patch.object(extract, "list_parquet_files", cohort_listing):
            result["replay_etl"] = [pipeline.run_etl_pipeline(datetime.fromisoformat(day))
                                    for day in sorted({row["day"] for row in expected})]
        with engine.connect() as conn:
            replay = conn.execute(text("SELECT COUNT(*), SUM(quantity), SUM(subtotal) FROM Fact_Orders WHERE order_id LIKE :prefix"),
                                  {"prefix": run_id + "%"}).one()
        check("warehouse_replay", tuple(replay) == (len(rows), sum(row["quantity"] for row in rows), sum(row["subtotal"] for row in rows)), list(replay))
        check("etl_accounting", all(stats["raw_records"] == stats["accounted_records"] for stats in result["etl"]), result["etl"])
        result["status"] = "PASS" if all(row["status"] == "PASS" for row in result["checks"]) else "FAIL"
    except Exception as exc:
        result.update(status="ERROR", error=f"{type(exc).__name__}: {exc}")
        raise
    finally:
        result["completed_at_vietnam"] = datetime.now(TZ).isoformat()
        (output / "result.json").write_text(json.dumps(result, indent=2, ensure_ascii=False, default=str), encoding="utf-8")
        report = [f"# Bài 1 — Toàn vẹn dữ liệu: {result['status']}", "", f"Run ID: `{run_id}`",
                  f"Thời gian Việt Nam: {result['completed_at_vietnam']}", "",
                  "Kafka → consumer thật → MinIO Parquet → ETL thật → PostgreSQL. Chỉ chọn các file của lượt thử này khi chạy ETL.",
                  f"{len(events)} sự kiện tổng hợp: {len(expected)} dòng hợp lệ và {len(invalid)} dòng lỗi. Bao gồm hai dòng cùng SKU có line ID khác nhau. Đây là kiểm tra chức năng toàn vẹn, chưa phải kiểm tra tải production.", "",
                  "| Kiểm tra | Kết quả |", "|---|---|"]
        report += [f"| {row['name']} | {row['status']} |" for row in result["checks"]]
        report += ["", "Chi tiết bằng chứng:", "", "```json", json.dumps(result, indent=2, ensure_ascii=False, default=str), "```", ""]
        report_path = Path("docs/03-testing-and-benchmark") / f"data_integrity_{run_id}.md"
        report_path.write_text("\n".join(report), encoding="utf-8")
        print("DATA_INTEGRITY_RESULT", json.dumps({"run_id": run_id, "status": result["status"],
              "checks": [{"name": row["name"], "status": row["status"]} for row in result["checks"]],
              "result": str(output / "result.json"), "report": str(report_path)}, ensure_ascii=False))
    return 0 if result["status"] == "PASS" else 1


if __name__ == "__main__":
    sys.exit(main())
