"""Audit ETL without loading facts, changing coverage, or writing to MinIO.

Writes aggregate evidence locally. Runtime mode uses a read-only DB transaction.
Example: --audit-date 2026-10-19 --source-date 2026-10-14 --runtime
"""
import argparse
from datetime import date, datetime
from decimal import Decimal
import json
import logging
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import pandas as pd
from sqlalchemy import text
from warehouse.etl.transform import unify_schema, clean_data, deduplicate_lines
from warehouse.etl.data_validation import DataValidator


def event(**updates):
    row = {"_platform": "shopee", "order_sn": "AUDIT", "item_sku": "A",
           "quantity": 2, "original_price": 10, "total_order_value": 20,
           "buyer_total_amount": 20, "create_time": "2026-10-13T17:01:00Z",
           "order_status": "COMPLETED"}
    row.update(updates)
    return row


def normalize(raw):
    frame = clean_data(unify_schema(raw), preserve_invalid=True)
    valid, rejected, _ = DataValidator().validate(frame)
    valid, duplicates = deduplicate_lines(valid)
    return valid, rejected, duplicates


def offline_checks():
    rows = [event(order_line_id="a"), event(item_sku="B", order_line_id="b")]
    raw = pd.DataFrame(rows + rows + [event(quantity=0), event(item_sku=None)])
    valid, rejected, duplicates = normalize(raw)
    assert len(valid) == 2 and len(rejected) == 2 and duplicates == 2
    assert len(raw) == len(valid) + len(rejected) + duplicates

    older = event(order_status="READY_TO_SHIP")
    newer = event(order_status="COMPLETED", completed_time="2026-10-15T01:00:00Z")
    forward, _, _ = normalize(pd.DataFrame([older, newer]))
    reverse, _, _ = normalize(pd.DataFrame([newer, older]))
    assert forward.iloc[0].order_status == "COMPLETED"
    assert reverse.iloc[0].order_status == "COMPLETED"

    no_ids, _, collapsed = normalize(pd.DataFrame([event(quantity=2), event(quantity=3)]))
    assert len(no_ids) == 1 and collapsed == 1

    voucher_valid, voucher_rejected, _ = normalize(pd.DataFrame([event(voucher_from_seller="bad", voucher_from_shopee=0)]))
    assert voucher_valid.empty and len(voucher_rejected) == 1

    discounted, rejected_discounted, _ = normalize(
        pd.DataFrame([event(model_discounted_price="bad")]))
    assert discounted.empty and len(rejected_discounted) == 1
    return {
        "balanced_fixture": {"raw": len(raw), "valid": len(valid),
                             "duplicate": duplicates, "rejected": len(rejected)},
        "verified_fixes": {
            "dedupe_independent_of_input_order": [str(forward.iloc[0].order_status),
                                               str(reverse.iloc[0].order_status)],
            "same_sku_without_line_ids_collapses": {"inputs": 2, "remaining": len(no_ids)},
            "malformed_voucher_quarantined": len(voucher_rejected),
            "malformed_discounted_price_quarantined": len(rejected_discounted),
        },
    }


def runtime_checks(source_date):
    from warehouse.etl.extract import build_minio_client, extract_orders
    from warehouse.etl.load import get_engine

    engine = get_engine()
    try:
        with engine.connect() as conn:
            conn.execute(text("SET TRANSACTION READ ONLY"))
            summary = conn.execute(text("""
                SELECT json_build_object(
                    'fact_lines', count(*), 'orders', count(DISTINCT (platform, order_id)),
                    'quantity', sum(quantity), 'cancelled_lines', count(*) FILTER(WHERE is_cancelled),
                    'missing_product', count(*) FILTER(WHERE product_key IS NULL),
                    'missing_date', count(*) FILTER(WHERE date_key IS NULL),
                    'missing_shop', count(*) FILTER(WHERE shop_key IS NULL),
                    'first_create_time', min(create_time), 'last_create_time', max(create_time)
                ) FROM Fact_Orders
            """)).scalar_one()
            summary["coverage_rows"] = conn.execute(text(
                "SELECT count(*) FROM Warehouse_Daily_Coverage")).scalar_one()
            historical = conn.execute(text("""
                SELECT json_build_object('rows', count(*), 'skus', count(DISTINCT sku),
                    'first_create_time', min(create_time), 'last_create_time', max(create_time))
                FROM Fact_Historical_Order_Lines
            """)).scalar_one()
            indexes = conn.execute(text("""
                SELECT indexname, indexdef FROM pg_indexes
                WHERE tablename='fact_orders' AND indexname='uq_fact_orders_order_platform_line'
            """)).mappings().all()

            raw = extract_orders(build_minio_client(), target_date=datetime.combine(source_date, datetime.min.time()))
            if raw.empty:
                return {"warehouse": summary, "historical": historical,
                        "reconciliation": {"source_date": str(source_date), "status": "NO_RAW_DATA"}}
            valid, rejected, duplicates = normalize(raw)
            assert len(raw) == len(valid) + len(rejected) + duplicates
            facts = conn.execute(text("""
                SELECT f.order_id, f.platform, f.source_line_id, f.quantity, f.subtotal,
                       f.buyer_total_amount, f.order_status, f.is_cancelled, p.sku
                FROM Fact_Orders f LEFT JOIN Dim_Products p USING(product_key)
                LEFT JOIN Dim_Dates d USING(date_key) WHERE d.full_date=:day
            """), {"day": source_date}).mappings().all()
            key = lambda row: (row["platform"], row["order_id"], row["source_line_id"])
            expected = {key(row): row for row in valid.to_dict("records")}
            actual = {key(row): row for row in facts}
            mismatches = 0
            for identity in expected.keys() & actual.keys():
                left, right = expected[identity], actual[identity]
                same = all(left[column] == right[column] for column in
                           ["quantity", "order_status", "is_cancelled", "sku"])
                same = same and all(Decimal(str(left[column])).quantize(Decimal("0.01")) == right[column]
                                    for column in ["subtotal", "buyer_total_amount"])
                mismatches += not same
            rejected_sources = {(row["source_object"], row["source_row"])
                                for row in rejected.to_dict("records")}
            audited_sources = set()
            quarantine_objects = 0
            client = build_minio_client()
            from warehouse.etl.extract import MINIO_BUCKET
            for obj in client.list_objects(MINIO_BUCKET, prefix="quarantine/orders/", recursive=True):
                if not obj.object_name.endswith(".jsonl"):
                    continue
                quarantine_objects += 1
                response = client.get_object(MINIO_BUCKET, obj.object_name)
                try:
                    payload = response.read().decode("utf-8")
                finally:
                    response.close()
                    response.release_conn()
                for line in payload.splitlines():
                    if line.strip():
                        audit_row = json.loads(line)
                        audited_sources.add((audit_row.get("source_object"), audit_row.get("source_row")))
            return {
                "warehouse": summary, "historical": historical,
                "line_unique_index": [dict(row) for row in indexes],
                "reconciliation": {
                    "source_date": str(source_date), "raw_objects": int(raw["_source_object"].nunique()),
                    "raw_rows": len(raw), "valid_lines": len(valid), "duplicate_rows": duplicates,
                    "rejected_rows": len(rejected), "accounting_balanced": True,
                    "quarantine_objects_inspected": quarantine_objects,
                    "rejected_source_rows_found_in_existing_quarantine": len(rejected_sources & audited_sources),
                    "warehouse_lines_on_date": len(facts),
                    "missing_in_warehouse": len(expected.keys() - actual.keys()),
                    "warehouse_keys_absent_from_selected_raw": len(actual.keys() - expected.keys()),
                    "common_keys_with_value_mismatch": mismatches,
                    "compared_fields": ["quantity", "order_status", "is_cancelled", "sku", "subtotal", "buyer_total_amount"],
                    "raw_valid_quantity": int(valid["quantity"].sum()),
                    "warehouse_quantity": sum(row["quantity"] for row in facts),
                    "rejection_reasons": rejected["rejection_reasons"].value_counts().to_dict()
                    if not rejected.empty else {},
                },
            }
    finally:
        engine.dispose()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--audit-date", type=date.fromisoformat, required=True)
    parser.add_argument("--source-date", type=date.fromisoformat, default=date(2026, 10, 14))
    parser.add_argument("--runtime", action="store_true")
    args = parser.parse_args()
    logging.disable(logging.CRITICAL)
    evidence = {"audit_date": str(args.audit_date), "offline": offline_checks()}
    if args.runtime:
        evidence["runtime_readonly"] = runtime_checks(args.source_date)
    output = Path(__file__).resolve().parents[1] / "docs/07-bao-cao-do-an/evidence" / f"etl_warehouse_{args.audit_date}.json"
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(evidence, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")
    print(json.dumps(evidence, ensure_ascii=False, indent=2, default=str))


if __name__ == "__main__":
    main()
