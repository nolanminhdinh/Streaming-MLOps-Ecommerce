"""Regressions for lossless raw ingestion, invalid payloads and event ordering."""
from datetime import date
from types import SimpleNamespace
from unittest import mock
import os

import pandas as pd
import pytest
from sqlalchemy import text

from ingestion.consumer_inventory_to_postgres import prepare_snapshots, upsert_inventory
from warehouse.etl import extract
from warehouse.etl.load import get_engine, load_fact_orders, ensure_dim_dates
from warehouse.etl.transform import clean_data, unify_schema, deduplicate_lines
from warehouse.etl.data_validation import DataValidator
from tests.test_order_line_integrity import event


def test_null_empty_and_unsupported_platforms_are_preserved():
    from tests.test_ingestion import partitions_by_id
    result = partitions_by_id([dict(id=str(i), _platform=value, create_time="2026-10-20T00:00:00Z")
                               for i, value in enumerate([None, "", " ", "bad/path", "Shopee"])])
    assert len(result) == 5
    assert all("platform=unknown/" in result[str(i)] for i in range(4))
    assert "platform=shopee/" in result["4"]


def test_date_extraction_includes_unknown_partition():
    client = mock.Mock()
    client.list_objects.side_effect = lambda bucket, prefix, recursive: [SimpleNamespace(object_name=prefix + "data.parquet")]
    paths = extract.list_parquet_files(client, target_date=date(2026, 10, 20))
    assert len(paths) == 3 and any("platform=unknown/" in path for path in paths)


@pytest.mark.parametrize("updates,reason", [
    ({"model_discounted_price": "bad"}, "INVALID_DISCOUNTED_PRICE_TYPE"),
    ({"voucher_from_seller": "bad"}, "INVALID_VOUCHER_TOTAL_TYPE"),
    ({"update_time": "bad"}, "INVALID_SOURCE_UPDATED_AT_TYPE"),
    ({"pay_time": "bad"}, "INVALID_PAY_TIME_TYPE"),
    ({"item_weight": "bad"}, "INVALID_WEIGHT_KG"),
    ({"item_name": "x" * 501}, "TOO_LONG_PRODUCT_NAME"),
])
def test_bad_optional_values_reach_quarantine(updates, reason):
    df = clean_data(unify_schema(pd.DataFrame([event(**updates)])), preserve_invalid=True)
    valid, rejected, _ = DataValidator().validate(df)
    assert valid.empty and len(rejected) == 1
    assert reason in rejected.iloc[0].rejection_reasons


def test_dedupe_chooses_source_version_independent_of_arrival_order():
    old = event(update_time="2026-10-20T00:00:00Z", order_status="READY_TO_SHIP")
    new = event(update_time="2026-10-20T01:00:00Z", order_status="COMPLETED")
    for rows in ([old, new], [new, old]):
        df = clean_data(unify_schema(pd.DataFrame(rows)), preserve_invalid=True)
        latest, count = deduplicate_lines(df)
        assert count == 1 and latest.iloc[0].order_status == "COMPLETED"


@pytest.mark.parametrize("updates", [{"loaded_at": "bad"}, {"sku": " "}, {"stock_on_hand": -1},
                                      {"stock_on_hand": 1.5}, {"stock_on_hand": None}, {"sku": "x" * 51}])
def test_bad_inventory_is_not_silently_committed(updates):
    record = dict(sku="A", loaded_at="2026-10-20T00:00:00Z", stock_on_hand=3)
    record.update(updates)
    with pytest.raises(ValueError):
        prepare_snapshots([record])


def test_nonfinite_amounts_are_rejected_and_quarantine_is_valid_json():
    from warehouse.etl.quarantine import persist_quarantine
    df = clean_data(unify_schema(pd.DataFrame([event(original_price=float("inf"))])), preserve_invalid=True)
    valid, rejected, _ = DataValidator().validate(df)
    assert valid.empty and len(rejected) == 1
    client = mock.Mock()
    persist_quarantine(client, rejected, "nonfinite")
    payload = client.put_object.call_args.kwargs["data"].getvalue()
    assert b'"original_price": "inf"' in payload


def test_sparse_source_dates_cannot_pass_training_gate():
    from ml.training.data_readiness import assess_order_history
    report = assess_order_history(pd.DataFrame({"sku": ["A", "A"], "create_time": ["2026-01-01", "2026-10-20"]}))
    assert not report["data_sufficient"]
    assert report["missing_history_days"] == 90
    assert report["calendar_span_days"] > 92


def test_prices_never_backfill_from_future_or_other_sku():
    from ml.features.time_series_features import TimeSeriesFeatureExtractor
    df = pd.DataFrame([dict(sku="A", create_time="2026-10-01", quantity=1, original_price=10, order_id="a", platform="shopee"),
                       dict(sku="B", create_time="2026-10-03", quantity=1, original_price=99, order_id="b", platform="shopee")])
    df["buyer_total_amount"] = df["original_price"]
    daily = TimeSeriesFeatureExtractor().aggregate_daily(df)
    assert (daily.loc[(daily.sku == "B") & (daily.date < date(2026, 10, 3)), "avg_unit_price"] == 0).all()


@pytest.mark.parametrize("updates", [{"lead_time_days": -1}, {"lead_time_days": 31}, {"service_level": 2},
                                      {"service_level": None}, {"current_stocks": {"A": -3}}])
def test_inventory_request_rejects_invalid_business_parameters(updates):
    from pydantic import ValidationError
    from serving.app.schemas import ReorderAlertRequest
    with pytest.raises(ValidationError):
        ReorderAlertRequest(**updates)


def test_malformed_json_reaches_raw_quarantine_instead_of_crashing_deserializer():
    from ingestion.consumer_to_minio import decode_event
    for payload in [b"not json", b"[]", b"null", b"\xff"]:
        decoded = decode_event(payload)
        assert decoded["_ingest_error"] == "INVALID_JSON_OBJECT"
        normalized = clean_data(unify_schema(pd.DataFrame([decoded])), preserve_invalid=True)
        valid, rejected, _ = DataValidator().validate(normalized)
        assert valid.empty and len(rejected) == 1


def test_mixed_numeric_and_text_payloads_survive_parquet_roundtrip():
    import io
    from ingestion.consumer_to_minio import flush_batch_to_minio
    from tests.test_ingestion import MemoryMinio
    client = MemoryMinio()
    assert flush_batch_to_minio([event(quantity=2), event(quantity="bad")], client) == 1
    path, stored = next(iter(client.objects.items()))
    response = mock.Mock()
    response.read.return_value = stored.to_parquet(index=False)
    raw = extract.read_parquet_from_minio(mock.Mock(get_object=mock.Mock(return_value=response)), "bucket", path)
    frame = clean_data(unify_schema(raw), preserve_invalid=True)
    valid, rejected, _ = DataValidator().validate(frame)
    assert len(valid) == len(rejected) == 1
    assert rejected.iloc[0].raw_payload["quantity"] == "bad"


def test_inventory_quarantine_failure_prevents_database_write():
    from ingestion.consumer_inventory_to_postgres import flush_inventory_batch
    with mock.patch("ingestion.consumer_inventory_to_postgres.persist_quarantine", side_effect=RuntimeError("storage failed")), \
         mock.patch("ingestion.consumer_inventory_to_postgres.upsert_inventory") as upsert:
        with pytest.raises(RuntimeError, match="storage failed"):
            flush_inventory_batch(mock.Mock(), [{"sku": None}], mock.Mock())
        upsert.assert_not_called()


def test_inventory_mixed_batch_persists_reject_and_valid_snapshot():
    from ingestion.consumer_inventory_to_postgres import flush_inventory_batch
    with mock.patch("ingestion.consumer_inventory_to_postgres.persist_quarantine") as quarantine, \
         mock.patch("ingestion.consumer_inventory_to_postgres.upsert_inventory", return_value=1) as upsert:
        count = flush_inventory_batch(mock.Mock(), [{"sku": None}, dict(sku="A", stock_on_hand=2, loaded_at="2026-10-20T00:00:00Z")], mock.Mock())
    assert count == 1 and len(upsert.call_args.args[1]) == 1
    assert len(quarantine.call_args.args[1]) == 1 and quarantine.call_args.kwargs["stream"] == "inventory"


def test_worker_only_checkpoints_after_export_and_retries_idempotently(tmp_path):
    from scripts import run_pipeline_worker as worker
    path = tmp_path / "checkpoint.json"
    state = {"completed_objects": ["old"]}
    with mock.patch.object(worker, "build_minio_client"), \
         mock.patch.object(worker, "list_parquet_files", return_value=["new", "old"]), \
         mock.patch.object(worker, "run_etl_pipeline", return_value={"status": "SUCCESS"}) as etl, \
         mock.patch.object(worker, "export_powerbi_data", side_effect=RuntimeError("export failed")) as export:
        with pytest.raises(RuntimeError):
            worker.run_cycle(state, path)
        assert not path.exists() and state == {"completed_objects": ["old"]}
        export.side_effect = None
        updated = worker.run_cycle(state, path)
        assert updated["completed_objects"] == ["new", "old"] and path.exists()
        assert etl.call_count == 2
        worker.run_cycle(updated, path)
        assert etl.call_count == 2 and export.call_count == 3
        assert export.call_args.kwargs == {"install_views": False}


def test_failed_csv_write_preserves_previous_dataset(tmp_path):
    from powerbi.export_powerbi_dataset import _write_csv
    path = tmp_path / "data.csv"
    path.write_text("existing", encoding="utf-8")
    def bad_rows():
        yield {"a": 1}
        raise RuntimeError("interrupted")
    with pytest.raises(RuntimeError):
        _write_csv(str(path), ["a"], bad_rows())
    assert path.read_text(encoding="utf-8") == "existing"
    assert list(tmp_path.iterdir()) == [path]


@pytest.mark.skipif(os.getenv("RUN_POWERBI_SQL_TESTS") != "1", reason="Opt-in PostgreSQL integration")
def test_postgres_rejects_stale_order_and_inventory_and_extends_calendar():
    # All schemas, sequences and rows below are session-local and rolled back.
    engine = get_engine()
    with engine.connect() as conn:
        transaction = conn.begin()
        try:
            for table in ["Fact_Orders", "Fact_Inventory_Daily", "Dim_Dates", "Dim_Products"]:
                conn.exec_driver_sql(f"CREATE TEMP TABLE {table} ON COMMIT DROP AS SELECT * FROM public.{table} WITH NO DATA")
            for table, column in [("Dim_Dates", "date_key"), ("Dim_Products", "product_key")]:
                conn.exec_driver_sql(f"ALTER TABLE {table} ALTER COLUMN {column} SET NOT NULL")
                conn.exec_driver_sql(f"ALTER TABLE {table} ALTER COLUMN {column} ADD GENERATED BY DEFAULT AS IDENTITY")
            for table, columns in [("Dim_Dates", "full_date"), ("Dim_Products", "sku"),
                                    ("Fact_Orders", "order_id, platform, source_line_id"),
                                    ("Fact_Inventory_Daily", "product_key, date_key")]:
                conn.exec_driver_sql(f"ALTER TABLE {table} ADD UNIQUE ({columns})")
            conn.exec_driver_sql("INSERT INTO Dim_Products(sku) VALUES ('A')")
            ensure_dim_dates(conn, [date(2032, 1, 1)])
            day_key = conn.execute(text("SELECT date_key FROM Dim_Dates WHERE full_date='2032-01-01'")).scalar_one()
            proxy = mock.MagicMock()
            proxy.begin.return_value.__enter__.return_value = conn
            for status, version, qty in [("COMPLETED", "2032-01-01T02:00:00Z", 3), ("READY_TO_SHIP", "2032-01-01T01:00:00Z", 1)]:
                frame = clean_data(unify_schema(pd.DataFrame([event(create_time="2032-01-01T00:00:00Z", update_time=version,
                    order_status=status, quantity=qty)])), preserve_invalid=True)
                written = load_fact_orders(proxy, frame, {"A": 1}, {}, {}, {date(2032, 1, 1): day_key}, {}, {})
                assert written == (1 if status == "COMPLETED" else 0)
            assert conn.execute(text("SELECT order_status,quantity FROM Fact_Orders")).one() == ("COMPLETED", 3)
            for stock, ts in [(50, "2032-01-01T02:00:00Z"), (10, "2032-01-01T01:00:00Z")]:
                written = upsert_inventory(proxy, prepare_snapshots([dict(sku="A", stock_on_hand=stock, loaded_at=ts)]))
                assert written == (1 if stock == 50 else 0)
            assert conn.execute(text("SELECT stock_on_hand FROM Fact_Inventory_Daily")).scalar_one() == 50
            assert conn.execute(text("SELECT COUNT(*) FROM Dim_Dates")).scalar_one() == 1
        finally:
            transaction.rollback()
    engine.dispose()
