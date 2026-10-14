import io
import json
from unittest.mock import Mock
from unittest.mock import patch

import pandas as pd
import pytest
from datetime import date, timedelta

from warehouse.etl.transform import clean_data, unify_schema, deduplicate_lines, generate_quality_report
from warehouse.etl.data_validation import DataValidator
from warehouse.etl.quarantine import persist_quarantine


def event(**updates):
    row = {"_platform": "shopee", "order_sn": "order", "item_sku": "A",
           "quantity": 2, "original_price": 10, "total_order_value": 20,
           "buyer_total_amount": 20, "create_time": "2026-10-13T17:01:00Z",
           "order_status": "COMPLETED"}
    row.update(updates)
    return row


def validated(events):
    normalized = clean_data(unify_schema(pd.DataFrame(events)), preserve_invalid=True)
    valid, rejected, report = DataValidator().validate(normalized)
    valid, duplicates = deduplicate_lines(valid)
    return valid, rejected, duplicates


def test_multi_sku_and_same_sku_explicit_line_ids_survive_replay():
    inputs = [event(), event(item_sku="B"), event(order_line_id="first"), event(order_line_id="second")]
    valid, rejected, duplicates = validated(inputs + inputs)
    assert len(valid) == 4
    assert duplicates == 4
    assert rejected.empty
    assert set(valid.source_line_id) == {"sku:A", "sku:B", "id:first", "id:second"}
    assert set(valid.order_date.astype(str)) == {"2026-10-14"}


def test_invalid_duplicate_identity_cannot_hide_rejections():
    valid, rejected, duplicates = validated([event(), event(quantity="bad"), event(item_sku=None),
                                           event(quantity=0), event(quantity=1.5)])
    assert len(valid) == 1
    assert len(rejected) == 4
    assert duplicates == 0
    assert rejected.rejection_reasons.str.contains("QUANTITY").sum() == 3


def test_unknown_platform_and_invalid_money_are_auditable():
    valid, rejected, _ = validated([event(_platform="unsupported"), event(original_price="oops")])
    assert valid.empty
    assert len(rejected) == 2
    assert rejected.raw_payload.map(bool).all()


def test_quarantine_payload_and_storage_failure():
    _, rejected, _ = validated([event(item_sku=None, _source_object="raw/orders/test.parquet", _source_row=3)])
    stored = {}
    client = Mock()
    def put(**kwargs):
        stored.update(kwargs)
        stored["bytes"] = kwargs["data"].read()
    client.put_object.side_effect = put
    assert persist_quarantine(client, rejected, "test-run").endswith("rejected.jsonl")
    row = json.loads(stored["bytes"])
    assert row["raw_payload"]["item_sku"] is None
    assert row["source_row"] == 3
    assert row["rejection_reasons"].find("MISSING_SKU") >= 0
    client.put_object.side_effect = RuntimeError("storage unavailable")
    with pytest.raises(RuntimeError, match="storage unavailable"):
        persist_quarantine(client, rejected, "test-run")


def test_quality_report_does_not_count_invalid_rows_as_duplicates():
    raw = unify_schema(pd.DataFrame([event(), event(item_sku=None)]))
    cleaned = clean_data(raw)
    report = generate_quality_report(raw, cleaned)
    assert report["removed_total"] == 1
    assert report["duplicates_removed"] == 0


def test_baseline_fast_path_matches_recursive_dataframe_predictions():
    from serving.app.baseline_models import MovingAverageModel
    from serving.app.features import build_feature_row, to_model_input
    model = MovingAverageModel()
    history = [{"daily_demand": float(index % 11)} for index in range(35)]
    columns = ["lag_1", "rolling_mean_7", "ema_7"]
    for step in range(90):
        row = build_feature_row(history, date(2026, 7, 1) + timedelta(days=step))
        expected = model.predict(pd.DataFrame([to_model_input(row, columns)], columns=columns))[0]
        actual = model.predict_from_history(history, columns)
        assert actual == pytest.approx(expected)
        history.append({"daily_demand": actual})
    assert model.predict_from_history(history, ["lag_1"]) == 0


def test_features_count_orders_not_lines_for_repeated_sku():
    from ml.features.time_series_features import TimeSeriesFeatureExtractor
    inputs = [event(order_line_id="first"), event(order_line_id="second", quantity=3)]
    valid, _, _ = validated(inputs)
    daily = TimeSeriesFeatureExtractor().aggregate_daily(valid)
    assert daily.iloc[0].daily_demand == 5
    assert daily.iloc[0].order_count == 1


def test_numeric_line_id_is_stable_across_nullable_parquet_batches():
    from warehouse.etl.transform import source_line_key
    assert source_line_key(event(order_line_id=17)) == source_line_key(event(order_line_id=17.0)) == "id:17"
    assert source_line_key(event(order_line_id="0017")) == "id:0017"


def test_pipeline_cannot_load_when_quarantine_storage_fails():
    from warehouse.etl import pipeline
    raw = pd.DataFrame([event(), event(item_sku=None)])
    with patch.object(pipeline, "build_minio_client"), patch.object(pipeline, "extract_orders", return_value=raw), \
         patch.object(pipeline, "persist_quarantine", side_effect=RuntimeError("audit write failed")), \
         patch.object(pipeline, "load") as load:
        with pytest.raises(RuntimeError, match="audit write failed"):
            pipeline.run_etl_pipeline()
        load.assert_not_called()
