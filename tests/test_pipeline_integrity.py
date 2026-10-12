"""Failures and provenance must survive across pipeline boundaries."""

import json
from datetime import date
from types import SimpleNamespace
from unittest import mock

import pandas as pd
import pytest
from minio.error import S3Error

from warehouse.etl import extract as extraction
from monitoring.evidently import trigger_retraining as retraining
from serving.app.data_access import WarehouseRepository


def test_minio_error_aborts_extraction_instead_of_partial_success():
    client = mock.Mock()
    client.list_objects.return_value = [SimpleNamespace(object_name="good.parquet"), SimpleNamespace(object_name="bad.parquet")]
    failure = S3Error(None, "AccessDenied", "unreadable object", "bad.parquet", "request", "host", None)

    def read(client, bucket, path):
        if path == "bad.parquet":
            raise failure
        return pd.DataFrame({"id": [1]})

    with mock.patch.object(extraction, "read_parquet_from_minio", side_effect=read):
        with pytest.raises(S3Error):
            extraction.extract(client=client)


def test_minio_storage_error_is_propagated():
    client = mock.Mock()
    client.get_object.side_effect = S3Error(None, "AccessDenied", "unreadable", "object", "request", "host", None)
    with pytest.raises(S3Error):
        extraction.read_parquet_from_minio(client, "bucket", "object")


def test_minio_response_is_closed_when_read_fails():
    response = mock.Mock()
    response.read.side_effect = OSError("connection interrupted")
    with pytest.raises(OSError):
        extraction.read_parquet_from_minio(mock.Mock(get_object=mock.Mock(return_value=response)), "bucket", "object")
    response.close.assert_called_once()
    response.release_conn.assert_called_once()


def test_default_retraining_reads_deployment_csv_fingerprints():
    orders = pd.DataFrame()
    orders.attrs = {"data_source": "warehouse", "data_origin": "public_anonymized_historical_csv", "source_datasets": ["file-a"]}
    summary = pd.DataFrame([{ "Mô hình": "moving_average", "WAPE (%)": 12, "MAE": 1, "RMSE": 2 }])
    with mock.patch("ml.features.feature_pipeline.load_orders_data", return_value=orders) as load, \
         mock.patch("ml.training.data_readiness.require_sufficient_order_history"), \
         mock.patch("ml.training.train_baseline.run_training_pipeline", return_value=summary) as train:
        result = retraining.default_train_fn("public_anonymized_historical_csv", ["file-a"])
    load.assert_called_once_with(data_origin="public_anonymized_historical_csv", source_datasets=["file-a"])
    assert train.call_args.kwargs["orders_df"] is orders
    assert result["cv_wape"] == 12


def test_retraining_rejects_changed_origin_before_training():
    orders = pd.DataFrame()
    orders.attrs = {"data_source": "warehouse", "data_origin": "unverified_warehouse"}
    with mock.patch("ml.features.feature_pipeline.load_orders_data", return_value=orders), \
         mock.patch("ml.training.train_baseline.run_training_pipeline") as train:
        with pytest.raises(RuntimeError, match="provenance"):
            retraining.default_train_fn("public_anonymized_historical_csv", ["file-a"])
    train.assert_not_called()


def test_stale_drift_origin_does_not_train_or_change_manifest(tmp_path):
    original = {"version": "2", "data_origin": "public_anonymized_historical_csv", "source_datasets": ["file-a"]}
    manifest = tmp_path / "model_manifest.json"
    manifest.write_text(json.dumps(original))
    summary = tmp_path / "drift.json"
    summary.write_text(json.dumps({"drift_detected": True, "data_source": "feature_store", "feature_data_source": "warehouse", "data_origin": "unverified_warehouse", "source_datasets": []}))
    with mock.patch.object(retraining, "default_train_fn") as train:
        result = retraining.ClosedLoopRetrainer(summary_path=str(summary), manifest_path=str(manifest), retraining_log_path=str(tmp_path / "audit.json")).evaluate_and_trigger()
    assert result["status"] == "FAILED"
    train.assert_not_called()
    assert json.loads(manifest.read_text()) == original


def test_registration_cannot_change_deployment_fingerprints(tmp_path):
    original = {"version": "2", "data_origin": "public_anonymized_historical_csv", "source_datasets": ["file-a"]}
    manifest = tmp_path / "model_manifest.json"
    manifest.write_text(json.dumps(original))
    reload = mock.Mock(return_value=True)
    result = retraining.ClosedLoopRetrainer(
        summary_path=str(tmp_path / "none.json"), manifest_path=str(manifest), retraining_log_path=str(tmp_path / "audit.json"),
        train_fn=lambda: {}, register_fn=lambda _: {"version": "3", "run_id": "new-run", "data_origin": original["data_origin"], "source_datasets": ["file-b"]}, reload_fn=reload,
    ).evaluate_and_trigger(force_retrain=True)
    assert result["stage"] == "registration"
    assert json.loads(manifest.read_text()) == original
    reload.assert_not_called()


def test_forecast_audit_persists_source_and_canonical_fingerprints():
    repo = WarehouseRepository(enabled=True)
    connection = mock.Mock()
    connection.execute.return_value.rowcount = 1
    engine = mock.MagicMock()
    engine.begin.return_value.__enter__.return_value = connection
    with mock.patch.object(repo, "available", return_value=True), mock.patch.object(repo, "_get_engine", return_value=engine):
        count = repo.record_served_forecasts("A", [{"date": "2026-07-02", "predicted_quantity": 3}], "model", "2", "mlflow_registry", "warehouse", "2026-07-01", data_origin="public_anonymized_historical_csv", source_datasets=["file-b", "file-a", "file-a"])
    assert count == 1
    rows = connection.execute.call_args.args[1]
    assert rows[0]["data_origin"] == "public_anonymized_historical_csv"
    assert json.loads(rows[0]["source_datasets"]) == ["file-a", "file-b"]


def test_forecast_without_source_is_not_written():
    repo = WarehouseRepository(enabled=True)
    with mock.patch.object(repo, "_get_engine") as engine:
        assert repo.record_served_forecasts("A", [{"date": "2026-07-02", "predicted_quantity": 3}], "model", "2", "mlflow_registry", "warehouse", "2026-07-01") == 0
    engine.assert_not_called()


def test_drift_summary_carries_feature_store_provenance(tmp_path):
    from monitoring.evidently.drift_detector import load_feature_store_windows
    path = tmp_path / "features_daily.parquet"
    pd.DataFrame({"date": pd.date_range("2026-07-01", periods=40), "daily_demand": range(40)}).to_parquet(path)
    (tmp_path / "feature_spec.json").write_text(json.dumps({"data_source": "warehouse", "data_origin": "public_anonymized_historical_csv", "source_datasets": ["file-a"]}))
    _, _, meta = load_feature_store_windows(str(path))
    assert meta["data_origin"] == "public_anonymized_historical_csv"
    assert meta["source_datasets"] == ["file-a"]
    assert meta["feature_data_source"] == "warehouse"


def test_coverage_refuses_unfinished_vietnam_business_day():
    from datetime import datetime
    from zoneinfo import ZoneInfo
    from scripts import confirm_daily_coverage as coverage
    with mock.patch.object(coverage, "get_engine") as engine:
        with pytest.raises(ValueError, match="completed Vietnam"):
            coverage.confirm_daily_coverage("unverified_warehouse", datetime.now(ZoneInfo("Asia/Ho_Chi_Minh")).date())
    engine.assert_not_called()


def test_coverage_refuses_missing_historical_dataset():
    from scripts import confirm_daily_coverage as coverage
    connection = mock.Mock()
    connection.execute.return_value.scalar.return_value = None
    engine = mock.MagicMock()
    engine.begin.return_value.__enter__.return_value = connection
    with mock.patch.object(coverage, "get_engine", return_value=engine):
        with pytest.raises(ValueError, match="does not exist"):
            coverage.confirm_daily_coverage("public_anonymized_historical_csv", date(2026, 7, 2), "missing-file")
    assert connection.execute.call_count == 1
    engine.dispose.assert_called_once()


def test_empty_warehouse_does_not_publish_powerbi_csvs(tmp_path):
    from powerbi.export_powerbi_dataset import export_powerbi_data
    with mock.patch("sqlalchemy.create_engine"), \
         mock.patch("powerbi.export_powerbi_dataset._install_views"), \
         mock.patch("powerbi.export_powerbi_dataset._query", return_value=[]):
        with pytest.raises(RuntimeError, match="chưa có đơn hàng"):
            export_powerbi_data(str(tmp_path))
    assert list(tmp_path.glob("*.csv")) == []
