"""Read ML artifacts, evaluate existing baselines, and optionally inspect live sources.

Does not train, register, reload, or overwrite deployment artifacts.
Only writes the local aggregate evidence file requested by --audit-date.
"""
import argparse
from datetime import date
import json
import logging
from pathlib import Path
import sys
from urllib.parse import urlencode, urlparse
from urllib.request import Request, urlopen

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import numpy as np
import pandas as pd
from sqlalchemy import text
from data_simulator.data_simulator import PRODUCT_CATALOG
from ml.features.feature_pipeline import get_feature_columns
from ml.features.time_series_features import TimeSeriesFeatureExtractor
from ml.training.data_readiness import assess_order_history, required_history_days
from ml.training.metrics import calculate_all_metrics
from ml.training.recursive_evaluation import forecast_fold_recursively
from serving.app.baseline_models import NaiveModel, SeasonalNaiveModel, MovingAverageModel
from serving.app.features import build_feature_row

ROOT = Path(__file__).resolve().parents[1]


def summarize(frame):
    return {"rows": len(frame), "columns": len(frame.columns), "skus": int(frame.sku.nunique()),
            "dates": int(frame.date.nunique()), "start": str(frame.date.min()), "end": str(frame.date.max())}


def boundary_checks():
    tiny = pd.DataFrame({"sku": ["A", "A"], "create_time": ["2026-01-01", "2026-04-20"],
                         "quantity": [1, 1], "is_cancelled": [False, False]})
    readiness = assess_order_history(tiny)
    assert not readiness["data_sufficient"] and readiness["active_source_days"] == 2
    price_input = pd.DataFrame({"sku": ["A", "B"], "create_time": ["2026-01-01", "2026-01-04"],
                               "quantity": [1, 1], "original_price": [10, 99],
                               "buyer_total_amount": [10, 99], "seller_discount": [0, 0]})
    daily = TimeSeriesFeatureExtractor().aggregate_daily(price_input)
    early_price = float(daily[(daily.sku == "B") & (daily.date == date(2026, 1, 1))].iloc[0].avg_unit_price)
    assert early_price == 0
    history = [{"daily_demand": float(i)} for i in range(1, 11)]
    row = build_feature_row(history, date(2026, 1, 10))
    baseline_values = {"latest_observed": 10, "naive_lag_1_prediction": row["lag_1"],
                       "seasonal_lag_7_prediction": row["lag_7"],
                       "moving_average_shifted_7": row["rolling_mean_7"]}
    assert row["lag_1"] == 9 and row["lag_7"] == 3
    return {"readiness_two_observed_dates": {key: readiness[key] for key in
                                           ["data_sufficient", "available_history_days", "active_source_days", "required_history_days"]},
            "price_backfill_before_first_sale": early_price,
            "baseline_alignment_for_forecast_jan11": baseline_values,
            "all_zero_actual_nonzero_prediction": calculate_all_metrics([0, 0], [4, 4])}


def evaluate(features, daily, columns):
    frame = features.copy()
    frame["target_date"] = pd.to_datetime(frame.date).dt.date + pd.Timedelta(days=1)
    output = {}
    for name, model in [("naive", NaiveModel()), ("seasonal_naive", SeasonalNaiveModel()),
                        ("moving_average", MovingAverageModel())]:
        folds, all_y, all_p = [], [], []
        horizon_values = {}
        for train, val, test in TimeSeriesFeatureExtractor.walk_forward_split(frame, date_col="target_date"):
            scored, y, p = forecast_fold_recursively(model, frame, test, columns,
                                                    "target_t_plus_1", daily_history=daily, history_days=240)
            start = min(test.target_date)
            for target, actual, predicted in zip(scored["_target_date"], y, p):
                h = (target - start).days + 1
                a, b = horizon_values.setdefault(h, ([], []))
                a.append(float(actual)); b.append(float(predicted))
            folds.append({"fit_target_end": str(max(pd.concat([train, val]).target_date)),
                          "test_target_start": str(start), "test_target_end": str(max(test.target_date)),
                          "test_rows": len(test), "scored_rows": len(scored),
                          "test_skus": int(test.sku.nunique()), "scored_skus": int(scored.sku.nunique()),
                          "metrics": calculate_all_metrics(y, p)})
            all_y.extend(y); all_p.extend(p)
        output[name] = {"folds": folds, "mean_fold_wape": round(float(np.mean([f["metrics"]["wape"] for f in folds])), 2),
                        "pooled_metrics": calculate_all_metrics(all_y, all_p),
                        "horizon_metrics": {str(h): calculate_all_metrics(a, b) for h, (a, b) in horizon_values.items()}}
    return output


def get_json(path, params=None):
    address = "http://localhost:5000/api/2.0/mlflow/" + path
    if params:
        address += "?" + urlencode(params)
    with urlopen(Request(address), timeout=20) as response:
        return json.load(response)


def runtime(spec, manifest, daily):
    from warehouse.etl.load import get_engine
    from warehouse.etl.extract import build_minio_client
    engine = get_engine()
    try:
        with engine.connect() as conn:
            conn.execute(text("SET TRANSACTION READ ONLY"))
            source = pd.read_sql(text("""SELECT sku, create_time, quantity, is_cancelled,
                source_dataset, 0::numeric AS original_price, 0::numeric AS buyer_total_amount,
                0::numeric AS seller_discount FROM Fact_Historical_Order_Lines
                WHERE data_origin='public_anonymized_historical_csv'"""), conn)
            coverage_count = conn.execute(text("SELECT count(*) FROM Warehouse_Daily_Coverage")).scalar_one()
    finally:
        engine.dispose()
    rebuilt = TimeSeriesFeatureExtractor().aggregate_daily(source)
    expected = rebuilt.set_index(["sku", "date"])["daily_demand"]
    actual = daily.set_index(["sku", "date"])["daily_demand"]
    common = expected.index.intersection(actual.index)
    db_info = {"rows": len(source), "all_skus": int(source.sku.nunique()),
               "noncancelled_skus": int(source.loc[~source.is_cancelled, "sku"].nunique()),
               "cancelled_only_skus": len(set(source.sku) - set(source.loc[~source.is_cancelled, "sku"])),
               "coverage_rows": int(coverage_count),
               "source_fingerprints_match_local_spec": sorted(source.source_dataset.unique()) == sorted(spec.get("source_datasets", [])),
               "rebuilt_daily": summarize(rebuilt), "matching_daily_keys": len(common),
               "keys_only_rebuilt": len(expected.index.difference(actual.index)),
               "keys_only_artifact": len(actual.index.difference(expected.index)),
               "demand_mismatches_on_common_keys": int((expected.loc[common] != actual.loc[common]).sum())}
    version = get_json("model-versions/get", {"name": manifest["model_registry_name"], "version": manifest["version"]})["model_version"]
    run = get_json("runs/get", {"run_id": version["run_id"]})["run"]
    params = {v["key"]: v["value"] for v in run["data"].get("params", [])}
    metrics = {v["key"]: v["value"] for v in run["data"].get("metrics", [])}
    files = get_json("artifacts/list", {"run_id": version["run_id"], "path": "model"})
    registry = {"version": version["version"], "stage": version["current_stage"], "status": version.get("status"),
                "run_id_matches_manifest": version["run_id"] == manifest["run_id"],
                "run_status": run["info"].get("status"),
                "parameters": {k: params.get(k) for k in ["model_name", "evaluation_strategy", "data_origin", "n_walk_forward_splits"]},
                "metrics": metrics, "artifacts": [item["path"] for item in files.get("files", [])]}
    parsed = urlparse(run["info"].get("artifact_uri", ""))
    if parsed.scheme == "s3":
        client = build_minio_client()
        response = client.get_object(parsed.netloc, parsed.path.lstrip("/").rstrip("/") + "/model/feature_spec.json")
        try:
            remote_spec = json.loads(response.read())
        finally:
            response.close(); response.release_conn()
        registry["run_feature_spec_matches_local"] = remote_spec == spec
        registry["feature_column_order_matches_local"] = remote_spec.get("feature_columns") == spec.get("feature_columns")
        registry["fingerprints_match_local"] = sorted(remote_spec.get("source_datasets", [])) == sorted(spec.get("source_datasets", []))
    return {"source_reconciliation": db_info, "registry": registry}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--audit-date", type=date.fromisoformat, required=True)
    parser.add_argument("--runtime", action="store_true")
    args = parser.parse_args()
    logging.disable(logging.CRITICAL)
    spec = json.loads((ROOT / "data/feature_spec.json").read_text(encoding="utf-8"))
    manifest = json.loads((ROOT / "data/model_manifest.json").read_text(encoding="utf-8"))
    features = pd.read_parquet(ROOT / "data/features_daily.parquet")
    daily = pd.read_parquet(ROOT / "data/daily_history.parquet")
    columns = spec["feature_columns"]
    labelled = features.copy()
    labelled["next_date"] = pd.to_datetime(labelled.date).dt.date + pd.Timedelta(days=1)
    joined = labelled.merge(daily[["sku", "date", "daily_demand"]], left_on=["sku", "next_date"],
                            right_on=["sku", "date"], suffixes=("", "_next"), how="left")
    result = {"audit_date": str(args.audit_date), "mode": "no_training_or_registry_mutations",
              "artifacts": {"daily": summarize(daily), "features": summarize(features),
                            "input_columns": len(columns), "feature_order_matches_contract": get_feature_columns(features) == columns,
                            "target_next_day_mismatches": int((joined.target_t_plus_1 != joined.daily_demand_next).sum()),
                            "daily_zero_fraction": float(daily.daily_demand.eq(0).mean()),
                            "segment_cutoff": spec.get("segment_cutoff_date"), "data_origin": spec.get("data_origin"),
                            "simulator_skus_in_history": len({p["sku"] for p in PRODUCT_CATALOG} & set(daily.sku)),
                            "manifest_has_source_fingerprints": bool(manifest.get("source_datasets"))},
              "minimum_history": required_history_days(), "boundary_checks": boundary_checks(),
              "baseline_reevaluation": evaluate(features, daily, columns)}
    if args.runtime:
        result["runtime_readonly"] = runtime(spec, manifest, daily)
    destination = ROOT / "docs/07-bao-cao-do-an/evidence" / f"ml_pipeline_{args.audit_date}.json"
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(json.dumps(result, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")
    compact = {k: v for k, v in result.items() if k != "baseline_reevaluation"}
    compact["baseline_reevaluation"] = {name: {"mean_fold_wape": entry["mean_fold_wape"],
                                              "pooled_metrics": entry["pooled_metrics"],
                                              "scored_rows": [f["scored_rows"] for f in entry["folds"]]}
                                         for name, entry in result["baseline_reevaluation"].items()}
    print(json.dumps(compact, ensure_ascii=False, indent=2, default=str))


if __name__ == "__main__":
    main()
