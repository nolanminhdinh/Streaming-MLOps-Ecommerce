"""Offline, auditable baseline comparison without changing deployment artifacts.

Reads the existing daily/feature snapshots, adds lag_6 IN MEMORY, compares the
legacy and corrected baselines on identical folds, selects on development folds,
and evaluates the selected baseline on a final 14-day holdout. Coverage and source
licensing still require external confirmation; this is not production approval.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import platform
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import numpy as np
import pandas as pd

from ml.features.feature_pipeline import get_feature_columns
from ml.features.time_series_features import TimeSeriesFeatureExtractor
from ml.training.metrics import EVALUATION_PROTOCOL, aggregate_fold_metrics, calculate_all_metrics
from ml.training.recursive_evaluation import forecast_fold_recursively
from serving.app.baseline_models import MovingAverageModel, NaiveModel, SeasonalNaiveModel


class ZeroDiagnostic:
    def predict(self, frame):
        return np.zeros(len(frame))


def sha256(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def evaluate(frame, daily, columns, models, splits):
    output = {}
    reference_keys = None
    for name, model in models.items():
        folds, actuals, predictions, keys, horizons = [], [], [], [], {}
        for _, _, test in splits:
            scored, y, pred = forecast_fold_recursively(
                model, frame, test, columns, "target_t_plus_1", daily_history=daily, history_days=240)
            fold_keys = [(str(s), str(d)) for s, d in zip(scored["sku"], scored["_target_date"])]
            keys.extend(fold_keys)
            actuals.extend(y.tolist()); predictions.extend(pred.tolist())
            fold = {"test_start": str(test.target_date.min()), "test_end": str(test.target_date.max()),
                    "expected_rows": len(test), "scored_rows": len(scored),
                    "expected_skus": int(test.sku.nunique()), "scored_skus": int(scored.sku.nunique()),
                    "metrics": calculate_all_metrics(y, pred)}
            folds.append(fold)
            for target, a, b in zip(scored["_target_date"], y, pred):
                h = (target - test.target_date.min()).days + 1
                aa, bb = horizons.setdefault(h, ([], [])); aa.append(float(a)); bb.append(float(b))
        if reference_keys is None:
            reference_keys = keys
        if keys != reference_keys:
            raise ValueError("Models were not scored on identical SKU/date pairs.")
        metrics = aggregate_fold_metrics([f["metrics"] for f in folds], actuals, predictions)
        expected = sum(f["expected_rows"] for f in folds)
        output[name] = {"folds": folds, "pooled_metrics": metrics,
                        "expected_rows": expected, "scored_rows": len(actuals),
                        "scored_fraction": len(actuals) / expected,
                        "same_scored_pairs": True,
                        "horizon_metrics": {str(h): calculate_all_metrics(a, b)
                                            for h, (a, b) in sorted(horizons.items())}}
    return output


def model_set():
    return {"naive_v2": NaiveModel(), "seasonal_naive_v2": SeasonalNaiveModel(),
            "moving_average_v2": MovingAverageModel(), "zero_diagnostic": ZeroDiagnostic()}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", default="docs/07-bao-cao-do-an/evidence/remediation-2026-11-07")
    args = parser.parse_args()
    out = ROOT / args.output_dir; out.mkdir(parents=True, exist_ok=True)
    started = time.perf_counter()
    sources = [ROOT / "data" / f for f in ["daily_history.parquet", "features_daily.parquet",
                                                   "feature_spec.json", "model_manifest.json"]]
    before = {p.relative_to(ROOT).as_posix(): sha256(p) for p in sources}
    daily = pd.read_parquet(sources[0]).copy()
    frame = pd.read_parquet(sources[1]).copy()
    for data in [daily, frame]:
        data["date"] = pd.to_datetime(data["date"]).dt.date
        data["sku"] = data.sku.astype(str)
    daily = daily.sort_values(["sku", "date"]).reset_index(drop=True)
    if daily.duplicated(["sku", "date"]).any():
        raise ValueError("Daily history must have one row per SKU/date.")
    daily["lag_6"] = daily.groupby("sku")["daily_demand"].shift(6)
    frame = frame.drop(columns=["lag_6"], errors="ignore").merge(
        daily[["sku", "date", "lag_6"]], on=["sku", "date"], how="left", validate="one_to_one")
    if frame.lag_6.isna().any():
        raise ValueError("Cannot reconstruct lag_6 for every scored feature row.")
    frame["target_date"] = pd.to_datetime(frame.date).dt.date + pd.Timedelta(days=1)
    columns = get_feature_columns(frame)
    # Historical before/after on exactly the original three test windows.
    legacy = {"naive_legacy": NaiveModel(alignment_version=1),
              "seasonal_naive_legacy": SeasonalNaiveModel(alignment_version=1),
              "moving_average_legacy": MovingAverageModel(alignment_version=1)}
    full_splits = list(TimeSeriesFeatureExtractor.walk_forward_split(frame, date_col="target_date"))
    same_folds = evaluate(frame, daily, columns, {**legacy, **model_set()}, full_splits)

    dates = sorted(frame.target_date.unique())
    holdout_start = dates[-14]
    development = frame[frame.target_date < holdout_start].copy()
    holdout = frame[frame.target_date >= holdout_start].copy()
    dev_splits = list(TimeSeriesFeatureExtractor.walk_forward_split(development, date_col="target_date"))
    dev = evaluate(development, daily, columns, model_set(), dev_splits)
    eligible = {name: row for name, row in dev.items() if name != "zero_diagnostic"
                and row["pooled_metrics"]["wape"] is not None}
    if not eligible:
        raise ValueError("No baseline with defined development WAPE is eligible.")
    selected = min(eligible, key=lambda n: eligible[n]["pooled_metrics"]["wape"])
    holdout_scores = evaluate(frame, daily, columns, model_set(), [(development, development.iloc[:0], holdout)])
    after = {p.relative_to(ROOT).as_posix(): sha256(p) for p in sources}
    if before != after:
        raise RuntimeError("Deployment artifacts changed during offline evaluation.")
    zero_fraction = float(daily.daily_demand.eq(0).mean())
    result = {"generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "evaluation_protocol": EVALUATION_PROTOCOL,
        "mode": "offline_no_training_no_registration_no_reload",
        "environment": {"python": platform.python_version(), "pandas": pd.__version__,
                        "numpy": np.__version__, "platform": platform.platform(),
                        "logical_cpu_count": os.cpu_count(), "omp_num_threads": os.getenv("OMP_NUM_THREADS")},
        "source_sha256": before, "deployment_artifacts_unchanged": before == after,
        "data": {"source_origin": json.loads(sources[2].read_text(encoding="utf-8")).get("data_origin"),
                 "daily_rows": len(daily), "feature_rows": len(frame), "skus": int(daily.sku.nunique()),
                 "history_start": str(daily.date.min()), "history_end": str(daily.date.max()),
                 "daily_zero_fraction": zero_fraction,
                 "coverage_status": "unverified_offline_not_inferred_from_zero_fill",
                 "old_feature_count": len(columns) - 1, "evaluation_feature_count": len(columns),
                 "lag_6_added_in_memory_only": True},
        "zero_actual_probe": calculate_all_metrics([0, 0], [4, 4]),
        "same_original_folds_before_after": same_folds,
        "development": dev,
        "holdout": {"start": str(holdout_start), "end": str(holdout.target_date.max()),
                    "selected_using_development_only": selected, "scores": holdout_scores,
                    "selection_policy": "minimum pooled WAPE among three corrected baselines; zero is diagnostic only",
                    "used_for_tuning_or_reselection": False},
        "limitations": ["Daily completeness and source-license documents are not verified by this computation.",
                        "Metrics condition on SKU/history eligibility; excluded rows are reported.",
                        "No statistical significance or operational inventory benefit is established.",
                        "Original-fold comparison is diagnostic; only development-selected holdout is final evaluation."],
        "elapsed_seconds": round(time.perf_counter() - started, 3)}
    (out / "baseline-evaluation.json").write_text(json.dumps(result, ensure_ascii=False, indent=2, allow_nan=False), encoding="utf-8")
    rows = []
    for scope, scores in [("original_folds", same_folds), ("development", dev), ("holdout", holdout_scores)]:
        for name, row in scores.items():
            rows.append({"scope": scope, "model": name, **row["pooled_metrics"],
                         "expected_rows": row["expected_rows"], "scored_fraction": row["scored_fraction"]})
    pd.DataFrame(rows).to_csv(out / "baseline-comparison.csv", index=False, encoding="utf-8-sig")
    eda = {"daily_demand_description": daily.daily_demand.describe().to_dict(),
           "daily_demand_median": float(daily.daily_demand.median()),
           "zero_fraction": zero_fraction,
           "missing_by_column": daily.isna().sum().astype(int).to_dict(),
           "duplicate_sku_date_keys": int(daily.duplicated(["sku", "date"]).sum()),
           "note": "Missing lag_6 in first six days is burn-in; no raw cleaning or coverage certification performed."}
    (out / "historical-eda.json").write_text(json.dumps(eda, ensure_ascii=False, indent=2, allow_nan=False), encoding="utf-8")
    try:
        from scripts.render_remediation_charts import render_charts
    except ModuleNotFoundError:
        print("Optional ReportLab charts unavailable; JSON/CSV evaluation completed.")
    else:
        render_charts(out, result, daily)
    print(json.dumps({"output": str(out), "selected": selected,
        "holdout_wape": holdout_scores[selected]["pooled_metrics"]["wape"],
        "original_fold_wape": {n: r["pooled_metrics"]["wape"] for n, r in same_folds.items()},
        "artifacts_unchanged": True, "elapsed_seconds": result["elapsed_seconds"]}, ensure_ascii=False))


if __name__ == "__main__":
    main()
