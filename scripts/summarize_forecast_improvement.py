"""Summarize saved experiments without refitting or changing evaluation labels."""
from pathlib import Path
import hashlib
import json
import sys
import xml.etree.ElementTree as ET

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
import numpy as np
import pandas as pd

OUT = ROOT / "docs/07-bao-cao-do-an/evidence/forecast-improvement-2026-11-08"


def main():
    evaluation = json.loads((OUT / "model-evaluation.json").read_text(encoding="utf-8"))
    calibration = json.loads((OUT / "calibration-evaluation.json").read_text(encoding="utf-8"))
    audit = json.loads((OUT / "data-quality.json").read_text(encoding="utf-8"))
    errors = pd.read_csv(OUT / "errors-by-sku.csv")
    grouped = errors.groupby(["model", "sku"])[["actual_units", "absolute_error", "zero_day_error"]].sum()
    baseline = grouped.loc["moving_average7"]
    hurdle = grouped.loc["hurdle_direct"].reindex(baseline.index)
    rng = np.random.default_rng(42)
    diff = []
    for _ in range(1000):
        sample = rng.integers(0, len(baseline), len(baseline))
        denominator = baseline.actual_units.to_numpy()[sample].sum()
        if denominator:
            diff.append(100 * (baseline.absolute_error.to_numpy()[sample].sum()
                               - hurdle.absolute_error.to_numpy()[sample].sum()) / denominator)
    suite = ET.parse(ROOT / "docs/07-bao-cao-do-an/evidence/pytest_forecast_improvement_2026-11-08.xml").getroot().find("testsuite")
    tests = {k: int(suite.get(k, "0")) for k in ["tests", "failures", "errors", "skipped"]}
    tests["passed"] = tests["tests"] - tests["failures"] - tests["errors"] - tests["skipped"]
    decomposition = {}
    for name in ["moving_average7", "hurdle_direct", "histgb_poisson_direct"]:
        rows = [w["scores"][name] for w in evaluation["outer_windows"]]
        decomposition[name] = {k: sum(r[k] for r in rows) for k in [
            "error_on_zero_actual", "error_on_positive_actual", "absolute_error_sum",
            "positive_actual_cells", "predicted_positive_cells"]}
    top = baseline.sort_values("absolute_error", ascending=False).head(10)
    summary = {
        "evaluation_type": "retrospective; final window previously exposed; no independent new test",
        "test_result": tests,
        "error_decomposition": decomposition,
        "baseline_top10_sku_error_share": float(top.absolute_error.sum() / baseline.absolute_error.sum()),
        "baseline_top10_sku_errors": top.reset_index().to_dict("records"),
        "hurdle_paired_sku_bootstrap_wape_reduction_95pct_points": np.quantile(diff, [.025, .975]).tolist(),
        "bootstrap_limit": "exploratory fixed-candidate paired SKU resampling; not time-series or source-coverage uncertainty",
        "candidate_decisions": {
            "hurdle_direct": {"decision": "experimental_daily_point_forecast_only", "inventory_approved": False,
                "reason": "retrospective WAPE improved but total volume underpredicted 65.86%; nested selection WAPE 102.15%"},
            "histgb_l1_direct": {"decision": "rejected_all_zero", "inventory_approved": False},
            "calibrated_mean_candidate": {"decision": "rejected", "inventory_approved": False,
                "reason": "pooled WAPE 231.46% and lead14 RMSE 9.28 versus MA7 121.15% and 3.99"},
            "production_change": {"registry_updated": False, "serving_reloaded": False,
                                  "data_manifest_feature_spec_unchanged": True}},
    }
    daily = pd.read_parquet(ROOT / "data/daily_history.parquet")
    daily["date"] = pd.to_datetime(daily["date"])
    all_units = float(daily.loc[daily.date.between("2026-07-01", "2026-08-11"), "daily_demand"].sum())
    scored_units = evaluation["pooled_outer"]["moving_average7"]["actual_sum"]
    summary["evaluation_scope"] = {"all_sku_units_in_outer_dates": all_units,
        "known_sku_scored_units": scored_units, "cold_start_units_outside_comparison": all_units-scored_units,
        "cold_start_evaluated": False, "scored_sku_day_pairs": 8484}
    hashes = evaluation["protocol"]["source_sha256"]
    summary["deployment_hashes_rechecked"] = {p: hashlib.sha256((ROOT/p).read_bytes()).hexdigest() == sha
                                              for p, sha in hashes.items()}
    assert all(summary["deployment_hashes_rechecked"].values())
    (OUT / "candidate-decisions.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    model_dir = ROOT / "output/models/forecast-improvement-2026-11-08"
    card_path = model_dir / "model-card.json"
    card = json.loads(card_path.read_text(encoding="utf-8"))
    card["acceptance"] = summary["candidate_decisions"]["hurdle_direct"]
    card["runtime_compatibility"] = {"trained_sklearn": evaluation["environment"]["sklearn"],
        "serving_requirements_sklearn": "1.5.1", "container_load_validated": False,
        "action_required_before_deployment": "refit in the pinned serving environment or rebuild and validate the image; never assume cross-version pickle compatibility"}
    card_path.write_text(json.dumps(card, ensure_ascii=False, indent=2), encoding="utf-8")
    (model_dir / "calibrated-model-card.json").write_text(json.dumps({
        "artifact": calibration["model_artifact"], "artifact_sha256": calibration["model_sha256"],
        **summary["candidate_decisions"]["calibrated_mean_candidate"],
        "registry_updated": False, "serving_reloaded": False,
        "runtime_compatibility": card["runtime_compatibility"]}, ensure_ascii=False, indent=2), encoding="utf-8")

    # Standalone scientific figures; English labels avoid font substitution in SVG converters.
    svg = ['<svg xmlns="http://www.w3.org/2000/svg" width="1100" height="590" viewBox="0 0 1100 590">',
           '<rect width="1100" height="590" fill="white"/>',
           '<g font-family="Arial" fill="#172b4d">',
           '<text x="30" y="36" font-size="23" font-weight="bold">Retrospective daily WAPE and total volume bias</text>',
           '<text x="30" y="65" font-size="17">Three rolling windows, 1 Jul to 11 Aug 2026; 1,069 actual units</text>']
    chart_rows = [("MA7 corrected", "moving_average7"), ("Zero diagnostic", "zero_diagnostic"),
                  ("Median28", "median28"), ("Hurdle direct", "hurdle_direct"),
                  ("Nested selection", "nested_selected_daily"), ("Poisson direct", "histgb_poisson_direct"),
                  ("Calibrated Poisson", None)]
    for i, (label, name) in enumerate(chart_rows):
        m = evaluation["pooled_outer"][name] if name else calibration["pooled"]
        y = 110 + i*57
        color = "#1d7682" if name == "hurdle_direct" else "#425c7e" if name else "#b05b50"
        svg += [f'<text x="30" y="{y+22}" font-size="18">{label}</text>',
                f'<rect x="255" y="{y}" width="{m["wape"]*2.1:.2f}" height="32" fill="{color}"/>',
                f'<text x="{267+m["wape"]*2.1:.2f}" y="{y+23}" font-size="18">{m["wape"]:.2f}%</text>',
                f'<text x="910" y="{y+23}" font-size="18">{m["bias"]:+.2f}%</text>']
    svg += ['<text x="910" y="94" font-size="17" font-weight="bold">Volume bias</text>',
            '<text x="30" y="550" font-size="16">Lower WAPE alone does not establish inventory suitability. Negative bias means underforecast.</text>',
            '<text x="30" y="575" font-size="16">Final window was previously inspected. Zero is a diagnostic reference, not a replenishment policy.</text>', '</g></svg>']
    (OUT / "wape-bias.svg").write_text("\n".join(svg), encoding="utf-8")
    months = sorted({r["month"] for r in audit["original_csv"]["monthly_non_cancelled_units"]})
    source = {(r["month"], r["platform"]): r["quantity"] for r in audit["original_csv"]["monthly_non_cancelled_units"]}
    svg = ['<svg xmlns="http://www.w3.org/2000/svg" width="1100" height="520" viewBox="0 0 1100 520">',
           '<rect width="1100" height="520" fill="white"/>', '<g font-family="Arial" fill="#172b4d">',
           '<text x="30" y="36" font-size="23" font-weight="bold">Observed non-cancelled units by source and month</text>',
           '<rect x="700" y="55" width="18" height="18" fill="#425c7e"/><text x="726" y="71" font-size="17">Shopee</text>',
           '<rect x="850" y="55" width="18" height="18" fill="#1d7682"/><text x="876" y="71" font-size="17">TikTok</text>']
    for tick in [0, 500, 1000, 1500, 2000]:
        yy = 405 - tick*.145
        svg += [f'<line x1="80" x2="1065" y1="{yy}" y2="{yy}" stroke="#dddddd"/>',
                f'<text x="20" y="{yy+6}" font-size="16">{tick}</text>']
    for i, month in enumerate(months):
        x = 108+i*106; a=source.get((month,"shopee"),0); b=source.get((month,"tiktok"),0)
        svg += [f'<rect x="{x}" y="{405-a*.145}" width="58" height="{a*.145}" fill="#425c7e"/>',
                f'<rect x="{x}" y="{405-(a+b)*.145}" width="58" height="{b*.145}" fill="#1d7682"/>',
                f'<text x="{x+29}" y="{393-(a+b)*.145}" text-anchor="middle" font-size="17">{a+b}</text>',
                f'<text x="{x+29}" y="438" text-anchor="middle" font-size="16">{month[5:]}/{month[2:4]}</text>']
    svg += ['<text x="30" y="483" font-size="16">Dec 2025 and Aug 2026 are partial months. File completeness does not certify daily collection coverage.</text>',
            '<text x="30" y="508" font-size="16">Original anonymized CSV fingerprints match the deployment manifest; no evaluation labels removed.</text>', '</g></svg>']
    (OUT / "source-monthly.svg").write_text("\n".join(svg), encoding="utf-8")
    print(json.dumps({"tests": tests, "decomposition": decomposition,
                      "hurdle_bootstrap": summary["hurdle_paired_sku_bootstrap_wape_reduction_95pct_points"],
                      "baseline_top10_error_share": summary["baseline_top10_sku_error_share"]}, ensure_ascii=False))


if __name__ == "__main__":
    main()

