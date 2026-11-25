"""Read-only audit of the forecast snapshot and optional warehouse source lines."""
from pathlib import Path
import argparse
import hashlib
import json
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
import numpy as np
import pandas as pd


def audit_snapshot(daily, features):
    d = daily.copy(); d["date"] = pd.to_datetime(d.date)
    y = pd.to_numeric(d.daily_demand, errors="coerce")
    if not np.isfinite(y).all() or y.lt(0).any():
        raise ValueError("Invalid nonnegative finite target in daily snapshot.")
    d["month"] = d.date.dt.to_period("M").astype(str)
    profiles = []
    prelaunch_zeros = 0
    for sku, rows in d.sort_values("date").groupby("sku"):
        positive = rows[rows.daily_demand > 0]
        first, last = positive.date.min(), positive.date.max()
        prelaunch_zeros += int(rows.date.lt(first).sum())
        active = rows[rows.date.ge(first)]
        values = positive.daily_demand
        profiles.append({"sku": str(sku), "positive_days": len(positive), "units": float(values.sum()),
                         "first_positive": str(first.date()), "last_positive": str(last.date()),
                         "age_after_first_positive": len(active),
                         "positive_rate_after_first": len(positive) / len(active),
                         "adi_after_first": len(active) / len(positive),
                         "positive_size_cv2": float(values.var(ddof=0) / values.mean() ** 2),
                         "max_daily": float(values.max())})
    p = pd.DataFrame(profiles)
    labels = features[["sku", "date", "target_t_plus_1"]].copy()
    labels["target_date"] = pd.to_datetime(labels.date) + pd.Timedelta(days=1)
    joined = labels.merge(d[["sku", "date", "daily_demand"]], left_on=["sku", "target_date"],
                          right_on=["sku", "date"], how="left", validate="one_to_one")
    months = d.groupby("month").agg(units=("daily_demand", "sum"),
                    positive_cells=("daily_demand", lambda x: int(x.gt(0).sum()))).reset_index()
    positive = y[y > 0]
    total = float(y.sum())
    result = {"daily_rows": len(d), "skus": d.sku.nunique(), "days": d.date.nunique(),
              "target_finite_nonnegative": True, "duplicate_sku_date": int(d.duplicated(["sku", "date"]).sum()),
              "target_t_plus_1_mismatches": int((joined.target_t_plus_1 != joined.daily_demand).sum()),
              "missing_target_dates": int(joined.daily_demand.isna().sum()),
              "zero_fraction": float(y.eq(0).mean()), "pre_first_sale_zero_cells": prelaunch_zeros,
              "skus_with_at_most_3_positive_days": int(p.positive_days.le(3).sum()),
              "median_positive_days_per_sku": float(p.positive_days.median()),
              "positive_quantity_quantiles": {str(k): float(v) for k, v in positive.quantile([.5,.9,.95,.99,1]).items()},
              "top_10_daily_cells_share_units": float(y.nlargest(10).sum() / total),
              "largest_daily_cell": float(y.max()),
              "monthly": months.to_dict(orient="records"),
              "missing_by_column": d.isna().sum().astype(int).to_dict(),
              "zero_fill_completeness": "unverified; zero is not inferred to mean a confirmed no-sale day",
              "outlier_policy": "preserve all actuals; no removal or clipping of evaluation labels"}
    return result, p


def audit_warehouse(daily):
    from sqlalchemy import create_engine, text
    from warehouse.etl.load import DATABASE_URL
    engine = create_engine(DATABASE_URL, connect_args={"connect_timeout": 5,
                            "options": "-c default_transaction_read_only=on"})
    try:
        with engine.connect() as conn:
            orders = pd.read_sql(text("""SELECT source_dataset, source_record_id, source_order_id,
                platform, sku, create_time, quantity, is_cancelled, order_status
                FROM Fact_Historical_Order_Lines
                WHERE data_origin = 'public_anonymized_historical_csv'"""), conn)
            coverage = conn.execute(text("SELECT COUNT(*) FROM Warehouse_Daily_Coverage WHERE data_origin='public_anonymized_historical_csv'" )).scalar_one()
            ambiguous = conn.execute(text("""SELECT COUNT(*) FROM (SELECT sku FROM Fact_Historical_Order_Lines
                WHERE data_origin='public_anonymized_historical_csv'
                GROUP BY sku HAVING COUNT(DISTINCT source_dataset)>1) s""")).scalar_one()
        if orders.empty: raise ValueError("No historical warehouse source lines.")
        o = orders.copy();o["date"] = pd.to_datetime(o.create_time).dt.date
        valid = o[~o.is_cancelled.astype(bool)]
        rebuilt = valid.groupby(["sku", "date"]).quantity.sum().rename("rebuilt").reset_index()
        d = daily[["sku", "date", "daily_demand"]].copy();d["date"]=pd.to_datetime(d.date).dt.date
        merged = d.merge(rebuilt, how="outer", on=["sku", "date"])
        mismatch = int((merged.daily_demand.fillna(0) != merged.rebuilt.fillna(0)).sum())
        by_source = o.groupby(["source_dataset", "platform"]).agg(rows=("quantity", "size"),
            cancelled=("is_cancelled", "sum"), min_date=("date", "min"), max_date=("date", "max"),
            distinct_days=("date", "nunique"), max_line_quantity=("quantity", "max")).reset_index()
        for c in ["min_date", "max_date"]: by_source[c] = by_source[c].astype(str)
        same_order = o[o.source_order_id.notna()].groupby(["source_dataset", "platform", "source_order_id"]).date.nunique()
        return {"status": "read_only_verified", "rows": len(o),
                "source_key_duplicates": int(o.duplicated(["source_dataset", "platform", "source_record_id"]).sum()),
                "invalid_quantity_rows": int((o.quantity.le(0) | o.quantity.gt(100)).sum()),
                "cancelled_rows": int(o.is_cancelled.sum()), "coverage_rows": int(coverage),
                "skus_shared_across_datasets": int(ambiguous),
                "source_order_ids_reused_across_dates": int(same_order.gt(1).sum()),
                "rebuilt_daily_value_mismatches": mismatch, "by_source": by_source.to_dict(orient="records"),
                "source_timezone": "stored timestamps lack source offset; original CSV needed to verify import assumption",
                "raw_csv_status": "not located in supplied workspace; no claim that original file rows were audited"}
    finally:
        engine.dispose()


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--warehouse",action="store_true")
    parser.add_argument("--shopee-csv",type=Path)
    parser.add_argument("--tiktok-csv",type=Path)
    args=parser.parse_args()
    out=ROOT/'docs/07-bao-cao-do-an/evidence/forecast-improvement-2026-11-08';out.mkdir(parents=True,exist_ok=True)
    daily=pd.read_parquet(ROOT/'data/daily_history.parquet');features=pd.read_parquet(ROOT/'data/features_daily.parquet')
    report,profiles=audit_snapshot(daily,features)
    if args.shopee_csv and args.tiktok_csv:
        from scripts.import_public_historical_orders import _normalize_file, _read_allowlisted, SHOPEE_FIELDS, TIKTOK_FIELDS
        lines=[];summaries=[]
        fingerprints=json.loads((ROOT/'data/model_manifest.json').read_text(encoding='utf-8')).get('source_datasets',[])
        for platform,path,allowed,time_col in [('shopee',args.shopee_csv,SHOPEE_FIELDS,'create_time'),
                                              ('tiktok',args.tiktok_csv,TIKTOK_FIELDS,'created_time')]:
            rows,summary=_normalize_file(path,platform);raw,columns=_read_allowlisted(path,allowed)
            summary['matches_deployment_fingerprint']=summary['source_dataset'] in fingerprints
            summary['identical_allowlisted_rows']=int(raw.duplicated().sum())
            summary['timestamps_with_explicit_offset']=int(raw[time_col].str.contains(r'(?:Z|[+-]\d{2}:?\d{2})$',regex=True).sum())
            summary['status_counts']=pd.Series([x['order_status'] for x in rows]).value_counts().astype(int).to_dict()
            summaries.append(summary);lines.extend(rows)
        orders=pd.DataFrame(lines);orders['date']=pd.to_datetime(orders.create_time).dt.date
        active=orders[~orders.is_cancelled]
        rebuilt=active.groupby(['sku','date']).quantity.sum().rename('rebuilt').reset_index()
        saved=daily[['sku','date','daily_demand']].copy();saved['date']=pd.to_datetime(saved.date).dt.date
        joined=saved.merge(rebuilt,how='outer',on=['sku','date'])
        shared=active.groupby('sku').source_dataset.nunique().gt(1)
        report['original_csv']={'status':'allowlist_read_only_verified','sources':summaries,
          'rebuilt_daily_value_mismatches':int((joined.daily_demand.fillna(0)!=joined.rebuilt.fillna(0)).sum()),
          'shared_skus_across_datasets':int(shared.sum()),
          'active_units_with_shared_skus':int(active.loc[active.sku.isin(shared[shared].index),'quantity'].sum()),
          'monthly_non_cancelled_units':active.assign(month=pd.to_datetime(active.create_time).dt.to_period('M').astype(str)).groupby(['platform','month']).quantity.sum().reset_index().to_dict(orient='records'),
          'timezone_status':'all raw timestamps are naive; UTC assumption retained for reproducibility, not externally certified',
          'deduplication_policy':'no deletion: identical allowlisted values and reused anonymized IDs are not proof of a duplicate source event',
          'coverage_status':'complete read of supplied files verified; complete collection of each business day is not established'}
    if args.warehouse:
        try: report['warehouse']=audit_warehouse(daily)
        except Exception as exc:
            # Do not print connection URLs or source-row values.
            report['warehouse']={'status':'unavailable','error_type':type(exc).__name__}
    (out/'data-quality.json').write_text(json.dumps(report,ensure_ascii=False,indent=2,allow_nan=False),encoding='utf-8')
    profiles.to_csv(out/'sku-demand-profile.csv',index=False,encoding='utf-8-sig')
    print(json.dumps(report,ensure_ascii=False))
    if args.warehouse and report['warehouse']['status']=='unavailable':sys.exit(2)


if __name__=='__main__':main()
