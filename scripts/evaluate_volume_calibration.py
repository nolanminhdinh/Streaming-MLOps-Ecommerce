"""Exploratory round two: volume calibration fitted only on prior OOF windows.

The initial experiment stays unchanged. This follow-up tests a fixed calibration
rule rather than selecting a scale from any outer test actuals.
"""
from pathlib import Path
from datetime import datetime, timezone
import hashlib,json,sys,time,warnings
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT))
import joblib
import numpy as np
import pandas as pd
from serving.app.sparse_demand import SparseDemandRegressor,VolumeCalibratedSparseModel,statistical_forecast
from scripts.improve_sparse_forecast import score,pooled


def calibration_factor(previous_windows):
    actual=sum(float(y.sum()) for y,p in previous_windows)
    forecast=sum(float(p.sum()) for y,p in previous_windows)
    if forecast<=0 or actual<=0:return 1.
    return float(np.clip(actual/forecast,.1,2.))


def main():
    out=ROOT/'docs/07-bao-cao-do-an/evidence/forecast-improvement-2026-11-08'
    protocol={'round':'exploratory_followup_after_initial_comparison',
              'created_at_utc':datetime.now(timezone.utc).isoformat(),
              'base_model':'HistGradientBoostingRegressor Poisson direct horizons 1..14',
              'rule':'factor=clip(sum actual / sum forecast in two prior OOF windows, 0.1, 2.0)',
              'outer_actuals_used_for_calibration':False,'candidate_or_scale_selected_from_outer_actuals':False,
              'final_window_previously_exposed':True,'external_prospective_validation':False}
    p=out/'calibration-protocol.json'
    if not p.exists():p.write_text(json.dumps(protocol,indent=2),encoding='utf-8')
    warnings.filterwarnings('ignore',message='X does not have valid feature names.*')
    d=pd.read_parquet(ROOT/'data/daily_history.parquet');d['date']=pd.to_datetime(d.date)
    wide=d.pivot(index='sku',columns='date',values='daily_demand').sort_index(axis=1)
    y=wide.to_numpy(float);dates=list(wide.columns);skus=wide.index.astype(str).to_numpy()
    lookup={str(x.date()):i for i,x in enumerate(dates)}
    starts=['2026-07-01','2026-07-15','2026-07-29']
    origins=sorted({lookup[s]-1-k for s in starts for k in [0,14,28]})
    bank={};started=time.perf_counter()
    for origin in origins:
        history=y[:,:origin+1];known=(history>0).any(axis=1)
        model=SparseDemandRegressor(objective='poisson').fit_history(history,dates[:origin+1])
        pred=model.forecast_panel(history,dates[origin],14)[known]
        bank[origin]={'y':y[known,origin+1:origin+15],'pred':pred,'known':known}
    rows=[];metrics=[];baseline=[];paired_a=np.zeros(len(y));paired_b=np.zeros(len(y));paired_y=np.zeros(len(y))
    for start in starts:
        origin=lookup[start]-1;entry=bank[origin]
        factor=calibration_factor([(bank[o]['y'],bank[o]['pred']) for o in [origin-28,origin-14]])
        actual=entry['y'];pred=entry['pred']*factor;known=entry['known'];history=y[known,:origin+1]
        old=statistical_forecast(history,14,'moving_average7')
        m=score(actual,pred,history);b=score(actual,old,history)
        rows.append({'test_start':start,'calibration_factor':factor,
                     'calibration_target_start':str(dates[origin-27].date()),
                     'calibration_target_end':str(dates[origin].date()),
                     'training_end':str(dates[origin].date()),'metrics':m,'moving_average_metrics':b})
        metrics.append(m);baseline.append(b)
        paired_a[known]+=np.abs(actual-pred).sum(axis=1)
        paired_b[known]+=np.abs(actual-old).sum(axis=1);paired_y[known]+=actual.sum(axis=1)
    rng=np.random.default_rng(42);diff=[]
    for _ in range(1000):
        sample=rng.integers(0,len(y),len(y));denom=paired_y[sample].sum()
        if denom:diff.append(100*(paired_b[sample].sum()-paired_a[sample].sum())/denom)
    # Refit after retrospective evaluation; calibration uses only OOF predictions
    # for the two most recent fully observed windows in the saved snapshot.
    full=SparseDemandRegressor(objective='poisson').fit_history(y,dates)
    final_factor=calibration_factor([(bank[o]['y'],bank[o]['pred']) for o in origins[-2:]])
    wrapped=VolumeCalibratedSparseModel(full,final_factor)
    model_dir=ROOT/'output/models/forecast-improvement-2026-11-08';model_dir.mkdir(exist_ok=True,parents=True)
    artifact=model_dir/'calibrated_mean_candidate.joblib';joblib.dump(wrapped,artifact)
    loaded=joblib.load(artifact)
    assert np.allclose(loaded.forecast_panel(y[:2],dates[-1],14),wrapped.forecast_panel(y[:2],dates[-1],14))
    result={'protocol':protocol,'windows':rows,'pooled':pooled(metrics),'moving_average_pooled':pooled(baseline),
            'macro_lead14_rmse':float(np.mean([m['lead14_rmse'] for m in metrics])),
            'moving_average_macro_lead14_rmse':float(np.mean([m['lead14_rmse'] for m in baseline])),
            'paired_sku_bootstrap_wape_reduction_95pct':np.quantile(diff,[.025,.975]).tolist(),
            'bootstrap_scope':'conditional exploratory resampling by SKU; time dependence and data completeness not resolved',
            'model_artifact':artifact.relative_to(ROOT).as_posix(),'model_sha256':hashlib.sha256(artifact.read_bytes()).hexdigest(),
            'final_refit_training_end':full.training_end,'final_calibration_factor':final_factor,
            'model_registered':False,'serving_reloaded':False,'elapsed_seconds':time.perf_counter()-started}
    (out/'calibration-evaluation.json').write_text(json.dumps(result,ensure_ascii=False,indent=2,allow_nan=False),encoding='utf-8')
    pd.DataFrame({'sku':skus,'absolute_error_calibrated':paired_a,'absolute_error_moving_average':paired_b,'actual_units':paired_y}).to_csv(out/'calibrated-errors-by-sku.csv',index=False,encoding='utf-8-sig')
    print(json.dumps(result,ensure_ascii=False),flush=True)


if __name__=='__main__':main()
