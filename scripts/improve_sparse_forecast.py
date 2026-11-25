"""Frozen-candidate nested rolling backtest and error analysis; no live deployment.

The July 29-August 11 window has been inspected in previous work. Its scores
are retrospective diagnostics, not a new untouched holdout. Nested selection
still uses only target dates prior to each outer forecast origin.
"""
from __future__ import annotations
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import platform
import sys
import time
import warnings

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
import joblib
import numpy as np
import pandas as pd

from ml.training.metrics import calculate_all_metrics
from serving.app.sparse_demand import SparseDemandRegressor, statistical_forecast, FEATURE_NAMES

OUT = ROOT/'docs/07-bao-cao-do-an/evidence/forecast-improvement-2026-11-08'
CANDIDATES = {
    'moving_average7': {'method':'moving_average7'},
    'mean28': {'method':'mean28'},
    'median28': {'method':'median28'},
    'croston_sba': {'method':'sba','alpha':.1},
    'tsb_slow': {'method':'tsb','alpha':.1,'beta':.05},
    'tsb_fast': {'method':'tsb','alpha':.1,'beta':.2},
    'histgb_l1_direct': {'objective':'absolute_error'},
    'histgb_poisson_direct': {'objective':'poisson'},
    'hurdle_direct': {'objective':'hurdle'},
    'zero_diagnostic': {'method':'zero'},
}


def score(actual, pred, history):
    y,p=np.asarray(actual),np.asarray(pred)
    m=calculate_all_metrics(y.reshape(-1),p.reshape(-1))
    error=np.abs(y-p);positive=y>0
    scale=np.mean(np.diff(history,axis=1)**2,axis=1)
    defined=scale>0
    m.update({'absolute_error_sum':float(error.sum()),'actual_sum':float(y.sum()),
              'prediction_sum':float(p.sum()),'error_on_zero_actual':float(error[~positive].sum()),
              'error_on_positive_actual':float(error[positive].sum()),
              'positive_actual_cells':int(positive.sum()),
              'predicted_positive_cells':int((p>1e-8).sum()),
              'lead14_wape':calculate_all_metrics(y.sum(axis=1),p.sum(axis=1))['wape'],
              'lead14_rmse':float(np.sqrt(np.mean((y.sum(axis=1)-p.sum(axis=1))**2))),
              'rmsse_macro':float(np.sqrt(np.mean((y-p)**2,axis=1)[defined]/scale[defined]).mean()) if defined.any() else None,
              'rmsse_defined_skus':int(defined.sum())})
    return m


def pooled(rows):
    a=sum(r['absolute_error_sum'] for r in rows);y=sum(r['actual_sum'] for r in rows)
    return {'wape':100*a/y if y else None,'absolute_error_sum':a,'actual_sum':y,
            'prediction_sum':sum(r['prediction_sum'] for r in rows),
            'bias':100*(sum(r['prediction_sum'] for r in rows)-y)/y if y else None}


def main():
    OUT.mkdir(parents=True,exist_ok=True)
    warnings.filterwarnings('ignore',message='X does not have valid feature names.*')
    paths=[ROOT/'data'/f for f in ['daily_history.parquet','features_daily.parquet','feature_spec.json','model_manifest.json']]
    hashes={p.relative_to(ROOT).as_posix():hashlib.sha256(p.read_bytes()).hexdigest() for p in paths}
    frame=pd.read_parquet(paths[0]);frame['date']=pd.to_datetime(frame.date).dt.normalize()
    if frame.duplicated(['sku','date']).any():raise ValueError('Duplicate daily keys.')
    wide=frame.pivot(index='sku',columns='date',values='daily_demand').sort_index(axis=1)
    if wide.isna().any().any():raise ValueError('Missing daily grid cells: explicit policy required.')
    y=wide.to_numpy(float);dates=list(wide.columns);skus=wide.index.astype(str).to_numpy()
    lookup={str(d.date()):i for i,d in enumerate(dates)}
    outer_starts=['2026-07-01','2026-07-15','2026-07-29']
    origins=sorted({lookup[start]-1-offset for start in outer_starts for offset in [0,14,28]})
    protocol={'created_at_utc':datetime.now(timezone.utc).isoformat(),
              'candidates':CANDIDATES,'outer_test_starts':outer_starts,'horizon':14,
              'inner_windows':2,'inner_window_days':14,'seed':42,'trees':100,
              'training_window_days':120,'selection_metric':'pooled daily WAPE; zero excluded from selection',
              'inventory_companion_selection':'minimum inner lead14 RMSE among mean28, SBA, TSB and Poisson',
              'learned_backend':'sklearn HistGradientBoosting (LightGBM native fit failed before scoring)',
              'previously_exposed_final_window':True,
              'experiment_type':'retrospective nested rolling evaluation; no claim of an untouched external test',
              'features':FEATURE_NAMES,'source_sha256':hashes,
              'actual_policy':'no clipping, deletion, deduplication or coverage certification'}
    protocol_path=OUT/'experiment-protocol.json'
    if protocol_path.exists():
        old=json.loads(protocol_path.read_text(encoding='utf-8'))
        if old['candidates']!=CANDIDATES:raise ValueError('Candidate set changed; use a new experiment directory.')
    else:protocol_path.write_text(json.dumps(protocol,ensure_ascii=False,indent=2),encoding='utf-8')
    started=time.perf_counter();bank={};fit_cache={};prediction_rows=[]
    for origin in origins:
        history=y[:,:origin+1]
        known=(history>0).any(axis=1)
        actual=y[known,origin+1:origin+15]
        if actual.shape[1]!=14:raise ValueError('Incomplete outer/inner target window.')
        bank[origin]={}
        for name,config in CANDIDATES.items():
            if 'objective' in config:
                model=SparseDemandRegressor(**config).fit_history(history,dates[:origin+1])
                pred=model.forecast_panel(history,dates[origin],14)[known]
                fit_cache[origin,name]=model
            else:pred=statistical_forecast(history,14,**config)[known]
            if pred.shape!=actual.shape or not np.isfinite(pred).all() or (pred<0).any():raise ValueError('Invalid forecast.')
            metrics=score(actual,pred,history[known])
            bank[origin][name]={'metrics':metrics,'actual':actual,'pred':pred,'known':known}
        print(json.dumps({'origin':str(dates[origin].date()),'scored_skus':int(known.sum()),
              'daily_wape':{n:round(v['metrics']['wape'],2) for n,v in bank[origin].items()}},ensure_ascii=False),flush=True)
    outer=[];chosen_daily=[];chosen_inventory=[]
    mean_candidates=['mean28','croston_sba','tsb_slow','tsb_fast','histgb_poisson_direct']
    for start in outer_starts:
        origin=lookup[start]-1;inner=[origin-28,origin-14]
        scores={n:pooled([bank[i][n]['metrics'] for i in inner]) for n in CANDIDATES}
        eligible=[n for n in CANDIDATES if n!='zero_diagnostic']
        selected=min(eligible,key=lambda n:(scores[n]['wape'],n))
        inventory=min(mean_candidates,key=lambda n:sum(bank[i][n]['metrics']['lead14_rmse'] for i in inner)/len(inner))
        row={'start':start,'end':str(dates[origin+14].date()),'origin':str(dates[origin].date()),
             'inner_test_end_before_or_at_origin':True,'inner_scores':scores,
             'selected_daily':selected,'selected_inventory_mean':inventory,
             'scores':{n:bank[origin][n]['metrics'] for n in CANDIDATES}}
        outer.append(row);chosen_daily.append(row['scores'][selected]);chosen_inventory.append(row['scores'][inventory])
        for name,entry in bank[origin].items():
            actual,pred,known=entry['actual'],entry['pred'],entry['known']
            for sku,a,p in zip(skus[known],actual,pred):
                m=calculate_all_metrics(a,p)
                prediction_rows.append({'window_start':start,'model':name,'sku':sku,**m,
                                        'actual_units':float(a.sum()),'predicted_units':float(p.sum()),
                                        'absolute_error':float(np.abs(a-p).sum()),
                                        'zero_day_error':float(p[a==0].sum()),'positive_actual_days':int((a>0).sum())})
    pooled_outer={name:pooled([r['scores'][name] for r in outer]) for name in CANDIDATES}
    pooled_outer['nested_selected_daily']=pooled(chosen_daily)
    pooled_outer['nested_selected_inventory_mean']=pooled(chosen_inventory)
    # Refit the fixed, development-selected learned daily candidate after evaluation.
    last_origin=lookup['2026-07-29']-1
    inner=[last_origin-28,last_origin-14]
    learned=[n for n,c in CANDIDATES.items() if 'objective' in c]
    learned_choice=min(learned,key=lambda n:pooled([bank[i][n]['metrics'] for i in inner])['wape'])
    candidate=SparseDemandRegressor(**CANDIDATES[learned_choice]).fit_history(y,dates)
    model_dir=ROOT/'output/models/forecast-improvement-2026-11-08';model_dir.mkdir(parents=True,exist_ok=True)
    artifact=model_dir/'sparse_demand_candidate.joblib';joblib.dump(candidate,artifact)
    loaded=joblib.load(artifact)
    probe=loaded.forecast_panel(y[:1],dates[-1],14)
    assert np.allclose(probe,candidate.forecast_panel(y[:1],dates[-1],14))
    card={'model_name':learned_choice,'artifact':artifact.relative_to(ROOT).as_posix(),
          'artifact_sha256':hashlib.sha256(artifact.read_bytes()).hexdigest(),
          'training_end':candidate.training_end,'training_rows':candidate.training_rows,
          'features':FEATURE_NAMES,'prediction_semantics':'conditional median-like daily point forecast' if candidate.objective!='poisson' else 'conditional mean daily rate',
          'source_sha256':hashes,'registry_updated':False,'serving_reloaded':False,
          'evaluation_warning':'retrospective; daily sparsity and unverified coverage limit operational interpretation',
          'do_not_use_median_as_inventory_mean':candidate.objective!='poisson'}
    (model_dir/'model-card.json').write_text(json.dumps(card,ensure_ascii=False,indent=2),encoding='utf-8')
    pd.DataFrame(prediction_rows).to_csv(OUT/'errors-by-sku.csv',index=False,encoding='utf-8-sig')
    summary=[]
    for r in outer:
        for name,m in r['scores'].items():summary.append({'test_start':r['start'],'model':name,**m})
    pd.DataFrame(summary).to_csv(OUT/'model-comparison.csv',index=False,encoding='utf-8-sig')
    after={p.relative_to(ROOT).as_posix():hashlib.sha256(p.read_bytes()).hexdigest() for p in paths}
    if hashes!=after:raise ValueError('Deployment artifacts changed.')
    result={'protocol':protocol,'outer_windows':outer,'pooled_outer':pooled_outer,
            'candidate_model':card,'deployment_artifacts_unchanged':True,
            'environment':{'python':platform.python_version(),'numpy':np.__version__,'pandas':pd.__version__,
                           'cpu_count':os.cpu_count(),'sklearn':__import__('sklearn').__version__},
            'elapsed_seconds':time.perf_counter()-started}
    (OUT/'model-evaluation.json').write_text(json.dumps(result,ensure_ascii=False,indent=2,allow_nan=False),encoding='utf-8')
    print(json.dumps({'pooled_outer':pooled_outer,'candidate_model':learned_choice,
                      'elapsed_seconds':result['elapsed_seconds']},ensure_ascii=False),flush=True)


if __name__=='__main__':main()
