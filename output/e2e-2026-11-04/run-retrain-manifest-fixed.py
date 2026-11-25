import json, os, pathlib, shutil, subprocess, sys, time, traceback, uuid
from urllib.request import Request, urlopen
from urllib.error import HTTPError
root=pathlib.Path('/workspace')
out=root/'output/e2e-2026-11-04'
work=pathlib.Path('/tmp/e2e')
work.mkdir()
for part in ['ml','monitoring','serving','warehouse']:
    shutil.copytree(root/part,work/part,ignore=shutil.ignore_patterns('__pycache__'))
data=work/'data'
data.mkdir()
shutil.copyfile(root/'data/model_manifest.json',data/'model_manifest.json')
sys.path.insert(0,str(work))
os.chdir(work)
os.environ.update(MODEL_MANIFEST_PATH=str(data/'model_manifest.json'),TRAINING_STATUS_PATH=str(data/'training_status.json'),TRAINING_ALERT_WEBHOOK_URL='',TRAINING_ALERT_WEBHOOK_TOKEN='',SERVING_RELOAD_URL='http://127.0.0.1:8010/model/reload')
from monitoring.evidently import trigger_retraining as tr
from ml.training.register_model import register_champion_model
from mlflow.tracking import MlflowClient
case='e2e-retraining-'+uuid.uuid4().hex[:8]
tr.RETRAIN_EXPERIMENT=case
result={'experiment':case,'status':'FAIL'}
def save():
    (out/'retrain-manifest-fixed.json').write_text(json.dumps(result,ensure_ascii=False,indent=2,default=str),encoding='utf-8')
def call(path,payload=None,host='http://127.0.0.1:8010'):
    req=Request(host+path,data=json.dumps(payload).encode() if payload is not None else None,headers={'Content-Type':'application/json'})
    try:
        with urlopen(req,timeout=20) as r: return {'http':r.status,'body':json.load(r)}
    except HTTPError as e: return {'http':e.code,'body':json.load(e)}
def check_ready():
    end=time.monotonic()+90
    while time.monotonic()<end:
        try:
            r=call('/ready')
            if r['http']==200:return r
        except OSError: pass
        time.sleep(2)
    raise RuntimeError('Isolated serving failed readiness')
api=None
try:
    manifest_path=data/'model_manifest.json'
    original=json.loads(manifest_path.read_text())
    main_before=call('/model/metadata',host='http://fastapi:8000')['body']
    client=MlflowClient(tracking_uri=os.environ['MLFLOW_TRACKING_URI'])
    previous=client.get_model_version(original['model_registry_name'],str(original['version']))
    from mlflow import artifacts
    artifact_dir=pathlib.Path(artifacts.download_artifacts(artifact_uri=previous.source,tracking_uri=os.environ['MLFLOW_TRACKING_URI']))
    remote_spec=json.loads(next(artifact_dir.rglob('feature_spec.json')).read_text())
    local_spec=json.loads((root/'data/feature_spec.json').read_text())
    ids=sorted(remote_spec.get('source_datasets') or [])
    assert remote_spec['data_origin']==original['data_origin']==local_spec['data_origin']
    assert ids and ids==sorted(local_spec.get('source_datasets') or []), 'Registry artifact and workspace feature spec disagree'
    assert sorted(original['source_datasets'])==ids, 'Checked-in manifest differs from deployed artifact'
    result['isolated_manifest_fingerprints']={'source':'existing_registry_artifact','matches_workspace_spec':True,'read_directly_from_manifest':True,'count':len(ids)}
    audit_model='ECommerceDemandForecastAudit_'+case.rsplit('-',1)[-1]
    client.create_registered_model(audit_model)
    seeded=client.create_model_version(audit_model,source=previous.source,run_id=previous.run_id)
    client.transition_model_version_stage(audit_model,seeded.version,'Staging')
    original.update(model_registry_name=audit_model,version=str(seeded.version))
    manifest_path.write_text(json.dumps(original))
    api=subprocess.Popen([sys.executable,'-m','uvicorn','serving.app.main:app','--host','127.0.0.1','--port','8010'],env={**os.environ,'PYTHONPATH':str(work)},stdout=open(out/'manifest-fixed-api.log','w'),stderr=subprocess.STDOUT)
    result['before_ready']=check_ready()
    result['before']=call('/model/metadata')['body']
    fixture=json.load(open(out/'api-monitor.json'))['forecast']['data']
    forecast={k:fixture[k] for k in ('sku','from_date','to_date')}
    result['before_forecast']=call('/predict/demand',forecast)
    save()
    loop=tr.ClosedLoopRetrainer(manifest_path=str(manifest_path),retraining_log_path=str(data/'audit.json'),register_fn=lambda md:register_champion_model(experiment_name=case,model_name=audit_model,stage='Staging',manifest_dir=md))
    started=time.monotonic()
    result['retraining']=loop.evaluate_and_trigger(force_retrain=True)
    result['retraining_seconds']=round(time.monotonic()-started,2)
    save()
    assert result['retraining']['status']=='RETRAINED', result['retraining']
    new=json.loads(manifest_path.read_text())
    run=client.get_run(new['run_id'])
    mv=client.get_model_version(audit_model,new['version'])
    result['registry']={'name':mv.name,'version':mv.version,'status':mv.status,'stage':mv.current_stage,'run_id':mv.run_id,'run_status':run.info.status,'metrics':run.data.metrics}
    result['training_report']=json.loads((data/'training_status.json').read_text())
    result['after_ready']=call('/ready')
    result['after_forecast']=call('/predict/demand',forecast)
    assert result['after_forecast']['body']['model_version']==new['version'] and result['after_forecast']['body']['model_source']=='mlflow_registry'
    assert result['before_forecast']['body']['model_version']==str(seeded.version)
    assert new['version']!=str(seeded.version) and mv.status=='READY' and run.info.status=='FINISHED'
    manifest_path.write_text(json.dumps({**new,'version':'9999999'}))
    result['bad_reload']=call('/model/reload',{})
    result['forecast_after_bad_reload']=call('/predict/demand',forecast)
    manifest_path.write_text(json.dumps(original))
    result['rollback']=call('/model/reload',{})
    result['forecast_after_rollback']=call('/predict/demand',forecast)
    assert result['bad_reload']['http']==503 and result['forecast_after_bad_reload']['body']['model_version']==new['version']
    assert result['rollback']['http']==200 and result['forecast_after_rollback']['body']['model_version']==str(seeded.version)
    main_after=call('/model/metadata',host='http://fastapi:8000')['body']
    result['main_before']={k:main_before.get(k) for k in ('version','model_source','model_registry_name')}
    result['main_after']={k:main_after.get(k) for k in result['main_before']}
    assert result['main_before']==result['main_after']
    result['status']='PASS'
except Exception as ex:
    result['error']=repr(ex)
    traceback.print_exc()
finally:
    if api: api.terminate()
    save()
    print('RESULT '+result['status'],flush=True)
sys.exit(result['status']!='PASS')


