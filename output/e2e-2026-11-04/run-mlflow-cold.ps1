$ErrorActionPreference='Stop'
$dir='output/e2e-2026-11-04'
$api='http://localhost:8000'
function Probe($path,$body=$null) {
 try {
  if($null -eq $body){$r=Invoke-WebRequest "$api$path" -TimeoutSec 6 -SkipHttpErrorCheck}
  else {$r=Invoke-WebRequest "$api$path" -Method Post -ContentType 'application/json' -Body (ConvertTo-Json -Compress $body) -TimeoutSec 12 -SkipHttpErrorCheck}
  @{http=[int]$r.StatusCode;data=($r.Content|ConvertFrom-Json)}
 } catch {@{http=0;error=$_.Exception.Message}}
}
function Until($path,$http,$seconds=90){
 $end=(Get-Date).AddSeconds($seconds)
 do {$r=Probe $path; if($r.http -eq $http){return $r}; Start-Sleep -Seconds 2} until((Get-Date) -ge $end)
 return $r
}
$x=[ordered]@{started=(Get-Date -Format o);passed=$false}
$inputEvidence=Get-Content "$dir/api-monitor.json" -Raw|ConvertFrom-Json
$f=$inputEvidence.forecast.data
$req=@{sku=$f.sku;from_date=$f.from_date;to_date=$f.to_date}
try {
 docker compose stop mlflow | Out-Host
 if($LASTEXITCODE){throw 'Cannot stop MLflow'}
 $x.warm_forecast=Probe '/predict/demand' $req
 docker restart fastapi-serving | Out-Host
 if($LASTEXITCODE){throw 'Cannot restart FastAPI'}
 $x.cold_ready=Until '/ready' 503 45
 $x.cold_forecast=Probe '/predict/demand' $req
 $source=(Get-Content 'ml/mlflow/setup_tracking.py' -Raw) + [Environment]::NewLine + @"
import json
import mlflow
uri=configure_mlflow(experiment_name='e2e-local-fallback-smoke')
with mlflow.start_run(run_name='outage-smoke'):
    mlflow.log_metric('test_value', 1.0)
print(json.dumps({'local_fallback':uri.startswith('file:'),'metric_logged':True}))
"@
 $testResult=$source | docker compose exec -T fastapi python -c "import sys; __file__='/tmp/ml/mlflow/setup_tracking.py'; exec(compile(sys.stdin.read(),__file__,'exec'),{'__name__':'e2e_check','__file__':__file__})"
 if(!$LASTEXITCODE){$x.training_tracking_smoke=$testResult | Select-Object -Last 1 | ConvertFrom-Json}
} catch {$x.error=$_.Exception.Message}
finally {docker compose start mlflow | Out-Host; if($LASTEXITCODE){$x.restore_error='Could not start MLflow'}}
$x.ready_after=Until '/ready' 200 140
$x.forecast_after=Probe '/predict/demand' $req
$x.passed=$x.warm_forecast.data.model_source -eq 'mlflow_registry' -and $x.cold_ready.http -eq 503 -and $x.cold_forecast.data.model_source -like 'heuristic*' -and $x.ready_after.http -eq 200 -and $x.forecast_after.data.model_source -eq 'mlflow_registry' -and $x.training_tracking_smoke.local_fallback -and !$x.error -and !$x.restore_error
$x | ConvertTo-Json -Depth 16 | Set-Content -Encoding utf8 "$dir/mlflow-cold.json"
Write-Output "MLflow cold/warm and local tracking fallback PASS=$($x.passed)"
