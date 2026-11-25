$ErrorActionPreference='Stop'
$dir='output/e2e-2026-11-04'
$demo='http://127.0.0.1:8050'
function Save($name,$obj) {$obj | ConvertTo-Json -Depth 15 | Set-Content -Encoding utf8 "$dir/$name.json"}
function Probe($url,$sec=7) {
 try {$r=Invoke-WebRequest $url -TimeoutSec $sec -SkipHttpErrorCheck; @{http=[int]$r.StatusCode}}
 catch {@{http=0;error=$_.Exception.Message}}
}
function WaitFor($predicate,$seconds=100) {
 $end=(Get-Date).AddSeconds($seconds)
 do { $val=& $predicate; if($val){return $val}; Start-Sleep -Seconds 2 } until((Get-Date) -ge $end)
 return $null
}
function Control($op,$name) {
 & docker compose $op $name | Out-Host
 if($LASTEXITCODE){throw "Docker $op $name exit $LASTEXITCODE"}
}
function Report {Invoke-RestMethod "$demo/api/report" -TimeoutSec 10}
function SendRun {
 $token=(Invoke-RestMethod "$demo/api/state" -TimeoutSec 10).csrf
 Invoke-RestMethod "$demo/api/demo/start" -Method Post -Headers @{'X-Demo-Token'=$token} -ContentType 'application/json' -Body '{"count":4,"invalid":false}' -TimeoutSec 10
}
function Summary($r) {
 if(!$r){return $null}
 @{run=$r.run.run_id; status=$r.run.status;ack=$r.run.sent_orders;inventory_ack=$r.run.sent_inventory;raw=$r.lake.raw_rows; facts=$r.warehouse.run_count;processed=$r.lake.processed_objects;objects=@($r.lake.objects).Count;worker=$r.worker.status; services=$r.services}
}
foreach($svc in 'kafka','minio','postgres','pipeline-worker','fastapi','mlflow','grafana','prometheus'){
 $case=[ordered]@{service=$svc;started=(Get-Date -Format o);passed=$false}
 $run=$null
 try {
  Control stop $svc
  switch($svc){
   'kafka' {
    $run=SendRun
    $err=WaitFor { $r=Report; if($r.run.run_id -eq $run.run_id -and $r.run.status -eq 'error'){Summary $r} } 45
    $case.down=$err
   }
   {$_ -in 'minio','postgres','pipeline-worker','fastapi','mlflow','grafana','prometheus'} {
    $run=SendRun
    $obs=WaitFor { $r=Report; if($r.run.run_id -eq $run.run_id -and $r.run.status -eq 'published'){Summary $r} } 40
    $case.down=$obs
    if($svc -in 'minio','postgres','pipeline-worker'){
     $case.outage_observed=WaitFor {
       $r=Report
       if($r.run.run_id -eq $run.run_id){
        if($svc -eq 'minio' -and !$r.services.minio.ok){Summary $r}
        elseif($svc -eq 'postgres' -and !$r.services.postgres.ok){Summary $r}
        elseif($svc -eq 'pipeline-worker' -and !$r.services.worker.ok){Summary $r}
       }
     } 45
    }
    switch($svc){
     'fastapi' {$case.endpoint_down=Probe 'http://localhost:8000/ready'}
     'mlflow' {$case.endpoint_down=Probe 'http://localhost:5000/health';$case.serving_during=Probe 'http://localhost:8000/ready'}
     'grafana' {$case.endpoint_down=Probe 'http://localhost:3000/api/health';$case.prometheus_during=Probe 'http://localhost:9090/-/ready'}
     'prometheus' {$case.endpoint_down=Probe 'http://localhost:9090/-/ready';$case.grafana_during=Probe 'http://localhost:3000/api/health'}
    }
    if($svc -in 'fastapi','mlflow','grafana','prometheus'){
     $case.ingested_during=WaitFor { $r=Report; if($r.run.run_id -eq $run.run_id -and $r.warehouse.run_count -eq 4){Summary $r} } 70
    }
   }
  }
 } catch {$case.error=$_.Exception.Message}
 finally {
  try {Control start $svc} catch {$case.restore_error=$_.Exception.Message}
 }
 try {
  if($svc -eq 'kafka'){
   $k=WaitFor {$r=Report; if($r.services.kafka.ok){$true}} 80
   if($k){$run=SendRun}
  }
  if($run){
   $case.after=WaitFor { $r=Report; if($r.run.run_id -eq $run.run_id -and $r.run.sent_orders -eq 4 -and $r.run.sent_inventory -eq 2 -and $r.lake.raw_rows -ge 4 -and $r.warehouse.run_count -eq 4 -and @($r.lake.objects).Count -gt 0 -and $r.lake.processed_objects -eq @($r.lake.objects).Count){Summary $r} } 120
  }
  $case.ready_after=WaitFor { $p=Probe 'http://localhost:8000/ready' 4; if($p.http -eq 200){$p} } 70
  $case.passed=($null -ne $case.down -and $null -ne $case.after -and $null -ne $case.ready_after -and !$case.error -and !$case.restore_error)
  if($svc -in 'fastapi','mlflow','grafana','prometheus'){$case.passed=$case.passed -and $case.endpoint_down.http -eq 0 -and $null -ne $case.ingested_during}
 } catch {$case.recovery_error=$_.Exception.Message}
 if($svc -in 'minio','postgres','pipeline-worker'){$case.passed=$case.passed -and $null -ne $case.outage_observed}
 Save "fault-$svc" $case
 Write-Output "$svc PASS=$($case.passed) run=$($case.after.run)"
}

