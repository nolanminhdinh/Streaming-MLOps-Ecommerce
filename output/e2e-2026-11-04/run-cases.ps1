$ErrorActionPreference='Stop'
$base='http://127.0.0.1:8050'
$dir='output/e2e-2026-11-04'
function Save($name,$value) { $value | ConvertTo-Json -Depth 22 | Set-Content -Encoding utf8 "$dir/$name.json" }
function GetDemo($path) { Invoke-RestMethod "$base$path" -TimeoutSec 25 }
function PostDemo($path,$body) {
  $token=(GetDemo '/api/state').csrf
  Invoke-RestMethod "$base$path" -Method Post -Headers @{'X-Demo-Token'=$token} -ContentType 'application/json' -Body (ConvertTo-Json -Compress $body) -TimeoutSec 25
}
function StreamCase($name,$count,$invalid,$replay=$false) {
  $repeats=1
  if($replay){$run=PostDemo '/api/demo/replay' @{}; $repeats=2}
  else {$run=PostDemo '/api/demo/start' @{count=$count;invalid=[bool]$invalid}}
  $deadline=(Get-Date).AddSeconds(160)
  do {
    $r=GetDemo '/api/report'
    $ok=$r.run.run_id -eq $run.run_id -and $r.run.status -eq 'published' -and $r.run.sent_orders -eq (($count+$invalid)*$repeats) -and $r.run.sent_inventory -eq (2*$repeats) -and $r.lake.raw_rows -eq (($count+$invalid)*$repeats) -and $r.warehouse.run_count -eq $count -and $r.lake.rejected_count -ge ($invalid*$repeats) -and @($r.lake.objects).Count -gt 0 -and $r.lake.processed_objects -eq @($r.lake.objects).Count
    if(!$ok -and $r.run.status -ne 'error'){Start-Sleep -Seconds 2}
  } until($ok -or $r.run.status -eq 'error' -or (Get-Date) -ge $deadline)
  Save $name @{passed=$ok;run=$r.run;lake=$r.lake;warehouse=$r.warehouse;captured=(Get-Date -Format o)}
  Write-Output "$name PASS=$ok run=$($run.run_id) ACK=$($r.run.sent_orders) raw=$($r.lake.raw_rows) facts=$($r.warehouse.run_count) rejected=$($r.lake.rejected_count)"
}
try {StreamCase normal 12 0} catch {Save normal @{error=$_.Exception.Message}; Write-Output $_.Exception.Message}
try {StreamCase invalid 12 1} catch {Save invalid @{error=$_.Exception.Message}; Write-Output $_.Exception.Message}
try {StreamCase replay 12 1 $true} catch {Save replay @{error=$_.Exception.Message}; Write-Output $_.Exception.Message}
try {StreamCase burst 50 0} catch {Save burst @{error=$_.Exception.Message}; Write-Output $_.Exception.Message}
foreach($stock in 0,100){
  try {$res=PostDemo '/api/inventory' @{sku='KCN-ANESSA';current_stock=$stock;lead_time_days=3}; Save "stock$stock" $res; $res.data.alerts | Select-Object sku,current_stock,alert_level,stock_source,demand_source | ConvertTo-Json -Compress}
  catch {Save "stock$stock" @{error=$_.Exception.Message}; Write-Output $_.Exception.Message}
}
