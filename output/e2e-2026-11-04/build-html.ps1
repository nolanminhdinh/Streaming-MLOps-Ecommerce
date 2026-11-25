$dir='output/e2e-2026-11-04'
node "$dir/build-report.cjs"
if($LASTEXITCODE){throw 'Could not build Markdown report'}
$body=(ConvertFrom-Markdown -Path "$dir/Bao-cao-kiem-thu-end-to-end.md").Html
$body=[regex]::Replace($body,'src="([a-zA-Z0-9_-]+\.jpg)"',{param($m) $bytes=[IO.File]::ReadAllBytes((Join-Path (Resolve-Path $dir) $m.Groups[1].Value)); 'src="data:image/jpeg;base64,'+[Convert]::ToBase64String($bytes)+'"'})
$style=@"
html{background:#eef2f7}body{max-width:980px;margin:28px auto;padding:36px;background:#fff;color:#1b293a;font:16px/1.55 system-ui;border:1px solid #d8e0ea;border-radius:16px}h1,h2,h3{line-height:1.25;color:#102d49}h2{border-bottom:1px solid #d8e0ea;padding-bottom:.35em;margin-top:2em}img{max-width:100%;height:auto;border:1px solid #c9d5e1;border-radius:8px}table{border-collapse:collapse;width:100%}td,th{border:1px solid #d8e0ea;padding:.45em;text-align:left}th{background:#eaf1f8}a{color:#0b4b86}code{color:#603b63} @media print{body{margin:0;border:0}img{break-inside:avoid;max-height:690px;object-fit:contain}h2,h3{break-after:avoid}}
"@
'<!doctype html><html lang="vi"><meta charset="utf-8"><title>Báo cáo kiểm thử end-to-end</title><style>'+$style+'</style><body>'+$body+'</body></html>' | Set-Content -Encoding utf8 "$dir/Bao-cao-kiem-thu-end-to-end.html"
