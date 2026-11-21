# 8gua-api start script (run on server)
# usage: powershell -File start.ps1
$ErrorActionPreference = 'Stop'
$dir = 'C:/apps/8gua-api'
$port = 8123

# stop old process listening on 8123
Get-NetTCPConnection -LocalPort $port -State Listen -ErrorAction SilentlyContinue |
  ForEach-Object { Stop-Process -Id $_.OwningProcess -Force -ErrorAction SilentlyContinue }

New-Item -ItemType Directory -Force -Path (Join-Path $dir 'logs') | Out-Null
New-Item -ItemType Directory -Force -Path (Join-Path $dir 'data') | Out-Null

Start-Process -FilePath 'node' -ArgumentList 'index.mjs' -WorkingDirectory $dir `
  -WindowStyle Hidden `
  -RedirectStandardOutput (Join-Path $dir 'logs/out.log') `
  -RedirectStandardError (Join-Path $dir 'logs/err.log')

Start-Sleep -Seconds 2
try {
  $r = Invoke-WebRequest -Uri ('http://127.0.0.1:' + $port + '/api/health') -UseBasicParsing -TimeoutSec 10
  Write-Output ('HEALTH ' + $r.StatusCode + ' ' + $r.Content)
} catch {
  Write-Output 'HEALTH FAIL'
  Get-Content (Join-Path $dir 'logs/err.log') -Tail 30 -ErrorAction SilentlyContinue
}
