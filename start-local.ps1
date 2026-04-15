param(
    [switch]$Https
)

$root = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $root

if ($Https) {
    $env:FLASK_USE_HTTPS = "1"
    Write-Host "Mode HTTPS demande. Le certificat local sera utilise s'il existe dans .\certs\" -ForegroundColor Yellow
} else {
    Remove-Item Env:FLASK_USE_HTTPS -ErrorAction SilentlyContinue
}

& ".\.venv\Scripts\python.exe" ".\app.py"
