<#
.SYNOPSIS
    DataLink Command Center — Windows-side network recovery (one-shot).

.DESCRIPTION
    Resets all stale Windows host-networking state that prevents the
    Windows browser from reaching the WSL2 Docker stack. Run this when
    `http://localhost:8000` returns "Site can't be reached" / connection
    refused / connection reset, AND `docker ps` from WSL bash shows the
    containers are healthy.

    Specifically:
      1. Resets all `netsh interface portproxy` rules (cleared cleanly)
      2. Restarts `iphlpsvc` (IP Helper) to release cached port bindings
      3. Stops all WSL distros so the next boot picks up fresh networking
      4. Verifies the 7 DataLink ports are now free on Windows side

    Companion to `make recover` in WSL — run this FIRST (admin PS), then
    `make recover` (WSL bash). Together: ~3 minute total recovery.

.NOTES
    Must run as Administrator (UAC prompt elevates if not already).
    Idempotent — safe to run multiple times.
    Captures full output for support / audit.
#>

#Requires -Version 5.1

[CmdletBinding()]
param(
    [int[]] $Ports = @(8000, 8088, 3000, 5050, 8082, 8081, 8090)
)

# Re-elevate if not already admin.
$isAdmin = ([Security.Principal.WindowsPrincipal] [Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
if (-not $isAdmin) {
    Write-Host "Re-launching with admin elevation (UAC will prompt — click Yes)..." -ForegroundColor Yellow
    Start-Process powershell.exe -Verb RunAs -ArgumentList "-NoProfile -ExecutionPolicy Bypass -File `"$PSCommandPath`"" -Wait
    return
}

$ErrorActionPreference = 'Continue'
$transcript = "$env:TEMP\datalink-recover-$(Get-Date -Format yyyyMMdd-HHmmss).log"
Start-Transcript -Path $transcript -Force | Out-Null

Write-Host ""
Write-Host "============================================================" -ForegroundColor Cyan
Write-Host "  DataLink Command Center — Windows network recovery" -ForegroundColor Cyan
Write-Host "============================================================" -ForegroundColor Cyan
Write-Host ""

# --- 1. Snapshot of "before" state -----------------------------------------
Write-Host ">>> Step 1/5 — Snapshot current state" -ForegroundColor Cyan
$before = netstat -ano | Select-String "$(($Ports | ForEach-Object { ":$_.*LISTENING" }) -join '|')"
$beforeCount = ($before | Measure-Object).Count
Write-Host "  Listeners on DataLink ports BEFORE: $beforeCount"
$proxyBefore = (netsh interface portproxy show v4tov4 | Select-String '^\d|^[0-9.]' | Measure-Object).Count
Write-Host "  netsh portproxy rules BEFORE: $proxyBefore"
Write-Host ""

# --- 2. Reset netsh portproxy ----------------------------------------------
Write-Host ">>> Step 2/5 — Reset netsh interface portproxy" -ForegroundColor Cyan
netsh interface portproxy reset | Out-Null
$proxyAfter = (netsh interface portproxy show v4tov4 | Select-String '^\d|^[0-9.]' | Measure-Object).Count
Write-Host "  netsh portproxy rules AFTER:  $proxyAfter"
Write-Host ""

# --- 3. Restart iphlpsvc to release cached listeners -----------------------
Write-Host ">>> Step 3/5 — Restart iphlpsvc (IP Helper) to release cached bindings" -ForegroundColor Cyan
try {
    Restart-Service iphlpsvc -Force -ErrorAction Stop
    Start-Sleep -Seconds 2
    Write-Host "  iphlpsvc restarted"
} catch {
    Write-Host "  WARN: iphlpsvc restart failed: $($_.Exception.Message)" -ForegroundColor Yellow
}
Write-Host ""

# --- 4. WSL shutdown so next boot has fresh networking ---------------------
Write-Host ">>> Step 4/5 — wsl --shutdown (allows next boot to grab fresh ports)" -ForegroundColor Cyan
wsl --shutdown
Start-Sleep -Seconds 4
$running = wsl --list --running 2>&1 | Out-String
if ($running -match 'No running distributions|no running') {
    Write-Host "  All WSL distros stopped"
} else {
    Write-Host "  WSL state: $running"
}
Write-Host ""

# --- 5. Verify ports are free ----------------------------------------------
Write-Host ">>> Step 5/5 — Verify all 7 DataLink ports are now free" -ForegroundColor Cyan
$after = netstat -ano | Select-String "$(($Ports | ForEach-Object { ":$_.*LISTENING" }) -join '|')"
$afterCount = ($after | Measure-Object).Count
if ($afterCount -eq 0) {
    Write-Host "  SUCCESS: 0 listeners — all ports free" -ForegroundColor Green
} else {
    Write-Host "  WARN: $afterCount listeners still present:" -ForegroundColor Yellow
    $after | ForEach-Object { Write-Host "    $_" }
    Write-Host "  May need a Windows reboot if these don't release on next WSL boot." -ForegroundColor Yellow
}

Write-Host ""
Write-Host "============================================================" -ForegroundColor Cyan
Write-Host "  Windows side recovery complete." -ForegroundColor Green
Write-Host ""
Write-Host "  NEXT (in WSL bash):" -ForegroundColor Cyan
Write-Host "    cd /home/jatin/dev/DataPipelinesWithGX && make recover" -ForegroundColor White
Write-Host ""
Write-Host "  Transcript: $transcript" -ForegroundColor DarkGray
Write-Host "============================================================" -ForegroundColor Cyan
Write-Host ""

Stop-Transcript | Out-Null

# Pause so the window doesn't auto-close (when launched via Run as Admin).
if ($Host.Name -ne 'ConsoleHost' -or $env:WT_SESSION -eq $null) {
    Write-Host "Press any key to close..." -ForegroundColor DarkGray
    $null = $Host.UI.RawUI.ReadKey('NoEcho,IncludeKeyDown')
}
