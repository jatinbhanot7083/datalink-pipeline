<#
.SYNOPSIS
    DataLink Command Center -- Windows-side boot-time bridge.

.DESCRIPTION
    Runs at every Windows login (via the DataLink-Network-AutoRecover
    scheduled task). Establishes deterministic Windows -> WSL2 TCP
    forwarding so http://localhost:8000 (and the other 6 DataLink UIs)
    work without relying on WSL2's flaky localhost-mirror feature.

    Sequence:
      1. Wait for WSL to be up (poll wsl hostname -I for up to 90s).
      2. Read the current WSL VM eth0 IP (changes on every WSL boot).
      3. Reset any stale netsh portproxy rules.
      4. Add explicit rules: 0.0.0.0:PORT -> WSL_IP:PORT for 7 ports.
      5. Add Windows Firewall inbound TCP allow rules for the same ports.
      6. Log every step + result so any failure is post-mortem-able.

    Idempotent -- safe to run any number of times.

.NOTES
    Production-portable: this file is Windows-only by extension. It is
    never executed on Linux production hosts. Sits alongside the rest
    of the dev-only Windows scaffolding under scripts/.

    Container side requirement: the 7 ports must be bound on 0.0.0.0
    inside the WSL VM (controlled by DL_BIND_HOST env var; default
    127.0.0.1, dev .env overrides to 0.0.0.0).
#>

#Requires -Version 5.1

[CmdletBinding()]
param(
    [int[]] $Ports = @(8000, 8088, 3000, 5050, 8082, 8081, 8090),
    [string] $LogPath = "$env:USERPROFILE\datalink-network-recover.log"
)

$ErrorActionPreference = 'Continue'

function Write-Log {
    param([string]$Msg)
    $line = "$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')  $Msg"
    Add-Content -Path $LogPath -Value $line -Encoding utf8
}

Write-Log "===== DataLink network-recover-at-login starting ====="

# 1. Wait for WSL to be up + reachable
$wslIp = $null
for ($i = 1; $i -le 18; $i++) {
    try {
        $rawIp = (wsl hostname -I 2>$null) -as [string]
        if ($rawIp) {
            $candidate = $rawIp.Trim().Split(' ')[0]
            if ($candidate -match '^\d+\.\d+\.\d+\.\d+$') {
                $wslIp = $candidate
                Write-Log "WSL reachable on attempt $i -- eth0 IP = $wslIp"
                break
            }
        }
    } catch {
        # WSL not up yet; keep polling
    }
    Start-Sleep -Seconds 5
}

if (-not $wslIp) {
    Write-Log "FATAL: WSL not reachable after 90s of polling. Aborting."
    exit 1
}

# 2. Reset stale portproxy rules
Write-Log "Resetting netsh portproxy state"
netsh interface portproxy reset 2>&1 | Out-Null

# 3. Add fresh portproxy rules pointing to current WSL IP
$addedProxies = 0
foreach ($p in $Ports) {
    $r = netsh interface portproxy add v4tov4 listenaddress=0.0.0.0 listenport=$p connectaddress=$wslIp connectport=$p 2>&1
    if ($LASTEXITCODE -eq 0) {
        $addedProxies++
        Write-Log "  added portproxy 0.0.0.0:$p -> ${wslIp}:$p"
    } else {
        Write-Log "  WARN portproxy add failed for :$p -- $r"
    }
}

# 4. Ensure Windows Firewall allows inbound on these ports
$addedFw = 0
foreach ($p in $Ports) {
    $name = "DataLink-WSL-Port-$p"
    Get-NetFirewallRule -DisplayName $name -ErrorAction SilentlyContinue | Remove-NetFirewallRule -ErrorAction SilentlyContinue
    try {
        New-NetFirewallRule -DisplayName $name -Direction Inbound -Action Allow -Protocol TCP -LocalPort $p -Profile Any -ErrorAction Stop | Out-Null
        $addedFw++
    } catch {
        Write-Log "  WARN firewall rule failed for :$p -- $($_.Exception.Message)"
    }
}
Write-Log "Firewall rules added: $addedFw / $($Ports.Count)"

# 5. Quick verification
$pattern = ($Ports | ForEach-Object { ":$_.*LISTENING" }) -join '|'
$listeners = (netstat -ano | Select-String $pattern | Measure-Object).Count
Write-Log "Verification: $listeners ports now have Windows-side listeners"

Write-Log "===== Done. portproxies=$addedProxies firewalls=$addedFw listeners=$listeners ====="
exit 0
