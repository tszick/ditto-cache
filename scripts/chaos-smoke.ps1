param(
    [string]$ComposeDir = "..\\ditto-docker",
    [int]$Iterations = 3,
    [string]$Namespace = "",
    [string]$NetworkName = "ditto-docker_ditto-net",
    [int]$DelayPauseSeconds = 6,
    [switch]$SkipDelay,
    [switch]$SkipPartition,
    [switch]$DryRun
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Invoke-Docker {
    param([string[]]$CmdArgs)
    if ($DryRun) {
        Write-Host ("DRYRUN docker " + ($CmdArgs -join " "))
        return ""
    }
    $output = & docker @CmdArgs 2>&1
    if ($LASTEXITCODE -ne 0) {
        throw ("docker command failed: docker " + ($CmdArgs -join " ") + "`n" + ($output -join "`n"))
    }
    return ($output -join "`n")
}

function Invoke-NodeCurl {
    param(
        [string]$NodeContainer,
        [string]$Method,
        [string]$Path,
        [string]$Body = ""
    )
    $nsHeader = ""
    if (-not [string]::IsNullOrWhiteSpace($Namespace)) {
        $nsHeader = "-H 'X-Ditto-Namespace: $Namespace'"
    }
    $bodyArg = ""
    if ($Method -eq "PUT") {
        $bodyArg = "-d '$Body'"
    }
    $cmd = "curl -sfk -u ditto:qwe123asd $nsHeader -X $Method https://localhost:7778/$Path $bodyArg"
    return Invoke-Docker -CmdArgs @("exec", $NodeContainer, "sh", "-lc", $cmd)
}

function Assert-Contains {
    param(
        [string]$Text,
        [string]$Expected,
        [string]$Context
    )
    if ($Text -notmatch [Regex]::Escape($Expected)) {
        throw "Assertion failed ($Context): expected '$Expected' in '$Text'"
    }
}

function Wait-NodeValue {
    param(
        [string]$NodeContainer,
        [string]$Path,
        [string]$Expected,
        [int]$Attempts = 10,
        [int]$SleepSeconds = 2
    )
    for ($a = 1; $a -le $Attempts; $a++) {
        try {
            $text = Invoke-NodeCurl -NodeContainer $NodeContainer -Method "GET" -Path $Path
            if ($text -match [Regex]::Escape($Expected)) {
                return $true
            }
        } catch {
            # retry
        }
        Start-Sleep -Seconds $SleepSeconds
    }
    return $false
}

function Invoke-NodePutWithRetry {
    param(
        [string]$NodeContainer,
        [string]$Path,
        [string]$Body,
        [int]$Attempts = 6,
        [int]$SleepSeconds = 2
    )
    for ($a = 1; $a -le $Attempts; $a++) {
        try {
            $null = Invoke-NodeCurl -NodeContainer $NodeContainer -Method "PUT" -Path $Path -Body $Body
            return $true
        } catch {
            if ($a -eq $Attempts) {
                throw
            }
            Start-Sleep -Seconds $SleepSeconds
        }
    }
    return $false
}

Write-Host "Chaos smoke starting..."
Write-Host "ComposeDir=$ComposeDir Iterations=$Iterations Namespace='$Namespace' SkipDelay=$SkipDelay DelayPauseSeconds=$DelayPauseSeconds SkipPartition=$SkipPartition DryRun=$DryRun"

if (-not $DryRun) {
    Invoke-Docker -CmdArgs @("ps") | Out-Null
}

# Seed baseline key.
$baselineValue = "ok"
$null = Invoke-NodeCurl -NodeContainer "ditto-node-1" -Method "PUT" -Path "key/chaos_baseline" -Body $baselineValue
$baselineRead = Invoke-NodeCurl -NodeContainer "ditto-node-3" -Method "GET" -Path "key/chaos_baseline"
if (-not $DryRun) {
    Assert-Contains -Text $baselineRead -Expected $baselineValue -Context "baseline replication"
}
Write-Host "Baseline replication check OK."

for ($i = 1; $i -le $Iterations; $i++) {
    Write-Host "Restart loop iteration $i/$Iterations..."
    Invoke-Docker -CmdArgs @("stop", "ditto-node-2") | Out-Null
    Start-Sleep -Seconds 5

    $k = "chaos_restart_$i"
    $v = "v$i"
    $null = Invoke-NodePutWithRetry -NodeContainer "ditto-node-1" -Path "key/$k" -Body $v -Attempts 6 -SleepSeconds 2
    $read1 = Invoke-NodeCurl -NodeContainer "ditto-node-1" -Method "GET" -Path "key/$k"
    if (-not $DryRun) {
        Assert-Contains -Text $read1 -Expected $v -Context "write while node-2 down"
    }

    Invoke-Docker -CmdArgs @("start", "ditto-node-2") | Out-Null
    Start-Sleep -Seconds 5

    $read3 = Invoke-NodeCurl -NodeContainer "ditto-node-3" -Method "GET" -Path "key/$k"
    if (-not $DryRun) {
        Assert-Contains -Text $read3 -Expected $v -Context "post-restart replication"
    }
}
Write-Host "Restart loop checks OK."

if (-not $SkipDelay) {
    Write-Host "Running timing-fault scenario on ditto-node-3 (pause/unpause)..."
    $dk = "chaos_delay"
    $dv = "delay-ok"
    Invoke-Docker -CmdArgs @("pause", "ditto-node-3") | Out-Null
    try {
        Start-Sleep -Seconds $DelayPauseSeconds
        $null = Invoke-NodePutWithRetry -NodeContainer "ditto-node-1" -Path "key/$dk" -Body $dv -Attempts 6 -SleepSeconds 2
    } finally {
        Invoke-Docker -CmdArgs @("unpause", "ditto-node-3") | Out-Null
    }
    Start-Sleep -Seconds 3

    if (-not $DryRun) {
        $ok = Wait-NodeValue -NodeContainer "ditto-node-3" -Path "key/$dk" -Expected $dv -Attempts 12 -SleepSeconds 2
        if (-not $ok) {
            throw "Assertion failed (post-delay catch-up): expected '$dv' on node-3 after unpause"
        }
    }
    Write-Host "Timing-fault scenario check OK."
}

if (-not $SkipPartition) {
    Write-Host "Running partition scenario on ditto-node-2..."
    Invoke-Docker -CmdArgs @("network", "disconnect", "-f", $NetworkName, "ditto-node-2") | Out-Null
    Start-Sleep -Seconds 3

    $pk = "chaos_partition"
    $pv = "partition-ok"
    $null = Invoke-NodeCurl -NodeContainer "ditto-node-1" -Method "PUT" -Path "key/$pk" -Body $pv

    Invoke-Docker -CmdArgs @("network", "connect", $NetworkName, "ditto-node-2") | Out-Null
    Start-Sleep -Seconds 3

    if (-not $DryRun) {
        $ok = Wait-NodeValue -NodeContainer "ditto-node-2" -Path "key/$pk" -Expected $pv -Attempts 12 -SleepSeconds 2
        if (-not $ok) {
            throw "Assertion failed (post-partition catch-up): expected '$pv' on node-2 after reconnect"
        }
    }
    Write-Host "Partition scenario check OK."
}

Write-Host "Chaos smoke finished successfully."
