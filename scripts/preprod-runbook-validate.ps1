param(
    [string]$ComposeDir = "..\\ditto-docker",
    [string]$Namespace = "preprod-drill",
    [int]$NodeLossIterations = 1,
    [switch]$DryRun
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$chaosScript = Join-Path $PSScriptRoot "chaos-smoke.ps1"
if (-not (Test-Path $chaosScript)) {
    throw "Missing chaos script: $chaosScript"
}

function Invoke-DockerCmd {
    param([string[]]$Args)
    $out = & docker @Args 2>&1
    if ($LASTEXITCODE -ne 0) {
        throw "docker command failed: docker $($Args -join ' ')`n$($out -join '`n')"
    }
    return ($out -join "`n")
}

function Invoke-NodeCurl {
    param(
        [string]$NodeContainer,
        [string]$Method,
        [string]$Path,
        [string]$Body = "",
        [string]$NamespaceHeader = ""
    )

    $nsArg = ""
    if (-not [string]::IsNullOrWhiteSpace($NamespaceHeader)) {
        $nsArg = "-H 'X-Ditto-Namespace: $NamespaceHeader'"
    }
    $bodyArg = ""
    if ($Method -eq "PUT") {
        $bodyArg = "-d '$Body'"
    }
    $cmd = "curl -sfk -u ditto:qwe123asd $nsArg -X $Method https://localhost:7778/$Path $bodyArg"
    return Invoke-DockerCmd -Args @("exec", $NodeContainer, "sh", "-lc", $cmd)
}

function Get-NodeSummary {
    param(
        [string]$NodeContainer,
        [int]$Attempts = 10,
        [int]$SleepSeconds = 1
    )
    for ($i = 1; $i -le $Attempts; $i++) {
        try {
            $raw = Invoke-NodeCurl -NodeContainer $NodeContainer -Method "GET" -Path "health/summary"
            return ($raw | ConvertFrom-Json)
        } catch {
            if ($i -eq $Attempts) { throw }
            Start-Sleep -Seconds $SleepSeconds
        }
    }
    throw "failed to fetch summary from $NodeContainer"
}

function Assert-HasField {
    param($Obj, [string]$FieldName)
    if (-not ($Obj.PSObject.Properties.Name -contains $FieldName)) {
        throw "Missing expected field: $FieldName"
    }
}

function Assert-InRange {
    param([double]$Value, [double]$Min, [double]$Max, [string]$Label)
    if ($Value -lt $Min -or $Value -gt $Max) {
        throw "$Label out of range [$Min,$Max]: $Value"
    }
}

Write-Host "Runbook validation started (node-loss, restore telemetry, quota/namespace telemetry)."
Write-Host "ComposeDir=$ComposeDir Namespace=$Namespace DryRun=$DryRun"

$scenario = [ordered]@{
    node_loss = "pending"
    restore_telemetry = "pending"
    quota_namespace_telemetry = "pending"
}

$preSummary = $null
if (-not $DryRun) {
    $preSummary = Get-NodeSummary -NodeContainer "ditto-node-1"
}

try {
    if ($DryRun) {
        & $chaosScript -ComposeDir $ComposeDir -Iterations $NodeLossIterations -Namespace $Namespace -SkipDelay -SkipPartition -DryRun
    } else {
        & $chaosScript -ComposeDir $ComposeDir -Iterations $NodeLossIterations -Namespace $Namespace -SkipDelay -SkipPartition
    }
    $scenario.node_loss = "ok"
} catch {
    $scenario.node_loss = "failed"
    throw
}

if ($DryRun) {
    $scenario.restore_telemetry = "dry-run"
    $scenario.quota_namespace_telemetry = "dry-run"
} else {
    $nodeSummaries = @{}
    foreach ($node in @("ditto-node-1", "ditto-node-2", "ditto-node-3")) {
        $nodeSummaries[$node] = Get-NodeSummary -NodeContainer $node
    }

    foreach ($kv in $nodeSummaries.GetEnumerator()) {
        $summary = $kv.Value
        foreach ($f in @(
            "availability",
            "committed_index",
            "snapshot_restore_attempt_total",
            "snapshot_restore_success_total",
            "snapshot_restore_failure_total",
            "snapshot_restore_policy_block_total",
            "snapshot_restore_success_ratio_pct",
            "namespace_quota_reject_total",
            "namespace_latency_top",
            "hot_key_top_usage"
        )) {
            Assert-HasField -Obj $summary -FieldName $f
        }
        Assert-InRange -Value ([double]$summary.snapshot_restore_success_ratio_pct) -Min 0 -Max 100 -Label "$($kv.Key).snapshot_restore_success_ratio_pct"
        if ([double]$summary.committed_index -lt 0) {
            throw "$($kv.Key).committed_index must be non-negative"
        }
        if ($summary.availability -notin @("healthy", "degraded", "unavailable")) {
            throw "$($kv.Key).availability has unexpected value: $($summary.availability)"
        }
    }

    $probeKey = "runbook_probe_" + [DateTimeOffset]::UtcNow.ToUnixTimeSeconds()
    $probeVal = "ok"
    $null = Invoke-NodeCurl -NodeContainer "ditto-node-1" -Method "PUT" -Path "key/$probeKey" -Body $probeVal -NamespaceHeader $Namespace
    $probeRead = Invoke-NodeCurl -NodeContainer "ditto-node-3" -Method "GET" -Path "key/$probeKey" -NamespaceHeader $Namespace
    if ($probeRead -notmatch [Regex]::Escape($probeVal)) {
        throw "namespace probe read-back failed for key '$probeKey'"
    }

    $postProbe = Get-NodeSummary -NodeContainer "ditto-node-1"
    $latTopText = ($postProbe.namespace_latency_top | ConvertTo-Json -Compress)
    if ($latTopText -notmatch [Regex]::Escape($Namespace)) {
        throw "namespace_latency_top does not include namespace '$Namespace' after probe traffic"
    }
    $hotTopText = ($postProbe.hot_key_top_usage | ConvertTo-Json -Compress)
    if ($hotTopText -notmatch [Regex]::Escape("::" + $probeKey)) {
        throw "hot_key_top_usage does not include probe key '$probeKey'"
    }

    if ($null -ne $preSummary) {
        if ([uint64]$postProbe.snapshot_restore_attempt_total -lt [uint64]$preSummary.snapshot_restore_attempt_total) {
            throw "snapshot_restore_attempt_total regressed unexpectedly"
        }
        if ([uint64]$postProbe.namespace_quota_reject_total -lt [uint64]$preSummary.namespace_quota_reject_total) {
            throw "namespace_quota_reject_total regressed unexpectedly"
        }
    }

    $scenario.restore_telemetry = "ok"
    $scenario.quota_namespace_telemetry = "ok"
}

$result = [ordered]@{
    timestamp_utc = (Get-Date).ToUniversalTime().ToString("o")
    compose_dir = $ComposeDir
    namespace = $Namespace
    dry_run = [bool]$DryRun
    scenarios = $scenario
}

$result | ConvertTo-Json -Depth 6 | Write-Host
Write-Host "Runbook validation finished successfully."
