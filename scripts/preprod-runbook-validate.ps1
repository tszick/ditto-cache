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

Write-Host "Runbook validation started (node-loss, restore telemetry, quota/namespace telemetry)."
Write-Host "ComposeDir=$ComposeDir Namespace=$Namespace DryRun=$DryRun"

$scenario = [ordered]@{
    node_loss = "pending"
    restore_telemetry = "pending"
    quota_namespace_telemetry = "pending"
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
    # In full mode we currently validate the telemetry endpoint shape.
    # Deeper restore/quota drills depend on environment-specific policy setup.
    $summary = docker exec ditto-node-1 sh -lc "curl -sfk -u ditto:qwe123asd https://localhost:7778/health/summary"
    if ($LASTEXITCODE -ne 0) {
        throw "Failed to fetch /health/summary from ditto-node-1"
    }
    $json = $summary | ConvertFrom-Json

    foreach ($f in @("snapshot_restore_attempt_total", "snapshot_restore_success_ratio_pct", "namespace_quota_reject_total", "namespace_latency_top")) {
        if (-not ($json.PSObject.Properties.Name -contains $f)) {
            throw "Missing expected telemetry field in /health/summary: $f"
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
