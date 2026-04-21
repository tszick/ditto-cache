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
    param([string[]]$CmdArgs)
    $out = & docker @CmdArgs 2>&1
    if ($LASTEXITCODE -ne 0) {
        throw "docker command failed: docker $($CmdArgs -join ' ')`n$($out -join '`n')"
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
    return Invoke-DockerCmd -CmdArgs @("exec", $NodeContainer, "sh", "-lc", $cmd)
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

function Wait-NodeValue {
    param(
        [string]$NodeContainer,
        [string]$Path,
        [string]$Expected,
        [string]$NamespaceHeader = "",
        [int]$Attempts = 10,
        [int]$SleepSeconds = 1
    )
    for ($i = 1; $i -le $Attempts; $i++) {
        try {
            $raw = Invoke-NodeCurl -NodeContainer $NodeContainer -Method "GET" -Path $Path -NamespaceHeader $NamespaceHeader
            if ($raw -match [Regex]::Escape($Expected)) {
                return $true
            }
        } catch {
            # retry
        }
        Start-Sleep -Seconds $SleepSeconds
    }
    return $false
}

function Assert-KeyReplicatedAcrossNodes {
    param(
        [string[]]$Nodes,
        [string]$Path,
        [string]$Expected,
        [string]$NamespaceHeader = "",
        [int]$Attempts = 10,
        [int]$SleepSeconds = 1
    )
    foreach ($node in $Nodes) {
        $ok = Wait-NodeValue -NodeContainer $node -Path $Path -Expected $Expected -NamespaceHeader $NamespaceHeader -Attempts $Attempts -SleepSeconds $SleepSeconds
        if (-not $ok) {
            throw "replication check failed: node '$node' did not return expected value '$Expected' for path '$Path'"
        }
    }
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

function Assert-Condition {
    param(
        [bool]$Condition,
        [string]$Label
    )
    if (-not $Condition) {
        throw $Label
    }
}

function Emit-RunbookDiagnostics {
    Write-Host "Runbook diagnostics: collecting node summaries and recent logs..."
    $nodes = @("ditto-node-1", "ditto-node-2", "ditto-node-3", "ditto-mgmt")
    foreach ($node in $nodes) {
        try {
            if ($node -like "ditto-node-*") {
                $summary = Get-NodeSummary -NodeContainer $node -Attempts 3 -SleepSeconds 1
                $summaryJson = $summary | ConvertTo-Json -Depth 6 -Compress
                Write-Host "diag.summary.$node=$summaryJson"
            }
        } catch {
            Write-Host "diag.summary.$node=unavailable ($($_.Exception.Message))"
        }
        try {
            $tail = Invoke-DockerCmd -CmdArgs @("logs", "--tail", "80", $node)
            Write-Host "diag.logs.$node<<"
            Write-Host $tail
            Write-Host ">>diag.logs.$node"
        } catch {
            Write-Host "diag.logs.$node=unavailable ($($_.Exception.Message))"
        }
    }
}

function Invoke-DittoctlDoctor {
    param(
        [string]$RepoRoot
    )

    $tempConfig = Join-Path ([System.IO.Path]::GetTempPath()) ("dittoctl-preprod-" + [guid]::NewGuid().ToString("N") + ".toml")
    $configBody = @"
[mgmt]
url = "https://localhost:7781"
timeout_ms = 5000
username = "admin"
password = "qwe123asd"
insecure_skip_verify = true

[output]
format = "binary"
"@

    try {
        Set-Content -LiteralPath $tempConfig -Value $configBody -Encoding ASCII
        Push-Location $RepoRoot
        try {
            $doctorOutput = & cargo run -q -p dittoctl -- --config $tempConfig node doctor all 2>&1
            if ($LASTEXITCODE -ne 0) {
                throw "dittoctl node doctor all failed:`n$($doctorOutput -join "`n")"
            }
        } finally {
            Pop-Location
        }
        return ($doctorOutput -join "`n")
    } finally {
        if (Test-Path -LiteralPath $tempConfig) {
            Remove-Item -LiteralPath $tempConfig -Force
        }
    }
}

Write-Host "Runbook validation started (node-loss, restore telemetry, quota/namespace telemetry)."
Write-Host "ComposeDir=$ComposeDir Namespace=$Namespace DryRun=$DryRun"

$scenario = [ordered]@{
    node_loss = "pending"
    restore_telemetry = "pending"
    quota_namespace_telemetry = "pending"
    go_no_go_operational_checks = "pending"
    operator_diagnostics = "pending"
}

$preSummary = $null
if (-not $DryRun) {
    $preSummary = Get-NodeSummary -NodeContainer "ditto-node-1"
}

try {
    try {
        if ($DryRun) {
            & $chaosScript -ComposeDir $ComposeDir -Iterations $NodeLossIterations -Namespace $Namespace -SkipDelay -SkipPartition -DryRun
        } else {
            & $chaosScript -ComposeDir $ComposeDir -Iterations $NodeLossIterations -Namespace $Namespace -SkipDelay -SkipPartition
            $restartKeyPath = "key/chaos_restart_$NodeLossIterations"
            $restartExpected = "v$NodeLossIterations"
            Assert-KeyReplicatedAcrossNodes `
                -Nodes @("ditto-node-1", "ditto-node-2", "ditto-node-3") `
                -Path $restartKeyPath `
                -Expected $restartExpected `
                -NamespaceHeader $Namespace `
                -Attempts 12 `
                -SleepSeconds 1
        }
        $scenario.node_loss = "ok"
    } catch {
        $scenario.node_loss = "failed"
        throw
    }

    if ($DryRun) {
        $scenario.restore_telemetry = "dry-run"
        $scenario.quota_namespace_telemetry = "dry-run"
        $scenario.go_no_go_operational_checks = "dry-run"
        $scenario.operator_diagnostics = "dry-run"
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
                "hot_key_top_usage",
                "tcp_client_auth_enabled",
                "tcp_client_bind_loopback_only",
                "tcp_production_safe",
                "tcp_supported_topology",
                "insecure_runtime_enabled",
                "strict_security_enforced"
            )) {
                Assert-HasField -Obj $summary -FieldName $f
            }
            Assert-InRange -Value ([double]$summary.snapshot_restore_success_ratio_pct) -Min 0 -Max 100 -Label "$($kv.Key).snapshot_restore_success_ratio_pct"
            if ([double]$summary.committed_index -lt 0) {
                throw "$($kv.Key).committed_index must be non-negative"
            }
            if ($summary.availability -notin @("ready", "healthy", "degraded", "unavailable")) {
                throw "$($kv.Key).availability has unexpected value: $($summary.availability)"
            }
            Assert-Condition -Condition (-not [bool]$summary.insecure_runtime_enabled) -Label "$($kv.Key) is running with insecure runtime bypass enabled"
            Assert-Condition -Condition ([bool]$summary.strict_security_enforced) -Label "$($kv.Key) is not enforcing strict security"
            Assert-Condition -Condition ([bool]$summary.tcp_production_safe) -Label "$($kv.Key) reports unsupported TCP production topology"
        }

        $probeKey = "runbook_probe_" + [DateTimeOffset]::UtcNow.ToUnixTimeSeconds()
        $probeVal = "ok"
        $null = Invoke-NodeCurl -NodeContainer "ditto-node-1" -Method "PUT" -Path "key/$probeKey" -Body $probeVal -NamespaceHeader $Namespace
        Assert-KeyReplicatedAcrossNodes `
            -Nodes @("ditto-node-1", "ditto-node-2", "ditto-node-3") `
            -Path "key/$probeKey" `
            -Expected $probeVal `
            -NamespaceHeader $Namespace `
            -Attempts 10 `
            -SleepSeconds 1

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

        $repoRoot = Split-Path -Parent $PSScriptRoot
        if (-not (Get-Command cargo -ErrorAction SilentlyContinue)) {
            throw "cargo is required on the self-hosted runner to execute dittoctl doctor during real-run validation"
        }
        $doctorOutput = Invoke-DittoctlDoctor -RepoRoot $repoRoot

        $scenario.restore_telemetry = "ok"
        $scenario.quota_namespace_telemetry = "ok"
        $scenario.go_no_go_operational_checks = "ok"
        $scenario.operator_diagnostics = "ok"
    }

    $goNoGo = if ($DryRun) {
        [ordered]@{
            strict_security_enforced = "dry-run"
            tcp_topology_supported = "dry-run"
            recovery_and_telemetry_validated = "dry-run"
            doctor_clean = "dry-run"
            release_candidate = "dry-run"
        }
    } else {
        [ordered]@{
            strict_security_enforced = "pass"
            tcp_topology_supported = "pass"
            recovery_and_telemetry_validated = "pass"
            doctor_clean = "pass"
            release_candidate = "pass"
        }
    }

    $result = [ordered]@{
        timestamp_utc = (Get-Date).ToUniversalTime().ToString("o")
        compose_dir = $ComposeDir
        namespace = $Namespace
        dry_run = [bool]$DryRun
        scenarios = $scenario
        go_no_go = $goNoGo
        doctor_output = if ($DryRun) { "dry-run" } else { $doctorOutput }
    }

    $result | ConvertTo-Json -Depth 6 | Write-Host
    Write-Host "Runbook validation finished successfully."
} catch {
    if (-not $DryRun) {
        Emit-RunbookDiagnostics
    }
    throw
}
