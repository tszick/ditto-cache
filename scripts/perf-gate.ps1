param(
    [int]$Samples = 120,
    [int]$Warmup = 20,
    [int]$Port = 17778,
    [string]$BaselinePath = "docs/perf-baseline.json",
    [switch]$WriteBaseline
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$baselineFile = Join-Path $repoRoot $BaselinePath
$tempDir = Join-Path $repoRoot ".tmp-perf"
$configPath = Join-Path $tempDir "perf-node.toml"
$outLog = Join-Path $tempDir "dittod.out.log"
$errLog = Join-Path $tempDir "dittod.err.log"
$baseUrl = "http://127.0.0.1:$Port"

if (Test-Path $tempDir) {
    Remove-Item -Recurse -Force $tempDir
}
New-Item -ItemType Directory -Path $tempDir | Out-Null

@"
[node]
id = "perf-dev"
active = true
bind_addr = "127.0.0.1"
client_port = 17777
http_port = $Port
cluster_port = 17779
gossip_port = 17780
cluster_bind_addr = "127.0.0.1"

[cluster]
seeds = []
max_nodes = 3

[cache]
max_memory_mb = 128
default_ttl_secs = 0
eviction_policy = "lfu"
value_size_limit_bytes = 1048576
max_keys = 10000

[replication]
write_timeout_ms = 500
write_quorum_mode = "majority"
gossip_interval_ms = 200
gossip_dead_ms = 15000
version_check_interval_ms = 0
read_repair_on_miss_enabled = false
read_repair_min_interval_ms = 5000
read_repair_max_per_minute = 30
anti_entropy_enabled = false
anti_entropy_interval_ms = 60000
anti_entropy_min_repair_interval_ms = 30000
anti_entropy_lag_threshold = 32
anti_entropy_key_sample_size = 0
anti_entropy_full_reconcile_every = 0
anti_entropy_full_reconcile_max_keys = 0
anti_entropy_budget_max_checks_per_run = 0
anti_entropy_budget_max_duration_ms = 0
mixed_version_probe_enabled = false
mixed_version_probe_interval_ms = 30000

[tls]
enabled = false

[http_auth]

[backup]
enabled = false
schedule = "0 2 * * *"
path = "./backups"
format = "binary"
retain_days = 7
restore_on_start = false

[persistence]
platform_allowed = false
runtime_enabled = false
backup_allowed = false
export_allowed = false
import_allowed = false

[tenancy]
enabled = false
default_namespace = "default"
max_keys_per_namespace = 0

[rate_limit]
enabled = false
requests_per_sec = 20000
burst = 40000

[circuit_breaker]
enabled = false
failure_threshold = 50
open_ms = 5000
half_open_max_requests = 10

[hot_key]
enabled = false
max_waiters = 64
follower_wait_timeout_ms = 25
stale_ttl_ms = 0
stale_max_entries = 128
adaptive_waiters_enabled = false
adaptive_min_waiters = 4
adaptive_success_threshold = 8
adaptive_state_max_keys = 4096

[compression]
enabled = false
threshold_bytes = 4096

[log]
enabled = false
path = "./logs"
rotation = "daily"
retain_days = 7
level = "warn"
"@ | Set-Content -Path $configPath -Encoding utf8

$env:DITTO_INSECURE = "true"
$env:RUST_LOG = "error"

Write-Host "Building dittod for perf gate..."
& cargo build -q -p dittod
if ($LASTEXITCODE -ne 0) {
    throw "cargo build -p dittod failed"
}

$binPath = Join-Path $repoRoot "target/debug/dittod.exe"
if (-not (Test-Path $binPath)) {
    throw "dittod binary not found: $binPath"
}

Write-Host "Starting dittod for perf gate..."
$job = Start-Job -ScriptBlock {
    param($bin, $cfg, $root)
    Set-Location $root
    $env:DITTO_INSECURE = "true"
    $env:RUST_LOG = "error"
    & $bin $cfg
} -ArgumentList $binPath, $configPath, $repoRoot

function Stop-Node {
    param([System.Management.Automation.Job]$J)
    if ($null -ne $J) {
        Stop-Job -Job $J -ErrorAction SilentlyContinue | Out-Null
        Remove-Job -Job $J -Force -ErrorAction SilentlyContinue | Out-Null
    }
}

function Wait-NodeReady {
    param([string]$Url, [int]$Retries = 240)
    for ($i = 1; $i -le $Retries; $i++) {
        try {
            $r = Invoke-WebRequest -Uri "$Url/ping" -Method GET -TimeoutSec 2 -UseBasicParsing
            if ($r.StatusCode -eq 200) {
                return $true
            }
        } catch {
            Start-Sleep -Milliseconds 500
        }
    }
    return $false
}

function Get-PercentileMs {
    param([double[]]$Values, [int]$Percentile)
    if ($Values.Count -eq 0) { return 0.0 }
    $sorted = $Values | Sort-Object
    $rank = [Math]::Ceiling(($Percentile / 100.0) * $sorted.Count)
    $idx = [Math]::Min([Math]::Max([int]$rank - 1, 0), $sorted.Count - 1)
    return [Math]::Round([double]$sorted[$idx], 2)
}

try {
    if (-not (Wait-NodeReady -Url $baseUrl)) {
        $jobOut = (Receive-Job -Job $job -Keep -ErrorAction SilentlyContinue | Out-String)
        throw "dittod did not become ready on $baseUrl. job output:`n$jobOut"
    }

    Write-Host "Warmup phase..."
    for ($i = 1; $i -le $Warmup; $i++) {
        $k = "warm-$i"
        Invoke-WebRequest -Uri "$baseUrl/key/$k" -Method PUT -Body "v$i" -TimeoutSec 3 -UseBasicParsing | Out-Null
        Invoke-WebRequest -Uri "$baseUrl/key/$k" -Method GET -TimeoutSec 3 -UseBasicParsing | Out-Null
    }

    Write-Host "Sampling $Samples requests..."
    $lat = New-Object System.Collections.Generic.List[double]
    $failures = 0
    for ($i = 1; $i -le $Samples; $i++) {
        $k = "perf-$i"
        try {
            $sw = [System.Diagnostics.Stopwatch]::StartNew()
            Invoke-WebRequest -Uri "$baseUrl/key/$k" -Method PUT -Body "v$i" -TimeoutSec 5 -UseBasicParsing | Out-Null
            $sw.Stop()
            $lat.Add($sw.Elapsed.TotalMilliseconds)

            $sw = [System.Diagnostics.Stopwatch]::StartNew()
            Invoke-WebRequest -Uri "$baseUrl/key/$k" -Method GET -TimeoutSec 5 -UseBasicParsing | Out-Null
            $sw.Stop()
            $lat.Add($sw.Elapsed.TotalMilliseconds)
        } catch {
            $failures++
        }
    }

    $p50 = Get-PercentileMs -Values $lat.ToArray() -Percentile 50
    $p95 = Get-PercentileMs -Values $lat.ToArray() -Percentile 95
    $p99 = Get-PercentileMs -Values $lat.ToArray() -Percentile 99

    $result = [ordered]@{
        gate_mode = "smoke-regression"
        runtime_profile = "debug-single-node-http"
        security_profile = "insecure-dev-bypass"
        release_signoff_ready = $false
        scenario = "single_node_http"
        samples_requested = $Samples
        request_count_measured = $lat.Count
        failures = $failures
        p50_ms = $p50
        p95_ms = $p95
        p99_ms = $p99
    }

    if ($WriteBaseline) {
        $baseline = [ordered]@{
            version = 1
            gate_mode = "smoke-regression"
            runtime_profile = "debug-single-node-http"
            security_profile = "insecure-dev-bypass"
            release_signoff_ready = $false
            scenarios = [ordered]@{
                single_node_http = [ordered]@{
                    p50_ms_max = [Math]::Ceiling($p50 * 1.15)
                    p95_ms_max = [Math]::Ceiling($p95 * 1.15)
                    p99_ms_max = [Math]::Ceiling($p99 * 1.15)
                    max_failures = $failures
                }
            }
        }
        $dir = Split-Path -Parent $baselineFile
        if (-not (Test-Path $dir)) { New-Item -ItemType Directory -Path $dir -Force | Out-Null }
        $baseline | ConvertTo-Json -Depth 8 | Set-Content -Path $baselineFile -Encoding utf8
        Write-Host "Baseline written to $baselineFile"
    }

    if (-not (Test-Path $baselineFile)) {
        throw "Missing baseline file: $baselineFile"
    }

    $baseline = Get-Content $baselineFile -Raw | ConvertFrom-Json
    $limits = $baseline.scenarios.single_node_http
    if ($baseline.PSObject.Properties.Name -contains "release_signoff_ready" -and $baseline.release_signoff_ready) {
        throw "Perf baseline is incorrectly marked as release_signoff_ready=true. This gate is intended as a smoke regression check only."
    }

    Write-Host (($result | ConvertTo-Json -Depth 6))
    Write-Host "Perf gate classification: smoke-regression only (debug build, single-node HTTP, insecure dev bypass)."

    if ($failures -gt [int]$limits.max_failures) {
        throw "Perf gate failed: failures=$failures > max_failures=$($limits.max_failures)"
    }
    if ($p50 -gt [double]$limits.p50_ms_max) {
        throw "Perf gate failed: p50=$p50 > p50_ms_max=$($limits.p50_ms_max)"
    }
    if ($p95 -gt [double]$limits.p95_ms_max) {
        throw "Perf gate failed: p95=$p95 > p95_ms_max=$($limits.p95_ms_max)"
    }
    if ($p99 -gt [double]$limits.p99_ms_max) {
        throw "Perf gate failed: p99=$p99 > p99_ms_max=$($limits.p99_ms_max)"
    }

    Write-Host "Performance gate PASSED."
}
finally {
    Stop-Node -J $job
}
