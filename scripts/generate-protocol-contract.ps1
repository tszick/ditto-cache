param(
    [string]$RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path,
    [switch]$Check
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$protocolPath = Join-Path $RepoRoot "ditto-protocol/src/lib.rs"
$nodePath = Join-Path $RepoRoot "dittod/src/node.rs"
$outPath = Join-Path $RepoRoot "ditto-protocol/schema/protocol-contract.json"

if (-not (Test-Path $protocolPath)) {
    throw "Protocol source not found: $protocolPath"
}
if (-not (Test-Path $nodePath)) {
    throw "Node source not found: $nodePath"
}

function Get-BraceDelta {
    param([string]$Line)
    $open = ([regex]::Matches($Line, "\{")).Count
    $close = ([regex]::Matches($Line, "\}")).Count
    return ($open - $close)
}

function Get-EnumVariants {
    param(
        [string[]]$Lines,
        [string]$EnumName
    )

    $start = -1
    for ($i = 0; $i -lt $Lines.Length; $i++) {
        if ($Lines[$i] -match "^\s*pub\s+enum\s+$([regex]::Escape($EnumName))\b") {
            $start = $i
            break
        }
    }
    if ($start -lt 0) {
        throw "Enum not found: $EnumName"
    }

    $variants = [System.Collections.Generic.List[string]]::new()
    $depth = 0
    $started = $false

    for ($i = $start; $i -lt $Lines.Length; $i++) {
        $line = $Lines[$i]
        if (-not $started) {
            if ($line -match "\{") {
                $started = $true
                $depth += (Get-BraceDelta -Line $line)
            }
            continue
        }

        if ($depth -eq 1) {
            if ($line -match "^\s*([A-Za-z][A-Za-z0-9_]*)\s*(?:\{|\(|,)") {
                $variants.Add($Matches[1])
            }
        }

        $depth += (Get-BraceDelta -Line $line)
        if ($depth -le 0) {
            break
        }
    }

    return $variants.ToArray()
}

function Get-StructFields {
    param(
        [string[]]$Lines,
        [string]$StructName
    )

    $start = -1
    for ($i = 0; $i -lt $Lines.Length; $i++) {
        if ($Lines[$i] -match "^\s*pub\s+struct\s+$([regex]::Escape($StructName))\b") {
            $start = $i
            break
        }
    }
    if ($start -lt 0) {
        throw "Struct not found: $StructName"
    }

    $fields = [System.Collections.Generic.List[string]]::new()
    $depth = 0
    $started = $false

    for ($i = $start; $i -lt $Lines.Length; $i++) {
        $line = $Lines[$i]
        if (-not $started) {
            if ($line -match "\{") {
                $started = $true
                $depth += (Get-BraceDelta -Line $line)
            }
            continue
        }

        if ($depth -eq 1) {
            if ($line -match "^\s*pub\s+([A-Za-z_][A-Za-z0-9_]*)\s*:") {
                $fields.Add($Matches[1])
            }
        }

        $depth += (Get-BraceDelta -Line $line)
        if ($depth -le 0) {
            break
        }
    }

    return $fields.ToArray()
}

$protocolLines = Get-Content $protocolPath
$nodeRaw = Get-Content $nodePath -Raw
$versionMatch = [regex]::Match($nodeRaw, "const\s+PROTOCOL_VERSION\s*:\s*u16\s*=\s*(\d+)\s*;")
if (-not $versionMatch.Success) {
    throw "Could not parse PROTOCOL_VERSION from $nodePath"
}
$protocolVersion = [int]$versionMatch.Groups[1].Value

$enums = [ordered]@{}
foreach ($name in @(
    "ClientRequest",
    "ClientResponse",
    "ErrorCode",
    "ClusterMessage",
    "GossipMessage",
    "AdminRequest",
    "AdminResponse",
    "NodeStatus"
)) {
    $enums[$name] = @((Get-EnumVariants -Lines $protocolLines -EnumName $name))
}

$structs = [ordered]@{}
foreach ($name in @(
    "NamespaceQuotaUsage",
    "NamespaceLatencySummary",
    "HotKeyUsage",
    "NodeStats"
)) {
    $structs[$name] = @((Get-StructFields -Lines $protocolLines -StructName $name))
}

$doc = [ordered]@{
    protocol_version = $protocolVersion
    source = "ditto-protocol/src/lib.rs"
    enums = $enums
    structs = $structs
}

$json = $doc | ConvertTo-Json -Depth 8

if ($Check) {
    if (-not (Test-Path $outPath)) {
        throw "Contract file missing: $outPath"
    }
    $currentObj = Get-Content $outPath -Raw | ConvertFrom-Json
    $current = $currentObj | ConvertTo-Json -Depth 8
    if ($current.Trim() -ne $json.Trim()) {
        throw "Protocol contract drift detected. Run: pwsh ./scripts/generate-protocol-contract.ps1"
    }
    Write-Host "Protocol contract check OK."
    exit 0
}

$dir = Split-Path -Parent $outPath
if (-not (Test-Path $dir)) {
    New-Item -ItemType Directory -Path $dir -Force | Out-Null
}

Set-Content -Path $outPath -Value $json -Encoding utf8
Write-Host "Wrote protocol contract: $outPath"
