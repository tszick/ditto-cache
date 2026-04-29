param(
    [string]$RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path,
    [switch]$Check
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$protoPath = Join-Path $RepoRoot "ditto-protocol/proto/ditto.proto"
$outPath = Join-Path $RepoRoot "ditto-protocol/schema/protocol-contract.json"

if (-not (Test-Path $protoPath)) {
    throw "Protocol source not found: $protoPath"
}

function Clean-Line {
    param([string]$Line)
    return ($Line -replace "//.*$", "").Trim()
}

function Proto-Depth-Delta {
    param([string]$Line)
    return (([regex]::Matches($Line, "\{")).Count - ([regex]::Matches($Line, "\}")).Count)
}

function Enum-Values {
    param([string[]]$Body)
    $values = [System.Collections.Generic.List[string]]::new()
    foreach ($line in $Body) {
        if ($line -match "^([A-Z][A-Z0-9_]*)\s*=\s*\d+\s*;") {
            $values.Add($Matches[1])
        }
    }
    return $values.ToArray()
}

function Message-Fields {
    param([string[]]$Body)
    $fields = [System.Collections.Generic.List[object]]::new()
    $oneofDepth = 0
    foreach ($line in $Body) {
        if ($line -match "^oneof\s+[A-Za-z_][A-Za-z0-9_]*\s*\{") {
            $oneofDepth += 1
            continue
        }
        if ($line -match "^\}") {
            if ($oneofDepth -gt 0) {
                $oneofDepth -= 1
            }
            continue
        }
        if ($line -match "^(repeated\s+)?([A-Za-z_][A-Za-z0-9_]*)\s+([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(\d+)\s*;") {
            $fields.Add([ordered]@{
                name = $Matches[3]
                type = $Matches[2]
                repeated = [bool]$Matches[1]
                tag = [int]$Matches[4]
                oneof = ($oneofDepth -gt 0)
            })
        }
    }
    return $fields.ToArray()
}

$protoLines = Get-Content $protoPath
$protoRaw = Get-Content $protoPath -Raw

$packageMatch = [regex]::Match($protoRaw, "package\s+ditto\.protocol\.v(\d+)\s*;")
if (-not $packageMatch.Success) {
    throw "Could not parse protocol package version from $protoPath"
}
$protocolVersion = [int]$packageMatch.Groups[1].Value

$enums = [ordered]@{}
$messages = [ordered]@{}

$currentKind = $null
$currentName = $null
$currentBody = [System.Collections.Generic.List[string]]::new()
$depth = 0

foreach ($rawLine in $protoLines) {
    $line = Clean-Line -Line $rawLine
    if ($line.Length -eq 0) {
        continue
    }

    if ($null -eq $currentKind) {
        if ($line -match "^(enum|message)\s+([A-Za-z_][A-Za-z0-9_]*)\s*\{") {
            $currentKind = $Matches[1]
            $currentName = $Matches[2]
            $currentBody.Clear()
            $depth = Proto-Depth-Delta -Line $line
            if ($depth -le 0) {
                if ($currentKind -eq "enum") {
                    $enums[$currentName] = @()
                } else {
                    $messages[$currentName] = @()
                }
                $currentKind = $null
                $currentName = $null
            }
        }
        continue
    }

    $delta = Proto-Depth-Delta -Line $line
    if (-not ($line -match "^\}" -and ($depth + $delta) -le 0)) {
        $currentBody.Add($line)
    }
    $depth += $delta

    if ($depth -le 0) {
        if ($currentKind -eq "enum") {
            $enums[$currentName] = @((Enum-Values -Body $currentBody.ToArray()))
        } else {
            $messages[$currentName] = @((Message-Fields -Body $currentBody.ToArray()))
        }
        $currentKind = $null
        $currentName = $null
        $currentBody.Clear()
    }
}

$doc = [ordered]@{
    protocol_version = $protocolVersion
    source = "ditto-protocol/proto/ditto.proto"
    package = "ditto.protocol.v$protocolVersion"
    enums = $enums
    messages = $messages
}

$json = $doc | ConvertTo-Json -Depth 12

if ($Check) {
    if (-not (Test-Path $outPath)) {
        throw "Contract file missing: $outPath"
    }
    $currentObj = Get-Content $outPath -Raw | ConvertFrom-Json
    $current = $currentObj | ConvertTo-Json -Depth 12
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
