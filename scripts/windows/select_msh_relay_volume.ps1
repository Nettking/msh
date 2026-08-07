[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string]$DataDirectory,

    [Parameter(Mandatory = $true)]
    [string]$OutputFile
)

$ErrorActionPreference = "Stop"
if (Test-Path variable:PSNativeCommandUseErrorActionPreference) {
    $PSNativeCommandUseErrorActionPreference = $false
}

function Write-Selection {
    param([Parameter(Mandatory = $true)][string]$VolumeName)

    $absolute = [System.IO.Path]::GetFullPath($OutputFile)
    $parent = [System.IO.Path]::GetDirectoryName($absolute)
    if (-not [string]::IsNullOrWhiteSpace($parent)) {
        [System.IO.Directory]::CreateDirectory($parent) | Out-Null
    }
    $encoding = New-Object System.Text.UTF8Encoding($false)
    [System.IO.File]::WriteAllText($absolute, $VolumeName, $encoding)
}

function Get-SavedNodeId {
    $path = Join-Path $DataDirectory "federation\device\identity.json"
    if (-not (Test-Path -LiteralPath $path -PathType Leaf)) {
        return ""
    }
    try {
        $payload = Get-Content -LiteralPath $path -Raw | ConvertFrom-Json
        return [string]$payload.node_id
    }
    catch {
        throw "Saved device identity is not valid JSON."
    }
}

function Find-ProbeImage {
    foreach ($image in @("msh-relay:latest", "msh-flask:latest", "msh-recorder:latest")) {
        & docker image inspect $image *> $null
        if ($LASTEXITCODE -eq 0) {
            return $image
        }
    }
    return ""
}

function Get-RelayCandidates {
    $values = @()
    $values += @(
        & docker volume ls --format "{{.Name}}" 2>$null
    ) | Where-Object {
        -not [string]::IsNullOrWhiteSpace($_) -and (
            $_ -eq "msh_relay_state" -or
            $_ -match "(^|_)relay_state$"
        )
    }

    $relayContainers = @(
        & docker ps -a `
            --filter "label=com.docker.compose.service=relay" `
            --format "{{.ID}}" 2>$null
    ) | Where-Object { -not [string]::IsNullOrWhiteSpace($_) }
    foreach ($containerId in $relayContainers) {
        $raw = (& docker inspect $containerId 2>$null) -join "`n"
        if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($raw)) {
            continue
        }
        try {
            $inspection = @($raw | ConvertFrom-Json)
            if ($inspection.Count -eq 0) {
                continue
            }
            $mount = @($inspection[0].Mounts) |
                Where-Object {
                    [string]$_.Destination -eq "/var/lib/msh-relay" -and
                    [string]$_.Type -eq "volume"
                } |
                Select-Object -First 1
            if ($null -ne $mount -and -not [string]::IsNullOrWhiteSpace([string]$mount.Name)) {
                $values += [string]$mount.Name
            }
        }
        catch {
            continue
        }
    }

    return @($values |
        Where-Object { -not [string]::IsNullOrWhiteSpace($_) } |
        Select-Object -Unique)
}

function Get-RelayProbe {
    param(
        [Parameter(Mandatory = $true)][string]$VolumeName,
        [Parameter(Mandatory = $true)][string]$ProbeImage,
        [string]$NodeId = ""
    )

    $python = @'
import json
import os
import sqlite3
import sys

path = "/state/control.sqlite3"
node_id = sys.argv[1] if len(sys.argv) > 1 else ""
result = {"exists": 0, "size": 0, "nodes": 0, "sessions": 0, "memberships": 0}
if os.path.isfile(path):
    result["exists"] = 1
    result["size"] = os.path.getsize(path)
    try:
        connection = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
        for key, table in (("nodes", "nodes"), ("sessions", "sessions")):
            try:
                result[key] = int(connection.execute(
                    f"SELECT COUNT(*) FROM {table}"
                ).fetchone()[0])
            except sqlite3.Error:
                pass
        if node_id:
            try:
                result["memberships"] = int(connection.execute(
                    "SELECT COUNT(*) FROM session_memberships "
                    "WHERE node_id=? AND removed_at IS NULL",
                    (node_id,),
                ).fetchone()[0])
            except sqlite3.Error:
                pass
        connection.close()
    except sqlite3.Error:
        pass
print(json.dumps(result, sort_keys=True))
'@

    # Windows PowerShell and Docker can reinterpret quotes/newlines passed to
    # `python -c`. Encode the complete program and pass only an ASCII one-liner.
    $encoded = [Convert]::ToBase64String(
        [System.Text.Encoding]::UTF8.GetBytes($python)
    )
    $launcher = "import base64;exec(base64.b64decode('$encoded'))"
    $mount = "${VolumeName}:/state:ro"
    $arguments = @(
        "run", "--rm", "--network", "none",
        "--entrypoint", "python",
        "-v", $mount,
        $ProbeImage,
        "-c", $launcher,
        $NodeId
    )
    $raw = (& docker @arguments 2>&1) -join "`n"
    if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($raw)) {
        return $null
    }
    try {
        return $raw | ConvertFrom-Json
    }
    catch {
        return $null
    }
}

try {
    $nodeId = Get-SavedNodeId
    $candidates = @(Get-RelayCandidates)
    $image = Find-ProbeImage
    $probes = @()

    if (-not [string]::IsNullOrWhiteSpace($image)) {
        foreach ($volume in $candidates) {
            $probe = Get-RelayProbe `
                -VolumeName $volume `
                -ProbeImage $image `
                -NodeId $nodeId
            if ($null -ne $probe) {
                $probes += [pscustomobject]@{
                    volume = [string]$volume
                    memberships = [int]$probe.memberships
                    sessions = [int]$probe.sessions
                    nodes = [int]$probe.nodes
                    size = [long]$probe.size
                }
            }
        }
    }

    $selected = ""
    if (-not [string]::IsNullOrWhiteSpace($nodeId)) {
        $memberMatch = @($probes |
            Where-Object { $_.memberships -gt 0 } |
            Sort-Object memberships, sessions, nodes, size -Descending |
            Select-Object -First 1)
        if ($memberMatch.Count -gt 0) {
            $selected = [string]($memberMatch[0].volume)
        }
    }

    if ([string]::IsNullOrWhiteSpace($selected)) {
        $populated = @($probes |
            Where-Object { $_.sessions -gt 0 -or $_.nodes -gt 0 } |
            Sort-Object sessions, nodes, size -Descending |
            Select-Object -First 1)
        if ($populated.Count -gt 0) {
            $selected = [string]($populated[0].volume)
        }
    }

    if ([string]::IsNullOrWhiteSpace($selected)) {
        if ($candidates -contains "msh_relay_state") {
            $selected = "msh_relay_state"
        }
        elseif ($candidates.Count -gt 0) {
            $selected = [string]($candidates[0])
        }
        else {
            $selected = "msh_relay_state"
        }
    }

    Write-Selection -VolumeName $selected
    exit 0
}
catch {
    [Console]::Error.WriteLine(
        "MSH Federation volume selection failed: " + $_.Exception.Message
    )
    exit 1
}