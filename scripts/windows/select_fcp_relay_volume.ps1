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

function Invoke-Docker {
    param([string[]]$Arguments = @())

    $previousPreference = $ErrorActionPreference
    $hasNativePreference = Test-Path variable:PSNativeCommandUseErrorActionPreference
    if ($hasNativePreference) {
        $previousNativePreference = $PSNativeCommandUseErrorActionPreference
    }
    try {
        # Windows PowerShell 5.1 can promote native stderr to a terminating
        # PowerShell error while ErrorActionPreference is Stop. Docker writes
        # ordinary lookup failures (for example a missing local image) to
        # stderr, so treat the native exit code as authoritative instead.
        $ErrorActionPreference = "Continue"
        if ($hasNativePreference) {
            $PSNativeCommandUseErrorActionPreference = $false
        }
        $global:LASTEXITCODE = 0
        $output = @(& docker @Arguments 2>&1)
        $exitCode = [int]$LASTEXITCODE
    }
    finally {
        if ($hasNativePreference) {
            $PSNativeCommandUseErrorActionPreference = $previousNativePreference
        }
        $ErrorActionPreference = $previousPreference
    }

    return [pscustomobject]@{
        ExitCode = $exitCode
        Output = @($output | ForEach-Object { [string]$_ })
    }
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

function Get-RelayContainerInfo {
    $result = Invoke-Docker -Arguments @(
        "ps", "-a",
        "--filter", "label=com.docker.compose.service=relay",
        "--format", "{{.ID}}"
    )
    if ($result.ExitCode -ne 0) {
        return @()
    }

    $values = @()
    $containerIds = @($result.Output |
        Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
    foreach ($containerId in $containerIds) {
        $inspectionResult = Invoke-Docker -Arguments @("inspect", $containerId)
        if ($inspectionResult.ExitCode -ne 0) {
            continue
        }
        $raw = ($inspectionResult.Output -join "`n")
        if ([string]::IsNullOrWhiteSpace($raw)) {
            continue
        }
        try {
            $inspection = @($raw | ConvertFrom-Json)
            if ($inspection.Count -eq 0) {
                continue
            }
            $mount = @($inspection[0].Mounts) |
                Where-Object {
                    [string]$_.Destination -eq "/var/lib/fcp-relay" -and
                    [string]$_.Type -eq "volume"
                } |
                Select-Object -First 1
            if ($null -eq $mount -or [string]::IsNullOrWhiteSpace([string]$mount.Name)) {
                continue
            }
            $running = $false
            if ($null -ne $inspection[0].State) {
                $running = [bool]($inspection[0].State.Running)
            }
            $values += [pscustomobject]@{
                volume = [string]($mount.Name)
                image = [string]($inspection[0].Image)
                running = $running
            }
        }
        catch {
            continue
        }
    }
    return @($values)
}

function Find-ProbeImages {
    param([object[]]$RelayContainers = @())

    $candidates = @(
        "fcp-relay:latest",
        "fcp-flask:latest",
        "fcp-recorder:latest"
    )
    foreach ($container in $RelayContainers) {
        if (-not [string]::IsNullOrWhiteSpace([string]$container.image)) {
            $candidates += [string]($container.image)
        }
    }

    $available = @()
    foreach ($image in @($candidates | Select-Object -Unique)) {
        $inspection = Invoke-Docker -Arguments @("image", "inspect", $image)
        if ($inspection.ExitCode -eq 0) {
            $available += $image
        }
    }
    return @($available | Select-Object -Unique)
}

function Get-RelayCandidates {
    param([object[]]$RelayContainers = @())

    $values = @()
    $volumeResult = Invoke-Docker -Arguments @(
        "volume", "ls", "--format", "{{.Name}}"
    )
    if ($volumeResult.ExitCode -eq 0) {
        $values += @($volumeResult.Output) | Where-Object {
            -not [string]::IsNullOrWhiteSpace($_) -and (
                $_ -eq "fcp_relay_state" -or
                $_ -match "(^|_)relay_state$"
            )
        }
    }

    foreach ($container in $RelayContainers) {
        if (-not [string]::IsNullOrWhiteSpace([string]$container.volume)) {
            $values += [string]($container.volume)
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
    $result = Invoke-Docker -Arguments $arguments
    if ($result.ExitCode -ne 0) {
        return $null
    }
    $raw = ($result.Output -join "`n")
    if ([string]::IsNullOrWhiteSpace($raw)) {
        return $null
    }
    try {
        return $raw | ConvertFrom-Json
    }
    catch {
        return $null
    }
}

function Get-UniqueContainerVolumes {
    param(
        [object[]]$RelayContainers,
        [bool]$RunningOnly = $false
    )

    $items = @($RelayContainers)
    if ($RunningOnly) {
        $items = @($items | Where-Object { $_.running })
    }
    return @($items |
        ForEach-Object { [string]$_.volume } |
        Where-Object { -not [string]::IsNullOrWhiteSpace($_) } |
        Select-Object -Unique)
}

try {
    $nodeId = Get-SavedNodeId
    $relayContainers = @(Get-RelayContainerInfo)
    $candidates = @(Get-RelayCandidates -RelayContainers $relayContainers)
    $images = @(Find-ProbeImages -RelayContainers $relayContainers)
    $probes = @()

    foreach ($volume in $candidates) {
        foreach ($image in $images) {
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
                break
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

    # A legacy installation may not have any of the current fcp-* image tags.
    # In that case the volume mounted by the existing relay container is a
    # stronger preservation signal than blindly preferring fcp_relay_state.
    if ([string]::IsNullOrWhiteSpace($selected)) {
        $runningVolumes = @(Get-UniqueContainerVolumes `
            -RelayContainers $relayContainers `
            -RunningOnly $true)
        if ($runningVolumes.Count -eq 1) {
            $selected = [string]($runningVolumes[0])
        }
        elseif ($runningVolumes.Count -gt 1) {
            throw "Multiple running relay containers use different state volumes; refusing to guess."
        }
    }

    if ([string]::IsNullOrWhiteSpace($selected)) {
        $mountedVolumes = @(Get-UniqueContainerVolumes `
            -RelayContainers $relayContainers)
        if ($mountedVolumes.Count -eq 1) {
            $selected = [string]($mountedVolumes[0])
        }
        elseif ($mountedVolumes.Count -gt 1) {
            throw "Multiple retained relay volumes are mounted by existing containers; refusing to guess."
        }
    }

    if ([string]::IsNullOrWhiteSpace($selected)) {
        if ($candidates.Count -eq 1) {
            $selected = [string]($candidates[0])
        }
        elseif ($candidates.Count -gt 1) {
            throw "Multiple relay-state volumes exist but none can be identified safely; refusing to guess."
        }
        else {
            $selected = "fcp_relay_state"
        }
    }

    Write-Selection -VolumeName $selected
    exit 0
}
catch {
    [Console]::Error.WriteLine(
        "FCP Federation volume selection failed: " + $_.Exception.Message
    )
    exit 1
}
