[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string]$BindAddress,

    [Parameter(Mandatory = $true)]
    [ValidateRange(1, 65535)]
    [int]$PreferredPort,

    [string]$CurrentProjectName = "msh",

    [switch]$AllowFallback
)

$ErrorActionPreference = "Stop"

function Test-PortAvailable {
    param(
        [Parameter(Mandatory = $true)][string]$Address,
        [Parameter(Mandatory = $true)][int]$Port
    )

    $ipAddress = [System.Net.IPAddress]::Parse($Address)
    $listener = [System.Net.Sockets.TcpListener]::new($ipAddress, $Port)
    try {
        $listener.Start()
        return $true
    }
    catch [System.Net.Sockets.SocketException] {
        return $false
    }
    finally {
        $listener.Stop()
    }
}

function Get-ContainerInspection {
    param([Parameter(Mandatory = $true)][string]$ContainerId)

    $raw = (& docker inspect $ContainerId 2>$null) -join "`n"
    if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($raw)) {
        return $null
    }
    $decoded = @($raw | ConvertFrom-Json)
    if ($decoded.Count -gt 0) {
        return $decoded[0]
    }
    return $null
}

function Get-ComposeProjectName {
    param([Parameter(Mandatory = $true)]$Inspection)

    $labels = $Inspection.Config.Labels
    if ($null -eq $labels) {
        return ""
    }
    return [string]$labels.'com.docker.compose.project'
}

function Get-Mount {
    param(
        [Parameter(Mandatory = $true)]$Inspection,
        [Parameter(Mandatory = $true)][string]$Destination
    )

    return @($Inspection.Mounts) |
        Where-Object { [string]$_.Destination -eq $Destination } |
        Select-Object -First 1
}

function Test-MshFlaskContainer {
    param([Parameter(Mandatory = $true)]$Inspection)

    $labels = $Inspection.Config.Labels
    $service = if ($null -ne $labels) {
        [string]$labels.'com.docker.compose.service'
    }
    else {
        ""
    }
    if ($service -ne "flask") {
        return $false
    }

    $hasMshEnvironment = @($Inspection.Config.Env) | Where-Object {
        $_ -like "MSH_FLASK_SECRET=*" -or
        $_ -like "MSH_SCAN_DIRS=*" -or
        $_ -like "MSH_FEDERATION_NODE_STATE_DIR=*"
    }
    $hasMshDataMount = Get-Mount -Inspection $Inspection -Destination "/app/data"
    return @($hasMshEnvironment).Count -gt 0 -or $null -ne $hasMshDataMount
}

function Get-VolumeInspection {
    param([Parameter(Mandatory = $true)][string]$VolumeName)

    $raw = (& docker volume inspect $VolumeName 2>$null) -join "`n"
    if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($raw)) {
        return $null
    }
    $decoded = @($raw | ConvertFrom-Json)
    if ($decoded.Count -gt 0) {
        return $decoded[0]
    }
    return $null
}

function Find-ProjectVolume {
    param(
        [Parameter(Mandatory = $true)][string]$ProjectName,
        [Parameter(Mandatory = $true)][string]$LogicalName
    )

    if ([string]::IsNullOrWhiteSpace($ProjectName)) {
        return ""
    }
    $values = @(
        & docker volume ls `
            --filter "label=com.docker.compose.project=$ProjectName" `
            --filter "label=com.docker.compose.volume=$LogicalName" `
            --format "{{.Name}}" 2>$null
    ) | Where-Object { -not [string]::IsNullOrWhiteSpace($_) }
    if ($values.Count -gt 0) {
        return [string]$values[0]
    }
    return ""
}

function Get-IdentityNodeId {
    param([Parameter(Mandatory = $true)][string]$DataDirectory)

    $identityPath = Join-Path $DataDirectory "federation\device\identity.json"
    if (-not (Test-Path -LiteralPath $identityPath -PathType Leaf)) {
        return ""
    }
    try {
        $identity = Get-Content -LiteralPath $identityPath -Raw | ConvertFrom-Json
        $nodeId = [string]$identity.node_id
        if ([string]::IsNullOrWhiteSpace($nodeId)) {
            return ""
        }
        return $nodeId
    }
    catch {
        Write-Warning "The saved public device identity could not be read during state recovery."
        return ""
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

function Get-RelayVolumeProbe {
    param(
        [Parameter(Mandatory = $true)][string]$VolumeName,
        [Parameter(Mandatory = $true)][string]$ProbeImage,
        [string]$NodeId = ""
    )

    $probeCode = @'
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
                result[key] = int(connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
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

    $mount = "${VolumeName}:/state:ro"
    $raw = (& docker run --rm --network none --entrypoint python `
        -v $mount $ProbeImage -c $probeCode $NodeId 2>$null) -join "`n"
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

function Remove-LegacyMshProject {
    param([Parameter(Mandatory = $true)][string]$ProjectName)

    if ([string]::IsNullOrWhiteSpace($ProjectName) -or $ProjectName -eq $CurrentProjectName) {
        return
    }

    Write-Warning ((
        "Port {0} is owned by the older MSH Compose project '{1}'. " +
        "Its containers will be replaced after its data paths and named volumes " +
        "have been retained. No Docker volume is deleted."
    ) -f $PreferredPort, $ProjectName)

    $projectContainers = @(
        & docker ps -a `
            --filter "label=com.docker.compose.project=$ProjectName" `
            --format "{{.ID}}" 2>$null
    ) | Where-Object { -not [string]::IsNullOrWhiteSpace($_) }

    foreach ($containerId in $projectContainers) {
        $running = (& docker inspect --format "{{.State.Running}}" $containerId 2>$null) -join ""
        if ($running -eq "true") {
            & docker stop --time 20 $containerId 2>$null | Out-Null
            if ($LASTEXITCODE -ne 0) {
                throw "Could not stop the older MSH container $containerId."
            }
        }
        & docker rm $containerId 2>$null | Out-Null
        if ($LASTEXITCODE -ne 0) {
            throw "Could not remove the older MSH container $containerId."
        }
    }
}

$selectedDataDirectory = [string]$env:MSH_DATA_DIR
$selectedResultsDirectory = [string]$env:MSH_RESULTS_DIR
$ownerProject = ""
$ownerRelayVolume = ""
$ownerDataDirectory = ""
$ownerResultsDirectory = ""

$publishedOwners = @(
    & docker ps --filter "publish=$PreferredPort" --format "{{.ID}}" 2>$null
) | Where-Object { -not [string]::IsNullOrWhiteSpace($_) }

foreach ($containerId in $publishedOwners) {
    $inspection = Get-ContainerInspection -ContainerId $containerId
    if ($null -eq $inspection -or -not (Test-MshFlaskContainer -Inspection $inspection)) {
        continue
    }
    $ownerProject = Get-ComposeProjectName -Inspection $inspection
    $relayMount = Get-Mount -Inspection $inspection -Destination "/var/lib/msh-relay"
    $dataMount = Get-Mount -Inspection $inspection -Destination "/app/data"
    $resultsMount = Get-Mount -Inspection $inspection -Destination "/app/results"
    if ($null -ne $relayMount -and [string]$relayMount.Type -eq "volume") {
        $ownerRelayVolume = [string]$relayMount.Name
    }
    if ($null -ne $dataMount -and [string]$dataMount.Type -eq "bind") {
        $ownerDataDirectory = [string]$dataMount.Source
    }
    if ($null -ne $resultsMount -and [string]$resultsMount.Type -eq "bind") {
        $ownerResultsDirectory = [string]$resultsMount.Source
    }
    break
}

if ($env:MSH_DATA_DIR_DEFAULTED -eq "1" -and -not [string]::IsNullOrWhiteSpace($ownerDataDirectory)) {
    $selectedDataDirectory = $ownerDataDirectory
}
if ($env:MSH_RESULTS_DIR_DEFAULTED -eq "1" -and -not [string]::IsNullOrWhiteSpace($ownerResultsDirectory)) {
    $selectedResultsDirectory = $ownerResultsDirectory
}

$nodeId = Get-IdentityNodeId -DataDirectory $selectedDataDirectory
$selectedRelayVolume = [string]$env:MSH_RELAY_VOLUME_NAME
$relayWasExplicit = -not [string]::IsNullOrWhiteSpace($selectedRelayVolume)
$probeImage = Find-ProbeImage

if (-not $relayWasExplicit) {
    $relayCandidates = @()
    if (-not [string]::IsNullOrWhiteSpace($ownerRelayVolume)) {
        $relayCandidates += $ownerRelayVolume
    }
    $relayCandidates += @(
        & docker volume ls `
            --filter "label=com.docker.compose.volume=relay_state" `
            --format "{{.Name}}" 2>$null
    )
    $relayCandidates = @($relayCandidates |
        Where-Object { -not [string]::IsNullOrWhiteSpace($_) } |
        Select-Object -Unique)

    $probes = @()
    if (-not [string]::IsNullOrWhiteSpace($probeImage)) {
        foreach ($volume in $relayCandidates) {
            $probe = Get-RelayVolumeProbe `
                -VolumeName $volume `
                -ProbeImage $probeImage `
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

    if (-not [string]::IsNullOrWhiteSpace($nodeId)) {
        $matching = @($probes |
            Where-Object { $_.memberships -gt 0 } |
            Sort-Object memberships, sessions, nodes, size -Descending)
        if ($matching.Count -gt 0) {
            $selectedRelayVolume = [string]$matching[0].volume
            if ($selectedRelayVolume -ne $ownerRelayVolume) {
                Write-Warning (
                    "Recovered Federation coordinator volume '$selectedRelayVolume' " +
                    "because it contains the saved device's active membership."
                )
            }
        }
    }
    if ([string]::IsNullOrWhiteSpace($selectedRelayVolume) -and -not [string]::IsNullOrWhiteSpace($ownerRelayVolume)) {
        $selectedRelayVolume = $ownerRelayVolume
    }
    if ([string]::IsNullOrWhiteSpace($selectedRelayVolume)) {
        $selectedRelayVolume = "msh_relay_state"
    }
}

$relayInspection = Get-VolumeInspection -VolumeName $selectedRelayVolume
$stateProject = ""
if ($null -ne $relayInspection -and $null -ne $relayInspection.Labels) {
    $stateProject = [string]$relayInspection.Labels.'com.docker.compose.project'
}
if ([string]::IsNullOrWhiteSpace($stateProject)) {
    $stateProject = $ownerProject
}

$selectedOllamaVolume = [string]$env:MSH_OLLAMA_VOLUME_NAME
if ([string]::IsNullOrWhiteSpace($selectedOllamaVolume)) {
    $selectedOllamaVolume = Find-ProjectVolume -ProjectName $stateProject -LogicalName "ollama_models"
    if ([string]::IsNullOrWhiteSpace($selectedOllamaVolume)) {
        $selectedOllamaVolume = "msh_ollama_models"
    }
}

$selectedProviderVolume = [string]$env:MSH_MODEL_PROVIDER_VOLUME_NAME
if ([string]::IsNullOrWhiteSpace($selectedProviderVolume)) {
    $selectedProviderVolume = Find-ProjectVolume -ProjectName $stateProject -LogicalName "model_provider_models"
    if ([string]::IsNullOrWhiteSpace($selectedProviderVolume)) {
        $selectedProviderVolume = "msh_model_provider_models"
    }
}

if (-not [string]::IsNullOrWhiteSpace($ownerProject) -and $ownerProject -ne $CurrentProjectName) {
    Remove-LegacyMshProject -ProjectName $ownerProject
}

$selectedPort = $PreferredPort
$portAvailable = Test-PortAvailable -Address $BindAddress -Port $PreferredPort
$currentOwnerStillUsesPort = -not [string]::IsNullOrWhiteSpace($ownerProject) -and $ownerProject -eq $CurrentProjectName
if (-not $portAvailable -and -not $currentOwnerStillUsesPort) {
    if (-not $AllowFallback) {
        Write-Error (
            "MSH web port $PreferredPort is already in use by a non-MSH process. " +
            "Close that process or choose another port with MSH_WEB_PORT."
        )
        exit 2
    }
    $maximumPort = [Math]::Min(65535, $PreferredPort + 99)
    $selectedPort = 0
    for ($candidate = $PreferredPort + 1; $candidate -le $maximumPort; $candidate++) {
        if (Test-PortAvailable -Address $BindAddress -Port $candidate) {
            $selectedPort = $candidate
            Write-Warning (
                "Port $PreferredPort is in use by another application. " +
                "MSH will use http://localhost:$candidate for this start."
            )
            break
        }
    }
    if ($selectedPort -eq 0) {
        Write-Error "No free MSH web port was found between $PreferredPort and $maximumPort."
        exit 3
    }
}

Write-Output "MSH_WEB_PORT=$selectedPort"
Write-Output "MSH_RELAY_VOLUME_NAME=$selectedRelayVolume"
Write-Output "MSH_OLLAMA_VOLUME_NAME=$selectedOllamaVolume"
Write-Output "MSH_MODEL_PROVIDER_VOLUME_NAME=$selectedProviderVolume"
Write-Output "MSH_DATA_DIR=$selectedDataDirectory"
Write-Output "MSH_RESULTS_DIR=$selectedResultsDirectory"
exit 0
