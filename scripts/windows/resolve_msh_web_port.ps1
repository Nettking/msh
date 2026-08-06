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
        [Parameter(Mandatory = $true)]
        [string]$Address,

        [Parameter(Mandatory = $true)]
        [int]$Port
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
    if ($decoded.Count -eq 0) {
        return $null
    }
    return $decoded[0]
}

function Get-ComposeProjectName {
    param([Parameter(Mandatory = $true)]$Inspection)

    $labels = $Inspection.Config.Labels
    if ($null -eq $labels) {
        return ""
    }
    return [string]$labels.'com.docker.compose.project'
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
    $hasMshDataMount = @($Inspection.Mounts) | Where-Object {
        [string]$_.Destination -eq "/app/data"
    }
    return @($hasMshEnvironment).Count -gt 0 -or @($hasMshDataMount).Count -gt 0
}

function Remove-LegacyMshProject {
    param(
        [Parameter(Mandatory = $true)]
        [string]$ProjectName
    )

    if ([string]::IsNullOrWhiteSpace($ProjectName)) {
        return
    }

    Write-Warning ((
        "Port {0} is owned by an older MSH Compose project '{1}'. " +
        "Its containers will be replaced; named volumes and host data are preserved."
    ) -f $PreferredPort, $ProjectName)

    $projectContainers = @(
        & docker ps -a --filter "label=com.docker.compose.project=$ProjectName" --format "{{.ID}}" 2>$null
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

$publishedOwners = @(
    & docker ps --filter "publish=$PreferredPort" --format "{{.ID}}" 2>$null
) | Where-Object { -not [string]::IsNullOrWhiteSpace($_) }

foreach ($containerId in $publishedOwners) {
    $inspection = Get-ContainerInspection -ContainerId $containerId
    if ($null -eq $inspection -or -not (Test-MshFlaskContainer -Inspection $inspection)) {
        continue
    }
    $ownerProject = Get-ComposeProjectName -Inspection $inspection
    if ($ownerProject -eq $CurrentProjectName) {
        # Docker Compose owns this exact binding already and will reuse or
        # recreate the current service during `docker compose up`.
        Write-Output $PreferredPort
        exit 0
    }
    Remove-LegacyMshProject -ProjectName $ownerProject
}

if (Test-PortAvailable -Address $BindAddress -Port $PreferredPort) {
    Write-Output $PreferredPort
    exit 0
}

if (-not $AllowFallback) {
    Write-Error (
        "MSH web port $PreferredPort is already in use by a non-MSH process. " +
        "Close that process or choose another port with MSH_WEB_PORT."
    )
    exit 2
}

$maximumPort = [Math]::Min(65535, $PreferredPort + 99)
for ($candidate = $PreferredPort + 1; $candidate -le $maximumPort; $candidate++) {
    if (Test-PortAvailable -Address $BindAddress -Port $candidate) {
        Write-Warning (
            "Port $PreferredPort is in use by another application. " +
            "MSH will use http://localhost:$candidate for this start."
        )
        Write-Output $candidate
        exit 0
    }
}

Write-Error "No free MSH web port was found between $PreferredPort and $maximumPort."
exit 3
