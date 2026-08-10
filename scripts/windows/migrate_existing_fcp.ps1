[CmdletBinding()]
param(
    [string]$RepoRoot = ""
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version 2.0
if (Test-Path variable:PSNativeCommandUseErrorActionPreference) {
    $PSNativeCommandUseErrorActionPreference = $false
}

$ApprovedRepository = "Nettking/msh"
$ApprovedBranch = "main"
$ApprovedRemote = "origin"
$OidPattern = "^[0-9a-fA-F]{40}$"
$sourceUpdated = $false

function Resolve-Application {
    param([Parameter(Mandatory = $true)][string]$Name)

    $command = Get-Command $Name -CommandType Application -ErrorAction SilentlyContinue |
        Select-Object -First 1
    if ($null -eq $command) {
        throw "$Name was not found on PATH."
    }
    return [string]$command.Source
}

function Invoke-Native {
    param(
        [Parameter(Mandatory = $true)][string]$FilePath,
        [string[]]$Arguments = @()
    )

    $previousPreference = $ErrorActionPreference
    $hasNativePreference = Test-Path variable:PSNativeCommandUseErrorActionPreference
    if ($hasNativePreference) {
        $previousNativePreference = $PSNativeCommandUseErrorActionPreference
    }
    try {
        $ErrorActionPreference = "Continue"
        if ($hasNativePreference) {
            $PSNativeCommandUseErrorActionPreference = $false
        }
        $global:LASTEXITCODE = 0
        $output = @(& $FilePath @Arguments 2>&1)
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

function Get-UsefulOutput {
    param([object]$Result)

    $lines = @($Result.Output | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
    if ($lines.Count -gt 12) {
        $lines = @($lines | Select-Object -Last 12)
    }
    return ($lines -join [Environment]::NewLine)
}

function Require-Success {
    param(
        [Parameter(Mandatory = $true)][object]$Result,
        [Parameter(Mandatory = $true)][string]$Operation
    )

    if ($Result.ExitCode -eq 0) {
        return
    }
    $detail = Get-UsefulOutput -Result $Result
    if ([string]::IsNullOrWhiteSpace($detail)) {
        throw "$Operation failed with exit code $($Result.ExitCode)."
    }
    throw "$Operation failed with exit code $($Result.ExitCode):`n$detail"
}

function Get-SingleLine {
    param(
        [Parameter(Mandatory = $true)][object]$Result,
        [Parameter(Mandatory = $true)][string]$Operation
    )

    Require-Success -Result $Result -Operation $Operation
    $lines = @($Result.Output | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
    if ($lines.Count -ne 1) {
        throw "$Operation returned an unexpected result."
    }
    return ([string]$lines[0]).Trim()
}

function Test-ApprovedRemote {
    param([Parameter(Mandatory = $true)][string]$Value)

    $candidate = $Value.Trim().TrimEnd('/')
    if ($candidate.EndsWith(".git", [StringComparison]::OrdinalIgnoreCase)) {
        $candidate = $candidate.Substring(0, $candidate.Length - 4)
    }

    if ($candidate.StartsWith("git@github.com:", [StringComparison]::OrdinalIgnoreCase)) {
        $path = $candidate.Substring("git@github.com:".Length)
        return $path.Equals($ApprovedRepository, [StringComparison]::OrdinalIgnoreCase)
    }

    try {
        $uri = New-Object System.Uri($candidate)
    }
    catch {
        return $false
    }

    if ($uri.Scheme -notin @("https", "ssh")) {
        return $false
    }
    if (-not $uri.Host.Equals("github.com", [StringComparison]::OrdinalIgnoreCase)) {
        return $false
    }
    if (-not [string]::IsNullOrEmpty($uri.Query) -or -not [string]::IsNullOrEmpty($uri.Fragment)) {
        return $false
    }
    if ($uri.Scheme -eq "https" -and -not [string]::IsNullOrEmpty($uri.UserInfo)) {
        return $false
    }
    if (
        $uri.Scheme -eq "ssh" -and
        -not [string]::IsNullOrEmpty($uri.UserInfo) -and
        $uri.UserInfo -ne "git"
    ) {
        return $false
    }

    $path = $uri.AbsolutePath.Trim('/')
    return $path.Equals($ApprovedRepository, [StringComparison]::OrdinalIgnoreCase)
}

function Test-Ancestor {
    param(
        [Parameter(Mandatory = $true)][string]$Git,
        [Parameter(Mandatory = $true)][string]$Older,
        [Parameter(Mandatory = $true)][string]$Newer
    )

    $result = Invoke-Native -FilePath $Git -Arguments @(
        "merge-base", "--is-ancestor", $Older, $Newer
    )
    if ($result.ExitCode -eq 0) {
        return $true
    }
    if ($result.ExitCode -eq 1) {
        return $false
    }
    Require-Success -Result $result -Operation "Git ancestry validation"
    return $false
}

function Get-DefaultDataDirectory {
    param([Parameter(Mandatory = $true)][string]$Root)

    if (-not [string]::IsNullOrWhiteSpace($env:FCP_DATA_DIR)) {
        return [System.IO.Path]::GetFullPath($env:FCP_DATA_DIR)
    }
    return [System.IO.Path]::GetFullPath((Join-Path $Root "data"))
}

function Select-RetainedRelayVolume {
    param(
        [Parameter(Mandatory = $true)][string]$Root,
        [Parameter(Mandatory = $true)][string]$PowerShell
    )

    if (-not [string]::IsNullOrWhiteSpace($env:FCP_RELAY_VOLUME_NAME)) {
        Write-Host "Using explicitly selected Federation state: $env:FCP_RELAY_VOLUME_NAME"
        return
    }

    $selector = Join-Path $Root "scripts\windows\select_fcp_relay_volume.ps1"
    $dataDirectory = Get-DefaultDataDirectory -Root $Root
    $selectionFile = Join-Path $env:TEMP ("fcp-migration-relay-{0}.txt" -f [Guid]::NewGuid().ToString("N"))
    try {
        Write-Host "Locating the retained Federation state..."
        $selection = Invoke-Native -FilePath $PowerShell -Arguments @(
            "-NoProfile",
            "-ExecutionPolicy", "Bypass",
            "-File", $selector,
            "-DataDirectory", $dataDirectory,
            "-OutputFile", $selectionFile
        )
        Require-Success -Result $selection -Operation "Federation state selection"
        if (-not (Test-Path -LiteralPath $selectionFile -PathType Leaf)) {
            throw "Federation state selection did not create its result file."
        }
        $volume = (Get-Content -LiteralPath $selectionFile -Raw).Trim()
        if ([string]::IsNullOrWhiteSpace($volume)) {
            throw "Federation state selection returned an empty volume name."
        }
        $env:FCP_RELAY_VOLUME_NAME = $volume
        Write-Host "Federation state selected: $volume"
    }
    finally {
        Remove-Item -LiteralPath $selectionFile -Force -ErrorAction SilentlyContinue
    }
}

try {
    Write-Host ""
    Write-Host "FCP existing-install migration"
    Write-Host "=============================="
    Write-Host "This bootstrap only accepts the approved Nettking/msh main branch,"
    Write-Host "requires a clean checkout, and performs a fast-forward-only source update."
    Write-Host "It never resets, cleans, stashes, rebases, or deletes Docker volumes."
    Write-Host ""

    if ([string]::IsNullOrWhiteSpace($RepoRoot)) {
        $RepoRoot = (Get-Location).Path
    }
    try {
        $root = (Resolve-Path -LiteralPath $RepoRoot -ErrorAction Stop).Path
    }
    catch {
        throw "Repository path does not exist: $RepoRoot"
    }
    if (-not (Test-Path -LiteralPath $root -PathType Container)) {
        throw "Repository path is not a directory: $root"
    }

    $git = Resolve-Application -Name "git"
    $docker = Resolve-Application -Name "docker"
    $powershell = Resolve-Application -Name "powershell"

    $topLevel = Get-SingleLine -Result (
        Invoke-Native -FilePath $git -Arguments @("-C", $root, "rev-parse", "--show-toplevel")
    ) -Operation "Git repository validation"
    $resolvedTop = (Resolve-Path -LiteralPath $topLevel).Path
    if (-not $resolvedTop.Equals($root, [StringComparison]::OrdinalIgnoreCase)) {
        throw "RepoRoot must be the top level of the FCP checkout."
    }

    $remoteUrl = Get-SingleLine -Result (
        Invoke-Native -FilePath $git -Arguments @("-C", $root, "remote", "get-url", $ApprovedRemote)
    ) -Operation "Git remote validation"
    if (-not (Test-ApprovedRemote -Value $remoteUrl)) {
        throw "Migration stopped: origin is not the approved Nettking/msh GitHub repository."
    }

    $branch = Get-SingleLine -Result (
        Invoke-Native -FilePath $git -Arguments @("-C", $root, "symbolic-ref", "--quiet", "--short", "HEAD")
    ) -Operation "Git branch validation"
    if ($branch -ne $ApprovedBranch) {
        throw "Migration stopped: the checkout must be on main. Current branch: $branch"
    }

    $current = Get-SingleLine -Result (
        Invoke-Native -FilePath $git -Arguments @("-C", $root, "rev-parse", "--verify", "HEAD^{commit}")
    ) -Operation "Current commit validation"
    if ($current -notmatch $OidPattern) {
        throw "Migration stopped: HEAD is not an immutable Git commit."
    }
    $current = $current.ToLowerInvariant()

    $status = Invoke-Native -FilePath $git -Arguments @(
        "-C", $root, "status", "--porcelain=v1", "--untracked-files=all"
    )
    Require-Success -Result $status -Operation "Git status"
    if (@($status.Output | Where-Object { -not [string]::IsNullOrWhiteSpace($_) }).Count -gt 0) {
        $detail = Get-UsefulOutput -Result $status
        throw "Migration stopped without changing local files because the checkout is dirty:`n$detail"
    }

    Require-Success -Result (
        Invoke-Native -FilePath $docker -Arguments @("info")
    ) -Operation "Docker availability check"
    Require-Success -Result (
        Invoke-Native -FilePath $docker -Arguments @("compose", "version")
    ) -Operation "Docker Compose availability check"

    Write-Host "Fetching approved main..."
    $fetch = Invoke-Native -FilePath $git -Arguments @(
        "-C", $root, "fetch", "--no-tags", $ApprovedRemote, $ApprovedBranch
    )
    Require-Success -Result $fetch -Operation "Git fetch of approved main"

    $target = Get-SingleLine -Result (
        Invoke-Native -FilePath $git -Arguments @("-C", $root, "rev-parse", "--verify", "FETCH_HEAD^{commit}")
    ) -Operation "Approved target resolution"
    if ($target -notmatch $OidPattern) {
        throw "Migration stopped: approved main did not resolve to an immutable commit."
    }
    $target = $target.ToLowerInvariant()

    $requiredTargetFiles = @(
        "start.cmd",
        "docker-compose.yml",
        "scripts/windows/select_fcp_relay_volume.ps1",
        "scripts/windows/resolve_fcp_web_port.ps1",
        "scripts/windows/fcp_update_agent.ps1"
    )
    foreach ($path in $requiredTargetFiles) {
        $probe = Invoke-Native -FilePath $git -Arguments @(
            "-C", $root, "cat-file", "-e", "${target}:$path"
        )
        if ($probe.ExitCode -ne 0) {
            throw "Migration stopped: approved target does not contain required migration component $path."
        }
    }

    Write-Host ("Current source:  {0}" -f $current.Substring(0, 12))
    Write-Host ("Approved target: {0}" -f $target.Substring(0, 12))

    if ($current -ne $target) {
        $canFastForward = Test-Ancestor -Git $git -Older $current -Newer $target
        if (-not $canFastForward) {
            $targetIsOlder = Test-Ancestor -Git $git -Older $target -Newer $current
            if ($targetIsOlder) {
                throw "Migration stopped: this checkout is ahead of approved main. Nothing was changed."
            }
            throw "Migration stopped: this checkout has diverged from approved main. Nothing was changed."
        }

        Write-Host "Fast-forwarding the legacy checkout to the approved target..."
        $merge = Invoke-Native -FilePath $git -Arguments @(
            "-C", $root, "merge", "--ff-only", $target
        )
        Require-Success -Result $merge -Operation "Fast-forward migration"
        $sourceUpdated = $true
    }
    else {
        Write-Host "Source is already at the approved target. Runtime migration will still run."
    }

    $after = Get-SingleLine -Result (
        Invoke-Native -FilePath $git -Arguments @("-C", $root, "rev-parse", "--verify", "HEAD^{commit}")
    ) -Operation "Post-migration commit validation"
    if ($after.ToLowerInvariant() -ne $target) {
        throw "Migration stopped: checkout did not land on the approved target commit."
    }
    $afterStatus = Invoke-Native -FilePath $git -Arguments @(
        "-C", $root, "status", "--porcelain=v1", "--untracked-files=all"
    )
    Require-Success -Result $afterStatus -Operation "Post-migration Git status"
    if (@($afterStatus.Output | Where-Object { -not [string]::IsNullOrWhiteSpace($_) }).Count -gt 0) {
        throw "Migration stopped: the fast-forward did not leave a clean checkout."
    }

    $env:COMPOSE_PROJECT_NAME = "fcp"
    if ([string]::IsNullOrWhiteSpace($env:FCP_DATA_DIR)) {
        $env:FCP_DATA_DIR = Join-Path $root "data"
    }
    if ([string]::IsNullOrWhiteSpace($env:FCP_RESULTS_DIR)) {
        $env:FCP_RESULTS_DIR = Join-Path $root "results"
    }

    Select-RetainedRelayVolume -Root $root -PowerShell $powershell

    $startScript = Join-Path $root "start.cmd"
    if (-not (Test-Path -LiteralPath $startScript -PathType Leaf)) {
        throw "Migration stopped: updated start.cmd is missing."
    }

    Write-Host ""
    Write-Host "Starting the current FCP resume path..."
    Write-Host "Saved identity, Federation membership, evidence, data, models, and Docker volumes are retained."
    $previousPreference = $ErrorActionPreference
    try {
        $ErrorActionPreference = "Continue"
        $global:LASTEXITCODE = 0
        & $startScript --resume
        $startExit = [int]$LASTEXITCODE
    }
    finally {
        $ErrorActionPreference = $previousPreference
    }
    if ($startExit -ne 0) {
        throw "Source migration completed, but start.cmd --resume exited with code $startExit. Retained state was not deleted; rerun start.cmd --resume after resolving the reported runtime error."
    }

    Write-Host ""
    Write-Host "Migration complete."
    Write-Host ("Running checkout target: {0}" -f $target)
    Write-Host "This device now has the current host update agent and can participate in Federation -> Update all devices."
    exit 0
}
catch {
    [Console]::Error.WriteLine("")
    [Console]::Error.WriteLine("FCP migration stopped safely: " + $_.Exception.Message)
    if ($sourceUpdated) {
        [Console]::Error.WriteLine(
            "The source checkout was already fast-forwarded safely; no device/Federation state or Docker volume was deleted."
        )
    }
    exit 1
}
