[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string]$RepoRoot,
    [Parameter(Mandatory = $true)]
    [string]$DataDirectory,
    [ValidateRange(1, 30)]
    [int]$PollSeconds = 1,
    [switch]$Once
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$ApprovedRepository = 'Nettking/msh'
$ApprovedBranch = 'main'
$RequestSchema = 'fcp.host-update-request.v1'
$ResultSchema = 'fcp.host-update-result.v1'
$MaxBytes = 8192
$OidPattern = '^[0-9a-f]{40}$'
$RequestIdPattern = '^[A-Za-z0-9._:-]{1,128}$'
$ModelPattern = '^[A-Za-z0-9][A-Za-z0-9._:/-]{0,255}$'

function Normalize-DirectoryPath([string]$Value) {
    $full = [System.IO.Path]::GetFullPath($Value)
    $root = [System.IO.Path]::GetPathRoot($full)
    if ($full.Length -le $root.Length) { return $full }
    $separators = [char[]]@(
        [System.IO.Path]::DirectorySeparatorChar,
        [System.IO.Path]::AltDirectorySeparatorChar
    )
    return $full.TrimEnd($separators)
}

$RepoRoot = Normalize-DirectoryPath $RepoRoot
$DataDirectory = Normalize-DirectoryPath $DataDirectory
$AgentScriptPath = [System.IO.Path]::GetFullPath($PSCommandPath)
$AgentDirectory = Join-Path $DataDirectory 'federation\update-agent'
$RequestFile = Join-Path $AgentDirectory 'request.json'
$ResultFile = Join-Path $AgentDirectory 'result.json'
New-Item -ItemType Directory -Path $AgentDirectory -Force | Out-Null
$InitialAgentHash = (Get-FileHash -LiteralPath $AgentScriptPath -Algorithm SHA256).Hash

function Get-PathHash([string]$Value) {
    $sha = [System.Security.Cryptography.SHA256]::Create()
    try {
        $bytes = [System.Text.Encoding]::UTF8.GetBytes($Value.ToLowerInvariant())
        return ([System.BitConverter]::ToString($sha.ComputeHash($bytes))).Replace('-', '').Substring(0, 24)
    }
    finally {
        $sha.Dispose()
    }
}

function Get-RequestResultFile([string]$RequestId) {
    $sha = [System.Security.Cryptography.SHA256]::Create()
    try {
        $bytes = [System.Text.Encoding]::UTF8.GetBytes($RequestId)
        $digest = ([System.BitConverter]::ToString($sha.ComputeHash($bytes))).Replace('-', '').ToLowerInvariant()
        return Join-Path $AgentDirectory "result-$digest.json"
    }
    finally {
        $sha.Dispose()
    }
}

function Last-Text([object[]]$Values) {
    $last = $Values | Select-Object -Last 1
    if ($null -eq $last) { return '' }
    return ([string]$last)
}

function Get-AgentHash {
    return (Get-FileHash -LiteralPath $AgentScriptPath -Algorithm SHA256).Hash
}

function ConvertTo-PowerShellLiteral([string]$Value) {
    return "'" + $Value.Replace("'", "''") + "'"
}

function Start-ReplacementAgent {
    $scriptLiteral = ConvertTo-PowerShellLiteral $AgentScriptPath
    $rootLiteral = ConvertTo-PowerShellLiteral $RepoRoot
    $dataLiteral = ConvertTo-PowerShellLiteral $DataDirectory
    $command = (
        "Start-Sleep -Milliseconds 500; & $scriptLiteral " +
        "-RepoRoot $rootLiteral -DataDirectory $dataLiteral " +
        "-PollSeconds $PollSeconds"
    )
    $bytes = [System.Text.Encoding]::Unicode.GetBytes($command)
    $encoded = [Convert]::ToBase64String($bytes)
    $process = Start-Process `
        -FilePath 'powershell.exe' `
        -ArgumentList @(
            '-NoProfile',
            '-WindowStyle', 'Hidden',
            '-ExecutionPolicy', 'Bypass',
            '-EncodedCommand', $encoded
        ) `
        -WindowStyle Hidden `
        -PassThru
    if ($null -eq $process) {
        throw 'update_agent_reload_failed'
    }
}

$mutexName = 'Global\FCPUpdateAgent-' + (Get-PathHash $RepoRoot)
$createdNew = $false
$mutex = [System.Threading.Mutex]::new($true, $mutexName, [ref]$createdNew)
if (-not $createdNew) {
    exit 0
}
$mutexReleased = $false

function Write-AtomicJsonFile([string]$Path, [string]$Json) {
    $temporary = "$Path.tmp-$PID-$([guid]::NewGuid().ToString('N'))"
    [System.IO.File]::WriteAllText(
        $temporary,
        $Json,
        (New-Object System.Text.UTF8Encoding($false))
    )
    Move-Item -LiteralPath $temporary -Destination $Path -Force
}

function Write-AgentResult {
    param(
        [string]$RequestId,
        [string]$Action,
        [string]$State,
        [AllowNull()][string]$CurrentCommit,
        [AllowNull()][string]$TargetCommit,
        [AllowNull()][string]$RunningCommit,
        [AllowNull()][string]$Code,
        [string]$Message
    )
    $value = [ordered]@{
        schema = $ResultSchema
        request_id = $RequestId
        action = $Action
        state = $State
        current_commit = $CurrentCommit
        target_commit = $TargetCommit
        running_commit = $RunningCommit
        code = $Code
        message = $Message
        completed_at = [DateTimeOffset]::UtcNow.ToString('o')
    }
    $json = $value | ConvertTo-Json -Compress -Depth 5
    if ([System.Text.Encoding]::UTF8.GetByteCount($json) -gt $MaxBytes) {
        throw 'result_too_large'
    }
    $requestResultFile = Get-RequestResultFile $RequestId
    Write-AtomicJsonFile $requestResultFile $json
    Write-AtomicJsonFile $ResultFile $json
}

function Invoke-External {
    param([string]$FilePath, [string[]]$Arguments)
    # Windows PowerShell 5.1 can promote native stderr redirected with 2>&1
    # into an ErrorRecord. With the agent-wide Stop preference that turns
    # harmless diagnostics (for example `git fetch`'s "From ...") into a
    # terminating PowerShell error before LASTEXITCODE can be inspected.
    # Native process success/failure is therefore decided only by its exit code.
    $previousErrorActionPreference = $ErrorActionPreference
    try {
        $ErrorActionPreference = 'Continue'
        $output = & $FilePath @Arguments 2>&1
        $exit = $LASTEXITCODE
    }
    finally {
        $ErrorActionPreference = $previousErrorActionPreference
    }
    if ($exit -ne 0) {
        $rendered = ($output | ForEach-Object { [string]$_ }) -join "`n"
        throw "external_command_failed:${FilePath}:$exit`n$rendered"
    }
    return @($output | ForEach-Object { [string]$_ })
}

function Invoke-Git([string[]]$Arguments) {
    return Invoke-External 'git' (@('-C', $RepoRoot) + $Arguments)
}

function Test-ApprovedRemote([string]$Value) {
    $normalized = $Value.Trim().TrimEnd('/')
    if ($normalized.EndsWith('.git', [StringComparison]::OrdinalIgnoreCase)) {
        $normalized = $normalized.Substring(0, $normalized.Length - 4)
    }
    if ($normalized -match '^git@github\.com:(?<repo>[^?&#]+)$') {
        return $Matches.repo.Equals(
            $ApprovedRepository,
            [StringComparison]::OrdinalIgnoreCase
        )
    }
    try { $uri = [Uri]$normalized } catch { return $false }
    if ($uri.Scheme -notin @('https', 'ssh') -or $uri.Host -ne 'github.com') {
        return $false
    }
    if ($uri.UserInfo -and $uri.UserInfo -ne 'git') { return $false }
    if ($uri.Query -or $uri.Fragment) { return $false }
    return $uri.AbsolutePath.Trim('/').Equals(
        $ApprovedRepository,
        [StringComparison]::OrdinalIgnoreCase
    )
}

function Get-RunningCommit {
    try {
        $output = Invoke-External 'docker' @(
            'compose', 'exec', '-T', 'flask', 'python', '-c',
            "import os; print(os.environ.get('FCP_BUILD_COMMIT',''))"
        )
        $value = (Last-Text $output).Trim().ToLowerInvariant()
        if ($value -match $OidPattern) { return $value }
    }
    catch {
        return $null
    }
    return $null
}

function Test-Ancestor([string]$Older, [string]$Newer) {
    & git -C $RepoRoot merge-base --is-ancestor $Older $Newer 2>$null
    return $LASTEXITCODE -eq 0
}

function Inspect-Checkout([AllowNull()][string]$RequestedTarget) {
    $top = (Last-Text (Invoke-Git @('rev-parse', '--show-toplevel'))).Trim()
    if ((Normalize-DirectoryPath $top) -ne $RepoRoot) {
        throw 'unsupported_checkout'
    }
    $remote = (Last-Text (Invoke-Git @('remote', 'get-url', 'origin'))).Trim()
    if (-not (Test-ApprovedRemote $remote)) { throw 'unapproved_remote' }
    $branch = (Last-Text (
        Invoke-Git @('symbolic-ref', '--quiet', '--short', 'HEAD')
    )).Trim()
    if ($branch -ne $ApprovedBranch) { throw 'detached_head' }
    $current = (Last-Text (
        Invoke-Git @('rev-parse', '--verify', 'HEAD^{commit}')
    )).Trim().ToLowerInvariant()
    if ($current -notmatch $OidPattern) { throw 'unsupported_checkout' }
    $status = Invoke-Git @('status', '--porcelain=v1', '--untracked-files=all')
    if (($status -join '').Length -gt 0) {
        return [ordered]@{
            state='dirty'; current=$current; target=$RequestedTarget;
            code='dirty'; message='Local changes must be reviewed before updating.'
        }
    }
    Invoke-Git @('fetch', '--no-tags', 'origin', $ApprovedBranch) | Out-Null
    $approvedTip = (Last-Text (
        Invoke-Git @('rev-parse', '--verify', 'FETCH_HEAD^{commit}')
    )).Trim().ToLowerInvariant()
    $target = if ($RequestedTarget) {
        $RequestedTarget.ToLowerInvariant()
    } else {
        $approvedTip
    }
    if ($target -notmatch $OidPattern) { throw 'target_unavailable' }
    & git -C $RepoRoot cat-file -e "$target^{commit}" 2>$null
    if (
        $LASTEXITCODE -ne 0 -or
        -not (Test-Ancestor $target $approvedTip)
    ) {
        throw 'target_unavailable'
    }
    if ($current -eq $target) {
        return [ordered]@{
            state='up_to_date'; current=$current; target=$target;
            code=$null; message=$null
        }
    }
    if (Test-Ancestor $current $target) {
        return [ordered]@{
            state='update_available'; current=$current; target=$target;
            code=$null; message=$null
        }
    }
    if (Test-Ancestor $target $current) {
        return [ordered]@{
            state='ahead'; current=$current; target=$target; code='ahead';
            message='This checkout is ahead of approved main.'
        }
    }
    return [ordered]@{
        state='diverged'; current=$current; target=$target; code='diverged';
        message='This checkout has diverged from approved main.'
    }
}

function Wait-RuntimeVerified([string]$Target) {
    $deadline = [DateTimeOffset]::UtcNow.AddSeconds(120)
    while ([DateTimeOffset]::UtcNow -lt $deadline) {
        try {
            $commitOutput = Invoke-External 'docker' @(
                'compose', 'exec', '-T', 'flask', 'python', '-c',
                "import os; print(os.environ.get('FCP_BUILD_COMMIT',''))"
            )
            $running = (Last-Text $commitOutput).Trim().ToLowerInvariant()
            if ($running -eq $Target) {
                Invoke-External 'docker' @(
                    'compose', 'exec', '-T', 'flask', 'python', '-c',
                    "import urllib.request; r=urllib.request.urlopen('http://127.0.0.1:5000/federation', timeout=3); assert 200 <= r.status < 500"
                ) | Out-Null
                $services = Invoke-External 'docker' @(
                    'compose', 'ps', '--status', 'running', '--services'
                )
                $runningServices = @($services | ForEach-Object { $_.Trim() })
                foreach ($required in @('relay', 'recorder', 'flask')) {
                    if ($required -notin $runningServices) {
                        throw "service_not_running:$required"
                    }
                }
                return $running
            }
        }
        catch {
            # The old Flask container is expected to disappear while activation
            # is in progress.
        }
        Start-Sleep -Seconds 2
    }
    throw 'runtime_verification_timeout'
}

function Ensure-OllamaModel {
    $probe = Invoke-External 'docker' @(
        'compose', 'run', '--rm', '--no-deps', '--entrypoint', 'python',
        'flask', '-c',
        "import os; print(os.environ.get('FCP_AI_MODEL') or 'llama3.2:3b')"
    )
    $model = (Last-Text $probe).Trim()
    if ($model -notmatch $ModelPattern) {
        throw 'invalid_model_identifier'
    }
    & docker compose exec -T ollama ollama show $model *> $null
    if ($LASTEXITCODE -eq 0) {
        return $model
    }
    Invoke-External 'docker' @(
        'compose', '--profile', 'model-install', 'run', '--rm',
        '--entrypoint', '/bin/ollama', 'ollama-pull', 'pull', $model
    ) | Out-Null
    & docker compose exec -T ollama ollama show $model *> $null
    if ($LASTEXITCODE -ne 0) {
        throw 'model_verification_failed'
    }
    return $model
}

function Process-Request {
    if (-not (Test-Path -LiteralPath $RequestFile)) { return $false }
    $processing = Join-Path $AgentDirectory (
        "processing-$([guid]::NewGuid().ToString('N')).json"
    )
    try {
        Move-Item -LiteralPath $RequestFile -Destination $processing -ErrorAction Stop
    }
    catch {
        if (-not (Test-Path -LiteralPath $RequestFile)) { return $false }
        throw
    }

    $requestId = $null
    $action = $null
    $target = $null
    try {
        $info = Get-Item -LiteralPath $processing
        if ($info.Length -gt $MaxBytes) {
            return $true
        }
        $raw = [System.IO.File]::ReadAllText($processing)
        try {
            $request = $raw | ConvertFrom-Json
        }
        catch {
            return $true
        }

        $requestId = [string]$request.request_id
        $action = [string]$request.action
        $target = if ($null -eq $request.target_commit) {
            $null
        } else {
            ([string]$request.target_commit).ToLowerInvariant()
        }
        if ($request.schema -ne $RequestSchema) { throw 'malformed_message' }
        if ($requestId -notmatch $RequestIdPattern) {
            throw 'malformed_request_id'
        }
        if ($action -notin @('check', 'apply')) { throw 'malformed_action' }
        if (
            [string]$request.repository -ne $ApprovedRepository -or
            [string]$request.branch -ne $ApprovedBranch
        ) {
            throw 'unapproved_source'
        }
        if (
            $action -eq 'apply' -and
            ($null -eq $target -or $target -notmatch $OidPattern)
        ) {
            throw 'malformed_target'
        }
        if ($target -and $target -notmatch $OidPattern) {
            throw 'malformed_target'
        }
        $created = [DateTimeOffset]::Parse(
            [string]$request.created_at
        ).ToUniversalTime()
        $expires = [DateTimeOffset]::Parse(
            [string]$request.expires_at
        ).ToUniversalTime()
        $now = [DateTimeOffset]::UtcNow
        if (
            $created -gt $now.AddMinutes(1) -or
            $expires -le $now -or
            ($expires - $created).TotalMinutes -gt 15
        ) {
            throw 'expired_or_invalid_request'
        }

        if ($action -eq 'apply') {
            $activateProperty = $request.PSObject.Properties['activate_after']
            if ($null -eq $activateProperty) {
                throw 'malformed_activation_grace'
            }
            $activateAfter = [DateTimeOffset]::Parse(
                [string]$activateProperty.Value
            ).ToUniversalTime()
            if (
                $activateAfter -lt $created -or
                $activateAfter -gt $expires -or
                ($activateAfter - $created).TotalSeconds -gt 30
            ) {
                throw 'invalid_activation_grace'
            }
            $delayMilliseconds = [Math]::Ceiling(
                ($activateAfter - [DateTimeOffset]::UtcNow).TotalMilliseconds
            )
            if ($delayMilliseconds -gt 0) {
                Start-Sleep -Milliseconds ([int][Math]::Min(
                    $delayMilliseconds,
                    30000
                ))
            }
        }

        $inspection = Inspect-Checkout $target
        $runningBefore = Get-RunningCommit
        if ($action -eq 'check') {
            $message = if ($null -eq $inspection.message) {
                ''
            } else {
                [string]$inspection.message
            }
            Write-AgentResult `
                -RequestId $requestId `
                -Action $action `
                -State $inspection.state `
                -CurrentCommit $inspection.current `
                -TargetCommit $inspection.target `
                -RunningCommit $runningBefore `
                -Code $inspection.code `
                -Message $message
            return $true
        }
        if ($inspection.state -notin @('update_available', 'up_to_date')) {
            $message = if ($null -eq $inspection.message) {
                'The checkout is not eligible for activation.'
            } else {
                [string]$inspection.message
            }
            Write-AgentResult `
                -RequestId $requestId `
                -Action $action `
                -State $inspection.state `
                -CurrentCommit $inspection.current `
                -TargetCommit $inspection.target `
                -RunningCommit $runningBefore `
                -Code $inspection.code `
                -Message $message
            return $true
        }
        if ($inspection.state -eq 'update_available') {
            Invoke-Git @('merge', '--ff-only', $target) | Out-Null
        }
        $proven = (Last-Text (
            Invoke-Git @('rev-parse', '--verify', 'HEAD^{commit}')
        )).Trim().ToLowerInvariant()
        if ($proven -ne $target) { throw 'source_verification_failed' }
        $buildStatus = Invoke-Git @(
            'status', '--porcelain=v1', '--untracked-files=all'
        )
        if (($buildStatus -join '').Length -gt 0) {
            throw 'dirty_build_context'
        }

        $env:FCP_BUILD_COMMIT = $target
        Invoke-External 'docker' @(
            'compose', 'build', 'relay', 'flask', 'recorder'
        ) | Out-Null
        Invoke-External 'docker' @(
            'compose', 'up', '-d', 'relay', 'ollama', 'recorder'
        ) | Out-Null
        Ensure-OllamaModel | Out-Null
        & docker compose stop flask *> $null
        if ($LASTEXITCODE -ne 0) {
            throw 'flask_stop_failed'
        }
        & docker compose run --rm --no-deps --entrypoint python flask `
            -m catalog.flask_app.services.existing_setup_resume
        $resumeExit = $LASTEXITCODE
        if ($resumeExit -notin @(0, 4)) {
            throw "resume_failed:$resumeExit"
        }
        Invoke-External 'docker' @(
            'compose', 'up', '-d', 'flask'
        ) | Out-Null
        $running = Wait-RuntimeVerified $target
        Write-AgentResult `
            -RequestId $requestId `
            -Action $action `
            -State 'runtime_verified' `
            -CurrentCommit $target `
            -TargetCommit $target `
            -RunningCommit $running `
            -Code 'updated' `
            -Message 'FCP source, images, services, required model, and running commit were updated and verified.'
        return $true
    }
    catch {
        $safeRequestId = if (
            $requestId -and $requestId -match $RequestIdPattern
        ) {
            $requestId
        } else {
            'invalid-request'
        }
        $safeAction = if ($action -in @('check', 'apply')) {
            $action
        } else {
            'unknown'
        }
        $current = $null
        try {
            $current = (Last-Text (
                Invoke-Git @('rev-parse', '--verify', 'HEAD^{commit}')
            )).Trim().ToLowerInvariant()
        }
        catch {}
        $running = Get-RunningCommit
        Write-AgentResult `
            -RequestId $safeRequestId `
            -Action $safeAction `
            -State 'error' `
            -CurrentCommit $current `
            -TargetCommit $target `
            -RunningCommit $running `
            -Code 'host_update_failed' `
            -Message 'The host update agent stopped safely before it could verify the requested runtime.'
        Write-Warning "FCP update request failed: $($_.Exception.Message)"
        return $true
    }
    finally {
        Remove-Item `
            -LiteralPath $processing `
            -Force `
            -ErrorAction SilentlyContinue
    }
}

try {
    Set-Location -LiteralPath $RepoRoot
    while ($true) {
        $processed = Process-Request
        if ($Once) { break }
        if ($processed -and (Get-AgentHash) -ne $InitialAgentHash) {
            # Git may have updated this updater. Spawn the newly checked-out
            # script with a short delay, then release this process's mutex.
            Start-ReplacementAgent
            $mutex.ReleaseMutex() | Out-Null
            $mutexReleased = $true
            $mutex.Dispose()
            exit 0
        }
        if (-not $processed) {
            Start-Sleep -Seconds $PollSeconds
        }
    }
}
finally {
    if (-not $mutexReleased) {
        $mutex.ReleaseMutex() | Out-Null
        $mutex.Dispose()
    }
}