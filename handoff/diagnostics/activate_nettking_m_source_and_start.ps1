$ErrorActionPreference='Stop'
$repo='C:\wsl\fcp-v1-73c779-nettking-runtime-20260910'
$control='C:\wsl\fcp-v1-fba508-nettking-20260910\.acceptance\nettking-73c779-runtime-control'
$expected='9b286f931497bf6291e215f6340443c5162826b0'
$previous='0536f03d67eb277e11573c2188d8e820399627e3'
$hash=[System.Security.Cryptography.SHA256]::Create()
try { $pathHash=([BitConverter]::ToString($hash.ComputeHash([Text.Encoding]::UTF8.GetBytes($repo.ToLowerInvariant())))).Replace('-','').Substring(0,24) } finally {$hash.Dispose()}
$lease=[Threading.Mutex]::new($false,"Global\FCPHostMutation-$pathHash")
$held=$false
try {
    try {$held=$lease.WaitOne([TimeSpan]::FromSeconds(30))} catch [Threading.AbandonedMutexException] {$held=$true}
    if (-not $held) {throw 'host_mutation_busy'}
    if (Test-Path -LiteralPath (Join-Path $control 'data\federation\update-agent\request.json')) {throw 'pending_update_request'}
    Set-Location -LiteralPath $repo
    if ((& git rev-parse HEAD) -ne $previous) {throw 'unexpected_source_before_activation'}
    if ((& git branch --show-current) -ne 'main') {throw 'unexpected_branch'}
    if ((& git remote get-url origin) -ne 'https://github.com/Nettking/msh.git') {throw 'unexpected_origin'}
    if (-not [string]::IsNullOrWhiteSpace((& git status --porcelain --untracked-files=all | Out-String))) {throw 'dirty_checkout'}
    & git merge --ff-only $expected
    if ($LASTEXITCODE -ne 0) {throw 'fast_forward_failed'}
    if ((& git rev-parse HEAD) -ne $expected) {throw 'source_identity_mismatch'}
    if (-not [string]::IsNullOrWhiteSpace((& git status --porcelain --untracked-files=all | Out-String))) {throw 'dirty_target'}
    # Same lease protocol as the checked-in fcp_host_activation_lease.ps1;
    # extend its critical section to cover the preceding clean fast-forward.
    $env:FCP_HOST_MUTATION_LEASE_ACTIVE='1'
    $env:FCP_HOST_MUTATION_LEASE_OWNER_PID=[string]$PID
    & $env:ComSpec /d /c start.cmd
    $result=$LASTEXITCODE
    if ((& git rev-parse HEAD) -ne $expected) {throw 'source_changed_during_activation'}
    exit $result
} finally {
    Remove-Item Env:FCP_HOST_MUTATION_LEASE_ACTIVE -ErrorAction SilentlyContinue
    Remove-Item Env:FCP_HOST_MUTATION_LEASE_OWNER_PID -ErrorAction SilentlyContinue
    if ($held) {$lease.ReleaseMutex()}
    $lease.Dispose()
}
