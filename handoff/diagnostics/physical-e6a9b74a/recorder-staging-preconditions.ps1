$ErrorActionPreference='Stop'
[Console]::OutputEncoding=[Text.UTF8Encoding]::new($false)
if ([Environment]::MachineName -ne 'DESKTOP-N5KI14R') {throw 'Wrong host'}
$source='C:\Users\Martin\fcp-v1-fba508-20260910\campaign\recorder-source'
$python='C:\Users\Utlån\fcp-v1-0536f03d-20260911\tooling\python312\python.exe'
$target=Join-Path $env:USERPROFILE 'fcp-v1-e6a9b74a-20260913'
$version=(& $python -B -c "import sys; print(sys.version.split()[0])" | Out-String).Trim()
if ($LASTEXITCODE -ne 0) {throw 'Native pinned Python unavailable'}
$top=(& git -C $source rev-parse --show-toplevel | Out-String).Trim()
if ($LASTEXITCODE -ne 0) {throw 'Owned source unavailable'}
@{host='msh-recorder';native_python=$version;owned_source_root=$top;target_exists=(Test-Path -LiteralPath $target);target=$target;free_c_bytes=(Get-PSDrive C).Free;protected_recorder_data_accessed=$false;runtime_changed=$false} | ConvertTo-Json -Compress
