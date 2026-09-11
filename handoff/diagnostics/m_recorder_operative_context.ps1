$ErrorActionPreference='Stop'
[Console]::OutputEncoding=[Text.UTF8Encoding]::new($false)
if ([Environment]::MachineName -ne 'DESKTOP-N5KI14R' -or $env:USERNAME -ne 'Utlån') {throw 'wrong_context'}
$base='C:\Users\Utlån\fcp-v1-0536f03d-20260911'
$python=Join-Path $base 'tooling\python312\python.exe'
$code="import hashlib,json,platform; print(json.dumps(dict(node=platform.node(),system=platform.system(),machine=platform.machine(),python=platform.python_version(),fingerprint=hashlib.sha256((platform.node()+'|'+platform.system()+'|'+platform.machine()).encode()).hexdigest()[:16])))"
$raw=& $python -B -c $code
if ($LASTEXITCODE -ne 0) {throw 'operative_python_failed'}
$native=$raw | Out-String | ConvertFrom-Json
if ($native.fingerprint -ne '8589698c32d44b1f') {throw 'unexpected_fingerprint'}
$source=Join-Path $base 'source'
$sha=& git -C $source rev-parse HEAD
if ($LASTEXITCODE -ne 0) {throw 'source_read_failed'}
$clean=[string]::IsNullOrWhiteSpace((& git -C $source status --porcelain --untracked-files=all | Out-String))
@{observed_at=[DateTimeOffset]::UtcNow.ToString('o');native=$native;user=[Security.Principal.WindowsIdentity]::GetCurrent().Name;python=$python;source=$source;source_sha=([string]$sha).Trim();source_clean=$clean;state_changed=$false;protected_record_files_read=$false;physical_acceptance=$false;prior_audit_failure='Wrong legacy per-Martin venv, resolved by existing operative tooling without host changes'} | ConvertTo-Json -Depth 5 -Compress
