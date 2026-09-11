$ErrorActionPreference='Stop'
[Console]::OutputEncoding=[Text.UTF8Encoding]::new($false)
if ([Environment]::MachineName -ne 'DESKTOP-N5KI14R') {throw 'wrong_host'}
$python='C:\Users\Martin\fcp-v1-fba508-20260910\campaign\runtime-venv\Scripts\python.exe'
$code="import hashlib,platform; print(platform.node()); print(platform.system()); print(platform.machine()); print(hashlib.sha256((platform.node()+'|'+platform.system()+'|'+platform.machine()).encode()).hexdigest()[:16])"
$output=@(& $python -B -c $code 2>&1 | ForEach-Object { [string]$_ })
$codeExit=$LASTEXITCODE
@{observed_at=[DateTimeOffset]::UtcNow.ToString('o');command='native campaign Python platform/fingerprint only';exit_code=$codeExit;output=$output;state_changed=$false;protected_record_files_read=$false} | ConvertTo-Json -Depth 4 -Compress
