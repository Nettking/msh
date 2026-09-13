$ErrorActionPreference='Stop'
[Console]::OutputEncoding=[Text.UTF8Encoding]::new($false)
if ([Environment]::MachineName -ne 'DESKTOP-N5KI14R') {throw 'Wrong physical host'}
$campaign='C:\Users\Martin\fcp-v1-fba508-20260910\campaign'
$source=Join-Path $campaign 'recorder-source'
$status=Join-Path $campaign 'recorder-voter\control\c03\recorder-voter-status.json'
$native=Join-Path $campaign 'runtime-venv\Scripts\python.exe'
$sha=(& git -C $source rev-parse HEAD | Select-Object -Last 1)
if ($LASTEXITCODE -ne 0) {throw 'Owned source unavailable'}
$clean=[string]::IsNullOrWhiteSpace((& git -C $source status --porcelain | Out-String))
$control=Get-Content -LiteralPath $status -Raw | ConvertFrom-Json
$task=Get-ScheduledTask -TaskName 'FCP-v1-fba508-recorder-voter'
$version=if (Test-Path -LiteralPath $native) { & $native -B -c "import sys; print(sys.version.split()[0])" } else {'unavailable'}
@{candidate='e6a9b74a1d555609eed6bf40c800e1258f1c9077';host='msh-recorder';source_sha=$sha;source_clean=$clean;native_python=$version;voter_task_state=[string]$task.State;voter_ready=$control.ready;voter_role=$control.role;voter_status_modified=(Get-Item -LiteralPath $status).LastWriteTimeUtc.ToString('o');protected_recorder_data_accessed=$false;runtime_changed=$false;physical_pass=$false} | ConvertTo-Json -Compress
