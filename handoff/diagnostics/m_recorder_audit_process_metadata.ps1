$ErrorActionPreference='Stop'
[Console]::OutputEncoding=[Text.UTF8Encoding]::new($false)
if ([Environment]::MachineName -ne 'DESKTOP-N5KI14R') {throw 'wrong_host'}
$processes=@(Get-CimInstance Win32_Process | Where-Object { $_.ProcessId -ne $PID -and $_.CommandLine -like '*fcp-v1-0536f03d-20260911*' } | ForEach-Object {
 @{pid=$_.ProcessId;parent=$_.ParentProcessId;created=$_.CreationDate;exe=$_.ExecutablePath;is_fingerprint_probe=($_.CommandLine -like '*hashlib.sha256*');is_git=($_.Name -eq 'git.exe');is_powershell=($_.Name -eq 'powershell.exe')}
})
$p='C:\Users\Utlån\fcp-v1-0536f03d-20260911\tooling\python312\python.exe'
$file=Get-Item -LiteralPath $p
@{observed_at=[DateTimeOffset]::UtcNow.ToString('o');processes=$processes;native_exe=@{exists=$file.Exists;length=$file.Length;last_write=$file.LastWriteTimeUtc.ToString('o')};state_changed=$false;protected_record_files_read=$false} | ConvertTo-Json -Depth 5 -Compress
