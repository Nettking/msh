$ErrorActionPreference='Stop'
[Console]::OutputEncoding=[Text.UTF8Encoding]::new($false)
if ([Environment]::MachineName -ne 'DESKTOP-N5KI14R' -or $env:USERNAME -ne 'Utlån') {throw 'wrong_context'}
$python='C:\Users\Utlån\fcp-v1-0536f03d-20260911\tooling\python312\python.exe'
$results=@()
foreach ($test in @(@{name='interpreter';code='import sys;print(sys.version.split()[0])'},@{name='fingerprint';code="import hashlib,platform;print(hashlib.sha256((platform.node()+'|'+platform.system()+'|'+platform.machine()).encode()).hexdigest()[:16])"})) {
 $info=[Diagnostics.ProcessStartInfo]::new();$info.FileName=$python;$info.Arguments='-B -c "'+$test.code+'"'
 $info.UseShellExecute=$false;$info.CreateNoWindow=$true;$info.RedirectStandardOutput=$true;$info.RedirectStandardError=$true
 $proc=[Diagnostics.Process]::new();$proc.StartInfo=$info;$clock=[Diagnostics.Stopwatch]::StartNew();[void]$proc.Start()
 $pidStarted=$proc.Id;$complete=$proc.WaitForExit(10000)
 if (-not $complete) {$proc.Kill();[void]$proc.WaitForExit(5000)}
 $results+=@{probe=$test.name;completed_within_10s=$complete;exit_code=$proc.ExitCode;elapsed_seconds=$clock.Elapsed.TotalSeconds;pid=$pidStarted;stdout=$proc.StandardOutput.ReadToEnd().Trim();stderr=$proc.StandardError.ReadToEnd().Trim();owned_probe_stopped=(-not $complete)}
 $proc.Dispose()
 if (-not $complete) {break}
}
@{observed_at=[DateTimeOffset]::UtcNow.ToString('o');host=[Environment]::MachineName;user=[Security.Principal.WindowsIdentity]::GetCurrent().Name;results=$results;product_state_changed=$false;protected_record_files_read=$false;physical_acceptance=$false} | ConvertTo-Json -Depth 5 -Compress
