$ErrorActionPreference='Stop'
[Console]::OutputEncoding=[Text.UTF8Encoding]::new($false)
if ([Environment]::MachineName -ne 'DESKTOP-N5KI14R' -or $env:USERNAME -ne 'Utlån') {throw 'wrong_context'}
$source='C:\Users\Utlån\fcp-v1-0536f03d-20260911\source'
$git=(Get-Command git).Source
$results=@()
foreach ($test in @(@{name='source_sha';args='rev-parse HEAD'},@{name='source_clean';args='status --porcelain --untracked-files=all'})) {
 $info=[Diagnostics.ProcessStartInfo]::new();$info.FileName=$git;$info.Arguments='-C "'+$source+'" '+$test.args
 $info.EnvironmentVariables['GIT_OPTIONAL_LOCKS']='0';$info.UseShellExecute=$false;$info.CreateNoWindow=$true;$info.RedirectStandardOutput=$true;$info.RedirectStandardError=$true
 $proc=[Diagnostics.Process]::new();$proc.StartInfo=$info;$clock=[Diagnostics.Stopwatch]::StartNew();[void]$proc.Start()
 $pidStarted=$proc.Id;$complete=$proc.WaitForExit(10000)
 if (-not $complete) {$proc.Kill();[void]$proc.WaitForExit(5000)}
 $results+=@{probe=$test.name;completed_within_10s=$complete;exit_code=$proc.ExitCode;elapsed_seconds=$clock.Elapsed.TotalSeconds;pid=$pidStarted;stdout=$proc.StandardOutput.ReadToEnd().Trim();stderr=$proc.StandardError.ReadToEnd().Trim();owned_probe_stopped=(-not $complete)}
 $proc.Dispose();if (-not $complete) {break}
}
@{observed_at=[DateTimeOffset]::UtcNow.ToString('o');host=[Environment]::MachineName;source=$source;results=$results;product_state_changed=$false;protected_record_files_read=$false;physical_acceptance=$false} | ConvertTo-Json -Depth 5 -Compress
