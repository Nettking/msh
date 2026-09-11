$ErrorActionPreference='Stop'
[Console]::OutputEncoding=[Text.UTF8Encoding]::new($false)
if ([Environment]::MachineName -ne 'DESKTOP-N5KI14R') {throw 'wrong_host'}
$base='C:\Users\Martin\fcp-v1-fba508-20260910\campaign'
$source=Join-Path $base 'recorder-source'
$statusPath=Join-Path $base 'recorder-voter\control\c03\recorder-voter-status.json'
$s=Get-Content -LiteralPath $statusPath -Raw | ConvertFrom-Json
$task=Get-ScheduledTask -TaskName 'FCP-v1-fba508-recorder-voter'
$c=(& docker inspect 777cc74ab983786ee44c124ea57adef045a5d2f94f44c4eb43b5765117678ccd | ConvertFrom-Json)[0]
if ($LASTEXITCODE -ne 0) {throw 'protected_container_unavailable'}
$image=(& docker image inspect $c.Image | ConvertFrom-Json)[0]
$fingerprint=& (Join-Path $base 'runtime-venv\Scripts\python.exe') -B -c "import hashlib,platform;print(hashlib.sha256(f'{platform.node()}|{platform.system()}|{platform.machine()}'.encode()).hexdigest()[:16])"
$invariant=$c.Id -eq '777cc74ab983786ee44c124ea57adef045a5d2f94f44c4eb43b5765117678ccd' -and $c.Image -eq 'sha256:204976ddb98d3c3eea3a8ac05dcd7f99be7ba460d58fc4b8204d20255c934208' -and $c.State.StartedAt -eq '2026-09-09T19:24:27.823369204Z' -and $c.State.Running
$mounts=@($c.Mounts | Select-Object Type,Source,Destination,RW)
$invariant=$invariant -and $mounts.Count -eq 1 -and $mounts[0].Source -eq 'C:\msh\git\data' -and $mounts[0].Destination -eq '/app/data' -and $mounts[0].RW
@{observed_at=[DateTimeOffset]::UtcNow.ToString('o');target='9b286f931497bf6291e215f6340443c5162826b0';host=[Environment]::MachineName;fingerprint=($fingerprint | Select-Object -Last 1);
 source_sha=(& git -C $source rev-parse HEAD | Select-Object -Last 1);source_clean=[string]::IsNullOrWhiteSpace((& git -C $source status --porcelain --untracked-files=all | Out-String));
 voter=@{role=$s.role;ready=$s.ready;term=$s.term;index=$s.commit_index;last_applied=$s.last_applied;file_modified=(Get-Item -LiteralPath $statusPath).LastWriteTimeUtc.ToString('o');task_state=[string]$task.State};
 protected_container=@{id=$c.Id;image=$c.Image;started_at=$c.State.StartedAt;running=$c.State.Running;build_commit=$image.Config.Labels.'no.fcp.build_commit';mounts=$mounts};
 protected_metadata_invariant=$invariant;protected_record_files_read=$false;protected_state_mutated=$false;physical_acceptance=$false;state_changed=$false} | ConvertTo-Json -Depth 8 -Compress
