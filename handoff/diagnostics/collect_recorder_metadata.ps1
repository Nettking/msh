# Read only native/container/voter metadata. Never open the protected record corpus.
$ErrorActionPreference='Stop'
[Console]::OutputEncoding=[Text.UTF8Encoding]::new($false)
if ([Environment]::MachineName -ne 'DESKTOP-N5KI14R') { throw 'wrong_host' }
$candidate='0536f03d67eb277e11573c2188d8e820399627e3'
$source='C:\Users\Utlån\fcp-v1-0536f03d-20260911\source'
$python='C:\Users\Utlån\fcp-v1-0536f03d-20260911\tooling\python312\python.exe'
$voter='C:\Users\Martin\fcp-v1-fba508-20260910\campaign\recorder-voter\control\c03\recorder-voter-status.json'
$container=(& docker inspect 777cc74ab983786ee44c124ea57adef045a5d2f94f44c4eb43b5765117678ccd | ConvertFrom-Json)[0]
if ($LASTEXITCODE -ne 0) { throw 'protected_container_metadata_unavailable' }
$image=(& docker image inspect $container.Image | ConvertFrom-Json)[0]
$status=Get-Content -Raw -LiteralPath $voter | ConvertFrom-Json
$task=Get-ScheduledTask -TaskName 'FCP-v1-fba508-recorder-voter'
$os=Get-CimInstance Win32_OperatingSystem
$native=(& $python -B -c "import hashlib,platform,json; print(json.dumps(dict(python=platform.python_version(),fingerprint=hashlib.sha256(f'{platform.node()}|{platform.system()}|{platform.machine()}'.encode()).hexdigest()[:16])))" | ConvertFrom-Json)
$metadata=@{
 id=$container.Id;image=$container.Image;build_commit=$image.Config.Labels.'no.fcp.build_commit';
 running=$container.State.Running;started_at=$container.State.StartedAt;oom=$container.State.OOMKilled;
 mounts=@($container.Mounts | Select-Object Type,Source,Destination,RW)
}
@{
 observed_at=[DateTimeOffset]::UtcNow.ToString('o');candidate_sha=$candidate;
 mode='DIAGNOSTIC ONLY — NOT PHYSICAL ACCEPTANCE EVIDENCE';host=[Environment]::MachineName;
 native=$native;source_sha=(& git -C $source rev-parse HEAD | Select-Object -Last 1);
 source_clean=[string]::IsNullOrWhiteSpace((& git -C $source status --porcelain | Out-String));
 memory=@{total_kib=$os.TotalVisibleMemorySize;free_kib=$os.FreePhysicalMemory};
 protected_container=$metadata;protected_recorder_data_untouched=$true;state_changed=$false;
 voter=@{role=$status.role;ready=$status.ready;term=$status.consensus_term;index=$status.commit_index;last_applied=$status.last_applied;file_modified=(Get-Item -LiteralPath $voter).LastWriteTimeUtc.ToString('o');task_state=[string]$task.State};
 physical_acceptance=$false
} | ConvertTo-Json -Depth 8 -Compress
