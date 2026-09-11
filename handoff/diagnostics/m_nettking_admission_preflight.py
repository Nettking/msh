"""Read current Nettking runtime/configuration metadata before M admission."""
import datetime,hashlib,json,pathlib,platform,shutil,subprocess
R=pathlib.Path('C:/wsl/fcp-v1-73c779-nettking-runtime-20260910')
A=pathlib.Path('C:/wsl/fcp-v1-fba508-nettking-20260910/.acceptance')
CONTROL=A/'nettking-73c779-runtime-control'
N='0536f03d67eb277e11573c2188d8e820399627e3';M='9b286f931497bf6291e215f6340443c5162826b0'
def run(args):return subprocess.check_output(args,cwd=R,text=True,encoding='utf-8',errors='replace',timeout=30).strip()
assert platform.node().casefold()=='nettking'
assert run(['git','rev-parse','HEAD'])==N and not run(['git','status','--porcelain','--untracked-files=all'])
services=[]
for name in ('flask','relay','recorder','ollama'):
    cid=run(['docker','ps','-q','--no-trunc','--filter','label=com.docker.compose.project=fcp-v1-73c779-nettking','--filter','label=com.docker.compose.service='+name])
    assert cid and '\n' not in cid
    d=json.loads(run(['docker','inspect',cid]))[0];im=json.loads(run(['docker','image','inspect',d['Image']]))[0]
    env=dict(x.split('=',1) for x in d['Config']['Env'] if '=' in x)
    item={'service':name,'id':cid,'image':d['Image'],'image_sha':im['Config'].get('Labels',{}).get('no.fcp.build_commit'),
          'environment_sha':env.get('FCP_BUILD_COMMIT'),'running':d['State']['Running'],'started_at':d['State']['StartedAt'],
          'oom_killed':d['State']['OOMKilled'],'restarts':d['RestartCount'],
          'mounts':[{k:m[k] for k in ('Type','Source','Destination','RW')} for m in d['Mounts']]}
    assert item['running'] and not item['oom_killed']
    if name!='ollama': assert item['image_sha']==item['environment_sha']==N
    else:
        item['version']=run(['docker','exec',cid,'ollama','--version'])
        item['model_list']=run(['docker','exec',cid,'ollama','list'])
        item['loaded_models']=run(['docker','exec',cid,'ollama','ps'])
    services.append(item)
ps="[Console]::OutputEncoding=[Text.UTF8Encoding]::new($false); $x=Get-CimInstance Win32_OperatingSystem; $p=@(Get-CimInstance Win32_Process | Where-Object {$_.CommandLine -like '*fcp-v1-73c779-nettking-runtime-20260910*' -and ($_.CommandLine -match 'fcp_update|host_update|tailnet_join_responder')} | ForEach-Object {@{pid=$_.ProcessId;parent=$_.ParentProcessId;executable=$_.ExecutablePath;created=$_.CreationDate;is_updater=($_.CommandLine -match 'fcp_update|host_update');is_responder=($_.CommandLine -match 'tailnet_join_responder')}}); @{total_kib=$x.TotalVisibleMemorySize;free_kib=$x.FreePhysicalMemory;user=[Security.Principal.WindowsIdentity]::GetCurrent().Name;processes=$p} | ConvertTo-Json -Depth 5 -Compress"
host=json.loads(run(['powershell.exe','-NoProfile','-NonInteractive','-Command',ps]))
manifest=pathlib.Path('C:/Users/Martin/.ollama/models/manifests/registry.ollama.ai/library/llama3.2/3b')
digest=hashlib.sha256(manifest.read_bytes()).hexdigest();assert digest=='a80c4f17acd55265feec403c7aef86be0c25983ab279d83f3bcd3abbcb5b8b72'
paths=['start.cmd','scripts/windows/fcp_update_agent.ps1','scripts/windows/fcp_update_agent_runner.ps1','scripts/windows/fcp_update_engine.ps1','scripts/windows/fcp_docker_build_proxy.cmd','scripts/windows/fcp_host_build.ps1']
assert not run(['git','diff','--name-only',N,M,'--',*paths])
tailnet=json.loads(run(['tailscale','status','--json']))
out={'observed_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'target':M,'current_source':N,'source_clean':True,
 'fingerprint':hashlib.sha256(f'{platform.node()}|{platform.system()}|{platform.machine()}'.encode()).hexdigest()[:16],
 'services':services,'host':host,'disk_free_bytes':shutil.disk_usage(R).free,
 'gpu':run(['nvidia-smi','--query-gpu=name,memory.total,memory.used,utilization.gpu,driver_version','--format=csv,noheader']),
 'model_manifest_sha256':digest,'pending_update_request':(CONTROL/'data/federation/update-agent/request.json').exists(),
 'configured_capture_file_exists':(CONTROL/'data/capabilities/config.json').exists(),
 'unchanged_windows_activation_scripts':paths,'ignored_build_files':run(['git','ls-files','--others','--ignored','--exclude-standard']).splitlines(),
 'tailscale_backend':tailnet['BackendState'],'tailnet_hosts':{x['HostName']:bool(x.get('Online')) for x in [tailnet['Self'],*tailnet['Peer'].values()] if x['HostName'].casefold() in ('nettking','nitro','beast','desktop-n5ki14r','aqg7ncc')},
 'physical_acceptance':False,'state_changed':False,'protected_recorder_data_untouched':True}
pathlib.Path(__file__).with_name('m-nettking-admission-preflight.json').write_text(json.dumps(out,indent=2)+'\n')
print(json.dumps({k:out[k] for k in ('current_source','source_clean','host','disk_free_bytes','gpu','pending_update_request','configured_capture_file_exists','ignored_build_files','tailnet_hosts')}))
