"""Read only current Nitro source, updater, pending request and voter status."""
import datetime,hashlib,json,os,pathlib,platform,subprocess
B=pathlib.Path('/home/martin/fcp-v1-73c779-nitro-20260910');R=B/'source'
def run(args):return subprocess.check_output(args,cwd=R,text=True,timeout=25).strip()
assert platform.node().casefold()=='nitro'
cid=run(['docker','ps','-q','--no-trunc','--filter','label=com.docker.compose.project=fcp-v1-fba508-nitro','--filter','label=com.docker.compose.service=relay'])
status=json.loads(run(['docker','exec',cid,'cat','/var/lib/fcp-relay/control-plane-status.json']))
fields=('role','ready','term','commit_index','last_applied','quorum','quorum_size','voter_count','connected_voters','leader_id')
processes=[]
for p in pathlib.Path('/proc').iterdir():
    if not p.name.isdigit():continue
    try:
        command=(p/'cmdline').read_bytes()
        if str(B).encode() in command and any(k in command for k in (b'tailnet_join_responder',b'host_update_agent',b'fcp_update')):
            processes.append({'pid':int(p.name),'cwd':os.readlink(p/'cwd'),'is_responder':b'tailnet_join_responder' in command,'is_updater':b'update_agent' in command,'exe':os.readlink(p/'exe')})
    except OSError:pass
data=pathlib.Path('/home/martin/fcp-v1-fba508-20260910/campaign/nitro-runtime/data')
print(json.dumps({'observed_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'target':'9b286f931497bf6291e215f6340443c5162826b0',
 'host':'nitro','fingerprint':hashlib.sha256(f'{platform.node()}|{platform.system()}|{platform.machine()}'.encode()).hexdigest()[:16],
 'source_sha':run(['git','rev-parse','HEAD']),'source_clean':not run(['git','status','--porcelain','--untracked-files=all']),
 'control':{k:status.get(k) for k in fields},'control_field_names':list(status),
 'pending_update_request':(data/'federation/update-agent/request.json').exists(),
 'configured_capture_file_exists':(data/'capabilities/config.json').exists(),'processes':processes,
 'memory':{x.split(':')[0]:x.split(':')[1].strip() for x in pathlib.Path('/proc/meminfo').read_text().splitlines() if x.startswith(('MemAvailable:','MemTotal:'))},
 'state_changed':False,'protected_recorder_data_untouched':True,'physical_acceptance':False},indent=2))
