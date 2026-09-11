"""One-shot, owned runtime admission. Not a software test or physical PASS."""
import datetime, hashlib, json, os, pathlib, subprocess
D=pathlib.Path(__file__).resolve().parent
A=pathlib.Path('C:/wsl/fcp-v1-fba508-nettking-20260910/.acceptance')
R=pathlib.Path(r'C:\wsl\fcp-v1-73c779-nettking-runtime-20260910')
CONTROL=A/'nettking-73c779-runtime-control'
N='9b286f931497bf6291e215f6340443c5162826b0'
C='0536f03d67eb277e11573c2188d8e820399627e3'
PROJECT='fcp-v1-73c779-nettking'
OUT=A/'nettking-9b286f93-supported-start.json'
assert not OUT.exists(), 'Inspect prior activation; never launch a duplicate'
env={k:v for k,v in os.environ.items() if not k.startswith(('FCP_','COMPOSE_','OLLAMA_')) and k.upper()!='PSMODULEPATH'}
env.update(json.loads((CONTROL/'environment.private.json').read_text()))
env['PYTHONDONTWRITEBYTECODE']='1'
env['PATH']=str(A.parent/'.venv/Scripts')+os.pathsep+env['PATH']
def now():return datetime.datetime.now(datetime.timezone.utc).isoformat()
def run(args,timeout=40):
    p=subprocess.run(args,cwd=R,env=env,capture_output=True,text=True,encoding='utf-8',errors='replace',timeout=timeout)
    if p.returncode:raise RuntimeError('Preflight command failed: '+str(args[:3])+' exit '+str(p.returncode))
    return p.stdout.strip()
assert run(['git','rev-parse','HEAD'])==C
assert not run(['git','status','--porcelain','--untracked-files=all'])
assert not run(['git','ls-files','--others','--ignored','--exclude-standard']), 'Review ignored build-context files'
assert not (CONTROL/'data/federation/update-agent/request.json').exists()
for path in ['start.cmd','scripts/windows/fcp_update_agent.ps1','scripts/windows/fcp_update_agent_runner.ps1','scripts/windows/fcp_update_engine.ps1','scripts/windows/fcp_docker_build_proxy.cmd','scripts/windows/fcp_host_build.ps1']:
    assert run(['git','rev-parse',C+':'+path])==run(['git','rev-parse',N+':'+path]), 'Review changed loaded launcher/updater'
raw=run(['docker','compose','config','--format','json']); config=json.loads(raw)
assert config['name']==PROJECT
before=[]
for name in ['flask','relay','recorder','ollama']:
    cid=run(['docker','ps','-q','--no-trunc','--filter','label=com.docker.compose.project='+PROJECT,'--filter','label=com.docker.compose.service='+name]);assert cid and '\n' not in cid
    c=json.loads(run(['docker','inspect',cid]))[0]
    assert c['State']['Running'] and not c['State']['OOMKilled']
    before.append({'service':name,'id':cid,'image':c['Image'],'started_at':c['State']['StartedAt'],'mounts':c['Mounts']})
    if name!='ollama':
        assert json.loads(run(['docker','image','inspect',c['Image']]))[0]['Config']['Labels']['no.fcp.build_commit']==C
        assert pathlib.Path(config['services'][name]['build']['context']).resolve()==R.resolve()
for name in ['flask','recorder']:
    destinations=[('/app/data',CONTROL/'data')]
    if name=='flask':destinations.append(('/app/results',CONTROL/'results'))
    for destination,path in destinations:
        mount=next(x for x in config['services'][name]['volumes'] if x['target']==destination)
        assert pathlib.Path(mount['source']).resolve()==path.resolve()
ollama=config['services']['ollama']
assert int(ollama['mem_limit'])==int(ollama['memswap_limit'])==1536*1024**2
assert ollama['gpus']==[{'count':-1}]
assert ollama['image']=='ollama/ollama:0.32.6@sha256:b88c73ace3e115f8ec53dc8761ae1c0aabfa675406e3681786b98757ce050f42'
mount=next(x for x in ollama['volumes'] if x['target']=='/root/.ollama/models')
assert mount['read_only'] and pathlib.Path(mount['source']).resolve()==pathlib.Path(r'C:\Users\Martin\.ollama\models').resolve()
manifest=pathlib.Path(mount['source'])/'manifests/registry.ollama.ai/library/llama3.2/3b'
assert hashlib.sha256(manifest.read_bytes()).hexdigest()=='a80c4f17acd55265feec403c7aef86be0c25983ab279d83f3bcd3abbcb5b8b72'
assert 'llama3.2:3b' in run(['docker','exec',next(x['id'] for x in before if x['service']=='ollama'),'ollama','list'])
for logical,suffix in [('relay_state','relay-state'),('ollama_models','ollama-models')]:assert config['volumes'][logical]['name']==PROJECT+'-'+suffix
assert env['FCP_MODEL_PROVIDER_VOLUME_NAME']==PROJECT+'-provider-models'
assert not (CONTROL/'data/capabilities/config.json').exists(), 'Review configured capture workload'
import shutil
assert shutil.disk_usage(R).free>40*1024**3
receipt={'started_at':now(),'candidate':N,'previous_candidate':C,'project':PROJECT,'source_root':str(R),'before':before,'resolved_config_sha256':hashlib.sha256(raw.encode()).hexdigest(),'safety_review_sha256':hashlib.sha256((D.parent/'PHYSICAL_M_ADMISSION.md').read_bytes()).hexdigest(),'protected_recorder_operation':False,'physical_pass':False,'status':'STARTING','source_and_launch_serialized':True}
def save():OUT.write_text(json.dumps(receipt,indent=2)+'\n')
save()
try:
    with (A/'nettking-9b286f93-supported-start.log').open('xb') as log:
        process=subprocess.Popen(['powershell.exe','-NoProfile','-NonInteractive','-ExecutionPolicy','Bypass','-File',str(D/'activate_nettking_m_source_and_start.ps1')],cwd=R,env=env,stdin=subprocess.DEVNULL,stdout=log,stderr=subprocess.STDOUT,creationflags=subprocess.CREATE_NO_WINDOW)
        receipt.update(status='RUNNING',pid=process.pid);save();print(json.dumps({'status':'RUNNING','pid':process.pid,'receipt':str(OUT)}),flush=True)
        code=process.wait(timeout=1500)
    receipt.update(exit_code=code,status='LAUNCHER_SUCCEEDED_REVIEW_REQUIRED' if code==0 else 'LAUNCHER_FAILED_RETAINED')
except Exception as error:
    receipt.update(status='INSPECT_ACTIVE_PROCESS_BEFORE_RETRY',error=str(error))
finally:
    receipt['finished_at']=now();save()
print(json.dumps({k:receipt[k] for k in ['status','candidate','finished_at']}),flush=True)
