import datetime,hashlib,json,os,pathlib,subprocess,sys
D=pathlib.Path(__file__).resolve().parent;A=pathlib.Path('C:/wsl/fcp-v1-fba508-nettking-20260910/.acceptance');H=pathlib.Path(r'C:\wsl\fcp-v1-9b286f93-merged-main-20260911');R=pathlib.Path(r'C:\wsl\fcp-v1-73c779-nettking-runtime-20260910');CONTROL=A/'nettking-73c779-runtime-control'
N='9b286f931497bf6291e215f6340443c5162826b0';PROJECT='fcp-v1-73c779-nettking';OUT=A/'nettking-9b286f93-runtime-admission.json'
assert not OUT.exists()
env={k:v for k,v in os.environ.items() if not k.startswith(('FCP_','COMPOSE_','OLLAMA_'))};env.update(json.loads((CONTROL/'environment.private.json').read_text()))
def run(args,cwd=R):return subprocess.check_output(args,cwd=cwd,env=env,text=True,encoding='utf-8',errors='replace',timeout=40).strip()
for root in [H,R]:assert run(['git','rev-parse','HEAD'],root)==N and not run(['git','status','--porcelain','--untracked-files=all'],root)
start=json.loads((A/'nettking-9b286f93-supported-start.json').read_text());assert start['exit_code']==0
sys.path.insert(0,str(H));from scripts.acceptance import v1_physical_campaign as campaign
assert campaign.host_fingerprint()=='0efb6f56566ac0eb'
services=[]
for name in ['flask','relay','recorder','ollama']:
    cid=run(['docker','compose','ps','-q',name]);assert cid and '\n' not in cid
    info=json.loads(run(['docker','inspect',cid]))[0];image=json.loads(run(['docker','image','inspect',info['Image']]))[0]
    assert info['State']['Running'] and not info['State']['OOMKilled']
    old=next(x for x in start['before'] if x['service']==name)
    def canonical_source(value):
        value=value.replace('\\','/').lower()
        if value.startswith('/run/desktop/mnt/host/c/'):value='c:/'+value[len('/run/desktop/mnt/host/c/'):]
        return value
    def mounts(x):return sorted((m['Type'],canonical_source(m['Source']),m['Destination'],m['RW']) for m in x)
    assert mounts(info['Mounts'])==mounts(old['mounts']), 'Mount changed'
    e=dict(v.split('=',1) for v in info['Config']['Env'] if '=' in v)
    item={'service':name,'container_id':info['Id'],'image_id':info['Image'],'image_commit':image['Config'].get('Labels',{}).get('no.fcp.build_commit'),'environment_commit':e.get('FCP_BUILD_COMMIT'),'running':True,'oom_killed':False,'started_at':info['State']['StartedAt'],'mounts_preserved':True}
    if name!='ollama':
        assert item['image_commit']==N and item['environment_commit']==N
        if name=='flask': assert e['FCP_AUTO_JOIN_PORT']=='5151'
        py="import hashlib,json,os,platform;from pathlib import Path;print(json.dumps(dict(python=platform.python_version(),environment_commit=os.getenv('FCP_BUILD_COMMIT'),host_build_sha256=hashlib.sha256(Path('/app/catalog/federation/host_build.py').read_bytes()).hexdigest())))"
        item['runtime_process_probe']=json.loads(run(['docker','exec',cid,'python','-B','-c',py]))
        assert item['runtime_process_probe']['environment_commit']==N
        assert item['runtime_process_probe']['host_build_sha256']==hashlib.sha256((R/'catalog/federation/host_build.py').read_bytes()).hexdigest()
    else:
        item['version']=run(['docker','exec',cid,'ollama','--version'])
        item['existing_model']=[line.strip() for line in run(['docker','exec',cid,'ollama','list']).splitlines() if line.startswith('llama3.2:3b')]
        assert item['existing_model']
        assert info['Image']=='sha256:b88c73ace3e115f8ec53dc8761ae1c0aabfa675406e3681786b98757ce050f42'
    if name!='ollama':
        paths=['catalog/federation/tailscale_host_discovery.py','catalog/flask_app/federation_pairing_routes.py','catalog/flask_app/services/capability_onboarding_service.py','catalog/flask_app/services/federation_pairing_service.py']
        probe="import hashlib,json;from pathlib import Path; print(json.dumps({p:hashlib.sha256(Path('/app',p).read_bytes()).hexdigest() for p in "+repr(paths)+"}))"
        actual=json.loads(run(['docker','exec',cid,'python','-B','-c',probe]))
        assert actual=={p:hashlib.sha256((R/p).read_bytes()).hexdigest() for p in paths}
        item['repaired_source_hashes']=actual
    services.append(item)
receipt={'observed_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'candidate':N,'host':'nettking','fingerprint':campaign.host_fingerprint(),'source_and_harness_clean':True,'services':services,'runtime_admission':'EXACT_CANDIDATE_SERVICES_VERIFIED_MEMBERSHIP_PENDING','physical_pass':False,'protected_recorder_operation':False,'startup_log_sha256':hashlib.sha256((A/'nettking-9b286f93-supported-start.log').read_bytes()).hexdigest(),'model_redownloaded':False,'mount_normalization':'Docker Desktop /run/desktop/mnt/host/c/ and C:/ are the same Windows backing path; original before metadata retained unchanged.'}
OUT.write_text(json.dumps(receipt,indent=2)+'\n')
binding=json.loads((CONTROL/'runtime-binding.json').read_text());binding.update(target_candidate_sha=N,acceptance_harness_sha=N,harness_checkout=str(H))
binding_path=CONTROL/'runtime-binding-9b286f93.json';assert not binding_path.exists();binding_path.write_text(json.dumps(binding,indent=2)+'\n')
print(json.dumps({'receipt':str(OUT),'binding':str(binding_path),'core_services_on_candidate':3,'fingerprint':receipt['fingerprint'],'membership':'PENDING','physical_pass':False}))
