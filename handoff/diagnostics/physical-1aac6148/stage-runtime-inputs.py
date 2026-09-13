"""Review preserved live inputs and stage frozen source under the host mutation lock."""
import argparse,contextlib,ctypes,hashlib,json,os,pathlib,shutil,subprocess,sys
windows=os.name=='nt';sha='1aac6148759d7b2fd488ec26b97e1a786bdafa80';baseline='e6a9b74a1d555609eed6bf40c800e1258f1c9077'
h=pathlib.Path('C:/wsl/fcp-v1-1aac6148-main-20260913' if windows else '/home/martin/fcp-v1-1aac6148-main-20260913/source')
old=pathlib.Path('C:/wsl/fcp-v1-p01-filesystem-growth-20260913' if windows else '/home/martin/fcp-v1-501b528e-harness-20260913')
r=pathlib.Path('C:/wsl/fcp-v1-73c779-nettking-runtime-20260910' if windows else '/home/martin/fcp-v1-73c779-nitro-20260910/source')
host='nettking' if windows else 'nitro';control=h/'.acceptance/runtime-control';before=old/'.acceptance/runtime-control'
assert json.loads((h/'.acceptance/authoritative-freeze.json').read_text())['candidate_sha']==sha
ready=json.loads((h/'.acceptance/native-readiness/status.json').read_text());assert ready['status']=='COMPLETED' and ready['gate_summary']['passed']
p=argparse.ArgumentParser();p.add_argument('--resume-reviewed-staging',action='store_true');args=p.parse_args()
if control.exists():
 assert args.resume_reviewed_staging and not any((control/n).exists() for n in ['runtime-binding.json','environment.private.json','input-review.json']),'Inspect existing staged inputs instead of rewriting them'
def run(args,env=None,cwd=r):return subprocess.check_output(args,cwd=cwd,env=env,text=True,timeout=40,stderr=subprocess.PIPE).strip()
def git(*args):return run(['git',*args])
assert git('rev-parse','HEAD')==baseline and not git('status','--porcelain')
assert not any((r/n).exists() for n in ['.acceptance','evidence','.env','.venv'])
assert not run(['git','diff','--name-only',baseline,sha,'--','docker-compose.yml'],cwd=h)
binding=json.loads((before/'runtime-binding.json').read_text());assert binding['host_id']==host and binding['target_candidate_sha']==baseline
assert pathlib.Path(binding['runtime']['working_directory']).resolve()==r.resolve()
project=binding['runtime']['project'];live={}
for service in ['flask','relay','recorder']:
 cid=run(['docker','ps','-q','--filter','label=com.docker.compose.project='+project,'--filter','label=com.docker.compose.service='+service,'--filter','label=com.docker.compose.oneoff=False']);assert cid and '\n' not in cid
 item=json.loads(run(['docker','inspect',cid]))[0];assert item['State']['Running']
 image=json.loads(run(['docker','image','inspect',item['Image']]))[0];assert image['Config']['Labels']['no.fcp.build_commit']==baseline
 live[service]=item
values=json.loads((before/'environment.private.json').read_text());values['FCP_BUILD_COMMIT']=sha
control.mkdir(parents=True,exist_ok=True)
files=[str(r/'docker-compose.yml')]
for i,name in enumerate(binding['runtime']['config_files'][1:],1):
 source=pathlib.Path(name);target=control/f'compose-{i}{source.suffix}'
 if target.exists():assert target.read_bytes()==source.read_bytes(),'Previously staged overlay changed'
 else:shutil.copyfile(source,target)
 files.append(str(target))
# Make the existing effective FCP environment explicit, including Dockerfile
# defaults and configuration already applied to these owned containers.
preserved={'services':{service:{'environment':{k:v for k,v in (entry.split('=',1) for entry in item['Config']['Env'] if '=' in entry) if k.startswith('FCP_') and k!='FCP_BUILD_COMMIT'}} for service,item in live.items()}}
preserved_path=control/'compose.live-preserved.json'
preserved_bytes=(json.dumps(preserved,indent=2)+'\n').encode()
if preserved_path.exists():assert preserved_path.read_bytes()==preserved_bytes,'Live product configuration changed during staging'
else:preserved_path.write_bytes(preserved_bytes)
files.append(str(preserved_path))
values['COMPOSE_FILE']=os.pathsep.join(files)
env={k:v for k,v in os.environ.items() if not k.startswith(('FCP_','COMPOSE_','OLLAMA_'))};env.update(values)
config=json.loads(run(['docker','compose','config','--format','json'],env));assert config['name']==project
for service,item in live.items():
 proposed=config['services'][service];assert pathlib.Path(proposed['build']['context']).resolve()==r.resolve()
 assert proposed['build']['args']['FCP_BUILD_COMMIT']==sha
 # Compose may use an implicit project/service image name. The checked-in
 # build controller proves the embedded commit label after the actual build.
 if 'image' in proposed:assert proposed['image'].endswith(':'+sha)
 previous=dict(v.split('=',1) for v in item['Config']['Env'] if '=' in v)
 assert all(proposed['environment'].get(k)==v for k,v in previous.items() if k.startswith('FCP_') and k!='FCP_BUILD_COMMIT'),'Live product environment drift'
 actual_mounts={(m['Destination'],m['Type'],m.get('Name',m['Source'])) for m in item['Mounts']}
 proposed_mounts={(m['target'],m['type'],config['volumes'][m['source']]['name'] if m['type']=='volume' else m['source']) for m in proposed.get('volumes',[])}
 assert actual_mounts==proposed_mounts and proposed.get('user','')==item['Config']['User'],'Live mounts or runtime user drift'
sys.path.insert(0,str(h))
from catalog.federation.host_build import host_mutation_lock,builder_name
path_hash=hashlib.sha256(str(r.resolve()).lower().encode()).hexdigest()[:24].upper()
builder='fcp-build-'+path_hash if windows else builder_name(r)
@contextlib.contextmanager
def lease():
 if not windows:
  with host_mutation_lock(r):yield
  return
 from ctypes import wintypes
 k=ctypes.WinDLL('kernel32',use_last_error=True)
 k.CreateMutexW.argtypes=[ctypes.c_void_p,wintypes.BOOL,wintypes.LPCWSTR];k.CreateMutexW.restype=wintypes.HANDLE
 k.WaitForSingleObject.argtypes=[wintypes.HANDLE,wintypes.DWORD];k.WaitForSingleObject.restype=wintypes.DWORD
 k.ReleaseMutex.argtypes=[wintypes.HANDLE];k.CloseHandle.argtypes=[wintypes.HANDLE]
 mutex=k.CreateMutexW(None,False,'Global\\FCPHostMutation-'+path_hash);assert mutex,'Host mutation mutex unavailable'
 acquired=False
 try:
  acquired=k.WaitForSingleObject(mutex,30000) in [0,0x80];assert acquired,'Host mutation busy'
  yield
 finally:
  if acquired:k.ReleaseMutex(mutex)
  k.CloseHandle(mutex)
with lease():
 assert git('rev-parse','HEAD')==baseline and not git('status','--porcelain')
 names=run(['docker','ps','--format','{{.Names}}']).splitlines()
 assert not any(n.casefold().startswith(('buildx_buildkit_'+builder).casefold()) for n in names),'Owned build writer is active'
 bundle=h/'.acceptance/native-source.bundle' if windows else pathlib.Path('/home/martin/fcp-v1-e6a9b74a-nitro-20260913/source/.acceptance/1aac6148-native-source.bundle')
 git('fetch',str(bundle),'HEAD');git('checkout','--detach',sha)
 assert git('rev-parse','HEAD')==sha and not git('status','--porcelain')
 for item in live.values():
  now=json.loads(run(['docker','inspect',item['Id']]))[0]
  assert now['State']['Running'] and now['Image']==item['Image'] and now['RestartCount']==item['RestartCount']
binding.update(target_candidate_sha=sha,acceptance_harness_sha=sha,harness_checkout=str(h));binding['runtime']['config_files']=files
(control/'runtime-binding.json').write_text(json.dumps(binding,indent=2)+'\n');(control/'environment.private.json').write_text(json.dumps(values,indent=2)+'\n')
(control/'compose.resolved.private.json').write_text(json.dumps(config,indent=2)+'\n')
from scripts.acceptance import v1_physical_runtime_binding
bound=v1_physical_runtime_binding.load(control/'runtime-binding.json',host_id=host,target_candidate_sha=sha);assert bound.acceptance_harness_sha==sha
review={'host':host,'candidate':sha,'source_clean':True,'source_mutation_lock_held':True,'prior_owned_builder_quiescent':True,'runtime_build_context_clean':True,'existing_product_environment_preserved':True,'live_mounts_and_user_preserved':True,'existing_containers_unchanged':True,'configuration_sha256':hashlib.sha256(json.dumps(config,sort_keys=True).encode()).hexdigest(),'runtime_activated':False,'physical_pass':False,'protected_data_accessed':False}
(control/'input-review.json').write_text(json.dumps(review,indent=2)+'\n');print(json.dumps(review))
