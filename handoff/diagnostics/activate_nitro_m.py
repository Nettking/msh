"""One controlled Nitro N-to-M source/build/start operation; no physical PASS."""
import datetime,hashlib,json,os,pathlib,platform,shutil,subprocess,sys
B=pathlib.Path('/home/martin/fcp-v1-73c779-nitro-20260910');R=B/'source';S=B/'inputs/c03-activation'
N='0536f03d67eb277e11573c2188d8e820399627e3';M='9b286f931497bf6291e215f6340443c5162826b0'
OUT=B/'inputs/supported-start-9b286f93.json';LOG=B/'inputs/supported-start-9b286f93-native.log'
assert platform.node().casefold()=='nitro' and os.getuid()==1000
assert not OUT.exists() and not LOG.exists(), 'Inspect any existing operation before retry'
def now():return datetime.datetime.now(datetime.timezone.utc).isoformat()
def run(args,env=None):
 p=subprocess.run(args,cwd=R,env=env,text=True,capture_output=True,timeout=40)
 assert p.returncode==0,str(args[:3])+' failed '+str(p.returncode)+': '+p.stderr[-500:]
 return p.stdout.strip()
assert run(['git','rev-parse','HEAD'])==N and not run(['git','status','--porcelain','--untracked-files=all'])
saved=json.loads((S/'supported-start-0536f03d-distinct-images.environment.private.json').read_text())
env={k:v for k,v in os.environ.items() if not k.startswith(('FCP_','COMPOSE_','OLLAMA_','PYTHON'))};env.update(saved);env['PYTHONDONTWRITEBYTECODE']='1'
assert env['FCP_BUILD_COMMIT']==N and env['COMPOSE_PROJECT_NAME']=='fcp-v1-fba508-nitro'
assert run(['python3','-B','-c','import platform;print(platform.python_version())'],env)=='3.12.13'
data=pathlib.Path(env['FCP_DATA_DIR']);assert data==pathlib.Path('/home/martin/fcp-v1-fba508-20260910/campaign/nitro-runtime/data')
assert not (data/'capabilities/config.json').exists() and not (data/'federation/update-agent/request.json').exists()
assert shutil.disk_usage(R).free>40*1024**3
ignored=run(['git','ls-files','--others','--ignored','--exclude-standard']).splitlines()
assert all('/__pycache__/' in '/'+p and p.endswith('.pyc') for p in ignored)
assert '__pycache__' in (R/'.dockerignore').read_text().splitlines()
for p in pathlib.Path('/proc').iterdir():
 if not p.name.isdigit():continue
 try:command=(p/'cmdline').read_bytes()
 except OSError:continue
 assert not (str(R).encode() in command and b'fcp_update_agent.py' in command), 'Review an existing updater before starting another'
before_config=json.loads(run(['docker','compose','config','--format','json'],env))
before=[]
for name in ('flask','relay','recorder','ollama'):
 cid=run(['docker','ps','-q','--no-trunc','--filter','label=com.docker.compose.project=fcp-v1-fba508-nitro','--filter','label=com.docker.compose.service='+name]);assert cid and '\n' not in cid
 info=json.loads(run(['docker','inspect',cid]))[0];assert info['State']['Running'] and not info['State']['OOMKilled']
 e=dict(x.split('=',1) for x in info['Config']['Env'] if '=' in x)
 if name!='ollama':
  assert e.get('FCP_BUILD_COMMIT')==N
  assert before_config['services'][name]['image']=='fcp-v1-nitro-'+name+':'+N
  assert pathlib.Path(before_config['services'][name]['build']['context']).resolve()==R
 before.append({'service':name,'id':cid,'image':info['Image'],'started_at':info['State']['StartedAt'],'mounts':info['Mounts']})
relay=next(x['id'] for x in before if x['service']=='relay')
control=json.loads(run(['docker','exec',relay,'cat','/var/lib/fcp-relay/control-plane-status.json']))
assert control['ready'] and control['role']=='LEADER' and control['commit_index']==control['last_applied']
assert before_config['services']['ollama']['image']=='ollama/ollama:0.32.6@sha256:b88c73ace3e115f8ec53dc8761ae1c0aabfa675406e3681786b98757ce050f42'
model=next(x for x in before_config['services']['ollama']['volumes'] if x['target']=='/root/.ollama/models')
assert model['read_only'] and model['source']=='/var/lib/docker/volumes/fcp_ollama_models/_data/models'
bundle=B/'inputs/source-9b286f93-from-0536.bundle'
assert hashlib.sha256(bundle.read_bytes()).hexdigest()=='af475a0f31a8aed6827feb6318ac3dd2d5e7ab218d753824db70bac424a83d29'
sys.path.insert(0,str(R));from catalog.federation.host_mutation import host_mutation_lock
receipt={'started_at':now(),'candidate':M,'previous_source':N,'status':'PRECONDITIONS_VERIFIED',
 'before':before,'control_before':control,'project':'fcp-v1-fba508-nitro','source_root':str(R),
 'command':['bash','start.sh'],'physical_pass':False,'protected_recorder_operation':False,
 'no_active_capture_configured':True,'expected_transient':'Owned Nitro services restart; authority may be unavailable during restart and must remain fail-closed. Fixed voters/data unchanged.',
 'responder':'N responder remains until M cores verified, then replace through checked-in exact-process guard; never count its old imported source as M'}
def save():OUT.write_text(json.dumps(receipt,indent=2)+'\n');OUT.chmod(0o600)
save()
try:
 with host_mutation_lock(R,timeout_seconds=30):
  assert run(['git','rev-parse','HEAD'])==N and not run(['git','status','--porcelain','--untracked-files=all'])
  assert not (data/'federation/update-agent/request.json').exists()
  run(['git','bundle','verify',str(bundle)]);run(['git','fetch','--no-tags',str(bundle),'HEAD'])
  assert run(['git','rev-parse','FETCH_HEAD'])==M
  run(['git','merge','--ff-only',M]);assert run(['git','rev-parse','HEAD'])==M
  receipt['status']='SOURCE_ADVANCED_M_CONFIG_CHECK_PENDING';save()
  env['FCP_BUILD_COMMIT']=M
  after_config=json.loads(run(['docker','compose','config','--format','json'],env))
  expected=json.loads(json.dumps(before_config))
  for name in ('flask','relay','recorder'):
   expected['services'][name]['image']='fcp-v1-nitro-'+name+':'+M
   expected['services'][name]['build']['args']['FCP_BUILD_COMMIT']=M
   expected['services'][name]['environment']['FCP_BUILD_COMMIT']=M
  expected['services']['flask']['environment']['FCP_AUTO_JOIN_PORT']='5151'
  assert after_config==expected, 'Resolved deployment changes exceeded candidate image/build/env SHA and default5151'
  assert not run(['git','status','--porcelain','--untracked-files=all'])
  ef=S/'supported-start-9b286f93.environment.private.json';assert not ef.exists()
  saved['FCP_BUILD_COMMIT']=M;ef.write_text(json.dumps(saved,indent=2)+'\n');ef.chmod(0o600)
  env['FCP_HOST_MUTATION_LEASE_ACTIVE']='1';env['FCP_HOST_MUTATION_LEASE_OWNER_PID']=str(os.getpid())
  receipt.update(environment_file=str(ef),configuration_only_expected_delta=True)
  with LOG.open('xb') as log:
   p=subprocess.Popen(['bash','start.sh'],cwd=R,env=env,stdin=subprocess.DEVNULL,stdout=log,stderr=subprocess.STDOUT)
   receipt.update(status='RUNNING',pid=p.pid);save()
   code=p.wait(timeout=1500)
  receipt.update(exit_code=code,status='LAUNCHER_SUCCEEDED_REVIEW_REQUIRED' if code==0 else 'LAUNCHER_FAILED_RETAINED')
except Exception as exc:receipt.update(status='INSPECT_OPERATION_BEFORE_RETRY',error=str(exc))
finally:receipt['finished_at']=now();save()
print(json.dumps({k:receipt.get(k) for k in ('status','candidate','exit_code','error','finished_at')}))
