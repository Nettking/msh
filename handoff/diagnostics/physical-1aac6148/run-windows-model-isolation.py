"""One owned model-service outage proves P03 isolation and P05 absence; restore it."""
import datetime,hashlib,json,os,pathlib,subprocess,time,urllib.request
h=pathlib.Path('C:/wsl/fcp-v1-1aac6148-main-20260913');c=h/'.acceptance/runtime-control';r=pathlib.Path('C:/wsl/fcp-v1-73c779-nettking-runtime-20260910');sha='1aac6148759d7b2fd488ec26b97e1a786bdafa80'
py='C:/wsl/fcp-v1-e6a9b74a-main-20260913/.venv/Scripts/python.exe';record=c/'P03-model-isolation-status.json';assert not record.exists(),'Inspect the existing outage; never repeat it'
assert json.loads((c/'P03-launchers-status.json').read_text())['status']=='STOPPED'
env={k:v for k,v in os.environ.items() if not k.startswith(('FCP_','COMPOSE_','OLLAMA_'))};env.update(json.loads((c/'environment.private.json').read_text()));env.pop('FCP_HOST_MUTATION_LEASE_ACTIVE',None);env['FCP_BUILD_COMMIT']=sha
runner=[py,'-m','scripts.acceptance.v1_physical_runner','--checkout',str(h),'--evidence-root',str(h/'evidence/v1-physical'),'--runtime-binding',str(c/'runtime-binding.json')]
def docker(*args):return subprocess.check_output(['docker',*args],cwd=r,env=env,text=True,timeout=40).strip()
def identity(service):
 cid=docker('compose','ps','-q',service);assert cid and '\n' not in cid
 item=json.loads(docker('inspect',cid))[0];assert item['State']['Running']
 return item
model=identity('ollama');core_before={svc:{k:identity(svc)[k] for k in ['Id','Image','RestartCount']} for svc in ['flask','relay','recorder']}
assert model['Config']['Labels']['com.docker.compose.project']==env['COMPOSE_PROJECT_NAME'] and model['Config']['Labels']['com.docker.compose.service']=='ollama'
status={'candidate':sha,'harness_sha':sha,'host':'nettking','status':'RUNNING','started_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'model_container':model['Id'],'core_before':core_before,'results':[],'model_stopped':False,'model_restored':False,'physical_pass':False,'protected_data_accessed':False}
def save():record.write_text(json.dumps(status,indent=2)+'\n')
def call(command,label):
 p=subprocess.run(command,cwd=h,env=env,capture_output=True,text=True,timeout=120)
 (c/(label+'.json')).write_text(p.stdout)
 if p.stderr:(c/(label+'.private.stderr')).write_text(p.stderr)
 assert p.returncode==0,label+' refused; retain evidence and restore model'
 return json.loads(p.stdout)
save();prepared=[];failure=None
try:
 for scenario,assertion in [('P03','identity-and-isolation'),('P05','ollama-absence')]:
  target=['--commit',sha,'--host','nettking','--scenario',scenario,'--assertion',assertion]
  prep=call([*runner,'prepare',*target],scenario+'-'+assertion+'-prepare');prepared.append((scenario,assertion,target,prep['prepare_id']))
 status['prepared']=[{'scenario':s,'assertion':a,'prepare_id':p} for s,a,t,p in prepared];save()
 docker('stop','--time','20',model['Id']);status['model_stopped']=True;save()
 assert not json.loads(docker('inspect',model['Id']))[0]['State']['Running']
 code="import json,os,urllib.request; url=os.environ.get('OLLAMA_BASE_URL','http://ollama:11434').rstrip('/')+'/api/tags'; unavailable=False\ntry:\n urllib.request.urlopen(url,timeout=3).close()\nexcept Exception:\n unavailable=True\nprint(json.dumps({'provider_path_unavailable':unavailable}));raise SystemExit(0 if unavailable else 1)"
 unavailable=json.loads(docker('exec',core_before['flask']['Id'],'python','-c',code));assert unavailable['provider_path_unavailable']
 bind=env.get('FCP_WEB_BIND','127.0.0.1');address='127.0.0.1' if bind in ['0.0.0.0','::'] else bind
 with urllib.request.urlopen('http://'+address+':'+env.get('FCP_WEB_PORT','5000')+'/onboarding',timeout=10) as response:assert response.status==200
 status.update(provider_path_unavailable=True,workbench_http_status_during_outage=200);save()
 for scenario,assertion,target,prepare_id in prepared:
  args=[*target,'--prepare-id',prepare_id]
  call([*runner,'action',*args,'--note','Stopped only the verified Ollama container in the owned acceptance Compose project. A request from the running Flask container confirmed its configured provider/network path was unavailable. The workbench continued to return HTTP200. Core service identities and frozen candidate were preserved; the same model container is restored after verification.'],scenario+'-'+assertion+'-action')
  proof=call([*runner,'verify',*args],scenario+'-'+assertion+'-verify');status['results'].append({'scenario':scenario,'assertion':assertion,'verdict':proof.get('verdict'),'prepare_id':prepare_id});save()
 assert {svc:{k:identity(svc)[k] for k in ['Id','Image','RestartCount']} for svc in core_before}==core_before
 status['core_containers_unchanged']=True;save()
except BaseException as exc:
 failure=exc;status.update(error_type=type(exc).__name__,error=str(exc));save()
finally:
 if status['model_stopped']:
  docker('start',model['Id']);deadline=time.monotonic()+100
  while time.monotonic()<deadline:
   current=json.loads(docker('inspect',model['Id']))[0]
   if current['State']['Running'] and current['State'].get('Health',{}).get('Status')=='healthy':status['model_restored']=True;break
   time.sleep(2)
  assert status['model_restored'],'Existing owned model did not recover; inspect before further work'
 status.update(status='STOPPED' if failure else 'COMPLETED',finished_at=datetime.datetime.now(datetime.timezone.utc).isoformat());save()
print(json.dumps(status))
if failure:raise failure
