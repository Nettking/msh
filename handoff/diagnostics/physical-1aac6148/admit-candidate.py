"""One supported activation of the frozen candidate on an owned test installation."""
import datetime,hashlib,json,os,pathlib,subprocess,urllib.request
windows=os.name=='nt';sha='1aac6148759d7b2fd488ec26b97e1a786bdafa80'
h=pathlib.Path('C:/wsl/fcp-v1-1aac6148-main-20260913' if windows else '/home/martin/fcp-v1-1aac6148-main-20260913/source')
r=pathlib.Path('C:/wsl/fcp-v1-73c779-nettking-runtime-20260910' if windows else '/home/martin/fcp-v1-73c779-nitro-20260910/source')
control=h/'.acceptance/runtime-control';status_file=control/'candidate-admission.json'
assert not status_file.exists(),'Inspect the existing bounded activation; never dispatch twice'
assert json.loads((h/'.acceptance/authoritative-freeze.json').read_text())['candidate_sha']==sha
inputs=json.loads((control/'input-review.json').read_text());assert inputs['candidate']==sha and inputs['source_mutation_lock_held'] and inputs['live_mounts_and_user_preserved']
ready=json.loads((h/'.acceptance/native-readiness/status.json').read_text());assert ready['status']=='COMPLETED' and ready['gate_summary']['passed']
def git(*args):return subprocess.check_output(['git',*args],cwd=r,text=True).strip()
assert git('rev-parse','HEAD')==sha and not git('status','--porcelain')
assert not any((r/n).exists() for n in ['.acceptance','evidence','.env','.venv'])
env={k:v for k,v in os.environ.items() if not k.startswith(('FCP_','COMPOSE_','OLLAMA_'))};env.update(json.loads((control/'environment.private.json').read_text()))
start_env=env.copy();start_env.pop('FCP_BUILD_COMMIT',None);start_env.pop('FCP_HOST_MUTATION_LEASE_ACTIVE',None)
status={'candidate':sha,'host':'nettking' if windows else 'nitro','status':'RUNNING','started_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'operation':'one supported activation of the owned acceptance installation','runtime_source_clean':True,'physical_pass':False,'protected_data_accessed':False}
def save():status_file.write_text(json.dumps(status,indent=2)+'\n')
save();log=control/'candidate-admission.private.log'
try:
 command=['cmd.exe','/d','/c',str(r/'start.cmd')] if windows else ['bash','start.sh']
 with log.open('wb') as output:
  p=subprocess.run(command,cwd=r,env=start_env,stdin=subprocess.DEVNULL,stdout=output,stderr=subprocess.STDOUT,timeout=1200,**({'creationflags':subprocess.CREATE_NO_WINDOW} if windows else {}))
 status['launcher_exit_code']=p.returncode;status['log_sha256']=hashlib.sha256(log.read_bytes()).hexdigest();save()
 assert p.returncode==0,'Supported activation refused; inspect the retained log without retrying'
 status['core_images']=[]
 for svc in ['flask','relay','recorder']:
  cid=subprocess.check_output(['docker','compose','ps','-q',svc],cwd=r,env=env,text=True).strip();assert cid and '\n' not in cid
  info=json.loads(subprocess.check_output(['docker','inspect',cid],text=True))[0]
  image=json.loads(subprocess.check_output(['docker','image','inspect',info['Image']],text=True))[0]
  assert info['State']['Running'] and image['Config']['Labels']['no.fcp.build_commit']==sha
  subprocess.run(['docker','exec',cid,'python','-c',"import pathlib; assert not any(pathlib.Path(p).exists() for p in ['/app/.acceptance','/app/evidence','/app/.env'])"],capture_output=True,check=True,timeout=20)
  status['core_images'].append({'service':svc,'container':cid,'image':info['Image'],'candidate':sha});save()
 bind=env.get('FCP_WEB_BIND','127.0.0.1');port=env.get('FCP_WEB_PORT','5000')
 if bind in ['0.0.0.0','::']:bind='127.0.0.1'
 with urllib.request.urlopen('http://'+bind+':'+str(port)+'/onboarding',timeout=10) as response:status['configured_http_status']=response.status
 assert status['configured_http_status']==200
 assert git('rev-parse','HEAD')==sha and not git('status','--porcelain')
 status.update(status='COMPLETED',finished_at=datetime.datetime.now(datetime.timezone.utc).isoformat());save()
 print(json.dumps({'host':status['host'],'candidate':sha,'status':'COMPLETED','core_images_verified':3,'http_status':200,'physical_pass':False}),flush=True)
except BaseException as error:
 status.update(status='STOPPED',error_type=type(error).__name__,error=str(error),finished_at=datetime.datetime.now(datetime.timezone.utc).isoformat());save();raise
