"""One fresh native CF7 readiness preparation, without runtime activation."""
import datetime,json,os,pathlib,platform,subprocess
windows=os.name=='nt';sha='1aac6148759d7b2fd488ec26b97e1a786bdafa80';baseline='e6a9b74a1d555609eed6bf40c800e1258f1c9077'
root=pathlib.Path('C:/wsl/fcp-v1-1aac6148-main-20260913' if windows else '/home/martin/fcp-v1-1aac6148-main-20260913/source')
old=pathlib.Path('C:/wsl/fcp-v1-e6a9b74a-main-20260913' if windows else '/home/martin/fcp-v1-e6a9b74a-nitro-20260913/source')
python=str(old/('.venv/Scripts/python.exe' if windows else '.venv/bin/python'));profile='local-ai' if windows else 'school-control'
control=root/'.acceptance/native-readiness';control.mkdir(exist_ok=True)
status_file=control/'status.json';assert not status_file.exists(),'Inspect existing native preparation; do not run it twice'
freeze=json.loads((root/'.acceptance/authoritative-freeze.json').read_text());assert freeze['candidate_sha']==sha and freeze['state']=='AUTHORITATIVE_FROZEN_CANDIDATE'
def git(*args):return subprocess.check_output(['git',*args],cwd=root,text=True).strip()
assert git('rev-parse','HEAD')==sha and not git('status','--porcelain')
assert not git('diff','--name-only',baseline,sha,'--','requirements.txt','constraints-phase2.txt','requirements-test.txt','pyproject.toml')
env={k:v for k,v in os.environ.items() if not k.startswith(('FCP_','COMPOSE_','OLLAMA_'))}
env['PATH']=str(pathlib.Path(python).parent)+os.pathsep+env['PATH'];env['PYTHONUTF8']='1';env['PYTHONIOENCODING']='utf-8'
tmp=root/'.acceptance/native-test-tmp';tmp.mkdir(exist_ok=True)
for k in ['TMP','TEMP','TMPDIR']:env[k]=str(tmp)
out={'candidate':sha,'host':'nettking' if windows else 'nitro','os':platform.system(),'status':'RUNNING','started_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'reused_native_python_environment':True,'dependency_declarations_unchanged_from':baseline,'physical_pass':False,'runtime_activated':False,'protected_data_accessed':False}
def save():status_file.write_text(json.dumps(out,indent=2)+'\n')
def call(command,label,timeout=90):
 result=subprocess.run(command,cwd=root,env=env,capture_output=True,text=True,timeout=timeout,**({'creationflags':subprocess.CREATE_NO_WINDOW} if windows else {}))
 (control/(label+'.private.log')).write_text(result.stdout+'\n'+result.stderr)
 assert result.returncode==0,label+' refused; inspect retained evidence'
 return result.stdout
save()
try:
 assert 'No broken requirements' in call([python,'-m','pip','check'],'dependency-check')
 out['python_version']=call([python,'--version'],'python-version').strip();save()
 call([python,'scripts/ci_release_disk_preflight.py'],'storage-precondition')
 if windows:
  values=json.loads(pathlib.Path('C:/wsl/fcp-v1-p01-filesystem-growth-20260913/.acceptance/runtime-control/environment.private.json').read_text())
  env.update({k:values[k] for k in ['FCP_CF7_OLLAMA_URL','FCP_CF7_OLLAMA_MODEL']})
 else:
  assert platform.node().casefold()=='nitro'
  cid=subprocess.check_output(['docker','ps','-q','--filter','label=com.docker.compose.project=fcp-v1-fba508-nitro','--filter','label=com.docker.compose.service=relay'],text=True).strip();assert cid and '\n' not in cid
  live=json.loads(subprocess.check_output(['docker','inspect',cid],text=True))[0]
  values=dict(v.split('=',1) for v in live['Config']['Env'] if '=' in v)
  config=json.loads(subprocess.check_output(['docker','exec',cid,'cat',values['FCP_REPLICATED_CONTROL_PLANE_CONFIG']],text=True))
  peers={f'configured-peer-{i}':str(p['host'])+':'+str(p['port']) for i,p in enumerate(config['peers']) if p['voter_id']!=config['local_voter_id']};assert len(peers)==2
  env['FCP_CF7_PEERS']=json.dumps(peers)
 base=[python,'-m','scripts.acceptance.cf7_physical_readiness','--checkout',str(root),'--evidence-root','evidence']
 target=['--machine',profile,'--commit',sha]
 call([*base,'init',*target,'--operator','Martin'],'init')
 out['preflight']=json.loads(call([*base,'preflight',*target],'preflight'));save()
 call([*base,'gate',*target],'gates',timeout=600)
 out['gate_summary']=json.loads((root/'evidence'/profile/'gate-summary.json').read_text())
 out.update(status='COMPLETED',finished_at=datetime.datetime.now(datetime.timezone.utc).isoformat());save()
 print(json.dumps({'host':out['host'],'candidate':sha,'status':'COMPLETED','physical_pass':False,'runtime_activated':False}),flush=True)
except BaseException as error:
 out.update(status='STOPPED',error_type=type(error).__name__,error=str(error),finished_at=datetime.datetime.now(datetime.timezone.utc).isoformat());save();raise
