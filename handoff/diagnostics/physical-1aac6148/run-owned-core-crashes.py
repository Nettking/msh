"""Crash only verified owned service PIDs; let the unchanged Docker policy recover them."""
import datetime,json,os,pathlib,subprocess,time,urllib.request
h=pathlib.Path('C:/wsl/fcp-v1-1aac6148-main-20260913');c=h/'.acceptance/onboarding-test';r=pathlib.Path('C:/wsl/fcp-v1-1aac6148-onboarding-runtime-20260913');sha='1aac6148759d7b2fd488ec26b97e1a786bdafa80'
py='C:/wsl/fcp-v1-e6a9b74a-main-20260913/.venv/Scripts/python.exe';record=c/'P05-core-crashes-status.json';assert not record.exists(),'Inspect existing fault receipt before any continuation'
env={k:v for k,v in os.environ.items() if not k.startswith(('FCP_','COMPOSE_','OLLAMA_'))};env.update(json.loads((c/'environment.private.json').read_text()))
runner=[py,'-m','scripts.acceptance.v1_physical_runner','--checkout',str(h),'--evidence-root',str(h/'evidence/v1-physical'),'--runtime-binding',str(c/'runtime-binding.json')]
def docker(*args):return subprocess.check_output(['docker',*args],cwd=r,env=env,text=True,timeout=30).strip()
def identity():
 ids=docker('compose','ps','-q').split();items=json.loads(docker('inspect',*ids));rows={x['Config']['Labels']['com.docker.compose.service']:x for x in items}
 for service in ['flask','relay','recorder']:
  x=rows[service];assert x['Config']['Labels']['com.docker.compose.project']==env['COMPOSE_PROJECT_NAME'] and x['Config']['Labels']['no.fcp.build_commit']==sha
 return rows
def call(args,label):
 p=subprocess.run([*runner,*args],cwd=h,env=env,capture_output=True,text=True,timeout=120)
 (c/(label+'.json')).write_text(p.stdout)
 if p.stderr:(c/(label+'.private.stderr')).write_text(p.stderr)
 assert p.returncode==0,label+' refused; retain evidence'
 return json.loads(p.stdout)
status={'candidate':sha,'harness_sha':sha,'status':'RUNNING','started_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'results':[],'physical_pass':False,'protected_data_accessed':False,'AQG_requested':False,'product_configuration_changed':False}
def save():record.write_text(json.dumps(status,indent=2)+'\n')
code='''import json,os,pathlib,signal,sys
pid=int(sys.argv[1]);expected=sys.argv[2];module=sys.argv[3];mode=sys.argv[4]
assert pid>1 and len(expected)==64
root=pathlib.Path('/proc')/str(pid)
cgroup=(root/'cgroup').read_text();command=(root/'cmdline').read_bytes().split(b'\\0')
assert expected in cgroup,'PID does not belong to the exact owned container'
assert module.encode() in command,'PID is not the expected service process'
stat=(root/'stat').read_text().rsplit(')',1)[1].split();start=stat[19]
assert (root/'stat').read_text().rsplit(')',1)[1].split()[19]==start
if mode=='kill':os.kill(pid,signal.SIGKILL)
print(json.dumps({'verified_container':expected,'pid':pid,'module':module,'process_start_ticks':start,'signal_sent':mode=='kill'}))
'''
save()
try:
 for service,module in [('flask','catalog.flask_app.app'),('relay','catalog.relay.provider_service')]:
  before=identity();target=before[service];assert target['State']['Running'] and target['HostConfig']['RestartPolicy']['Name']=='unless-stopped'
  image=target['Image'];image_info=json.loads(docker('image','inspect',image))[0];assert not image_info['Config'].get('Volumes')
  base=['run','--rm','--pull','never','--network','none','--read-only','--pids-limit','16','--memory','64m','--cpus','0.25','--cap-drop','ALL','--pid','host','--security-opt','no-new-privileges','--user','0','--entrypoint','python']
  # The read-only preflight has no signal capability and does not mount any host data.
  review=json.loads(docker(*base,image,'-c',code,str(target['State']['Pid']),target['Id'],module,'read'))
  assertion=service+'-crash';status.update(phase=assertion,target_review=review);save();print(json.dumps({'phase':assertion}),flush=True)
  args=['--commit',sha,'--host','nettking','--scenario','P05','--assertion',assertion];prepared=call(['prepare',*args],'P05-'+assertion+'-prepare')
  status['prepare_id']=prepared['prepare_id'];save()
  # One transient injector adds only KILL, verifies the exact container and service,
  # sends one signal and exits. No host filesystem mount or network is available.
  proof=json.loads(docker(*base,'--cap-add','KILL',image,'-c',code,str(target['State']['Pid']),target['Id'],module,'kill'))
  status['fault_receipt']=proof;save();started=time.monotonic();recovered=None
  while time.monotonic()-started<45:
   current=json.loads(docker('inspect',target['Id']))[0]
   if current['State']['Running'] and current['State']['Pid']!=target['State']['Pid'] and current['RestartCount']==target['RestartCount']+1:
    try:
     with urllib.request.urlopen('http://'+env['FCP_WEB_BIND']+':'+env['FCP_WEB_PORT']+'/onboarding',timeout=10) as response:
      if response.status==200:recovered=current;break
    except OSError:pass
   time.sleep(0.5)
  assert recovered is not None,'Owned service did not prove one automatic restart in the bounded observation'
  after=identity()
  for name in ['flask','relay','recorder']:
   assert after[name]['Id']==before[name]['Id'] and after[name]['Image']==before[name]['Image'] and after[name]['State']['Running']
   if name!=service:assert after[name]['RestartCount']==before[name]['RestartCount'] and after[name]['State']['Pid']==before[name]['State']['Pid']
  full=[*args,'--prepare-id',prepared['prepare_id']]
  call(['action',*full,'--note','Sent one SIGKILL to the service process only after its host PID, exact owned container ID and module were verified. The existing Docker restart policy recovered the same container/image with exactly one restart and a new process. Other core IDs/images/processes/restart counts stayed unchanged; workbench HTTP200. No manual container stop/start or product configuration change.'],'P05-'+assertion+'-action')
  result=call(['verify',*full],'P05-'+assertion+'-verify');assert result['verdict']=='pass'
  status['results'].append({'assertion':assertion,'verdict':'pass','prepare_id':prepared['prepare_id'],'container':target['Id'],'image':image,'before_pid':target['State']['Pid'],'after_pid':recovered['State']['Pid'],'before_restarts':target['RestartCount'],'after_restarts':recovered['RestartCount'],'observed_recovery_seconds':time.monotonic()-started,'other_cores_unchanged':True});save()
 status.update(status='COMPLETED',phase='COMPLETE',finished_at=datetime.datetime.now(datetime.timezone.utc).isoformat());save();print(json.dumps(status))
except BaseException as exc:
 status.update(status='STOPPED',error_type=type(exc).__name__,finished_at=datetime.datetime.now(datetime.timezone.utc).isoformat());save();raise
